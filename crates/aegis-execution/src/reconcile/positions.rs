//! 3-phase unlocked exchange position synchronization, unified close-ladder legacy flatten
//! (`R14-M2`), `EXCHANGE_RECONCILED_FLAT` fill-resolved cooldowns (`P1-Acct`), and Phase-4 TP/SL sync
//! (`aegis-execution::reconcile::positions`).

use crate::close_ladder::{submit_reduce_only_close_ladder, ReduceOnlyCloseLadderRequest};
use crate::http_client::{HyperliquidHttpClient, OpenOrdersSnapshot};
use crate::reconcile::ledger::{
    derive_open_position_opened_at_ms, fill_is_closing_dir, resolve_flat_exit_fill_stats,
    FifoFundingReplayOutput, ParsedHlFill,
};
use crate::reconcile::orders::resolve_working_entry_oid;
use crate::signing::infer_tick_size;
use crate::tpsl_sync::{sync_tpsl_batch_for_positions, TpslSyncRequest};
use aegis_core::{
    normalize_hyperliquid_symbol, parse_hl_f64, round_dp, ConfigMap, ExchangeAsset, HeldAssetView,
    OrderSide, Result, ENTRY_PARTIAL_GRACE_MS,
};
use aegis_db::{
    AssetUpsertInput, DatabaseManager, ExchangeOpenPositionSyncInput, FundingEventUpsertInput,
    ReconciledClosedTradeInput,
};
use aegis_risk::{
    compute_atr_from_bars, compute_initial_protective_stop, compute_initial_take_profit,
    RiskLimitsConfig,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

pub(crate) type MetaCatalog = HashMap<String, (u32, u32, f64)>;

pub(crate) fn fetch_meta_catalog_unlocked(
    http: &HyperliquidHttpClient,
    cfg: &ConfigMap,
) -> MetaCatalog {
    let mut map = HashMap::new();
    if let Ok(meta_ctxs) = http.fetch_meta_and_asset_ctxs(cfg) {
        if let Some(universe) = meta_ctxs
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|u| u.get("universe"))
            .and_then(Value::as_array)
        {
            for (idx, u) in universe.iter().enumerate() {
                let raw_name = u.get("name").and_then(Value::as_str).unwrap_or("").trim();
                if let Ok((_, norm_coin)) = normalize_hyperliquid_symbol(raw_name) {
                    let sz_dec = u
                        .get("szDecimals")
                        .and_then(Value::as_u64)
                        .unwrap_or(2)
                        .min(6) as u32;
                    let max_lev = u
                        .get("maxLeverage")
                        .and_then(|v| v.as_f64().or_else(|| v.as_u64().map(|n| n as f64)))
                        .unwrap_or(20.0)
                        .max(1.0);
                    map.insert(
                        norm_coin.to_ascii_uppercase(),
                        (idx as u32, sz_dec, max_lev),
                    );
                }
            }
        }
    }
    map
}

pub(crate) fn synthetic_asset_from_meta(
    coin: &str,
    symbol: &str,
    ref_px: f64,
    meta: (u32, u32, f64),
) -> ExchangeAsset {
    let (asset_index, sz_decimals, max_leverage) = meta;
    let lot_step_size = 10_f64.powi(-(sz_decimals as i32)).max(1e-6);
    let tick_size = infer_tick_size(ref_px.max(0.0001), sz_decimals);
    ExchangeAsset {
        id: 0,
        symbol: symbol.to_string(),
        coin: coin.to_string(),
        asset_index,
        sz_decimals,
        tick_size,
        lot_step_size,
        max_leverage,
        mark_price: ref_px,
        mid_price: ref_px,
        oracle_price: ref_px,
        funding_rate_hourly: 0.0,
        day_volume_usdc: 100_000.0,
        open_interest_usdc: 50_000.0,
        prev_day_price: ref_px,
        is_core_eligible: true,
        is_event_eligible: true,
        is_priority_watched: true,
        created_at_ms: 0,
        updated_at_ms: 0,
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AssetSeedSpec<'a> {
    pub coin: &'a str,
    pub symbol: &'a str,
    pub ref_px: f64,
    pub is_core: bool,
    pub is_event: bool,
    pub is_prio: bool,
}

pub(crate) fn resolve_or_upsert_asset_locked(
    db: &DatabaseManager,
    spec: AssetSeedSpec<'_>,
    meta_catalog: Option<&MetaCatalog>,
    assets_by_symbol: &mut HashMap<String, ExchangeAsset>,
) -> Result<Option<ExchangeAsset>> {
    let sym_upper = spec.symbol.to_ascii_uppercase();
    if let Some(existing) = assets_by_symbol.get(&sym_upper) {
        return Ok(Some(existing.clone()));
    }
    if let Some(existing) = db.get_asset_by_symbol(spec.symbol)? {
        assets_by_symbol.insert(sym_upper, existing.clone());
        return Ok(Some(existing));
    }
    let Some(&(asset_index, sz_decimals, max_leverage)) =
        meta_catalog.and_then(|m| m.get(&spec.coin.to_ascii_uppercase()))
    else {
        tracing::warn!(
            coin = %spec.coin,
            symbol = %spec.symbol,
            "Could not resolve asset_index from metaAndAssetCtxs; skipping instead of defaulting to asset_index=0"
        );
        return Ok(None);
    };
    let lot_step_size = 10_f64.powi(-(sz_decimals as i32)).max(1e-6);
    let tick = infer_tick_size(spec.ref_px, sz_decimals);
    let aid = db.upsert_exchange_asset(&AssetUpsertInput {
        raw_symbol_or_coin: spec.coin.to_string(),
        asset_index,
        sz_decimals,
        tick_size: tick,
        lot_step_size,
        max_leverage,
        mark_price: spec.ref_px,
        mid_price: spec.ref_px,
        oracle_price: spec.ref_px,
        funding_rate_hourly: 0.0,
        day_volume_usdc: 100_000.0,
        open_interest_usdc: 50_000.0,
        prev_day_price: spec.ref_px,
        is_core_eligible: spec.is_core,
        is_event_eligible: spec.is_event,
        is_priority_watched: spec.is_prio,
    })?;
    let loaded = db.get_asset_by_id(aid)?;
    if let Some(ref asset) = loaded {
        assets_by_symbol.insert(sym_upper, asset.clone());
    }
    Ok(loaded)
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedExchangePosition {
    pub coin: String,
    pub coin_upper: String,
    pub symbol: String,
    pub side: OrderSide,
    pub szi: f64,
    pub remaining_qty: f64,
    pub entry_px: f64,
    pub mark_px: f64,
    pub live_mid: f64,
    pub pos_val: f64,
    pub upnl: f64,
    pub since_open: f64,
    pub closed_on_exchange: bool,
    pub legacy_close_detail: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingTpslSync {
    pub position_id: i64,
    pub asset: ExchangeAsset,
    pub side: OrderSide,
    pub quantity: f64,
    pub mark_price: f64,
    pub desired_stop_px: f64,
    pub desired_tp_px: f64,
    pub existing_sl_oid: Option<String>,
    pub existing_tp_oid: Option<String>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_exchange_positions_unlocked(
    http: &HyperliquidHttpClient,
    db: &DatabaseManager,
    ch_state: &Value,
    assets_by_symbol: &HashMap<String, ExchangeAsset>,
    existing_open_by_coin: &HashMap<String, HeldAssetView>,
    meta_catalog: &mut Option<MetaCatalog>,
    open_orders_snap: Option<&OpenOrdersSnapshot>,
    private_key: &str,
    taker_fee_bps: f64,
    close_legacy_positions: bool,
    cfg: &ConfigMap,
) -> Vec<PreparedExchangePosition> {
    let mut out = Vec::new();
    let Some(asset_positions) = ch_state.get("assetPositions").and_then(Value::as_array) else {
        return out;
    };

    for item in asset_positions {
        let pos = item.get("position").unwrap_or(item);
        let raw_coin = pos.get("coin").and_then(Value::as_str).unwrap_or("").trim();
        let szi = parse_hl_f64(pos.get("szi"));
        if raw_coin.is_empty() || szi.abs() <= 1e-9 {
            continue;
        }
        let Ok((symbol, coin)) = normalize_hyperliquid_symbol(raw_coin) else {
            continue;
        };
        let coin_upper = coin.to_ascii_uppercase();
        let entry_px = parse_hl_f64(pos.get("entryPx")).max(0.0001);
        let pos_val = parse_hl_f64(pos.get("positionValue"));
        let upnl = parse_hl_f64(pos.get("unrealizedPnl"));
        let since_open = parse_hl_f64(pos.pointer("/cumFunding/sinceOpen"));
        let side = if szi > 0.0 {
            OrderSide::Long
        } else {
            OrderSide::Short
        };
        let qty = szi.abs();

        let known_asset = assets_by_symbol.get(&symbol.to_ascii_uppercase()).cloned();
        let mark_px = if pos_val > 0.0 && qty > 1e-9 {
            round_dp(pos_val / qty, 6)
        } else if let Some(ref a) = known_asset {
            if a.mark_price > 0.0 {
                a.mark_price
            } else {
                entry_px
            }
        } else {
            entry_px
        };
        let live_mid = known_asset
            .as_ref()
            .map(|a| {
                if a.mid_price > 0.0 {
                    a.mid_price
                } else {
                    mark_px
                }
            })
            .unwrap_or(mark_px);

        let mut closed_on_exchange = false;
        let mut remaining_qty = qty;
        let mut legacy_close_detail = None;

        if close_legacy_positions && !private_key.is_empty() {
            let close_asset = if let Some(a) = known_asset {
                Some(a)
            } else {
                if meta_catalog.is_none() {
                    *meta_catalog = Some(fetch_meta_catalog_unlocked(http, cfg));
                }
                meta_catalog
                    .as_ref()
                    .and_then(|m| m.get(&coin_upper).copied())
                    .map(|meta| synthetic_asset_from_meta(&coin, &symbol, mark_px, meta))
            };
            if let Some(ref asset) = close_asset {
                let l2_bbo = http.fetch_l2_book(&coin, cfg).ok().flatten();
                let pos_id = existing_open_by_coin
                    .get(&coin_upper)
                    .map(|h| h.position_id);
                let ladder_req = ReduceOnlyCloseLadderRequest {
                    asset,
                    position_id: pos_id,
                    live_szi: szi,
                    reference_mark_px: mark_px,
                    l2_bbo,
                    taker_fee_bps,
                    cloid_prefix: "cutover-close",
                    max_slippage_pct: None,
                    allow_passive_fallback: false,
                    existing_open_orders: open_orders_snap,
                };
                if let Ok(res) =
                    submit_reduce_only_close_ladder(http, Some(db), private_key, cfg, &ladder_req)
                {
                    remaining_qty = res.remaining_qty.max(0.0);
                    if res.fully_closed || remaining_qty <= asset.lot_step_size * 0.5 {
                        closed_on_exchange = true;
                        let avg_px = res.vwap_fill_price.unwrap_or(mark_px);
                        legacy_close_detail = Some(format!(
                            "Closed position {} ({} {:.4} @ ${:.4})",
                            asset.symbol,
                            side.as_str(),
                            qty,
                            avg_px
                        ));
                    }
                }
            }
        }

        out.push(PreparedExchangePosition {
            coin,
            coin_upper,
            symbol,
            side,
            szi,
            remaining_qty: remaining_qty.max(1e-9),
            entry_px,
            mark_px,
            live_mid,
            pos_val,
            upnl,
            since_open,
            closed_on_exchange,
            legacy_close_detail,
        });
    }

    out
}

/// Phase 3 locked DB commit for working clearances, open position 1:1 sync, `EXCHANGE_RECONCILED_FLAT`
/// fill-resolved cooldowns, closed trade replay, and `funding_events` remaining balances.
#[allow(clippy::too_many_arguments)]
pub(crate) fn commit_positions_and_ledger_locked(
    db: &DatabaseManager,
    fresh_cfg: &ConfigMap,
    snapshot_ts_ms: i64,
    exchange_equity_usdc: f64,
    eff_cap_eq: f64,
    open_coins: &HashSet<String>,
    resting_exchange_oids: &HashSet<u64>,
    cloid_to_oid: &HashMap<String, u64>,
    open_orders_fetch_ok: bool,
    active_chasing_partial_coins: &HashSet<String>,
    prepared_positions: &[PreparedExchangePosition],
    existing_open_by_coin: &HashMap<String, HeldAssetView>,
    parsed_fills_by_coin: &BTreeMap<String, Vec<ParsedHlFill>>,
    replay_out: &FifoFundingReplayOutput,
    ledger_sync_ok: bool,
    run_ledger_replay: bool,
    meta_catalog: Option<&MetaCatalog>,
    assets_by_symbol: &mut HashMap<String, ExchangeAsset>,
    in_flight_positions: &HashSet<i64>,
    close_legacy_positions: bool,
    details: &mut Vec<String>,
) -> Result<(usize, usize, usize, Vec<PendingTpslSync>)> {
    let mut legacy_positions_found = 0usize;
    let mut legacy_positions_closed = 0usize;
    let mut legacy_positions_adopted = 0usize;
    let mut local_orphans_closed = 0usize;
    let mut tpsl_syncs = Vec::new();

    // Cancel stale WORKING rows created <= snapshot_ts_ms (never cancelling in-flight or post-snapshot rows)
    if let Ok(local_ordered) = db.get_assets_ordered() {
        for o in local_ordered {
            if in_flight_positions.contains(&o.position_id) {
                continue;
            }
            if o.created_at_ms > snapshot_ts_ms {
                continue;
            }
            let is_open_on_exch = open_coins.contains(&o.coin.to_ascii_uppercase());
            let is_expired =
                snapshot_ts_ms.saturating_sub(o.created_at_ms) >= ENTRY_PARTIAL_GRACE_MS;
            let is_resting = resolve_working_entry_oid(&o, cloid_to_oid)
                .is_some_and(|oid| resting_exchange_oids.contains(&oid));
            if close_legacy_positions
                || (!is_open_on_exch && (is_expired || (open_orders_fetch_ok && !is_resting)))
            {
                let _ =
                    db.cancel_working_order(o.position_id, "EXCHANGE_RECONCILED_WORKING_CLEARED");
            }
        }
    }

    let limits = RiskLimitsConfig::from_config_map(fresh_cfg);
    let mut exchange_open_asset_ids: HashSet<i64> = HashSet::new();
    let mut coin_asset_id_cache: HashMap<String, i64> = HashMap::new();

    for prep in prepared_positions {
        let Some(asset) = resolve_or_upsert_asset_locked(
            db,
            AssetSeedSpec {
                coin: &prep.coin,
                symbol: &prep.symbol,
                ref_px: prep.entry_px,
                is_core: true,
                is_event: true,
                is_prio: true,
            },
            meta_catalog,
            assets_by_symbol,
        )?
        else {
            continue;
        };
        coin_asset_id_cache.insert(prep.coin_upper.clone(), asset.id);
        legacy_positions_found += 1;

        let _ = db.update_asset_live_quote(asset.id, prep.mark_px, prep.live_mid);

        if prep.closed_on_exchange {
            legacy_positions_closed += 1;
            let _ = db.insert_asset_cooldown(
                asset.id,
                "MANUAL",
                "Cutover position flatten cooldown",
                900,
            );
            if let Some(ref d) = prep.legacy_close_detail {
                details.push(d.clone());
            }
            continue;
        }

        if active_chasing_partial_coins.contains(&prep.coin_upper) {
            let _ = db.set_asset_priority_watch(asset.id, true);
            exchange_open_asset_ids.insert(asset.id);
            details.push(format!(
                "Preserved active chasing WORKING partial fill on {} ({} {:.4} on Hyperliquid)",
                asset.symbol,
                prep.side.as_str(),
                prep.remaining_qty
            ));
            continue;
        }

        let funding_accrued_usdc = if ledger_sync_ok {
            let partial_booked = replay_out
                .open_epoch_booked_funding_by_coin
                .get(&prep.coin_upper)
                .copied()
                .unwrap_or(0.0);
            round_dp((-prep.since_open) - partial_booked, 4)
        } else if let Some(existing) = existing_open_by_coin.get(&prep.coin_upper) {
            if existing.side.eq_ignore_ascii_case(prep.side.as_str())
                && (existing.funding_accrued_usdc.abs() > 1e-9
                    || parsed_fills_by_coin
                        .get(&prep.coin_upper)
                        .is_some_and(|fs| fs.iter().any(fill_is_closing_dir)))
            {
                existing.funding_accrued_usdc
            } else {
                round_dp(-prep.since_open, 4)
            }
        } else {
            round_dp(-prep.since_open, 4)
        };

        let tick = asset.tick_size.max(1e-8);
        let bars_1h = db.get_ohlcv_bars(asset.id, "1h", 20).unwrap_or_default();
        let atr_opt = (!bars_1h.is_empty()).then(|| {
            compute_atr_from_bars(
                &bars_1h,
                14,
                prep.mark_px,
                limits.protective_stop_default_pct,
            )
        });
        let entry_stop =
            compute_initial_protective_stop(prep.side, prep.entry_px, atr_opt, tick, &limits);
        let stop_px = if match prep.side {
            OrderSide::Long => entry_stop < prep.mark_px - tick * 0.5,
            OrderSide::Short => entry_stop > prep.mark_px + tick * 0.5,
        } {
            entry_stop
        } else {
            compute_initial_protective_stop(prep.side, prep.mark_px, atr_opt, tick, &limits)
        };
        let entry_tp =
            compute_initial_take_profit(prep.side, prep.entry_px, atr_opt, tick, &limits);
        let tp_px = if match prep.side {
            OrderSide::Long => entry_tp > prep.mark_px + tick * 0.5,
            OrderSide::Short => entry_tp < prep.mark_px - tick * 0.5,
        } {
            entry_tp
        } else {
            compute_initial_take_profit(prep.side, prep.mark_px, atr_opt, tick, &limits)
        };

        let committed_usdc = if prep.pos_val > 0.0 {
            round_dp(prep.pos_val, 4)
        } else {
            round_dp(prep.mark_px * prep.remaining_qty, 4)
        };
        let eff_basis_eq = if eff_cap_eq >= 10.0 {
            exchange_equity_usdc.min(eff_cap_eq)
        } else {
            exchange_equity_usdc
        };
        let weight = round_dp(
            (committed_usdc / eff_basis_eq.max(1.0)) * prep.side.sign(),
            4,
        );
        let opened_at_ms_opt = parsed_fills_by_coin
            .get(&prep.coin_upper)
            .and_then(|fills| derive_open_position_opened_at_ms(fills, prep.szi));

        let (pid, _newly_adopted) =
            db.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
                asset_id: asset.id,
                coin: &prep.coin,
                side: prep.side,
                entry_price: prep.entry_px,
                mark_price: prep.mark_px,
                quantity: prep.remaining_qty,
                committed_notional_usdc: committed_usdc,
                unrealized_pnl_usdc: prep.upnl,
                portfolio_weight: weight,
                protective_stop_px: stop_px,
                take_profit_px: tp_px,
                funding_accrued_usdc,
                opened_at_ms: opened_at_ms_opt,
            })?;
        if let Ok(Some(synced_pos)) = db.get_position_by_id(pid) {
            tpsl_syncs.push(PendingTpslSync {
                position_id: pid,
                asset: asset.clone(),
                side: prep.side,
                quantity: prep.remaining_qty,
                mark_price: prep.mark_px,
                desired_stop_px: synced_pos.protective_stop_px,
                desired_tp_px: synced_pos.take_profit_px,
                existing_sl_oid: synced_pos.protective_stop_oid.clone(),
                existing_tp_oid: synced_pos.take_profit_oid.clone(),
            });
        }
        let _ = db.set_asset_priority_watch(asset.id, true);
        exchange_open_asset_ids.insert(asset.id);
        legacy_positions_adopted += 1;
        details.push(format!(
            "Synced 1:1 exchange position {} ({} {:.4} @ entry=${:.4}, mark=${:.4}, notional=${:.2}, uPnL=${:+.2})",
            asset.symbol,
            prep.side.as_str(),
            prep.remaining_qty,
            prep.entry_px,
            prep.mark_px,
            committed_usdc,
            prep.upnl
        ));
    }

    // Close local `OPEN` rows flat on Hyperliquid (guarded by snapshot_ts_ms + in_flight_positions + P1-Acct cooldowns)
    if let Ok(local_held) = db.get_assets_held() {
        for h in local_held {
            if exchange_open_asset_ids.contains(&h.asset_id) {
                continue;
            }
            if in_flight_positions.contains(&h.position_id) {
                continue;
            }
            let origin_ts = h.opened_at_ms.unwrap_or(h.created_at_ms);
            if h.created_at_ms > snapshot_ts_ms || origin_ts > snapshot_ts_ms {
                continue;
            }
            let coin_upper = h.coin.to_ascii_uppercase();
            let (exit_px, close_fee) =
                resolve_flat_exit_fill_stats(&h, parsed_fills_by_coin.get(&coin_upper));
            match db.close_position(
                h.position_id,
                exit_px,
                close_fee,
                "EXCHANGE_RECONCILED_FLAT",
            ) {
                Ok(realized_pnl) => {
                    if realized_pnl < 0.0 {
                        let cd_sec = limits.stop_out_cooldown_sec.max(60);
                        let _ = db.insert_asset_cooldown(
                            h.asset_id,
                            "STOP_OUT",
                            &format!(
                                "Exchange-reconciled flat loss on {} (${realized_pnl:.2})",
                                h.symbol
                            ),
                            cd_sec,
                        );
                    } else if realized_pnl > 0.0 {
                        let _ = db.insert_asset_cooldown(
                            h.asset_id,
                            "CHURN_GUARD",
                            &format!(
                                "Exchange-reconciled flat profit on {} (+${realized_pnl:.2})",
                                h.symbol
                            ),
                            300,
                        );
                    }
                    local_orphans_closed += 1;
                    details.push(format!(
                        "Closed local position #{} on {} (flat on Hyperliquid, exit=${exit_px:.4}, rpnl=${realized_pnl:+.2})",
                        h.position_id, h.symbol
                    ));
                }
                Err(e) => {
                    tracing::warn!(
                        position_id = h.position_id,
                        symbol = %h.symbol,
                        error = %e,
                        "Failed closing local flat position during EXCHANGE_RECONCILED_FLAT"
                    );
                }
            }
        }
    }

    if ledger_sync_ok && run_ledger_replay {
        if !replay_out.closed_orders.is_empty() {
            let mut closed_inputs = Vec::with_capacity(replay_out.closed_orders.len());
            for (oid, agg) in &replay_out.closed_orders {
                if agg.total_sz <= 1e-9 {
                    continue;
                }
                let window_start = agg.open_time_ms.unwrap_or(agg.min_time_ms);
                let exit_px = agg.notional_sum / agg.total_sz;
                let side = agg.side.unwrap_or(OrderSide::Long);
                let entry_px = match side {
                    OrderSide::Long => (exit_px - (agg.gross_closed_pnl / agg.total_sz)).max(1e-8),
                    OrderSide::Short => (exit_px + (agg.gross_closed_pnl / agg.total_sz)).max(1e-8),
                };
                let total_fees = agg.close_fees_usdc + agg.open_fees_usdc;
                let net_rpnl = round_dp(
                    agg.gross_closed_pnl - total_fees + agg.funding_accrued_usdc,
                    4,
                );
                let symbol = format!("{}-USDC", agg.coin);
                let Some(asset) = resolve_or_upsert_asset_locked(
                    db,
                    AssetSeedSpec {
                        coin: &agg.coin,
                        symbol: &symbol,
                        ref_px: exit_px,
                        is_core: false,
                        is_event: true,
                        is_prio: false,
                    },
                    meta_catalog,
                    assets_by_symbol,
                )?
                else {
                    continue;
                };
                coin_asset_id_cache.insert(agg.coin.to_ascii_uppercase(), asset.id);

                closed_inputs.push(ReconciledClosedTradeInput {
                    asset_id: asset.id,
                    client_order_id: format!("hl-closed-{}-{oid}", agg.coin.to_ascii_lowercase()),
                    exchange_order_id: format!("hl-{oid}"),
                    close_epoch_id: Some(agg.close_epoch_id.clone()),
                    side,
                    entry_price: entry_px,
                    exit_price: exit_px,
                    filled_quantity: agg.total_sz,
                    realized_pnl_usdc: net_rpnl,
                    fees_paid_usdc: round_dp(total_fees, 4),
                    funding_accrued_usdc: round_dp(agg.funding_accrued_usdc, 4),
                    created_at_ms: window_start,
                    closed_at_ms: agg.max_time_ms,
                    exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
                });
            }
            if !closed_inputs.is_empty() {
                if let Ok(reconciled_closed_count) =
                    db.reconcile_closed_positions_from_exchange_fills(&closed_inputs)
                {
                    details.push(format!(
                        "Reconciled {reconciled_closed_count} historical closed trades from Hyperliquid userFillsByTime"
                    ));
                }
            }
        }

        if !replay_out.funding_events_with_remaining.is_empty() {
            let mut funding_inputs =
                Vec::with_capacity(replay_out.funding_events_with_remaining.len());
            for (ev, remaining_usdc) in &replay_out.funding_events_with_remaining {
                let coin_upper = ev.coin.to_ascii_uppercase();
                let asset_id = if let Some(&aid) = coin_asset_id_cache.get(&coin_upper) {
                    aid
                } else {
                    let symbol = format!("{}-USDC", ev.coin);
                    let Some(asset) = resolve_or_upsert_asset_locked(
                        db,
                        AssetSeedSpec {
                            coin: &ev.coin,
                            symbol: &symbol,
                            ref_px: 1.0,
                            is_core: false,
                            is_event: true,
                            is_prio: false,
                        },
                        meta_catalog,
                        assets_by_symbol,
                    )?
                    else {
                        continue;
                    };
                    coin_asset_id_cache.insert(coin_upper, asset.id);
                    asset.id
                };
                funding_inputs.push(FundingEventUpsertInput {
                    asset_id,
                    coin: ev.coin.clone(),
                    event_time_ms: ev.time_ms,
                    usdc: ev.usdc,
                    remaining_usdc: *remaining_usdc,
                    position_szi: ev.szi,
                    funding_rate: ev.funding_rate,
                    dedup_key: ev.dedup_key.clone(),
                });
            }
            if !funding_inputs.is_empty() {
                if let Err(e) = db.upsert_funding_events_batch(&funding_inputs) {
                    tracing::warn!(
                        error = %e,
                        count = funding_inputs.len(),
                        "Failed persisting reconciled funding_events batch"
                    );
                }
            }
        }
    }

    let _ = legacy_positions_found;
    Ok((
        legacy_positions_closed,
        legacy_positions_adopted,
        local_orphans_closed,
        tpsl_syncs,
    ))
}

/// Phase 4 unlocked TP/SL sync across all adopted/open positions using `sync_tpsl_batch_for_positions`.
pub(crate) fn sync_tpsl_unlocked(
    http: &HyperliquidHttpClient,
    db: &DatabaseManager,
    targets: &[PendingTpslSync],
    open_orders_snap: Option<&OpenOrdersSnapshot>,
    cfg: &ConfigMap,
) {
    if targets.is_empty() {
        return;
    }
    let reqs: Vec<TpslSyncRequest<'_>> = targets
        .iter()
        .map(|t| TpslSyncRequest {
            position_id: t.position_id,
            asset: &t.asset,
            side: t.side,
            quantity: t.quantity,
            mark_price: t.mark_price,
            desired_stop_px: t.desired_stop_px,
            desired_tp_px: t.desired_tp_px,
            existing_sl_oid: t.existing_sl_oid.as_deref(),
            existing_tp_oid: t.existing_tp_oid.as_deref(),
            replace_sl: false,
            replace_tp: false,
        })
        .collect();
    let _ = sync_tpsl_batch_for_positions(http, db, &reqs, None, open_orders_snap, cfg);
}

pub(crate) fn collect_meta_coins_needed(
    prepared: &[PreparedExchangePosition],
    replay_out: &FifoFundingReplayOutput,
    assets_by_symbol: &HashMap<String, ExchangeAsset>,
) -> bool {
    for p in prepared {
        if !assets_by_symbol.contains_key(&p.symbol.to_ascii_uppercase()) {
            return true;
        }
    }
    for (_, agg) in &replay_out.closed_orders {
        let sym = format!("{}-USDC", agg.coin).to_ascii_uppercase();
        if !assets_by_symbol.contains_key(&sym) {
            return true;
        }
    }
    for (ev, _) in &replay_out.funding_events_with_remaining {
        let sym = format!("{}-USDC", ev.coin).to_ascii_uppercase();
        if !assets_by_symbol.contains_key(&sym) {
            return true;
        }
    }
    false
}
