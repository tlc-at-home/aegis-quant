//! Dynamic Cutover & Live 1:1 Hyperliquid Exchange Reconciliation (`aegis-execution::reconcile`).
//!
//! Queries live Hyperliquid Testnet/Mainnet `clearinghouseState` and `openOrders` using the
//! preserved `wallet_address` and decrypted `private_key` from MariaDB `system_config`.
//! Synchronizes MariaDB `positions` 1:1 with Hyperliquid's actual `assetPositions` (`szi`, `entryPx`,
//! `positionValue`, `unrealizedPnl`), cancels orphan/stale open orders on Hyperliquid, closes local
//! phantom/flat rows, and persists live `exchange_equity_usdc`.

use crate::http_client::{is_testnet_configured, HyperliquidHttpClient};
use crate::order_chaser::{extract_order_oid, parse_hl_oid, OrderChaser};
use crate::signing::{
    derive_hl_cloid_hex, format_hl_wire_price, format_hl_wire_size_floor, infer_tick_size,
    CancelWire, LimitOrderType, OrderTypeWire, OrderWire,
};
use aegis_alerts::AlertEngine;
use aegis_core::{
    mask_identifier, normalize_hyperliquid_symbol, now_ms, round_dp, ExchangeAsset, ExecutionMode,
    OrderSide, Result,
};
use aegis_db::{AssetUpsertInput, DatabaseManager, ReconciledClosedTradeInput};
use aegis_risk::{
    compute_atr_from_bars, compute_initial_protective_stop, compute_initial_take_profit,
    RiskLimitsConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

/// Structured report produced by [`CutoverReconciler::run_cutover_reconciliation`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CutoverReconcileReport {
    pub reconciled_at_ms: i64,
    pub execution_mode: String,
    pub is_testnet: bool,
    pub wallet_address: String,
    pub exchange_reachable: bool,
    pub exchange_equity_usdc: f64,
    pub stale_orders_found: usize,
    pub stale_orders_cancelled: usize,
    pub legacy_positions_found: usize,
    pub legacy_positions_closed: usize,
    pub legacy_positions_adopted: usize,
    pub local_orphans_closed: usize,
    pub details: Vec<String>,
}

#[derive(Clone)]
struct ParsedHlFill {
    coin: String,
    side_str: String,
    px: f64,
    sz: f64,
    fee: f64,
    closed_pnl: f64,
    dir: String,
    time_ms: i64,
    oid: u64,
    tid: u64,
    start_position: Option<f64>,
}

fn derive_open_position_opened_at_ms(fills: &[ParsedHlFill], current_szi: f64) -> Option<i64> {
    if fills.is_empty() || current_szi.abs() <= 1e-9 {
        return None;
    }
    let mut pos_after = current_szi;
    let mut oldest_open_ts: Option<i64> = None;
    for f in fills.iter().rev() {
        let fill_signed = if f.side_str == "B" { f.sz } else { -f.sz };
        if fill_signed * current_szi > 0.0 {
            oldest_open_ts = Some(f.time_ms);
        }
        let pos_before = pos_after - fill_signed;
        if pos_before.abs() <= 1e-7 || (pos_before * current_szi) <= 0.0 {
            break;
        }
        pos_after = pos_before;
    }
    oldest_open_ts
}

/// Dynamic Cutover & Live 1:1 Exchange State Reconciler.
#[derive(Clone)]
pub struct CutoverReconciler {
    pub db: DatabaseManager,
    pub http: HyperliquidHttpClient,
}

impl CutoverReconciler {
    pub fn new(db: DatabaseManager, http: HyperliquidHttpClient) -> Self {
        Self { db, http }
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_or_upsert_asset(
        &self,
        coin: &str,
        symbol: &str,
        ref_px: f64,
        is_core: bool,
        is_event: bool,
        is_prio: bool,
        meta_cache: &mut Option<HashMap<String, (u32, u32, f64)>>,
        cfg: &aegis_core::ConfigMap,
    ) -> Result<Option<ExchangeAsset>> {
        if let Some(existing) = self.db.get_asset_by_symbol(symbol)? {
            return Ok(Some(existing));
        }
        if meta_cache.is_none() {
            let mut map = HashMap::new();
            if let Ok(meta_ctxs) = self.http.fetch_meta_and_asset_ctxs(cfg) {
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
            *meta_cache = Some(map);
        }

        let Some(&(asset_index, sz_decimals, max_leverage)) = meta_cache
            .as_ref()
            .and_then(|m| m.get(&coin.to_ascii_uppercase()))
        else {
            tracing::warn!(
                coin = %coin,
                symbol = %symbol,
                "Could not resolve asset_index from metaAndAssetCtxs; skipping instead of defaulting to asset_index=0"
            );
            return Ok(None);
        };

        let lot_step_size = 10_f64.powi(-(sz_decimals as i32)).max(1e-6);
        let tick = infer_tick_size(ref_px, sz_decimals);
        let aid = self.db.upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: coin.to_string(),
            asset_index,
            sz_decimals,
            tick_size: tick,
            lot_step_size,
            max_leverage,
            mark_price: ref_px,
            mid_price: ref_px,
            oracle_price: ref_px,
            funding_rate_hourly: 0.0,
            day_volume_usdc: 100_000.0,
            open_interest_usdc: 50_000.0,
            prev_day_price: ref_px,
            is_core_eligible: is_core,
            is_event_eligible: is_event,
            is_priority_watched: is_prio,
        })?;
        self.db.get_asset_by_id(aid)
    }

    /// Performs an exact 1:1 synchronization of MariaDB `positions` and `portfolio_snapshots`
    /// against live Hyperliquid `clearinghouseState` and `openOrders`.
    pub fn sync_db_with_hyperliquid(&self) -> Result<CutoverReconcileReport> {
        self.run_cutover_reconciliation(false)
    }

    /// Queries live Hyperliquid `clearinghouseState` and `openOrders`, cancels stale/untracked open orders,
    /// and either closes (`close_legacy_positions == true`) or 1:1 synchronizes all open Hyperliquid positions
    /// into MariaDB while closing any local rows that are flat on Hyperliquid.
    pub fn run_cutover_reconciliation(
        &self,
        close_legacy_positions: bool,
    ) -> Result<CutoverReconcileReport> {
        let _exec_guard = crate::engine::execution_lock().lock();
        let ts = now_ms();
        let cfg = self.db.load_config_or_default();
        let mode = cfg.execution_mode();
        let is_testnet = is_testnet_configured(&cfg);
        let wallet = cfg.get_str("wallet_address", "");
        let private_key = cfg.get_str("private_key", "");
        let fallback_eq = cfg.get_f64("initial_paper_equity_usdc", 10_000.0);

        let mut report = CutoverReconcileReport {
            reconciled_at_ms: ts,
            execution_mode: mode.as_str().to_string(),
            is_testnet,
            wallet_address: mask_identifier(&wallet),
            exchange_reachable: false,
            exchange_equity_usdc: fallback_eq,
            ..Default::default()
        };

        if mode == ExecutionMode::Paper || wallet.trim().is_empty() {
            report.details.push(format!(
                "Cutover reconciliation ran in {} mode (wallet_configured={})",
                mode.as_str(),
                !wallet.trim().is_empty()
            ));
            let _ = self.db.log_telemetry(
                "INFO",
                "execution.reconcile",
                "Cutover reconciliation completed in PAPER / local mode",
                Some(serde_json::to_value(&report).unwrap_or_default()),
            );
            return Ok(report);
        }

        let ch_state = match self.http.fetch_clearinghouse_state(&wallet, &cfg) {
            Ok(val) => {
                report.exchange_reachable = true;
                val
            }
            Err(e) => {
                report
                    .details
                    .push(format!("Clearinghouse state unreachable: {e}"));
                let _ = self.db.log_telemetry(
                    "WARN",
                    "execution.reconcile",
                    &format!("Hyperliquid clearinghouseState unreachable during cutover: {e}"),
                    Some(serde_json::to_value(&report).unwrap_or_default()),
                );
                return Ok(report);
            }
        };

        let parse_f_opt = |v: Option<&Value>| -> Option<f64> {
            v.and_then(|x| {
                x.as_f64()
                    .or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .filter(|val| val.is_finite())
        };
        let parse_f = |v: Option<&Value>| -> f64 { parse_f_opt(v).unwrap_or(0.0) };

        // 1. Resolve live on-chain account equity strictly from Perp `marginSummary.accountValue`
        let perp_eq = {
            let m_eq = parse_f(ch_state.pointer("/marginSummary/accountValue"));
            let cm_eq = parse_f(ch_state.pointer("/crossMarginSummary/accountValue"));
            let w_eq = parse_f(ch_state.get("withdrawable"));
            if m_eq > 0.0 {
                m_eq
            } else if cm_eq > 0.0 {
                cm_eq
            } else {
                w_eq
            }
        };
        let configured_live_eq = cfg.get_f64("live_exchange_equity_usdc", 0.0);
        let cap_unset = cfg
            .get("risk_capital_cap_usdc")
            .map(|s| s.trim().is_empty())
            .unwrap_or(true);
        if cap_unset && configured_live_eq > 10.0 {
            let _ = self.db.set_config_value(
                "risk_capital_cap_usdc",
                &format!("{configured_live_eq:.2}"),
                Some("Preserved initial capital sizing cap from legacy live_exchange_equity_usdc"),
            );
        }

        report.exchange_equity_usdc = if perp_eq >= 1.0 {
            round_dp(perp_eq, 2)
        } else {
            fallback_eq
        };
        if perp_eq >= 1.0 {
            let _ = self.db.set_config_value(
                "live_exchange_equity_usdc",
                &format!("{:.2}", report.exchange_equity_usdc),
                Some("Latest reconciled Hyperliquid perp marginSummary.accountValue in USDC"),
            );
        }

        // Identify currently open coins on Hyperliquid before clearing orphan openOrders
        let mut open_coins: HashSet<String> = HashSet::new();
        if let Some(asset_positions) = ch_state.get("assetPositions").and_then(Value::as_array) {
            for item in asset_positions {
                let pos = item.get("position").unwrap_or(item);
                let raw_coin = pos.get("coin").and_then(Value::as_str).unwrap_or("").trim();
                let szi = parse_f(pos.get("szi"));
                if !raw_coin.is_empty() && szi.abs() > 1e-9 {
                    if let Ok((_, coin)) = normalize_hyperliquid_symbol(raw_coin) {
                        open_coins.insert(coin.to_ascii_uppercase());
                    }
                }
            }
        }

        // 2. Inspect resting orders via frontendOpenOrders (includes SL/TP trigger orders) and preserve active tracked orders
        let (open_orders, open_orders_fetch_ok) = match self
            .http
            .fetch_frontend_open_orders(&wallet, &cfg)
            .or_else(|_| self.http.fetch_open_orders(&wallet, &cfg))
        {
            Ok(v) => (v, true),
            Err(_) => (Vec::new(), false),
        };

        let mut resting_exchange_oids: HashSet<u64> = HashSet::new();
        let mut cloid_to_oid: HashMap<String, u64> = HashMap::new();
        for ord in &open_orders {
            if let Some(oid) = extract_order_oid(ord) {
                resting_exchange_oids.insert(oid);
                if let Some(cloid) = ord.get("cloid").and_then(Value::as_str) {
                    cloid_to_oid.insert(cloid.trim().to_ascii_lowercase(), oid);
                }
            }
        }

        let mut protected_oids: HashSet<u64> = HashSet::new();
        if !close_legacy_positions {
            if let Ok(local_held) = self.db.get_assets_held() {
                for h in local_held {
                    if !open_coins.contains(&h.coin.to_ascii_uppercase()) {
                        continue;
                    }
                    for raw_oid in [
                        h.protective_stop_oid.as_deref(),
                        h.take_profit_oid.as_deref(),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        if let Some(oid) = parse_hl_oid(Some(raw_oid)).or_else(|| {
                            cloid_to_oid
                                .get(&raw_oid.trim().to_ascii_lowercase())
                                .copied()
                        }) {
                            protected_oids.insert(oid);
                        }
                    }
                }
            }
            if let Ok(local_ordered) = self.db.get_assets_ordered() {
                for o in local_ordered {
                    if ts.saturating_sub(o.created_at_ms) < 45_000
                        && !open_coins.contains(&o.coin.to_ascii_uppercase())
                    {
                        if let Some(oid) = parse_hl_oid(o.exchange_order_id.as_deref()) {
                            protected_oids.insert(oid);
                        }
                    }
                }
            }
        }

        let mut batch_cancels: Vec<CancelWire> = Vec::new();
        let mut cancelled_oid_candidates: Vec<u64> = Vec::new();
        for ord in &open_orders {
            let coin = ord.get("coin").and_then(Value::as_str).unwrap_or("").trim();
            let Some(oid) = extract_order_oid(ord) else {
                continue;
            };
            if coin.is_empty() {
                continue;
            }
            if !close_legacy_positions && protected_oids.contains(&oid) {
                continue;
            }
            report.stale_orders_found += 1;
            if let Ok(Some(asset)) = self.db.get_asset_by_symbol(coin) {
                batch_cancels.push(CancelWire {
                    a: asset.asset_index,
                    o: oid,
                });
                cancelled_oid_candidates.push(oid);
            }
        }

        let mut cancelled_oid_set: HashSet<u64> = HashSet::new();
        if !private_key.is_empty() && !batch_cancels.is_empty() {
            for (chunk, oid_chunk) in batch_cancels
                .chunks(25)
                .zip(cancelled_oid_candidates.chunks(25))
            {
                if self
                    .http
                    .cancel_orders_batch(chunk.to_vec(), &private_key, Some(&self.db), &cfg)
                    .is_ok()
                {
                    report.stale_orders_cancelled += chunk.len();
                    for oid in oid_chunk {
                        resting_exchange_oids.remove(oid);
                        cancelled_oid_set.insert(*oid);
                    }
                }
            }
            report.details.push(format!(
                "Cancelled {}/{} resting orphan orders on Hyperliquid",
                report.stale_orders_cancelled, report.stale_orders_found
            ));
        }
        let remaining_open_orders: Vec<Value> = open_orders
            .into_iter()
            .filter(|ord| {
                extract_order_oid(ord)
                    .map(|id| !cancelled_oid_set.contains(&id))
                    .unwrap_or(true)
            })
            .collect();

        // Cancel local WORKING rows only if close_legacy_positions, or (not yet open on exchange AND (expired or no longer resting))
        // When `is_open_on_exch` is true, let `sync_open_position_from_exchange_v2` promote it in-place preserving sleeve weights.
        if let Ok(local_ordered) = self.db.get_assets_ordered() {
            for o in local_ordered {
                let is_open_on_exch = open_coins.contains(&o.coin.to_ascii_uppercase());
                let is_expired = ts.saturating_sub(o.created_at_ms) >= 45_000;
                let is_resting = parse_hl_oid(o.exchange_order_id.as_deref())
                    .is_some_and(|oid| resting_exchange_oids.contains(&oid));
                if close_legacy_positions || (!is_open_on_exch && (is_expired || !is_resting)) {
                    let _ = self
                        .db
                        .cancel_working_order(o.position_id, "EXCHANGE_RECONCILED_WORKING_CLEARED");
                }
            }
        }

        // Fetch userFills and userFunding once so both open position timestamps and closed trade FIFO/funding match accurately
        let user_fills_opt = self.http.fetch_user_fills(&wallet, &cfg).ok();
        let mut parsed_fills_by_coin: BTreeMap<String, Vec<ParsedHlFill>> = BTreeMap::new();
        if let Some(ref raw_fills) = user_fills_opt {
            for fill in raw_fills {
                let raw_coin = fill
                    .get("coin")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim();
                if raw_coin.is_empty() {
                    continue;
                }
                let Ok((_, coin)) = normalize_hyperliquid_symbol(raw_coin) else {
                    continue;
                };
                let px = parse_f(fill.get("px"));
                let sz = parse_f(fill.get("sz"));
                if px <= 0.0 || sz <= 0.0 {
                    continue;
                }
                let fill_time = fill
                    .get("time")
                    .and_then(Value::as_i64)
                    .filter(|&t| t > 0)
                    .unwrap_or(ts);
                let tid = fill.get("tid").and_then(Value::as_u64).unwrap_or(0);
                let oid = fill
                    .get("oid")
                    .and_then(Value::as_u64)
                    .or_else(|| {
                        fill.get("oid")
                            .and_then(Value::as_str)
                            .and_then(|s| s.parse::<u64>().ok())
                    })
                    .filter(|&id| id > 0)
                    .or((tid > 0).then_some(tid))
                    .unwrap_or(fill_time as u64);
                parsed_fills_by_coin
                    .entry(coin.to_ascii_uppercase())
                    .or_default()
                    .push(ParsedHlFill {
                        coin,
                        side_str: fill
                            .get("side")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .trim()
                            .to_string(),
                        px,
                        sz,
                        fee: parse_f(fill.get("fee")),
                        closed_pnl: parse_f(fill.get("closedPnl")),
                        dir: fill
                            .get("dir")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .trim()
                            .to_string(),
                        time_ms: fill_time,
                        oid,
                        tid,
                        start_position: parse_f_opt(fill.get("startPosition")),
                    });
            }
            for fills in parsed_fills_by_coin.values_mut() {
                fills.sort_by_key(|f| (f.time_ms, f.tid));
            }
        }

        let earliest_fill_ms = parsed_fills_by_coin
            .values()
            .filter_map(|v| v.first().map(|f| f.time_ms))
            .min();
        let user_funding_raw = self
            .http
            .fetch_user_funding(&wallet, earliest_fill_ms, &cfg)
            .unwrap_or_default();
        let mut funding_by_coin: HashMap<String, Vec<(i64, f64, bool)>> = HashMap::new();
        for ev in &user_funding_raw {
            let f_time = ev.get("time").and_then(Value::as_i64).unwrap_or(0);
            let Some(delta) = ev.get("delta") else {
                continue;
            };
            let d_type = delta.get("type").and_then(Value::as_str).unwrap_or("");
            if !d_type.eq_ignore_ascii_case("funding") {
                continue;
            }
            let raw_coin = delta
                .get("coin")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            if raw_coin.is_empty() {
                continue;
            }
            let Ok((_, norm_coin)) = normalize_hyperliquid_symbol(raw_coin) else {
                continue;
            };
            let usdc = parse_f(delta.get("usdc"));
            if f_time > 0 && usdc.abs() > 1e-12 {
                funding_by_coin
                    .entry(norm_coin.to_ascii_uppercase())
                    .or_default()
                    .push((f_time, usdc, false));
            }
        }
        for list in funding_by_coin.values_mut() {
            list.sort_by_key(|(t, _, _)| *t);
        }

        let mut meta_cache: Option<HashMap<String, (u32, u32, f64)>> = None;

        // 3. Inspect live Hyperliquid `assetPositions` and synchronize 1:1 with MariaDB `positions` + SL/TP orders
        let mut exchange_open_asset_ids: HashSet<i64> = HashSet::new();
        let limits = RiskLimitsConfig::from_config_map(&cfg);
        let taker_fee_bps = limits.taker_fee_bps;
        let chaser = OrderChaser::new(
            self.db.clone(),
            self.http.clone(),
            AlertEngine::new(self.db.clone()),
        );

        if let Some(asset_positions) = ch_state.get("assetPositions").and_then(Value::as_array) {
            for item in asset_positions {
                let pos = item.get("position").unwrap_or(item);
                let raw_coin = pos.get("coin").and_then(Value::as_str).unwrap_or("").trim();
                let szi = parse_f(pos.get("szi"));
                if raw_coin.is_empty() || szi.abs() <= 1e-9 {
                    continue;
                }
                let entry_px = parse_f(pos.get("entryPx")).max(0.0001);
                let pos_val = parse_f(pos.get("positionValue"));
                let upnl = parse_f(pos.get("unrealizedPnl"));
                let since_open = parse_f(pos.pointer("/cumFunding/sinceOpen"));
                let funding_accrued_usdc = round_dp(-since_open, 4);

                let Ok((symbol, coin)) = normalize_hyperliquid_symbol(raw_coin) else {
                    continue;
                };

                let Some(asset) = self.resolve_or_upsert_asset(
                    &coin,
                    &symbol,
                    entry_px,
                    true,
                    true,
                    true,
                    &mut meta_cache,
                    &cfg,
                )?
                else {
                    continue;
                };

                report.legacy_positions_found += 1;
                let side = if szi > 0.0 {
                    OrderSide::Long
                } else {
                    OrderSide::Short
                };
                let qty = szi.abs();
                let mark_px = if pos_val > 0.0 && qty > 1e-9 {
                    round_dp(pos_val / qty, 6)
                } else if asset.mark_price > 0.0 {
                    asset.mark_price
                } else {
                    entry_px
                };
                let live_mid = if asset.mid_price > 0.0 {
                    asset.mid_price
                } else {
                    mark_px
                };
                let _ = self.db.update_asset_live_quote(asset.id, mark_px, live_mid);

                let mut closed_on_exchange = false;
                let mut remaining_qty = qty;
                if close_legacy_positions && !private_key.is_empty() {
                    let close_is_buy = !side.is_buy();
                    let l2_bbo = self.http.fetch_l2_book(&coin, &cfg).ok().flatten();
                    let bbo_anchor = l2_bbo
                        .map(|(bid, ask)| if close_is_buy { ask } else { bid })
                        .filter(|&px| px > 0.0)
                        .unwrap_or(mark_px);
                    let band_lo = (mark_px * (1.0 - 0.038)).max(asset.tick_size.max(1e-8));
                    let band_hi = mark_px * (1.0 + 0.038);

                    for (attempt_idx, (base_px, slip_pct)) in [
                        (bbo_anchor, 0.0),
                        (bbo_anchor, 0.01),
                        (bbo_anchor, 0.025),
                        (mark_px, 0.038),
                    ]
                    .into_iter()
                    .enumerate()
                    {
                        if remaining_qty <= asset.lot_step_size * 0.5 || remaining_qty <= 1e-9 {
                            break;
                        }
                        let raw_limit_px = if close_is_buy {
                            base_px * (1.0 + slip_pct)
                        } else {
                            base_px * (1.0 - slip_pct)
                        };
                        let limit_px = raw_limit_px.clamp(band_lo, band_hi);
                        let sz_str = format_hl_wire_size_floor(
                            remaining_qty,
                            asset.lot_step_size,
                            asset.sz_decimals,
                        );
                        if sz_str == "0" {
                            break;
                        }
                        let wire = OrderWire {
                            a: asset.asset_index,
                            b: close_is_buy,
                            p: format_hl_wire_price(limit_px, asset.tick_size, asset.sz_decimals),
                            s: sz_str,
                            r: true,
                            t: OrderTypeWire {
                                limit: Some(LimitOrderType {
                                    tif: "Ioc".to_string(),
                                }),
                                trigger: None,
                            },
                            c: Some(derive_hl_cloid_hex(&format!(
                                "cutover-close-{}-{}-{attempt_idx}-{}",
                                coin.to_ascii_lowercase(),
                                slip_pct,
                                ts
                            ))),
                        };
                        if let Ok(ack) = self.http.submit_order_wire(
                            wire,
                            remaining_qty,
                            base_px,
                            taker_fee_bps,
                            &private_key,
                            Some(&self.db),
                            &cfg,
                        ) {
                            if ack.accepted && ack.status == "FILLED" && ack.filled_quantity > 0.0 {
                                remaining_qty = (remaining_qty - ack.filled_quantity).max(0.0);
                                if remaining_qty <= asset.lot_step_size * 0.5 {
                                    closed_on_exchange = true;
                                    report.legacy_positions_closed += 1;
                                    let _ = self.db.insert_asset_cooldown(
                                        asset.id,
                                        "MANUAL",
                                        "Cutover position flatten cooldown",
                                        900,
                                    );
                                    report.details.push(format!(
                                        "Closed position {} ({} {:.4} @ ${:.4})",
                                        asset.symbol,
                                        side.as_str(),
                                        qty,
                                        ack.avg_fill_price
                                    ));
                                    break;
                                }
                            }
                        }
                    }
                }

                let qty = remaining_qty.max(1e-9);

                if !closed_on_exchange {
                    let tick = asset.tick_size.max(1e-8);
                    let bars_1h = self
                        .db
                        .get_ohlcv_bars(asset.id, "1h", 20)
                        .unwrap_or_default();
                    let atr_opt = (!bars_1h.is_empty()).then(|| {
                        compute_atr_from_bars(
                            &bars_1h,
                            14,
                            mark_px,
                            limits.protective_stop_default_pct,
                        )
                    });
                    let entry_stop =
                        compute_initial_protective_stop(side, entry_px, atr_opt, tick, &limits);
                    let stop_px = if match side {
                        OrderSide::Long => entry_stop < mark_px - tick * 0.5,
                        OrderSide::Short => entry_stop > mark_px + tick * 0.5,
                    } {
                        entry_stop
                    } else {
                        compute_initial_protective_stop(side, mark_px, atr_opt, tick, &limits)
                    };
                    let entry_tp =
                        compute_initial_take_profit(side, entry_px, atr_opt, tick, &limits);
                    let tp_px = if match side {
                        OrderSide::Long => entry_tp > mark_px + tick * 0.5,
                        OrderSide::Short => entry_tp < mark_px - tick * 0.5,
                    } {
                        entry_tp
                    } else {
                        compute_initial_take_profit(side, mark_px, atr_opt, tick, &limits)
                    };

                    let committed_usdc = if pos_val > 0.0 {
                        round_dp(pos_val, 4)
                    } else {
                        round_dp(mark_px * qty, 4)
                    };
                    let weight = round_dp(
                        (committed_usdc / report.exchange_equity_usdc.max(1.0)) * side.sign(),
                        4,
                    );
                    let opened_at_ms_opt = parsed_fills_by_coin
                        .get(&coin.to_ascii_uppercase())
                        .and_then(|fills| derive_open_position_opened_at_ms(fills, szi));

                    let (pid, _newly_adopted) = self.db.sync_open_position_from_exchange_v2(
                        asset.id,
                        &coin,
                        side,
                        entry_px,
                        mark_px,
                        qty,
                        committed_usdc,
                        upnl,
                        weight,
                        stop_px,
                        tp_px,
                        funding_accrued_usdc,
                        opened_at_ms_opt,
                    )?;
                    if let Ok(Some(synced_pos)) = self.db.get_position_by_id(pid) {
                        let _ = chaser.ensure_exchange_tpsl_orders_with_orders(
                            pid,
                            &asset,
                            side,
                            qty,
                            mark_px,
                            synced_pos.protective_stop_px,
                            synced_pos.take_profit_px,
                            synced_pos.protective_stop_oid.as_deref(),
                            synced_pos.take_profit_oid.as_deref(),
                            false,
                            false,
                            open_orders_fetch_ok.then_some(remaining_open_orders.as_slice()),
                            &cfg,
                        );
                    }
                    let _ = self.db.set_asset_priority_watch(asset.id, true);
                    exchange_open_asset_ids.insert(asset.id);
                    report.legacy_positions_adopted += 1;
                    report.details.push(format!(
                        "Synced 1:1 exchange position {} ({} {:.4} @ entry=${:.4}, mark=${:.4}, notional=${:.2}, uPnL=${:+.2})",
                        asset.symbol,
                        side.as_str(),
                        qty,
                        entry_px,
                        mark_px,
                        committed_usdc,
                        upnl
                    ));
                }
            }
        }

        // 4. Close any local `OPEN` rows in MariaDB that are flat on Hyperliquid
        if let Ok(local_held) = self.db.get_assets_held() {
            for h in local_held {
                if !exchange_open_asset_ids.contains(&h.asset_id) {
                    let _ = self.db.close_position(
                        h.position_id,
                        h.mark_price,
                        0.0,
                        "EXCHANGE_RECONCILED_FLAT",
                    );
                    report.local_orphans_closed += 1;
                    report.details.push(format!(
                        "Closed local position #{} on {} (flat on Hyperliquid)",
                        h.position_id, h.symbol
                    ));
                }
            }
        }

        // 5. Reconcile historical `CLOSED` trades 1:1 from Hyperliquid `userFills` + `userFunding` (FIFO open fees + flip tracking)
        if !parsed_fills_by_coin.is_empty() {
            struct OpenLot {
                rem_sz: f64,
                orig_sz: f64,
                fee: f64,
                time_ms: i64,
            }
            #[derive(Default)]
            struct AggClosingOrder {
                coin: String,
                side: Option<OrderSide>,
                total_sz: f64,
                notional_sum: f64,
                gross_closed_pnl: f64,
                close_fees_usdc: f64,
                open_fees_usdc: f64,
                open_time_ms: Option<i64>,
                min_time_ms: i64,
                max_time_ms: i64,
                funding_accrued_usdc: f64,
            }

            let mut agg_by_key: BTreeMap<(String, u64), AggClosingOrder> = BTreeMap::new();

            for fills in parsed_fills_by_coin.values() {
                let mut open_lots: VecDeque<OpenLot> = VecDeque::new();
                let mut prev_szi = 0.0_f64;
                let mut initialized_start_pos = false;

                for fill in fills {
                    if !initialized_start_pos {
                        if let Some(sp) = fill.start_position {
                            prev_szi = sp;
                        }
                        initialized_start_pos = true;
                    }
                    let fill_signed_sz = if fill.side_str == "B" {
                        fill.sz
                    } else {
                        -fill.sz
                    };
                    let is_closing_dir = fill.closed_pnl.abs() > 1e-9
                        || fill.dir.starts_with("Close")
                        || fill.dir.contains('>');

                    if prev_szi.abs() <= 1e-9 {
                        prev_szi = 0.0;
                    }
                    if (prev_szi == 0.0 && !is_closing_dir)
                        || (prev_szi != 0.0 && prev_szi.signum() == fill_signed_sz.signum())
                    {
                        open_lots.push_back(OpenLot {
                            rem_sz: fill.sz,
                            orig_sz: fill.sz,
                            fee: fill.fee,
                            time_ms: fill.time_ms,
                        });
                        prev_szi += fill_signed_sz;
                        continue;
                    }

                    // Closing or flip fill (`dir.contains('>')`): attribute only closed_sz to the closed side
                    let (closed_sz, closed_pos_side) = if prev_szi.abs() > 1e-9 {
                        (
                            prev_szi.abs().min(fill.sz),
                            if prev_szi > 0.0 {
                                OrderSide::Long
                            } else {
                                OrderSide::Short
                            },
                        )
                    } else {
                        let fallback_side = if fill.dir.starts_with("Close Long")
                            || fill.dir.starts_with("Long >")
                            || (!fill.dir.contains("Short") && fill.side_str == "A")
                        {
                            OrderSide::Long
                        } else {
                            OrderSide::Short
                        };
                        (fill.sz, fallback_side)
                    };
                    let flipped_open_sz = (fill.sz - closed_sz).max(0.0);
                    let close_fee = if fill.sz > 0.0 {
                        fill.fee * (closed_sz / fill.sz)
                    } else {
                        0.0
                    };
                    let open_fee = (fill.fee - close_fee).max(0.0);

                    let mut matched_open_fee = 0.0_f64;
                    let mut earliest_open_time_ms: Option<i64> = None;
                    let mut rem_to_close = closed_sz;
                    while rem_to_close > 1e-9 {
                        let Some(front) = open_lots.front_mut() else {
                            break;
                        };
                        let take = rem_to_close.min(front.rem_sz);
                        if front.orig_sz > 0.0 {
                            matched_open_fee += front.fee * (take / front.orig_sz);
                        }
                        earliest_open_time_ms = Some(
                            earliest_open_time_ms.map_or(front.time_ms, |t| t.min(front.time_ms)),
                        );
                        front.rem_sz -= take;
                        rem_to_close -= take;
                        if front.rem_sz <= 1e-9 {
                            open_lots.pop_front();
                        }
                    }

                    if closed_sz > 1e-9 && is_closing_dir {
                        let entry = agg_by_key
                            .entry((fill.coin.clone(), fill.oid))
                            .or_insert_with(|| AggClosingOrder {
                                coin: fill.coin.clone(),
                                side: Some(closed_pos_side),
                                min_time_ms: fill.time_ms,
                                max_time_ms: fill.time_ms,
                                ..Default::default()
                            });
                        entry.total_sz += closed_sz;
                        entry.notional_sum += fill.px * closed_sz;
                        entry.gross_closed_pnl += fill.closed_pnl;
                        entry.close_fees_usdc += close_fee;
                        entry.open_fees_usdc += matched_open_fee;
                        if let Some(ot) = earliest_open_time_ms {
                            entry.open_time_ms =
                                Some(entry.open_time_ms.map_or(ot, |cur| cur.min(ot)));
                        }
                        entry.min_time_ms = entry.min_time_ms.min(fill.time_ms);
                        entry.max_time_ms = entry.max_time_ms.max(fill.time_ms);
                    }

                    if flipped_open_sz > 1e-9 {
                        open_lots.push_back(OpenLot {
                            rem_sz: flipped_open_sz,
                            orig_sz: flipped_open_sz,
                            fee: open_fee,
                            time_ms: fill.time_ms,
                        });
                    }

                    if prev_szi.abs() > 1e-9 || fill.start_position.is_some() {
                        prev_szi += fill_signed_sz;
                    } else if flipped_open_sz > 1e-9 {
                        prev_szi = if fill_signed_sz > 0.0 {
                            flipped_open_sz
                        } else {
                            -flipped_open_sz
                        };
                    } else {
                        prev_szi = 0.0;
                    }
                }
            }

            // Order closed trades chronologically so funding events match cleanly once per trade window
            let mut closed_list: Vec<(u64, AggClosingOrder)> = agg_by_key
                .into_iter()
                .map(|((_, oid), agg)| (oid, agg))
                .collect();
            closed_list.sort_by_key(|(_, agg)| agg.max_time_ms);

            let mut closed_inputs = Vec::with_capacity(closed_list.len());
            for (oid, mut agg) in closed_list {
                if agg.total_sz <= 1e-9 {
                    continue;
                }
                let window_start = agg.open_time_ms.unwrap_or(agg.min_time_ms);
                if let Some(f_list) = funding_by_coin.get_mut(&agg.coin.to_ascii_uppercase()) {
                    for (f_time, usdc, used) in f_list.iter_mut() {
                        if !*used && *f_time >= window_start && *f_time <= agg.max_time_ms {
                            agg.funding_accrued_usdc += *usdc;
                            *used = true;
                        }
                    }
                }

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
                let Some(asset) = self.resolve_or_upsert_asset(
                    &agg.coin,
                    &symbol,
                    exit_px,
                    false,
                    true,
                    false,
                    &mut meta_cache,
                    &cfg,
                )?
                else {
                    continue;
                };

                closed_inputs.push(ReconciledClosedTradeInput {
                    asset_id: asset.id,
                    client_order_id: format!("hl-closed-{}-{oid}", agg.coin.to_ascii_lowercase()),
                    exchange_order_id: format!("hl-{oid}"),
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

            if let Ok(reconciled_closed_count) = self
                .db
                .reconcile_closed_positions_from_exchange_fills(&closed_inputs)
            {
                report.details.push(format!(
                    "Reconciled {reconciled_closed_count} historical closed trades from Hyperliquid userFills"
                ));
            }
        }

        let _ = self.db.log_telemetry(
            "INFO",
            "execution.reconcile",
            &format!(
                "Hyperliquid 1:1 reconciliation complete: mode={}, equity=${:.2}, orders_cancelled={}/{}, open_positions_synced={}, local_flat_closed={}",
                report.execution_mode,
                report.exchange_equity_usdc,
                report.stale_orders_cancelled,
                report.stale_orders_found,
                report.legacy_positions_adopted,
                report.local_orphans_closed
            ),
            Some(json!(report)),
        );

        Ok(report)
    }
}
