//! Dynamic Cutover & Live 1:1 Hyperliquid Exchange Reconciliation (`aegis-execution::reconcile`).
//!
//! Queries live Hyperliquid Testnet/Mainnet `clearinghouseState` and `openOrders` using the
//! preserved `wallet_address` and decrypted `private_key` from MariaDB `system_config`.
//! Synchronizes MariaDB `positions` 1:1 with Hyperliquid's actual `assetPositions` (`szi`, `entryPx`,
//! `positionValue`, `unrealizedPnl`), cancels orphan/stale open orders on Hyperliquid, closes local
//! phantom/flat rows, and persists live `exchange_equity_usdc`.

use crate::http_client::{is_testnet_configured, HyperliquidHttpClient};
use crate::signing::{
    derive_hl_cloid_hex, format_hl_wire_price, format_hl_wire_size, infer_tick_size, snap_to_tick,
    CancelWire, LimitOrderType, OrderTypeWire, OrderWire,
};
use aegis_core::{
    mask_identifier, normalize_hyperliquid_symbol, now_ms, round_dp, ExecutionMode, OrderSide,
    Result,
};
use aegis_db::{AssetUpsertInput, DatabaseManager, ReconciledClosedTradeInput};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};

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

        let parse_f = |v: Option<&Value>| -> f64 {
            v.and_then(|x| {
                x.as_f64()
                    .or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .filter(|val| val.is_finite())
            .unwrap_or(0.0)
        };

        // 1. Resolve live on-chain account equity from perp + spot clearinghouse state
        let perp_eq = parse_f(ch_state.pointer("/marginSummary/accountValue"))
            .max(parse_f(
                ch_state.pointer("/crossMarginSummary/accountValue"),
            ))
            .max(parse_f(ch_state.get("withdrawable")));
        let mut live_eq = perp_eq;
        if let Ok(spot_state) = self.http.fetch_spot_clearinghouse_state(&wallet, &cfg) {
            if let Some(balances) = spot_state.get("balances").and_then(Value::as_array) {
                for b in balances {
                    let coin = b.get("coin").and_then(Value::as_str).unwrap_or("");
                    if coin.eq_ignore_ascii_case("USDC") {
                        let total = parse_f(b.get("total"));
                        let hold = parse_f(b.get("hold"));
                        let unheld_spot_usdc = (total - hold).max(0.0);
                        live_eq += unheld_spot_usdc;
                    }
                }
            }
        }
        report.exchange_equity_usdc = if live_eq >= 1.0 {
            round_dp(live_eq, 2)
        } else {
            fallback_eq
        };
        let _ = self.db.set_config_value(
            "live_exchange_equity_usdc",
            &format!("{:.2}", report.exchange_equity_usdc),
            Some("Latest reconciled Hyperliquid accountValue in USDC"),
        );

        // 2. Cancel all stale/orphan open orders on Hyperliquid
        if let Ok(open_orders) = self.http.fetch_open_orders(&wallet, &cfg) {
            let mut batch_cancels: Vec<CancelWire> = Vec::new();
            for ord in &open_orders {
                let coin = ord.get("coin").and_then(Value::as_str).unwrap_or("").trim();
                let oid = ord
                    .get("oid")
                    .and_then(Value::as_u64)
                    .or_else(|| {
                        ord.get("oid")
                            .and_then(Value::as_str)
                            .and_then(|s| s.parse::<u64>().ok())
                    })
                    .unwrap_or(0);
                if oid == 0 || coin.is_empty() {
                    continue;
                }
                report.stale_orders_found += 1;
                if let Ok(Some(asset)) = self.db.get_asset_by_symbol(coin) {
                    batch_cancels.push(CancelWire {
                        a: asset.asset_index,
                        o: oid,
                    });
                }
            }

            if !private_key.is_empty() && !batch_cancels.is_empty() {
                for chunk in batch_cancels.chunks(25) {
                    if self
                        .http
                        .cancel_orders_batch(chunk.to_vec(), &private_key, Some(&self.db), &cfg)
                        .is_ok()
                    {
                        report.stale_orders_cancelled += chunk.len();
                    }
                }
                report.details.push(format!(
                    "Cancelled {}/{} resting orders on Hyperliquid",
                    report.stale_orders_cancelled, report.stale_orders_found
                ));
            }
        }

        // Cancel any local WORKING rows since all resting entry orders on Hyperliquid were cleared
        if let Ok(local_ordered) = self.db.get_assets_ordered() {
            for o in local_ordered {
                let _ = self
                    .db
                    .cancel_working_order(o.position_id, "EXCHANGE_RECONCILED_WORKING_CLEARED");
            }
        }

        // 3. Inspect live Hyperliquid `assetPositions` and synchronize 1:1 with MariaDB `positions`
        let mut exchange_open_asset_ids: HashSet<i64> = HashSet::new();
        let default_stop_pct = cfg
            .get_f64("protective_stop_default_pct", 0.04)
            .clamp(0.005, 0.25);
        let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);

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

                let Ok((symbol, coin)) = normalize_hyperliquid_symbol(raw_coin) else {
                    continue;
                };

                let asset = match self.db.get_asset_by_symbol(&symbol)? {
                    Some(a) => a,
                    None => {
                        let tick = infer_tick_size(entry_px, 2);
                        let aid = self.db.upsert_exchange_asset(&AssetUpsertInput {
                            raw_symbol_or_coin: coin.clone(),
                            asset_index: 0,
                            sz_decimals: 2,
                            tick_size: tick,
                            lot_step_size: 0.01,
                            max_leverage: 20.0,
                            mark_price: entry_px,
                            mid_price: entry_px,
                            oracle_price: entry_px,
                            funding_rate_hourly: 0.0,
                            day_volume_usdc: 100_000.0,
                            open_interest_usdc: 50_000.0,
                            prev_day_price: entry_px,
                            is_core_eligible: true,
                            is_event_eligible: true,
                            is_priority_watched: true,
                        })?;
                        self.db.get_asset_by_id(aid)?.ok_or_else(|| {
                            aegis_core::AegisError::Database(format!("Asset {aid} missing"))
                        })?
                    }
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
                let _ = self.db.update_asset_live_quote(asset.id, mark_px, mark_px);

                let mut closed_on_exchange = false;
                let mut remaining_qty = qty;
                if close_legacy_positions && !private_key.is_empty() {
                    let close_is_buy = !side.is_buy();
                    let l2_bbo = self.http.fetch_l2_book(&coin, &cfg).ok().flatten();
                    let bbo_anchor = l2_bbo
                        .map(|(bid, ask)| if close_is_buy { ask } else { bid })
                        .filter(|&px| px > 0.0)
                        .unwrap_or(mark_px);

                    for (attempt_idx, (base_px, slip_pct)) in [
                        (bbo_anchor, 0.0),
                        (bbo_anchor, 0.01),
                        (bbo_anchor, 0.025),
                        (mark_px, 0.0395),
                        (bbo_anchor, 0.05),
                        (mark_px, 0.075),
                        (bbo_anchor, 0.075),
                        (bbo_anchor, 0.12),
                        (mark_px, 0.12),
                    ]
                    .into_iter()
                    .enumerate()
                    {
                        if remaining_qty <= 1e-9 {
                            break;
                        }
                        let limit_px = if close_is_buy {
                            base_px * (1.0 + slip_pct)
                        } else {
                            base_px * (1.0 - slip_pct)
                        };
                        let wire = OrderWire {
                            a: asset.asset_index,
                            b: close_is_buy,
                            p: format_hl_wire_price(limit_px, asset.tick_size, asset.sz_decimals),
                            s: format_hl_wire_size(
                                remaining_qty,
                                asset.lot_step_size,
                                asset.sz_decimals,
                            ),
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
                    let stop_px = if side.is_buy() {
                        snap_to_tick(entry_px.min(mark_px) * (1.0 - default_stop_pct), tick)
                    } else {
                        snap_to_tick(entry_px.max(mark_px) * (1.0 + default_stop_pct), tick)
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
                    let (_pid, _newly_adopted) = self.db.sync_open_position_from_exchange(
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
                    )?;
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

        // 5. Reconcile historical `CLOSED` trades 1:1 from Hyperliquid `userFills`
        if let Ok(user_fills) = self.http.fetch_user_fills(&wallet, &cfg) {
            #[derive(Default)]
            struct AggClosingOrder {
                coin: String,
                side: Option<OrderSide>,
                total_sz: f64,
                notional_sum: f64,
                gross_closed_pnl: f64,
                fees_usdc: f64,
                min_time_ms: i64,
                max_time_ms: i64,
            }

            let mut agg_by_key: BTreeMap<(String, u64), AggClosingOrder> = BTreeMap::new();

            for fill in &user_fills {
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
                let closed_pnl = parse_f(fill.get("closedPnl"));
                let dir = fill.get("dir").and_then(Value::as_str).unwrap_or("").trim();
                let is_closing =
                    closed_pnl.abs() > 1e-9 || dir.starts_with("Close") || dir.contains('>');
                if !is_closing {
                    continue;
                }

                let px = parse_f(fill.get("px"));
                let sz = parse_f(fill.get("sz"));
                if px <= 0.0 || sz <= 0.0 {
                    continue;
                }
                let fee = parse_f(fill.get("fee"));
                let fill_time = fill
                    .get("time")
                    .and_then(Value::as_i64)
                    .filter(|&t| t > 0)
                    .unwrap_or(ts);
                let oid = fill
                    .get("oid")
                    .and_then(Value::as_u64)
                    .or_else(|| {
                        fill.get("oid")
                            .and_then(Value::as_str)
                            .and_then(|s| s.parse::<u64>().ok())
                    })
                    .filter(|&id| id > 0)
                    .or_else(|| fill.get("tid").and_then(Value::as_u64))
                    .unwrap_or(fill_time as u64);

                let fill_side_str = fill.get("side").and_then(Value::as_str).unwrap_or("");
                let closed_pos_side = if dir.starts_with("Close Long")
                    || dir.starts_with("Long >")
                    || (!dir.contains("Short") && fill_side_str == "A")
                {
                    OrderSide::Long
                } else {
                    OrderSide::Short
                };

                let entry =
                    agg_by_key
                        .entry((coin.clone(), oid))
                        .or_insert_with(|| AggClosingOrder {
                            coin: coin.clone(),
                            side: Some(closed_pos_side),
                            min_time_ms: fill_time,
                            max_time_ms: fill_time,
                            ..Default::default()
                        });
                entry.total_sz += sz;
                entry.notional_sum += px * sz;
                entry.gross_closed_pnl += closed_pnl;
                entry.fees_usdc += fee;
                entry.min_time_ms = entry.min_time_ms.min(fill_time);
                entry.max_time_ms = entry.max_time_ms.max(fill_time);
            }

            let mut closed_inputs = Vec::with_capacity(agg_by_key.len());
            for ((_coin_key, oid), agg) in agg_by_key {
                if agg.total_sz <= 1e-9 {
                    continue;
                }
                let exit_px = agg.notional_sum / agg.total_sz;
                let side = agg.side.unwrap_or(OrderSide::Long);
                let entry_px = match side {
                    OrderSide::Long => (exit_px - (agg.gross_closed_pnl / agg.total_sz)).max(1e-8),
                    OrderSide::Short => (exit_px + (agg.gross_closed_pnl / agg.total_sz)).max(1e-8),
                };
                let net_rpnl = round_dp(agg.gross_closed_pnl - agg.fees_usdc, 4);

                let asset_id = match self.db.get_asset_by_symbol(&agg.coin)? {
                    Some(a) => a.id,
                    None => {
                        let tick = infer_tick_size(exit_px, 2);
                        self.db.upsert_exchange_asset(&AssetUpsertInput {
                            raw_symbol_or_coin: agg.coin.clone(),
                            asset_index: 0,
                            sz_decimals: 2,
                            tick_size: tick,
                            lot_step_size: 0.01,
                            max_leverage: 20.0,
                            mark_price: exit_px,
                            mid_price: exit_px,
                            oracle_price: exit_px,
                            funding_rate_hourly: 0.0,
                            day_volume_usdc: 100_000.0,
                            open_interest_usdc: 50_000.0,
                            prev_day_price: exit_px,
                            is_core_eligible: false,
                            is_event_eligible: true,
                            is_priority_watched: false,
                        })?
                    }
                };

                closed_inputs.push(ReconciledClosedTradeInput {
                    asset_id,
                    client_order_id: format!("hl-closed-{}-{oid}", agg.coin.to_ascii_lowercase()),
                    exchange_order_id: format!("hl-{oid}"),
                    side,
                    entry_price: entry_px,
                    exit_price: exit_px,
                    filled_quantity: agg.total_sz,
                    realized_pnl_usdc: net_rpnl,
                    fees_paid_usdc: round_dp(agg.fees_usdc, 4),
                    created_at_ms: agg.min_time_ms,
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
