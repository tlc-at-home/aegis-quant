//! 1-Second High-Frequency Surveillance Loop (`aegis-execution::priority_monitor`).
//!
//! Queries `v_priority_watchlist`, `v_assets_ordered`, and `v_assets_held` every 1 second:
//! - Advances all `WORKING` Maker Limit Chaser orders (`v_assets_ordered`), promoting `WORKING -> OPEN` upon fill confirmation.
//! - Monitors all `OPEN` holdings (`v_assets_held`), updating live mark prices & unrealized PnL,
//!   ratcheting 1s High-Water Chandelier/ATR protective trailing stops, and executing immediate exits +
//!   inserting a post-stop cooldown (`asset_cooldowns`) + rich HTML Telegram alert on stop breach.

use crate::http_client::HyperliquidHttpClient;
use crate::order_chaser::{parse_hl_oid, ChaseStepOutcome, OrderChaser};
use crate::signing::{build_limit_order_wire, snap_to_tick, CancelWire, HL_ORACLE_BAND_PCT};
use aegis_alerts::{AlertEngine, PositionClosedAlert};
use aegis_core::{now_ms, round_dp, HeldAssetView, OrderSide, Result};
use aegis_db::DatabaseManager;
use aegis_risk::{compute_atr_from_bars, RiskEngine};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq)]
struct StopSyncDecision {
    persisted_stop_px: f64,
    sl_ratcheted_materially: bool,
    should_sync_tpsl: bool,
    count_ratchet: bool,
}

fn resolve_stop_sync_decision(
    current_stop_px: f64,
    eval_new_stop_px: f64,
    eval_stop_ratcheted: bool,
    missing_sl: bool,
    missing_tp: bool,
) -> StopSyncDecision {
    let first_stop_attach = current_stop_px <= 0.0 && eval_new_stop_px > 0.0;
    let sl_ratcheted_materially = eval_stop_ratcheted
        && current_stop_px > 0.0
        && ((eval_new_stop_px - current_stop_px).abs() / current_stop_px) >= 0.0025;
    let persisted_stop_px = if first_stop_attach || sl_ratcheted_materially {
        eval_new_stop_px
    } else {
        current_stop_px
    };
    let should_sync_tpsl = missing_sl || missing_tp || sl_ratcheted_materially || first_stop_attach;
    let count_ratchet = sl_ratcheted_materially || first_stop_attach;
    StopSyncDecision {
        persisted_stop_px,
        sl_ratcheted_materially,
        should_sync_tpsl,
        count_ratchet,
    }
}

/// Telemetry report emitted by each 1-second Priority Monitor tick.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PriorityTickReport {
    pub tick_at_ms: i64,
    pub watchlist_count: usize,
    pub working_orders_checked: usize,
    pub orders_chased: usize,
    pub orders_promoted_to_open: usize,
    pub open_holdings_checked: usize,
    pub stops_ratcheted: usize,
    pub stops_triggered_closed: usize,
}

/// 1-Second High-Frequency Priority Position & Active Order Monitor.
#[derive(Clone)]
pub struct PriorityMonitor {
    pub db: DatabaseManager,
    pub http: HyperliquidHttpClient,
    pub chaser: OrderChaser,
    pub alerts: AlertEngine,
    widened_watermarks: Arc<Mutex<HashMap<i64, (f64, f64)>>>,
}

impl PriorityMonitor {
    pub fn new(
        db: DatabaseManager,
        http: HyperliquidHttpClient,
        chaser: OrderChaser,
        alerts: AlertEngine,
    ) -> Self {
        Self {
            db,
            http,
            chaser,
            alerts,
            widened_watermarks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Runs a single 1-second surveillance tick over `v_priority_watchlist`, `v_assets_ordered`, and `v_assets_held`.
    pub fn run_1s_priority_tick(&self) -> Result<PriorityTickReport> {
        let ts = now_ms();
        let cfg = self.db.load_config_or_default();
        let watchlist = self.db.get_priority_watchlist()?;

        // Poll Hyperliquid `allMids` outside the execution lock so slow network responses never block closes
        let mids_opt = if !watchlist.is_empty() && cfg.execution_mode().is_live_or_testnet() {
            self.http.fetch_all_mids(&cfg).ok()
        } else {
            None
        };

        let _exec_guard = crate::engine::execution_lock().lock();

        // Update ONLY `mid_price` from `allMids` (never overwrite true `mark_price`)
        if let Some(ref mids) = mids_opt {
            let mid_updates: Vec<(i64, f64)> = watchlist
                .iter()
                .filter_map(|item| {
                    mids.get(&item.coin).copied().and_then(|live_mid| {
                        (live_mid.is_finite()
                            && live_mid > 0.0
                            && (live_mid - item.mid_price).abs() > 1e-12)
                            .then_some((item.asset_id, live_mid))
                    })
                })
                .collect();
            let _ = self.db.update_assets_live_mid_batch(&mid_updates);
        }

        let mut report = PriorityTickReport {
            tick_at_ms: ts,
            watchlist_count: watchlist.len(),
            ..Default::default()
        };

        // 1. Advance all WORKING Maker Limit Chaser orders in `v_assets_ordered`
        let ordered = self.db.get_assets_ordered()?;
        report.working_orders_checked = ordered.len();
        for ord in &ordered {
            match self.chaser.advance_working_order(ord)? {
                ChaseStepOutcome::Chased { .. } => {
                    report.orders_chased += 1;
                }
                ChaseStepOutcome::FilledOpen { .. } => {
                    report.orders_promoted_to_open += 1;
                }
                ChaseStepOutcome::Resting { .. } | ChaseStepOutcome::Cancelled { .. } => {}
            }
        }

        // 2. Monitor all OPEN holdings in `v_assets_held` for 1s stop ratchet & stop breach
        let held = self.db.get_assets_held()?;
        report.open_holdings_checked = held.len();
        let risk = RiskEngine::from_config_map(&cfg);
        let mut pos_updates: Vec<(i64, f64, f64)> = Vec::with_capacity(held.len());
        let mut tpsl_syncs = Vec::new();

        for h in &held {
            let asset_opt = self.db.get_asset_by_id(h.asset_id)?;
            let tick_size = asset_opt.as_ref().map(|a| a.tick_size).unwrap_or(0.01);
            let live_mark = asset_opt
                .as_ref()
                .map(|a| a.mark_price)
                .filter(|&p| p > 0.0)
                .unwrap_or(h.mark_price);

            let bars_1h = self
                .db
                .get_ohlcv_bars(h.asset_id, "1h", 20)
                .unwrap_or_default();
            let atr_opt = (!bars_1h.is_empty()).then(|| {
                compute_atr_from_bars(
                    &bars_1h,
                    14,
                    live_mark,
                    risk.limits.protective_stop_default_pct,
                )
            });

            let side = OrderSide::parse(&h.side);
            let (eff_highest, eff_lowest) = {
                let mut guard = self.widened_watermarks.lock();
                if let Some((wh, wl)) = guard.get_mut(&h.position_id) {
                    if live_mark.is_finite() && live_mark > 0.0 {
                        *wh = wh.max(live_mark);
                        *wl = if *wl > 0.0 {
                            wl.min(live_mark)
                        } else {
                            live_mark
                        };
                    }
                    (*wh, *wl)
                } else {
                    (h.highest_price_seen, h.lowest_price_seen)
                }
            };
            let eval = risk.evaluate_1s_stop_and_tp_ratchet(
                side,
                h.entry_price,
                live_mark,
                h.quantity,
                h.protective_stop_px,
                h.take_profit_px,
                eff_highest,
                eff_lowest,
                atr_opt,
                tick_size,
            );

            if eval.triggered {
                let exit_reason = eval
                    .exit_reason
                    .as_deref()
                    .unwrap_or("PROTECTIVE_STOP_BREACH");
                if self
                    .execute_protective_stop_exit(h, live_mark, exit_reason, &risk)?
                    .is_some()
                {
                    self.widened_watermarks.lock().remove(&h.position_id);
                    report.stops_triggered_closed += 1;
                }
            } else {
                let missing_sl = h
                    .protective_stop_oid
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or("")
                    .is_empty();
                let missing_tp = h
                    .take_profit_oid
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or("")
                    .is_empty()
                    || h.take_profit_px <= 0.0;
                let decision = resolve_stop_sync_decision(
                    h.protective_stop_px,
                    eval.new_protective_stop_px,
                    eval.stop_ratcheted,
                    missing_sl,
                    missing_tp,
                );
                pos_updates.push((h.position_id, live_mark, decision.persisted_stop_px));
                if decision.count_ratchet {
                    report.stops_ratcheted += 1;
                }
                if decision.should_sync_tpsl {
                    if let Some(asset) = asset_opt {
                        tpsl_syncs.push((
                            h,
                            asset,
                            side,
                            live_mark,
                            decision.persisted_stop_px,
                            eval.take_profit_px,
                            decision.sl_ratcheted_materially,
                        ));
                    }
                }
            }
        }

        self.db
            .update_open_positions_mark_and_stop_batch(&pos_updates)?;

        for (h, asset, side, live_mark, stop_px, tp_px, sl_ratcheted_materially) in tpsl_syncs {
            let _ = self.chaser.ensure_exchange_tpsl_orders(
                h.position_id,
                &asset,
                side,
                h.quantity,
                live_mark,
                stop_px,
                tp_px,
                h.protective_stop_oid.as_deref(),
                h.take_profit_oid.as_deref(),
                sl_ratcheted_materially,
                false,
                None,
                &cfg,
            );
        }

        Ok(report)
    }

    /// Executes an immediate stop-out or take-profit exit for an `OPEN` holding, inserts a post-exit cooldown
    /// into `asset_cooldowns`, logs telemetry, and dispatches a rich HTML Telegram alert.
    /// In `TESTNET`/`LIVE` mode, only closes in MariaDB and fires an alert if Hyperliquid confirms a fill;
    /// if the Testnet orderbook cannot fill the close order, re-anchors `protective_stop_px` / `take_profit_px`
    /// beyond `exit_price` so it never enters a 1s alert/close loop.
    pub fn execute_protective_stop_exit(
        &self,
        held: &HeldAssetView,
        exit_price: f64,
        exit_reason: &str,
        risk: &RiskEngine,
    ) -> Result<Option<f64>> {
        let cfg = self.db.load_config_or_default();
        let side = OrderSide::parse(&held.side);
        let is_tp_exit = exit_reason.starts_with("TAKE_PROFIT");
        let taker_fee_bps = risk.limits.taker_fee_bps;
        let default_stop_pct = risk.limits.protective_stop_default_pct.clamp(0.01, 0.25);
        let mut fees_paid_usdc =
            round_dp(exit_price * held.quantity * (taker_fee_bps / 10_000.0), 4);
        let mut eff_exit_price = exit_price;
        let mut live_close_oid: Option<String> = None;

        let is_live_exchange = crate::engine::is_live_exchange_active(&self.db, &cfg);

        if is_live_exchange {
            let pk = cfg.get_str("private_key", "");
            let wallet = cfg.get_str("wallet_address", "");
            if wallet.trim().is_empty() {
                tracing::warn!(
                    position_id = held.position_id,
                    symbol = %held.symbol,
                    "wallet_address is empty; skipping live protective stop close"
                );
                return Ok(None);
            }

            let parse_f = |v: Option<&Value>| -> f64 {
                v.and_then(|x| {
                    x.as_f64()
                        .or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))
                })
                .filter(|val| val.is_finite())
                .unwrap_or(0.0)
            };

            let ch_state = match self.http.fetch_clearinghouse_state(&wallet, &cfg) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        position_id = held.position_id,
                        symbol = %held.symbol,
                        error = %e,
                        "clearinghouseState unreachable during protective exit; skipping DB close"
                    );
                    return Ok(None);
                }
            };

            let mut live_szi = 0.0_f64;
            if let Some(arr) = ch_state.get("assetPositions").and_then(Value::as_array) {
                for item in arr {
                    let p = item.get("position").unwrap_or(item);
                    let c = p.get("coin").and_then(Value::as_str).unwrap_or("").trim();
                    if c.eq_ignore_ascii_case(&held.coin) {
                        live_szi = parse_f(p.get("szi"));
                        break;
                    }
                }
            }

            let asset_opt = self.db.get_asset_by_id(held.asset_id)?;
            let tick = asset_opt
                .as_ref()
                .map(|a| a.tick_size)
                .unwrap_or(0.01)
                .max(1e-8);
            let mut filled_on_hl = false;

            if live_szi.abs() <= 1e-9 {
                // HL native SL/TP trigger already filled on exchange: close DB without sending IOC retries or phantom SL
                filled_on_hl = true;
                fees_paid_usdc = 0.0;
                if let Ok(fills) = self.http.fetch_user_fills(&wallet, &cfg) {
                    let now = now_ms();
                    let mut sz_sum = 0.0_f64;
                    let mut ntl_sum = 0.0_f64;
                    let mut fee_sum = 0.0_f64;
                    let mut last_oid: Option<u64> = None;
                    for f in fills.iter().rev() {
                        let fc = f.get("coin").and_then(Value::as_str).unwrap_or("").trim();
                        let ft = f.get("time").and_then(Value::as_i64).unwrap_or(0);
                        let cpnl = parse_f(f.get("closedPnl"));
                        let dir = f.get("dir").and_then(Value::as_str).unwrap_or("");
                        if fc.eq_ignore_ascii_case(&held.coin)
                            && now.saturating_sub(ft) <= 120_000
                            && (cpnl.abs() > 1e-9 || dir.starts_with("Close") || dir.contains('>'))
                        {
                            let px = parse_f(f.get("px"));
                            let sz = parse_f(f.get("sz"));
                            let fee = parse_f(f.get("fee"));
                            if px > 0.0 && sz > 0.0 {
                                sz_sum += sz;
                                ntl_sum += px * sz;
                                fee_sum += fee;
                                if last_oid.is_none() {
                                    last_oid = f.get("oid").and_then(Value::as_u64).or_else(|| {
                                        f.get("oid")
                                            .and_then(Value::as_str)
                                            .and_then(|s| s.parse::<u64>().ok())
                                    });
                                }
                            }
                        }
                    }
                    if sz_sum > 0.0 {
                        eff_exit_price = round_dp(ntl_sum / sz_sum, 6);
                        fees_paid_usdc = round_dp(fee_sum, 4);
                        live_close_oid = last_oid.map(|id| format!("hl-{id}"));
                    }
                }
            } else if let Some(ref asset) = asset_opt {
                let exit_is_buy = live_szi < 0.0;
                let l2_bbo = self.http.fetch_l2_book(&asset.coin, &cfg).ok().flatten();
                let bbo_anchor = l2_bbo
                    .map(|(bid, ask)| if exit_is_buy { ask } else { bid })
                    .filter(|&px| px > 0.0)
                    .unwrap_or(exit_price);
                let band_lo = (exit_price * (1.0 - 0.038)).max(tick);
                let band_hi = exit_price * (1.0 + 0.038);

                let mut rem_qty = live_szi.abs();
                let mut total_hl_fee = 0.0;
                let mut fill_ntl_sum = 0.0;
                let mut fill_qty_sum = 0.0;
                for (attempt, (ref_anchor, slip)) in [
                    (bbo_anchor, 0.0),
                    (bbo_anchor, 0.008),
                    (bbo_anchor, 0.02),
                    (exit_price, HL_ORACLE_BAND_PCT.min(0.038)),
                ]
                .into_iter()
                .enumerate()
                {
                    if rem_qty <= asset.lot_step_size * 0.5 || rem_qty <= 1e-9 {
                        filled_on_hl = true;
                        break;
                    }
                    let raw_limit_px = if exit_is_buy {
                        ref_anchor * (1.0 + slip)
                    } else {
                        (ref_anchor * (1.0 - slip)).max(tick)
                    };
                    let limit_px = raw_limit_px.clamp(band_lo, band_hi);
                    let wire = build_limit_order_wire(
                        asset.asset_index,
                        exit_is_buy,
                        limit_px,
                        rem_qty,
                        asset.tick_size,
                        asset.lot_step_size,
                        asset.sz_decimals,
                        true,
                        "Ioc",
                        Some(&format!(
                            "v2-{}-{}-{attempt}-{}",
                            if is_tp_exit { "tpexit" } else { "stopexit" },
                            held.position_id,
                            now_ms()
                        )),
                    );
                    if let Ok(ack) = self.http.submit_order_wire(
                        wire,
                        rem_qty,
                        ref_anchor,
                        taker_fee_bps,
                        &pk,
                        Some(&self.db),
                        &cfg,
                    ) {
                        if ack.accepted && ack.status == "FILLED" && ack.filled_quantity > 0.0 {
                            let fill_px = if ack.avg_fill_price > 0.0 {
                                ack.avg_fill_price
                            } else {
                                ref_anchor
                            };
                            total_hl_fee += ack.fee_usdc;
                            fill_ntl_sum += fill_px * ack.filled_quantity;
                            fill_qty_sum += ack.filled_quantity;
                            live_close_oid = ack.exchange_order_id;
                            rem_qty = (rem_qty - ack.filled_quantity).max(0.0);
                            if rem_qty <= asset.lot_step_size * 0.5 || rem_qty <= 1e-9 {
                                filled_on_hl = true;
                                break;
                            }
                        }
                    }
                }
                if fill_qty_sum > 0.0 {
                    eff_exit_price = round_dp(fill_ntl_sum / fill_qty_sum, 6);
                    fees_paid_usdc = round_dp(total_hl_fee, 4);
                }

                if filled_on_hl {
                    let mut cancels = Vec::new();
                    if let Some(oid) = parse_hl_oid(held.protective_stop_oid.as_deref()) {
                        cancels.push(CancelWire {
                            a: asset.asset_index,
                            o: oid,
                        });
                    }
                    if let Some(oid) = parse_hl_oid(held.take_profit_oid.as_deref()) {
                        cancels.push(CancelWire {
                            a: asset.asset_index,
                            o: oid,
                        });
                    }
                    if !cancels.is_empty() {
                        let _ = self
                            .http
                            .cancel_orders_batch(cancels, &pk, Some(&self.db), &cfg);
                    }
                }
            }

            if !filled_on_hl {
                if is_tp_exit {
                    let reanchored_tp = if side.is_buy() {
                        snap_to_tick(exit_price * 1.01, tick)
                    } else {
                        snap_to_tick((exit_price * 0.99).max(tick), tick)
                    };
                    if let Some(ref asset) = asset_opt {
                        let _ = self.chaser.ensure_exchange_tpsl_orders(
                            held.position_id,
                            asset,
                            side,
                            held.quantity,
                            exit_price,
                            held.protective_stop_px,
                            reanchored_tp,
                            held.protective_stop_oid.as_deref(),
                            held.take_profit_oid.as_deref(),
                            false,
                            true,
                            None,
                            &cfg,
                        );
                    }
                } else {
                    // Re-anchor protective_stop_px beyond current mark/exit price and reset watermarks so the 1s ratchet doesn't snap back immediately
                    let widened_stop = if side.is_buy() {
                        snap_to_tick((exit_price * (1.0 - default_stop_pct)).max(tick), tick)
                    } else {
                        snap_to_tick(exit_price * (1.0 + default_stop_pct), tick)
                    };
                    self.db.update_open_position_mark_and_stop(
                        held.position_id,
                        exit_price,
                        widened_stop,
                    )?;
                    self.widened_watermarks
                        .lock()
                        .insert(held.position_id, (exit_price, exit_price));
                    if let Some(ref asset) = asset_opt {
                        let _ = self.chaser.ensure_exchange_tpsl_orders(
                            held.position_id,
                            asset,
                            side,
                            held.quantity,
                            exit_price,
                            widened_stop,
                            held.take_profit_px,
                            held.protective_stop_oid.as_deref(),
                            held.take_profit_oid.as_deref(),
                            true,
                            false,
                            None,
                            &cfg,
                        );
                    }
                }
                return Ok(None);
            }
        }

        let realized_pnl = self.db.close_position(
            held.position_id,
            eff_exit_price,
            fees_paid_usdc,
            exit_reason,
        )?;

        if let Some(ref eoid) = live_close_oid {
            let oid_num = eoid.strip_prefix("hl-").unwrap_or(eoid.as_str());
            let coid = format!("hl-closed-{}-{oid_num}", held.coin.to_ascii_lowercase());
            let _ = self
                .db
                .set_position_closed_exchange_ids(held.position_id, &coid, eoid);
        }

        let (cooldown_type, cooldown_sec, cd_reason) = if is_tp_exit {
            (
                "CHURN_GUARD",
                300,
                format!(
                    "Take-profit target triggered on {} at ${:.4} (entry=${:.4}, PnL=${:+.2})",
                    held.symbol, eff_exit_price, held.entry_price, realized_pnl
                ),
            )
        } else {
            (
                "STOP_OUT",
                risk.limits.stop_out_cooldown_sec.max(60),
                format!(
                    "Protective stop triggered on {} at ${:.4} (entry=${:.4}, PnL=${:+.2})",
                    held.symbol, eff_exit_price, held.entry_price, realized_pnl
                ),
            )
        };
        self.db
            .insert_asset_cooldown(held.asset_id, cooldown_type, &cd_reason, cooldown_sec)?;

        let _ = self.db.log_telemetry(
            "WARN",
            "execution.priority_monitor",
            &cd_reason,
            Some(json!({
                "position_id": held.position_id,
                "asset_id": held.asset_id,
                "symbol": held.symbol,
                "side": held.side,
                "entry_price": held.entry_price,
                "exit_price": eff_exit_price,
                "realized_pnl_usdc": realized_pnl,
                "cooldown_seconds": cooldown_sec,
            })),
        );

        let avail_cash = self
            .db
            .get_latest_portfolio_snapshot()
            .map(|s| (s.available_cash_usdc + held.committed_notional_usdc + realized_pnl).max(0.0))
            .unwrap_or(10_000.0);

        let alert = PositionClosedAlert {
            symbol: held.symbol.clone(),
            side,
            exit_reason: exit_reason.to_string(),
            entry_price: held.entry_price,
            exit_price: eff_exit_price,
            filled_qty: held.quantity,
            realized_pnl_usdc: realized_pnl,
            available_cash_usdc: avail_cash,
            cooldown_type: Some(cooldown_type.to_string()),
            cooldown_seconds: cooldown_sec,
            timestamp_ms: now_ms(),
        };
        let _ = self.alerts.notify_position_closed(&alert);

        Ok(Some(realized_pnl))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_db::{AssetUpsertInput, BarUpsertInput};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::Path;
    use std::time::Duration;

    #[test]
    fn test_resolve_stop_sync_decision_gates_sub_25bps_db_writes() {
        // Sub-0.25% tick drift (e.g. PYTH 0.050351 -> 0.050349 = 0.004%, and -> 0.050250 = 0.20%)
        // must keep current_stop_px unchanged in DB and must not trigger tpsl_syncs or count ratchet.
        for small_stop in [0.050349, 0.050250] {
            let dec = resolve_stop_sync_decision(0.050351, small_stop, true, false, false);
            assert_eq!(dec.persisted_stop_px, 0.050351);
            assert!(!dec.sl_ratcheted_materially);
            assert!(!dec.should_sync_tpsl);
            assert!(!dec.count_ratchet);

            // Even if TP leg is missing and triggers sync, persisted_stop_px stays pinned at 0.050351
            let dec_missing_tp =
                resolve_stop_sync_decision(0.050351, small_stop, true, false, true);
            assert_eq!(dec_missing_tp.persisted_stop_px, 0.050351);
            assert!(!dec_missing_tp.sl_ratcheted_materially);
            assert!(dec_missing_tp.should_sync_tpsl);
            assert!(!dec_missing_tp.count_ratchet);
        }

        // Material >= 0.25% ratchet (0.050351 -> 0.050220 = 0.26%) updates persisted stop + syncs + counts
        let material = resolve_stop_sync_decision(0.050351, 0.050220, true, false, false);
        assert_eq!(material.persisted_stop_px, 0.050220);
        assert!(material.sl_ratcheted_materially);
        assert!(material.should_sync_tpsl);
        assert!(material.count_ratchet);

        // Initial stop attachment (current_stop_px <= 0.0 -> > 0.0) updates DB + syncs + counts
        let attach = resolve_stop_sync_decision(0.0, 0.049, false, false, false);
        assert_eq!(attach.persisted_stop_px, 0.049);
        assert!(!attach.sl_ratcheted_materially);
        assert!(attach.should_sync_tpsl);
        assert!(attach.count_ratchet);
    }

    #[test]
    fn test_priority_tick_sub_25bps_ratchet_preserves_db_stop_and_skips_hl_replace() {
        let stop_trigger_px = Arc::new(Mutex::new("1720.0".to_string()));
        let stop_trigger_srv = stop_trigger_px.clone();
        let exchange_requests = Arc::new(Mutex::new(Vec::<Value>::new()));
        let exchange_srv = exchange_requests.clone();

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback mock server");
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();

        std::thread::spawn(move || {
            while stop_rx.try_recv().is_err() {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                        let mut buf = Vec::with_capacity(4096);
                        let mut tmp = [0u8; 2048];
                        let mut header_end = None;
                        let mut content_len = 0usize;
                        while let Ok(n) = stream.read(&mut tmp) {
                            if n == 0 {
                                break;
                            }
                            buf.extend_from_slice(&tmp[..n]);
                            if header_end.is_none() {
                                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                    header_end = Some(pos + 4);
                                    let hdr = String::from_utf8_lossy(&buf[..pos]);
                                    for line in hdr.lines() {
                                        if let Some((k, v)) = line.split_once(':') {
                                            if k.trim().eq_ignore_ascii_case("content-length") {
                                                content_len =
                                                    v.trim().parse::<usize>().unwrap_or(0);
                                            }
                                        }
                                    }
                                }
                            }
                            if let Some(hend) = header_end {
                                if buf.len() >= hend + content_len {
                                    break;
                                }
                            }
                        }
                        let Some(hdr_end) = header_end else {
                            continue;
                        };
                        let hdr_str = String::from_utf8_lossy(&buf[..hdr_end]).to_string();
                        let path = hdr_str
                            .lines()
                            .next()
                            .and_then(|l| l.split_whitespace().nth(1))
                            .unwrap_or("/")
                            .to_string();
                        let body_bytes = &buf[hdr_end..buf.len().min(hdr_end + content_len)];
                        let req_json: Value =
                            serde_json::from_slice(body_bytes).unwrap_or(Value::Null);
                        let resp_json = if path == "/info" {
                            let typ = req_json.get("type").and_then(Value::as_str).unwrap_or("");
                            match typ {
                                "allMids" => json!({ "ETH": "1734.0" }),
                                "frontendOpenOrders" | "openOrders" => json!([
                                    {
                                        "coin": "ETH",
                                        "oid": 90001u64,
                                        "isTrigger": true,
                                        "orderType": "Stop Market",
                                        "triggerPx": stop_trigger_srv.lock().clone(),
                                        "sz": "0.0253"
                                    },
                                    {
                                        "coin": "ETH",
                                        "oid": 90002u64,
                                        "isTrigger": true,
                                        "orderType": "Take Profit Market",
                                        "triggerPx": "1831.6",
                                        "sz": "0.0253"
                                    }
                                ]),
                                _ => json!([]),
                            }
                        } else {
                            exchange_srv.lock().push(req_json.clone());
                            if req_json.pointer("/action/type").and_then(Value::as_str)
                                == Some("order")
                            {
                                json!({
                                    "status": "ok",
                                    "response": {
                                        "type": "order",
                                        "data": {
                                            "statuses": [{ "waitingForTrigger": { "oid": 90003u64 } }]
                                        }
                                    }
                                })
                            } else {
                                json!({ "status": "ok" })
                            }
                        };
                        let resp_body = resp_json.to_string();
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            resp_body.len(),
                            resp_body
                        );
                        let _ = stream.write_all(resp.as_bytes());
                        let _ = stream.flush();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        let db = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
        let http = HyperliquidHttpClient::new(5);
        let alerts = AlertEngine::new(db.clone());
        let chaser = OrderChaser::new(db.clone(), http.clone(), alerts.clone());
        let monitor = PriorityMonitor::new(db.clone(), http, chaser, alerts);

        let base_url = format!("http://127.0.0.1:{port}");
        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_testnet_url", &base_url, None)
            .unwrap();
        db.set_config_value(
            "hyperliquid_allowed_hosts",
            "127.0.0.1,api.hyperliquid.xyz,api.hyperliquid-testnet.xyz",
            None,
        )
        .unwrap();
        db.set_config_value(
            "wallet_address",
            "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
            None,
        )
        .unwrap();
        db.set_config_value(
            "private_key",
            "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
            None,
        )
        .unwrap();

        let eth_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "ETH".to_string(),
                asset_index: 1,
                sz_decimals: 3,
                tick_size: 0.1,
                lot_step_size: 0.001,
                max_leverage: 25.0,
                mark_price: 1734.0,
                mid_price: 1734.0,
                oracle_price: 1734.0,
                funding_rate_hourly: 0.0000125,
                day_volume_usdc: 20_000_000.0,
                open_interest_usdc: 8_000_000.0,
                prev_day_price: 1700.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();

        let base_ts = now_ms() - 20 * 3_600_000;
        let bars: Vec<BarUpsertInput> = (0..20)
            .map(|i| BarUpsertInput {
                asset_id: eth_id,
                timeframe: "1h".to_string(),
                timestamp_ms: base_ts + i * 3_600_000,
                open: 1718.0,
                high: 1724.0,
                low: 1712.0,
                close: 1718.0,
                volume: 1000.0,
                funding_rate: 0.00001,
            })
            .collect();
        db.upsert_ohlcv_bars(&bars).unwrap();

        let (pos_id, _) = db
            .sync_open_position_from_exchange_v2(
                eth_id,
                "ETH",
                OrderSide::Long,
                1718.0,
                1734.0,
                0.0253,
                0.0253 * 1734.0,
                0.0,
                0.05,
                1720.0,
                1831.6,
                0.0,
                Some(now_ms() - 600_000),
            )
            .unwrap();
        db.update_position_tpsl_orders(pos_id, 1720.0, Some("hl-90001"), 1831.6, Some("hl-90002"))
            .unwrap();

        // 1. Mark 1734.0 gives a sub-0.25% Tier-1 ratchet (1720.0 -> 1720.6, +0.035%):
        //    DB protective_stop_px MUST stay 1720.0, highest_price_seen tracks 1734.0, and 0 HL requests fire.
        let tick1 = monitor.run_1s_priority_tick().unwrap();
        assert_eq!(tick1.stops_ratcheted, 0);
        let pos1 = db.get_position_by_id(pos_id).unwrap().unwrap();
        assert!((pos1.protective_stop_px - 1720.0).abs() < 1e-6);
        assert!((pos1.highest_price_seen - 1734.0).abs() < 1e-6);
        assert!(exchange_requests.lock().is_empty());

        // 2. Mark 1746.0 crosses 1.0R (Tier 2: 1731.4 vs 1720.0 = +0.66% >= 0.25%):
        //    DB protective_stop_px updates to 1731.4, report.stops_ratcheted == 1, and HL SL order replaces.
        db.update_asset_live_quote(eth_id, 1746.0, 1746.0).unwrap();
        let tick2 = monitor.run_1s_priority_tick().unwrap();
        assert_eq!(tick2.stops_ratcheted, 1);
        let pos2 = db.get_position_by_id(pos_id).unwrap().unwrap();
        assert!((pos2.protective_stop_px - 1731.4).abs() < 1e-6);
        assert!((pos2.highest_price_seen - 1746.0).abs() < 1e-6);
        assert_eq!(pos2.protective_stop_oid.as_deref(), Some("hl-90003"));
        let exch_reqs = exchange_requests.lock();
        assert!(exch_reqs
            .iter()
            .any(|r| r.pointer("/action/type").and_then(Value::as_str) == Some("order")));

        let _ = stop_tx.send(());
    }
}
