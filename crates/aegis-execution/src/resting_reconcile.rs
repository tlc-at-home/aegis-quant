//! Hourly PIT funding synchronization and resting `OPEN_ON_BOOK` / `PENDING` order reconciliation (`NM-4`, `NH-4`, `R3-B1`, `R3-H2`, `R3-H4`, `R3-H16`, `R4-N4`, `R9-N1`, `R9-N3`, `R9-QB`).

use std::collections::{HashMap, HashSet};

use aegis_core::{round_dp, TradeRecord};
use serde_json::Value;

use crate::{ExecutionEngine, FundingSyncReport, FundingSyncStatus};

impl ExecutionEngine {
    /// Accrues or reconciles hourly funding payments across open Book A (`PIT`) perpetual positions (`NM-4`, `R3-B1`, `R4-N4`).
    ///
    /// Note (`R3-B1`): Book B (`POND`) cells accrue hourly funding exclusively inside
    /// `PondOrchestrator::evaluate_open_cells` via `accrue_pond_cell_funding_for_hour`
    /// so Pond cells are never double-credited or double-debited.
    pub fn sync_funding_payments(
        &self,
        current_ts_ms: Option<i64>,
        is_backtest: bool,
    ) -> FundingSyncReport {
        let now_ms = current_ts_ms.unwrap_or_else(|| self.db.now_ms());
        let hour_bucket = (now_ms / 3_600_000) * 3_600_000;
        let hour_bucket_idx = now_ms.div_euclid(3_600_000);
        if *self.last_funding_sync_hour_ts.lock() == Some(hour_bucket) {
            return FundingSyncReport {
                status: FundingSyncStatus::AlreadySyncedThisHour,
                reason: None,
                hour_bucket_ms: hour_bucket,
                positions_updated: 0,
                net_funding_delta_usdc: 0.0,
            };
        }

        let open_trades = match self.db.get_open_trades() {
            Ok(trades) => trades,
            Err(e) => {
                tracing::error!(error = %e, "Failed to query open trades during sync_funding_payments");
                return FundingSyncReport {
                    status: FundingSyncStatus::Error,
                    reason: Some(e.to_string()),
                    hour_bucket_ms: hour_bucket,
                    positions_updated: 0,
                    net_funding_delta_usdc: 0.0,
                };
            }
        };

        if open_trades.is_empty() {
            *self.last_funding_sync_hour_ts.lock() = Some(hour_bucket);
            return FundingSyncReport {
                status: FundingSyncStatus::NoOpenPositions,
                reason: None,
                hour_bucket_ms: hour_bucket,
                positions_updated: 0,
                net_funding_delta_usdc: 0.0,
            };
        }

        let mut onchain_rate_by_coin: HashMap<String, f64> = HashMap::new();
        if !is_backtest {
            let cfg = self.load_config_logged("sync_funding_payments");
            let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
            let pond_mode = cfg
                .get_str("pond_execution_mode", &mode)
                .to_ascii_uppercase();
            let is_live_or_testnet = mode == "LIVE"
                || mode == "TESTNET"
                || pond_mode == "LIVE"
                || pond_mode == "TESTNET";
            let is_testnet = cfg.get_bool("hyperliquid_testnet", false)
                || mode == "TESTNET"
                || pond_mode == "TESTNET";
            let wallet = cfg.get_str("wallet_address", "");
            if is_live_or_testnet && !wallet.trim().is_empty() {
                if let Some(entries) = self.fetch_hyperliquid_user_funding(
                    &wallet,
                    hour_bucket.saturating_sub(3_600_000),
                    is_testnet,
                ) {
                    for entry in entries {
                        let delta_obj = entry.get("delta").unwrap_or(&entry);
                        let coin = delta_obj
                            .get("coin")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .trim()
                            .to_uppercase();
                        let rate_opt = delta_obj.get("fundingRate").and_then(|v| {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                        });
                        if !coin.is_empty() {
                            if let Some(rate) = rate_opt.filter(|r| r.is_finite()) {
                                onchain_rate_by_coin.insert(coin, rate);
                            }
                        }
                    }
                }
            }
        }

        let mut updated = 0usize;
        let mut skipped_missing = 0usize;
        let mut net_funding_delta = 0.0;

        for trade in open_trades {
            if trade.fsm_state == "OPEN_ON_BOOK" && trade.filled_units <= 0.0 {
                continue;
            }
            let bars = self
                .db
                .get_ohlcv_bars(&trade.symbol, "1h", 2, Some(now_ms))
                .unwrap_or_default();
            let coin = trade.base_asset.trim().to_uppercase();
            let Some(funding_rate) = onchain_rate_by_coin
                .get(&coin)
                .copied()
                .or_else(|| bars.last().map(|b| b.funding_rate_hourly))
            else {
                skipped_missing += 1;
                let should_warn = {
                    let mut guard = self.missing_funding_warn_ms.lock();
                    let prev = guard.get(&coin).copied().unwrap_or(0);
                    if prev == 0 || now_ms.saturating_sub(prev) >= 3_600_000 {
                        guard.insert(coin.clone(), now_ms);
                        true
                    } else {
                        false
                    }
                };
                if should_warn {
                    tracing::warn!(
                        trade_id = trade.trade_id,
                        symbol = %trade.symbol,
                        "Skipping hourly funding accrual: no on-chain or 1h bar funding rate available (ME-8: will retry this hour)"
                    );
                }
                continue;
            };
            let mark_px = bars.last().map(|b| b.close).unwrap_or(trade.current_price);
            let notional = trade.quantity.abs() * mark_px;
            let delta = round_dp(
                if trade.side == "LONG" {
                    -notional * funding_rate
                } else {
                    notional * funding_rate
                },
                4,
            );
            let upnl = round_dp(
                if trade.side == "LONG" {
                    (mark_px - trade.entry_price) * trade.quantity.abs()
                } else {
                    (trade.entry_price - mark_px) * trade.quantity.abs()
                },
                4,
            );
            match self.db.accrue_trade_funding_for_hour(
                trade.trade_id,
                hour_bucket_idx,
                mark_px,
                upnl,
                delta,
            ) {
                Ok(true) => {
                    net_funding_delta += delta;
                    updated += 1;
                }
                Ok(false) => {}
                Err(e) => {
                    skipped_missing += 1;
                    tracing::error!(
                        trade_id = trade.trade_id,
                        error = %e,
                        "Failed to accrue open trade funding (R4-N4, ME-8)"
                    );
                }
            }
        }

        // ME-8: Only mark the hour bucket as completed when no open trades were skipped due to missing rates/errors.
        if skipped_missing == 0 {
            *self.last_funding_sync_hour_ts.lock() = Some(hour_bucket);
        }

        if !is_backtest && updated > 0 {
            let _ = self.db.log_telemetry(
                "INFO",
                "engine.execution",
                &format!(
                    "Synced 1H funding across {updated} open positions (net delta: ${net_funding_delta:+.4})"
                ),
                None,
            );
        }

        FundingSyncReport {
            status: FundingSyncStatus::Synced,
            reason: None,
            hour_bucket_ms: hour_bucket,
            positions_updated: updated,
            net_funding_delta_usdc: round_dp(net_funding_delta, 4),
        }
    }

    /// Resolves the bounded fill price when promoting a `"GONE"` resting PIT order or `ARMED` Pond cell
    /// via `clearinghouseState` during `userFills` indexer lag (`R13-N1`, `R13-N4(a)`).
    ///
    /// - When the coin already holds an open position from either book (`already_covered_qty > 1e-6`),
    ///   `clearinghouseState`'s `entryPx` is an account-level blended average (e.g. 1 BTC at 90k +
    ///   1 BTC resting fill at 95k -> `entryPx` 92.5k), so we strictly prefer `resting_limit_px`.
    /// - On an unblended single position (`already_covered_qty <= 1e-6`), we accept `ex_entry_px`
    ///   only when it is within 50 bps (`0.5%`) of `resting_limit_px` (or when `resting_limit_px <= 0.0`).
    /// - Never falls back to a magic `100.0` price; returns `0.0` when no valid price exists so the
    ///   caller rejects promotion (`R13-N4(a)`).
    pub fn resolve_fix4_clearinghouse_fill_price(
        _side: &str,
        resting_limit_px: f64,
        ex_entry_px: f64,
        already_covered_qty: f64,
    ) -> f64 {
        let valid_limit = resting_limit_px > 0.0 && resting_limit_px.is_finite();
        let valid_ex = ex_entry_px > 0.0 && ex_entry_px.is_finite();
        if valid_limit {
            if already_covered_qty > 1e-6 {
                return resting_limit_px;
            }
            if valid_ex {
                let rel_diff = (ex_entry_px - resting_limit_px).abs() / resting_limit_px;
                if rel_diff <= 0.005 {
                    return ex_entry_px;
                }
            }
            resting_limit_px
        } else if valid_ex && already_covered_qty <= 1e-6 {
            ex_entry_px
        } else {
            0.0
        }
    }

    pub fn register_pending_fix4_pit_trueup(
        &self,
        trade_id: i64,
        oid: Option<&str>,
        estimated_fee_usdc: f64,
    ) {
        if let Some(oid_str) = oid.map(str::trim).filter(|s| !s.is_empty()) {
            self.pending_fix4_fill_trueups.lock().insert(
                (false, trade_id),
                (oid_str.to_string(), estimated_fee_usdc.max(0.0)),
            );
        }
    }

    pub fn register_pending_fix4_pond_trueup(
        &self,
        cell_id: i64,
        oid: Option<&str>,
        estimated_fee_usdc: f64,
    ) {
        if let Some(oid_str) = oid.map(str::trim).filter(|s| !s.is_empty()) {
            self.pending_fix4_fill_trueups.lock().insert(
                (true, cell_id),
                (oid_str.to_string(), estimated_fee_usdc.max(0.0)),
            );
        }
    }

    /// Trues up `entry_price`, `notional_usdc`, `risk_budget_usdc`, and `fees_paid_usdc` on any
    /// PIT trade or Pond cell that was race-promoted via `clearinghouseState` once its actual
    /// maker fill is indexed in `userFills` (`R13-N1`, `R13-N4(b)`).
    pub fn true_up_pending_fix4_promotions(&self, simulated_fills: Option<&[Value]>) -> usize {
        let pending_entries: Vec<((bool, i64), (String, f64))> = self
            .pending_fix4_fill_trueups
            .lock()
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect();
        if pending_entries.is_empty() {
            return 0;
        }
        let mut trued_up = 0usize;
        for ((is_pond, row_id), (oid_str, est_fee)) in pending_entries {
            let status_opt = if let Some(fills) = simulated_fills {
                let oid_num = oid_str
                    .strip_prefix("hl-")
                    .unwrap_or(&oid_str)
                    .parse::<u64>()
                    .ok()
                    .filter(|&id| id > 0);
                oid_num.and_then(|target_oid| {
                    let mut filled_units = 0.0;
                    let mut notional_sum = 0.0;
                    let mut fee_sum = 0.0;
                    for f in fills {
                        let f_oid = f.get("oid").and_then(Value::as_u64).or_else(|| {
                            f.get("oid")
                                .and_then(Value::as_str)
                                .and_then(|s| s.parse::<u64>().ok())
                        });
                        if f_oid == Some(target_oid) {
                            let sz = f
                                .get("sz")
                                .and_then(|v| {
                                    v.as_f64()
                                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                                })
                                .unwrap_or(0.0)
                                .abs();
                            let px = f
                                .get("px")
                                .and_then(|v| {
                                    v.as_f64()
                                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                                })
                                .unwrap_or(0.0);
                            let fee = f
                                .get("fee")
                                .and_then(|v| {
                                    v.as_f64()
                                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                                })
                                .unwrap_or(0.0);
                            if sz > 0.0 && px > 0.0 {
                                filled_units += sz;
                                notional_sum += sz * px;
                                fee_sum += fee.max(0.0);
                            }
                        }
                    }
                    if filled_units > 0.0 {
                        Some((filled_units, notional_sum / filled_units, fee_sum))
                    } else {
                        None
                    }
                })
            } else {
                let st = self.check_resting_order_status_with_open_orders(Some(&oid_str), true);
                if st.state == "FILLED" && st.filled_units > 0.0 && st.entry_price > 0.0 {
                    Some((st.filled_units, st.entry_price, st.fee_usdc))
                } else {
                    None
                }
            };

            if let Some((fill_qty, fill_px, actual_fee)) = status_opt {
                let ok = if is_pond {
                    matches!(
                        self.db.true_up_promoted_pond_cell_from_user_fills(
                            row_id, fill_qty, fill_px, actual_fee, est_fee,
                        ),
                        Ok(true)
                    )
                } else {
                    matches!(
                        self.db.true_up_promoted_trade_from_user_fills(
                            row_id, fill_qty, fill_px, actual_fee, est_fee,
                        ),
                        Ok(true)
                    )
                };
                if ok {
                    trued_up += 1;
                    self.pending_fix4_fill_trueups
                        .lock()
                        .remove(&(is_pond, row_id));
                }
            }
        }
        trued_up
    }

    /// Single shared helper for `GONE`-to-promote-via-`clearinghouseState` across Book A (`PIT`)
    /// and Book B (`POND`) reconciliation paths (`Fix 4`, `R13-N1`, `R13-N4(a),(b)`, `R13-QA`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_promote_gone_via_clearinghouse<F>(
        &self,
        coin: &str,
        side: &str,
        resting_qty: f64,
        resting_limit_px: f64,
        maker_fee_bps: f64,
        ex_positions: &HashMap<String, (f64, f64, f64, f64)>,
        existing_open_qty_by_coin: &mut HashMap<String, f64>,
        promote_fn: F,
    ) -> Option<(f64, f64, f64)>
    where
        F: FnOnce(f64, f64, f64) -> aegis_core::Result<bool>,
    {
        let &(szi, ex_entry_px, _, _) = ex_positions.get(coin)?;
        let same_side = (side.eq_ignore_ascii_case("LONG") && szi > 0.0)
            || (side.eq_ignore_ascii_case("SHORT") && szi < 0.0);
        let already_covered = existing_open_qty_by_coin.get(coin).copied().unwrap_or(0.0);
        let uncovered_qty = (szi.abs() - already_covered).max(0.0);
        if !same_side || uncovered_qty <= 1e-6 {
            return None;
        }
        let fill_px = Self::resolve_fix4_clearinghouse_fill_price(
            side,
            resting_limit_px,
            ex_entry_px,
            already_covered,
        );
        if fill_px <= 0.0 {
            return None;
        }
        let fill_qty = if resting_qty > 0.0 {
            uncovered_qty.min(resting_qty)
        } else {
            uncovered_qty
        };
        let fee_usdc = round_dp(fill_qty * fill_px * (maker_fee_bps / 10_000.0), 4);
        if matches!(promote_fn(fill_qty, fill_px, fee_usdc), Ok(true)) {
            *existing_open_qty_by_coin
                .entry(coin.to_string())
                .or_insert(0.0) += fill_qty.abs();
            Some((fill_qty, fill_px, fee_usdc))
        } else {
            None
        }
    }

    /// Polls resting Book A (`PIT`) orders (`status = 'PENDING'` / `fsm_state = 'OPEN_ON_BOOK'`)
    /// that are no longer present in the 15s `openOrders` snapshot, promoting filled orders to
    /// `OPEN` or cancelling expired orders immediately (`R9-N1`, `R9-N3`, `R9-QB`, `R13-QA`).
    ///
    /// Cross-checks `ex_positions` before cancelling a `"GONE"` resting order so an order that filled
    /// on-chain during Hyperliquid's `userFills` indexing window is promoted to `OPEN` with its
    /// original stops intact rather than cancelled and re-imported as a degraded orphan (Fix 4).
    pub(crate) fn poll_resting_pit_trades_on_15s_tick(
        &self,
        resting_trades: &[TradeRecord],
        open_order_oids: &HashSet<u64>,
        ex_positions: &HashMap<String, (f64, f64, f64, f64)>,
        existing_open_qty_by_coin: &mut HashMap<String, f64>,
        maker_fee_bps: f64,
        is_simulated: bool,
    ) -> bool {
        let mut resting_promoted_or_cancelled = false;
        for rt in resting_trades {
            let oid_num = rt
                .exchange_order_id
                .as_deref()
                .and_then(|s| s.strip_prefix("hl-").or(Some(s)))
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|&id| id > 0);
            if oid_num
                .map(|id| open_order_oids.contains(&id))
                .unwrap_or(false)
            {
                continue;
            }
            let coin = rt.base_asset.to_uppercase();
            let r_status = if is_simulated {
                crate::RestingOrderStatus {
                    state: "GONE".to_string(),
                    filled_units: 0.0,
                    entry_price: 0.0,
                    fee_usdc: 0.0,
                }
            } else {
                self.check_resting_order_status_with_open_orders(
                    rt.exchange_order_id.as_deref(),
                    true,
                )
            };
            if r_status.state == "FILLED" {
                let fill_px = if r_status.entry_price > 0.0 {
                    r_status.entry_price
                } else {
                    rt.entry_price
                };
                if fill_px <= 0.0 {
                    continue;
                }
                let fill_qty = if r_status.filled_units > 0.0 {
                    r_status.filled_units
                } else {
                    rt.quantity
                };
                let fee_usdc = if r_status.fee_usdc > 0.0 {
                    round_dp(r_status.fee_usdc, 4)
                } else {
                    round_dp(fill_qty * fill_px * (maker_fee_bps / 10_000.0), 4)
                };
                if matches!(
                    self.db.promote_resting_trade_to_open(
                        rt.trade_id,
                        fill_qty,
                        fill_px,
                        fee_usdc,
                        0.0
                    ),
                    Ok(true)
                ) {
                    *existing_open_qty_by_coin.entry(coin).or_insert(0.0) += fill_qty.abs();
                    tracing::info!(
                        trade_id = rt.trade_id,
                        symbol = %rt.symbol,
                        fill_qty = fill_qty,
                        fill_px = fill_px,
                        "Promoted filled resting PIT order to OPEN during 15s exchange reconciliation (R9-N1)"
                    );
                    resting_promoted_or_cancelled = true;
                }
            } else if r_status.state == "GONE" {
                if let Some((fill_qty, fill_px, fee_usdc)) = self
                    .try_promote_gone_via_clearinghouse(
                        &coin,
                        &rt.side,
                        rt.quantity,
                        rt.entry_price,
                        maker_fee_bps,
                        ex_positions,
                        existing_open_qty_by_coin,
                        |q, p, fee| {
                            self.db
                                .promote_resting_trade_to_open(rt.trade_id, q, p, fee, 0.0)
                        },
                    )
                {
                    self.register_pending_fix4_pit_trueup(
                        rt.trade_id,
                        rt.exchange_order_id.as_deref(),
                        fee_usdc,
                    );
                    tracing::info!(
                        trade_id = rt.trade_id,
                        symbol = %rt.symbol,
                        fill_qty = fill_qty,
                        fill_px = fill_px,
                        "Promoted resting PIT order to OPEN via clearinghouseState cross-check during userFills indexer lag (Fix 4)"
                    );
                    resting_promoted_or_cancelled = true;
                } else if !is_simulated
                    && matches!(
                        self.db.cancel_resting_trade(
                            rt.trade_id,
                            "RESTING_ORDER_CANCELLED_OR_EXPIRED"
                        ),
                        Ok(true)
                    )
                {
                    tracing::info!(
                        trade_id = rt.trade_id,
                        symbol = %rt.symbol,
                        "Cancelled expired/gone resting PIT order during 15s exchange reconciliation (R9-N1)"
                    );
                    resting_promoted_or_cancelled = true;
                }
            }
        }
        if resting_promoted_or_cancelled {
            self.enforce_post_promotion_heat_cap(is_simulated);
        }
        resting_promoted_or_cancelled
    }

    /// Re-checks Book A (`PIT`) open portfolio heat after promoting resting orders (`HI-8`).
    /// If open portfolio heat exceeds `max_portfolio_heat_pct * pit_equity`, cancels any remaining
    /// `OPEN_ON_BOOK` resting PIT orders so delayed/blended resting fills cannot push heat above cap.
    pub fn enforce_post_promotion_heat_cap(&self, is_simulated: bool) -> usize {
        let cfg = self.load_config_logged("enforce_post_promotion_heat_cap");
        let max_heat_pct = cfg
            .get_f64("max_portfolio_heat_pct", 0.04)
            .clamp(0.01, 0.50);
        let initial_equity = cfg.get_f64("initial_equity_usdc", 10_000.0).max(50.0);
        let pit_equity = self
            .db
            .compute_pit_equity_and_idle_usdc()
            .map(|(eq, _)| eq)
            .unwrap_or(initial_equity)
            .max(50.0);
        let max_heat_usdc = pit_equity * max_heat_pct;
        let open_trades = self.db.get_open_trades().unwrap_or_default();
        let mut open_heat_usdc = 0.0;
        for t in &open_trades {
            let trade_heat = aegis_core::effective_position_open_heat_usdc(
                t.side.as_str(),
                t.entry_price,
                t.initial_stop_price,
                (t.chandelier_stop_price > 0.0).then_some(t.chandelier_stop_price),
                t.quantity,
                t.risk_budget_usdc,
                t.funding_accrued_usdc,
            );
            open_heat_usdc += trade_heat;
        }
        if open_heat_usdc <= max_heat_usdc + 1e-6 {
            return 0;
        }
        let resting = self.db.get_resting_pit_trades().unwrap_or_default();
        let mut cancelled = 0usize;
        for rt in resting {
            if !is_simulated {
                if let Some(oid_str) = rt.exchange_order_id.as_deref() {
                    let _ = self.cancel_resting_order(&rt.symbol, oid_str);
                }
            }
            if matches!(
                self.db
                    .cancel_resting_trade(rt.trade_id, "RESTING_CANCELLED_HEAT_CAP_EXCEEDED"),
                Ok(true)
            ) {
                cancelled += 1;
                tracing::warn!(
                    trade_id = rt.trade_id,
                    symbol = %rt.symbol,
                    open_heat_usdc = open_heat_usdc,
                    max_heat_usdc = max_heat_usdc,
                    "Cancelled remaining resting PIT order after post-promotion heat exceeded cap (HI-8)"
                );
                let _ = self.db.log_telemetry(
                    "WARN",
                    "engine.execution",
                    &format!(
                        "RESTING_FILL_HEAT_CAP_ENFORCED: cancelled resting PIT order #{} ({}) because open heat ${:.2} > cap ${:.2}",
                        rt.trade_id, rt.symbol, open_heat_usdc, max_heat_usdc
                    ),
                    Some(serde_json::json!({
                        "event": "RESTING_FILL_HEAT_CAP_ENFORCED",
                        "trade_id": rt.trade_id,
                        "symbol": rt.symbol,
                        "open_heat_usdc": open_heat_usdc,
                        "max_heat_usdc": max_heat_usdc,
                    })),
                );
            }
        }
        cancelled
    }

    /// Polls and reconciles off-book `ARMED` Pond cells during the 15s exchange reconciliation tick (`Fix 4`, `R13-QA`).
    /// If a resting Pond entry order is no longer on `openOrders` and has filled (via `userFills` or
    /// `clearinghouseState` cross-check during `userFills` indexer lag), promotes the `ARMED` Pond cell
    /// to `OPEN` with its original `initial_stop` and `chandelier_stop` preserved instead of allowing it
    /// to be imported as a degraded Book A orphan.
    pub(crate) fn poll_armed_pond_cells_on_15s_tick(
        &self,
        armed_cells: &[aegis_core::PondCell],
        open_order_oids: &HashSet<u64>,
        ex_positions: &HashMap<String, (f64, f64, f64, f64)>,
        existing_open_qty_by_coin: &mut HashMap<String, f64>,
        maker_fee_bps: f64,
        is_simulated: bool,
    ) -> bool {
        let mut armed_promoted = false;
        for cell in armed_cells {
            let resting_oid = cell.resting_order_id.as_deref().or_else(|| {
                cell.exit_reason
                    .as_deref()
                    .and_then(|r| r.strip_prefix("RESTING_OID:"))
            });
            let oid_num = resting_oid
                .and_then(|s| s.strip_prefix("hl-").or(Some(s)))
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|&id| id > 0);
            if oid_num
                .map(|id| open_order_oids.contains(&id))
                .unwrap_or(false)
            {
                continue;
            }
            let r_status = if is_simulated {
                crate::RestingOrderStatus {
                    state: "GONE".to_string(),
                    filled_units: 0.0,
                    entry_price: 0.0,
                    fee_usdc: 0.0,
                }
            } else {
                self.check_resting_order_status_with_open_orders(resting_oid, true)
            };
            let coin = cell
                .symbol
                .split('/')
                .next()
                .unwrap_or(&cell.symbol)
                .trim()
                .to_uppercase();
            let cell_qty = cell.quantity.unwrap_or(0.0).abs();
            let cell_limit_px = cell.entry_price.unwrap_or(0.0);
            if r_status.state == "FILLED" && (r_status.filled_units > 0.0 || cell_qty > 0.0) {
                let fill_px = if r_status.entry_price > 0.0 {
                    r_status.entry_price
                } else {
                    cell_limit_px
                };
                if fill_px <= 0.0 {
                    tracing::warn!(
                        cell_id = cell.cell_id,
                        symbol = %cell.symbol,
                        "Rejecting ARMED Pond cell promotion because fill_px <= 0.0 (R13-N4)"
                    );
                    continue;
                }
                let fill_qty = if r_status.filled_units > 0.0 {
                    r_status.filled_units
                } else {
                    cell_qty
                };
                let fee_usdc = if r_status.fee_usdc > 0.0 {
                    round_dp(r_status.fee_usdc, 4)
                } else {
                    round_dp(fill_qty * fill_px * (maker_fee_bps / 10_000.0), 4)
                };
                if matches!(
                    self.db.promote_armed_pond_cell_to_open(
                        cell.cell_id,
                        fill_qty,
                        fill_px,
                        fee_usdc
                    ),
                    Ok(true)
                ) {
                    *existing_open_qty_by_coin.entry(coin).or_insert(0.0) += fill_qty.abs();
                    tracing::info!(
                        cell_id = cell.cell_id,
                        symbol = %cell.symbol,
                        fill_qty = fill_qty,
                        fill_px = fill_px,
                        "Promoted filled ARMED Pond cell to OPEN during 15s exchange reconciliation (Fix 4)"
                    );
                    armed_promoted = true;
                }
            } else if r_status.state == "GONE" {
                if let Some((fill_qty, fill_px, fee_usdc)) = self
                    .try_promote_gone_via_clearinghouse(
                        &coin,
                        &cell.side,
                        cell_qty,
                        cell_limit_px,
                        maker_fee_bps,
                        ex_positions,
                        existing_open_qty_by_coin,
                        |q, p, fee| {
                            self.db
                                .promote_armed_pond_cell_to_open(cell.cell_id, q, p, fee)
                        },
                    )
                {
                    self.register_pending_fix4_pond_trueup(cell.cell_id, resting_oid, fee_usdc);
                    tracing::info!(
                        cell_id = cell.cell_id,
                        symbol = %cell.symbol,
                        fill_qty = fill_qty,
                        fill_px = fill_px,
                        "Promoted ARMED Pond cell to OPEN via clearinghouseState cross-check during userFills indexer lag (Fix 4)"
                    );
                    armed_promoted = true;
                }
            }
        }
        armed_promoted
    }

    /// Queries Hyperliquid `clearinghouseState` to check if an unattributed on-chain position
    /// exists on the same side for `symbol` (`Fix 4`, `R13-N1`, `R13-N4(a)`).
    /// Used when `check_resting_order_status` returns `"GONE"` during `userFills` indexer lag
    /// so filled resting PIT orders and `ARMED` Pond cells promote to `OPEN` instead of being
    /// cancelled/rejected and re-imported as degraded orphans.
    pub fn check_unattributed_onchain_position_for_symbol(
        &self,
        symbol: &str,
        side: &str,
        resting_limit_px: f64,
    ) -> Option<(f64, f64)> {
        let cfg = self.load_config_logged("check_unattributed_onchain_position_for_symbol");
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        if mode == "PAPER" {
            return None;
        }
        let wallet = cfg.get_str("wallet_address", "");
        if wallet.trim().is_empty() {
            return None;
        }
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
        let target_coin = symbol
            .split('/')
            .next()
            .unwrap_or(symbol)
            .trim()
            .to_uppercase();
        let ch = self.fetch_hyperliquid_clearinghouse_state(&wallet, is_testnet)?;
        let asset_positions = ch.get("assetPositions")?.as_array()?;
        let mut found_szi = 0.0;
        let mut found_entry_px = 0.0;
        for item in asset_positions {
            let pos = item.get("position").unwrap_or(item);
            let coin = pos
                .get("coin")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_uppercase();
            if coin == target_coin {
                found_szi = pos
                    .get("szi")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                found_entry_px = pos
                    .get("entryPx")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                break;
            }
        }
        let same_side = (side.eq_ignore_ascii_case("LONG") && found_szi > 0.0)
            || (side.eq_ignore_ascii_case("SHORT") && found_szi < 0.0);
        if !same_side || found_szi.abs() <= 1e-6 {
            return None;
        }
        let mut covered_qty = 0.0;
        if let Ok(open_trades) = self.db.get_open_trades() {
            for t in open_trades {
                if t.base_asset.eq_ignore_ascii_case(&target_coin) {
                    covered_qty += t.quantity.abs();
                }
            }
        }
        if let Ok(open_cells) = self.db.get_open_pond_cells() {
            for c in open_cells {
                let c_coin = c.symbol.split('/').next().unwrap_or(&c.symbol).trim();
                if c_coin.eq_ignore_ascii_case(&target_coin) {
                    covered_qty += c.quantity.unwrap_or(0.0).abs();
                }
            }
        }
        let uncovered = (found_szi.abs() - covered_qty).max(0.0);
        if uncovered > 1e-6 {
            let bounded_px = Self::resolve_fix4_clearinghouse_fill_price(
                side,
                resting_limit_px,
                found_entry_px,
                covered_qty,
            );
            (bounded_px > 0.0).then_some((uncovered, bounded_px))
        } else {
            None
        }
    }

    /// Polls and reconciles resting Book A (`PIT`) orders at the start of a PIT execution cycle (`NH-4`, `R3-H2`, `R3-H4`, `R3-H16`, `R9-N3`, `R9-QB`, `Fix 4`).
    pub(crate) fn reconcile_resting_pit_trades_for_cycle(
        &self,
        freeze_entries: bool,
        is_backtest: bool,
        maker_fee_bps: f64,
    ) {
        if !is_backtest {
            let _ = self.true_up_pending_fix4_promotions(None);
        }
        let resting_trades = self.db.get_resting_pit_trades().unwrap_or_default();
        for r_trade in resting_trades {
            let mut r_status =
                self.check_resting_order_status(r_trade.exchange_order_id.as_deref());
            if r_status.state == "OPEN_ON_BOOK" && !freeze_entries && !is_backtest {
                if let Some(oid_str) = r_trade.exchange_order_id.as_deref() {
                    let cancel_confirmed = match self.cancel_resting_order(&r_trade.symbol, oid_str)
                    {
                        Ok(confirmed) => confirmed,
                        Err(e) => {
                            tracing::warn!(
                                symbol = %r_trade.symbol,
                                trade_id = r_trade.trade_id,
                                oid = %oid_str,
                                error = %e,
                                "cancel_resting_order returned Err during resting PIT trade reconciliation (R10-N3)"
                            );
                            false
                        }
                    };
                    r_status = self.verify_post_cancel_resting_order(
                        &r_trade.symbol,
                        oid_str,
                        cancel_confirmed,
                    );
                }
            }
            let mut promoted_via_clearinghouse = false;
            if r_status.state == "GONE" && !is_backtest {
                if let Some((uncovered_qty, ex_entry_px)) = self
                    .check_unattributed_onchain_position_for_symbol(
                        &r_trade.symbol,
                        &r_trade.side,
                        r_trade.entry_price,
                    )
                {
                    r_status.state = "FILLED".to_string();
                    r_status.filled_units = if r_trade.quantity > 0.0 {
                        uncovered_qty.min(r_trade.quantity)
                    } else {
                        uncovered_qty
                    };
                    if ex_entry_px > 0.0 {
                        r_status.entry_price = ex_entry_px;
                    }
                    promoted_via_clearinghouse = true;
                }
            }
            if r_status.state == "FILLED" {
                let fill_px = if r_status.entry_price > 0.0 {
                    r_status.entry_price
                } else {
                    r_trade.entry_price
                };
                if fill_px <= 0.0 {
                    continue;
                }
                let fill_qty = if r_status.filled_units > 0.0 {
                    r_status.filled_units
                } else {
                    r_trade.quantity
                };
                let fee_usdc = if r_status.fee_usdc > 0.0 {
                    round_dp(r_status.fee_usdc, 4)
                } else {
                    round_dp(fill_qty * fill_px * (maker_fee_bps / 10_000.0), 4)
                };
                match self.db.promote_resting_trade_to_open(
                    r_trade.trade_id,
                    fill_qty,
                    fill_px,
                    fee_usdc,
                    0.0,
                ) {
                    Ok(true) => {
                        if promoted_via_clearinghouse {
                            self.register_pending_fix4_pit_trueup(
                                r_trade.trade_id,
                                r_trade.exchange_order_id.as_deref(),
                                fee_usdc,
                            );
                        }
                    }
                    Ok(false) => {}
                    Err(e) => {
                        tracing::error!(
                            trade_id = r_trade.trade_id,
                            error = %e,
                            "Failed to promote resting PIT trade to OPEN (R3-H2)"
                        );
                    }
                }
            } else if r_status.state == "GONE" {
                if let Err(e) = self
                    .db
                    .cancel_resting_trade(r_trade.trade_id, "RESTING_ORDER_CANCELLED_OR_EXPIRED")
                {
                    tracing::error!(
                        trade_id = r_trade.trade_id,
                        error = %e,
                        "Failed to cancel expired resting PIT trade"
                    );
                }
            }
        }
        let _ = self.enforce_post_promotion_heat_cap(is_backtest);
    }
}
