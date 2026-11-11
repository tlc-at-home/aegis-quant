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
                tracing::warn!(
                    trade_id = trade.trade_id,
                    symbol = %trade.symbol,
                    "Skipping hourly funding accrual: no on-chain or 1h bar funding rate available"
                );
                continue;
            };
            let mark_px = bars
                .last()
                .map(|b| b.close)
                .unwrap_or(trade.current_price);
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
                    tracing::error!(
                        trade_id = trade.trade_id,
                        error = %e,
                        "Failed to accrue open trade funding (R4-N4)"
                    );
                }
            }
        }

        *self.last_funding_sync_hour_ts.lock() = Some(hour_bucket);

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

    /// Polls resting Book A (`PIT`) orders (`status = 'PENDING'` / `fsm_state = 'OPEN_ON_BOOK'`)
    /// that are no longer present in the 15s `openOrders` snapshot, promoting filled orders to
    /// `OPEN` or cancelling expired orders immediately (`R9-N1`, `R9-N3`, `R9-QB`).
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
            let r_status = self.check_resting_order_status_with_open_orders(
                rt.exchange_order_id.as_deref(),
                true,
            );
            if r_status.state == "FILLED" {
                let fill_px = if r_status.entry_price > 0.0 {
                    r_status.entry_price
                } else {
                    rt.entry_price
                };
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
                let on_chain_fill = ex_positions.get(&coin).and_then(|&(szi, ex_entry_px, _, _)| {
                    let same_side = (rt.side == "LONG" && szi > 0.0)
                        || (rt.side == "SHORT" && szi < 0.0);
                    let already_covered = existing_open_qty_by_coin
                        .get(&coin)
                        .copied()
                        .unwrap_or(0.0);
                    let uncovered = (szi.abs() - already_covered).max(0.0);
                    if same_side && uncovered > 1e-6 {
                        Some((uncovered, ex_entry_px))
                    } else {
                        None
                    }
                });
                if let Some((uncovered_qty, ex_entry_px)) = on_chain_fill {
                    let fill_px = if ex_entry_px > 0.0 {
                        ex_entry_px
                    } else {
                        rt.entry_price
                    };
                    let fill_qty = if rt.quantity > 0.0 {
                        uncovered_qty.min(rt.quantity)
                    } else {
                        uncovered_qty
                    };
                    let fee_usdc = round_dp(fill_qty * fill_px * (maker_fee_bps / 10_000.0), 4);
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
                            "Promoted resting PIT order to OPEN via clearinghouseState cross-check during userFills indexer lag (Fix 4)"
                        );
                        resting_promoted_or_cancelled = true;
                    }
                } else if matches!(
                    self.db.cancel_resting_trade(
                        rt.trade_id,
                        "RESTING_ORDER_CANCELLED_OR_EXPIRED"
                    ),
                    Ok(true)
                ) {
                    tracing::info!(
                        trade_id = rt.trade_id,
                        symbol = %rt.symbol,
                        "Cancelled expired/gone resting PIT order during 15s exchange reconciliation (R9-N1)"
                    );
                    resting_promoted_or_cancelled = true;
                }
            }
        }
        resting_promoted_or_cancelled
    }

    /// Polls and reconciles off-book `ARMED` Pond cells during the 15s exchange reconciliation tick (`Fix 4`).
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
            let r_status = self.check_resting_order_status_with_open_orders(resting_oid, true);
            let coin = cell
                .symbol
                .split('/')
                .next()
                .unwrap_or(&cell.symbol)
                .trim()
                .to_uppercase();
            let cell_qty = cell.quantity.unwrap_or(0.0).abs();
            if r_status.state == "FILLED" && (r_status.filled_units > 0.0 || cell_qty > 0.0) {
                let fill_px = if r_status.entry_price > 0.0 {
                    r_status.entry_price
                } else {
                    cell.entry_price.unwrap_or(100.0)
                };
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
                let on_chain_fill = ex_positions.get(&coin).and_then(|&(szi, ex_entry_px, _, _)| {
                    let same_side = (cell.side == "LONG" && szi > 0.0)
                        || (cell.side == "SHORT" && szi < 0.0);
                    let already_covered = existing_open_qty_by_coin
                        .get(&coin)
                        .copied()
                        .unwrap_or(0.0);
                    let uncovered = (szi.abs() - already_covered).max(0.0);
                    if same_side && uncovered > 1e-6 {
                        Some((uncovered, ex_entry_px))
                    } else {
                        None
                    }
                });
                if let Some((uncovered_qty, ex_entry_px)) = on_chain_fill {
                    let fill_px = if ex_entry_px > 0.0 {
                        ex_entry_px
                    } else {
                        cell.entry_price.unwrap_or(100.0)
                    };
                    let fill_qty = if cell_qty > 0.0 {
                        uncovered_qty.min(cell_qty)
                    } else {
                        uncovered_qty
                    };
                    let fee_usdc = round_dp(fill_qty * fill_px * (maker_fee_bps / 10_000.0), 4);
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
                            "Promoted ARMED Pond cell to OPEN via clearinghouseState cross-check during userFills indexer lag (Fix 4)"
                        );
                        armed_promoted = true;
                    }
                }
            }
        }
        armed_promoted
    }

    /// Queries Hyperliquid `clearinghouseState` to check if an unattributed on-chain position
    /// exists on the same side for `symbol` (`Fix 4`).
    /// Used when `check_resting_order_status` returns `"GONE"` during `userFills` indexer lag
    /// so filled resting PIT orders and `ARMED` Pond cells promote to `OPEN` instead of being
    /// cancelled/rejected and re-imported as degraded orphans.
    pub fn check_unattributed_onchain_position_for_symbol(
        &self,
        symbol: &str,
        side: &str,
    ) -> Option<(f64, f64)> {
        let cfg = self
            .load_config_logged("check_unattributed_onchain_position_for_symbol");
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
            Some((uncovered, found_entry_px))
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
        let resting_trades = self.db.get_resting_pit_trades().unwrap_or_default();
        for r_trade in resting_trades {
            let mut r_status =
                self.check_resting_order_status(r_trade.exchange_order_id.as_deref());
            if r_status.state == "OPEN_ON_BOOK" && !freeze_entries && !is_backtest {
                if let Some(oid_str) = r_trade.exchange_order_id.as_deref() {
                    let cancel_confirmed =
                        match self.cancel_resting_order(&r_trade.symbol, oid_str) {
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
            if r_status.state == "GONE" && !is_backtest {
                if let Some((uncovered_qty, ex_entry_px)) =
                    self.check_unattributed_onchain_position_for_symbol(&r_trade.symbol, &r_trade.side)
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
                }
            }
            if r_status.state == "FILLED" {
                let fill_px = if r_status.entry_price > 0.0 {
                    r_status.entry_price
                } else {
                    r_trade.entry_price
                };
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
                if let Err(e) = self.db.promote_resting_trade_to_open(
                    r_trade.trade_id,
                    fill_qty,
                    fill_px,
                    fee_usdc,
                    0.0,
                ) {
                    tracing::error!(
                        trade_id = r_trade.trade_id,
                        error = %e,
                        "Failed to promote resting PIT trade to OPEN (R3-H2)"
                    );
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
    }
}
