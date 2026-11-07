use std::collections::{HashMap, HashSet};

use aegis_core::{round_dp, PortfolioSnapshot, Result, SignalEvaluation, TradeRecord, UniverseAsset};
use aegis_risk::RiskEngine;
use serde_json::{json, Value};

use crate::ExecutionEngine;

impl ExecutionEngine {
    /// Accrues or reconciles hourly funding payments across open PIT perpetual positions
    /// and open Pond cells (`NM-4`).
    pub fn sync_funding_payments(&self, current_ts_ms: Option<i64>, is_backtest: bool) -> Value {
        let now_ms = current_ts_ms.unwrap_or_else(|| self.db.now_ms());
        let hour_bucket = (now_ms / 3_600_000) * 3_600_000;
        if *self.last_funding_sync_hour_ts.lock() == Some(hour_bucket) {
            return json!({
                "status": "ALREADY_SYNCED_THIS_HOUR",
                "hour_bucket_ms": hour_bucket,
                "positions_updated": 0,
            });
        }

        let open_trades = match self.db.get_open_trades() {
            Ok(trades) => trades,
            Err(e) => {
                tracing::error!(error = %e, "Failed to query open trades during sync_funding_payments");
                return json!({
                    "status": "ERROR",
                    "reason": e.to_string(),
                    "hour_bucket_ms": hour_bucket,
                    "positions_updated": 0,
                });
            }
        };
        let open_pond_cells = if is_backtest {
            Vec::new()
        } else {
            match self.db.get_open_pond_cells() {
                Ok(cells) => cells,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to query open pond cells during sync_funding_payments");
                    return json!({
                        "status": "ERROR",
                        "reason": e.to_string(),
                        "hour_bucket_ms": hour_bucket,
                        "positions_updated": 0,
                    });
                }
            }
        };

        if open_trades.is_empty() && open_pond_cells.is_empty() {
            *self.last_funding_sync_hour_ts.lock() = Some(hour_bucket);
            return json!({
                "status": "NO_OPEN_POSITIONS",
                "hour_bucket_ms": hour_bucket,
                "positions_updated": 0,
            });
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
            let delta = if trade.side == "LONG" {
                -notional * funding_rate
            } else {
                notional * funding_rate
            };
            let new_funding = round_dp(trade.funding_accrued_usdc + delta, 4);
            if let Err(e) = self.db.update_open_trade(
                trade.trade_id,
                mark_px,
                trade.chandelier_stop_price,
                trade.highest_price_seen,
                trade.lowest_price_seen,
                trade.unrealized_pnl_usdc,
                new_funding,
            ) {
                tracing::error!(
                    trade_id = trade.trade_id,
                    error = %e,
                    "Failed to update open trade funding"
                );
            }
            net_funding_delta += delta;
            updated += 1;
        }

        for cell in open_pond_cells {
            let qty = cell.quantity.unwrap_or(0.0).abs();
            if qty <= 0.0 {
                continue;
            }
            let bars = self
                .db
                .get_ohlcv_bars(&cell.symbol, "1h", 2, Some(now_ms))
                .unwrap_or_default();
            let coin = cell
                .symbol
                .split('/')
                .next()
                .unwrap_or(&cell.symbol)
                .trim()
                .to_uppercase();
            let Some(funding_rate) = onchain_rate_by_coin
                .get(&coin)
                .copied()
                .or_else(|| bars.last().map(|b| b.funding_rate_hourly))
                .or(cell.funding_rate_entry)
            else {
                tracing::warn!(
                    cell_id = cell.cell_id,
                    symbol = %cell.symbol,
                    "Skipping pond cell hourly funding accrual: no on-chain or 1h bar funding rate available"
                );
                continue;
            };
            let mark_px = bars
                .last()
                .map(|b| b.close)
                .or(cell.entry_price)
                .unwrap_or(0.0);
            if mark_px <= 0.0 {
                continue;
            }
            let notional = qty * mark_px;
            let is_long = cell.side.eq_ignore_ascii_case("LONG")
                || cell.side.eq_ignore_ascii_case("BUY");
            let delta = if is_long {
                -notional * funding_rate
            } else {
                notional * funding_rate
            };
            let new_funding = round_dp(cell.funding_accrued_usdc + delta, 4);
            if let Err(e) = self.db.update_pond_cell_funding(cell.cell_id, new_funding) {
                tracing::error!(
                    cell_id = cell.cell_id,
                    error = %e,
                    "Failed to update open pond cell funding"
                );
            }
            net_funding_delta += delta;
            updated += 1;
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

        json!({
            "status": "SYNCED",
            "hour_bucket_ms": hour_bucket,
            "positions_updated": updated,
            "net_funding_delta_usdc": round_dp(net_funding_delta, 4),
        })
    }

    /// Reconciles local SQLite `OPEN` trades and Pond cells against Hyperliquid L1
    /// `clearinghouseState` and `openOrders` (`NM-5`, `NM-10`).
    pub fn reconcile_exchange_state(
        &self,
        simulated_exchange_positions: Option<&HashMap<String, Value>>,
        simulated_open_orders: Option<&[Value]>,
    ) -> Value {
        let cfg = self.load_config_logged("reconcile_exchange_state");
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
        let wallet = cfg.get_str("wallet_address", "");

        if mode == "PAPER"
            && simulated_exchange_positions.is_none()
            && simulated_open_orders.is_none()
        {
            let open_trades = self.db.get_open_trades().unwrap_or_default();
            return json!({
                "status": "VERIFIED_PAPER",
                "mode": "PAPER",
                "local_open_trades": open_trades.len(),
                "discrepancies_found": 0,
                "discrepancies": [],
            });
        }

        let mut ex_positions: HashMap<String, (f64, f64, f64, f64)> = HashMap::new();
        if let Some(sim_map) = simulated_exchange_positions {
            for (coin, v) in sim_map {
                let szi = v
                    .get("szi")
                    .and_then(|val| {
                        val.as_f64()
                            .or_else(|| val.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                if szi.abs() > 1e-9 {
                    let entry_px = v
                        .get("entryPx")
                        .and_then(|val| {
                            val.as_f64()
                                .or_else(|| val.as_str().and_then(|s| s.parse::<f64>().ok()))
                        })
                        .unwrap_or(100.0);
                    let mark_px = v
                        .get("markPx")
                        .and_then(|val| {
                            val.as_f64()
                                .or_else(|| val.as_str().and_then(|s| s.parse::<f64>().ok()))
                        })
                        .unwrap_or(entry_px);
                    let upnl = v
                        .get("unrealizedPnl")
                        .and_then(|val| {
                            val.as_f64()
                                .or_else(|| val.as_str().and_then(|s| s.parse::<f64>().ok()))
                        })
                        .unwrap_or(0.0);
                    ex_positions.insert(coin.to_uppercase(), (szi, entry_px, mark_px, upnl));
                }
            }
        } else if let Some(ch) = self.fetch_hyperliquid_clearinghouse_state(&wallet, is_testnet) {
            if let Some(asset_positions) = ch.get("assetPositions").and_then(Value::as_array) {
                for item in asset_positions {
                    let pos = item.get("position").unwrap_or(item);
                    let coin = pos
                        .get("coin")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_uppercase();
                    let szi = pos
                        .get("szi")
                        .and_then(|v| {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                        })
                        .unwrap_or(0.0);
                    if !coin.is_empty() && szi.abs() > 1e-9 {
                        let entry_px = pos
                            .get("entryPx")
                            .and_then(|v| {
                                v.as_f64()
                                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                            })
                            .unwrap_or(0.0);
                        let upnl = pos
                            .get("unrealizedPnl")
                            .and_then(|v| {
                                v.as_f64()
                                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                            })
                            .unwrap_or(0.0);
                        ex_positions.insert(coin, (szi, entry_px, entry_px, upnl));
                    }
                }
            } else {
                return json!({
                    "status": "SKIPPED_OFFLINE",
                    "reason": "CLEARINGHOUSE_STATE_UNAVAILABLE",
                    "discrepancies_found": 0,
                });
            }
        } else {
            return json!({
                "status": "SKIPPED_OFFLINE",
                "reason": "CLEARINGHOUSE_API_UNREACHABLE",
                "discrepancies_found": 0,
            });
        }

        let live_open_orders: Vec<Value>;
        let open_orders_slice: Option<&[Value]> = if let Some(sim_orders) = simulated_open_orders {
            Some(sim_orders)
        } else if simulated_exchange_positions.is_none() {
            live_open_orders = self
                .fetch_hyperliquid_open_orders(&wallet, is_testnet)
                .unwrap_or_default();
            Some(&live_open_orders)
        } else {
            None
        };
        let open_order_coins: HashSet<String> = open_orders_slice
            .unwrap_or(&[])
            .iter()
            .filter_map(|ord| ord.get("coin").and_then(Value::as_str))
            .map(|s| s.to_uppercase())
            .collect();

        let local_trades = match self.db.get_open_trades() {
            Ok(trades) => trades,
            Err(e) => {
                tracing::error!(error = %e, "Aborting reconcile_exchange_state: failed to load open trades");
                return json!({
                    "status": "ERROR_DB_UNAVAILABLE",
                    "reason": e.to_string(),
                    "discrepancies_found": 0,
                });
            }
        };
        let resting_trades = match self.db.get_resting_pit_trades() {
            Ok(trades) => trades,
            Err(e) => {
                tracing::error!(error = %e, "Aborting reconcile_exchange_state: failed to load resting trades");
                return json!({
                    "status": "ERROR_DB_UNAVAILABLE",
                    "reason": e.to_string(),
                    "discrepancies_found": 0,
                });
            }
        };
        let local_pond_cells = match self.db.get_open_pond_cells() {
            Ok(cells) => cells,
            Err(e) => {
                tracing::error!(error = %e, "Aborting reconcile_exchange_state: failed to load open pond cells");
                return json!({
                    "status": "ERROR_DB_UNAVAILABLE",
                    "reason": e.to_string(),
                    "discrepancies_found": 0,
                });
            }
        };
        let all_pond_cells = self.db.get_pond_cells(200).unwrap_or_default();

        // Aggregate local quantities per coin across both books (NM-5, NM-10)
        let mut pit_qty_by_coin: HashMap<String, f64> = HashMap::new();
        for lt in &local_trades {
            *pit_qty_by_coin
                .entry(lt.base_asset.to_uppercase())
                .or_insert(0.0) += lt.quantity.abs();
        }
        let mut pond_qty_by_coin: HashMap<String, f64> = HashMap::new();
        for cell in &local_pond_cells {
            let coin = cell
                .symbol
                .split('/')
                .next()
                .unwrap_or(&cell.symbol)
                .trim()
                .to_uppercase();
            *pond_qty_by_coin.entry(coin).or_insert(0.0) += cell.quantity.unwrap_or(0.0).abs();
        }

        let mut seen_coins: HashSet<String> = HashSet::new();
        for rt in &resting_trades {
            seen_coins.insert(rt.base_asset.to_uppercase());
        }
        for c in &all_pond_cells {
            if c.state == "OPEN_ON_BOOK" {
                let coin = c
                    .symbol
                    .split('/')
                    .next()
                    .unwrap_or(&c.symbol)
                    .trim()
                    .to_uppercase();
                seen_coins.insert(coin);
            }
        }

        let mut discrepancies: Vec<Value> = Vec::new();

        for lt in &local_trades {
            let coin = lt.base_asset.to_uppercase();
            seen_coins.insert(coin.clone());
            let pond_co_qty = pond_qty_by_coin.get(&coin).copied().unwrap_or(0.0);

            match ex_positions.get(&coin) {
                None => {
                    if !open_order_coins.contains(&coin) {
                        if let Err(e) = self.db.close_trade(
                            lt.trade_id,
                            lt.current_price,
                            0.0,
                            lt.fees_paid_usdc,
                            "RECONCILED_CLOSED_ON_EXCHANGE",
                        ) {
                            tracing::error!(
                                trade_id = lt.trade_id,
                                error = %e,
                                "Failed to close phantom local trade"
                            );
                        }
                        discrepancies.push(json!({
                            "type": "PHANTOM_LOCAL_CLOSED",
                            "base_asset": coin,
                            "trade_id": lt.trade_id,
                        }));
                    }
                }
                Some(&(szi, entry_px, mark_px, upnl)) => {
                    let ex_side = if szi > 0.0 { "LONG" } else { "SHORT" };
                    if !lt.side.eq_ignore_ascii_case(ex_side) && pond_co_qty <= 0.0 {
                        if let Err(e) = self.db.close_trade(
                            lt.trade_id,
                            mark_px,
                            0.0,
                            lt.fees_paid_usdc,
                            "RECONCILED_DIRECTION_FLIP",
                        ) {
                            tracing::error!(
                                trade_id = lt.trade_id,
                                error = %e,
                                "Failed to close flipped local trade"
                            );
                        }
                        let _ = self.db.reconcile_on_chain_orphan_position(
                            &lt.symbol,
                            &coin,
                            ex_side,
                            szi.abs(),
                            entry_px,
                            mark_px,
                            upnl,
                        );
                        discrepancies.push(json!({
                            "type": "DIRECTION_FLIP_RECONCILED",
                            "base_asset": coin,
                            "trade_id": lt.trade_id,
                            "local_side": lt.side,
                            "exchange_side": ex_side,
                            "exchange_qty": szi.abs(),
                        }));
                        continue;
                    }
                    let ex_qty = szi.abs();
                    let expected_pit_qty = (ex_qty - pond_co_qty).max(0.0);
                    if (expected_pit_qty - lt.quantity.abs()).abs() > 1e-6 && expected_pit_qty > 0.0
                    {
                        if let Err(e) = self.db.reconcile_open_trade_size(
                            lt.trade_id,
                            expected_pit_qty,
                            Some(entry_px),
                        ) {
                            tracing::error!(
                                trade_id = lt.trade_id,
                                error = %e,
                                "Failed to reconcile open trade size"
                            );
                        }
                        discrepancies.push(json!({
                            "type": "SIZE_DRIFT_RECONCILED",
                            "base_asset": coin,
                            "trade_id": lt.trade_id,
                            "local_qty": lt.quantity,
                            "exchange_qty": expected_pit_qty,
                        }));
                    }
                }
            }
        }

        for cell in &local_pond_cells {
            let coin = cell
                .symbol
                .split('/')
                .next()
                .unwrap_or(&cell.symbol)
                .trim()
                .to_uppercase();
            seen_coins.insert(coin.clone());
            let pit_co_qty = pit_qty_by_coin.get(&coin).copied().unwrap_or(0.0);

            match ex_positions.get(&coin) {
                None => {
                    if !open_order_coins.contains(&coin) {
                        if let Err(e) = self.db.reconcile_close_phantom_pond_cell(
                            cell.cell_id,
                            cell.entry_price.unwrap_or(0.0),
                            "RECONCILED_CLOSED_ON_EXCHANGE",
                        ) {
                            tracing::error!(
                                cell_id = cell.cell_id,
                                error = %e,
                                "Failed to close phantom pond cell"
                            );
                        }
                        discrepancies.push(json!({
                            "type": "PHANTOM_POND_CELL_CLOSED",
                            "base_asset": coin,
                            "cell_id": cell.cell_id,
                        }));
                    }
                }
                Some(&(szi, entry_px, mark_px, upnl)) => {
                    let ex_side = if szi > 0.0 { "LONG" } else { "SHORT" };
                    let local_is_long = cell.side.eq_ignore_ascii_case("LONG")
                        || cell.side.eq_ignore_ascii_case("BUY");
                    let local_side_norm = if local_is_long { "LONG" } else { "SHORT" };
                    if local_side_norm != ex_side && pit_co_qty <= 0.0 {
                        if let Err(e) = self.db.reconcile_close_phantom_pond_cell(
                            cell.cell_id,
                            mark_px,
                            "RECONCILED_DIRECTION_FLIP",
                        ) {
                            tracing::error!(
                                cell_id = cell.cell_id,
                                error = %e,
                                "Failed to close flipped pond cell"
                            );
                        }
                        let _ = self.db.reconcile_on_chain_orphan_position(
                            &cell.symbol,
                            &coin,
                            ex_side,
                            szi.abs(),
                            entry_px,
                            mark_px,
                            upnl,
                        );
                        discrepancies.push(json!({
                            "type": "DIRECTION_FLIP_POND_CELL_RECONCILED",
                            "base_asset": coin,
                            "cell_id": cell.cell_id,
                            "local_side": cell.side,
                            "exchange_side": ex_side,
                            "exchange_qty": szi.abs(),
                        }));
                        continue;
                    }
                    let ex_qty = szi.abs();
                    let expected_pond_qty = (ex_qty - pit_co_qty).max(0.0);
                    let local_qty = cell.quantity.unwrap_or(0.0).abs();
                    if (expected_pond_qty - local_qty).abs() > 1e-6 && expected_pond_qty > 0.0 {
                        if let Err(e) = self.db.reconcile_open_pond_cell_size(
                            cell.cell_id,
                            expected_pond_qty,
                            Some(entry_px),
                        ) {
                            tracing::error!(
                                cell_id = cell.cell_id,
                                error = %e,
                                "Failed to reconcile pond cell size"
                            );
                        }
                        discrepancies.push(json!({
                            "type": "SIZE_DRIFT_POND_CELL_RECONCILED",
                            "base_asset": coin,
                            "cell_id": cell.cell_id,
                            "local_qty": local_qty,
                            "exchange_qty": expected_pond_qty,
                        }));
                    }
                }
            }
        }

        for (coin, &(szi, entry_px, mark_px, upnl)) in &ex_positions {
            if !seen_coins.contains(coin) {
                let side = if szi > 0.0 { "LONG" } else { "SHORT" };
                let symbol = format!("{coin}/USDC:USDC");
                if let Ok(tid) = self.db.reconcile_on_chain_orphan_position(
                    &symbol,
                    coin,
                    side,
                    szi.abs(),
                    entry_px,
                    mark_px,
                    upnl,
                ) {
                    discrepancies.push(json!({
                        "type": "ORPHAN_ON_CHAIN_IMPORTED",
                        "base_asset": coin,
                        "trade_id": tid,
                        "side": side,
                        "quantity": szi.abs(),
                    }));
                }
            }
        }

        if let Some(open_orders) = open_orders_slice {
            for ord in open_orders {
                let coin = ord
                    .get("coin")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_uppercase();
                if !ex_positions.contains_key(&coin) && !seen_coins.contains(&coin) {
                    if simulated_open_orders.is_none() {
                        if let Some(oid) = ord.get("oid").and_then(Value::as_u64) {
                            let _ = self.cancel_resting_order(&coin, &format!("hl-{oid}"));
                        }
                    }
                    discrepancies.push(json!({
                        "type": "DANGLING_OPEN_ORDER_CANCELLED",
                        "base_asset": coin,
                        "oid": ord.get("oid"),
                    }));
                }
            }
        }

        if !discrepancies.is_empty() {
            let _ = self.alerts.notify_reconciliation_mismatch(
                discrepancies.len(),
                &discrepancies,
                &wallet,
            );
        }

        json!({
            "status": "RECONCILED",
            "mode": mode,
            "discrepancies_found": discrepancies.len(),
            "discrepancies": discrepancies,
        })
    }

    /// Runs a complete execution & portfolio management cycle (`engine/execution.py::reconcile_cycle`).
    pub fn reconcile_cycle(
        &self,
        evaluations: &[SignalEvaluation],
        is_backtest: bool,
    ) -> Result<PortfolioSnapshot> {
        let freeze_entries = !is_backtest && self.db.is_entry_frozen();
        self.reconcile_cycle_with_options(evaluations, is_backtest, freeze_entries)
    }

    /// Runs a complete execution & portfolio management cycle with explicit `freeze_entries` control (`NH-4`, `NH-8`, `NH-10`).
    ///
    /// When `freeze_entries` is `true`, resting order reconciliation and Chandelier stops / regime
    /// exits still execute normally, while new directional entries are skipped (`NH-10`).
    pub fn reconcile_cycle_with_options(
        &self,
        evaluations: &[SignalEvaluation],
        is_backtest: bool,
        freeze_entries: bool,
    ) -> Result<PortfolioSnapshot> {
        let cfg = self.db.load_config()?;
        let initial_equity = cfg.get_f64("initial_equity_usdc", 10000.0);
        let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);
        let maker_fee_bps = cfg.get_f64("maker_fee_bps", 1.5);
        let chop_prob_thresh = cfg.get_f64("hmm_chop_prob_threshold", 0.55);
        let bear_flip_prob = cfg.get_f64("execution_regime_flip_bear_prob", 0.62);
        let bull_flip_prob = cfg.get_f64("execution_regime_flip_bull_prob", 0.60);
        let chop_conv_max = cfg.get_f64("execution_high_chop_conviction_max", 0.08);
        let chop_prob_offset = cfg.get_f64("execution_high_chop_prob_offset", 0.20);
        let chop_prob_cap = cfg.get_f64("execution_high_chop_prob_cap", 0.82);
        let rs_breakdown_long_snr = cfg.get_f64("execution_rs_breakdown_long_snr", -0.10);
        let rs_reversal_short_snr = cfg.get_f64("execution_rs_reversal_short_snr", 0.10);
        let cooldown_bars = cfg
            .get_i64(
                "execution_stop_cooldown_bars",
                cfg.get_i64("post_stop_cooldown_bars", 14),
            )
            .max(1);
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();

        let now_ms = evaluations
            .first()
            .map(|e| e.timestamp_ms)
            .unwrap_or_else(|| self.db.now_ms());

        let _ = self.sync_funding_payments(Some(now_ms), is_backtest);

        // 0. Poll resting PIT orders (`OPEN_ON_BOOK`) before evaluating stops (`NH-4`)
        let resting_trades = self.db.get_resting_pit_trades().unwrap_or_default();
        for r_trade in resting_trades {
            let r_status = self
                .check_resting_order_status(&r_trade.symbol, r_trade.exchange_order_id.as_deref());
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
                let fee_usdc = round_dp(fill_qty * fill_px * (maker_fee_bps / 10_000.0), 4);
                let _ = self.db.promote_resting_trade_to_open(
                    r_trade.trade_id,
                    fill_qty,
                    fill_px,
                    fee_usdc,
                    0.0,
                );
            } else if r_status.state == "GONE" {
                let _ = self
                    .db
                    .cancel_resting_trade(r_trade.trade_id, "RESTING_ORDER_CANCELLED_OR_EXPIRED");
            }
        }

        let usdc_assets = self.db.get_usdc_perp_universe()?;
        let asset_by_symbol: HashMap<String, UniverseAsset> = usdc_assets
            .into_iter()
            .map(|a| (a.symbol.clone(), a))
            .collect();
        let eval_by_symbol: HashMap<String, &SignalEvaluation> = evaluations
            .iter()
            .map(|e| (e.exec_symbol.clone(), e))
            .collect();

        let prev_snap = self.db.get_latest_portfolio_snapshot()?;
        let mut accrued_yield_usdc = prev_snap.accrued_yield_usdc;

        // 1. Evaluate open positions (Two-Tier Chandelier Stop + Regime Exit)
        let open_trades = self.db.get_open_trades()?;
        for trade in open_trades {
            let Some(asset) = asset_by_symbol.get(&trade.symbol) else {
                continue;
            };
            let bars =
                self.db
                    .get_ohlcv_bars(&trade.symbol, &asset.exec_timeframe, 30, Some(now_ms))?;
            if bars.is_empty() {
                continue;
            }
            let last_bar = &bars[bars.len() - 1];
            if (mode == "LIVE" || mode == "TESTNET") && !is_backtest && last_bar.is_synthetic != 0 {
                tracing::warn!(
                    "Skipping stop/exit evaluation for {} on synthetic bar (ts={})",
                    trade.symbol,
                    last_bar.timestamp_ms
                );
                continue;
            }
            let atr_period = cfg
                .get_usize("atr_period", cfg.get_usize("atr_default_period", 14))
                .max(2);
            let atr = self.risk.compute_atr(&bars, atr_period);

            let stop_eval = self.risk.evaluate_two_tier_chandelier_stop(
                &trade.side,
                trade.entry_price,
                trade.quantity,
                trade.chandelier_stop_price,
                trade.highest_price_seen,
                trade.lowest_price_seen,
                last_bar.high,
                last_bar.low,
                last_bar.close,
                atr,
                asset.tick_size,
                Some(&cfg),
            );

            let ev_opt = eval_by_symbol.get(&trade.symbol).copied();
            let mut exit_reason = stop_eval.exit_reason.clone();
            let mut exit_px = if stop_eval.triggered {
                stop_eval.exit_fill_price
            } else {
                last_bar.close
            };

            if exit_reason.is_none() {
                if let Some(ev) = ev_opt {
                    if trade.side == "LONG"
                        && ev.hmm_regime == "BEAR"
                        && ev.prob_bear >= bear_flip_prob
                    {
                        exit_reason = Some("REGIME_FLIP_BEAR".to_string());
                        exit_px = last_bar.close;
                    } else if trade.side == "LONG" && ev.snr_synth < rs_breakdown_long_snr {
                        exit_reason = Some("RS_SYNTH_BREAKDOWN_LONG".to_string());
                        exit_px = last_bar.close;
                    } else if trade.side == "SHORT"
                        && ev.hmm_regime == "BULL"
                        && ev.prob_bull >= bull_flip_prob
                    {
                        exit_reason = Some("REGIME_FLIP_BULL".to_string());
                        exit_px = last_bar.close;
                    } else if trade.side == "SHORT" && ev.snr_synth > rs_reversal_short_snr {
                        exit_reason = Some("RS_SYNTH_REVERSAL_SHORT".to_string());
                        exit_px = last_bar.close;
                    } else if ev.prob_chop
                        >= (chop_prob_thresh + chop_prob_offset).min(chop_prob_cap)
                        && ev.blended_conviction.abs() < chop_conv_max
                    {
                        exit_reason = Some("HIGH_CHOP_DELEVERAGE".to_string());
                        exit_px = last_bar.close;
                    }
                }
            }

            if let Some(reason) = exit_reason {
                let mut actual_exit_px = exit_px;
                let actual_fees;

                if (mode == "LIVE" || mode == "TESTNET") && !is_backtest {
                    let close_side = if trade.side == "LONG" {
                        "SHORT"
                    } else {
                        "LONG"
                    };
                    let exec_close = self.execute_maker_limit_chaser(
                        asset,
                        close_side,
                        trade.quantity,
                        last_bar.close,
                        last_bar.high,
                        last_bar.low,
                        last_bar.volume,
                        None,
                        None,
                        None,
                        true,
                    );
                    if exec_close.fsm_state == "FILLED"
                        && exec_close.filled_units > 0.0
                        && exec_close.avg_fill_price > 0.0
                    {
                        if exec_close.filled_units + 1e-8 < trade.quantity.abs() {
                            // Partial live exit fill (`NH-8`): reduce open trade and keep remaining units open
                            let remaining_qty =
                                (trade.quantity.abs() - exec_close.filled_units).max(0.0);
                            let remaining_notional = round_dp(remaining_qty * trade.entry_price, 4);
                            let partial_gross_pnl = if trade.side == "LONG" {
                                (exec_close.avg_fill_price - trade.entry_price)
                                    * exec_close.filled_units
                            } else {
                                (trade.entry_price - exec_close.avg_fill_price)
                                    * exec_close.filled_units
                            };
                            self.db.reduce_open_trade_partial_exit(
                                trade.trade_id,
                                remaining_qty,
                                remaining_notional,
                                round_dp(partial_gross_pnl, 4),
                                round_dp(exec_close.fees_paid_usdc, 4),
                                round_dp(exec_close.slippage_paid_usd, 4),
                                last_bar.close,
                            )?;
                            continue;
                        }
                        actual_exit_px = exec_close.avg_fill_price;
                        actual_fees =
                            round_dp(trade.fees_paid_usdc + exec_close.fees_paid_usdc, 4);
                    } else {
                        let upnl = if trade.side == "LONG" {
                            (last_bar.close - trade.entry_price) * trade.quantity
                        } else {
                            (trade.entry_price - last_bar.close) * trade.quantity
                        };
                        self.db.update_open_trade(
                            trade.trade_id,
                            last_bar.close,
                            stop_eval.chandelier_stop_price,
                            stop_eval.highest_price_seen,
                            stop_eval.lowest_price_seen,
                            round_dp(upnl, 4),
                            trade.funding_accrued_usdc,
                        )?;
                        continue;
                    }
                } else {
                    let exit_notional = trade.quantity * actual_exit_px;
                    let exit_fee = exit_notional * (taker_fee_bps / 10_000.0);
                    actual_fees = round_dp(trade.fees_paid_usdc + exit_fee, 4);
                }

                let gross_pnl = if trade.side == "LONG" {
                    (actual_exit_px - trade.entry_price) * trade.quantity
                } else {
                    (trade.entry_price - actual_exit_px) * trade.quantity
                };
                let net_pnl = round_dp(
                    gross_pnl + trade.realized_pnl_usdc - actual_fees + trade.funding_accrued_usdc,
                    4,
                );
                self.db.close_trade(
                    trade.trade_id,
                    actual_exit_px,
                    net_pnl,
                    actual_fees,
                    &reason,
                )?;
                if stop_eval.triggered {
                    let tf_ms = match asset.exec_timeframe.as_str() {
                        "1h" => 3_600_000,
                        "2h" => 7_200_000,
                        _ => 14_400_000,
                    };
                    self.stop_cooldown_until_ms
                        .lock()
                        .insert(trade.symbol.clone(), now_ms + cooldown_bars * tf_ms);
                    if !is_backtest {
                        let _ = self.alerts.notify_stop_out(
                            &trade.symbol,
                            &trade.side,
                            trade.entry_price,
                            actual_exit_px,
                            net_pnl,
                            &reason,
                        );
                    }
                }
            } else {
                let upnl = if trade.side == "LONG" {
                    (last_bar.close - trade.entry_price) * trade.quantity
                } else {
                    (trade.entry_price - last_bar.close) * trade.quantity
                };
                self.db.update_open_trade(
                    trade.trade_id,
                    last_bar.close,
                    stop_eval.chandelier_stop_price,
                    stop_eval.highest_price_seen,
                    stop_eval.lowest_price_seen,
                    round_dp(upnl, 4),
                    trade.funding_accrued_usdc,
                )?;
            }
        }

        // 2. Compute current equity & open trade state before new entries
        let mut active_trades = self.db.get_open_trades()?;
        let resting_after = self.db.get_resting_pit_trades().unwrap_or_default();
        let realized_pnl = self.db.get_total_realized_pnl()?;
        let unrealized_pnl: f64 = active_trades
            .iter()
            .map(|t| t.unrealized_pnl_usdc + t.funding_accrued_usdc)
            .sum();

        let two_book_active = !is_backtest
            && (cfg.get_bool("platform_pp50_enabled", false)
                || cfg.get_bool("pond_enabled", false));
        let split_pct = if two_book_active {
            cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90)
        } else {
            1.0
        };
        let net_pit_transfers = if two_book_active {
            self.db.get_net_book_transfer_adjustment("PIT").unwrap_or(0.0)
        } else {
            0.0
        };
        let raw_platform_eq = if !is_backtest
            && (mode == "LIVE" || mode == "TESTNET")
            && prev_snap.total_equity_usdc > 0.0
        {
            prev_snap.total_equity_usdc + unrealized_pnl
        } else {
            initial_equity + realized_pnl + unrealized_pnl + accrued_yield_usdc
        };
        let current_equity = if !is_backtest && (mode == "LIVE" || mode == "TESTNET") {
            (raw_platform_eq * split_pct + net_pit_transfers).max(0.0)
        } else {
            (raw_platform_eq * split_pct + net_pit_transfers).max(50.0)
        };

        let mut open_symbols: HashSet<String> =
            active_trades.iter().map(|t| t.symbol.clone()).collect();
        for rt in &resting_after {
            open_symbols.insert(rt.symbol.clone());
        }
        if !is_backtest {
            if let Ok(pond_syms) = self.db.get_open_pond_symbols() {
                open_symbols.extend(pond_syms);
            }
        }

        let (_wallet_tot_eq, perps_trading_eq, vault_eq, mut total_wallet_deployed) =
            if !is_backtest {
                self.db
                    .get_wallet_perps_margin_state()
                    .unwrap_or((initial_equity, initial_equity, 0.0, 0.0))
            } else {
                (initial_equity, initial_equity, 0.0, 0.0)
            };

        // 3. Evaluate new directional entries for LONG / SHORT candidates (skipped if entries frozen — NH-10)
        if !freeze_entries {
            let atr_period = cfg
                .get_usize("atr_period", cfg.get_usize("atr_default_period", 14))
                .max(2);
            let max_longs = cfg.get_usize("max_concurrent_long_positions", 2).max(1);
            let max_shorts = cfg.get_usize("max_concurrent_short_positions", 2).max(1);
            for ev in evaluations {
                if ev.signal_action != "LONG" && ev.signal_action != "SHORT" {
                    continue;
                }
                if open_symbols.contains(&ev.exec_symbol) {
                    continue;
                }
                if ev.signal_action == "LONG"
                    && active_trades.iter().filter(|t| t.side == "LONG").count() >= max_longs
                {
                    continue;
                }
                if ev.signal_action == "SHORT"
                    && active_trades.iter().filter(|t| t.side == "SHORT").count() >= max_shorts
                {
                    continue;
                }
                if let Some(&cooldown_until) =
                    self.stop_cooldown_until_ms.lock().get(&ev.exec_symbol)
                {
                    if now_ms < cooldown_until {
                        continue;
                    }
                }
                let Some(asset) = asset_by_symbol.get(&ev.exec_symbol) else {
                    continue;
                };
                let bars = self.db.get_ohlcv_bars(
                    &asset.symbol,
                    &asset.exec_timeframe,
                    30,
                    Some(now_ms),
                )?;
                if bars.is_empty() {
                    continue;
                }
                let last_bar = &bars[bars.len() - 1];
                let atr = self.risk.compute_atr(&bars, atr_period);

                let mut sizing = self.risk.evaluate_order_risk(
                    asset,
                    &ev.signal_action,
                    last_bar.close,
                    atr,
                    current_equity,
                    Some(&active_trades),
                    ev.blended_conviction,
                    0.0,
                );
                if !sizing.approved {
                    continue;
                }

                if !is_backtest && vault_eq > 0.0 {
                    let remaining_perps_cap = (perps_trading_eq - total_wallet_deployed).max(0.0);
                    if remaining_perps_cap < asset.min_notional_usdc.max(10.0) {
                        continue;
                    }
                    if sizing.notional_usdc > remaining_perps_cap {
                        let capped_qty = RiskEngine::round_step(
                            remaining_perps_cap / last_bar.close.max(1e-8),
                            asset.lot_step_size,
                            true,
                        );
                        let capped_notional = round_dp(capped_qty * last_bar.close, 2);
                        if capped_qty <= 0.0 || capped_notional < asset.min_notional_usdc.max(10.0)
                        {
                            continue;
                        }
                        sizing.quantity = capped_qty;
                        sizing.notional_usdc = capped_notional;
                    }
                }

                let exec_res = self.execute_maker_limit_chaser(
                    asset,
                    &ev.signal_action,
                    sizing.quantity,
                    last_bar.close,
                    last_bar.high,
                    last_bar.low,
                    last_bar.volume,
                    None,
                    if is_backtest { Some("PAPER") } else { None },
                    None,
                    false,
                );
                let is_filled = exec_res.fsm_state == "FILLED" && exec_res.filled_units > 0.0;
                let is_open_book = exec_res.fsm_state == "OPEN_ON_BOOK";
                if !is_filled && !is_open_book {
                    continue;
                }

                let fill_px = if exec_res.avg_fill_price > 0.0 {
                    exec_res.avg_fill_price
                } else {
                    last_bar.close
                };
                let order_qty = if is_filled {
                    exec_res.filled_units
                } else {
                    sizing.quantity
                };
                let filled_notional = round_dp(order_qty * fill_px, 2);
                let trade_status = if is_filled {
                    "OPEN".to_string()
                } else {
                    "PENDING".to_string()
                };
                let new_trade = TradeRecord {
                    trade_id: 0,
                    client_order_id: exec_res.client_order_id,
                    symbol: asset.symbol.clone(),
                    base_asset: asset.base_asset.clone(),
                    side: ev.signal_action.clone(),
                    status: trade_status,
                    fsm_state: exec_res.fsm_state.clone(),
                    entry_price: fill_px,
                    current_price: fill_px,
                    exit_price: None,
                    quantity: order_qty,
                    filled_units: if is_filled { exec_res.filled_units } else { 0.0 },
                    avg_fill_price: if is_filled && exec_res.avg_fill_price > 0.0 {
                        Some(exec_res.avg_fill_price)
                    } else {
                        None
                    },
                    notional_usdc: filled_notional,
                    risk_budget_usdc: sizing.risk_budget_usdc,
                    conviction_multiplier: sizing.conviction_multiplier,
                    initial_stop_price: sizing.initial_stop_price,
                    chandelier_stop_price: sizing.initial_stop_price,
                    current_stop_price: sizing.initial_stop_price,
                    highest_price_seen: fill_px,
                    lowest_price_seen: fill_px,
                    realized_pnl_usdc: 0.0,
                    unrealized_pnl_usdc: 0.0,
                    fees_paid_usdc: exec_res.fees_paid_usdc,
                    funding_accrued_usdc: 0.0,
                    slippage_paid_usd: exec_res.slippage_paid_usd,
                    exchange_order_id: Some(exec_res.exchange_order_id),
                    opened_at: String::new(),
                    closed_at: None,
                    exit_reason: None,
                };
                let tid = self.db.insert_trade(&new_trade)?;
                let mut inserted = new_trade;
                inserted.trade_id = tid;
                open_symbols.insert(asset.symbol.clone());
                total_wallet_deployed = round_dp(total_wallet_deployed + filled_notional, 2);
                if is_filled {
                    active_trades.push(inserted);
                }
            }
        }

        // 4. Compute portfolio telemetry & Basis Harvest allocation
        let mut deployed_notional = 0.0;
        let mut portfolio_heat = 0.0;
        let mut open_longs = 0i64;
        let mut open_shorts = 0i64;
        let mut total_upnl = 0.0;

        for t in &active_trades {
            let notional = t.quantity.abs() * t.current_price;
            let stop_px = if t.chandelier_stop_price > 0.0 {
                t.chandelier_stop_price
            } else {
                t.initial_stop_price
            };
            let risk = (t.entry_price - stop_px).abs() * t.quantity.abs();
            deployed_notional += notional;
            portfolio_heat += risk;
            total_upnl += t.unrealized_pnl_usdc + t.funding_accrued_usdc;
            if t.side == "LONG" {
                open_longs += 1;
            } else {
                open_shorts += 1;
            }
        }

        let regime_ev = evaluations.first();
        let hmm_regime = regime_ev
            .map(|e| e.hmm_regime.clone())
            .unwrap_or_else(|| "CHOP".to_string());
        let prob_bull = regime_ev.map(|e| e.prob_bull).unwrap_or(0.3333);
        let prob_bear = regime_ev.map(|e| e.prob_bear).unwrap_or(0.3333);
        let prob_chop = regime_ev.map(|e| e.prob_chop).unwrap_or(0.3334);

        let realized_after = self.db.get_total_realized_pnl()?;
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
        let wallet = cfg.get_str("wallet_address", "").trim().to_string();

        let mut basis_notional = 0.0;
        let mut total_equity =
            round_dp(initial_equity + realized_after + total_upnl + accrued_yield_usdc, 2);

        if !is_backtest && (mode == "LIVE" || mode == "TESTNET") && !wallet.is_empty() {
            let vault_state = self.fetch_hlp_vault_state(Some(&wallet), None, Some(is_testnet));
            if vault_state
                .get("live_queried")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                let vault_eq_now = vault_state
                    .get("vault_equity_usdc")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                let vault_yield_now = vault_state
                    .get("accrued_yield_usdc")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                let perp_eq_now = vault_state
                    .get("perp_equity_usdc")
                    .and_then(Value::as_f64);

                basis_notional = round_dp(vault_eq_now, 2);
                accrued_yield_usdc = round_dp(vault_yield_now, 4);
                if let Some(peq) = perp_eq_now.filter(|&p| p > 0.0 || vault_eq_now > 0.0) {
                    total_equity = round_dp(peq + vault_eq_now, 2);
                } else if prev_snap.total_equity_usdc > 0.0 {
                    total_equity = prev_snap.total_equity_usdc;
                } else {
                    total_equity = round_dp(
                        initial_equity + realized_after + total_upnl + accrued_yield_usdc,
                        2,
                    );
                }
            } else if prev_snap.total_equity_usdc > 0.0 {
                basis_notional = prev_snap.basis_harvest_notional_usdc;
                total_equity = prev_snap.total_equity_usdc;
            }
        } else {
            let available_cash = (initial_equity + realized_after + total_upnl + accrued_yield_usdc
                - deployed_notional)
                .max(0.0);
            let basis_candidates = evaluations
                .iter()
                .filter(|e| e.signal_action == "BASIS_HARVEST")
                .count();
            if (hmm_regime == "CHOP" || prob_chop >= chop_prob_thresh) && basis_candidates > 0 {
                let alloc_pct = cfg.get_f64("basis_max_idle_cash_allocation_pct", 0.50);
                let basis_floor = cfg.get_f64(
                    "basis_harvest_min_funding_hourly",
                    cfg.get_f64("basis_min_hourly_funding_rate", 0.000015),
                );
                basis_notional = round_dp(available_cash * alloc_pct, 2);
                if is_backtest && cfg.get_bool("enable_hlp_floor_backtest", false) {
                    let avg_funding = evaluations
                        .iter()
                        .filter(|e| e.signal_action == "BASIS_HARVEST")
                        .map(|e| e.funding_rate_hourly.max(basis_floor))
                        .sum::<f64>()
                        / (basis_candidates as f64);
                    accrued_yield_usdc =
                        round_dp(accrued_yield_usdc + basis_notional * avg_funding * 4.0, 4);
                }
            }
            total_equity =
                round_dp(initial_equity + realized_after + total_upnl + accrued_yield_usdc, 2);
        }

        let pond_deployed_now: f64 = if !is_backtest && (mode == "LIVE" || mode == "TESTNET") {
            self.db
                .get_open_pond_cells()
                .unwrap_or_default()
                .iter()
                .map(|c| c.notional_usdc.unwrap_or(0.0))
                .sum()
        } else {
            0.0
        };
        let available_perps_cash = if !is_backtest && (mode == "LIVE" || mode == "TESTNET") {
            round_dp(
                (total_equity - basis_notional - deployed_notional - pond_deployed_now).max(0.0),
                2,
            )
        } else {
            round_dp((total_equity - deployed_notional).max(0.0), 2)
        };

        let pit_book_equity = if two_book_active {
            (total_equity * split_pct + net_pit_transfers).max(1.0)
        } else {
            total_equity.max(1.0)
        };
        let gross_lev = round_dp(deployed_notional / pit_book_equity, 4);
        let heat_pct = round_dp(portfolio_heat / pit_book_equity, 6);

        let snap = PortfolioSnapshot {
            snapshot_id: None,
            timestamp_ms: now_ms,
            total_equity_usdc: total_equity,
            available_cash_usdc: available_perps_cash,
            deployed_notional_usdc: round_dp(deployed_notional, 2),
            gross_leverage: gross_lev,
            portfolio_heat_usdc: round_dp(portfolio_heat, 2),
            portfolio_heat_pct: heat_pct,
            basis_harvest_notional_usdc: basis_notional,
            accrued_yield_usdc,
            hmm_regime: hmm_regime.clone(),
            prob_bull,
            prob_bear,
            prob_chop,
            open_long_count: open_longs,
            open_short_count: open_shorts,
            created_at: None,
        };
        self.db.save_portfolio_snapshot(&snap)?;

        if !is_backtest {
            let _ = self
                .alerts
                .check_and_alert_regime_flip(&hmm_regime, prob_bull, prob_bear, prob_chop);
            let _ = self.alerts.check_and_alert_daily_drawdown(total_equity);
        }

        Ok(snap)
    }
}
