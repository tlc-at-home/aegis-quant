use std::collections::{HashMap, HashSet};

use aegis_core::{
    round_dp, PortfolioSnapshot, Result, SignalEvaluation, TradeRecord, UniverseAsset,
};
use aegis_risk::RiskEngine;
use serde_json::{json, Value};

use crate::{ExecutionEngine, ReconcileReport, ReconcileStatus};

impl ExecutionEngine {
    /// Reconciles local SQLite `OPEN` trades and Pond cells against Hyperliquid L1
    /// `clearinghouseState` and `openOrders` (`NM-5`, `NM-10`, `R3-B4`, `R3-H3`, `R4-N11`, `R9-S1`).
    pub fn reconcile_exchange_state(
        &self,
        simulated_exchange_positions: Option<&HashMap<String, Value>>,
        simulated_open_orders: Option<&[Value]>,
    ) -> ReconcileReport {
        let cfg = self.load_config_logged("reconcile_exchange_state");
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
        let wallet = cfg.get_str("wallet_address", "");

        if mode == "PAPER"
            && simulated_exchange_positions.is_none()
            && simulated_open_orders.is_none()
        {
            return match self.db.get_open_trades() {
                Ok(open_trades) => ReconcileReport {
                    status: ReconcileStatus::VerifiedPaper,
                    mode: Some("PAPER".to_string()),
                    reason: None,
                    local_open_trades: Some(open_trades.len()),
                    discrepancies_found: 0,
                    discrepancies: Vec::new(),
                },
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "Paper-mode reconcile_exchange_state failed to query local open trades (R8-N5)"
                    );
                    ReconcileReport {
                        status: ReconcileStatus::ErrorDbUnavailable,
                        mode: Some("PAPER".to_string()),
                        reason: Some(e.to_string()),
                        local_open_trades: None,
                        discrepancies_found: 0,
                        discrepancies: Vec::new(),
                    }
                }
            };
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
                        let mark_px = pos
                            .get("markPx")
                            .and_then(|v| {
                                v.as_f64()
                                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                            })
                            .unwrap_or_else(|| {
                                if szi.abs() > 1e-9 && entry_px > 0.0 {
                                    let implied = entry_px + upnl / szi;
                                    if implied.is_finite() && implied > 0.0 {
                                        round_dp(implied, 6)
                                    } else {
                                        entry_px
                                    }
                                } else {
                                    entry_px
                                }
                            });
                        ex_positions.insert(coin, (szi, entry_px, mark_px, upnl));
                    }
                }
            } else {
                return ReconcileReport {
                    status: ReconcileStatus::SkippedOffline,
                    mode: None,
                    reason: Some("CLEARINGHOUSE_STATE_UNAVAILABLE".to_string()),
                    local_open_trades: None,
                    discrepancies_found: 0,
                    discrepancies: Vec::new(),
                };
            }
        } else {
            return ReconcileReport {
                status: ReconcileStatus::SkippedOffline,
                mode: None,
                reason: Some("CLEARINGHOUSE_API_UNREACHABLE".to_string()),
                local_open_trades: None,
                discrepancies_found: 0,
                discrepancies: Vec::new(),
            };
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
        let open_order_oids: HashSet<u64> = open_orders_slice
            .unwrap_or(&[])
            .iter()
            .filter_map(|ord| {
                ord.get("oid").and_then(Value::as_u64).or_else(|| {
                    ord.get("oid")
                        .and_then(Value::as_str)
                        .and_then(|s| s.parse::<u64>().ok())
                })
            })
            .collect();

        let mut local_trades = match self.db.get_open_trades() {
            Ok(trades) => trades,
            Err(e) => {
                tracing::error!(error = %e, "Aborting reconcile_exchange_state: failed to load open trades");
                return ReconcileReport {
                    status: ReconcileStatus::ErrorDbUnavailable,
                    mode: None,
                    reason: Some(e.to_string()),
                    local_open_trades: None,
                    discrepancies_found: 0,
                    discrepancies: Vec::new(),
                };
            }
        };
        let mut resting_trades = match self.db.get_resting_pit_trades() {
            Ok(trades) => trades,
            Err(e) => {
                tracing::error!(error = %e, "Aborting reconcile_exchange_state: failed to load resting trades");
                return ReconcileReport {
                    status: ReconcileStatus::ErrorDbUnavailable,
                    mode: None,
                    reason: Some(e.to_string()),
                    local_open_trades: None,
                    discrepancies_found: 0,
                    discrepancies: Vec::new(),
                };
            }
        };

        // In live/testnet mode, if a resting PIT order is no longer on the exchange openOrders book,
        // poll its fill status immediately during the 15s tick so filled orders promote to OPEN without
        // waiting up to 15 minutes for the next PIT cycle (`R9-N1`, `R9-N3`, `R9-QB`).
        if simulated_exchange_positions.is_none()
            && open_orders_slice.is_some()
            && !resting_trades.is_empty()
        {
            let maker_fee_bps = cfg.get_f64("maker_fee_bps", 1.5);
            if self.poll_resting_pit_trades_on_15s_tick(
                &resting_trades,
                &open_order_oids,
                maker_fee_bps,
            ) {
                if let Ok(updated_open) = self.db.get_open_trades() {
                    local_trades = updated_open;
                }
                if let Ok(updated_resting) = self.db.get_resting_pit_trades() {
                    resting_trades = updated_resting;
                }
            }
        }

        let local_pond_cells = match self.db.get_open_pond_cells() {
            Ok(cells) => cells,
            Err(e) => {
                tracing::error!(error = %e, "Aborting reconcile_exchange_state: failed to load open pond cells");
                return ReconcileReport {
                    status: ReconcileStatus::ErrorDbUnavailable,
                    mode: None,
                    reason: Some(e.to_string()),
                    local_open_trades: None,
                    discrepancies_found: 0,
                    discrepancies: Vec::new(),
                };
            }
        };
        let armed_pond_cells = self.db.get_armed_pond_cells().unwrap_or_default();

        // Aggregate local quantities and row IDs per coin across both books (NM-5, NM-10, R9-S1)
        let mut pit_qty_by_coin: HashMap<String, f64> = HashMap::new();
        let mut pit_ids_by_coin: HashMap<String, Vec<i64>> = HashMap::new();
        for lt in &local_trades {
            let coin = lt.base_asset.to_uppercase();
            *pit_qty_by_coin.entry(coin.clone()).or_insert(0.0) += lt.quantity.abs();
            pit_ids_by_coin.entry(coin).or_default().push(lt.trade_id);
        }
        let initial_pit_qty_by_coin = pit_qty_by_coin.clone();

        let mut pond_qty_by_coin: HashMap<String, f64> = HashMap::new();
        let mut pond_ids_by_coin: HashMap<String, Vec<i64>> = HashMap::new();
        for cell in &local_pond_cells {
            let coin = cell
                .symbol
                .split('/')
                .next()
                .unwrap_or(&cell.symbol)
                .trim()
                .to_uppercase();
            *pond_qty_by_coin.entry(coin.clone()).or_insert(0.0) +=
                cell.quantity.unwrap_or(0.0).abs();
            pond_ids_by_coin
                .entry(coin)
                .or_default()
                .push(cell.cell_id);
        }
        let initial_pond_qty_by_coin = pond_qty_by_coin.clone();

        let mut seen_coins: HashSet<String> = HashSet::new();
        let mut open_pos_coins: HashSet<String> = HashSet::new();
        let mut known_resting_oids: HashSet<u64> = HashSet::new();
        for rt in &resting_trades {
            seen_coins.insert(rt.base_asset.to_uppercase());
            if let Some(oid) = rt
                .exchange_order_id
                .as_deref()
                .and_then(|s| s.strip_prefix("hl-").or(Some(s)))
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|&id| id > 0)
            {
                known_resting_oids.insert(oid);
            }
        }
        // R3-B4, R4-N11, R9-N7: Protect resting Pond ARMED cells and their resting_order_id orders from dangling-order cancellation
        for c in &armed_pond_cells {
            let resting_oid_raw = c.resting_order_id.as_deref().or_else(|| {
                c.exit_reason
                    .as_deref()
                    .and_then(|r| r.strip_prefix("RESTING_OID:"))
            });
            let coin = c
                .symbol
                .split('/')
                .next()
                .unwrap_or(&c.symbol)
                .trim()
                .to_uppercase();
            seen_coins.insert(coin);
            if let Some(oid) = resting_oid_raw
                .and_then(|s| s.strip_prefix("hl-").or(Some(s)))
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|&id| id > 0)
            {
                known_resting_oids.insert(oid);
            }
        }

        let mut discrepancies: Vec<Value> = Vec::new();

        // R9-S1: Flag duplicate open rows sharing the same coin within PIT or POND
        let mut dup_pit_coins: Vec<_> = pit_ids_by_coin
            .iter()
            .filter(|(_, ids)| ids.len() > 1)
            .collect();
        dup_pit_coins.sort_by_key(|(a, _)| *a);
        for (coin, ids) in dup_pit_coins {
            let _ = self.db.log_telemetry(
                "WARNING",
                "engine.execution",
                &format!(
                    "Detected {} duplicate OPEN PIT trade rows for coin {} (trade_ids={:?})",
                    ids.len(),
                    coin,
                    ids
                ),
                Some(json!({
                    "book": "PIT",
                    "base_asset": coin,
                    "count": ids.len(),
                    "trade_ids": ids,
                })),
            );
            discrepancies.push(json!({
                "type": "DUPLICATE_SAME_COIN_OPEN_ROWS",
                "book": "PIT",
                "base_asset": coin,
                "count": ids.len(),
                "trade_ids": ids,
            }));
        }
        let mut dup_pond_coins: Vec<_> = pond_ids_by_coin
            .iter()
            .filter(|(_, ids)| ids.len() > 1)
            .collect();
        dup_pond_coins.sort_by_key(|(a, _)| *a);
        for (coin, ids) in dup_pond_coins {
            let _ = self.db.log_telemetry(
                "WARNING",
                "engine.pond",
                &format!(
                    "Detected {} duplicate OPEN/TRAILED POND cell rows for coin {} (cell_ids={:?})",
                    ids.len(),
                    coin,
                    ids
                ),
                Some(json!({
                    "book": "POND",
                    "base_asset": coin,
                    "count": ids.len(),
                    "cell_ids": ids,
                })),
            );
            discrepancies.push(json!({
                "type": "DUPLICATE_SAME_COIN_OPEN_ROWS",
                "book": "POND",
                "base_asset": coin,
                "count": ids.len(),
                "cell_ids": ids,
            }));
        }

        for lt in &local_trades {
            let coin = lt.base_asset.to_uppercase();
            seen_coins.insert(coin.clone());
            open_pos_coins.insert(coin.clone());
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
                        let cur_qty = pit_qty_by_coin.get(&coin).copied().unwrap_or(0.0);
                        pit_qty_by_coin.insert(coin.clone(), (cur_qty - lt.quantity.abs()).max(0.0));
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
                    // R9-S1: Net other same-coin PIT rows when computing expected per-trade size
                    let ex_qty = szi.abs();
                    let current_pit_coin_qty = pit_qty_by_coin.get(&coin).copied().unwrap_or(0.0);
                    let other_pit_qty_on_coin = (current_pit_coin_qty - lt.quantity.abs()).max(0.0);
                    let residual_pit_qty = (ex_qty - pond_co_qty).max(0.0);
                    if residual_pit_qty <= 1e-9 {
                        if !open_order_coins.contains(&coin) {
                            if let Err(e) = self.db.close_trade(
                                lt.trade_id,
                                mark_px,
                                0.0,
                                lt.fees_paid_usdc,
                                "RECONCILED_CLOSED_ON_EXCHANGE",
                            ) {
                                tracing::error!(
                                    trade_id = lt.trade_id,
                                    error = %e,
                                    "Failed to close phantom local trade covered by Pond position"
                                );
                            }
                            pit_qty_by_coin.insert(coin.clone(), other_pit_qty_on_coin);
                            discrepancies.push(json!({
                                "type": "PHANTOM_LOCAL_CLOSED",
                                "base_asset": coin,
                                "trade_id": lt.trade_id,
                            }));
                        }
                        continue;
                    }
                    let is_last_on_coin = pit_ids_by_coin
                        .get(&coin)
                        .and_then(|ids| ids.last())
                        .copied()
                        == Some(lt.trade_id);
                    let initial_pit_coin_qty =
                        initial_pit_qty_by_coin.get(&coin).copied().unwrap_or(0.0);
                    let expected_pit_qty = if (current_pit_coin_qty - residual_pit_qty).abs() <= 1e-6
                        || is_last_on_coin
                    {
                        round_dp((residual_pit_qty - other_pit_qty_on_coin).max(0.0), 6)
                    } else if initial_pit_coin_qty > 1e-9 {
                        round_dp(
                            residual_pit_qty * (lt.quantity.abs() / initial_pit_coin_qty),
                            6,
                        )
                    } else {
                        0.0
                    };
                    let mut size_reconciled = false;
                    if (expected_pit_qty - lt.quantity.abs()).abs() > 1e-6 && expected_pit_qty > 0.0
                    {
                        match self.db.reconcile_open_trade_size(
                            lt.trade_id,
                            expected_pit_qty,
                            Some(entry_px),
                        ) {
                            Ok(true) => {
                                size_reconciled = true;
                                pit_qty_by_coin.insert(
                                    coin.clone(),
                                    other_pit_qty_on_coin + expected_pit_qty,
                                );
                                discrepancies.push(json!({
                                    "type": "SIZE_DRIFT_RECONCILED",
                                    "base_asset": coin,
                                    "trade_id": lt.trade_id,
                                    "local_qty": lt.quantity,
                                    "exchange_qty": expected_pit_qty,
                                }));
                            }
                            Ok(false) => {}
                            Err(e) => {
                                tracing::error!(
                                    trade_id = lt.trade_id,
                                    error = %e,
                                    "Failed to reconcile open trade size"
                                );
                            }
                        }
                    }
                    if mark_px > 0.0 {
                        let eff_qty = if size_reconciled {
                            expected_pit_qty
                        } else {
                            lt.quantity.abs()
                        };
                        let eff_entry_px = if size_reconciled && entry_px > 0.0 {
                            entry_px
                        } else {
                            lt.entry_price
                        };
                        let live_upnl = round_dp(
                            if lt.side.eq_ignore_ascii_case("LONG")
                                || lt.side.eq_ignore_ascii_case("BUY")
                            {
                                (mark_px - eff_entry_px) * eff_qty
                            } else {
                                (eff_entry_px - mark_px) * eff_qty
                            },
                            4,
                        );
                        let high_seen = lt.highest_price_seen.max(mark_px);
                        let low_seen = if lt.lowest_price_seen <= 0.0 {
                            mark_px
                        } else {
                            lt.lowest_price_seen.min(mark_px)
                        };
                        let _ = self.db.update_open_trade(
                            lt.trade_id,
                            round_dp(mark_px, 6),
                            lt.chandelier_stop_price,
                            round_dp(high_seen, 6),
                            round_dp(low_seen, 6),
                            live_upnl,
                            lt.funding_accrued_usdc,
                        );
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
            open_pos_coins.insert(coin.clone());
            let pit_co_qty = pit_qty_by_coin.get(&coin).copied().unwrap_or(0.0);
            let local_qty = cell.quantity.unwrap_or(0.0).abs();

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
                        let cur_qty = pond_qty_by_coin.get(&coin).copied().unwrap_or(0.0);
                        pond_qty_by_coin.insert(coin.clone(), (cur_qty - local_qty).max(0.0));
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
                    // R9-S1: Net other same-coin POND cell rows when computing expected per-cell size
                    let ex_qty = szi.abs();
                    let current_pond_coin_qty =
                        pond_qty_by_coin.get(&coin).copied().unwrap_or(0.0);
                    let other_pond_qty_on_coin = (current_pond_coin_qty - local_qty).max(0.0);
                    let residual_pond_qty = (ex_qty - pit_co_qty).max(0.0);
                    if residual_pond_qty <= 1e-9 {
                        if !open_order_coins.contains(&coin) {
                            if let Err(e) = self.db.reconcile_close_phantom_pond_cell(
                                cell.cell_id,
                                mark_px,
                                "RECONCILED_CLOSED_ON_EXCHANGE",
                            ) {
                                tracing::error!(
                                    cell_id = cell.cell_id,
                                    error = %e,
                                    "Failed to close phantom pond cell covered by PIT position"
                                );
                            }
                            pond_qty_by_coin.insert(coin.clone(), other_pond_qty_on_coin);
                            discrepancies.push(json!({
                                "type": "PHANTOM_POND_CELL_CLOSED",
                                "base_asset": coin,
                                "cell_id": cell.cell_id,
                            }));
                        }
                        continue;
                    }
                    let is_last_on_coin = pond_ids_by_coin
                        .get(&coin)
                        .and_then(|ids| ids.last())
                        .copied()
                        == Some(cell.cell_id);
                    let initial_pond_coin_qty =
                        initial_pond_qty_by_coin.get(&coin).copied().unwrap_or(0.0);
                    let expected_pond_qty = if (current_pond_coin_qty - residual_pond_qty).abs()
                        <= 1e-6
                        || is_last_on_coin
                    {
                        round_dp((residual_pond_qty - other_pond_qty_on_coin).max(0.0), 6)
                    } else if initial_pond_coin_qty > 1e-9 {
                        round_dp(
                            residual_pond_qty * (local_qty / initial_pond_coin_qty),
                            6,
                        )
                    } else {
                        0.0
                    };
                    if (expected_pond_qty - local_qty).abs() > 1e-6 && expected_pond_qty > 0.0 {
                        match self.db.reconcile_open_pond_cell_size(
                            cell.cell_id,
                            expected_pond_qty,
                            Some(entry_px),
                        ) {
                            Ok(true) => {
                                pond_qty_by_coin.insert(
                                    coin.clone(),
                                    other_pond_qty_on_coin + expected_pond_qty,
                                );
                                discrepancies.push(json!({
                                    "type": "SIZE_DRIFT_POND_CELL_RECONCILED",
                                    "base_asset": coin,
                                    "cell_id": cell.cell_id,
                                    "local_qty": local_qty,
                                    "exchange_qty": expected_pond_qty,
                                }));
                            }
                            Ok(false) => {}
                            Err(e) => {
                                tracing::error!(
                                    cell_id = cell.cell_id,
                                    error = %e,
                                    "Failed to reconcile pond cell size"
                                );
                            }
                        }
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
                let ord_oid = ord.get("oid").and_then(Value::as_u64).or_else(|| {
                    ord.get("oid")
                        .and_then(Value::as_str)
                        .and_then(|s| s.parse::<u64>().ok())
                });
                let is_known_resting_oid = ord_oid
                    .map(|oid| known_resting_oids.contains(&oid))
                    .unwrap_or(false);
                if !is_known_resting_oid
                    && (!open_pos_coins.contains(&coin) || ex_positions.contains_key(&coin))
                {
                    if simulated_open_orders.is_none() {
                        if let Some(oid) = ord_oid {
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

        ReconcileReport {
            status: ReconcileStatus::Reconciled,
            mode: Some(mode),
            reason: None,
            local_open_trades: None,
            discrepancies_found: discrepancies.len(),
            discrepancies,
        }
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

        // 0. Poll resting PIT orders (`OPEN_ON_BOOK`) before evaluating stops (`NH-4`, `R3-H2`, `R3-H4`, `R3-H16`, `R9-N3`, `R9-QB`)
        self.reconcile_resting_pit_trades_for_cycle(freeze_entries, is_backtest, maker_fee_bps);

        let usdc_assets = self.db.get_usdc_perp_universe()?;
        let mut asset_by_symbol: HashMap<String, UniverseAsset> = usdc_assets
            .into_iter()
            .map(|a| (a.symbol.clone(), a))
            .collect();
        if let Ok(pond_rows) = self.db.get_pond_universe(false) {
            for p in pond_rows {
                asset_by_symbol.entry(p.symbol.clone()).or_insert_with(|| {
                    crate::build_orphan_or_pond_universe_asset(
                        &p.symbol,
                        "",
                        p.reference_price,
                        Some(&p),
                        "POND",
                    )
                });
            }
        }
        {
            let mut cache = self.resolved_asset_cache.lock();
            for (sym, asset) in &asset_by_symbol {
                cache.insert(sym.clone(), (asset.clone(), now_ms));
            }
        }
        let eval_by_symbol: HashMap<String, &SignalEvaluation> = evaluations
            .iter()
            .map(|e| (e.exec_symbol.clone(), e))
            .collect();

        let prev_snap = self.db.get_latest_portfolio_snapshot()?;
        let mut accrued_yield_usdc = prev_snap.accrued_yield_usdc;

        // 1. Evaluate open positions (Two-Tier Chandelier Stop + Regime Exit)
        let open_trades = self.db.get_open_trades()?;
        for trade in open_trades {
            if !asset_by_symbol.contains_key(&trade.symbol) {
                let resolved = self.resolve_trade_asset(
                    &trade.symbol,
                    &trade.base_asset,
                    trade.entry_price,
                );
                asset_by_symbol.insert(trade.symbol.clone(), resolved);
            }
            let asset = asset_by_symbol
                .get(&trade.symbol)
                .expect("trade symbol inserted above");
            let mut bars =
                self.db
                    .get_ohlcv_bars(&trade.symbol, &asset.exec_timeframe, 30, Some(now_ms))?;
            if bars.is_empty() && asset.exec_timeframe != "1h" {
                bars = self
                    .db
                    .get_ohlcv_bars(&trade.symbol, "1h", 30, Some(now_ms))?;
            }
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
                let is_paper_trade = trade
                    .exchange_order_id
                    .as_deref()
                    .map(|oid| oid.starts_with("hl-paper-") || oid.starts_with("sim-"))
                    .unwrap_or(false);

                if (mode == "LIVE" || mode == "TESTNET") && !is_backtest && !is_paper_trade {
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
                        let wallet = cfg.get_str("wallet_address", "").trim().to_string();
                        let is_testnet =
                            cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
                        let confirmed_absent_on_exchange = self
                            .is_coin_confirmed_absent_on_exchange(
                                &wallet,
                                is_testnet,
                                &asset.base_asset,
                            );
                        if !confirmed_absent_on_exchange {
                            let upnl = if trade.side == "LONG" {
                                (last_bar.close - trade.entry_price) * trade.quantity.abs()
                            } else {
                                (trade.entry_price - last_bar.close) * trade.quantity.abs()
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
                        let exit_notional = trade.quantity.abs() * actual_exit_px;
                        let exit_fee = exit_notional * (taker_fee_bps / 10_000.0);
                        actual_fees = round_dp(trade.fees_paid_usdc + exit_fee, 4);
                    }
                } else {
                    let exit_notional = trade.quantity.abs() * actual_exit_px;
                    let exit_fee = exit_notional * (taker_fee_bps / 10_000.0);
                    actual_fees = round_dp(trade.fees_paid_usdc + exit_fee, 4);
                }

                let gross_pnl = if trade.side == "LONG" {
                    (actual_exit_px - trade.entry_price) * trade.quantity.abs()
                } else {
                    (trade.entry_price - actual_exit_px) * trade.quantity.abs()
                };
                let net_pnl = round_dp(
                    gross_pnl + trade.realized_pnl_usdc - actual_fees + trade.funding_accrued_usdc,
                    4,
                );
                // R8-N4: Check bool returned by close_trade; if already closed concurrently, skip cooldown & duplicate alert
                let closed = self.db.close_trade(
                    trade.trade_id,
                    actual_exit_px,
                    net_pnl,
                    actual_fees,
                    &reason,
                )?;
                if !closed {
                    continue;
                }
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
                    (last_bar.close - trade.entry_price) * trade.quantity.abs()
                } else {
                    (trade.entry_price - last_bar.close) * trade.quantity.abs()
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
        let risk_scope = cfg
            .get_str("risk_caps_reference_scope", "BOOK")
            .trim()
            .to_ascii_uppercase();
        let split_pct = if risk_scope == "PLATFORM" && !two_book_active {
            1.0
        } else {
            cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90)
        };
        let net_pit_transfers = if !is_backtest && (risk_scope != "PLATFORM" || two_book_active) {
            self.db.get_net_book_transfer_adjustment("PIT").unwrap_or(0.0)
        } else {
            0.0
        };
        let current_equity = if !is_backtest && risk_scope != "PLATFORM" {
            self.db
                .compute_pit_equity_and_idle_usdc()
                .map(|(eq, _)| eq)
                .unwrap_or_else(|_| {
                    (initial_equity * split_pct
                        + net_pit_transfers
                        + realized_pnl
                        + unrealized_pnl
                        + accrued_yield_usdc)
                        .max(50.0)
                })
        } else {
            (initial_equity * split_pct
                + net_pit_transfers
                + realized_pnl
                + unrealized_pnl
                + accrued_yield_usdc)
                .max(50.0)
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
            let mut pending_longs = resting_after
                .iter()
                .filter(|t| t.side.eq_ignore_ascii_case("LONG"))
                .count();
            let mut pending_shorts = resting_after
                .iter()
                .filter(|t| t.side.eq_ignore_ascii_case("SHORT"))
                .count();
            for ev in evaluations {
                if ev.signal_action != "LONG" && ev.signal_action != "SHORT" {
                    continue;
                }
                if open_symbols.contains(&ev.exec_symbol) {
                    continue;
                }
                if ev.signal_action == "LONG"
                    && active_trades.iter().filter(|t| t.side == "LONG").count() + pending_longs
                        >= max_longs
                {
                    continue;
                }
                if ev.signal_action == "SHORT"
                    && active_trades.iter().filter(|t| t.side == "SHORT").count() + pending_shorts
                        >= max_shorts
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
                let ex_oid_for_rollback = new_trade.exchange_order_id.clone();
                let tid = match self.db.insert_trade(&new_trade) {
                    Ok(id) => id,
                    Err(e) => {
                        if is_open_book {
                            if let Some(ex_oid) = ex_oid_for_rollback.as_deref() {
                                let _ = self.cancel_resting_order(&asset.symbol, ex_oid);
                            }
                        }
                        // R9-QA: Emit CRITICAL telemetry and alert when insert_trade fails after order placement
                        let _ = self.db.log_telemetry(
                            "CRITICAL",
                            "engine.execution",
                            &format!(
                                "Failed to persist PIT trade in SQLite after order dispatch for {} ({} {} @ {:.4}, oid={:?}): {e}",
                                asset.symbol,
                                ev.signal_action,
                                order_qty,
                                fill_px,
                                ex_oid_for_rollback
                            ),
                            Some(json!({
                                "symbol": asset.symbol,
                                "side": ev.signal_action,
                                "quantity": order_qty,
                                "entry_price": fill_px,
                                "fsm_state": exec_res.fsm_state,
                                "exchange_order_id": ex_oid_for_rollback,
                                "error": e.to_string(),
                            })),
                        );
                        if !is_backtest {
                            let err_str = e.to_string();
                            if err_str.contains("trades table")
                                || err_str.contains("trades_v11_rebuild")
                            {
                                let _ = self.alerts.notify_critical_event(
                                    "TRADES_SCHEMA_SELF_HEAL_FAILED",
                                    &format!(
                                        "Legacy trades schema self-heal failed while inserting PIT trade for {}: {err_str}",
                                        asset.symbol
                                    ),
                                    Some(json!({
                                        "symbol": asset.symbol,
                                        "error": err_str,
                                    })),
                                );
                            }
                            let _ = self.alerts.notify_critical_event(
                                "TRADE_INSERT_FAILED_AFTER_LIVE_ORDER",
                                &format!(
                                    "PIT order for {} ({} {} @ {:.4}, oid={:?}) succeeded on exchange/sim, but SQLite insert_trade failed: {e}",
                                    asset.symbol,
                                    ev.signal_action,
                                    order_qty,
                                    fill_px,
                                    ex_oid_for_rollback
                                ),
                                Some(json!({
                                    "symbol": asset.symbol,
                                    "side": ev.signal_action,
                                    "quantity": order_qty,
                                    "entry_price": fill_px,
                                    "fsm_state": exec_res.fsm_state,
                                    "exchange_order_id": ex_oid_for_rollback,
                                    "error": e.to_string(),
                                })),
                            );
                        }
                        return Err(e);
                    }
                };
                let mut inserted = new_trade;
                inserted.trade_id = tid;
                open_symbols.insert(asset.symbol.clone());
                total_wallet_deployed = round_dp(total_wallet_deployed + filled_notional, 2);
                if is_filled {
                    active_trades.push(inserted);
                } else if is_open_book {
                    if inserted.side.eq_ignore_ascii_case("LONG") {
                        pending_longs += 1;
                    } else if inserted.side.eq_ignore_ascii_case("SHORT") {
                        pending_shorts += 1;
                    }
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
        let pond_total_pnl = if !is_backtest {
            let pond_realized = self.db.get_pond_realized_pnl().unwrap_or(0.0);
            let pond_upnl: f64 = self
                .db
                .get_open_pond_cells()
                .unwrap_or_default()
                .iter()
                .map(|c| {
                    let entry_px = c.entry_price.unwrap_or(0.0);
                    let qty = c.quantity.unwrap_or(0.0);
                    let mark_px = self
                        .db
                        .get_ohlcv_bars(&c.symbol, "1h", 1, None)
                        .ok()
                        .and_then(|bars| bars.last().map(|b| b.close))
                        .filter(|&p| p > 0.0)
                        .unwrap_or(entry_px);
                    let price_pnl = if entry_px > 0.0 && qty > 0.0 {
                        if c.side.eq_ignore_ascii_case("LONG") {
                            (mark_px - entry_px) * qty
                        } else {
                            (entry_px - mark_px) * qty
                        }
                    } else {
                        0.0
                    };
                    price_pnl + c.realized_pnl_usdc + c.funding_accrued_usdc - c.fees_paid_usdc
                })
                .sum();
            pond_realized + pond_upnl
        } else {
            0.0
        };
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
        let wallet = cfg.get_str("wallet_address", "").trim().to_string();

        let accounting_eq = round_dp(
            initial_equity + realized_after + total_upnl + pond_total_pnl + accrued_yield_usdc,
            2,
        );
        let mut basis_notional = 0.0;
        let mut total_equity = accounting_eq;

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
                let accounting_eq_now = round_dp(
                    initial_equity
                        + realized_after
                        + total_upnl
                        + pond_total_pnl
                        + accrued_yield_usdc,
                    2,
                );
                if let Some(peq) = perp_eq_now.filter(|&p| p > 0.0 || vault_eq_now > 0.0) {
                    let live_wallet_eq = round_dp(peq + vault_eq_now, 2);
                    total_equity = if live_wallet_eq > 0.0 && live_wallet_eq < accounting_eq_now {
                        live_wallet_eq
                    } else {
                        accounting_eq_now
                    };
                } else {
                    total_equity = accounting_eq_now;
                }
            } else if prev_snap.total_equity_usdc > 0.0 {
                basis_notional = prev_snap.basis_harvest_notional_usdc;
                total_equity = prev_snap.total_equity_usdc.min(accounting_eq);
            }
        } else {
            let available_cash = (initial_equity + realized_after + total_upnl + accrued_yield_usdc
                - deployed_notional)
                .max(0.0);
            let basis_rates: Vec<f64> = evaluations
                .iter()
                .filter(|e| e.signal_action == "BASIS_HARVEST")
                .map(|e| e.funding_rate_hourly)
                .collect();
            if (hmm_regime == "CHOP" || prob_chop >= chop_prob_thresh) && !basis_rates.is_empty() {
                let alloc_pct = cfg.get_f64("basis_max_idle_cash_allocation_pct", 0.50);
                let basis_floor = cfg.get_f64(
                    "basis_harvest_min_funding_hourly",
                    cfg.get_f64("basis_min_hourly_funding_rate", 0.000015),
                );
                let accrue_yield = is_backtest && cfg.get_bool("enable_hlp_floor_backtest", false);
                let (notional, yield_delta) = aegis_core::compute_basis_harvest_accrual(
                    available_cash,
                    alloc_pct,
                    basis_floor,
                    &basis_rates,
                    accrue_yield,
                );
                basis_notional = notional;
                if accrue_yield {
                    accrued_yield_usdc = round_dp(accrued_yield_usdc + yield_delta, 4);
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

        let pit_book_equity = if !is_backtest && (risk_scope != "PLATFORM" || two_book_active) {
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
