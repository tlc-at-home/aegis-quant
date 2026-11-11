use std::collections::{HashMap, HashSet};

use aegis_core::round_dp;
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

        let mut local_pond_cells = match self.db.get_open_pond_cells() {
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
        let mut armed_pond_cells = self.db.get_armed_pond_cells().unwrap_or_default();

        // In live/testnet mode (or simulated mode with explicit open_orders_slice), if a resting PIT order
        // or ARMED Pond cell is no longer on the exchange openOrders book, poll its fill status or cross-check
        // ex_positions immediately during the 15s tick so filled orders promote to OPEN with original stops intact (`R9-N1`, `R9-N3`, `R9-QB`, Fix 4).
        if open_orders_slice.is_some()
            && (!resting_trades.is_empty() || !armed_pond_cells.is_empty())
        {
            let maker_fee_bps = cfg.get_f64("maker_fee_bps", 1.5);
            let mut existing_open_qty_by_coin: HashMap<String, f64> = HashMap::new();
            for lt in &local_trades {
                *existing_open_qty_by_coin
                    .entry(lt.base_asset.to_uppercase())
                    .or_insert(0.0) += lt.quantity.abs();
            }
            for cell in &local_pond_cells {
                let coin = cell
                    .symbol
                    .split('/')
                    .next()
                    .unwrap_or(&cell.symbol)
                    .trim()
                    .to_uppercase();
                *existing_open_qty_by_coin.entry(coin).or_insert(0.0) +=
                    cell.quantity.unwrap_or(0.0).abs();
            }

            let changed = if simulated_exchange_positions.is_none() {
                let pit_changed = self.poll_resting_pit_trades_on_15s_tick(
                    &resting_trades,
                    &open_order_oids,
                    &ex_positions,
                    &mut existing_open_qty_by_coin,
                    maker_fee_bps,
                );
                let pond_changed = self.poll_armed_pond_cells_on_15s_tick(
                    &armed_pond_cells,
                    &open_order_oids,
                    &ex_positions,
                    &mut existing_open_qty_by_coin,
                    maker_fee_bps,
                );
                pit_changed || pond_changed
            } else {
                let mut promoted_sim = false;
                for rt in &resting_trades {
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
                    if let Some(&(szi, ex_entry_px, _, _)) = ex_positions.get(&coin) {
                        let same_side = (rt.side == "LONG" && szi > 0.0)
                            || (rt.side == "SHORT" && szi < 0.0);
                        let already_covered = existing_open_qty_by_coin
                            .get(&coin)
                            .copied()
                            .unwrap_or(0.0);
                        let uncovered = (szi.abs() - already_covered).max(0.0);
                        if same_side && uncovered > 1e-6 {
                            let fill_px = if ex_entry_px > 0.0 {
                                ex_entry_px
                            } else {
                                rt.entry_price
                            };
                            let fill_qty = if rt.quantity > 0.0 {
                                uncovered.min(rt.quantity)
                            } else {
                                uncovered
                            };
                            let fee_usdc =
                                round_dp(fill_qty * fill_px * (maker_fee_bps / 10_000.0), 4);
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
                                *existing_open_qty_by_coin.entry(coin).or_insert(0.0) +=
                                    fill_qty.abs();
                                promoted_sim = true;
                            }
                        }
                    }
                }
                for cell in &armed_pond_cells {
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
                    let coin = cell
                        .symbol
                        .split('/')
                        .next()
                        .unwrap_or(&cell.symbol)
                        .trim()
                        .to_uppercase();
                    if let Some(&(szi, ex_entry_px, _, _)) = ex_positions.get(&coin) {
                        let same_side = (cell.side == "LONG" && szi > 0.0)
                            || (cell.side == "SHORT" && szi < 0.0);
                        let already_covered = existing_open_qty_by_coin
                            .get(&coin)
                            .copied()
                            .unwrap_or(0.0);
                        let uncovered = (szi.abs() - already_covered).max(0.0);
                        if same_side && uncovered > 1e-6 {
                            let fill_px = if ex_entry_px > 0.0 {
                                ex_entry_px
                            } else {
                                cell.entry_price.unwrap_or(100.0)
                            };
                            let cell_qty = cell.quantity.unwrap_or(0.0).abs();
                            let fill_qty = if cell_qty > 0.0 {
                                uncovered.min(cell_qty)
                            } else {
                                uncovered
                            };
                            let fee_usdc =
                                round_dp(fill_qty * fill_px * (maker_fee_bps / 10_000.0), 4);
                            if matches!(
                                self.db.promote_armed_pond_cell_to_open(
                                    cell.cell_id,
                                    fill_qty,
                                    fill_px,
                                    fee_usdc
                                ),
                                Ok(true)
                            ) {
                                *existing_open_qty_by_coin.entry(coin).or_insert(0.0) +=
                                    fill_qty.abs();
                                promoted_sim = true;
                            }
                        }
                    }
                }
                promoted_sim
            };

            if changed {
                if let Ok(updated_open) = self.db.get_open_trades() {
                    local_trades = updated_open;
                }
                if let Ok(updated_resting) = self.db.get_resting_pit_trades() {
                    resting_trades = updated_resting;
                }
                if let Ok(updated_open_cells) = self.db.get_open_pond_cells() {
                    local_pond_cells = updated_open_cells;
                }
                if let Ok(updated_armed_cells) = self.db.get_armed_pond_cells() {
                    armed_pond_cells = updated_armed_cells;
                }
            }
        }

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

        // R9-S1 / R10-QB: Flag duplicate open rows sharing the same coin within PIT or POND via shared helper
        self.record_duplicate_same_coin_rows(
            &pit_ids_by_coin,
            "PIT",
            "engine.execution",
            "trade_ids",
            "OPEN PIT trade",
            &mut discrepancies,
        );
        self.record_duplicate_same_coin_rows(
            &pond_ids_by_coin,
            "POND",
            "engine.pond",
            "cell_ids",
            "OPEN/TRAILED POND cell",
            &mut discrepancies,
        );

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
                            lt.realized_pnl_usdc,
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
                            lt.realized_pnl_usdc,
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
                    // R9-S1 / R10-QB: Net other same-coin PIT rows when computing expected per-trade size
                    let is_last_on_coin = pit_ids_by_coin
                        .get(&coin)
                        .and_then(|ids| ids.last())
                        .copied()
                        == Some(lt.trade_id);
                    let (other_pit_qty_on_coin, residual_pit_qty, expected_pit_qty) =
                        Self::compute_same_coin_expected_row_qty(
                            szi.abs(),
                            pond_co_qty,
                            pit_qty_by_coin.get(&coin).copied().unwrap_or(0.0),
                            initial_pit_qty_by_coin.get(&coin).copied().unwrap_or(0.0),
                            lt.quantity.abs(),
                            is_last_on_coin,
                        );
                    if residual_pit_qty <= 1e-9 {
                        if !open_order_coins.contains(&coin) {
                            if let Err(e) = self.db.close_trade(
                                lt.trade_id,
                                mark_px,
                                lt.realized_pnl_usdc,
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
                        if let Err(e) = self.db.update_open_trade(
                            lt.trade_id,
                            round_dp(mark_px, 6),
                            lt.chandelier_stop_price,
                            round_dp(high_seen, 6),
                            round_dp(low_seen, 6),
                            live_upnl,
                            lt.funding_accrued_usdc,
                        ) {
                            tracing::error!(
                                trade_id = lt.trade_id,
                                symbol = %lt.symbol,
                                error = %e,
                                "Failed to persist 15s mark-to-market update on open trade (R11-N1)"
                            );
                        }
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
                    // R9-S1 / R10-QB: Net other same-coin POND cell rows when computing expected per-cell size
                    let is_last_on_coin = pond_ids_by_coin
                        .get(&coin)
                        .and_then(|ids| ids.last())
                        .copied()
                        == Some(cell.cell_id);
                    let (other_pond_qty_on_coin, residual_pond_qty, expected_pond_qty) =
                        Self::compute_same_coin_expected_row_qty(
                            szi.abs(),
                            pit_co_qty,
                            pond_qty_by_coin.get(&coin).copied().unwrap_or(0.0),
                            initial_pond_qty_by_coin.get(&coin).copied().unwrap_or(0.0),
                            local_qty,
                            is_last_on_coin,
                        );
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
                            let oid_str = format!("hl-{oid}");
                            match self.cancel_resting_order(&coin, &oid_str) {
                                Ok(true) => {}
                                Ok(false) => {
                                    tracing::warn!(
                                        coin = %coin,
                                        oid = %oid_str,
                                        "Dangling open order cancel returned false during exchange reconciliation (R10-N3)"
                                    );
                                }
                                Err(e) => {
                                    tracing::error!(
                                        coin = %coin,
                                        oid = %oid_str,
                                        error = %e,
                                        "Dangling open order cancel failed during exchange reconciliation (R10-N3)"
                                    );
                                }
                            }
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

    /// Logs a telemetry warning and records a `DUPLICATE_SAME_COIN_OPEN_ROWS` discrepancy
    /// for any coin with multiple active rows within a book (`R9-S1`, `R10-QB`).
    fn record_duplicate_same_coin_rows(
        &self,
        ids_by_coin: &HashMap<String, Vec<i64>>,
        book: &str,
        component: &str,
        id_key: &str,
        row_label: &str,
        discrepancies: &mut Vec<Value>,
    ) {
        let mut dup_coins: Vec<_> = ids_by_coin
            .iter()
            .filter(|(_, ids)| ids.len() > 1)
            .collect();
        dup_coins.sort_by_key(|(a, _)| *a);
        for (coin, ids) in dup_coins {
            let mut details = serde_json::Map::new();
            details.insert("book".to_string(), json!(book));
            details.insert("base_asset".to_string(), json!(coin));
            details.insert("count".to_string(), json!(ids.len()));
            details.insert(id_key.to_string(), json!(ids));
            let _ = self.db.log_telemetry(
                "WARNING",
                component,
                &format!(
                    "Detected {} duplicate {} rows for coin {} ({}={:?})",
                    ids.len(),
                    row_label,
                    coin,
                    id_key,
                    ids
                ),
                Some(Value::Object(details.clone())),
            );
            details.insert(
                "type".to_string(),
                json!("DUPLICATE_SAME_COIN_OPEN_ROWS"),
            );
            discrepancies.push(Value::Object(details));
        }
    }

    /// Computes `(other_book_qty_on_coin, residual_book_qty, expected_row_qty)` for a single
    /// PIT trade or Pond cell row when reconciling same-coin positions (`R9-S1`, `R10-QB`).
    pub(crate) fn compute_same_coin_expected_row_qty(
        ex_qty: f64,
        co_book_qty: f64,
        current_book_coin_qty: f64,
        initial_book_coin_qty: f64,
        row_qty: f64,
        is_last_on_coin: bool,
    ) -> (f64, f64, f64) {
        let other_book_qty_on_coin = (current_book_coin_qty - row_qty).max(0.0);
        let residual_book_qty = (ex_qty - co_book_qty).max(0.0);
        let expected_row_qty = if (current_book_coin_qty - residual_book_qty).abs() <= 1e-6
            || is_last_on_coin
        {
            round_dp((residual_book_qty - other_book_qty_on_coin).max(0.0), 6)
        } else if initial_book_coin_qty > 1e-9 {
            round_dp(residual_book_qty * (row_qty / initial_book_coin_qty), 6)
        } else {
            0.0
        };
        (other_book_qty_on_coin, residual_book_qty, expected_row_qty)
    }
}
