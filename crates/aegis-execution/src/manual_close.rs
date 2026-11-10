//! Manual Take-Profit / Close helpers and cached `UniverseAsset` resolution (`R7-S3`, `R7-S4`, `R7-N3`, `R8-N2`, `R8-N3`, `R8-N6`, `R9-QB`).

use aegis_core::{round_dp, AegisError, Result, UniverseAsset};
use serde_json::{json, Value};

use crate::ExecutionEngine;

impl ExecutionEngine {
    /// Returns `true` ONLY when `fetch_hyperliquid_clearinghouse_state` succeeds and positively
    /// confirms that `target_coin` has no open position (`|szi| <= 1e-8`) on the exchange (`R7-S4`, `R8-N3`).
    /// Returns `false` if `wallet` is empty, the API call fails/times out, or the position is still open.
    pub(crate) fn is_coin_confirmed_absent_on_exchange(
        &self,
        wallet: &str,
        is_testnet: bool,
        target_coin: &str,
    ) -> bool {
        if wallet.trim().is_empty() {
            return false;
        }
        let Some(ch) = self.fetch_hyperliquid_clearinghouse_state(wallet.trim(), is_testnet) else {
            return false;
        };
        let Some(asset_positions) = ch.get("assetPositions").and_then(Value::as_array) else {
            return false;
        };
        let coin_upper = target_coin.trim().to_uppercase();
        for item in asset_positions {
            let pos = item.get("position").unwrap_or(item);
            let coin = pos
                .get("coin")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_uppercase();
            let szi = pos
                .get("szi")
                .and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                })
                .unwrap_or(0.0);
            if coin == coin_upper && szi.abs() > 1e-8 {
                return false;
            }
        }
        true
    }

    /// Resolves `UniverseAsset` metadata for an open trade symbol from the engine's
    /// in-memory `resolved_asset_cache` with TTL expiry (`asset_index_cache_ttl_secs`, default 3600s; `R8-N2`),
    /// populating on miss from `universe_assets`, `pond_universe`, or price-scaled tick/lot defaults
    /// for orphan positions (`R7-N3`, `R7-N5`).
    pub fn resolve_trade_asset(
        &self,
        symbol: &str,
        base_asset: &str,
        ref_price: f64,
    ) -> UniverseAsset {
        let now_ms = self.db.now_ms();
        let ttl_ms = self
            .db
            .get_raw_config_value("asset_index_cache_ttl_secs")
            .ok()
            .flatten()
            .and_then(|v| v.trim().trim_matches('"').parse::<f64>().ok().map(|f| f as i64))
            .unwrap_or(3600)
            .max(60)
            * 1000;
        if let Some((cached, cached_at_ms)) = self.resolved_asset_cache.lock().get(symbol).cloned()
        {
            if (now_ms - cached_at_ms).abs() <= ttl_ms {
                return cached;
            }
        }
        let mut resolved: Option<UniverseAsset> = None;
        if let Ok(assets) = self.db.get_usdc_perp_universe() {
            let mut cache = self.resolved_asset_cache.lock();
            for a in assets {
                if resolved.is_none()
                    && (a.symbol == symbol
                        || (!base_asset.trim().is_empty() && a.base_asset == base_asset))
                {
                    resolved = Some(a.clone());
                }
                cache.insert(a.symbol.clone(), (a, now_ms));
            }
        }
        if let Some(found) = resolved {
            self.resolved_asset_cache
                .lock()
                .insert(symbol.to_string(), (found.clone(), now_ms));
            return found;
        }
        if let Ok(pond_rows) = self.db.get_pond_universe(false) {
            if let Some(p) = pond_rows.into_iter().find(|r| r.symbol == symbol) {
                let built = crate::build_orphan_or_pond_universe_asset(
                    &p.symbol,
                    base_asset,
                    ref_price,
                    Some(&p),
                    "POND",
                );
                self.resolved_asset_cache
                    .lock()
                    .insert(symbol.to_string(), (built.clone(), now_ms));
                return built;
            }
        }
        let orphan = crate::build_orphan_or_pond_universe_asset(
            symbol,
            base_asset,
            ref_price,
            None,
            "ORPHAN",
        );
        self.resolved_asset_cache
            .lock()
            .insert(symbol.to_string(), (orphan.clone(), now_ms));
        orphan
    }

    /// Manually closes / takes profit on an open Book A (`PIT`) position (`trade_id`) (`R7-S3`, `R7-S4`, `R7-N4`, `R7-N6`, `R8-N6`).
    pub fn close_open_trade_manual(
        &self,
        trade_id: i64,
        reason_override: Option<&str>,
    ) -> Result<Value> {
        let reason_owned = aegis_core::normalize_manual_exit_reason(reason_override);
        let reason = reason_owned.as_str();

        let open_trades = self.db.get_open_trades()?;
        let Some(trade) = open_trades.into_iter().find(|t| t.trade_id == trade_id) else {
            let resting = self.db.get_resting_pit_trades().unwrap_or_default();
            if let Some(rt) = resting.into_iter().find(|t| t.trade_id == trade_id) {
                if let Some(ref oid) = rt.exchange_order_id {
                    match self.cancel_resting_order(&rt.symbol, oid) {
                        Ok(true) => {}
                        Ok(false) => {
                            tracing::warn!(
                                trade_id = trade_id,
                                symbol = %rt.symbol,
                                oid = %oid,
                                "Resting PIT order cancel returned false during manual cancel (R10-N3)"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                trade_id = trade_id,
                                symbol = %rt.symbol,
                                oid = %oid,
                                error = %e,
                                "Resting PIT order cancel failed during manual cancel (R10-N3)"
                            );
                        }
                    }
                }
                let cancelled = self.db.cancel_resting_trade(trade_id, reason)?;
                if !cancelled {
                    return Err(AegisError::Validation(format!(
                        "Resting trade #{trade_id} is already cancelled or closed"
                    )));
                }
                return Ok(json!({
                    "status": "CANCELLED_RESTING",
                    "trade_id": trade_id,
                    "symbol": rt.symbol,
                    "exit_reason": reason,
                }));
            }
            return Err(AegisError::Validation(format!(
                "Open trade #{trade_id} not found or already closed"
            )));
        };

        let cfg = self.db.load_config()?;
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);
        let asset = self.resolve_trade_asset(&trade.symbol, &trade.base_asset, trade.entry_price);

        let mut bars = self
            .db
            .get_ohlcv_bars(&trade.symbol, &asset.exec_timeframe, 5, None)
            .unwrap_or_default();
        if bars.is_empty() && asset.exec_timeframe != "1h" {
            bars = self
                .db
                .get_ohlcv_bars(&trade.symbol, "1h", 5, None)
                .unwrap_or_default();
        }
        let last_bar = bars.last();
        let ref_px = last_bar
            .map(|b| b.close)
            .filter(|&p| p > 0.0)
            .unwrap_or(if trade.current_price > 0.0 {
                trade.current_price
            } else {
                trade.entry_price
            });
        let bar_high = last_bar.map(|b| b.high).unwrap_or(ref_px);
        let bar_low = last_bar.map(|b| b.low).unwrap_or(ref_px);
        let bar_vol = last_bar.map(|b| b.volume).unwrap_or(50_000.0);

        let close_side = if trade.side == "LONG" {
            "SHORT"
        } else {
            "LONG"
        };

        let mut actual_exit_px = ref_px;
        let actual_fees;
        let is_paper_trade = trade
            .exchange_order_id
            .as_deref()
            .map(|oid| oid.starts_with("hl-paper-") || oid.starts_with("sim-"))
            .unwrap_or(false);

        if (mode == "LIVE" || mode == "TESTNET") && !is_paper_trade {
            let exec_close = self.execute_maker_limit_chaser(
                &asset,
                close_side,
                trade.quantity.abs(),
                ref_px,
                bar_high,
                bar_low,
                bar_vol,
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
                    let remaining_qty = (trade.quantity.abs() - exec_close.filled_units).max(0.0);
                    let remaining_notional = round_dp(remaining_qty * trade.entry_price, 4);
                    let partial_gross_pnl = if trade.side == "LONG" {
                        (exec_close.avg_fill_price - trade.entry_price) * exec_close.filled_units
                    } else {
                        (trade.entry_price - exec_close.avg_fill_price) * exec_close.filled_units
                    };
                    self.db.reduce_open_trade_partial_exit(
                        trade.trade_id,
                        remaining_qty,
                        remaining_notional,
                        round_dp(partial_gross_pnl, 4),
                        round_dp(exec_close.fees_paid_usdc, 4),
                        round_dp(exec_close.slippage_paid_usd, 4),
                        ref_px,
                    )?;
                    let _ = self.reconcile_cycle_with_options(&[], false, true);
                    return Ok(json!({
                        "status": "PARTIAL_CLOSE",
                        "trade_id": trade.trade_id,
                        "symbol": trade.symbol,
                        "side": trade.side,
                        "filled_units": exec_close.filled_units,
                        "remaining_units": remaining_qty,
                        "exit_price": exec_close.avg_fill_price,
                        "realized_pnl_delta_usdc": round_dp(partial_gross_pnl - exec_close.fees_paid_usdc, 4),
                        "exit_reason": reason,
                    }));
                }
                actual_exit_px = exec_close.avg_fill_price;
                actual_fees = round_dp(trade.fees_paid_usdc + exec_close.fees_paid_usdc, 4);
            } else {
                // R7-S4 & R8-N3: Only fall back to local close if clearinghouseState succeeds AND positively confirms absence
                let wallet = cfg.get_str("wallet_address", "").trim().to_string();
                let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
                let confirmed_absent_on_exchange = self.is_coin_confirmed_absent_on_exchange(
                    &wallet,
                    is_testnet,
                    &asset.base_asset,
                );
                if !confirmed_absent_on_exchange {
                    return Err(AegisError::Validation(format!(
                        "Exchange reduce-only close order for {} (#{}) did not fill (state={}) and exchange position absence could not be confirmed",
                        trade.symbol, trade.trade_id, exec_close.fsm_state
                    )));
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
        let closed = self.db.close_trade(
            trade.trade_id,
            actual_exit_px,
            net_pnl,
            actual_fees,
            reason,
        )?;
        if !closed {
            return Err(AegisError::Validation(format!(
                "Open trade #{trade_id} is already closed"
            )));
        }
        let _ = self.db.log_telemetry(
            "INFO",
            "engine.execution",
            &format!(
                "Manual take-profit / close executed for {} (#{} {}) at {:.4} (net_pnl={:+.4} USDC)",
                trade.symbol, trade.trade_id, trade.side, actual_exit_px, net_pnl
            ),
            Some(json!({
                "trade_id": trade.trade_id,
                "symbol": trade.symbol,
                "side": trade.side,
                "entry_price": trade.entry_price,
                "exit_price": actual_exit_px,
                "quantity": trade.quantity.abs(),
                "realized_pnl_usdc": net_pnl,
                "fees_paid_usdc": actual_fees,
                "exit_reason": reason,
            })),
        );
        let _ = self.reconcile_cycle_with_options(&[], false, true);

        Ok(json!({
            "status": "CLOSED",
            "trade_id": trade.trade_id,
            "symbol": trade.symbol,
            "side": trade.side,
            "quantity": trade.quantity.abs(),
            "entry_price": trade.entry_price,
            "exit_price": actual_exit_px,
            "realized_pnl_usdc": net_pnl,
            "fees_paid_usdc": actual_fees,
            "exit_reason": reason,
        }))
    }

    /// Manually closes / takes profit on an open or armed Book B (`POND`) cell (`cell_id`) (`R7-S3`, `R7-S4`, `R7-N6`, `R8-N6`).
    pub fn close_open_pond_cell_manual(
        &self,
        cell_id: i64,
        reason_override: Option<&str>,
    ) -> Result<Value> {
        let reason_owned = aegis_core::normalize_manual_exit_reason(reason_override);
        let reason = reason_owned.as_str();

        let open_cells = self.db.get_open_pond_cells()?;
        let cell = if let Some(c) = open_cells.into_iter().find(|c| c.cell_id == cell_id) {
            c
        } else {
            let armed_cells = self.db.get_armed_pond_cells()?;
            let Some(armed) = armed_cells.into_iter().find(|c| c.cell_id == cell_id) else {
                return Err(AegisError::Validation(format!(
                    "Open Pond cell #{cell_id} not found or already closed"
                )));
            };
            if let Some(ref oid_str) = armed.resting_order_id {
                if !oid_str.starts_with("hl-paper-") && !oid_str.starts_with("sim-") {
                    match self.cancel_resting_order(&armed.symbol, oid_str) {
                        Ok(true) => {}
                        Ok(false) => {
                            tracing::warn!(
                                cell_id = armed.cell_id,
                                symbol = %armed.symbol,
                                oid = %oid_str,
                                "Resting POND order cancel returned false during manual cancel (R10-N3)"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                cell_id = armed.cell_id,
                                symbol = %armed.symbol,
                                oid = %oid_str,
                                error = %e,
                                "Resting POND order cancel failed during manual cancel (R10-N3)"
                            );
                        }
                    }
                }
            }
            let ref_px = armed.entry_price.unwrap_or(0.0);
            let closed_armed = self
                .db
                .reconcile_close_phantom_pond_cell(armed.cell_id, ref_px, reason)?;
            if !closed_armed {
                return Err(AegisError::Validation(format!(
                    "Armed Pond cell #{cell_id} is already closed"
                )));
            }
            let _ = self.db.log_telemetry(
                "INFO",
                "engine.pond",
                &format!(
                    "Manual cancel executed for ARMED Pond cell #{} ({} {})",
                    armed.cell_id, armed.symbol, armed.side
                ),
                Some(json!({
                    "cell_id": armed.cell_id,
                    "symbol": armed.symbol,
                    "side": armed.side,
                    "exit_reason": reason,
                })),
            );
            return Ok(json!({
                "status": "CANCELLED_ARMED",
                "cell_id": armed.cell_id,
                "symbol": armed.symbol,
                "side": armed.side,
                "exit_reason": reason,
            }));
        };

        let cfg = self.db.load_config()?;
        let pond_mode = cfg
            .get_str(
                "pond_execution_mode",
                &cfg.get_str("execution_mode", "PAPER"),
            )
            .to_ascii_uppercase();
        let entry_px = cell.entry_price.unwrap_or(100.0);
        let qty = cell.quantity.unwrap_or(0.0).abs();
        let asset = self.resolve_trade_asset(&cell.symbol, "", entry_px);
        let bars = self
            .db
            .get_ohlcv_bars(&cell.symbol, "1h", 5, None)
            .unwrap_or_default();
        let last_bar = bars.last();
        let mark_px = last_bar
            .map(|b| b.close)
            .filter(|&p| p > 0.0)
            .unwrap_or(entry_px);
        let high_px = last_bar.map(|b| b.high).unwrap_or(mark_px);
        let low_px = last_bar.map(|b| b.low).unwrap_or(mark_px);
        let vol = last_bar.map(|b| b.volume).unwrap_or(50_000.0);
        let close_side = if cell.side == "LONG" { "SELL" } else { "BUY" };

        let fill = self.execute_pond_order(
            &cell.symbol,
            close_side,
            qty,
            mark_px,
            asset.tick_size,
            asset.lot_step_size,
            high_px,
            low_px,
            vol,
            true,
            false,
            true,
            None,
        );
        if (pond_mode == "LIVE" || pond_mode == "TESTNET") && fill.fsm_state != "FILLED" {
            let wallet = cfg.get_str("wallet_address", "").trim().to_string();
            let is_testnet =
                cfg.get_bool("hyperliquid_testnet", false) || pond_mode == "TESTNET";
            let confirmed_absent_on_exchange = self.is_coin_confirmed_absent_on_exchange(
                &wallet,
                is_testnet,
                &asset.base_asset,
            );
            if !confirmed_absent_on_exchange {
                return Err(AegisError::Validation(format!(
                    "Exchange reduce-only close order for Pond cell #{} ({}) did not fill (state={}) and exchange position absence could not be confirmed",
                    cell.cell_id, cell.symbol, fill.fsm_state
                )));
            }
        }
        let exit_px = if fill.avg_fill_price > 0.0 {
            fill.avg_fill_price
        } else {
            mark_px
        };
        if fill.filled_units > 0.0 && fill.filled_units + 1e-8 < qty {
            let partial_gross = if cell.side == "LONG" {
                (exit_px - entry_px) * fill.filled_units
            } else {
                (entry_px - exit_px) * fill.filled_units
            };
            let rem_qty = round_dp((qty - fill.filled_units).max(0.0), 6);
            let rem_notional = round_dp(rem_qty * entry_px, 2);
            let _ = self.db.reduce_open_pond_cell_partial_exit(
                cell.cell_id,
                &cell.state,
                cell.chandelier_stop,
                cell.highest_price_seen,
                cell.lowest_price_seen,
                rem_qty,
                rem_notional,
                partial_gross,
                fill.fees_paid_usdc,
            )?;
            return Ok(json!({
                "status": "PARTIAL_CLOSE",
                "cell_id": cell.cell_id,
                "symbol": cell.symbol,
                "side": cell.side,
                "filled_units": fill.filled_units,
                "remaining_units": rem_qty,
                "exit_price": exit_px,
                "realized_pnl_delta_usdc": round_dp(partial_gross - fill.fees_paid_usdc, 4),
                "exit_reason": reason,
            }));
        }
        let gross_pnl = if cell.side == "LONG" {
            (exit_px - entry_px) * qty
        } else {
            (entry_px - exit_px) * qty
        };
        let cd_hours = cfg.get_i64("pond_cooldown_hours", 4).max(1);
        let now = chrono::Utc::now();
        let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let cooldown_str = (now + chrono::Duration::hours(cd_hours))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        let closed = self.db.close_open_pond_cell(
            cell.cell_id,
            "COOLDOWN",
            cell.chandelier_stop,
            cell.highest_price_seen,
            cell.lowest_price_seen,
            exit_px,
            gross_pnl,
            fill.fees_paid_usdc,
            Some(reason),
            &now_str,
            &cooldown_str,
        )?;
        if !closed {
            return Err(AegisError::Validation(format!(
                "Open Pond cell #{cell_id} is already closed"
            )));
        }
        let net_pnl = round_dp(
            cell.realized_pnl_usdc + gross_pnl + cell.funding_accrued_usdc
                - (cell.fees_paid_usdc + fill.fees_paid_usdc),
            4,
        );
        let _ = self.db.log_telemetry(
            "INFO",
            "engine.pond",
            &format!(
                "Manual take-profit / close executed for Pond cell #{} ({} {}) at {:.4} (net_pnl={:+.4} USDC)",
                cell.cell_id, cell.symbol, cell.side, exit_px, net_pnl
            ),
            Some(json!({
                "cell_id": cell.cell_id,
                "symbol": cell.symbol,
                "side": cell.side,
                "entry_price": entry_px,
                "exit_price": exit_px,
                "quantity": qty,
                "realized_pnl_usdc": net_pnl,
                "exit_reason": reason,
            })),
        );

        Ok(json!({
            "status": "CLOSED",
            "cell_id": cell.cell_id,
            "symbol": cell.symbol,
            "side": cell.side,
            "quantity": qty,
            "entry_price": entry_px,
            "exit_price": exit_px,
            "realized_pnl_usdc": net_pnl,
            "exit_reason": reason,
        }))
    }
}
