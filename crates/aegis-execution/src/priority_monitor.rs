//! 1-Second High-Frequency Surveillance Loop (`aegis-execution::priority_monitor`).
//!
//! Queries `v_priority_watchlist`, `v_assets_ordered`, and `v_assets_held` every 1 second:
//! - Advances all `WORKING` Maker Limit Chaser orders (`v_assets_ordered`), promoting `WORKING -> OPEN` upon fill confirmation.
//! - Monitors all `OPEN` holdings (`v_assets_held`), updating live mark prices & unrealized PnL,
//!   ratcheting 1s High-Water Chandelier/ATR protective trailing stops, and executing immediate exits +
//!   inserting a post-stop cooldown (`asset_cooldowns`) + rich HTML Telegram alert on stop breach.

use crate::http_client::HyperliquidHttpClient;
use crate::order_chaser::{ChaseStepOutcome, OrderChaser};
use crate::signing::{build_limit_order_wire, snap_to_tick, HL_ORACLE_BAND_PCT};
use aegis_alerts::{AlertEngine, PositionClosedAlert};
use aegis_core::{now_ms, round_dp, HeldAssetView, OrderSide, Result};
use aegis_db::DatabaseManager;
use aegis_risk::{compute_atr_from_bars, RiskEngine};
use serde::{Deserialize, Serialize};
use serde_json::json;

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
        }
    }

    /// Runs a single 1-second surveillance tick over `v_priority_watchlist`, `v_assets_ordered`, and `v_assets_held`.
    pub fn run_1s_priority_tick(&self) -> Result<PriorityTickReport> {
        let ts = now_ms();
        let cfg = self.db.load_config_or_default();
        let watchlist = self.db.get_priority_watchlist()?;

        // Refresh live mid/mark quotes from Hyperliquid `allMids` if any assets are under 1s priority watch
        if !watchlist.is_empty() && cfg.execution_mode().is_live_or_testnet() {
            if let Ok(mids) = self.http.fetch_all_mids(&cfg) {
                for item in &watchlist {
                    if let Some(&live_mid) = mids.get(&item.coin) {
                        if live_mid > 0.0 {
                            let _ =
                                self.db
                                    .update_asset_live_quote(item.asset_id, live_mid, live_mid);
                        }
                    }
                }
            }
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
            let eval = risk.evaluate_1s_stop_ratchet(
                side,
                h.entry_price,
                live_mark,
                h.quantity,
                h.protective_stop_px,
                h.highest_price_seen,
                h.lowest_price_seen,
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
                    report.stops_triggered_closed += 1;
                }
            } else {
                self.db.update_open_position_mark_and_stop(
                    h.position_id,
                    live_mark,
                    eval.new_protective_stop_px,
                )?;
                if eval.stop_ratcheted {
                    report.stops_ratcheted += 1;
                }
            }
        }

        Ok(report)
    }

    /// Executes an immediate stop-out exit for an `OPEN` holding, inserts a post-stop cooldown
    /// into `asset_cooldowns`, logs telemetry, and dispatches a rich HTML Telegram alert.
    /// In `TESTNET`/`LIVE` mode, only closes in MariaDB and fires an alert if Hyperliquid confirms a fill;
    /// if the Testnet orderbook cannot fill the close order, re-anchors `protective_stop_px` beyond `exit_price`
    /// so it never enters a 1s alert/close loop.
    pub fn execute_protective_stop_exit(
        &self,
        held: &HeldAssetView,
        exit_price: f64,
        exit_reason: &str,
        risk: &RiskEngine,
    ) -> Result<Option<f64>> {
        let cfg = self.db.load_config_or_default();
        let side = OrderSide::parse(&held.side);
        let taker_fee_bps = risk.limits.taker_fee_bps;
        let default_stop_pct = risk.limits.protective_stop_default_pct.clamp(0.01, 0.25);
        let mut fees_paid_usdc =
            round_dp(exit_price * held.quantity * (taker_fee_bps / 10_000.0), 4);
        let mut eff_exit_price = exit_price;
        let mut live_close_oid: Option<String> = None;

        let is_live_exchange = cfg.execution_mode().is_live_or_testnet()
            && !cfg.get_str("private_key", "").is_empty()
            && !self.db.is_ephemeral_test_db();

        if is_live_exchange {
            let pk = cfg.get_str("private_key", "");
            let asset_opt = self.db.get_asset_by_id(held.asset_id)?;
            let tick = asset_opt
                .as_ref()
                .map(|a| a.tick_size)
                .unwrap_or(0.01)
                .max(1e-8);
            let mut filled_on_hl = false;

            if let Some(asset) = asset_opt {
                let exit_is_buy = !side.is_buy();
                let limit_px = if exit_is_buy {
                    exit_price * (1.0 + HL_ORACLE_BAND_PCT)
                } else {
                    exit_price * (1.0 - HL_ORACLE_BAND_PCT)
                };
                let wire = build_limit_order_wire(
                    asset.asset_index,
                    exit_is_buy,
                    limit_px,
                    held.quantity,
                    asset.tick_size,
                    asset.lot_step_size,
                    asset.sz_decimals,
                    true,
                    "Ioc",
                    Some(&format!("v2-stopexit-{}-{}", held.position_id, now_ms())),
                );
                if let Ok(ack) = self.http.submit_order_wire(
                    wire,
                    held.quantity,
                    exit_price,
                    taker_fee_bps,
                    &pk,
                    Some(&self.db),
                    &cfg,
                ) {
                    if ack.accepted && ack.status == "FILLED" && ack.filled_quantity > 0.0 {
                        filled_on_hl = true;
                        if ack.avg_fill_price > 0.0 {
                            eff_exit_price = ack.avg_fill_price;
                        }
                        fees_paid_usdc = ack.fee_usdc;
                        live_close_oid = ack.exchange_order_id;
                    }
                }
            }

            if !filled_on_hl {
                // Re-anchor protective_stop_px beyond current mark/exit price so it does not spam 1s stop attempts on an illiquid Testnet book
                let widened_stop = if side.is_buy() {
                    snap_to_tick(exit_price * (1.0 - default_stop_pct), tick)
                } else {
                    snap_to_tick(exit_price * (1.0 + default_stop_pct), tick)
                };
                self.db.update_open_position_mark_and_stop(
                    held.position_id,
                    exit_price,
                    widened_stop,
                )?;
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

        let cooldown_sec = risk.limits.stop_out_cooldown_sec.max(60);
        let cd_reason = format!(
            "Protective stop triggered on {} at ${:.4} (entry=${:.4}, PnL=${:+.2})",
            held.symbol, eff_exit_price, held.entry_price, realized_pnl
        );
        self.db
            .insert_asset_cooldown(held.asset_id, "STOP_OUT", &cd_reason, cooldown_sec)?;

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
            cooldown_type: Some("STOP_OUT".to_string()),
            cooldown_seconds: cooldown_sec,
            timestamp_ms: now_ms(),
        };
        let _ = self.alerts.notify_position_closed(&alert);

        Ok(Some(realized_pnl))
    }
}
