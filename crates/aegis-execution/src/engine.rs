//! Unified Aegis Quant v2.0 Execution Engine (`aegis-execution::engine`).
//!
//! Coordinates:
//! - 1-Second High-Frequency Priority Surveillance Loop (`PriorityMonitor`)
//! - 3-Second Full-Exchange 150+ Perp Market Watcher (`MarketWatcher`)
//! - 1-Second Maker Limit Chaser & Order-to-Open Capital Lifecycle (`OrderChaser`)
//! - Multi-Sleeve S1/S2/S3 Signal Evaluation & Risk-Tier Allocator (`aegis-signals`, `aegis-risk`)
//! - Dynamic Cutover & Live Hyperliquid Exchange Reconciliation (`CutoverReconciler`)
//! - Rich HTML Telegram Alerting (`aegis-alerts`)

use crate::http_client::HyperliquidHttpClient;
use crate::market_watcher::{MarketScanReport, MarketWatcher};
use crate::order_chaser::{NewWorkingOrderRequest, OrderChaser};
use crate::priority_monitor::{PriorityMonitor, PriorityTickReport};
use crate::reconcile::{CutoverReconcileReport, CutoverReconciler};
use crate::signing::{build_limit_order_wire, HL_ORACLE_BAND_PCT};
use aegis_alerts::{AlertEngine, CutoverReconcileAlert, PositionClosedAlert};
use aegis_core::{
    normalize_hyperliquid_symbol, now_ms, round_dp, AegisError, ExchangeAsset, OhlcvBar, OrderSide,
    PortfolioSnapshot, PositionStatus, Result,
};
use aegis_db::{DatabaseManager, SignalUpsertInput, SnapshotInsertInput};
use aegis_risk::{compute_atr_from_bars, RiskEngine};
use aegis_signals::{
    SignalCycleOutput, SignalEngine, ACTION_CLOSE, ACTION_FLIP, ACTION_OPEN_LONG, ACTION_OPEN_SHORT,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

/// Report returned by [`ExecutionEngine::run_cutover_reconciliation`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverFullReport {
    pub market_scan: MarketScanReport,
    pub reconcile: CutoverReconcileReport,
    pub signals_evaluated: usize,
    pub snapshot: PortfolioSnapshot,
    pub alert_status: String,
}

/// Summary of rebalance orders placed or positions closed during a signal rebalance pass.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RebalanceExecutionReport {
    pub working_orders_placed: usize,
    pub positions_closed: usize,
    pub placed_position_ids: Vec<i64>,
    pub closed_position_ids: Vec<i64>,
}

/// Combined telemetry report from a single end-to-end execution cycle (`run_single_cycle`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionCycleReport {
    pub cycle_at_ms: i64,
    pub market_scan: MarketScanReport,
    pub priority_tick: PriorityTickReport,
    pub signals_evaluated: usize,
    pub rebalance: RebalanceExecutionReport,
    pub snapshot: PortfolioSnapshot,
}

/// Result of a manual position close or working order cancellation (`POST /api/v1/positions/close`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManualCloseReport {
    pub position_id: i64,
    pub asset_id: i64,
    pub symbol: String,
    pub previous_status: String,
    pub final_status: String,
    pub exit_price: Option<f64>,
    pub realized_pnl_usdc: f64,
    pub released_reserved_usdc: f64,
    pub cooldown_inserted_sec: i64,
    pub reason: String,
}

/// Primary Aegis Quant v2.0 Execution Engine.
#[derive(Clone)]
pub struct ExecutionEngine {
    pub db: DatabaseManager,
    pub http: HyperliquidHttpClient,
    pub watcher: MarketWatcher,
    pub chaser: OrderChaser,
    pub priority_monitor: PriorityMonitor,
    pub reconciler: CutoverReconciler,
    pub alerts: AlertEngine,
}

impl ExecutionEngine {
    pub fn new(db: DatabaseManager) -> Self {
        Self::with_timeout(db, 8)
    }

    pub fn with_timeout(db: DatabaseManager, timeout_sec: u64) -> Self {
        let http = HyperliquidHttpClient::new(timeout_sec);
        let alerts = AlertEngine::new(db.clone());
        let watcher = MarketWatcher::new(db.clone(), http.clone());
        let chaser = OrderChaser::new(db.clone(), http.clone(), alerts.clone());
        let priority_monitor =
            PriorityMonitor::new(db.clone(), http.clone(), chaser.clone(), alerts.clone());
        let reconciler = CutoverReconciler::new(db.clone(), http.clone());

        Self {
            db,
            http,
            watcher,
            chaser,
            priority_monitor,
            reconciler,
            alerts,
        }
    }

    /// Executes the full v2.0 cutover sequence:
    /// 1. Discovers and seeds all 150+ Hyperliquid perpetual assets via `metaAndAssetCtxs` + `allMids`.
    /// 2. Seeds 1h/4h OHLCV candle series for core/event eligible assets.
    /// 3. Dynamically reconciles live Hyperliquid `clearinghouseState` and `openOrders` (cancelling stale legacy orders and closing/adopting legacy positions).
    /// 4. Computes and persists initial multi-sleeve S1/S2/S3 signals (`sleeve_signals`).
    /// 5. Records the baseline `PortfolioSnapshot` and dispatches the rich HTML Cutover Verified alert.
    pub fn run_cutover_reconciliation(
        &self,
        close_legacy_positions: bool,
    ) -> Result<CutoverFullReport> {
        let cfg = self.db.load_config_or_default();
        let market_scan = self.watcher.run_3s_exchange_scan()?;
        let _ = self.watcher.refresh_bars_for_eligible_assets(
            35,
            72,
            market_scan.live_exchange_connected,
            &cfg,
        );

        let reconcile = self
            .reconciler
            .run_cutover_reconciliation(close_legacy_positions)?;

        let signal_out = self.evaluate_and_persist_signals(reconcile.exchange_equity_usdc)?;
        let snapshot = self
            .record_portfolio_snapshot(Some(reconcile.exchange_equity_usdc), Some(&signal_out))?;

        let mariadb_ver = self
            .db
            .server_version()
            .unwrap_or_else(|_| "13.0.2-MariaDB".to_string());
        let cutover_alert = CutoverReconcileAlert {
            execution_mode: cfg.execution_mode().as_str().to_string(),
            mariadb_version: mariadb_ver,
            risk_tier: cfg.risk_tier(),
            leverage_multiplier: Some(cfg.target_leverage_multiplier()),
            discovered_perps: market_scan.total_assets_upserted,
            cancelled_legacy_orders: reconcile.stale_orders_cancelled,
            closed_legacy_positions: reconcile.legacy_positions_closed
                + reconcile.legacy_positions_adopted,
            total_equity_usdc: snapshot.total_equity_usdc,
            available_cash_usdc: snapshot.available_cash_usdc,
            timestamp_ms: now_ms(),
        };
        let alert_res = self.alerts.notify_cutover_reconciled(&cutover_alert);

        Ok(CutoverFullReport {
            market_scan,
            reconcile,
            signals_evaluated: signal_out.signals.len(),
            snapshot,
            alert_status: alert_res.status,
        })
    }

    /// Evaluates all three alpha sleeves (`S1 Carry-Momentum`, `S2 Trend-Breakout`, `S3 Event-Reversion`)
    /// across all discovered `exchange_assets`, persists `sleeve_signals`, and returns [`SignalCycleOutput`].
    pub fn evaluate_and_persist_signals(&self, base_equity_usdc: f64) -> Result<SignalCycleOutput> {
        let cfg = self.db.load_config_or_default();
        let assets = self.db.list_exchange_assets()?;
        let held = self.db.get_assets_held()?;
        let ordered = self.db.get_assets_ordered()?;
        let cooldowns = self.db.get_assets_in_cooldown()?;

        let eq = base_equity_usdc.max(100.0);
        let mut current_weights: HashMap<i64, f64> = HashMap::new();
        for h in &held {
            let side = OrderSide::parse(&h.side);
            let w = if h.portfolio_weight.abs() > 1e-6 {
                h.portfolio_weight.abs() * side.sign()
            } else {
                (h.committed_notional_usdc / eq) * side.sign()
            };
            *current_weights.entry(h.asset_id).or_insert(0.0) += w;
        }
        for o in &ordered {
            let side = OrderSide::parse(&o.side);
            let notional = o.limit_price * o.requested_quantity;
            let w = (notional / eq) * side.sign();
            *current_weights.entry(o.asset_id).or_insert(0.0) += w;
        }

        let mut bars_by_asset: HashMap<i64, Vec<OhlcvBar>> = HashMap::new();
        for asset in &assets {
            if !(asset.is_core_eligible || asset.is_event_eligible || asset.is_priority_watched) {
                continue;
            }
            let mut bars = self
                .db
                .get_ohlcv_bars(asset.id, "4h", 96)
                .unwrap_or_default();
            if bars.len() < 16 {
                bars = self
                    .db
                    .get_ohlcv_bars(asset.id, "1h", 96)
                    .unwrap_or_default();
            }
            if bars.len() < 16 && asset.mark_price > 0.0 {
                let synth_inputs = MarketWatcher::generate_anchored_bars(
                    asset.id,
                    &asset.symbol,
                    "4h",
                    48,
                    asset.mark_price,
                    asset.prev_day_price,
                    asset.funding_rate_hourly,
                    asset.day_volume_usdc,
                );
                let _ = self.db.upsert_ohlcv_bars(&synth_inputs);
                bars = self
                    .db
                    .get_ohlcv_bars(asset.id, "4h", 96)
                    .unwrap_or_default();
            }
            if !bars.is_empty() {
                bars_by_asset.insert(asset.id, bars);
            }
        }

        let sig_engine = SignalEngine::from_config_map(&cfg);
        let output =
            sig_engine.evaluate_universe(&assets, &bars_by_asset, &current_weights, &cooldowns);

        for sig in &output.signals {
            self.db.upsert_sleeve_signal(&SignalUpsertInput {
                asset_id: sig.asset_id,
                mark_price: sig.mark_price,
                day_volume_usdc: sig.day_volume_usdc,
                funding_rate_hourly: sig.funding_rate_hourly,
                annualized_vol: sig.annualized_vol,
                market_beta: sig.market_beta,
                s1_mom_score: sig.s1_mom_score,
                s1_carry_score: sig.s1_carry_score,
                s2_trend_alignment: sig.s2_trend_alignment,
                s2_vol_surge_ratio: sig.s2_vol_surge_ratio,
                s3_funding_zscore: sig.s3_funding_zscore,
                s3_flush_atr_mult: sig.s3_flush_atr_mult,
                w_s1_carry_mom: sig.w_s1_carry_mom,
                w_s2_trend_breakout: sig.w_s2_trend_breakout,
                w_s3_event_reversion: sig.w_s3_event_reversion,
                raw_combined_weight: sig.raw_combined_weight,
                target_weight: sig.target_weight,
                current_weight: sig.current_weight,
                delta_weight: sig.delta_weight,
                event_trigger: sig.event_trigger.clone(),
                rebalance_action: sig.rebalance_action.clone(),
            })?;

            if sig.event_trigger.is_some() {
                let _ = self.db.set_asset_priority_watch(sig.asset_id, true);
            }
        }

        Ok(output)
    }

    /// Executes portfolio rebalance actions (`CLOSE`, `FLIP`, `OPEN_LONG`, `OPEN_SHORT`)
    /// by closing exited positions and placing new `WORKING` (`WORKING_ALO`) orders via `OrderChaser`.
    pub fn execute_signal_rebalance(
        &self,
        signal_out: &SignalCycleOutput,
        base_equity_usdc: f64,
    ) -> Result<RebalanceExecutionReport> {
        let cfg = self.db.load_config_or_default();
        let risk = RiskEngine::from_config_map(&cfg);
        let held = self.db.get_assets_held()?;
        let ordered = self.db.get_assets_ordered()?;
        let mut ledger = risk.evaluate_ledger_from_views(base_equity_usdc, &held, &ordered);

        let assets = self.db.list_exchange_assets()?;
        let asset_map: HashMap<i64, ExchangeAsset> =
            assets.into_iter().map(|a| (a.id, a)).collect();

        let mut report = RebalanceExecutionReport::default();

        // 1. Handle CLOSE or FLIP on currently OPEN holdings
        for sig in &signal_out.signals {
            if sig.rebalance_action == ACTION_CLOSE || sig.rebalance_action == ACTION_FLIP {
                if let Some(h) = held.iter().find(|x| x.asset_id == sig.asset_id) {
                    let close_res =
                        self.close_or_cancel_position(h.position_id, &sig.rebalance_action, None)?;
                    report.positions_closed += 1;
                    report.closed_position_ids.push(close_res.position_id);
                }
            }
        }

        // 2. Place new WORKING orders for top actionable OPEN_LONG / OPEN_SHORT signals
        let max_working_orders = cfg.get_usize("max_concurrent_working_orders", 6).max(1);
        let mut current_working_count = ordered.len();

        for sig in &signal_out.signals {
            if current_working_count >= max_working_orders {
                break;
            }
            if sig.rebalance_action != ACTION_OPEN_LONG && sig.rebalance_action != ACTION_OPEN_SHORT
            {
                continue;
            }
            let Some(asset) = asset_map.get(&sig.asset_id) else {
                continue;
            };
            if self.db.is_asset_in_cooldown(asset.id).unwrap_or(false) {
                continue;
            }
            if self
                .db
                .get_active_position_for_asset(asset.id)
                .ok()
                .flatten()
                .is_some()
            {
                continue;
            }

            let bars_1h = self
                .db
                .get_ohlcv_bars(asset.id, "1h", 20)
                .unwrap_or_default();
            let atr_opt = (!bars_1h.is_empty()).then(|| {
                compute_atr_from_bars(
                    &bars_1h,
                    14,
                    asset.mark_price,
                    risk.limits.protective_stop_default_pct,
                )
            });

            let plan = risk.size_working_order_from_weights(
                asset,
                sig.target_weight,
                sig.current_weight,
                sig.w_s1_carry_mom,
                sig.w_s2_trend_breakout,
                sig.w_s3_event_reversion,
                &ledger,
                atr_opt,
            );
            if !plan.approved {
                continue;
            }

            let reason = sig
                .event_trigger
                .clone()
                .unwrap_or_else(|| sig.rebalance_action.clone());

            if let Ok(pos_id) = self.chaser.place_working_order(&NewWorkingOrderRequest {
                asset: asset.clone(),
                side: plan.side,
                requested_quantity: plan.requested_quantity,
                reserved_margin_usdc: plan.reserved_margin_usdc,
                portfolio_weight: plan.portfolio_weight,
                s1_weight_contrib: plan.s1_weight_contrib,
                s2_weight_contrib: plan.s2_weight_contrib,
                s3_weight_contrib: plan.s3_weight_contrib,
                protective_stop_px: plan.protective_stop_px,
                reason,
            }) {
                ledger.available_cash_usdc =
                    (ledger.available_cash_usdc - plan.reserved_margin_usdc).max(0.0);
                ledger.reserved_order_cash_usdc += plan.reserved_margin_usdc;
                current_working_count += 1;
                report.working_orders_placed += 1;
                report.placed_position_ids.push(pos_id);
            }
        }

        Ok(report)
    }

    /// Computes and persists a unified [`PortfolioSnapshot`] reflecting the strict Order-to-Open Capital Lifecycle:
    /// - `reserved_order_cash_usdc` from `v_assets_ordered` (`WORKING` orders)
    /// - `committed_notional_usdc` from `v_assets_held` (`OPEN` holdings)
    pub fn record_portfolio_snapshot(
        &self,
        base_equity_override: Option<f64>,
        signal_out: Option<&SignalCycleOutput>,
    ) -> Result<PortfolioSnapshot> {
        let cfg = self.db.load_config_or_default();
        let risk = RiskEngine::from_config_map(&cfg);
        let held = self.db.get_assets_held()?;
        let ordered = self.db.get_assets_ordered()?;
        let assets = self.db.list_exchange_assets()?;

        let closed_realized_pnl: f64 = self
            .db
            .list_closed_positions(200)
            .unwrap_or_default()
            .iter()
            .map(|p| p.realized_pnl_usdc)
            .sum();

        let seed_equity = base_equity_override
            .filter(|&v| v > 0.0)
            .unwrap_or_else(|| cfg.get_f64("initial_paper_equity_usdc", 10_000.0));
        let base_cash_equity = (seed_equity + closed_realized_pnl).max(100.0);

        let ledger = risk.evaluate_ledger_from_views(base_cash_equity, &held, &ordered);
        let eligible_count = assets
            .iter()
            .filter(|a| a.is_core_eligible || a.is_event_eligible)
            .count() as i32;

        let (beta_exp, breadth_bull, regime_label, s1_w, s2_w, s3_w) = match signal_out {
            Some(s) => (
                s.breadth.market_beta_exposure,
                s.breadth.market_breadth_bull,
                s.breadth.market_regime_label.clone(),
                s.s1_gross_weight,
                s.s2_gross_weight,
                s.s3_gross_weight,
            ),
            None => {
                let prev = self.db.get_latest_portfolio_snapshot()?;
                let s1_sum: f64 = held.iter().map(|h| h.s1_weight_contrib.abs()).sum();
                let s2_sum: f64 = held.iter().map(|h| h.s2_weight_contrib.abs()).sum();
                let s3_sum: f64 = held.iter().map(|h| h.s3_weight_contrib.abs()).sum();
                (
                    prev.market_beta_exposure,
                    prev.market_breadth_bull,
                    prev.market_regime_label,
                    if s1_sum > 0.0 {
                        round_dp(s1_sum, 4)
                    } else {
                        prev.s1_gross_weight
                    },
                    if s2_sum > 0.0 {
                        round_dp(s2_sum, 4)
                    } else {
                        prev.s2_gross_weight
                    },
                    if s3_sum > 0.0 {
                        round_dp(s3_sum, 4)
                    } else {
                        prev.s3_gross_weight
                    },
                )
            }
        };

        self.db.insert_portfolio_snapshot(&SnapshotInsertInput {
            total_equity_usdc: ledger.total_equity_usdc,
            available_cash_usdc: ledger.available_cash_usdc,
            reserved_order_cash_usdc: ledger.reserved_order_cash_usdc,
            committed_notional_usdc: ledger.committed_notional_usdc,
            net_notional_usdc: ledger.net_notional_usdc,
            gross_leverage_x: ledger.gross_leverage_x,
            net_leverage_x: ledger.net_leverage_x,
            market_beta_exposure: beta_exp,
            market_breadth_bull: breadth_bull,
            market_regime_label: regime_label,
            risk_tier: cfg.risk_tier().as_str().to_string(),
            leverage_multiplier: cfg.target_leverage_multiplier(),
            s1_gross_weight: s1_w,
            s2_gross_weight: s2_w,
            s3_gross_weight: s3_w,
            working_order_count: ledger.working_order_count,
            open_long_count: ledger.open_long_count,
            open_short_count: ledger.open_short_count,
            eligible_coin_count: eligible_count,
        })?;

        self.db.get_latest_portfolio_snapshot()
    }

    /// Runs a single unified execution cycle (3s Market Scan + 1s Priority Tick + Multi-Sleeve Signal Evaluation + Optional Order Placement + Snapshot).
    pub fn run_single_cycle(&self, execute_orders: bool) -> Result<ExecutionCycleReport> {
        let ts = now_ms();
        let cfg = self.db.load_config_or_default();
        let market_scan = self.watcher.run_3s_exchange_scan()?;
        let priority_tick = self.priority_monitor.run_1s_priority_tick()?;

        let base_equity = cfg.get_f64("initial_paper_equity_usdc", 10_000.0);
        let signal_out = self.evaluate_and_persist_signals(base_equity)?;

        let rebalance = if execute_orders {
            self.execute_signal_rebalance(&signal_out, base_equity)?
        } else {
            RebalanceExecutionReport::default()
        };

        let snapshot = self.record_portfolio_snapshot(Some(base_equity), Some(&signal_out))?;
        let _ = self.alerts.poll_pending_queue(8);

        Ok(ExecutionCycleReport {
            cycle_at_ms: ts,
            market_scan,
            priority_tick,
            signals_evaluated: signal_out.signals.len(),
            rebalance,
            snapshot,
        })
    }

    /// Closes an `OPEN` position or cancels a `WORKING` order by `position_id`, optionally inserting a cooldown.
    pub fn close_or_cancel_position(
        &self,
        position_id: i64,
        reason: &str,
        cooldown_seconds: Option<i64>,
    ) -> Result<ManualCloseReport> {
        let pos = self
            .db
            .get_position_by_id(position_id)?
            .ok_or_else(|| AegisError::Validation(format!("Position #{position_id} not found")))?;
        let asset = self
            .db
            .get_asset_by_id(pos.asset_id)?
            .ok_or_else(|| AegisError::Database(format!("Asset #{} not found", pos.asset_id)))?;
        let cfg = self.db.load_config_or_default();
        let status = PositionStatus::parse(&pos.status);

        let mut exit_price_opt = None;
        let mut realized_pnl = 0.0;
        let mut released_reserved = 0.0;
        let final_status: String;

        match status {
            PositionStatus::Working => {
                if cfg.execution_mode().is_live_or_testnet() {
                    let pk = cfg.get_str("private_key", "");
                    if !pk.is_empty() {
                        if let Some(oid) = pos
                            .exchange_order_id
                            .as_deref()
                            .and_then(|s| s.strip_prefix("hl-").or(Some(s)))
                            .and_then(|s| s.parse::<u64>().ok())
                        {
                            let _ = self.http.cancel_order_wire(
                                asset.asset_index,
                                oid,
                                &pk,
                                Some(&self.db),
                                &cfg,
                            );
                        }
                    }
                }
                released_reserved = pos.reserved_margin_usdc;
                self.db.cancel_working_order(position_id, reason)?;
                final_status = "CANCELLED".to_string();
            }
            PositionStatus::Open => {
                let side = OrderSide::parse(&pos.side);
                let mark_px = if asset.mark_price > 0.0 {
                    asset.mark_price
                } else {
                    pos.current_price.max(pos.entry_price)
                };
                let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);
                let mut exit_px = mark_px;
                let mut fee_usdc = round_dp(
                    exit_px * pos.filled_quantity * (taker_fee_bps / 10_000.0),
                    4,
                );

                if cfg.execution_mode().is_live_or_testnet() {
                    let pk = cfg.get_str("private_key", "");
                    if !pk.is_empty() {
                        let close_is_buy = !side.is_buy();
                        let limit_px = if close_is_buy {
                            mark_px * (1.0 + HL_ORACLE_BAND_PCT)
                        } else {
                            mark_px * (1.0 - HL_ORACLE_BAND_PCT)
                        };
                        let wire = build_limit_order_wire(
                            asset.asset_index,
                            close_is_buy,
                            limit_px,
                            pos.filled_quantity,
                            asset.tick_size,
                            asset.lot_step_size,
                            asset.sz_decimals,
                            true,
                            "Ioc",
                            Some(&format!("v2-close-{position_id}-{}", now_ms())),
                        );
                        if let Ok(ack) = self.http.submit_order_wire(
                            wire,
                            pos.filled_quantity,
                            mark_px,
                            taker_fee_bps,
                            &pk,
                            Some(&self.db),
                            &cfg,
                        ) {
                            if ack.status == "FILLED" && ack.avg_fill_price > 0.0 {
                                exit_px = ack.avg_fill_price;
                                fee_usdc = ack.fee_usdc;
                            }
                        }
                    }
                }

                realized_pnl = self
                    .db
                    .close_position(position_id, exit_px, fee_usdc, reason)?;
                exit_price_opt = Some(exit_px);
                final_status = "CLOSED".to_string();

                let cd_sec = cooldown_seconds.unwrap_or(0).max(0);
                let snap = self.record_portfolio_snapshot(None, None)?;
                let alert = PositionClosedAlert {
                    symbol: asset.symbol.clone(),
                    side,
                    exit_reason: reason.to_string(),
                    entry_price: pos.entry_price,
                    exit_price: exit_px,
                    filled_qty: pos.filled_quantity,
                    realized_pnl_usdc: realized_pnl,
                    available_cash_usdc: snap.available_cash_usdc,
                    cooldown_type: (cd_sec > 0).then(|| "MANUAL".to_string()),
                    cooldown_seconds: cd_sec,
                    timestamp_ms: now_ms(),
                };
                let _ = self.alerts.notify_position_closed(&alert);
            }
            PositionStatus::Closed | PositionStatus::Cancelled => {
                final_status = pos.status.clone();
            }
        }

        let cd_sec = cooldown_seconds.unwrap_or(0).max(0);
        if cd_sec > 0 {
            self.db.insert_asset_cooldown(
                asset.id,
                "MANUAL",
                &format!("Manual close/cancel cooldown: {reason}"),
                cd_sec,
            )?;
        }

        let _ = self.record_portfolio_snapshot(None, None);
        let _ = self.db.log_telemetry(
            "INFO",
            "execution.engine",
            &format!(
                "Position #{position_id} ({}) transitioned {} -> {} (reason={reason})",
                asset.symbol, pos.status, final_status
            ),
            Some(json!({
                "position_id": position_id,
                "symbol": asset.symbol,
                "previous_status": pos.status,
                "final_status": final_status,
                "exit_price": exit_price_opt,
                "realized_pnl_usdc": realized_pnl,
                "cooldown_seconds": cd_sec,
            })),
        );

        Ok(ManualCloseReport {
            position_id,
            asset_id: asset.id,
            symbol: asset.symbol,
            previous_status: pos.status,
            final_status,
            exit_price: exit_price_opt,
            realized_pnl_usdc: realized_pnl,
            released_reserved_usdc: released_reserved,
            cooldown_inserted_sec: cd_sec,
            reason: reason.to_string(),
        })
    }

    /// Closes an `OPEN` position or cancels a `WORKING` order by canonical or raw symbol (`"SOL-USDC"`, `"SOL"`).
    pub fn close_or_cancel_by_symbol(
        &self,
        raw_symbol: &str,
        reason: &str,
        cooldown_seconds: Option<i64>,
    ) -> Result<ManualCloseReport> {
        let (symbol, _) = normalize_hyperliquid_symbol(raw_symbol)?;
        let asset = self
            .db
            .get_asset_by_symbol(&symbol)?
            .ok_or_else(|| AegisError::Validation(format!("Asset '{symbol}' not found")))?;
        let active = self
            .db
            .get_active_position_for_asset(asset.id)?
            .ok_or_else(|| {
                AegisError::Validation(format!("No active WORKING or OPEN position for '{symbol}'"))
            })?;
        self.close_or_cancel_position(active.id, reason, cooldown_seconds)
    }

    /// Spawns the unified v2.0 Two-Tier background runtime (1s Priority Loop + 3s Full-Exchange Scanner)
    /// onto dedicated OS threads and returns a shutdown sender (`send(true)` stops the runtime).
    pub fn spawn_background_runtime(&self) -> tokio::sync::watch::Sender<bool> {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let engine = self.clone();
        std::thread::spawn(move || {
            let mut tick_counter = 0u64;
            while !*rx.borrow() {
                let cfg = engine.db.load_config_or_default();
                let prio_ms = cfg
                    .get_i64("priority_monitor_interval_ms", 1000)
                    .clamp(250, 5000) as u64;
                let scan_ratio = (cfg.get_i64("scanner_interval_ms", 3000).max(1000) as u64
                    / prio_ms.max(1))
                .max(1);

                if tick_counter.is_multiple_of(scan_ratio) {
                    let execute_orders = cfg.get_bool("auto_rebalance_enabled", true);
                    if let Err(e) = engine.run_single_cycle(execute_orders) {
                        tracing::warn!(error = %e, "3s exchange scan & signal cycle error");
                    }
                } else if let Err(e) = engine.priority_monitor.run_1s_priority_tick() {
                    tracing::warn!(error = %e, "1s priority watchlist tick error");
                }

                tick_counter = tick_counter.wrapping_add(1);
                std::thread::sleep(Duration::from_millis(prio_ms));
            }
        });
        tx
    }
}
