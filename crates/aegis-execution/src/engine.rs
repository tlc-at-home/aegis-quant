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
use crate::signing::{
    derive_hl_cloid_hex, format_hl_wire_price, format_hl_wire_size, CancelWire, LimitOrderType,
    OrderTypeWire, OrderWire, HL_ORACLE_BAND_PCT,
};
use aegis_alerts::{
    format_position_closed_html, AlertEngine, CutoverReconcileAlert, PositionClosedAlert,
};
use aegis_core::{
    normalize_hyperliquid_symbol, now_ms, round_dp, AegisError, ConfigMap, ExchangeAsset,
    HeldAssetView, OhlcvBar, OrderSide, PortfolioSnapshot, PositionStatus, Result,
};
use aegis_db::{DatabaseManager, SignalUpsertInput, SnapshotInsertInput};
use aegis_risk::{compute_atr_from_bars, RiskEngine};
use aegis_signals::{
    SignalCycleOutput, SignalEngine, ACTION_CLOSE, ACTION_FLIP, ACTION_OPEN_LONG, ACTION_OPEN_SHORT,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
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
    pub illiquid_close_backoff_ms: Arc<Mutex<HashMap<i64, i64>>>,
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
            illiquid_close_backoff_ms: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn is_illiquid_close_backed_off(&self, position_id: i64, now: i64) -> bool {
        self.illiquid_close_backoff_ms
            .lock()
            .get(&position_id)
            .is_some_and(|&ts| now.saturating_sub(ts) < 180_000)
    }

    fn cancel_coin_exchange_orders(
        &self,
        asset: &ExchangeAsset,
        wallet: &str,
        pk: &str,
        cfg: &ConfigMap,
        include_triggers: bool,
    ) {
        if wallet.is_empty() {
            return;
        }
        if let Ok(open_orders) = self
            .http
            .fetch_frontend_open_orders(wallet, cfg)
            .or_else(|_| self.http.fetch_open_orders(wallet, cfg))
        {
            let mut cancels = Vec::new();
            for ord in open_orders {
                let c = ord.get("coin").and_then(|v| v.as_str()).unwrap_or("");
                let is_trig = ord
                    .get("isTrigger")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if c.eq_ignore_ascii_case(&asset.coin) && (include_triggers || !is_trig) {
                    if let Some(oid) = ord.get("oid").and_then(|v| v.as_u64()).or_else(|| {
                        ord.get("oid")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<u64>().ok())
                    }) {
                        cancels.push(CancelWire {
                            a: asset.asset_index,
                            o: oid,
                        });
                    }
                }
            }
            if !cancels.is_empty() {
                let _ = self
                    .http
                    .cancel_orders_batch(cancels, pk, Some(&self.db), cfg);
            }
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
                let use_live_candles =
                    cfg.execution_mode().is_live_or_testnet() && !self.db.is_ephemeral_test_db();
                let bar_inputs = self.watcher.fetch_or_synthesize_bars(
                    asset,
                    "4h",
                    72,
                    use_live_candles,
                    &cfg,
                );
                if !bar_inputs.is_empty() {
                    let _ = self.db.upsert_ohlcv_bars(&bar_inputs);
                }
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

    /// Resolves the active base portfolio equity in USDC:
    /// - In `TESTNET`/`LIVE` mode: strictly uses `live_exchange_equity_usdc` (`marginSummary.accountValue`) when `> 10.0`.
    /// - In `PAPER` mode: uses `initial_paper_equity_usdc` (`$10,000.0`).
    pub fn resolve_effective_base_equity_usdc(&self, cfg: &aegis_core::ConfigMap) -> f64 {
        if cfg.execution_mode().is_live_or_testnet() {
            let live_eq = cfg.get_f64("live_exchange_equity_usdc", 0.0);
            if live_eq > 10.0 {
                return live_eq;
            }
        }
        cfg.get_f64("initial_paper_equity_usdc", 10_000.0)
            .max(100.0)
    }

    /// Enforces `max_open_positions` (default `6`) and the hard gross-leverage ceiling
    /// (`base_equity_usdc * target_leverage_multiplier`), automatically cancelling excess `WORKING`
    /// orders and closing excess `OPEN` positions (weakest/opposite signal alignment or largest notional first).
    pub fn enforce_leverage_and_position_caps(&self) -> Result<Vec<ManualCloseReport>> {
        let cfg = self.db.load_config_or_default();
        let max_open_positions = cfg.get_usize("max_open_positions", 6).max(1);
        let base_equity_usdc = self.resolve_effective_base_equity_usdc(&cfg);
        let target_mult = cfg.target_leverage_multiplier();
        let max_gross_notional = base_equity_usdc * target_mult;

        let mut closed_reports = Vec::new();
        let ordered = self.db.get_assets_ordered()?;
        let mut held = self.db.get_assets_held()?;

        let mut current_committed: f64 = held.iter().map(|h| h.committed_notional_usdc).sum();
        let mut current_reserved: f64 = ordered
            .iter()
            .map(|o| o.limit_price * o.requested_quantity)
            .sum();
        let mut total_active = held.len() + ordered.len();

        // 1. Cancel WORKING orders first if position count or gross leverage is above cap
        if total_active > max_open_positions
            || (current_committed + current_reserved) > max_gross_notional * 1.03
        {
            for o in &ordered {
                if total_active <= max_open_positions
                    && (current_committed + current_reserved) <= max_gross_notional * 1.03
                {
                    break;
                }
                if let Ok(rep) = self.close_or_cancel_position(
                    o.position_id,
                    "AUTO_DELEVERAGE_WORKING_CANCEL",
                    Some(300),
                ) {
                    current_reserved =
                        (current_reserved - (o.limit_price * o.requested_quantity)).max(0.0);
                    total_active = total_active.saturating_sub(1);
                    closed_reports.push(rep);
                }
            }
        }

        // 2. Close excess OPEN holdings if open count > max_open_positions or committed_notional > max_gross_notional * 1.03
        let check_ts = now_ms();
        let mut open_count = held.len();
        for h in &held {
            if self.is_illiquid_close_backed_off(h.position_id, check_ts) {
                current_committed = (current_committed - h.committed_notional_usdc).max(0.0);
                open_count = open_count.saturating_sub(1);
            }
        }
        if open_count > max_open_positions || current_committed > max_gross_notional * 1.03 {
            held.sort_by(|a, b| {
                let a_align = a.latest_target_weight * OrderSide::parse(&a.side).sign();
                let b_align = b.latest_target_weight * OrderSide::parse(&b.side).sign();
                a_align
                    .partial_cmp(&b_align)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        b.committed_notional_usdc
                            .partial_cmp(&a.committed_notional_usdc)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
            });

            for h in &held {
                if self.is_illiquid_close_backed_off(h.position_id, check_ts) {
                    continue;
                }
                if open_count <= max_open_positions && current_committed <= max_gross_notional {
                    break;
                }
                let deleverage_cd =
                    if h.latest_target_weight * OrderSide::parse(&h.side).sign() < 0.0 {
                        0
                    } else {
                        900
                    };
                if let Ok(rep) = self.close_or_cancel_position(
                    h.position_id,
                    "AUTO_DELEVERAGE_RISK_CAP",
                    Some(deleverage_cd),
                ) {
                    if rep.final_status == "CLOSED" {
                        current_committed =
                            (current_committed - h.committed_notional_usdc).max(0.0);
                        open_count = open_count.saturating_sub(1);
                        closed_reports.push(rep);
                    }
                }
            }
        }

        if !closed_reports.is_empty() {
            let _ = self.record_portfolio_snapshot(Some(base_equity_usdc), None);
        }

        Ok(closed_reports)
    }

    /// Executes portfolio rebalance actions (`CLOSE`, `FLIP`, `OPEN_LONG`, `OPEN_SHORT`)
    /// while strictly enforcing:
    /// - Automatic deleveraging when `open_positions > max_open_positions` or `committed_notional > base_equity * target_mult`
    /// - `max_open_positions` hard cap (default `6`)
    /// - `min_entry_interval_sec` cooldown between new position entries (default `150` seconds, max 1 new entry per cycle)
    /// - Hard gross-leverage gate (`committed_notional + reserved + order_notional <= base_equity * target_mult`)
    pub fn execute_signal_rebalance(
        &self,
        signal_out: &SignalCycleOutput,
        base_equity_usdc: f64,
    ) -> Result<RebalanceExecutionReport> {
        let cfg = self.db.load_config_or_default();
        let eff_equity = if cfg.execution_mode().is_live_or_testnet() {
            self.resolve_effective_base_equity_usdc(&cfg)
        } else {
            base_equity_usdc.max(100.0)
        };
        let mut report = RebalanceExecutionReport::default();

        // 0. Enforce leverage & max_open_positions caps first (auto-deleveraging if over limit)
        if let Ok(deleveraged) = self.enforce_leverage_and_position_caps() {
            for r in deleveraged {
                report.positions_closed += 1;
                report.closed_position_ids.push(r.position_id);
            }
        }

        let held = self.db.get_assets_held()?;

        let rebalance_now = now_ms();

        // 1. Handle CLOSE or FLIP on currently OPEN holdings
        for sig in &signal_out.signals {
            if sig.rebalance_action == ACTION_CLOSE || sig.rebalance_action == ACTION_FLIP {
                if let Some(h) = held.iter().find(|x| x.asset_id == sig.asset_id) {
                    if report.closed_position_ids.contains(&h.position_id)
                        || self.is_illiquid_close_backed_off(h.position_id, rebalance_now)
                    {
                        continue;
                    }
                    let close_cd = if sig.rebalance_action == ACTION_FLIP {
                        Some(0)
                    } else {
                        Some(300)
                    };
                    let close_res = self.close_or_cancel_position(
                        h.position_id,
                        &sig.rebalance_action,
                        close_cd,
                    )?;
                    report.positions_closed += 1;
                    report.closed_position_ids.push(close_res.position_id);
                }
            }
        }

        // 2. Re-evaluate active holdings/orders and enforce max_open_positions (6) & min_entry_interval_sec (150s)
        let held_now = self.db.get_assets_held()?;
        let tradable_held: Vec<HeldAssetView> = held_now
            .into_iter()
            .filter(|h| !self.is_illiquid_close_backed_off(h.position_id, rebalance_now))
            .collect();
        let ordered_now = self.db.get_assets_ordered()?;
        let max_open_positions = cfg.get_usize("max_open_positions", 6).max(1);
        let min_entry_interval_sec = cfg.get_i64("min_entry_interval_sec", 150).max(0);
        let total_active = tradable_held.len() + ordered_now.len();

        if total_active >= max_open_positions {
            return Ok(report);
        }

        let has_flip_candidate = signal_out
            .signals
            .iter()
            .any(|s| s.rebalance_action == ACTION_FLIP);
        if min_entry_interval_sec > 0 && !has_flip_candidate {
            if let Ok(Some(last_entry_ms)) = self.db.last_position_entry_timestamp_ms() {
                if now_ms().saturating_sub(last_entry_ms) < min_entry_interval_sec * 1000 {
                    return Ok(report);
                }
            }
        }

        let risk = RiskEngine::from_config_map(&cfg);
        let ledger = risk.evaluate_ledger_from_views(eff_equity, &tradable_held, &ordered_now);
        let target_mult = cfg.target_leverage_multiplier();
        let max_allowed_gross_notional = eff_equity * target_mult;
        let max_per_pos_weight = ((target_mult / (max_open_positions as f64)) * 1.25)
            .min(risk.limits.max_single_asset_weight);

        let assets = self.db.list_exchange_assets()?;
        let asset_map: HashMap<i64, ExchangeAsset> =
            assets.into_iter().map(|a| (a.id, a)).collect();

        let mut candidate_signals = signal_out.signals.clone();
        candidate_signals.sort_by(|a, b| {
            let a_prio = ((a.rebalance_action == ACTION_FLIP) as i32) * 2
                + ((a.event_trigger.as_deref() == Some("CAPITULATION_FLUSH")
                    && a.s1_mom_score > 0.0) as i32);
            let b_prio = ((b.rebalance_action == ACTION_FLIP) as i32) * 2
                + ((b.event_trigger.as_deref() == Some("CAPITULATION_FLUSH")
                    && b.s1_mom_score > 0.0) as i32);
            b_prio.cmp(&a_prio).then_with(|| {
                b.target_weight
                    .abs()
                    .partial_cmp(&a.target_weight.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
        });

        // 3. Place at most 1 new WORKING order per cycle when all gates pass
        for sig in &candidate_signals {
            if report.working_orders_placed >= 1
                || (total_active + report.working_orders_placed) >= max_open_positions
            {
                break;
            }
            if sig.rebalance_action != ACTION_OPEN_LONG
                && sig.rebalance_action != ACTION_OPEN_SHORT
                && sig.rebalance_action != ACTION_FLIP
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

            let capped_target_weight = sig
                .target_weight
                .clamp(-max_per_pos_weight, max_per_pos_weight);

            let effective_current_weight = if sig.rebalance_action == ACTION_FLIP {
                0.0
            } else {
                sig.current_weight
            };
            let plan = risk.size_working_order_from_weights(
                asset,
                capped_target_weight,
                effective_current_weight,
                sig.w_s1_carry_mom,
                sig.w_s2_trend_breakout,
                sig.w_s3_event_reversion,
                &ledger,
                atr_opt,
            );
            if !plan.approved {
                continue;
            }

            // Hard Gross-Leverage Gate anchored strictly to effective equity * target_leverage_multiplier
            let current_reserved_notional = ledger.reserved_order_cash_usdc * target_mult.max(1.0);
            if ledger.committed_notional_usdc
                + current_reserved_notional
                + plan.target_notional_usdc
                > max_allowed_gross_notional + 1e-2
            {
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
                report.working_orders_placed += 1;
                report.placed_position_ids.push(pos_id);
                break;
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

        let live_eq_cfg = cfg.get_f64("live_exchange_equity_usdc", 0.0);
        let base_cash_equity = if let Some(explicit_eq) = base_equity_override.filter(|&v| v > 0.0)
        {
            if cfg.execution_mode().is_live_or_testnet() {
                let upnl_sum: f64 = held.iter().map(|h| h.unrealized_pnl_usdc).sum();
                (explicit_eq - upnl_sum).max(1.0)
            } else {
                let closed_realized_pnl: f64 = self
                    .db
                    .list_closed_positions(200)
                    .unwrap_or_default()
                    .iter()
                    .map(|p| p.realized_pnl_usdc)
                    .sum();
                (explicit_eq + closed_realized_pnl).max(100.0)
            }
        } else if cfg.execution_mode().is_live_or_testnet() && live_eq_cfg > 0.0 {
            let upnl_sum: f64 = held.iter().map(|h| h.unrealized_pnl_usdc).sum();
            (live_eq_cfg - upnl_sum).max(1.0)
        } else {
            let closed_realized_pnl: f64 = self
                .db
                .list_closed_positions(200)
                .unwrap_or_default()
                .iter()
                .map(|p| p.realized_pnl_usdc)
                .sum();
            let seed_equity = cfg.get_f64("initial_paper_equity_usdc", 10_000.0);
            (seed_equity + closed_realized_pnl).max(100.0)
        };

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

        let base_equity = if cfg.execution_mode().is_live_or_testnet() {
            let live_eq = cfg.get_f64("live_exchange_equity_usdc", 0.0);
            if live_eq > 0.0 {
                live_eq
            } else {
                cfg.get_f64("initial_paper_equity_usdc", 10_000.0)
            }
        } else {
            cfg.get_f64("initial_paper_equity_usdc", 10_000.0)
        };
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

    /// Synchronizes MariaDB `positions` and `portfolio_snapshots` 1:1 with live Hyperliquid `clearinghouseState`.
    pub fn sync_with_hyperliquid(&self) -> Result<CutoverFullReport> {
        self.run_cutover_reconciliation(false)
    }

    /// Closes an `OPEN` position or cancels a `WORKING` order by `position_id`, inserting a cooldown
    /// (defaults to 3600s / 1h when `cooldown_seconds` is `None` so manual closes are not immediately re-entered).
    /// In `TESTNET`/`LIVE` mode, cancels any resting `openOrders` on `asset.coin`, queries live Hyperliquid
    /// `clearinghouseState` for the actual `szi`, and submits a `reduceOnly: true` IOC market close order
    /// for the exact on-chain quantity synchronously before marking the position `CLOSED` in MariaDB.
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
        let cd_sec = cooldown_seconds.unwrap_or(3600).max(0);
        let is_live_exchange = cfg.execution_mode().is_live_or_testnet()
            && !cfg.get_str("private_key", "").is_empty()
            && !self.db.is_ephemeral_test_db();

        let mut exit_price_opt = None;
        let mut realized_pnl = 0.0;
        let mut released_reserved = 0.0;
        let mut exchange_close_unfilled = false;
        let final_status: String;
        let mut pending_close_alert: Option<PositionClosedAlert> = None;

        match status {
            PositionStatus::Working => {
                if is_live_exchange {
                    let pk = cfg.get_str("private_key", "");
                    let wallet = cfg.get_str("wallet_address", "");
                    let mut cancels = Vec::new();
                    if let Some(oid) = pos
                        .exchange_order_id
                        .as_deref()
                        .and_then(|s| s.strip_prefix("hl-").or(Some(s)))
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        cancels.push(CancelWire {
                            a: asset.asset_index,
                            o: oid,
                        });
                    } else if !wallet.is_empty() {
                        if let Ok(open_orders) = self
                            .http
                            .fetch_frontend_open_orders(&wallet, &cfg)
                            .or_else(|_| self.http.fetch_open_orders(&wallet, &cfg))
                        {
                            for ord in open_orders {
                                let c = ord.get("coin").and_then(|v| v.as_str()).unwrap_or("");
                                if c.eq_ignore_ascii_case(&asset.coin) {
                                    if let Some(oid) =
                                        ord.get("oid").and_then(|v| v.as_u64()).or_else(|| {
                                            ord.get("oid")
                                                .and_then(|v| v.as_str())
                                                .and_then(|s| s.parse::<u64>().ok())
                                        })
                                    {
                                        cancels.push(CancelWire {
                                            a: asset.asset_index,
                                            o: oid,
                                        });
                                    }
                                }
                            }
                        }
                    }
                    if !cancels.is_empty() {
                        let _ = self
                            .http
                            .cancel_orders_batch(cancels, &pk, Some(&self.db), &cfg);
                    }
                }
                released_reserved = pos.reserved_margin_usdc;
                self.db.cancel_working_order(position_id, reason)?;
                final_status = "CANCELLED".to_string();
            }
            PositionStatus::Open => {
                let side = OrderSide::parse(&pos.side);
                let mut exit_px = if asset.mark_price > 0.0 {
                    asset.mark_price
                } else {
                    pos.current_price.max(pos.entry_price)
                };
                let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);
                let mut closed_qty = pos.filled_quantity;
                let mut fee_usdc = round_dp(exit_px * closed_qty * (taker_fee_bps / 10_000.0), 4);

                if is_live_exchange {
                    let pk = cfg.get_str("private_key", "");
                    let wallet = cfg.get_str("wallet_address", "");

                    // 1. Cancel ONLY non-trigger open orders first; preserve resting SL/TP trigger orders until close fills
                    self.cancel_coin_exchange_orders(&asset, &wallet, &pk, &cfg, false);

                    if !wallet.is_empty() {
                        // 2. Query live Hyperliquid clearinghouseState for exact `szi` on this coin
                        if let Ok(ch_state) = self.http.fetch_clearinghouse_state(&wallet, &cfg) {
                            let parse_f = |v: Option<&serde_json::Value>| -> f64 {
                                v.and_then(|x| {
                                    x.as_f64()
                                        .or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))
                                })
                                .filter(|val| val.is_finite())
                                .unwrap_or(0.0)
                            };
                            let mut live_szi = 0.0;
                            let mut live_pos_val = 0.0;
                            if let Some(arr) =
                                ch_state.get("assetPositions").and_then(|v| v.as_array())
                            {
                                for item in arr {
                                    let p = item.get("position").unwrap_or(item);
                                    let c = p.get("coin").and_then(|v| v.as_str()).unwrap_or("");
                                    if c.eq_ignore_ascii_case(&asset.coin) {
                                        live_szi = parse_f(p.get("szi"));
                                        live_pos_val = parse_f(p.get("positionValue"));
                                        break;
                                    }
                                }
                            }

                            if live_szi.abs() > 1e-9 {
                                let live_qty = live_szi.abs();
                                closed_qty = live_qty;
                                let close_is_buy = live_szi < 0.0;
                                if live_pos_val > 0.0 {
                                    exit_px = round_dp(live_pos_val / live_qty, 6);
                                }

                                // Submit reduceOnly IOC order for exact live_qty (progressive oracle band + l2Book BBO fallback)
                                let mut filled_on_hl = false;
                                let mut close_eoid_opt: Option<String> = None;
                                let l2_bbo =
                                    self.http.fetch_l2_book(&asset.coin, &cfg).ok().flatten();
                                let bbo_anchor = l2_bbo
                                    .map(|(bid, ask)| if close_is_buy { ask } else { bid })
                                    .filter(|&px| px > 0.0)
                                    .unwrap_or(exit_px);

                                let mut rem_qty = live_qty;
                                for (attempt, (ref_anchor, slip)) in [
                                    (bbo_anchor, 0.0),
                                    (bbo_anchor, 0.01),
                                    (bbo_anchor, 0.025),
                                    (exit_px, HL_ORACLE_BAND_PCT),
                                    (bbo_anchor, 0.05),
                                    (exit_px, 0.075),
                                    (bbo_anchor, 0.075),
                                    (bbo_anchor, 0.12),
                                    (exit_px, 0.12),
                                ]
                                .into_iter()
                                .enumerate()
                                {
                                    if rem_qty <= 1e-9 {
                                        break;
                                    }
                                    let limit_px = if close_is_buy {
                                        ref_anchor * (1.0 + slip)
                                    } else {
                                        ref_anchor * (1.0 - slip)
                                    };
                                    let wire = OrderWire {
                                        a: asset.asset_index,
                                        b: close_is_buy,
                                        p: format_hl_wire_price(
                                            limit_px,
                                            asset.tick_size,
                                            asset.sz_decimals,
                                        ),
                                        s: format_hl_wire_size(
                                            rem_qty,
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
                                            "v2-close-{position_id}-{attempt}-{}",
                                            now_ms()
                                        ))),
                                    };
                                    if let Ok(ack) = self.http.submit_order_wire(
                                        wire,
                                        rem_qty,
                                        ref_anchor,
                                        taker_fee_bps,
                                        &pk,
                                        Some(&self.db),
                                        &cfg,
                                    ) {
                                        if ack.accepted
                                            && ack.status == "FILLED"
                                            && ack.filled_quantity > 0.0
                                        {
                                            if ack.avg_fill_price > 0.0 {
                                                exit_px = ack.avg_fill_price;
                                            }
                                            fee_usdc += ack.fee_usdc;
                                            close_eoid_opt = ack.exchange_order_id;
                                            rem_qty = (rem_qty - ack.filled_quantity).max(0.0);
                                            if rem_qty <= asset.lot_step_size * 0.5 {
                                                filled_on_hl = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                if let Some(ref eoid) = close_eoid_opt {
                                    let oid_num = eoid.strip_prefix("hl-").unwrap_or(eoid.as_str());
                                    let coid = format!(
                                        "hl-closed-{}-{oid_num}",
                                        asset.coin.to_ascii_lowercase()
                                    );
                                    let _ = self.db.set_position_closed_exchange_ids(
                                        position_id,
                                        &coid,
                                        eoid,
                                    );
                                }
                                if !filled_on_hl {
                                    exchange_close_unfilled = true;
                                    self.illiquid_close_backoff_ms
                                        .lock()
                                        .insert(position_id, now_ms());
                                    let resting_oids: Option<HashSet<u64>> = self
                                        .http
                                        .fetch_frontend_open_orders(&wallet, &cfg)
                                        .or_else(|_| self.http.fetch_open_orders(&wallet, &cfg))
                                        .ok()
                                        .map(|orders| {
                                            orders
                                                .iter()
                                                .filter_map(|o| {
                                                    o.get("oid").and_then(|v| v.as_u64()).or_else(
                                                        || {
                                                            o.get("oid")
                                                                .and_then(|v| v.as_str())
                                                                .and_then(|s| {
                                                                    s.parse::<u64>().ok()
                                                                })
                                                        },
                                                    )
                                                })
                                                .collect()
                                        });
                                    let _ = self.chaser.ensure_exchange_tpsl_orders(
                                        position_id,
                                        &asset,
                                        side,
                                        rem_qty,
                                        exit_px,
                                        pos.protective_stop_px,
                                        pos.take_profit_px,
                                        pos.protective_stop_oid.as_deref(),
                                        pos.take_profit_oid.as_deref(),
                                        false,
                                        false,
                                        resting_oids.as_ref(),
                                        &cfg,
                                    );
                                    tracing::warn!(
                                        symbol = %asset.symbol,
                                        live_qty = live_qty,
                                        rem_qty = rem_qty,
                                        "Hyperliquid reduceOnly IOC close did not completely fill due to illiquid Testnet book"
                                    );
                                } else {
                                    self.cancel_coin_exchange_orders(
                                        &asset,
                                        &wallet,
                                        &pk,
                                        &cfg,
                                        true,
                                    );
                                    self.illiquid_close_backoff_ms.lock().remove(&position_id);
                                }
                            } else {
                                self.cancel_coin_exchange_orders(&asset, &wallet, &pk, &cfg, true);
                                self.illiquid_close_backoff_ms.lock().remove(&position_id);
                            }
                        }
                    }
                }

                if exchange_close_unfilled {
                    final_status = "OPEN".to_string();
                } else {
                    realized_pnl =
                        self.db
                            .close_position(position_id, exit_px, fee_usdc, reason)?;
                    exit_price_opt = Some(exit_px);
                    final_status = "CLOSED".to_string();

                    pending_close_alert = Some(PositionClosedAlert {
                        symbol: asset.symbol.clone(),
                        side,
                        exit_reason: reason.to_string(),
                        entry_price: pos.entry_price,
                        exit_price: exit_px,
                        filled_qty: closed_qty,
                        realized_pnl_usdc: realized_pnl,
                        available_cash_usdc: 0.0,
                        cooldown_type: (cd_sec > 0).then(|| "MANUAL".to_string()),
                        cooldown_seconds: cd_sec,
                        timestamp_ms: now_ms(),
                    });
                }
            }
            PositionStatus::Closed | PositionStatus::Cancelled => {
                final_status = pos.status.clone();
            }
        }

        if cd_sec > 0 && !exchange_close_unfilled {
            self.db.insert_asset_cooldown(
                asset.id,
                "MANUAL",
                &format!("Manual close/cancel cooldown: {reason}"),
                cd_sec,
            )?;
        }

        let snap_opt = self.record_portfolio_snapshot(None, None).ok();
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

        if let Some(mut alert) = pending_close_alert {
            if let Some(ref snap) = snap_opt {
                alert.available_cash_usdc = snap.available_cash_usdc;
            }
            let html = format_position_closed_html(&alert);
            let payload_val = serde_json::to_value(&alert).ok();
            let _ = self.alerts.enqueue_html_alert(
                "POSITION_CLOSED",
                "INFO",
                &html,
                payload_val.as_ref(),
            );
            if !self.db.is_ephemeral_test_db() {
                let alerts_engine = self.alerts.clone();
                std::thread::spawn(move || {
                    let _ = alerts_engine.poll_pending_queue(4);
                });
            }
        }

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

    /// Closes all currently `OPEN` holdings (`v_assets_held`) and cancels all `WORKING` orders (`v_assets_ordered`).
    pub fn close_all_active_positions(
        &self,
        reason: &str,
        cooldown_seconds: Option<i64>,
    ) -> Result<Vec<ManualCloseReport>> {
        let mut reports = Vec::new();
        let ordered = self.db.get_assets_ordered().unwrap_or_default();
        for o in ordered {
            if let Ok(rep) = self.close_or_cancel_position(o.position_id, reason, cooldown_seconds)
            {
                reports.push(rep);
            }
        }
        let held = self.db.get_assets_held().unwrap_or_default();
        for h in held {
            if let Ok(rep) = self.close_or_cancel_position(h.position_id, reason, cooldown_seconds)
            {
                reports.push(rep);
            }
        }
        let _ = self.record_portfolio_snapshot(None, None);
        Ok(reports)
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
                    let scan_index = tick_counter / scan_ratio;
                    if cfg.execution_mode().is_live_or_testnet() && scan_index.is_multiple_of(5) {
                        if let Err(e) = engine.reconciler.run_cutover_reconciliation(false) {
                            tracing::warn!(error = %e, "15s Hyperliquid 1:1 reconciliation error");
                        }
                    }
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
