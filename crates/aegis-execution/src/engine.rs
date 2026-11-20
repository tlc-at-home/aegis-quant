//! Unified Aegis Quant v2.0 Execution Engine (`aegis-execution::engine`).
//!
//! Coordinates:
//! - 1-Second High-Frequency Priority Surveillance Loop (`PriorityMonitor`)
//! - 3-Second Full-Exchange 150+ Perp Market Watcher (`MarketWatcher`)
//! - 1-Second Maker Limit Chaser & Order-to-Open Capital Lifecycle (`OrderChaser`)
//! - Multi-Sleeve S1/S2/S3 Signal Evaluation & Risk-Tier Allocator (`aegis-signals`, `aegis-risk`)
//! - Dynamic Cutover & Live Hyperliquid Exchange Reconciliation (`CutoverReconciler`)
//! - Rich HTML Telegram Alerting (`aegis-alerts`)

use crate::http_client::{endpoints_are_loopback, HyperliquidHttpClient, L2BookDepthSnapshot};
use crate::market_watcher::{MarketScanReport, MarketWatcher};
use crate::order_chaser::{NewWorkingOrderRequest, OrderChaser};
use crate::priority_monitor::{PriorityMonitor, PriorityTickReport};
use crate::reconcile::{CutoverReconcileReport, CutoverReconciler};
use crate::signing::{
    derive_hl_cloid_hex, format_hl_wire_price, format_hl_wire_size_floor, snap_to_tick, CancelWire,
    LimitOrderType, OrderTypeWire, OrderWire, HL_ORACLE_BAND_PCT,
};
use aegis_alerts::{
    format_position_closed_html, AlertEngine, CutoverReconcileAlert, PositionClosedAlert,
};
use aegis_core::{
    normalize_hyperliquid_symbol, now_ms, round_dp, AegisError, ConfigMap, ExchangeAsset,
    HeldAssetView, OhlcvBar, OrderSide, OrderedAssetView, PortfolioSnapshot, PositionStatus,
    Result,
};
use aegis_db::{DatabaseManager, SignalUpsertInput, SnapshotInsertInput};
use aegis_risk::{compute_atr_from_bars, RiskEngine};
use aegis_signals::{
    SignalCycleOutput, SignalEngine, ACTION_CLOSE, ACTION_FLIP, ACTION_OPEN_LONG, ACTION_OPEN_SHORT,
};
use parking_lot::{Mutex, ReentrantMutex};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

static GLOBAL_EXECUTION_LOCK: OnceLock<Arc<ReentrantMutex<()>>> = OnceLock::new();

pub fn execution_lock() -> &'static Arc<ReentrantMutex<()>> {
    GLOBAL_EXECUTION_LOCK.get_or_init(|| Arc::new(ReentrantMutex::new(())))
}

pub(crate) fn is_live_exchange_active(db: &DatabaseManager, cfg: &ConfigMap) -> bool {
    cfg.execution_mode().is_live_or_testnet()
        && (!db.is_ephemeral_test_db() || endpoints_are_loopback(cfg))
}

pub const MAX_L2_DEPTH_CHECKS_PER_REBALANCE: usize = 6;

/// Pure pre-entry L2 liquidity gate checking bid-ask spread and two-sided 15 bps depth.
pub fn passes_entry_l2_liquidity_gate(
    book: &L2BookDepthSnapshot,
    target_notional_usdc: f64,
    max_spread_bps: f64,
) -> std::result::Result<(), String> {
    let eff_max_spread = if max_spread_bps.is_finite() {
        max_spread_bps.max(1.0)
    } else {
        20.0
    };
    if !book.best_bid.is_finite()
        || !book.best_ask.is_finite()
        || !book.spread_bps.is_finite()
        || book.best_bid <= 0.0
        || book.best_ask < book.best_bid
    {
        return Err("L2_BOOK_INVALID_BBO".to_string());
    }
    if book.spread_bps > eff_max_spread {
        return Err(format!(
            "ENTRY_SPREAD_TOO_WIDE: spread={:.2}bps > max={:.2}bps",
            book.spread_bps, eff_max_spread
        ));
    }
    let eff_notional = if target_notional_usdc.is_finite() && target_notional_usdc > 0.0 {
        target_notional_usdc
    } else {
        0.0
    };
    let min_side_depth = (eff_notional * 0.25).max(25.0);
    if !book.bid_depth_15bps_usdc.is_finite()
        || !book.ask_depth_15bps_usdc.is_finite()
        || book.bid_depth_15bps_usdc < min_side_depth
        || book.ask_depth_15bps_usdc < min_side_depth
    {
        return Err(format!(
            "ENTRY_L2_DEPTH_TOO_THIN: bid_15bps=${:.2}, ask_15bps=${:.2} < min_side=${:.2}",
            book.bid_depth_15bps_usdc, book.ask_depth_15bps_usdc, min_side_depth
        ));
    }
    Ok(())
}

pub(crate) fn is_active_cap_holding(h: &HeldAssetView, cap_epoch_ms: i64) -> bool {
    cap_epoch_ms <= 0 || h.opened_at_ms.unwrap_or(h.updated_at_ms) >= cap_epoch_ms
}

pub(crate) fn is_active_cap_order(o: &OrderedAssetView, cap_epoch_ms: i64) -> bool {
    cap_epoch_ms <= 0 || o.created_at_ms >= cap_epoch_ms
}

fn parse_hl_f64(val: Option<&Value>) -> f64 {
    val.and_then(|x| {
        x.as_f64()
            .or_else(|| x.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
    })
    .filter(|v| v.is_finite())
    .unwrap_or(0.0)
}

fn parse_hl_oid_val(ord: &Value) -> Option<u64> {
    ord.get("oid")
        .and_then(Value::as_u64)
        .or_else(|| {
            ord.get("oid")
                .and_then(Value::as_str)
                .and_then(|s| s.trim().parse::<u64>().ok())
        })
        .filter(|&o| o > 0)
}

fn is_hl_trigger_order(ord: &Value) -> bool {
    if ord.get("isTrigger").and_then(Value::as_bool) == Some(true)
        || ord.get("isPositionTpsl").and_then(Value::as_bool) == Some(true)
    {
        return true;
    }
    if parse_hl_f64(ord.get("triggerPx")) > 0.0 {
        return true;
    }
    let order_type = ord
        .get("orderType")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    order_type.contains("stop") || order_type.contains("take profit")
}

fn is_ioc_no_match_error(raw_error: Option<&str>) -> bool {
    let Some(err) = raw_error else {
        return false;
    };
    let lower = err.to_ascii_lowercase();
    lower.contains("could not immediately match") || lower.contains("ioccancelrejected")
}

fn compute_passive_close_px(
    asset: &ExchangeAsset,
    close_is_buy: bool,
    l2_bbo: Option<(f64, f64)>,
    mark_px: f64,
    band_lo: f64,
    band_hi: f64,
) -> (f64, String) {
    let eff_tick = asset.tick_size.max(1e-8);
    let passive_raw = if close_is_buy {
        l2_bbo
            .map(|(bid, _)| bid + eff_tick)
            .filter(|&px| px > 0.0)
            .unwrap_or(mark_px)
    } else {
        l2_bbo
            .map(|(_, ask)| (ask - eff_tick).max(eff_tick))
            .filter(|&px| px > 0.0)
            .unwrap_or(mark_px)
    };
    let clamped = passive_raw.clamp(band_lo, band_hi);
    let wire_px = format_hl_wire_price(clamped, eff_tick, asset.sz_decimals);
    let parsed_px = wire_px
        .parse::<f64>()
        .ok()
        .filter(|&p| p.is_finite() && p > 0.0)
        .unwrap_or_else(|| snap_to_tick(clamped, eff_tick));
    (parsed_px, wire_px)
}

fn is_matching_passive_reduce_only_close(
    ord: &Value,
    asset: &ExchangeAsset,
    close_is_buy: bool,
    target_qty: f64,
    passive_px: f64,
    passive_wire_px: &str,
) -> bool {
    let ord_coin = ord.get("coin").and_then(Value::as_str).unwrap_or("").trim();
    if !ord_coin.eq_ignore_ascii_case(&asset.coin) || is_hl_trigger_order(ord) {
        return false;
    }
    if ord.get("reduceOnly").and_then(Value::as_bool) != Some(true) {
        return false;
    }
    let order_type = ord
        .get("orderType")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if !order_type.is_empty() && order_type != "limit" {
        return false;
    }
    let ord_is_buy = match ord
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase()
        .as_str()
    {
        "B" | "BUY" | "LONG" => true,
        "A" | "SELL" | "SHORT" => false,
        _ => return false,
    };
    if ord_is_buy != close_is_buy {
        return false;
    }
    let ord_sz = parse_hl_f64(ord.get("sz"));
    let ord_px = parse_hl_f64(ord.get("limitPx"));
    if ord_sz <= 0.0 || ord_px <= 0.0 {
        return false;
    }
    let lot_tol = asset.lot_step_size.max(1e-8) * 1.05;
    let eff_tick = asset.tick_size.max(1e-8);
    let px_tol = eff_tick * 2.0 + 1e-12;
    let px_matches = (ord_px - passive_px).abs() <= px_tol
        || format_hl_wire_price(ord_px, eff_tick, asset.sz_decimals) == passive_wire_px;
    (ord_sz - target_qty).abs() <= lot_tol && px_matches
}

fn find_newest_matching_passive_close_oid(
    open_orders: &[Value],
    asset: &ExchangeAsset,
    close_is_buy: bool,
    target_qty: f64,
    passive_px: f64,
    passive_wire_px: &str,
) -> Option<u64> {
    open_orders
        .iter()
        .filter_map(|ord| {
            if !is_matching_passive_reduce_only_close(
                ord,
                asset,
                close_is_buy,
                target_qty,
                passive_px,
                passive_wire_px,
            ) {
                return None;
            }
            let oid = parse_hl_oid_val(ord)?;
            let ts = ord.get("timestamp").and_then(Value::as_i64).unwrap_or(0);
            Some((ts, oid))
        })
        .max()
        .map(|(_, oid)| oid)
}

fn collect_coin_non_trigger_cancels(
    open_orders: &[Value],
    asset: &ExchangeAsset,
    keep_oid: Option<u64>,
) -> Vec<CancelWire> {
    let mut cancels = Vec::new();
    for ord in open_orders {
        let c = ord.get("coin").and_then(Value::as_str).unwrap_or("").trim();
        if !c.eq_ignore_ascii_case(&asset.coin) || is_hl_trigger_order(ord) {
            continue;
        }
        if let Some(oid) = parse_hl_oid_val(ord) {
            if keep_oid != Some(oid) {
                cancels.push(CancelWire {
                    a: asset.asset_index,
                    o: oid,
                });
            }
        }
    }
    cancels
}

fn cancel_batch_response_ok(resp: &Value) -> std::result::Result<(), String> {
    if resp.get("status").and_then(Value::as_str) != Some("ok") {
        return Err(format!("non-ok cancel status: {resp}"));
    }
    if let Some(statuses) = resp
        .pointer("/response/data/statuses")
        .and_then(Value::as_array)
    {
        for st in statuses {
            if let Some(err_val) = st.get("error") {
                let err_str = err_val
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| err_val.to_string());
                let err_lower = err_str.to_ascii_lowercase();
                if err_lower.contains("filled")
                    || (!err_lower.contains("canceled")
                        && !err_lower.contains("cancelled")
                        && !err_lower.contains("never placed"))
                {
                    return Err(err_str);
                }
            }
        }
    }
    Ok(())
}

struct CloseFillAccumulator {
    close_eoid_opt: Option<String>,
    live_fee_usdc: f64,
    fill_notional_sum: f64,
    fill_qty_sum: f64,
    rem_qty: f64,
}

impl CloseFillAccumulator {
    fn new(rem_qty: f64) -> Self {
        Self {
            close_eoid_opt: None,
            live_fee_usdc: 0.0,
            fill_notional_sum: 0.0,
            fill_qty_sum: 0.0,
            rem_qty,
        }
    }

    fn record_fill(
        &mut self,
        ack: crate::http_client::ExchangeOrderAck,
        fallback_px: f64,
        lot_step_size: f64,
    ) -> bool {
        if ack.accepted && ack.status == "FILLED" && ack.filled_quantity > 0.0 {
            let fill_px = if ack.avg_fill_price > 0.0 {
                ack.avg_fill_price
            } else {
                fallback_px
            };
            self.live_fee_usdc += ack.fee_usdc;
            self.fill_notional_sum += fill_px * ack.filled_quantity;
            self.fill_qty_sum += ack.filled_quantity;
            self.close_eoid_opt = ack.exchange_order_id;
            self.rem_qty = (self.rem_qty - ack.filled_quantity).max(0.0);
            self.rem_qty <= lot_step_size * 0.5 || self.rem_qty <= 1e-9
        } else {
            false
        }
    }
}

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
            let cancels: Vec<CancelWire> = open_orders
                .iter()
                .filter_map(|ord| {
                    let c = ord.get("coin").and_then(Value::as_str).unwrap_or("").trim();
                    if c.eq_ignore_ascii_case(&asset.coin)
                        && (include_triggers || !is_hl_trigger_order(ord))
                    {
                        parse_hl_oid_val(ord).map(|o| CancelWire {
                            a: asset.asset_index,
                            o,
                        })
                    } else {
                        None
                    }
                })
                .collect();
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
        let cfg = self.db.load_config()?;
        let market_scan = self.watcher.run_3s_exchange_scan()?;
        let _ = self.watcher.refresh_bars_for_eligible_assets(
            35,
            72,
            market_scan.live_exchange_connected,
            &cfg,
        );

        let _exec_guard = execution_lock().lock();
        let reconcile = self
            .reconciler
            .run_cutover_reconciliation(close_legacy_positions)?;

        let post_reconcile_cfg = self.db.load_config()?;
        let signal_out = self.evaluate_and_persist_signals(
            self.resolve_effective_base_equity_usdc(&post_reconcile_cfg),
        )?;
        let snapshot = self.record_portfolio_snapshot(None, Some(&signal_out))?;

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
        let cfg = self.db.load_config()?;
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
                let use_live_candles = cfg.execution_mode().is_live_or_testnet()
                    && (!self.db.is_ephemeral_test_db() || endpoints_are_loopback(&cfg));
                let bar_inputs =
                    self.watcher
                        .fetch_or_synthesize_bars(asset, "4h", 72, use_live_candles, &cfg);
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

        let active_asset_ids: HashSet<i64> = held
            .iter()
            .map(|h| h.asset_id)
            .chain(ordered.iter().map(|o| o.asset_id))
            .collect();
        let asset_by_id: HashMap<i64, &ExchangeAsset> = assets.iter().map(|a| (a.id, a)).collect();

        let mut signal_inputs: Vec<SignalUpsertInput> = Vec::with_capacity(output.signals.len());
        let mut prio_updates: Vec<(i64, bool)> = Vec::new();
        for sig in &output.signals {
            signal_inputs.push(SignalUpsertInput {
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
            });

            if sig.event_trigger.is_some() {
                if !asset_by_id
                    .get(&sig.asset_id)
                    .is_some_and(|a| a.is_priority_watched)
                {
                    prio_updates.push((sig.asset_id, true));
                }
            } else if !active_asset_ids.contains(&sig.asset_id) {
                if let Some(a) = asset_by_id.get(&sig.asset_id) {
                    let day_chg = if a.prev_day_price > 0.0 {
                        ((a.mark_price - a.prev_day_price) / a.prev_day_price).abs()
                    } else {
                        0.0
                    };
                    if a.is_priority_watched
                        && a.funding_rate_hourly.abs() < 0.0004
                        && day_chg < 0.08
                    {
                        prio_updates.push((sig.asset_id, false));
                    }
                }
            }
        }
        self.db.upsert_sleeve_signals_batch(&signal_inputs)?;
        let _ = self.db.set_assets_priority_watch_batch(&prio_updates);

        Ok(output)
    }

    /// Resolves the live/testnet base equity override (`Some(usdc)`) if active, or `None` to use paper equity.
    pub fn resolve_live_base_equity_usdc(
        cfg: &aegis_core::ConfigMap,
        is_ephemeral_test_db: bool,
    ) -> Option<f64> {
        if !cfg.execution_mode().is_live_or_testnet() {
            return None;
        }
        let live_eq = cfg.get_f64("live_exchange_equity_usdc", 0.0);
        let cap_eq = cfg.get_f64("risk_capital_cap_usdc", 0.0);
        if live_eq > 0.0 {
            return Some(if cap_eq >= 10.0 {
                live_eq.min(cap_eq)
            } else {
                live_eq
            });
        }
        let has_wallet = !cfg.get_str("wallet_address", "").trim().is_empty();
        if has_wallet && !is_ephemeral_test_db && cap_eq >= 10.0 {
            return Some(cap_eq);
        }
        None
    }

    /// Resolves the active base portfolio equity in USDC:
    /// - In `TESTNET`/`LIVE` mode: reads `live_eq = live_exchange_equity_usdc` and `cap_eq = risk_capital_cap_usdc`;
    ///   caps `live_eq` with `live_eq.min(cap_eq)` when configured (`cap_eq >= 10.0`), falling back to `cap_eq` only when a live
    ///   non-ephemeral wallet is configured before initial exchange reconciliation.
    /// - In `PAPER` (or unconfigured local test DBs without `live_exchange_equity_usdc`): uses `initial_paper_equity_usdc + realized_pnl`.
    pub fn resolve_effective_base_equity_usdc(&self, cfg: &aegis_core::ConfigMap) -> f64 {
        if let Some(live_base) =
            Self::resolve_live_base_equity_usdc(cfg, self.db.is_ephemeral_test_db())
        {
            return live_base;
        }
        let seed_eq = cfg.get_f64("initial_paper_equity_usdc", 10_000.0);
        let closed_realized_pnl: f64 = self
            .db
            .list_closed_positions(200)
            .unwrap_or_default()
            .iter()
            .map(|p| p.realized_pnl_usdc)
            .sum();
        (seed_eq + closed_realized_pnl).max(100.0)
    }

    /// Enforces `max_open_positions` (default `6`) and the hard gross-leverage ceiling
    /// (`base_equity_usdc * min(target_leverage_multiplier, max_gross_leverage_x)`) over active-epoch positions/orders
    /// (`>= risk_capital_cap_updated_at_ms` in `LIVE`/`TESTNET`), automatically cancelling excess `WORKING` orders
    /// and closing excess `OPEN` positions (protecting young positions when older candidates exist).
    pub fn enforce_leverage_and_position_caps(&self) -> Result<Vec<ManualCloseReport>> {
        let _exec_guard = execution_lock().lock();
        let cfg = self.db.load_config()?;
        let max_open_positions = cfg.get_usize("max_open_positions", 6).max(1);
        let base_equity_usdc = self.resolve_effective_base_equity_usdc(&cfg);
        let max_gross_lev = cfg.get_f64("max_gross_leverage_x", 2.50).clamp(0.20, 5.00);
        let eff_lev_cap = cfg.target_leverage_multiplier().min(max_gross_lev);
        let max_gross_notional = base_equity_usdc * eff_lev_cap;
        let cap_epoch_ms = if cfg.execution_mode().is_live_or_testnet() {
            cfg.get_i64("risk_capital_cap_updated_at_ms", 0)
        } else {
            0
        };
        let min_hold_ms = cfg.get_i64("min_position_hold_sec", 300).max(0) * 1000;

        let mut closed_reports = Vec::new();
        let active_ordered: Vec<OrderedAssetView> = self
            .db
            .get_assets_ordered()?
            .into_iter()
            .filter(|o| is_active_cap_order(o, cap_epoch_ms))
            .collect();
        let mut active_held: Vec<HeldAssetView> = self
            .db
            .get_assets_held()?
            .into_iter()
            .filter(|h| is_active_cap_holding(h, cap_epoch_ms))
            .collect();

        let mut current_committed: f64 =
            active_held.iter().map(|h| h.committed_notional_usdc).sum();
        let mut current_reserved: f64 = active_ordered
            .iter()
            .map(|o| o.limit_price * o.requested_quantity)
            .sum();
        let mut total_active = active_held.len() + active_ordered.len();

        // 1. Cancel active-epoch WORKING orders first if position count or gross leverage is above cap
        if total_active > max_open_positions
            || (current_committed + current_reserved) > max_gross_notional * 1.03
        {
            for o in &active_ordered {
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
                    if rep.final_status != "OPEN" && rep.final_status != "WORKING" {
                        current_reserved =
                            (current_reserved - (o.limit_price * o.requested_quantity)).max(0.0);
                        total_active = total_active.saturating_sub(1);
                        closed_reports.push(rep);
                    }
                }
            }
        }

        // 2. Close excess active-epoch OPEN holdings without ever subtracting backed-off exposure
        let check_ts = now_ms();
        let mut open_count = active_held.len();
        if open_count > max_open_positions || current_committed > max_gross_notional * 1.03 {
            let is_young = |h: &HeldAssetView| -> bool {
                min_hold_ms > 0
                    && check_ts.saturating_sub(h.opened_at_ms.unwrap_or(check_ts)) < min_hold_ms
            };
            let has_older_candidate = active_held.iter().any(|h| !is_young(h));

            active_held.sort_by(|a, b| {
                let a_young = is_young(a);
                let b_young = is_young(b);
                a_young.cmp(&b_young).then_with(|| {
                    let a_align = a.latest_target_weight * OrderSide::parse(&a.side).sign();
                    let b_align = b.latest_target_weight * OrderSide::parse(&b.side).sign();
                    a_align.total_cmp(&b_align).then_with(|| {
                        b.committed_notional_usdc
                            .total_cmp(&a.committed_notional_usdc)
                    })
                })
            });

            for h in &active_held {
                if open_count <= max_open_positions && current_committed <= max_gross_notional {
                    break;
                }
                if is_young(h) && has_older_candidate {
                    continue;
                }
                if self.is_illiquid_close_backed_off(h.position_id, check_ts) {
                    continue;
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
            let _ = self.record_portfolio_snapshot(None, None);
        }

        Ok(closed_reports)
    }

    /// Executes portfolio rebalance actions (`CLOSE`, `FLIP`, `OPEN_LONG`, `OPEN_SHORT`)
    /// while strictly enforcing active-epoch cap isolation, illiquid-backoff gating,
    /// `min_position_hold_sec` whipsaw protection, daily drawdown breaker,
    /// symbol-scoped `ACTION_FLIP` cooldown bypass, and gross leverage.
    pub fn execute_signal_rebalance(
        &self,
        signal_out: &SignalCycleOutput,
        base_equity_usdc: f64,
    ) -> Result<RebalanceExecutionReport> {
        let _exec_guard = execution_lock().lock();
        let cfg = self.db.load_config()?;
        let eff_equity = if cfg.execution_mode().is_live_or_testnet() {
            self.resolve_effective_base_equity_usdc(&cfg)
        } else {
            base_equity_usdc.max(100.0)
        };
        let cap_epoch_ms = if cfg.execution_mode().is_live_or_testnet() {
            cfg.get_i64("risk_capital_cap_updated_at_ms", 0)
        } else {
            0
        };
        let mut report = RebalanceExecutionReport::default();

        // 0. Enforce leverage & max_open_positions caps first (auto-deleveraging if over limit)
        if let Ok(deleveraged) = self.enforce_leverage_and_position_caps() {
            for r in deleveraged {
                if r.final_status != "OPEN" {
                    report.positions_closed += 1;
                    report.closed_position_ids.push(r.position_id);
                }
            }
        }

        let held = self.db.get_assets_held()?;
        let rebalance_now = now_ms();
        let min_hold_ms = cfg.get_i64("min_position_hold_sec", 300).max(0) * 1000;
        let mut flipped_symbols: HashSet<i64> = HashSet::new();

        // 1. Handle CLOSE or FLIP on currently OPEN holdings (respecting min_position_hold_sec)
        for sig in &signal_out.signals {
            if sig.rebalance_action == ACTION_CLOSE || sig.rebalance_action == ACTION_FLIP {
                if let Some(h) = held.iter().find(|x| x.asset_id == sig.asset_id) {
                    let held_ms =
                        rebalance_now.saturating_sub(h.opened_at_ms.unwrap_or(h.updated_at_ms));
                    if min_hold_ms > 0 && held_ms < min_hold_ms {
                        continue;
                    }
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
                    if close_res.final_status != "OPEN" {
                        report.positions_closed += 1;
                        report.closed_position_ids.push(close_res.position_id);
                        if sig.rebalance_action == ACTION_FLIP && close_res.final_status == "CLOSED"
                        {
                            flipped_symbols.insert(sig.asset_id);
                        }
                    }
                }
            }
        }

        // 2. Re-evaluate active-epoch holdings/orders (never subtract illiquid-backed-off active holdings!)
        let active_held: Vec<HeldAssetView> = self
            .db
            .get_assets_held()?
            .into_iter()
            .filter(|h| is_active_cap_holding(h, cap_epoch_ms))
            .collect();
        let active_ordered: Vec<OrderedAssetView> = self
            .db
            .get_assets_ordered()?
            .into_iter()
            .filter(|o| is_active_cap_order(o, cap_epoch_ms))
            .collect();

        if active_held
            .iter()
            .any(|h| self.is_illiquid_close_backed_off(h.position_id, rebalance_now))
        {
            return Ok(report);
        }

        let max_open_positions = cfg.get_usize("max_open_positions", 6).max(1);
        let min_entry_interval_sec = cfg.get_i64("min_entry_interval_sec", 150).max(0);
        let total_active = active_held.len() + active_ordered.len();

        if total_active >= max_open_positions {
            return Ok(report);
        }

        // Daily drawdown breaker: compare latest snapshot equity vs 24h peak equity in portfolio_snapshots
        let max_dd_pct = cfg
            .get_f64("max_daily_drawdown_pct", 0.15)
            .clamp(0.01, 0.90);
        let dd_window_start = if cap_epoch_ms > 0 {
            (rebalance_now - 86_400_000).max(cap_epoch_ms)
        } else {
            rebalance_now - 86_400_000
        };
        if let Some((peak_24h, latest_equity)) =
            self.db.get_peak_and_latest_equity_since(dd_window_start)?
        {
            if peak_24h.is_finite()
                && latest_equity.is_finite()
                && peak_24h > 0.0
                && latest_equity > 0.0
            {
                let dd_pct = (peak_24h - latest_equity) / peak_24h;
                if dd_pct >= max_dd_pct {
                    tracing::warn!(
                        latest_equity = latest_equity,
                        peak_24h = peak_24h,
                        drawdown_pct = dd_pct * 100.0,
                        max_daily_drawdown_pct = max_dd_pct * 100.0,
                        "Daily drawdown breaker tripped; halting new entries"
                    );
                    return Ok(report);
                }
            }
        }

        let entry_interval_blocked = if min_entry_interval_sec > 0 {
            self.db
                .last_position_entry_timestamp_ms()
                .ok()
                .flatten()
                .is_some_and(|last_entry_ms| {
                    now_ms().saturating_sub(last_entry_ms) < min_entry_interval_sec * 1000
                })
        } else {
            false
        };
        if entry_interval_blocked && flipped_symbols.is_empty() {
            return Ok(report);
        }

        let risk = RiskEngine::from_config_map(&cfg);
        let ledger = risk.evaluate_ledger_from_views(eff_equity, &active_held, &active_ordered);
        let target_mult = cfg.target_leverage_multiplier();
        let eff_lev_cap = target_mult.min(risk.limits.max_gross_leverage_x);
        let max_allowed_gross_notional = eff_equity * eff_lev_cap;
        let current_reserved_notional: f64 = active_ordered
            .iter()
            .map(|o| (o.limit_price * o.requested_quantity).max(0.0))
            .sum();
        if ledger.committed_notional_usdc + current_reserved_notional >= max_allowed_gross_notional
        {
            return Ok(report);
        }
        let max_per_pos_weight = ((eff_lev_cap / (max_open_positions as f64)) * 1.25)
            .min(risk.limits.max_single_asset_weight);

        let assets = self.db.list_exchange_assets()?;
        let asset_map: HashMap<i64, ExchangeAsset> =
            assets.into_iter().map(|a| (a.id, a)).collect();

        let mut candidate_signals = signal_out.signals.clone();
        candidate_signals.sort_by(|a, b| {
            let a_prio = (((a.rebalance_action == ACTION_FLIP
                && flipped_symbols.contains(&a.asset_id)) as i32)
                * 2)
                + ((a.event_trigger.as_deref() == Some("CAPITULATION_FLUSH")
                    && a.s1_mom_score > 0.0) as i32);
            let b_prio = (((b.rebalance_action == ACTION_FLIP
                && flipped_symbols.contains(&b.asset_id)) as i32)
                * 2)
                + ((b.event_trigger.as_deref() == Some("CAPITULATION_FLUSH")
                    && b.s1_mom_score > 0.0) as i32);
            b_prio
                .cmp(&a_prio)
                .then_with(|| b.target_weight.abs().total_cmp(&a.target_weight.abs()))
        });

        let is_live_exchange = is_live_exchange_active(&self.db, &cfg);
        if is_live_exchange && cfg.get_str("private_key", "").trim().is_empty() {
            tracing::error!(
                "private_key is empty/missing in live mode; refusing to place new rebalance orders"
            );
            return Ok(report);
        }
        let max_entry_spread_bps = cfg.get_f64("max_entry_spread_bps", 20.0).max(1.0);
        let mut l2_depth_checks: usize = 0;

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
            let is_confirmed_flip =
                sig.rebalance_action == ACTION_FLIP && flipped_symbols.contains(&sig.asset_id);
            if entry_interval_blocked && !is_confirmed_flip {
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

            let effective_current_weight = if is_confirmed_flip {
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

            // Hard Gross-Leverage Gate anchored strictly to effective equity * min(target_leverage_multiplier, max_gross_leverage_x)
            if ledger.committed_notional_usdc
                + current_reserved_notional
                + plan.target_notional_usdc
                > max_allowed_gross_notional + 1e-2
            {
                continue;
            }

            // Pre-Entry L2 Spread & Two-Sided Top-of-Book Depth Gate (live exchange only)
            if is_live_exchange {
                if l2_depth_checks >= MAX_L2_DEPTH_CHECKS_PER_REBALANCE {
                    tracing::debug!(
                        checked = l2_depth_checks,
                        "Reached per-cycle L2 depth check cap; deferring remaining candidates"
                    );
                    break;
                }
                l2_depth_checks += 1;
                match self.http.fetch_l2_book_depth(&asset.coin, &cfg) {
                    Ok(Some(book)) => {
                        if let Err(reason) = passes_entry_l2_liquidity_gate(
                            &book,
                            plan.target_notional_usdc,
                            max_entry_spread_bps,
                        ) {
                            tracing::debug!(
                                symbol = %asset.symbol,
                                reason = %reason,
                                "Skipping candidate due to thin/wide L2 book"
                            );
                            continue;
                        }
                    }
                    Ok(None) => {
                        tracing::debug!(
                            symbol = %asset.symbol,
                            reason = "EMPTY_L2_BOOK",
                            "Skipping candidate due to thin/wide L2 book"
                        );
                        continue;
                    }
                    Err(err) => {
                        tracing::debug!(
                            symbol = %asset.symbol,
                            error = %err,
                            "Skipping candidate due to transient L2 book fetch error"
                        );
                        continue;
                    }
                }
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
                bypass_entry_interval: is_confirmed_flip,
            }) {
                report.working_orders_placed += 1;
                report.placed_position_ids.push(pos_id);
                break;
            }
        }

        Ok(report)
    }

    /// Computes and persists a unified [`PortfolioSnapshot`] reflecting the strict Order-to-Open Capital Lifecycle:
    /// - In `LIVE`/`TESTNET`, uses `live_exchange_equity_usdc` directly as `total_equity_usdc`.
    /// - In `PAPER`, computes `initial_paper_equity_usdc + closed_realized_pnl + unrealized_pnl`.
    pub fn record_portfolio_snapshot(
        &self,
        base_equity_override: Option<f64>,
        signal_out: Option<&SignalCycleOutput>,
    ) -> Result<PortfolioSnapshot> {
        let cfg = self.db.load_config()?;
        let risk = RiskEngine::from_config_map(&cfg);
        let held = self.db.get_assets_held()?;
        let ordered = self.db.get_assets_ordered()?;
        let assets = self.db.list_exchange_assets()?;

        let live_eq_cfg = cfg.get_f64("live_exchange_equity_usdc", 0.0);
        let ledger = if cfg.execution_mode().is_live_or_testnet() {
            let actual_live_eq = if live_eq_cfg > 0.0 {
                live_eq_cfg
            } else if let Some(explicit_eq) = base_equity_override.filter(|&v| v > 0.0) {
                explicit_eq
            } else {
                cfg.get_f64("initial_paper_equity_usdc", 10_000.0)
            };
            let mut l = risk.evaluate_ledger_from_views(actual_live_eq, &held, &ordered);
            let tot_eq = round_dp(actual_live_eq.max(1.0), 4);
            let eff_lev = cfg.target_leverage_multiplier().clamp(1.0, 20.0);
            let committed_margin_used = l.committed_notional_usdc / eff_lev;
            l.total_equity_usdc = tot_eq;
            l.available_cash_usdc = round_dp(
                (tot_eq - committed_margin_used - l.reserved_order_cash_usdc).max(0.0),
                4,
            );
            l.gross_leverage_x = round_dp(l.committed_notional_usdc / tot_eq, 4);
            l.net_leverage_x = round_dp(l.net_notional_usdc / tot_eq, 4);
            l
        } else {
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
            risk.evaluate_ledger_from_views(base_cash_equity, &held, &ordered)
        };
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
        let cfg = self.db.load_config()?;
        let market_scan = self.watcher.run_3s_exchange_scan()?;
        let priority_tick = self.priority_monitor.run_1s_priority_tick()?;

        let base_equity = self.resolve_effective_base_equity_usdc(&cfg);
        let signal_out = self.evaluate_and_persist_signals(base_equity)?;

        let (rebalance, snapshot) = {
            let _exec_guard = execution_lock().lock();
            let reb = if execute_orders {
                self.execute_signal_rebalance(&signal_out, base_equity)?
            } else {
                RebalanceExecutionReport::default()
            };
            let snap = self.record_portfolio_snapshot(None, Some(&signal_out))?;
            (reb, snap)
        };
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
        let _exec_guard = execution_lock().lock();
        let pos = self
            .db
            .get_position_by_id(position_id)?
            .ok_or_else(|| AegisError::Validation(format!("Position #{position_id} not found")))?;
        let asset = self
            .db
            .get_asset_by_id(pos.asset_id)?
            .ok_or_else(|| AegisError::Database(format!("Asset #{} not found", pos.asset_id)))?;
        let cfg = self.db.load_config()?;
        let status = PositionStatus::parse(&pos.status);
        let cd_sec = cooldown_seconds.unwrap_or(3600).max(0);
        let is_live_exchange = is_live_exchange_active(&self.db, &cfg);

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
                    if pk.trim().is_empty() {
                        exchange_close_unfilled = true;
                        tracing::error!(
                            position_id = position_id,
                            symbol = %asset.symbol,
                            "private_key is empty/missing in live mode; refusing to mark WORKING order CANCELLED without exchange cancel"
                        );
                    } else {
                        let mut cancels = Vec::new();
                        if let Some(oid) = pos
                            .exchange_order_id
                            .as_deref()
                            .map(str::trim)
                            .and_then(|s| s.strip_prefix("hl-").or(Some(s)))
                            .and_then(|s| s.parse::<u64>().ok())
                            .filter(|&o| o > 0)
                        {
                            cancels.push(CancelWire {
                                a: asset.asset_index,
                                o: oid,
                            });
                        } else if wallet.trim().is_empty() {
                            exchange_close_unfilled = true;
                            tracing::warn!(
                                position_id = position_id,
                                symbol = %asset.symbol,
                                "Working order lacks numeric exchange_order_id and wallet_address is empty in live mode; keeping WORKING"
                            );
                        } else {
                            match self
                                .http
                                .fetch_frontend_open_orders(&wallet, &cfg)
                                .or_else(|_| self.http.fetch_open_orders(&wallet, &cfg))
                            {
                                Ok(open_orders) => {
                                    cancels = collect_coin_non_trigger_cancels(
                                        &open_orders,
                                        &asset,
                                        None,
                                    );
                                }
                                Err(e) => {
                                    exchange_close_unfilled = true;
                                    tracing::warn!(
                                        position_id = position_id,
                                        symbol = %asset.symbol,
                                        error = %e,
                                        "Failed to fetch openOrders for unlinked WORKING order; keeping WORKING"
                                    );
                                }
                            }
                        }

                        if !exchange_close_unfilled && !cancels.is_empty() {
                            match self
                                .http
                                .cancel_orders_batch(cancels, &pk, Some(&self.db), &cfg)
                            {
                                Ok(resp) => {
                                    if let Err(err_detail) = cancel_batch_response_ok(&resp) {
                                        exchange_close_unfilled = true;
                                        tracing::warn!(
                                            position_id = position_id,
                                            symbol = %asset.symbol,
                                            error = %err_detail,
                                            response = %resp,
                                            "Hyperliquid WORKING order cancel not confirmed; keeping WORKING"
                                        );
                                    }
                                }
                                Err(e) => {
                                    exchange_close_unfilled = true;
                                    tracing::warn!(
                                        position_id = position_id,
                                        symbol = %asset.symbol,
                                        error = %e,
                                        "Hyperliquid cancel_orders_batch failed; keeping WORKING"
                                    );
                                }
                            }
                        }
                    }
                }
                if exchange_close_unfilled {
                    final_status = "WORKING".to_string();
                } else {
                    released_reserved = pos.reserved_margin_usdc;
                    self.db.cancel_working_order(position_id, reason)?;
                    final_status = "CANCELLED".to_string();
                }
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

                    if pk.trim().is_empty() {
                        exchange_close_unfilled = true;
                        tracing::error!(
                            position_id = position_id,
                            symbol = %asset.symbol,
                            "private_key is empty/missing in live mode; refusing to mark DB position CLOSED without exchange close"
                        );
                    } else if wallet.trim().is_empty() {
                        exchange_close_unfilled = true;
                        tracing::warn!(
                            position_id = position_id,
                            symbol = %asset.symbol,
                            "wallet_address is empty in live mode; refusing to mark DB position CLOSED without exchange close"
                        );
                    } else {
                        // 1. Query live Hyperliquid clearinghouseState BEFORE touching resting orders
                        //    so any matching passive reduceOnly GTC close on an illiquid book stays intact.
                        match self.http.fetch_clearinghouse_state(&wallet, &cfg) {
                            Err(e) => {
                                exchange_close_unfilled = true;
                                tracing::warn!(
                                    position_id = position_id,
                                    symbol = %asset.symbol,
                                    error = %e,
                                    "Hyperliquid clearinghouseState failed during close; refusing to mark DB position CLOSED blindly"
                                );
                            }
                            Ok(ch_state) => {
                                let mut live_szi = 0.0;
                                let mut live_pos_val = 0.0;
                                if let Some(arr) =
                                    ch_state.get("assetPositions").and_then(Value::as_array)
                                {
                                    for item in arr {
                                        let p = item.get("position").unwrap_or(item);
                                        let c = p.get("coin").and_then(Value::as_str).unwrap_or("");
                                        if c.eq_ignore_ascii_case(&asset.coin) {
                                            live_szi = parse_hl_f64(p.get("szi"));
                                            live_pos_val = parse_hl_f64(p.get("positionValue"));
                                            break;
                                        }
                                    }
                                }

                                if live_szi.abs() > 1e-9 {
                                    let live_qty = live_szi.abs();
                                    closed_qty = live_qty;
                                    let close_is_buy = live_szi < 0.0;
                                    let implied_mark = if live_pos_val > 0.0 && live_qty > 0.0 {
                                        round_dp(live_pos_val / live_qty, 6)
                                    } else {
                                        0.0
                                    };
                                    let mark_px = if implied_mark > 0.0 {
                                        implied_mark
                                    } else if asset.mark_price > 0.0 {
                                        asset.mark_price
                                    } else {
                                        exit_px
                                    };
                                    if implied_mark > 0.0 {
                                        exit_px = implied_mark;
                                    }

                                    let tick = asset.tick_size.max(1e-8);
                                    let band_lo = (mark_px * (1.0 - 0.038)).max(tick);
                                    let band_hi = mark_px * (1.0 + 0.038);

                                    let l2_bbo =
                                        self.http.fetch_l2_book(&asset.coin, &cfg).ok().flatten();
                                    let book_crossable_in_band =
                                        l2_bbo.is_some_and(|(bid, ask)| {
                                            if close_is_buy {
                                                ask > 0.0 && ask <= band_hi
                                            } else {
                                                bid > 0.0 && bid >= band_lo
                                            }
                                        });
                                    let (passive_px, passive_wire_px) = compute_passive_close_px(
                                        &asset,
                                        close_is_buy,
                                        l2_bbo,
                                        mark_px,
                                        band_lo,
                                        band_hi,
                                    );

                                    let (open_orders_opt, have_frontend_orders) =
                                        match self.http.fetch_frontend_open_orders(&wallet, &cfg) {
                                            Ok(orders) => (Some(orders), true),
                                            Err(_) => {
                                                match self.http.fetch_open_orders(&wallet, &cfg) {
                                                    Ok(orders) => (Some(orders), false),
                                                    Err(_) => (None, false),
                                                }
                                            }
                                        };

                                    let wire_sz_str = format_hl_wire_size_floor(
                                        live_qty,
                                        asset.lot_step_size,
                                        asset.sz_decimals,
                                    );
                                    let target_qty = wire_sz_str
                                        .parse::<f64>()
                                        .ok()
                                        .filter(|&q| q > 0.0)
                                        .unwrap_or(live_qty);

                                    let mut filled_on_hl = false;
                                    let mut fills = CloseFillAccumulator::new(live_qty);
                                    let mut kept_existing_passive_close = false;
                                    let mut mutated_open_orders = false;

                                    if !book_crossable_in_band {
                                        let keep_passive_oid = if wire_sz_str != "0" {
                                            open_orders_opt.as_deref().and_then(|orders| {
                                                find_newest_matching_passive_close_oid(
                                                    orders,
                                                    &asset,
                                                    close_is_buy,
                                                    target_qty,
                                                    passive_px,
                                                    &passive_wire_px,
                                                )
                                            })
                                        } else {
                                            None
                                        };

                                        if let Some(orders) = open_orders_opt.as_deref() {
                                            let cancels = collect_coin_non_trigger_cancels(
                                                orders,
                                                &asset,
                                                keep_passive_oid,
                                            );
                                            if !cancels.is_empty() {
                                                mutated_open_orders = true;
                                                let _ = self.http.cancel_orders_batch(
                                                    cancels,
                                                    &pk,
                                                    Some(&self.db),
                                                    &cfg,
                                                );
                                            }
                                        }

                                        if let Some(keep_oid) = keep_passive_oid {
                                            kept_existing_passive_close = true;
                                            tracing::info!(
                                                position_id = position_id,
                                                symbol = %asset.symbol,
                                                oid = keep_oid,
                                                passive_px = passive_px,
                                                live_qty = live_qty,
                                                "Preserving existing resting passive reduceOnly close order on illiquid Hyperliquid book"
                                            );
                                        }
                                    } else {
                                        if let Some(orders) = open_orders_opt.as_deref() {
                                            let cancels = collect_coin_non_trigger_cancels(
                                                orders, &asset, None,
                                            );
                                            if !cancels.is_empty() {
                                                mutated_open_orders = true;
                                                let _ = self.http.cancel_orders_batch(
                                                    cancels,
                                                    &pk,
                                                    Some(&self.db),
                                                    &cfg,
                                                );
                                            }
                                        }

                                        // Submit reduceOnly IOC order for exact live_qty clamped strictly inside oracle band
                                        let bbo_anchor = l2_bbo
                                            .map(|(bid, ask)| if close_is_buy { ask } else { bid })
                                            .filter(|&px| px > 0.0)
                                            .unwrap_or(mark_px);
                                        let band_limit_px =
                                            if close_is_buy { band_hi } else { band_lo };
                                        let band_limit_wire = format_hl_wire_price(
                                            band_limit_px,
                                            tick,
                                            asset.sz_decimals,
                                        );
                                        let mut rejected_wire_prices: HashSet<String> =
                                            HashSet::new();

                                        for (attempt, (ref_anchor, slip)) in [
                                            (bbo_anchor, 0.0),
                                            (bbo_anchor, 0.008),
                                            (bbo_anchor, 0.02),
                                            (mark_px, HL_ORACLE_BAND_PCT.min(0.038)),
                                        ]
                                        .into_iter()
                                        .enumerate()
                                        {
                                            if fills.rem_qty <= asset.lot_step_size * 0.5
                                                || fills.rem_qty <= 1e-9
                                            {
                                                filled_on_hl = true;
                                                break;
                                            }
                                            let wire_sz = format_hl_wire_size_floor(
                                                fills.rem_qty,
                                                asset.lot_step_size,
                                                asset.sz_decimals,
                                            );
                                            if wire_sz == "0" {
                                                filled_on_hl = fills.fill_qty_sum > 0.0;
                                                break;
                                            }
                                            let raw_limit_px = if close_is_buy {
                                                ref_anchor * (1.0 + slip)
                                            } else {
                                                ref_anchor * (1.0 - slip)
                                            };
                                            let limit_px = raw_limit_px.clamp(band_lo, band_hi);
                                            let wire_px = format_hl_wire_price(
                                                limit_px,
                                                tick,
                                                asset.sz_decimals,
                                            );
                                            if rejected_wire_prices.contains(&wire_px) {
                                                continue;
                                            }
                                            let at_band_limit = if close_is_buy {
                                                raw_limit_px >= band_hi - 1e-12
                                                    || wire_px == band_limit_wire
                                            } else {
                                                raw_limit_px <= band_lo + 1e-12
                                                    || wire_px == band_limit_wire
                                            };
                                            let wire = OrderWire {
                                                a: asset.asset_index,
                                                b: close_is_buy,
                                                p: wire_px.clone(),
                                                s: wire_sz,
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
                                            mutated_open_orders = true;
                                            match self.http.submit_order_wire(
                                                wire,
                                                fills.rem_qty,
                                                ref_anchor,
                                                taker_fee_bps,
                                                &pk,
                                                Some(&self.db),
                                                &cfg,
                                            ) {
                                                Ok(ack) => {
                                                    let accepted = ack.accepted;
                                                    let raw_err = ack.raw_error.clone();
                                                    if fills.record_fill(
                                                        ack,
                                                        ref_anchor,
                                                        asset.lot_step_size,
                                                    ) {
                                                        filled_on_hl = true;
                                                        break;
                                                    }
                                                    if !accepted {
                                                        rejected_wire_prices.insert(wire_px);
                                                        if is_ioc_no_match_error(raw_err.as_deref())
                                                            && at_band_limit
                                                        {
                                                            break;
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    tracing::warn!(
                                                        position_id = position_id,
                                                        symbol = %asset.symbol,
                                                        attempt = attempt,
                                                        error = %e,
                                                        "Hyperliquid IOC close submission error"
                                                    );
                                                }
                                            }
                                        }
                                    }

                                    // Fallback on illiquid books: submit ONE resting passive reduceOnly GTC limit order inside oracle band
                                    if !filled_on_hl
                                        && !kept_existing_passive_close
                                        && fills.rem_qty > asset.lot_step_size * 0.5
                                        && fills.rem_qty > 1e-9
                                    {
                                        let passive_sz = format_hl_wire_size_floor(
                                            fills.rem_qty,
                                            asset.lot_step_size,
                                            asset.sz_decimals,
                                        );
                                        if passive_sz != "0" {
                                            let passive_wire = OrderWire {
                                                a: asset.asset_index,
                                                b: close_is_buy,
                                                p: passive_wire_px,
                                                s: passive_sz,
                                                r: true,
                                                t: OrderTypeWire {
                                                    limit: Some(LimitOrderType {
                                                        tif: "Gtc".to_string(),
                                                    }),
                                                    trigger: None,
                                                },
                                                c: Some(derive_hl_cloid_hex(&format!(
                                                    "v2-close-passive-{position_id}-{}",
                                                    now_ms()
                                                ))),
                                            };
                                            mutated_open_orders = true;
                                            if let Ok(ack) = self.http.submit_order_wire(
                                                passive_wire,
                                                fills.rem_qty,
                                                passive_px,
                                                taker_fee_bps,
                                                &pk,
                                                Some(&self.db),
                                                &cfg,
                                            ) {
                                                if fills.record_fill(
                                                    ack,
                                                    passive_px,
                                                    asset.lot_step_size,
                                                ) {
                                                    filled_on_hl = true;
                                                }
                                            }
                                        }
                                    }

                                    if filled_on_hl {
                                        if fills.fill_qty_sum > 0.0 {
                                            exit_px = round_dp(
                                                fills.fill_notional_sum / fills.fill_qty_sum,
                                                6,
                                            );
                                            fee_usdc = round_dp(fills.live_fee_usdc, 4);
                                        }
                                        if let Some(ref eoid) = fills.close_eoid_opt {
                                            let oid_num =
                                                eoid.strip_prefix("hl-").unwrap_or(eoid.as_str());
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
                                        self.cancel_coin_exchange_orders(
                                            &asset, &wallet, &pk, &cfg, true,
                                        );
                                        self.illiquid_close_backoff_ms.lock().remove(&position_id);
                                    } else {
                                        exchange_close_unfilled = true;
                                        self.illiquid_close_backoff_ms
                                            .lock()
                                            .insert(position_id, now_ms());
                                        let tpsl_snapshot =
                                            if !mutated_open_orders && have_frontend_orders {
                                                open_orders_opt.as_deref()
                                            } else {
                                                None
                                            };
                                        let _ =
                                            self.chaser.ensure_exchange_tpsl_orders_with_orders(
                                                position_id,
                                                &asset,
                                                side,
                                                fills.rem_qty,
                                                exit_px,
                                                pos.protective_stop_px,
                                                pos.take_profit_px,
                                                pos.protective_stop_oid.as_deref(),
                                                pos.take_profit_oid.as_deref(),
                                                false,
                                                false,
                                                tpsl_snapshot,
                                                &cfg,
                                            );
                                        tracing::warn!(
                                            symbol = %asset.symbol,
                                            live_qty = live_qty,
                                            rem_qty = fills.rem_qty,
                                            kept_passive_close = kept_existing_passive_close,
                                            "Hyperliquid reduceOnly close did not completely fill due to illiquid Testnet book"
                                        );
                                    }
                                } else {
                                    fee_usdc = 0.0;
                                    self.cancel_coin_exchange_orders(
                                        &asset, &wallet, &pk, &cfg, true,
                                    );
                                    self.illiquid_close_backoff_ms.lock().remove(&position_id);
                                }
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
            cooldown_inserted_sec: if exchange_close_unfilled { 0 } else { cd_sec },
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

    /// Spawns independent background threads for:
    /// 1. 1-Second Priority Monitor & Maker Limit Chaser loop (never stalled by candle refreshes)
    /// 2. 3-Second Full-Exchange Scanner, OHLCV Candle Refresh & Multi-Sleeve Rebalance loop
    /// 3. 15-Second Hyperliquid 1:1 Exchange Reconciliation loop
    ///
    /// Returns a shutdown sender (`send(true)` stops all runtime threads).
    pub fn spawn_background_runtime(&self) -> tokio::sync::watch::Sender<bool> {
        let (tx, rx) = tokio::sync::watch::channel(false);

        let sleep_interruptible = |rx: &tokio::sync::watch::Receiver<bool>, total_ms: u64| {
            let mut slept = 0u64;
            while slept < total_ms && !*rx.borrow() {
                let chunk = (total_ms - slept).min(250);
                std::thread::sleep(Duration::from_millis(chunk));
                slept += chunk;
            }
        };

        // 1. Fast 1s Priority Monitor & ALO Chaser loop (fixed-rate tick scheduling)
        let prio_engine = self.clone();
        let prio_rx = rx.clone();
        std::thread::spawn(move || {
            while !*prio_rx.borrow() {
                let tick_start = std::time::Instant::now();
                let cfg = match prio_engine.db.load_config() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(error = %e, "1s priority watchlist failed to load config; skipping tick");
                        sleep_interruptible(&prio_rx, 1000);
                        continue;
                    }
                };
                let prio_ms = cfg
                    .get_i64("priority_monitor_interval_ms", 1000)
                    .clamp(250, 5000) as u64;
                if let Err(e) = prio_engine.priority_monitor.run_1s_priority_tick() {
                    tracing::warn!(error = %e, "1s priority watchlist tick error");
                }
                let elapsed_ms = tick_start.elapsed().as_millis() as u64;
                let rem_ms = prio_ms.saturating_sub(elapsed_ms).max(50);
                sleep_interruptible(&prio_rx, rem_ms);
            }
        });

        // 2. 3s Market Scanner, Candle Refresh, Signal Evaluation & Rebalance loop
        let scan_engine = self.clone();
        let scan_rx = rx.clone();
        std::thread::spawn(move || {
            while !*scan_rx.borrow() {
                let cfg = match scan_engine.db.load_config() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(error = %e, "3s scanner failed to load config; skipping cycle");
                        sleep_interruptible(&scan_rx, 3000);
                        continue;
                    }
                };
                let scan_ms = cfg.get_i64("scanner_interval_ms", 3000).max(1000) as u64;
                let execute_orders = cfg.get_bool("auto_rebalance_enabled", true);
                let eff_eq = scan_engine.resolve_effective_base_equity_usdc(&cfg);

                if let Err(e) = scan_engine.watcher.run_3s_exchange_scan() {
                    tracing::warn!(error = %e, "3s exchange scan error");
                } else {
                    match scan_engine.evaluate_and_persist_signals(eff_eq) {
                        Ok(signal_out) => {
                            let res = {
                                let _exec_guard = execution_lock().lock();
                                let reb_res = if execute_orders {
                                    scan_engine.execute_signal_rebalance(&signal_out, eff_eq)
                                } else {
                                    Ok(RebalanceExecutionReport::default())
                                };
                                let snap_res =
                                    scan_engine.record_portfolio_snapshot(None, Some(&signal_out));
                                reb_res.and(snap_res)
                            };
                            if let Err(e) = res {
                                tracing::warn!(error = %e, "3s signal rebalance/snapshot error");
                            }
                            let _ = scan_engine.alerts.poll_pending_queue(8);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "3s signal evaluation error");
                        }
                    }
                }
                sleep_interruptible(&scan_rx, scan_ms);
            }
        });

        // 3. 15s Hyperliquid 1:1 Reconciliation loop
        let rec_engine = self.clone();
        let rec_rx = rx;
        std::thread::spawn(move || {
            while !*rec_rx.borrow() {
                let cfg = match rec_engine.db.load_config() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(error = %e, "15s reconciler failed to load config; skipping reconcile");
                        sleep_interruptible(&rec_rx, 15_000);
                        continue;
                    }
                };
                let scan_ms = cfg.get_i64("scanner_interval_ms", 3000).max(1000) as u64;
                let reconcile_ms = (scan_ms * 5).max(5000);
                if cfg.execution_mode().is_live_or_testnet() {
                    if let Err(e) = rec_engine.reconciler.run_cutover_reconciliation(false) {
                        tracing::warn!(error = %e, "15s Hyperliquid 1:1 reconciliation error");
                    } else {
                        let _exec_guard = execution_lock().lock();
                        let _ = rec_engine.record_portfolio_snapshot(None, None);
                    }
                }
                sleep_interruptible(&rec_rx, reconcile_ms);
            }
        });

        tx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_db::AssetUpsertInput;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::Path;

    fn sample_met_asset() -> ExchangeAsset {
        ExchangeAsset {
            id: 142,
            symbol: "MET-USDC".to_string(),
            coin: "MET".to_string(),
            asset_index: 195,
            sz_decimals: 0,
            tick_size: 0.00001,
            lot_step_size: 1.0,
            max_leverage: 3.0,
            mark_price: 0.2137,
            mid_price: 0.188055,
            oracle_price: 0.25095,
            funding_rate_hourly: 0.0000125,
            day_volume_usdc: 1_000_000.0,
            open_interest_usdc: 500_000.0,
            prev_day_price: 0.22,
            is_core_eligible: false,
            is_event_eligible: false,
            is_priority_watched: true,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_000_000,
        }
    }

    fn spawn_mock_hl_server<F>(handler: F) -> (u16, std::sync::mpsc::Sender<()>)
    where
        F: FnMut(&str, &Value) -> (u16, Value) + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback mock server");
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        let handler_arc = Arc::new(Mutex::new(handler));

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
                        let (status, resp_json) = (handler_arc.lock())(&path, &req_json);
                        let resp_body = resp_json.to_string();
                        let resp = format!(
                            "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            status,
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

        (port, stop_tx)
    }

    fn setup_live_met_engine(
        port: u16,
        stale_db_mark: f64,
    ) -> (DatabaseManager, ExecutionEngine, i64) {
        let db = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
        let engine = ExecutionEngine::new(db.clone());
        let asset_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "MET".to_string(),
                asset_index: 195,
                sz_decimals: 0,
                tick_size: 0.00001,
                lot_step_size: 1.0,
                max_leverage: 3.0,
                mark_price: stale_db_mark,
                mid_price: stale_db_mark,
                oracle_price: 0.25095,
                funding_rate_hourly: 0.0000125,
                day_volume_usdc: 1_000_000.0,
                open_interest_usdc: 500_000.0,
                prev_day_price: 0.22,
                is_core_eligible: false,
                is_event_eligible: false,
                is_priority_watched: true,
            })
            .unwrap();

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

        let (pos_id, _) = db
            .sync_open_position_from_exchange_v2(
                asset_id,
                "MET",
                OrderSide::Long,
                0.23227,
                stale_db_mark,
                9634.0,
                2058.7858,
                0.0,
                0.5,
                0.20111,
                0.25086,
                0.0,
                Some(now_ms() - 600_000),
            )
            .unwrap();
        db.update_position_tpsl_orders(
            pos_id,
            0.20111,
            Some("hl-65605015926"),
            0.25086,
            Some("hl-65648930301"),
        )
        .unwrap();

        (db, engine, pos_id)
    }

    #[test]
    fn test_passive_reduce_only_close_matching_and_non_trigger_cancels() {
        let met = sample_met_asset();
        let band_lo = 0.2137 * (1.0 - 0.038);
        let band_hi = 0.2137 * (1.0 + 0.038);
        let (passive_px, passive_wire_px) = compute_passive_close_px(
            &met,
            false,
            Some((0.16241, 0.21370)),
            0.2137,
            band_lo,
            band_hi,
        );
        assert!((passive_px - 0.21369).abs() < 1e-9);
        assert_eq!(passive_wire_px, "0.21369");

        let orders = vec![
            // Resting SL trigger (must never match passive close or get cancelled)
            json!({
                "coin": "MET",
                "oid": 65605015926u64,
                "side": "A",
                "sz": "9634",
                "limitPx": "0.185",
                "reduceOnly": true,
                "isTrigger": true,
                "orderType": "Stop Market",
                "triggerPx": "0.20111",
                "timestamp": 1000
            }),
            // Older duplicate matching passive reduceOnly close
            json!({
                "coin": "MET",
                "oid": 70001u64,
                "side": "A",
                "sz": "9634",
                "limitPx": "0.21369",
                "reduceOnly": true,
                "isTrigger": false,
                "orderType": "Limit",
                "triggerPx": "0.0",
                "timestamp": 2000
            }),
            // Newest matching passive reduceOnly close (within 1 lot & 2 ticks)
            json!({
                "coin": "MET",
                "oid": 70002u64,
                "side": "A",
                "sz": "9633",
                "limitPx": "0.21371",
                "reduceOnly": true,
                "isTrigger": false,
                "orderType": "Limit",
                "triggerPx": "0.0",
                "timestamp": 3000
            }),
            // Non-reduceOnly entry order (must NOT match and MUST cancel)
            json!({
                "coin": "MET",
                "oid": 70003u64,
                "side": "B",
                "sz": "500",
                "limitPx": "0.21369",
                "reduceOnly": false,
                "isTrigger": false,
                "orderType": "Limit",
                "timestamp": 4000
            }),
        ];

        let keep = find_newest_matching_passive_close_oid(
            &orders,
            &met,
            false,
            9634.0,
            passive_px,
            &passive_wire_px,
        );
        assert_eq!(keep, Some(70002));

        let cancels = collect_coin_non_trigger_cancels(&orders, &met, keep);
        let cancel_oids: Vec<u64> = cancels.iter().map(|c| c.o).collect();
        assert_eq!(cancel_oids, vec![70001, 70003]);

        // Position TPSL Limit orders (`isPositionTpsl: true` even when `isTrigger: false`) must count as triggers
        let pos_tpsl_limit = json!({
            "coin": "MET",
            "oid": 70005u64,
            "side": "A",
            "sz": "9634",
            "limitPx": "0.21369",
            "reduceOnly": true,
            "isTrigger": false,
            "isPositionTpsl": true,
            "orderType": "Limit",
            "triggerPx": "0.0",
            "timestamp": 6000
        });
        assert!(is_hl_trigger_order(&pos_tpsl_limit));
        assert_eq!(
            find_newest_matching_passive_close_oid(
                std::slice::from_ref(&pos_tpsl_limit),
                &met,
                false,
                9634.0,
                passive_px,
                &passive_wire_px
            ),
            None
        );
        assert!(collect_coin_non_trigger_cancels(&[pos_tpsl_limit], &met, None).is_empty());
        assert!((parse_hl_f64(Some(&json!(" 0.2137 "))) - 0.2137).abs() < 1e-9);
        assert_eq!(parse_hl_oid_val(&json!({ "oid": " 70005 " })), Some(70005));

        // Off-price (>2 ticks) or off-size (>1 lot) orders must NOT match
        let too_far_px = json!({
            "coin": "MET",
            "oid": 70004u64,
            "side": "A",
            "sz": " 9634 ",
            "limitPx": "0.21372",
            "reduceOnly": true,
            "isTrigger": false,
            "orderType": "Limit",
            "timestamp": 5000
        });
        assert_eq!(
            find_newest_matching_passive_close_oid(
                &[too_far_px],
                &met,
                false,
                9634.0,
                passive_px,
                &passive_wire_px
            ),
            None
        );
        assert!(is_ioc_no_match_error(Some(
            "Order could not immediately match against any resting orders."
        )));
        assert!(is_ioc_no_match_error(Some("iocCancelRejected")));
    }

    #[test]
    fn test_close_or_cancel_illiquid_book_skips_iocs_and_preserves_passive_reduce_only() {
        let open_orders_state = Arc::new(Mutex::new(vec![
            json!({
                "coin": "MET",
                "oid": 65605015926u64,
                "isTrigger": true,
                "orderType": "Stop Market",
                "triggerPx": "0.20111",
                "sz": "9634"
            }),
            json!({
                "coin": "MET",
                "oid": 65648930301u64,
                "isTrigger": true,
                "orderType": "Take Profit Market",
                "triggerPx": "0.25086",
                "sz": "9634"
            }),
        ]));
        let open_orders_srv = open_orders_state.clone();
        let submitted_orders = Arc::new(Mutex::new(Vec::<Value>::new()));
        let submitted_srv = submitted_orders.clone();
        let cancelled_oids = Arc::new(Mutex::new(Vec::<u64>::new()));
        let cancelled_srv = cancelled_oids.clone();

        let (port, stop_tx) = spawn_mock_hl_server(move |path, body| {
            if path == "/info" {
                let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
                return match typ {
                    "clearinghouseState" => (
                        200,
                        json!({
                            "marginSummary": { "accountValue": "850.0" },
                            "assetPositions": [{
                                "position": {
                                    "coin": "MET",
                                    "szi": "9634",
                                    "entryPx": "0.23227",
                                    "positionValue": "2058.7858"
                                }
                            }]
                        }),
                    ),
                    "l2Book" => (
                        200,
                        json!({
                            "levels": [
                                [{"px": "0.16241", "sz": "193"}],
                                [{"px": "0.21370", "sz": "1400"}]
                            ]
                        }),
                    ),
                    "frontendOpenOrders" | "openOrders" => {
                        (200, Value::Array(open_orders_srv.lock().clone()))
                    }
                    _ => (200, json!([])),
                };
            }
            let action_type = body
                .pointer("/action/type")
                .and_then(Value::as_str)
                .unwrap_or("");
            if action_type == "cancel" {
                if let Some(arr) = body.pointer("/action/cancels").and_then(Value::as_array) {
                    for c in arr {
                        if let Some(o) = c.get("o").and_then(Value::as_u64) {
                            cancelled_srv.lock().push(o);
                        }
                    }
                }
                return (200, json!({ "status": "ok" }));
            }
            if action_type == "order" {
                if let Some(arr) = body.pointer("/action/orders").and_then(Value::as_array) {
                    for ord in arr {
                        submitted_srv.lock().push(ord.clone());
                    }
                }
                return (
                    200,
                    json!({
                        "status": "ok",
                        "response": {
                            "type": "order",
                            "data": {
                                "statuses": [{ "resting": { "oid": 880001u64 } }]
                            }
                        }
                    }),
                );
            }
            (200, json!({ "status": "ok" }))
        });

        // Stale DB mark 0.20115 must be superseded by live clearinghouse implied mark (2058.7858/9634 = 0.2137)
        let (_db, engine, pos_id) = setup_live_met_engine(port, 0.20115);

        // Pass 1: illiquid l2Book (bid 0.16241 < band_lo 0.20558) + no resting passive close -> 0 IOCs, 1 GTC @ 0.21369
        let rep1 = engine
            .close_or_cancel_position(pos_id, "CLOSE", Some(300))
            .unwrap();
        assert_eq!(rep1.final_status, "OPEN");
        assert!(engine
            .illiquid_close_backoff_ms
            .lock()
            .contains_key(&pos_id));
        assert!(cancelled_oids.lock().is_empty());
        {
            let subs = submitted_orders.lock();
            assert_eq!(subs.len(), 1);
            assert_eq!(subs[0].pointer("/t/limit/tif"), Some(&json!("Gtc")));
            assert_eq!(subs[0].get("r"), Some(&json!(true)));
            assert_eq!(subs[0].get("b"), Some(&json!(false)));
            assert_eq!(subs[0].get("p"), Some(&json!("0.21369")));
            assert_eq!(subs[0].get("s"), Some(&json!("9634")));
        }

        // Pass 2: resting passive reduceOnly limit order now exists on HL -> 0 IOCs, 0 GTCs, 0 cancels
        submitted_orders.lock().clear();
        cancelled_oids.lock().clear();
        open_orders_state.lock().push(json!({
            "coin": "MET",
            "oid": 880001u64,
            "side": "A",
            "sz": "9634",
            "limitPx": "0.21369",
            "reduceOnly": true,
            "isTrigger": false,
            "orderType": "Limit",
            "triggerPx": "0.0",
            "timestamp": now_ms()
        }));

        let rep2 = engine
            .close_or_cancel_position(pos_id, "CLOSE", Some(300))
            .unwrap();
        assert_eq!(rep2.final_status, "OPEN");
        assert!(submitted_orders.lock().is_empty());
        assert!(cancelled_oids.lock().is_empty());

        let _ = stop_tx.send(());
    }

    #[test]
    fn test_close_or_cancel_ioc_deduplicates_clamped_prices_and_breaks_early_on_band_reject() {
        let reject_error_msg = Arc::new(Mutex::new(
            "Order could not immediately match against any resting orders.".to_string(),
        ));
        let reject_srv = reject_error_msg.clone();
        let submitted_orders = Arc::new(Mutex::new(Vec::<Value>::new()));
        let submitted_srv = submitted_orders.clone();

        let (port, stop_tx) = spawn_mock_hl_server(move |path, body| {
            if path == "/info" {
                let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
                return match typ {
                    "clearinghouseState" => (
                        200,
                        json!({
                            "marginSummary": { "accountValue": "850.0" },
                            "assetPositions": [{
                                "position": {
                                    "coin": "MET",
                                    "szi": "9634",
                                    "entryPx": "0.23227",
                                    "positionValue": "1926.80"
                                }
                            }]
                        }),
                    ),
                    // mark_px = 0.20000 -> band_lo = 0.19240; bid 0.19300 is inside band so IOC loop runs
                    "l2Book" => (
                        200,
                        json!({
                            "levels": [
                                [{"px": "0.19300", "sz": "10"}],
                                [{"px": "0.21000", "sz": "1000"}]
                            ]
                        }),
                    ),
                    "frontendOpenOrders" | "openOrders" => (
                        200,
                        json!([
                            {
                                "coin": "MET",
                                "oid": 65605015926u64,
                                "isTrigger": true,
                                "orderType": "Stop Market",
                                "triggerPx": "0.20111",
                                "sz": "9634"
                            },
                            {
                                "coin": "MET",
                                "oid": 65648930301u64,
                                "isTrigger": true,
                                "orderType": "Take Profit Market",
                                "triggerPx": "0.25086",
                                "sz": "9634"
                            }
                        ]),
                    ),
                    _ => (200, json!([])),
                };
            }
            if body.pointer("/action/type").and_then(Value::as_str) == Some("order") {
                if let Some(arr) = body.pointer("/action/orders").and_then(Value::as_array) {
                    let mut is_ioc = false;
                    for ord in arr {
                        is_ioc = ord.pointer("/t/limit/tif").and_then(Value::as_str) == Some("Ioc");
                        submitted_srv.lock().push(ord.clone());
                    }
                    if is_ioc {
                        let msg = reject_srv.lock().clone();
                        return (
                            200,
                            json!({
                                "status": "ok",
                                "response": {
                                    "type": "order",
                                    "data": { "statuses": [{ "error": msg }] }
                                }
                            }),
                        );
                    }
                }
                return (
                    200,
                    json!({
                        "status": "ok",
                        "response": {
                            "type": "order",
                            "data": { "statuses": [{ "resting": { "oid": 880002u64 } }] }
                        }
                    }),
                );
            }
            (200, json!({ "status": "ok" }))
        });

        let (_db, engine, pos_id) = setup_live_met_engine(port, 0.20000);

        // Case A: ioc no-match at clamped band_lo (0.1924) breaks early after 2 IOC attempts (0.193, 0.1924) + 1 GTC
        let rep_a = engine
            .close_or_cancel_position(pos_id, "CLOSE", Some(300))
            .unwrap();
        assert_eq!(rep_a.final_status, "OPEN");
        {
            let subs = submitted_orders.lock();
            let ioc_prices: Vec<&str> = subs
                .iter()
                .filter(|o| o.pointer("/t/limit/tif").and_then(Value::as_str) == Some("Ioc"))
                .filter_map(|o| o.get("p").and_then(Value::as_str))
                .collect();
            assert_eq!(ioc_prices, vec!["0.193", "0.1924"]);
            let gtc_count = subs
                .iter()
                .filter(|o| o.pointer("/t/limit/tif").and_then(Value::as_str) == Some("Gtc"))
                .count();
            assert_eq!(gtc_count, 1);
        }

        // Case B: non-ioc error still deduplicates clamped band_lo across attempts 1, 2, 3 (only 2 IOC calls, never 4)
        submitted_orders.lock().clear();
        *reject_error_msg.lock() = "Temporary liquidity lock".to_string();
        let rep_b = engine
            .close_or_cancel_position(pos_id, "CLOSE", Some(300))
            .unwrap();
        assert_eq!(rep_b.final_status, "OPEN");
        {
            let subs = submitted_orders.lock();
            let ioc_prices: Vec<&str> = subs
                .iter()
                .filter(|o| o.pointer("/t/limit/tif").and_then(Value::as_str) == Some("Ioc"))
                .filter_map(|o| o.get("p").and_then(Value::as_str))
                .collect();
            assert_eq!(ioc_prices, vec!["0.193", "0.1924"]);
        }

        let _ = stop_tx.send(());
    }

    #[test]
    fn test_passes_entry_l2_liquidity_gate() {
        let healthy = L2BookDepthSnapshot {
            best_bid: 100.0,
            best_ask: 100.05,
            spread_bps: 5.0,
            bid_depth_15bps_usdc: 500.0,
            ask_depth_15bps_usdc: 500.0,
        };
        assert!(passes_entry_l2_liquidity_gate(&healthy, 200.0, 20.0).is_ok());

        // Exact boundary spread (20.0 bps) and exact min_side_depth ($25 floor for $60 notional) pass
        let exact_floor = L2BookDepthSnapshot {
            best_bid: 100.0,
            best_ask: 100.20,
            spread_bps: 20.0,
            bid_depth_15bps_usdc: 25.0,
            ask_depth_15bps_usdc: 25.0,
        };
        assert!(passes_entry_l2_liquidity_gate(&exact_floor, 60.0, 20.0).is_ok());

        // Wide spread (> 20 bps, e.g. 54 bps like kSHIB) rejects
        let wide = L2BookDepthSnapshot {
            spread_bps: 54.0,
            ..healthy
        };
        let err_wide = passes_entry_l2_liquidity_gate(&wide, 200.0, 20.0).unwrap_err();
        assert!(err_wide.contains("ENTRY_SPREAD_TOO_WIDE"));

        // One-sided thin ask depth (< $25 floor) rejects even with deep bids
        let thin_ask = L2BookDepthSnapshot {
            ask_depth_15bps_usdc: 24.99,
            ..healthy
        };
        let err_ask = passes_entry_l2_liquidity_gate(&thin_ask, 60.0, 20.0).unwrap_err();
        assert!(err_ask.contains("ENTRY_L2_DEPTH_TOO_THIN"));

        // One-sided thin bid depth (< 25% of $400 notional = $100) rejects even with deep asks
        let thin_bid = L2BookDepthSnapshot {
            bid_depth_15bps_usdc: 99.9,
            ask_depth_15bps_usdc: 800.0,
            ..healthy
        };
        let err_bid = passes_entry_l2_liquidity_gate(&thin_bid, 400.0, 20.0).unwrap_err();
        assert!(err_bid.contains("ENTRY_L2_DEPTH_TOO_THIN"));
        let ok_bid = L2BookDepthSnapshot {
            bid_depth_15bps_usdc: 100.0,
            ask_depth_15bps_usdc: 100.0,
            ..healthy
        };
        assert!(passes_entry_l2_liquidity_gate(&ok_bid, 400.0, 20.0).is_ok());

        // Invalid / crossed BBO rejects
        let crossed = L2BookDepthSnapshot {
            best_bid: 101.0,
            best_ask: 100.0,
            spread_bps: -99.0,
            bid_depth_15bps_usdc: 500.0,
            ask_depth_15bps_usdc: 500.0,
        };
        assert!(passes_entry_l2_liquidity_gate(&crossed, 100.0, 20.0).is_err());
    }
}
