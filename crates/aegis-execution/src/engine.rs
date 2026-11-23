//! Unified Aegis Quant v2.0 Execution Engine (`aegis-execution::engine`).
//!
//! Coordinates:
//! - 1-Second High-Frequency Priority Surveillance Loop (`PriorityMonitor`)
//! - 3-Second Full-Exchange 150+ Perp Market Watcher (`MarketWatcher`)
//! - 1-Second Maker Limit Chaser & Order-to-Open Capital Lifecycle (`OrderChaser`)
//! - Multi-Sleeve S1/S2/S3 Signal Evaluation & Risk-Tier Allocator (`aegis-signals`, `aegis-risk`)
//! - Dynamic Cutover & Live Hyperliquid Exchange Reconciliation (`CutoverReconciler`)
//! - Unlocked 60-Second Auxiliary Ledger Sync (`AuxiliarySyncWorker`) & Bounded Alert Dispatch (`AlertDispatchWorker`)

use crate::close_ladder::{
    cancel_batch_response_ok, collect_coin_non_trigger_cancels, submit_reduce_only_close_ladder,
    ReduceOnlyCloseLadderRequest,
};
use crate::http_client::{
    endpoints_are_loopback, HyperliquidHttpClient, L2BookDepthSnapshot, OpenOrdersSnapshot,
};
use crate::lock_guard::acquire_execution_lock;
use crate::market_watcher::{MarketScanReport, MarketWatcher};
use crate::order_chaser::{NewWorkingOrderRequest, OrderChaser};
use crate::priority_monitor::{PriorityMonitor, PriorityTickReport};
use crate::reconcile::{
    AuxiliarySyncWorker, CutoverReconcileReport, CutoverReconciler, AUX_SYNC_INTERVAL_MS,
};
use crate::signing::CancelWire;
use crate::ws_client::{
    build_ws_subscription_set_from_db, has_explicit_loopback_ws_url, should_enable_ws_manager,
    HyperliquidWsManager, WsStateCache, WsSubscriptionSet, WsTimingConfig,
};
use aegis_alerts::{
    format_cutover_reconcile_html, format_position_closed_html, AlertEngine, CutoverReconcileAlert,
    PositionClosedAlert,
};
use aegis_core::{
    extract_hl_oid, normalize_hyperliquid_symbol, now_ms, parse_hl_f64, round_dp, AegisError,
    ConfigMap, ExchangeAsset, HeldAssetView, OhlcvBar, OrderSide, OrderedAssetView,
    PortfolioSnapshot, PositionRecord, PositionStatus, Result,
};
use aegis_db::{DatabaseManager, SignalUpsertInput, SnapshotInsertInput};
use aegis_risk::{compute_atr_from_bars, RiskEngine};
use aegis_signals::{
    BarCloseCache, HeldPositionContext, SignalCycleOutput, SignalEngine, ACTION_CLOSE,
    ACTION_COOLDOWN_BLOCKED, ACTION_FLIP, ACTION_OPEN_LONG, ACTION_OPEN_SHORT,
};
use parking_lot::{Mutex, ReentrantMutex};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

static GLOBAL_EXECUTION_LOCK: OnceLock<Arc<ReentrantMutex<()>>> = OnceLock::new();

pub fn execution_lock() -> &'static Arc<ReentrantMutex<()>> {
    GLOBAL_EXECUTION_LOCK.get_or_init(|| Arc::new(ReentrantMutex::new(())))
}

pub(crate) fn is_live_exchange_active(db: &DatabaseManager, cfg: &ConfigMap) -> bool {
    cfg.execution_mode().is_live_or_testnet()
        && (!db.is_ephemeral_test_db()
            || endpoints_are_loopback(cfg)
            || has_explicit_loopback_ws_url(cfg))
}

pub const MAX_L2_DEPTH_CHECKS_PER_REBALANCE: usize = 6;
pub const CAP_ENFORCEMENT_TOLERANCE_MULT: f64 = 1.03;
const AUTO_DELEVERAGE_CONFIRM_WINDOW_MS: i64 = 30_000;
const AUTO_DELEVERAGE_EQUITY_FRESH_MS: i64 = 20_000;
const EARLY_LOSER_MIN_HOLD_MS: i64 = 120_000;
const EARLY_LOSER_PNL_THRESHOLD_FRAC: f64 = -0.003;
const DEFAULT_MIN_L2_DEPTH_MULT: f64 = 1.5;
const DEFAULT_MIN_L2_DEPTH_USDC: f64 = 150.0;

struct EntryGateThresholds {
    max_spread_bps: f64,
    max_oracle_deviation_pct: f64,
    min_l2_depth_mult: f64,
    min_l2_depth_usdc: f64,
}

#[derive(Debug, Default)]
struct DeleverageConfirmState {
    first_breach_ts_ms: Option<i64>,
    last_tick_ts_ms: Option<i64>,
    consecutive_ticks: usize,
}

impl DeleverageConfirmState {
    fn reset(&mut self) {
        self.first_breach_ts_ms = None;
        self.last_tick_ts_ms = None;
        self.consecutive_ticks = 0;
    }

    fn record_breach_tick(&mut self, ts_ms: i64) -> usize {
        let within_window = self
            .first_breach_ts_ms
            .is_some_and(|first| ts_ms.saturating_sub(first) <= AUTO_DELEVERAGE_CONFIRM_WINDOW_MS);
        if within_window {
            self.consecutive_ticks = self.consecutive_ticks.saturating_add(1);
        } else {
            self.first_breach_ts_ms = Some(ts_ms);
            self.consecutive_ticks = 1;
        }
        self.last_tick_ts_ms = Some(ts_ms);
        self.consecutive_ticks
    }
}

struct InFlightIdGuard {
    set: Arc<Mutex<HashSet<i64>>>,
    id: i64,
}

impl InFlightIdGuard {
    fn try_acquire(set: &Arc<Mutex<HashSet<i64>>>, id: i64) -> Option<Self> {
        let mut guard = set.lock();
        if guard.insert(id) {
            Some(Self {
                set: Arc::clone(set),
                id,
            })
        } else {
            None
        }
    }
}

impl Drop for InFlightIdGuard {
    fn drop(&mut self) {
        self.set.lock().remove(&self.id);
    }
}

/// Bounded asynchronous alert queue worker (`sync_channel(64)`) replacing unbounded
/// per-close `std::thread::spawn` dispatches (`R14-L5`).
pub struct AlertDispatchWorker;

impl AlertDispatchWorker {
    /// Spawns a single persistent alert dispatch worker backed by a bounded `sync_channel(64)`
    /// that exits cleanly as soon as all `SyncSender` clones are dropped.
    pub fn spawn(alerts: AlertEngine) -> (SyncSender<usize>, JoinHandle<()>) {
        let (tx, rx) = mpsc::sync_channel::<usize>(64);
        let handle =
            std::thread::spawn(
                move || {
                    while Self::drain_once(&alerts, &rx, Duration::from_secs(5)) {}
                },
            );
        (tx, handle)
    }

    /// Waits up to `timeout` for a wakeup signal and drains queued alerts, returning `false`
    /// once the channel disconnects.
    pub fn drain_once(alerts: &AlertEngine, rx: &Receiver<usize>, timeout: Duration) -> bool {
        match rx.recv_timeout(timeout) {
            Ok(initial_batch) => {
                let mut batch = initial_batch;
                while let Ok(extra) = rx.try_recv() {
                    batch = batch.max(extra);
                }
                let _ = alerts.poll_pending_queue(batch.clamp(1, 32));
                true
            }
            Err(RecvTimeoutError::Timeout) => {
                let _ = alerts.poll_pending_queue(8);
                true
            }
            Err(RecvTimeoutError::Disconnected) => false,
        }
    }
}

/// Validates asset oracle sanity and mark/mid vs oracle deviation fail-closed prior to
/// consuming an `l2Book` REST weight (`P0-Liq`).
pub fn passes_entry_oracle_deviation_gate(
    asset: &ExchangeAsset,
    max_entry_oracle_deviation_pct: f64,
) -> std::result::Result<(), String> {
    let max_dev = if max_entry_oracle_deviation_pct.is_finite() {
        max_entry_oracle_deviation_pct.clamp(0.001, 0.25)
    } else {
        0.015
    };
    if !asset.oracle_price.is_finite() || asset.oracle_price <= 0.0 {
        return Err("ENTRY_ORACLE_UNAVAILABLE".to_string());
    }
    let oracle = asset.oracle_price;
    let mid = if asset.mid_price.is_finite() && asset.mid_price > 0.0 {
        asset.mid_price
    } else if asset.mark_price.is_finite() && asset.mark_price > 0.0 {
        asset.mark_price
    } else {
        return Err("ENTRY_MID_PRICE_UNAVAILABLE".to_string());
    };
    let mid_dev = (mid - oracle).abs() / oracle;
    if !mid_dev.is_finite() || mid_dev > max_dev {
        return Err(format!(
            "ENTRY_MID_ORACLE_DEVIATION: mid={mid:.6}, oracle={oracle:.6}, dev={:.2}% > max={:.2}%",
            mid_dev * 100.0,
            max_dev * 100.0
        ));
    }
    if !asset.mark_price.is_finite() || asset.mark_price <= 0.0 {
        return Err("ENTRY_MARK_PRICE_UNAVAILABLE".to_string());
    }
    let mark_dev = (asset.mark_price - oracle).abs() / oracle;
    if !mark_dev.is_finite() || mark_dev > max_dev {
        return Err(format!(
            "ENTRY_MARK_ORACLE_DEVIATION: mark={:.6}, oracle={oracle:.6}, dev={:.2}% > max={:.2}%",
            asset.mark_price,
            mark_dev * 100.0,
            max_dev * 100.0
        ));
    }
    Ok(())
}

/// Pure pre-entry L2 liquidity gate checking bid-ask spread and configurable two-sided 15 bps depth
/// (`min_depth_mult` default `1.5`, `min_depth_usdc` default `$150`) (`P0-Liq`).
pub fn passes_entry_l2_liquidity_gate_with_thresholds(
    book: &L2BookDepthSnapshot,
    target_notional_usdc: f64,
    max_spread_bps: f64,
    min_depth_mult: f64,
    min_depth_usdc: f64,
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
    let eff_mult = if min_depth_mult.is_finite() && min_depth_mult >= 0.0 {
        min_depth_mult
    } else {
        DEFAULT_MIN_L2_DEPTH_MULT
    };
    let eff_floor = if min_depth_usdc.is_finite() && min_depth_usdc >= 0.0 {
        min_depth_usdc
    } else {
        DEFAULT_MIN_L2_DEPTH_USDC
    };
    let min_side_depth = (eff_notional * eff_mult).max(eff_floor);
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

/// Default-threshold entry L2 liquidity gate (`1.5x` notional, `$150` floor).
pub fn passes_entry_l2_liquidity_gate(
    book: &L2BookDepthSnapshot,
    target_notional_usdc: f64,
    max_spread_bps: f64,
) -> std::result::Result<(), String> {
    passes_entry_l2_liquidity_gate_with_thresholds(
        book,
        target_notional_usdc,
        max_spread_bps,
        DEFAULT_MIN_L2_DEPTH_MULT,
        DEFAULT_MIN_L2_DEPTH_USDC,
    )
}

/// Returns whether a held position is eligible for automated cap-epoch deleveraging (`R14-H1`).
/// Legacy pre-cap positions remain excluded ONLY from `AUTO_DELEVERAGE_RISK_CAP` liquidation;
/// they ALWAYS remain counted in total exposure, `max_open_positions`, sizing, and 24h drawdown.
pub(crate) fn is_deleverage_eligible_holding(h: &HeldAssetView, cap_epoch_ms: i64) -> bool {
    cap_epoch_ms <= 0 || h.opened_at_ms.unwrap_or(h.created_at_ms) >= cap_epoch_ms
}

/// Returns whether a resting working order is eligible for automated cap-epoch cancellation (`R14-H1`).
pub(crate) fn is_deleverage_eligible_order(o: &OrderedAssetView, cap_epoch_ms: i64) -> bool {
    cap_epoch_ms <= 0 || o.created_at_ms >= cap_epoch_ms
}

/// Inspects `risk_capital_cap_updated_at_ms` at startup/reconcile and returns warning strings
/// whenever the cap epoch lies in the future or open holdings pre-date it (`R14-H1`).
pub fn validate_cap_epoch_on_startup(
    cfg: &ConfigMap,
    held: &[HeldAssetView],
    now_ms_val: i64,
) -> Vec<String> {
    if !cfg.execution_mode().is_live_or_testnet() {
        return Vec::new();
    }
    let cap_epoch_ms = cfg.get_i64("risk_capital_cap_updated_at_ms", 0);
    if cap_epoch_ms <= 0 {
        return Vec::new();
    }
    let mut warnings = Vec::new();
    if cap_epoch_ms > now_ms_val + 60_000 {
        let msg = format!(
            "risk_capital_cap_updated_at_ms ({cap_epoch_ms}) is in the future relative to now_ms ({now_ms_val}); all existing positions would be excluded from AUTO_DELEVERAGE_RISK_CAP"
        );
        tracing::warn!(cap_epoch_ms = cap_epoch_ms, now_ms = now_ms_val, "{msg}");
        warnings.push(msg);
    }
    let excluded_symbols: Vec<String> = held
        .iter()
        .filter(|h| !is_deleverage_eligible_holding(h, cap_epoch_ms))
        .map(|h| format!("{}#{}", h.symbol, h.position_id))
        .collect();
    if !excluded_symbols.is_empty() {
        let msg = format!(
            "{} open position(s) pre-date risk_capital_cap_updated_at_ms ({cap_epoch_ms}) and are excluded from auto-deleverage (still counted in risk caps): {}",
            excluded_symbols.len(),
            excluded_symbols.join(", ")
        );
        tracing::warn!(
            cap_epoch_ms = cap_epoch_ms,
            excluded_count = excluded_symbols.len(),
            "{msg}"
        );
        warnings.push(msg);
    }
    warnings
}

/// DB-backed wrapper invoked at cutover/reconcile phase 1 (`R14-H1`). Does not acquire `execution_lock()`.
pub(crate) fn validate_cap_epoch_with_db(
    db: &DatabaseManager,
    cfg: &ConfigMap,
    emit_alert: bool,
) -> Result<Vec<String>> {
    let held = db.get_assets_held()?;
    let warnings = validate_cap_epoch_on_startup(cfg, &held, now_ms());
    if emit_alert && !warnings.is_empty() {
        let alerts = AlertEngine::new(db.clone());
        let body = warnings.join("\n");
        let _ = alerts.enqueue_html_alert(
            "CAP_EPOCH_WARNING",
            "WARNING",
            &format!("<b>CAP EPOCH WARNING</b>\n<pre>{body}</pre>"),
            Some(&json!({ "warnings": warnings })),
        );
    }
    Ok(warnings)
}

fn held_position_fractional_pnl(h: &HeldAssetView) -> f64 {
    if h.mark_price.is_finite()
        && h.mark_price > 0.0
        && h.entry_price.is_finite()
        && h.entry_price > 0.0
        && (h.mark_price - h.entry_price).abs() > 1e-12
    {
        if let Ok(side) = OrderSide::parse(&h.side) {
            return ((h.mark_price - h.entry_price) / h.entry_price) * side.sign();
        }
    }
    if h.unrealized_pnl_pct.is_finite() {
        h.unrealized_pnl_pct / 100.0
    } else {
        0.0
    }
}

/// Evaluates whether a held position's estimated net exit PnL (mark-to-market unrealized PnL
/// plus accrued funding minus paid fees and estimated taker exit fee) clears the profit threshold
/// `entry_notional * min_net_profit_pct` (and is strictly positive).
pub fn signal_exit_passes_profit_gate(
    h: &HeldAssetView,
    min_net_profit_pct: f64,
    taker_fee_bps: f64,
) -> (bool, f64, f64) {
    let live_upnl = if h.entry_price.is_finite()
        && h.entry_price > 0.0
        && h.mark_price.is_finite()
        && h.mark_price > 0.0
        && h.quantity.is_finite()
        && h.quantity > 0.0
    {
        match OrderSide::parse(&h.side) {
            Ok(OrderSide::Long) => (h.mark_price - h.entry_price) * h.quantity,
            Ok(OrderSide::Short) => (h.entry_price - h.mark_price) * h.quantity,
            Err(err) => {
                tracing::error!(
                    position_id = h.position_id,
                    symbol = %h.symbol,
                    side = %h.side,
                    error = %err,
                    "Invalid OrderSide in signal_exit_passes_profit_gate; failing closed"
                );
                return (false, 0.0, 0.0);
            }
        }
    } else if h.unrealized_pnl_usdc.is_finite() {
        h.unrealized_pnl_usdc
    } else {
        0.0
    };

    let entry_notional = if h.entry_price.is_finite()
        && h.entry_price > 0.0
        && h.quantity.is_finite()
        && h.quantity > 0.0
    {
        (h.entry_price * h.quantity)
            .abs()
            .max(h.committed_notional_usdc.abs())
    } else {
        h.committed_notional_usdc.abs()
    };
    let exit_notional = if h.mark_price.is_finite()
        && h.mark_price > 0.0
        && h.quantity.is_finite()
        && h.quantity > 0.0
    {
        (h.mark_price * h.quantity).abs()
    } else {
        entry_notional
    };

    let fee_rate = if taker_fee_bps.is_finite() {
        taker_fee_bps.clamp(0.0, 100.0) / 10_000.0
    } else {
        0.00045
    };
    let est_exit_fee = exit_notional * fee_rate;
    let funding = if h.funding_accrued_usdc.is_finite() {
        h.funding_accrued_usdc
    } else {
        0.0
    };
    let fees_paid = if h.fees_paid_usdc.is_finite() {
        h.fees_paid_usdc.max(0.0)
    } else {
        0.0
    };
    let net_exit_pnl_usdc = live_upnl + funding - fees_paid - est_exit_fee;
    let min_pct = if min_net_profit_pct.is_finite() {
        min_net_profit_pct.clamp(0.0, 0.50)
    } else {
        0.0025
    };
    let min_required_profit_usdc = entry_notional * min_pct;
    let allowed = net_exit_pnl_usdc.is_finite()
        && net_exit_pnl_usdc > 0.0
        && net_exit_pnl_usdc >= min_required_profit_usdc;
    (allowed, net_exit_pnl_usdc, min_required_profit_usdc)
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
    pub cooldown_blocked_count: usize,
    pub cooldown_blocked_symbols: Vec<String>,
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

struct OpenPositionCloseOutcome {
    exit_px: f64,
    closed_qty: f64,
    fee_usdc: f64,
    close_eoid_opt: Option<String>,
    exchange_close_unfilled: bool,
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
    pub aux_sync: AuxiliarySyncWorker,
    pub alerts: AlertEngine,
    pub ws_cache: Arc<WsStateCache>,
    pub illiquid_close_backoff_ms: Arc<Mutex<HashMap<i64, i64>>>,
    pub in_flight_position_ops: Arc<Mutex<HashSet<i64>>>,
    pub in_flight_entry_assets: Arc<Mutex<HashSet<i64>>>,
    deleverage_confirm_state: Arc<Mutex<DeleverageConfirmState>>,
    signal_bar_cache: Arc<Mutex<BarCloseCache>>,
    alert_wakeup_tx: Option<SyncSender<usize>>,
    ws_sub_tx: Arc<tokio::sync::watch::Sender<WsSubscriptionSet>>,
}

impl ExecutionEngine {
    pub fn new(db: DatabaseManager) -> Self {
        Self::with_timeout(db, 8)
    }

    pub fn with_timeout(db: DatabaseManager, timeout_sec: u64) -> Self {
        let ws_cache = Arc::new(WsStateCache::new());
        let (ws_sub_tx_raw, _ws_sub_rx) = tokio::sync::watch::channel(WsSubscriptionSet::default());
        let ws_sub_tx = Arc::new(ws_sub_tx_raw);

        let http = HyperliquidHttpClient::new(timeout_sec).with_ws_cache(Arc::clone(&ws_cache));
        let alerts = AlertEngine::new(db.clone());
        let watcher = MarketWatcher::new(db.clone(), http.clone())
            .with_ws_cache(Arc::clone(&ws_cache))
            .with_ws_subscriptions(Arc::clone(&ws_sub_tx));
        let in_flight_position_ops = Arc::new(Mutex::new(HashSet::new()));
        let in_flight_entry_assets = Arc::new(Mutex::new(HashSet::new()));

        let (alert_wakeup_tx, chaser, priority_monitor) = if !db.is_ephemeral_test_db() {
            let (tx, _worker) = AlertDispatchWorker::spawn(alerts.clone());
            let chaser = OrderChaser::new(db.clone(), http.clone(), alerts.clone())
                .with_alert_wakeup(tx.clone())
                .with_ws_cache(Arc::clone(&ws_cache));
            let priority_monitor =
                PriorityMonitor::new(db.clone(), http.clone(), chaser.clone(), alerts.clone())
                    .with_alert_wakeup(tx.clone())
                    .with_ws_cache(Arc::clone(&ws_cache));
            (Some(tx), chaser, priority_monitor)
        } else {
            let chaser = OrderChaser::new(db.clone(), http.clone(), alerts.clone())
                .with_ws_cache(Arc::clone(&ws_cache));
            let priority_monitor =
                PriorityMonitor::new(db.clone(), http.clone(), chaser.clone(), alerts.clone())
                    .with_ws_cache(Arc::clone(&ws_cache));
            (None, chaser, priority_monitor)
        };

        let reconciler = CutoverReconciler::new(db.clone(), http.clone())
            .with_in_flight_position_ops(Arc::clone(&in_flight_position_ops))
            .with_ws_cache(Arc::clone(&ws_cache));
        let aux_sync =
            AuxiliarySyncWorker::new(db.clone(), http.clone()).with_ws_cache(Arc::clone(&ws_cache));

        Self {
            db,
            http,
            watcher,
            chaser,
            priority_monitor,
            reconciler,
            aux_sync,
            alerts,
            ws_cache,
            illiquid_close_backoff_ms: Arc::new(Mutex::new(HashMap::new())),
            in_flight_position_ops,
            in_flight_entry_assets,
            deleverage_confirm_state: Arc::new(Mutex::new(DeleverageConfirmState::default())),
            signal_bar_cache: Arc::new(Mutex::new(BarCloseCache::default())),
            alert_wakeup_tx,
            ws_sub_tx,
        }
    }

    pub fn ws_cache(&self) -> &Arc<WsStateCache> {
        &self.ws_cache
    }

    pub fn ws_subscription_sender(&self) -> &Arc<tokio::sync::watch::Sender<WsSubscriptionSet>> {
        &self.ws_sub_tx
    }

    fn wake_alert_worker(&self, batch: usize) {
        if let Some(ref tx) = self.alert_wakeup_tx {
            let _ = tx.try_send(batch);
        }
    }

    fn enqueue_non_blocking_alert(
        &self,
        event_type: &str,
        severity: &str,
        html_message: &str,
        payload: Option<&Value>,
    ) -> &'static str {
        let status =
            match self
                .alerts
                .enqueue_html_alert(event_type, severity, html_message, payload)
            {
                Ok(_) => "ENQUEUED",
                Err(err) => {
                    tracing::warn!(
                        event_type = event_type,
                        error = %err,
                        "Failed to enqueue non-blocking HTML alert"
                    );
                    "ENQUEUE_FAILED"
                }
            };
        if !self.db.is_ephemeral_test_db() {
            self.wake_alert_worker(8);
        }
        status
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
        if wallet.trim().is_empty() {
            return;
        }
        let open_orders_opt = self
            .ws_cache
            .get_fresh_open_orders()
            .filter(|s| s.metadata_complete)
            .or_else(|| self.http.fetch_open_orders_with_metadata(wallet, cfg).ok());
        if let Some(snap) = open_orders_opt {
            let cancels: Vec<CancelWire> = if include_triggers {
                snap.orders
                    .iter()
                    .filter_map(|ord| {
                        let c = ord.get("coin").and_then(Value::as_str).unwrap_or("").trim();
                        if c.eq_ignore_ascii_case(&asset.coin) {
                            extract_hl_oid(ord).map(|o| CancelWire {
                                a: asset.asset_index,
                                o,
                            })
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                collect_coin_non_trigger_cancels(&snap, asset, None)
            };
            if !cancels.is_empty() {
                let _ = self
                    .http
                    .cancel_orders_batch(cancels, pk, Some(&self.db), cfg);
            }
        }
    }

    /// Executes the full v2.0 cutover sequence strictly unlocked (`R14-H2`):
    /// 1. Discovers and seeds perpetual assets (`metaAndAssetCtxs`).
    /// 2. Seeds 1h/4h OHLCV candle series for core/event eligible assets.
    /// 3. Runs unlocked startup auxiliary sync (`extraAgents`, `userFees`, `userFunding` backfill).
    /// 4. Dynamically reconciles live Hyperliquid `clearinghouseState`, `openOrders`, and `userFillsByTime`.
    /// 5. Runs unlocked post-reconcile auxiliary views (`portfolio`, `vaultDetails`).
    /// 6. Computes multi-sleeve signals, records `PortfolioSnapshot`, and enqueues the Cutover alert non-blockingly.
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
        let _ = self.aux_sync.sync_startup_before_reconcile();

        let reconcile = self
            .reconciler
            .run_cutover_reconciliation(close_legacy_positions)?;
        let _ = self.aux_sync.sync_post_reconcile_views();

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
            execution_mode: post_reconcile_cfg.execution_mode().as_str().to_string(),
            mariadb_version: mariadb_ver,
            risk_tier: post_reconcile_cfg.risk_tier(),
            leverage_multiplier: Some(post_reconcile_cfg.target_leverage_multiplier()),
            discovered_perps: market_scan.total_assets_upserted,
            cancelled_legacy_orders: reconcile.stale_orders_cancelled,
            closed_legacy_positions: reconcile.legacy_positions_closed
                + reconcile.legacy_positions_adopted,
            total_equity_usdc: snapshot.total_equity_usdc,
            available_cash_usdc: snapshot.available_cash_usdc,
            timestamp_ms: now_ms(),
        };
        let html = format_cutover_reconcile_html(&cutover_alert);
        let payload = serde_json::to_value(&cutover_alert).ok();
        let alert_status = self
            .enqueue_non_blocking_alert("CUTOVER_RECONCILED", "INFO", &html, payload.as_ref())
            .to_string();

        Ok(CutoverFullReport {
            market_scan,
            reconcile,
            signals_evaluated: signal_out.signals.len(),
            snapshot,
            alert_status,
        })
    }

    /// Evaluates all three alpha sleeves (`S1 Carry-Momentum`, `S2 Trend-Breakout`, `S3 Event-Reversion`)
    /// with asymmetric [`HeldPositionContext`] across `exchange_assets`, persists `sleeve_signals`,
    /// and returns [`SignalCycleOutput`] without holding `execution_lock()` over candle fetches.
    pub fn evaluate_and_persist_signals(&self, base_equity_usdc: f64) -> Result<SignalCycleOutput> {
        let (cfg, assets, held, ordered, cooldowns) = {
            let _exec_guard = acquire_execution_lock();
            (
                self.db.load_config()?,
                self.db.list_exchange_assets()?,
                self.db.get_assets_held()?,
                self.db.get_assets_ordered()?,
                self.db.get_assets_in_cooldown()?,
            )
        };

        let eq = base_equity_usdc.max(100.0);
        let held_contexts = build_held_position_contexts(&held, &ordered, eq)?;

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
                let use_live_candles = is_live_exchange_active(&self.db, &cfg);
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
        let output = {
            let mut bar_cache = self.signal_bar_cache.lock();
            sig_engine.evaluate_universe_with_cache(
                &assets,
                &bars_by_asset,
                &held_contexts,
                &cooldowns,
                &mut bar_cache,
            )
        };

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

        {
            let _exec_guard = acquire_execution_lock();
            self.db.upsert_sleeve_signals_batch(&signal_inputs)?;
            let _ = self.db.set_assets_priority_watch_batch(&prio_updates);
        }

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

    /// Resolves the active base portfolio equity in USDC.
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

    fn compute_ruin_halt_uncapped_equity(
        &self,
        cfg: &ConfigMap,
        held: &[HeldAssetView],
    ) -> Option<f64> {
        if cfg.execution_mode().is_live_or_testnet() {
            let live_eq = cfg.get_f64("live_exchange_equity_usdc", 0.0);
            let synced_at = cfg.get_i64("live_exchange_equity_synced_at_ms", 0);
            if synced_at > 0 || live_eq > 0.0 {
                return Some(live_eq.max(0.0));
            }
            if let Ok(snap) = self.db.get_latest_portfolio_snapshot() {
                if snap.uncapped_equity_usdc > 0.0 {
                    return Some(snap.uncapped_equity_usdc);
                }
                if snap.total_equity_usdc > 0.0 {
                    return Some(snap.total_equity_usdc);
                }
            }
            None
        } else {
            let seed_eq = cfg.get_f64("initial_paper_equity_usdc", 10_000.0);
            let closed_realized_pnl: f64 = self
                .db
                .list_closed_positions(200)
                .unwrap_or_default()
                .iter()
                .map(|p| p.realized_pnl_usdc)
                .sum();
            let total_upnl: f64 = held
                .iter()
                .map(|h| {
                    if h.unrealized_pnl_usdc.is_finite() {
                        h.unrealized_pnl_usdc
                    } else {
                        0.0
                    }
                })
                .sum();
            Some(seed_eq + closed_realized_pnl + total_upnl)
        }
    }

    fn snapshot_cap_state(&self) -> Result<(ConfigMap, Vec<OrderedAssetView>, Vec<HeldAssetView>)> {
        let _exec_guard = acquire_execution_lock();
        Ok((
            self.db.load_config()?,
            self.db.get_assets_ordered()?,
            self.db.get_assets_held()?,
        ))
    }

    /// Enforces `max_open_positions` (default `6`) and the hard gross-leverage ceiling
    /// (`base_equity_usdc * min(target_leverage_multiplier, max_gross_leverage_x)`) over **ALL**
    /// open positions and working orders (`R14-H1`).
    /// - `WORKING` cancellations on eligible orders (`is_deleverage_eligible_order`) execute immediately.
    /// - `OPEN` liquidations (`AUTO_DELEVERAGE_RISK_CAP`) require `auto_deleverage_confirm_ticks` (`2`)
    ///   consecutive ticks within 30s, triggering an unlocked equity refresh on Tick 1 and verifying
    ///   `< 20s` equity freshness on Tick 2 (`P0-Risk`) with symmetric `1.03` tolerance (`R14-M11`).
    pub fn enforce_leverage_and_position_caps(&self) -> Result<Vec<ManualCloseReport>> {
        let (cfg, all_ordered, mut all_held) = self.snapshot_cap_state()?;
        let max_open_positions = cfg.get_usize("max_open_positions", 6).max(1);
        let base_equity_usdc = self.resolve_effective_base_equity_usdc(&cfg);
        let max_gross_lev = cfg.get_f64("max_gross_leverage_x", 2.50).clamp(0.20, 5.00);
        let eff_lev_cap = cfg.target_leverage_multiplier().min(max_gross_lev);
        let max_gross_notional = base_equity_usdc * eff_lev_cap;
        let max_gross_with_tol = max_gross_notional * CAP_ENFORCEMENT_TOLERANCE_MULT;
        let cap_epoch_ms = if cfg.execution_mode().is_live_or_testnet() {
            cfg.get_i64("risk_capital_cap_updated_at_ms", 0)
        } else {
            0
        };
        let min_hold_ms = cfg.get_i64("min_position_hold_sec", 300).max(0) * 1000;
        let confirm_ticks = cfg
            .get_usize("auto_deleverage_confirm_ticks", 2)
            .clamp(1, 5);

        let mut closed_reports = Vec::new();
        let mut current_committed: f64 = all_held.iter().map(|h| h.committed_notional_usdc).sum();
        let mut current_reserved: f64 = all_ordered
            .iter()
            .map(|o| (o.limit_price * o.requested_quantity).max(0.0))
            .sum();
        let mut total_active = all_held.len() + all_ordered.len();

        // 1. Cancel eligible WORKING orders immediately when total portfolio count or gross exposure exceeds cap
        if total_active > max_open_positions
            || (current_committed + current_reserved) > max_gross_with_tol
        {
            for o in all_ordered
                .iter()
                .filter(|o| is_deleverage_eligible_order(o, cap_epoch_ms))
            {
                if total_active <= max_open_positions
                    && (current_committed + current_reserved) <= max_gross_with_tol
                {
                    break;
                }
                if self.in_flight_position_ops.lock().contains(&o.position_id) {
                    continue;
                }
                if let Ok(rep) = self.close_or_cancel_position(
                    o.position_id,
                    "AUTO_DELEVERAGE_WORKING_CANCEL",
                    Some(300),
                ) {
                    if rep.final_status != "OPEN" && rep.final_status != "WORKING" {
                        current_reserved = (current_reserved
                            - (o.limit_price * o.requested_quantity).max(0.0))
                        .max(0.0);
                        total_active = total_active.saturating_sub(1);
                        closed_reports.push(rep);
                    }
                }
            }
        }

        // Re-check breach after WORKING cancellations (counting ALL holdings + remaining orders)
        let (mut cfg, all_ordered_after, held_after) = if !closed_reports.is_empty() {
            self.snapshot_cap_state()?
        } else {
            (cfg, all_ordered, all_held)
        };
        all_held = held_after;
        current_committed = all_held.iter().map(|h| h.committed_notional_usdc).sum();
        current_reserved = all_ordered_after
            .iter()
            .map(|o| (o.limit_price * o.requested_quantity).max(0.0))
            .sum();
        let mut open_count = all_held.len();
        let live_active = is_live_exchange_active(&self.db, &cfg);
        let mut max_gross_notional = self.resolve_effective_base_equity_usdc(&cfg)
            * cfg.target_leverage_multiplier().min(max_gross_lev);
        let mut max_gross_with_tol = max_gross_notional * CAP_ENFORCEMENT_TOLERANCE_MULT;

        let over_open_cap = open_count > max_open_positions
            || (current_committed + current_reserved) > max_gross_with_tol;

        if !over_open_cap {
            self.deleverage_confirm_state.lock().reset();
            if !closed_reports.is_empty() {
                let _ = self.record_portfolio_snapshot(None, None);
            }
            return Ok(closed_reports);
        }

        // 2. P0-Risk 2-Tick Confirmation + Fresh Equity Verification before closing OPEN holdings
        let tick_now = now_ms();
        let consec = self
            .deleverage_confirm_state
            .lock()
            .record_breach_tick(tick_now);

        if consec < confirm_ticks {
            if live_active {
                let _ = self.reconciler.refresh_equity_snapshot(&cfg);
            }
            if !closed_reports.is_empty() {
                let _ = self.record_portfolio_snapshot(None, None);
            }
            return Ok(closed_reports);
        }

        if live_active && confirm_ticks > 1 {
            let synced_at = cfg.get_i64("live_exchange_equity_synced_at_ms", 0);
            if synced_at <= 0
                || tick_now.saturating_sub(synced_at) > AUTO_DELEVERAGE_EQUITY_FRESH_MS
            {
                let _ = self.reconciler.refresh_equity_snapshot(&cfg);
                let (fresh_cfg, fresh_ordered, fresh_held) = self.snapshot_cap_state()?;
                let fresh_synced_at = fresh_cfg.get_i64("live_exchange_equity_synced_at_ms", 0);
                if fresh_synced_at <= 0
                    || now_ms().saturating_sub(fresh_synced_at) > AUTO_DELEVERAGE_EQUITY_FRESH_MS
                {
                    tracing::warn!(
                        "Deferring AUTO_DELEVERAGE_RISK_CAP: live exchange equity could not be refreshed within 20s"
                    );
                    if !closed_reports.is_empty() {
                        let _ = self.record_portfolio_snapshot(None, None);
                    }
                    return Ok(closed_reports);
                }
                cfg = fresh_cfg;
                all_held = fresh_held;
                current_committed = all_held.iter().map(|h| h.committed_notional_usdc).sum();
                current_reserved = fresh_ordered
                    .iter()
                    .map(|o| (o.limit_price * o.requested_quantity).max(0.0))
                    .sum();
                open_count = all_held.len();
                max_gross_notional = self.resolve_effective_base_equity_usdc(&cfg)
                    * cfg.target_leverage_multiplier().min(max_gross_lev);
                max_gross_with_tol = max_gross_notional * CAP_ENFORCEMENT_TOLERANCE_MULT;
            }
        }

        if open_count <= max_open_positions
            && (current_committed + current_reserved) <= max_gross_with_tol
        {
            self.deleverage_confirm_state.lock().reset();
            if !closed_reports.is_empty() {
                let _ = self.record_portfolio_snapshot(None, None);
            }
            return Ok(closed_reports);
        }

        // 3. Close ONLY eligible holdings (`is_deleverage_eligible_holding`) with symmetric tolerance
        let check_ts = now_ms();
        let mut eligible_held: Vec<HeldAssetView> = all_held
            .into_iter()
            .filter(|h| is_deleverage_eligible_holding(h, cap_epoch_ms))
            .collect();

        let is_young = |h: &HeldAssetView| -> bool {
            min_hold_ms > 0
                && check_ts.saturating_sub(h.opened_at_ms.unwrap_or(h.created_at_ms)) < min_hold_ms
        };
        let has_older_candidate = eligible_held.iter().any(|h| !is_young(h));

        eligible_held.sort_by(|a, b| {
            let a_young = is_young(a);
            let b_young = is_young(b);
            a_young.cmp(&b_young).then_with(|| {
                let a_align = a.latest_target_weight
                    * OrderSide::parse(&a.side).map(|s| s.sign()).unwrap_or(0.0);
                let b_align = b.latest_target_weight
                    * OrderSide::parse(&b.side).map(|s| s.sign()).unwrap_or(0.0);
                a_align.total_cmp(&b_align).then_with(|| {
                    b.committed_notional_usdc
                        .total_cmp(&a.committed_notional_usdc)
                })
            })
        });

        for h in &eligible_held {
            if open_count <= max_open_positions
                && (current_committed + current_reserved) <= max_gross_with_tol
            {
                break;
            }
            if is_young(h) && has_older_candidate {
                continue;
            }
            if self.is_illiquid_close_backed_off(h.position_id, check_ts)
                || self.in_flight_position_ops.lock().contains(&h.position_id)
            {
                continue;
            }
            let deleverage_cd = if h.latest_target_weight
                * OrderSide::parse(&h.side).map(|s| s.sign()).unwrap_or(0.0)
                < 0.0
            {
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
                    current_committed = (current_committed - h.committed_notional_usdc).max(0.0);
                    open_count = open_count.saturating_sub(1);
                    closed_reports.push(rep);
                }
            }
        }

        if open_count <= max_open_positions
            && (current_committed + current_reserved) <= max_gross_with_tol
        {
            self.deleverage_confirm_state.lock().reset();
        }

        if !closed_reports.is_empty() {
            let _ = self.record_portfolio_snapshot(None, None);
        }

        Ok(closed_reports)
    }

    fn process_signal_exits_unlocked(
        &self,
        signal_out: &SignalCycleOutput,
        cfg: &ConfigMap,
        held: &[HeldAssetView],
        report: &mut RebalanceExecutionReport,
    ) -> Result<HashSet<i64>> {
        let rebalance_now = now_ms();
        let min_hold_ms = cfg.get_i64("min_position_hold_sec", 300).max(0) * 1000;
        let early_loser_enabled = cfg.get_bool("early_loser_invalidation_enabled", true);
        let profit_gate_signal_exits = cfg.get_bool("profit_gate_signal_exits", false);
        let signal_exit_min_net_profit_pct = cfg
            .get_f64("signal_exit_min_net_profit_pct", 0.0025)
            .clamp(0.0, 0.50);
        let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5).clamp(0.0, 100.0);
        let mut flipped_symbols: HashSet<i64> = HashSet::new();

        for sig in &signal_out.signals {
            if sig.rebalance_action != ACTION_CLOSE && sig.rebalance_action != ACTION_FLIP {
                continue;
            }
            let Some(h) = held.iter().find(|x| x.asset_id == sig.asset_id) else {
                continue;
            };
            let held_ms = rebalance_now.saturating_sub(h.opened_at_ms.unwrap_or(h.created_at_ms));
            let frac_upnl = held_position_fractional_pnl(h);
            let bypass_for_loser = early_loser_enabled
                && frac_upnl <= EARLY_LOSER_PNL_THRESHOLD_FRAC
                && held_ms >= EARLY_LOSER_MIN_HOLD_MS;
            if min_hold_ms > 0 && held_ms < min_hold_ms && !bypass_for_loser {
                continue;
            }
            if report.closed_position_ids.contains(&h.position_id)
                || self.is_illiquid_close_backed_off(h.position_id, rebalance_now)
                || self.in_flight_position_ops.lock().contains(&h.position_id)
            {
                continue;
            }
            // R14-M10: PROFIT_GATE only applies to CLOSE when explicitly enabled; FLIP is exempt.
            if profit_gate_signal_exits && sig.rebalance_action == ACTION_CLOSE {
                let (passes_profit_gate, net_exit_pnl_usdc, min_required_profit_usdc) =
                    signal_exit_passes_profit_gate(
                        h,
                        signal_exit_min_net_profit_pct,
                        taker_fee_bps,
                    );
                if !passes_profit_gate {
                    tracing::debug!(
                        position_id = h.position_id,
                        symbol = %h.symbol,
                        action = %sig.rebalance_action,
                        net_exit_pnl_usdc = net_exit_pnl_usdc,
                        min_required_profit_usdc = min_required_profit_usdc,
                        "Holding open position: signal CLOSE blocked by profit gate"
                    );
                    continue;
                }
            }
            let close_cd = if sig.rebalance_action == ACTION_FLIP {
                Some(0)
            } else {
                Some(300)
            };
            let close_res =
                self.close_or_cancel_position(h.position_id, &sig.rebalance_action, close_cd)?;
            if close_res.final_status != "OPEN" {
                report.positions_closed += 1;
                report.closed_position_ids.push(close_res.position_id);
                if sig.rebalance_action == ACTION_FLIP && close_res.final_status == "CLOSED" {
                    flipped_symbols.insert(sig.asset_id);
                }
            }
        }

        Ok(flipped_symbols)
    }

    fn record_cooldown_blocked(report: &mut RebalanceExecutionReport, symbol: &str) {
        report.cooldown_blocked_count += 1;
        if !report.cooldown_blocked_symbols.iter().any(|s| s == symbol) {
            report.cooldown_blocked_symbols.push(symbol.to_string());
        }
    }

    fn check_live_entry_gates_unlocked(
        &self,
        asset: &ExchangeAsset,
        target_notional_usdc: f64,
        thresholds: EntryGateThresholds,
        cfg: &ConfigMap,
        l2_depth_checks: &mut usize,
    ) -> std::result::Result<(), bool> {
        if let Err(reason) =
            passes_entry_oracle_deviation_gate(asset, thresholds.max_oracle_deviation_pct)
        {
            tracing::debug!(
                symbol = %asset.symbol,
                reason = %reason,
                "Skipping candidate due to pre-L2 oracle deviation gate"
            );
            return Err(false);
        }
        let book_res = if let Some(ws_book) = self.ws_cache.get_fresh_l2_book_depth(&asset.coin) {
            Ok(Some(ws_book))
        } else {
            if *l2_depth_checks >= MAX_L2_DEPTH_CHECKS_PER_REBALANCE {
                tracing::debug!(
                    checked = *l2_depth_checks,
                    "Reached per-cycle L2 depth check cap; deferring remaining candidates"
                );
                return Err(true);
            }
            *l2_depth_checks += 1;
            self.http.fetch_l2_book_depth(&asset.coin, cfg)
        };
        match book_res {
            Ok(Some(book)) => {
                if let Err(reason) = passes_entry_l2_liquidity_gate_with_thresholds(
                    &book,
                    target_notional_usdc,
                    thresholds.max_spread_bps,
                    thresholds.min_l2_depth_mult,
                    thresholds.min_l2_depth_usdc,
                ) {
                    tracing::debug!(
                        symbol = %asset.symbol,
                        reason = %reason,
                        "Skipping candidate due to thin/wide L2 book"
                    );
                    return Err(false);
                }
                let l2_mid = (book.best_bid + book.best_ask) * 0.5;
                let max_dev = thresholds.max_oracle_deviation_pct.clamp(0.001, 0.25);
                if !asset.oracle_price.is_finite()
                    || asset.oracle_price <= 0.0
                    || !l2_mid.is_finite()
                    || ((l2_mid - asset.oracle_price).abs() / asset.oracle_price) > max_dev
                {
                    tracing::debug!(
                        symbol = %asset.symbol,
                        l2_mid = l2_mid,
                        oracle = asset.oracle_price,
                        "Skipping candidate due to L2 mid vs oracle deviation"
                    );
                    return Err(false);
                }
                Ok(())
            }
            Ok(None) => {
                tracing::debug!(
                    symbol = %asset.symbol,
                    reason = "EMPTY_L2_BOOK",
                    "Skipping candidate due to empty L2 book"
                );
                Err(false)
            }
            Err(err) => {
                tracing::debug!(
                    symbol = %asset.symbol,
                    error = %err,
                    "Skipping candidate due to transient L2 book fetch error"
                );
                Err(false)
            }
        }
    }

    /// Executes portfolio rebalance actions (`CLOSE`, `FLIP`, `OPEN_LONG`, `OPEN_SHORT`) strictly
    /// unlocked across HTTP IO (`R14-H2`) while enforcing full-exposure risk caps (`R14-H1`),
    /// ruin halt (`R14-M12`), full-24h uncapped drawdown (`P1-Acct`), early loser bypass (`P1-Strat`),
    /// fail-closed cooldown/active-position checks (`R14-L12`), and oracle+L2 entry gates (`P0-Liq`).
    pub fn execute_signal_rebalance(
        &self,
        signal_out: &SignalCycleOutput,
        base_equity_usdc: f64,
    ) -> Result<RebalanceExecutionReport> {
        let mut report = RebalanceExecutionReport::default();

        // 0. Enforce leverage & max_open_positions caps first (unlocked; 2-tick confirm on OPEN)
        if let Ok(deleveraged) = self.enforce_leverage_and_position_caps() {
            for r in deleveraged {
                if r.final_status != "OPEN" {
                    report.positions_closed += 1;
                    report.closed_position_ids.push(r.position_id);
                }
            }
        }

        // 1. Handle CLOSE or FLIP on currently OPEN holdings (always allowed even under Ruin Halt / DD)
        let (cfg_exit, held_for_exits) = {
            let _exec_guard = acquire_execution_lock();
            (self.db.load_config()?, self.db.get_assets_held()?)
        };
        let flipped_symbols = self.process_signal_exits_unlocked(
            signal_out,
            &cfg_exit,
            &held_for_exits,
            &mut report,
        )?;

        // 2. Short DB snapshot of ALL holdings/orders & gate parameters for new entries
        let (cfg, all_held, all_ordered, assets, last_entry_ms_opt, peak_latest_uncapped) = {
            let _exec_guard = acquire_execution_lock();
            let c = self.db.load_config()?;
            let h = self.db.get_assets_held()?;
            let o = self.db.get_assets_ordered()?;
            let a = self.db.list_exchange_assets()?;
            let last_entry = self.db.last_position_entry_timestamp_ms().ok().flatten();
            let dd_window_start = now_ms() - 86_400_000;
            let dd_stats = self
                .db
                .get_peak_and_latest_uncapped_equity_since(dd_window_start)?;
            (c, h, o, a, last_entry, dd_stats)
        };

        let eff_equity = if cfg.execution_mode().is_live_or_testnet() {
            self.resolve_effective_base_equity_usdc(&cfg)
        } else {
            base_equity_usdc.max(100.0)
        };
        let rebalance_now = now_ms();

        if all_held
            .iter()
            .any(|h| self.is_illiquid_close_backed_off(h.position_id, rebalance_now))
        {
            return Ok(report);
        }

        // Ruin Halt (`R14-M12`): block all new entries & emit CRITICAL alert if uncapped equity < min_equity_halt_usdc
        let min_equity_halt_usdc = cfg
            .get_f64("min_equity_halt_usdc", 50.0)
            .clamp(0.0, 1_000_000.0);
        if min_equity_halt_usdc > 0.0 {
            if let Some(uncapped_eq) = self.compute_ruin_halt_uncapped_equity(&cfg, &all_held) {
                if !uncapped_eq.is_finite() || uncapped_eq < min_equity_halt_usdc {
                    tracing::error!(
                        uncapped_equity_usdc = uncapped_eq,
                        min_equity_halt_usdc = min_equity_halt_usdc,
                        "RUIN_HALT triggered: uncapped equity fell below min_equity_halt_usdc; halting all new entries"
                    );
                    let html = format!(
                        "<b>RUIN_HALT CRITICAL</b>\n\
                        Live uncapped equity <code>${uncapped_eq:.2}</code> fell below \
                        floor <code>${min_equity_halt_usdc:.2}</code>. All new entries blocked; CLOSE/REDUCE only."
                    );
                    let details = json!({
                        "uncapped_equity_usdc": uncapped_eq,
                        "min_equity_halt_usdc": min_equity_halt_usdc,
                    });
                    self.enqueue_non_blocking_alert("RUIN_HALT", "CRITICAL", &html, Some(&details));
                    return Ok(report);
                }
            }
        }

        let max_open_positions = cfg.get_usize("max_open_positions", 6).max(1);
        let min_entry_interval_sec = cfg.get_i64("min_entry_interval_sec", 150).max(0);
        let total_active = all_held.len() + all_ordered.len();
        if total_active >= max_open_positions {
            return Ok(report);
        }

        // Full 24h uncapped daily drawdown breaker (`R14-H1`, `P1-Acct`)
        let max_dd_pct = cfg
            .get_f64("max_daily_drawdown_pct", 0.15)
            .clamp(0.01, 0.90);
        if let Some((peak_24h, latest_equity)) = peak_latest_uncapped {
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
                        "Daily uncapped drawdown breaker tripped; halting new entries"
                    );
                    return Ok(report);
                }
            }
        }

        let entry_interval_blocked = min_entry_interval_sec > 0
            && last_entry_ms_opt.is_some_and(|last_entry_ms| {
                rebalance_now.saturating_sub(last_entry_ms) < min_entry_interval_sec * 1000
            });
        if entry_interval_blocked && flipped_symbols.is_empty() {
            return Ok(report);
        }

        let risk = RiskEngine::from_config_map(&cfg);
        let ledger = risk.evaluate_ledger_from_views(eff_equity, &all_held, &all_ordered);
        let target_mult = cfg.target_leverage_multiplier();
        let eff_lev_cap = target_mult.min(risk.limits.max_gross_leverage_x);
        let max_allowed_gross_notional = eff_equity * eff_lev_cap;
        let current_reserved_notional: f64 = all_ordered
            .iter()
            .map(|o| (o.limit_price * o.requested_quantity).max(0.0))
            .sum();
        if ledger.committed_notional_usdc + current_reserved_notional >= max_allowed_gross_notional
        {
            return Ok(report);
        }
        let max_per_pos_weight = ((eff_lev_cap / (max_open_positions as f64)) * 1.25)
            .min(risk.limits.max_single_asset_weight);

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
        let max_entry_oracle_deviation_pct = cfg
            .get_f64("max_entry_oracle_deviation_pct", 0.015)
            .clamp(0.001, 0.25);
        let min_l2_depth_mult = cfg
            .get_f64("min_entry_l2_depth_mult", DEFAULT_MIN_L2_DEPTH_MULT)
            .max(0.0);
        let min_l2_depth_usdc = cfg
            .get_f64("min_entry_l2_depth_usdc", DEFAULT_MIN_L2_DEPTH_USDC)
            .max(0.0);
        let mut l2_depth_checks: usize = 0;

        // 3. Place at most 1 new WORKING order per cycle under per-asset in-flight guard (`R14-H2`, `R14-L12`)
        for sig in &candidate_signals {
            if report.working_orders_placed >= 1
                || (total_active + report.working_orders_placed) >= max_open_positions
            {
                break;
            }
            let Some(asset) = asset_map.get(&sig.asset_id) else {
                continue;
            };
            if sig.rebalance_action == ACTION_COOLDOWN_BLOCKED {
                Self::record_cooldown_blocked(&mut report, &asset.symbol);
                continue;
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

            // R14-L12: Fail closed on DB errors when checking cooldown or active position
            match self.db.is_asset_in_cooldown(asset.id) {
                Ok(true) => {
                    Self::record_cooldown_blocked(&mut report, &asset.symbol);
                    continue;
                }
                Ok(false) => {}
                Err(err) => {
                    tracing::error!(
                        asset_id = asset.id,
                        symbol = %asset.symbol,
                        error = %err,
                        "is_asset_in_cooldown DB check failed; failing closed"
                    );
                    continue;
                }
            }
            match self.db.get_active_position_for_asset(asset.id) {
                Ok(None) => {}
                Ok(Some(_)) => continue,
                Err(err) => {
                    tracing::error!(
                        asset_id = asset.id,
                        symbol = %asset.symbol,
                        error = %err,
                        "get_active_position_for_asset DB check failed; failing closed"
                    );
                    continue;
                }
            }

            let Some(_entry_guard) =
                InFlightIdGuard::try_acquire(&self.in_flight_entry_assets, asset.id)
            else {
                continue;
            };

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

            if ledger.committed_notional_usdc
                + current_reserved_notional
                + plan.target_notional_usdc
                > max_allowed_gross_notional + 1e-2
            {
                continue;
            }

            if is_live_exchange {
                let thresholds = EntryGateThresholds {
                    max_spread_bps: max_entry_spread_bps,
                    max_oracle_deviation_pct: max_entry_oracle_deviation_pct,
                    min_l2_depth_mult,
                    min_l2_depth_usdc,
                };
                if let Err(should_break) = self.check_live_entry_gates_unlocked(
                    asset,
                    plan.target_notional_usdc,
                    thresholds,
                    &cfg,
                    &mut l2_depth_checks,
                ) {
                    if should_break {
                        break;
                    }
                    continue;
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

    /// Computes and persists a unified [`PortfolioSnapshot`] under a short DB lock (`<2ms`),
    /// capturing both effective (`total_equity_usdc`) and raw (`uncapped_equity_usdc`) equity (`P1-Acct`).
    pub fn record_portfolio_snapshot(
        &self,
        base_equity_override: Option<f64>,
        signal_out: Option<&SignalCycleOutput>,
    ) -> Result<PortfolioSnapshot> {
        let _exec_guard = acquire_execution_lock();
        let cfg = self.db.load_config()?;
        let risk = RiskEngine::from_config_map(&cfg);
        let held = self.db.get_assets_held()?;
        let ordered = self.db.get_assets_ordered()?;
        let assets = self.db.list_exchange_assets()?;

        let (ledger, uncapped_equity_usdc) = if cfg.execution_mode().is_live_or_testnet() {
            let eff_live_eq =
                Self::resolve_live_base_equity_usdc(&cfg, self.db.is_ephemeral_test_db())
                    .or_else(|| base_equity_override.filter(|&v| v > 0.0))
                    .unwrap_or_else(|| cfg.get_f64("initial_paper_equity_usdc", 10_000.0));
            let mut l = risk.evaluate_ledger_from_views(eff_live_eq, &held, &ordered);
            let tot_eq = round_dp(eff_live_eq.max(1.0), 4);
            let live_raw = cfg.get_f64("live_exchange_equity_usdc", 0.0);
            let uncapped = if live_raw > 0.0 {
                round_dp(live_raw.max(0.0), 4)
            } else {
                tot_eq
            };
            let eff_lev = cfg.target_leverage_multiplier().clamp(1.0, 20.0);
            let committed_margin_used = l.committed_notional_usdc / eff_lev;
            let budget_idle =
                (tot_eq - committed_margin_used - l.reserved_order_cash_usdc).max(0.0);
            let synced_at = cfg.get_i64("live_exchange_equity_synced_at_ms", 0);
            let avail_cash = if synced_at > 0 {
                let withdrawable = cfg
                    .get_f64("live_exchange_withdrawable_usdc", budget_idle)
                    .max(0.0);
                budget_idle.min(withdrawable)
            } else {
                budget_idle
            };
            l.total_equity_usdc = tot_eq;
            l.available_cash_usdc = round_dp(avail_cash, 4);
            l.gross_leverage_x = round_dp(l.committed_notional_usdc / tot_eq, 4);
            l.net_leverage_x = round_dp(l.net_notional_usdc / tot_eq, 4);
            (l, uncapped)
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
            let l = risk.evaluate_ledger_from_views(base_cash_equity, &held, &ordered);
            let uncapped = l.total_equity_usdc;
            (l, uncapped)
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
                let has_prev_sleeves =
                    (prev.s1_gross_weight + prev.s2_gross_weight + prev.s3_gross_weight) > 0.0;
                let (s1_prev_or_held, s2_prev_or_held, s3_prev_or_held) = if has_prev_sleeves {
                    (
                        prev.s1_gross_weight,
                        prev.s2_gross_weight,
                        prev.s3_gross_weight,
                    )
                } else {
                    let s1_sum: f64 = held.iter().map(|h| h.s1_weight_contrib.abs()).sum();
                    let s2_sum: f64 = held.iter().map(|h| h.s2_weight_contrib.abs()).sum();
                    let s3_sum: f64 = held.iter().map(|h| h.s3_weight_contrib.abs()).sum();
                    (
                        round_dp(s1_sum, 4),
                        round_dp(s2_sum, 4),
                        round_dp(s3_sum, 4),
                    )
                };
                (
                    prev.market_beta_exposure,
                    prev.market_breadth_bull,
                    prev.market_regime_label,
                    s1_prev_or_held,
                    s2_prev_or_held,
                    s3_prev_or_held,
                )
            }
        };

        self.db.insert_portfolio_snapshot(&SnapshotInsertInput {
            total_equity_usdc: ledger.total_equity_usdc,
            uncapped_equity_usdc,
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

    /// Runs a single unified execution cycle strictly unlocked (`R14-H2`):
    /// Market Scan + Priority Tick + Multi-Sleeve Signal Evaluation + Optional Order Placement + Snapshot.
    pub fn run_single_cycle(&self, execute_orders: bool) -> Result<ExecutionCycleReport> {
        let ts = now_ms();
        let cfg = self.db.load_config()?;
        let market_scan = self.watcher.run_3s_exchange_scan()?;
        let priority_tick = self.priority_monitor.run_1s_priority_tick()?;

        let base_equity = self.resolve_effective_base_equity_usdc(&cfg);
        let signal_out = self.evaluate_and_persist_signals(base_equity)?;
        let rebalance = if execute_orders {
            self.execute_signal_rebalance(&signal_out, base_equity)?
        } else {
            RebalanceExecutionReport::default()
        };
        let snapshot = self.record_portfolio_snapshot(None, Some(&signal_out))?;
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

    fn cancel_working_position_unlocked(
        &self,
        pos: &PositionRecord,
        asset: &ExchangeAsset,
        cfg: &ConfigMap,
    ) -> bool {
        let pk = cfg.get_str("private_key", "");
        let wallet = cfg.get_str("wallet_address", "");
        if pk.trim().is_empty() {
            tracing::error!(
                position_id = pos.id,
                symbol = %asset.symbol,
                "private_key is empty/missing in live mode; refusing to mark WORKING order CANCELLED without exchange cancel"
            );
            return true;
        }
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
            tracing::warn!(
                position_id = pos.id,
                symbol = %asset.symbol,
                "Working order lacks numeric exchange_order_id and wallet_address is empty in live mode; keeping WORKING"
            );
            return true;
        } else {
            match self.http.fetch_open_orders_with_metadata(&wallet, cfg) {
                Ok(snap) => {
                    if !snap.metadata_complete {
                        tracing::warn!(
                            position_id = pos.id,
                            symbol = %asset.symbol,
                            "openOrders metadata incomplete for unlinked WORKING order cancel; keeping WORKING (R14-M7)"
                        );
                        return true;
                    }
                    cancels = collect_coin_non_trigger_cancels(&snap, asset, None);
                }
                Err(e) => {
                    tracing::warn!(
                        position_id = pos.id,
                        symbol = %asset.symbol,
                        error = %e,
                        "Failed to fetch openOrders for unlinked WORKING order; keeping WORKING"
                    );
                    return true;
                }
            }
        }

        if !cancels.is_empty() {
            match self
                .http
                .cancel_orders_batch(cancels, &pk, Some(&self.db), cfg)
            {
                Ok(resp) => {
                    if let Err(err_detail) = cancel_batch_response_ok(&resp) {
                        tracing::warn!(
                            position_id = pos.id,
                            symbol = %asset.symbol,
                            error = %err_detail,
                            response = %resp,
                            "Hyperliquid WORKING order cancel not confirmed; keeping WORKING"
                        );
                        return true;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        position_id = pos.id,
                        symbol = %asset.symbol,
                        error = %e,
                        "Hyperliquid cancel_orders_batch failed; keeping WORKING"
                    );
                    return true;
                }
            }
        }
        false
    }

    fn execute_open_position_close_unlocked(
        &self,
        pos: &PositionRecord,
        asset: &ExchangeAsset,
        side: OrderSide,
        cfg: &ConfigMap,
    ) -> OpenPositionCloseOutcome {
        let mut exit_px = if asset.mark_price > 0.0 {
            asset.mark_price
        } else {
            pos.current_price.max(pos.entry_price)
        };
        let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);
        let closed_qty = pos.filled_quantity;
        let fee_usdc = round_dp(exit_px * closed_qty * (taker_fee_bps / 10_000.0), 4);
        let pk = cfg.get_str("private_key", "");
        let wallet = cfg.get_str("wallet_address", "");

        if pk.trim().is_empty() || wallet.trim().is_empty() {
            tracing::warn!(
                position_id = pos.id,
                symbol = %asset.symbol,
                "private_key or wallet_address is empty/missing in live mode; refusing to close OPEN position blindly"
            );
            return OpenPositionCloseOutcome {
                exit_px,
                closed_qty,
                fee_usdc,
                close_eoid_opt: None,
                exchange_close_unfilled: true,
            };
        }

        let ch_state = if let Some(ws_ch) = self.ws_cache.get_fresh_clearinghouse_state() {
            ws_ch
        } else {
            match self.http.fetch_clearinghouse_state(&wallet, cfg) {
                Ok(st) => st,
                Err(e) => {
                    tracing::warn!(
                        position_id = pos.id,
                        symbol = %asset.symbol,
                        error = %e,
                        "Hyperliquid clearinghouseState failed during close; refusing to mark DB position CLOSED blindly"
                    );
                    return OpenPositionCloseOutcome {
                        exit_px,
                        closed_qty,
                        fee_usdc,
                        close_eoid_opt: None,
                        exchange_close_unfilled: true,
                    };
                }
            }
        };

        let mut live_szi = 0.0;
        let mut live_pos_val = 0.0;
        if let Some(arr) = ch_state.get("assetPositions").and_then(Value::as_array) {
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

        if live_szi.abs() <= 1e-9 {
            self.cancel_coin_exchange_orders(asset, &wallet, &pk, cfg, true);
            self.illiquid_close_backoff_ms.lock().remove(&pos.id);
            return OpenPositionCloseOutcome {
                exit_px,
                closed_qty,
                fee_usdc: 0.0,
                close_eoid_opt: None,
                exchange_close_unfilled: false,
            };
        }

        let live_qty = live_szi.abs();
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

        let l2_bbo = self
            .ws_cache
            .get_fresh_l2_bbo(&asset.coin)
            .or_else(|| self.http.fetch_l2_book(&asset.coin, cfg).ok().flatten());
        let open_orders_snap: Option<OpenOrdersSnapshot> = self
            .ws_cache
            .get_fresh_open_orders()
            .filter(|s| s.metadata_complete)
            .or_else(|| self.http.fetch_open_orders_with_metadata(&wallet, cfg).ok());

        let cloid_prefix = format!("v2-close-{}", pos.id);
        let ladder_req = ReduceOnlyCloseLadderRequest {
            asset,
            position_id: Some(pos.id),
            live_szi,
            reference_mark_px: mark_px,
            l2_bbo,
            taker_fee_bps,
            cloid_prefix: &cloid_prefix,
            max_slippage_pct: None,
            allow_passive_fallback: true,
            existing_open_orders: open_orders_snap.as_ref(),
        };

        match submit_reduce_only_close_ladder(&self.http, Some(&self.db), &pk, cfg, &ladder_req) {
            Ok(res) if res.fully_closed => {
                if let Some(vwap) = res.vwap_fill_price {
                    exit_px = round_dp(vwap, 6);
                }
                self.cancel_coin_exchange_orders(asset, &wallet, &pk, cfg, true);
                self.illiquid_close_backoff_ms.lock().remove(&pos.id);
                OpenPositionCloseOutcome {
                    exit_px,
                    closed_qty: live_qty,
                    fee_usdc: round_dp(res.total_fee_usdc, 4),
                    close_eoid_opt: res.last_fill_eoid,
                    exchange_close_unfilled: false,
                }
            }
            Ok(res) => {
                self.illiquid_close_backoff_ms
                    .lock()
                    .insert(pos.id, now_ms());
                let tpsl_snapshot = if !res.mutated_open_orders {
                    open_orders_snap.as_ref()
                } else {
                    None
                };
                let _ = self.chaser.ensure_exchange_tpsl_orders_with_snapshot(
                    pos.id,
                    asset,
                    side,
                    res.remaining_qty,
                    exit_px,
                    pos.protective_stop_px,
                    pos.take_profit_px,
                    pos.protective_stop_oid.as_deref(),
                    pos.take_profit_oid.as_deref(),
                    false,
                    false,
                    tpsl_snapshot,
                    cfg,
                );
                tracing::warn!(
                    symbol = %asset.symbol,
                    live_qty = live_qty,
                    rem_qty = res.remaining_qty,
                    kept_passive_close = res.kept_existing_passive,
                    "Hyperliquid reduceOnly close did not completely fill due to illiquid book"
                );
                OpenPositionCloseOutcome {
                    exit_px,
                    closed_qty: live_qty,
                    fee_usdc: round_dp(res.total_fee_usdc, 4),
                    close_eoid_opt: res.last_fill_eoid,
                    exchange_close_unfilled: true,
                }
            }
            Err(err) => {
                tracing::warn!(
                    position_id = pos.id,
                    symbol = %asset.symbol,
                    error = %err,
                    "submit_reduce_only_close_ladder failed; keeping OPEN"
                );
                OpenPositionCloseOutcome {
                    exit_px,
                    closed_qty: live_qty,
                    fee_usdc,
                    close_eoid_opt: None,
                    exchange_close_unfilled: true,
                }
            }
        }
    }

    /// Closes an `OPEN` position or cancels a `WORKING` order by `position_id` strictly unlocked
    /// over exchange HTTP IO (`R14-H2`) using `submit_reduce_only_close_ladder` (`R14-M2`),
    /// bounded non-blocking alert dispatch (`R14-L5`), and per-position in-flight exclusion.
    pub fn close_or_cancel_position(
        &self,
        position_id: i64,
        reason: &str,
        cooldown_seconds: Option<i64>,
    ) -> Result<ManualCloseReport> {
        let _in_flight = InFlightIdGuard::try_acquire(&self.in_flight_position_ops, position_id)
            .ok_or_else(|| {
                AegisError::Execution(format!(
                    "Position #{position_id} close/cancel already in flight"
                ))
            })?;

        // Phase 1: Short DB lock to snapshot position, asset, and config
        let (pos, asset, cfg) = {
            let _exec_guard = acquire_execution_lock();
            let pos = self.db.get_position_by_id(position_id)?.ok_or_else(|| {
                AegisError::Validation(format!("Position #{position_id} not found"))
            })?;
            let asset = self.db.get_asset_by_id(pos.asset_id)?.ok_or_else(|| {
                AegisError::Database(format!("Asset #{} not found", pos.asset_id))
            })?;
            let cfg = self.db.load_config()?;
            (pos, asset, cfg)
        };

        let status = PositionStatus::parse(&pos.status);
        let cd_sec = cooldown_seconds.unwrap_or(3600).max(0);
        let is_live_exchange = is_live_exchange_active(&self.db, &cfg);

        // Phase 2: Unlocked exchange HTTP IO
        let (
            exit_price_opt,
            mut realized_pnl,
            released_reserved,
            exchange_close_unfilled,
            final_status,
            close_eoid_opt,
            closed_qty,
            fee_usdc,
            exit_px_to_commit,
            side_opt,
        ) = match status {
            PositionStatus::Working => {
                let unfilled = if is_live_exchange {
                    self.cancel_working_position_unlocked(&pos, &asset, &cfg)
                } else {
                    false
                };
                if unfilled {
                    (
                        None,
                        0.0,
                        0.0,
                        true,
                        "WORKING".to_string(),
                        None,
                        0.0,
                        0.0,
                        0.0,
                        None,
                    )
                } else {
                    (
                        None,
                        0.0,
                        pos.reserved_margin_usdc,
                        false,
                        "CANCELLED".to_string(),
                        None,
                        0.0,
                        0.0,
                        0.0,
                        None,
                    )
                }
            }
            PositionStatus::Open => {
                let side = OrderSide::parse(&pos.side)?;
                let outcome = if is_live_exchange {
                    self.execute_open_position_close_unlocked(&pos, &asset, side, &cfg)
                } else {
                    let px = if asset.mark_price > 0.0 {
                        asset.mark_price
                    } else {
                        pos.current_price.max(pos.entry_price)
                    };
                    let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);
                    let qty = pos.filled_quantity;
                    let fee = round_dp(px * qty * (taker_fee_bps / 10_000.0), 4);
                    OpenPositionCloseOutcome {
                        exit_px: px,
                        closed_qty: qty,
                        fee_usdc: fee,
                        close_eoid_opt: None,
                        exchange_close_unfilled: false,
                    }
                };
                if outcome.exchange_close_unfilled {
                    (
                        None,
                        0.0,
                        0.0,
                        true,
                        "OPEN".to_string(),
                        outcome.close_eoid_opt,
                        outcome.closed_qty,
                        outcome.fee_usdc,
                        outcome.exit_px,
                        Some(side),
                    )
                } else {
                    (
                        Some(outcome.exit_px),
                        0.0,
                        0.0,
                        false,
                        "CLOSED".to_string(),
                        outcome.close_eoid_opt,
                        outcome.closed_qty,
                        outcome.fee_usdc,
                        outcome.exit_px,
                        Some(side),
                    )
                }
            }
            PositionStatus::Closed | PositionStatus::Cancelled => (
                None,
                0.0,
                0.0,
                false,
                pos.status.clone(),
                None,
                0.0,
                0.0,
                0.0,
                None,
            ),
        };

        // Phase 3: Short DB commit under acquire_execution_lock()
        let mut pending_close_alert: Option<PositionClosedAlert> = None;
        {
            let _exec_guard = acquire_execution_lock();
            match status {
                PositionStatus::Working if !exchange_close_unfilled => {
                    self.db.cancel_working_order(position_id, reason)?;
                }
                PositionStatus::Open if !exchange_close_unfilled => {
                    if let Some(ref eoid) = close_eoid_opt {
                        let oid_num = eoid.strip_prefix("hl-").unwrap_or(eoid.as_str());
                        let coid =
                            format!("hl-closed-{}-{oid_num}", asset.coin.to_ascii_lowercase());
                        let _ = self
                            .db
                            .set_position_closed_exchange_ids(position_id, &coid, eoid);
                    }
                    realized_pnl =
                        self.db
                            .close_position(position_id, exit_px_to_commit, fee_usdc, reason)?;
                    if let Some(side) = side_opt {
                        pending_close_alert = Some(PositionClosedAlert {
                            symbol: asset.symbol.clone(),
                            side,
                            exit_reason: reason.to_string(),
                            entry_price: pos.entry_price,
                            exit_price: exit_px_to_commit,
                            filled_qty: closed_qty,
                            realized_pnl_usdc: realized_pnl,
                            available_cash_usdc: 0.0,
                            cooldown_type: (cd_sec > 0).then(|| "MANUAL".to_string()),
                            cooldown_seconds: cd_sec,
                            timestamp_ms: now_ms(),
                        });
                    }
                }
                _ => {}
            }

            if cd_sec > 0 && !exchange_close_unfilled {
                self.db.insert_asset_cooldown(
                    asset.id,
                    "MANUAL",
                    &format!("Manual close/cancel cooldown: {reason}"),
                    cd_sec,
                )?;
            }

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
        }

        let snap_opt = self.record_portfolio_snapshot(None, None).ok();
        if let Some(mut alert) = pending_close_alert {
            if let Some(ref snap) = snap_opt {
                alert.available_cash_usdc = snap.available_cash_usdc;
            }
            let html = format_position_closed_html(&alert);
            let payload_val = serde_json::to_value(&alert).ok();
            self.enqueue_non_blocking_alert("POSITION_CLOSED", "INFO", &html, payload_val.as_ref());
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

    /// Spawns independent unlocked background loops with elapsed-time cadence compensation (`R14-L4`):
    /// 1. 1-Second Priority Monitor & Maker Limit Chaser loop
    /// 2. 3-Second Full-Exchange Scanner, OHLCV Candle Refresh & Multi-Sleeve Rebalance loop
    /// 3. 15-Second Lightweight Hyperliquid 1:1 Synchronization loop (`sync_db_with_hyperliquid`)
    /// 4. 60-Second Unlocked Auxiliary Sync loop (`AuxiliarySyncWorker`: portfolio, vaultDetails, userFunding, userFees)
    ///
    /// Returns a shutdown sender (`send(true)` stops all runtime threads).
    pub fn spawn_background_runtime(&self) -> tokio::sync::watch::Sender<bool> {
        let (tx, rx) = tokio::sync::watch::channel(false);

        let startup_cfg = self.db.load_config_or_default();
        if should_enable_ws_manager(&self.db, &startup_cfg) {
            let init_subs = build_ws_subscription_set_from_db(&self.db, &startup_cfg);
            let _ = self.ws_sub_tx.send(init_subs);
            if let Err(err) = HyperliquidWsManager::spawn_background(
                self.db.clone(),
                Arc::clone(&self.ws_cache),
                self.ws_sub_tx.subscribe(),
                &startup_cfg,
                WsTimingConfig::default(),
                rx.clone(),
            ) {
                tracing::warn!(
                    error = %err,
                    "Failed starting HyperliquidWsManager; continuing with REST fallback"
                );
            }
        }

        let sleep_interruptible = |rx: &tokio::sync::watch::Receiver<bool>, total_ms: u64| {
            let mut slept = 0u64;
            while slept < total_ms && !*rx.borrow() {
                let chunk = (total_ms - slept).min(250);
                std::thread::sleep(Duration::from_millis(chunk));
                slept += chunk;
            }
        };

        // 1. Fast 1s Priority Monitor & ALO Chaser loop
        let prio_engine = self.clone();
        let prio_rx = rx.clone();
        std::thread::spawn(move || {
            while !*prio_rx.borrow() {
                let tick_start = Instant::now();
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
                    prio_engine.priority_monitor.record_tick_failure(&e);
                }
                let elapsed_ms = tick_start.elapsed().as_millis() as u64;
                let rem_ms = prio_ms.saturating_sub(elapsed_ms).max(50);
                sleep_interruptible(&prio_rx, rem_ms);
            }
        });

        // 2. 3s Market Scanner, Candle Refresh, Signal Evaluation & Unlocked Rebalance loop
        let scan_engine = self.clone();
        let scan_rx = rx.clone();
        std::thread::spawn(move || {
            while !*scan_rx.borrow() {
                let tick_start = Instant::now();
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
                            let reb_res = if execute_orders {
                                scan_engine.execute_signal_rebalance(&signal_out, eff_eq)
                            } else {
                                Ok(RebalanceExecutionReport::default())
                            };
                            if let Err(e) = reb_res {
                                tracing::warn!(error = %e, "3s signal rebalance error");
                            }
                            if let Err(e) =
                                scan_engine.record_portfolio_snapshot(None, Some(&signal_out))
                            {
                                tracing::warn!(error = %e, "3s portfolio snapshot error");
                            }
                            if scan_engine.alert_wakeup_tx.is_some() {
                                scan_engine.wake_alert_worker(8);
                            } else {
                                let _ = scan_engine.alerts.poll_pending_queue(8);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "3s signal evaluation error");
                        }
                    }
                }
                let elapsed_ms = tick_start.elapsed().as_millis() as u64;
                let rem_ms = scan_ms.saturating_sub(elapsed_ms).max(100);
                sleep_interruptible(&scan_rx, rem_ms);
            }
        });

        // 3. 15s Lightweight Hyperliquid 1:1 Synchronization loop
        let rec_engine = self.clone();
        let rec_rx = rx.clone();
        std::thread::spawn(move || {
            while !*rec_rx.borrow() {
                let tick_start = Instant::now();
                let cfg = match rec_engine.db.load_config() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(error = %e, "15s reconciler failed to load config; skipping sync");
                        sleep_interruptible(&rec_rx, 15_000);
                        continue;
                    }
                };
                let scan_ms = cfg.get_i64("scanner_interval_ms", 3000).max(1000) as u64;
                let reconcile_ms = (scan_ms * 5).max(5000);
                if cfg.execution_mode().is_live_or_testnet() {
                    if let Err(e) = rec_engine.reconciler.sync_db_with_hyperliquid() {
                        tracing::warn!(error = %e, "15s Hyperliquid 1:1 synchronization error");
                    } else {
                        let _ = rec_engine.record_portfolio_snapshot(None, None);
                    }
                }
                let elapsed_ms = tick_start.elapsed().as_millis() as u64;
                let rem_ms = reconcile_ms.saturating_sub(elapsed_ms).max(250);
                let mut slept = 0u64;
                while slept < rem_ms && !*rec_rx.borrow() {
                    if rec_engine.ws_cache.needs_rest_gap_reconcile() && slept >= 1000 {
                        break;
                    }
                    let chunk = (rem_ms - slept).min(250);
                    std::thread::sleep(Duration::from_millis(chunk));
                    slept += chunk;
                }
            }
        });

        // 4. 60s Unlocked Auxiliary Sync loop (portfolio, vaultDetails, userFunding, userFees) (`R14-H2`, Sec. 11)
        let aux_engine = self.clone();
        let aux_rx = rx;
        std::thread::spawn(move || {
            while !*aux_rx.borrow() {
                let tick_start = Instant::now();
                let _ = aux_engine.aux_sync.run_once(false);
                let elapsed_ms = tick_start.elapsed().as_millis() as u64;
                let rem_ms = (AUX_SYNC_INTERVAL_MS as u64)
                    .saturating_sub(elapsed_ms)
                    .max(500);
                sleep_interruptible(&aux_rx, rem_ms);
            }
        });

        tx
    }
}

fn build_held_position_contexts(
    held: &[HeldAssetView],
    ordered: &[OrderedAssetView],
    eq: f64,
) -> Result<HashMap<i64, HeldPositionContext>> {
    let mut weight_by_asset: HashMap<i64, f64> = HashMap::new();
    let mut pnl_acc: HashMap<i64, (f64, f64)> = HashMap::new();

    for h in held {
        let side = OrderSide::parse(&h.side)?;
        let w = if h.portfolio_weight.abs() > 1e-6 {
            h.portfolio_weight.abs() * side.sign()
        } else {
            (h.committed_notional_usdc / eq) * side.sign()
        };
        *weight_by_asset.entry(h.asset_id).or_insert(0.0) += w;

        let notional = if h.committed_notional_usdc.is_finite() && h.committed_notional_usdc > 0.0 {
            h.committed_notional_usdc
        } else if h.entry_price.is_finite()
            && h.entry_price > 0.0
            && h.quantity.is_finite()
            && h.quantity > 0.0
        {
            (h.entry_price * h.quantity).abs()
        } else {
            1.0
        };
        let frac = held_position_fractional_pnl(h);
        let entry = pnl_acc.entry(h.asset_id).or_insert((0.0, 0.0));
        entry.0 += frac * notional;
        entry.1 += notional;
    }

    for o in ordered {
        let side = OrderSide::parse(&o.side)?;
        let notional = o.limit_price * o.requested_quantity;
        let w = (notional / eq) * side.sign();
        *weight_by_asset.entry(o.asset_id).or_insert(0.0) += w;
    }

    let mut held_contexts = HashMap::with_capacity(weight_by_asset.len());
    for (asset_id, current_weight) in weight_by_asset {
        let unrealized_pnl_pct = pnl_acc
            .get(&asset_id)
            .filter(|(_, denom)| *denom > 1e-9)
            .map(|(num, denom)| num / denom)
            .unwrap_or(0.0);
        held_contexts.insert(
            asset_id,
            HeldPositionContext {
                current_weight,
                unrealized_pnl_pct,
            },
        );
    }

    Ok(held_contexts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_ladder::{
        collect_coin_non_trigger_cancels, compute_passive_close_px,
        find_newest_matching_passive_close_oid, is_ioc_no_match_error,
    };
    use aegis_core::is_hl_trigger_order;
    use aegis_db::{AssetUpsertInput, ExchangeOpenPositionSyncInput, OidUpdate};
    use aegis_test_support::mock_http::MockHttpServer;

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

    fn setup_live_met_engine(
        base_url: &str,
        stale_db_mark: f64,
        oracle_px: f64,
    ) -> (DatabaseManager, ExecutionEngine, i64) {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        db.init_schema().unwrap();
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
                oracle_price: oracle_px,
                funding_rate_hourly: 0.0000125,
                day_volume_usdc: 1_000_000.0,
                open_interest_usdc: 500_000.0,
                prev_day_price: 0.22,
                is_core_eligible: false,
                is_event_eligible: false,
                is_priority_watched: true,
            })
            .unwrap();

        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_testnet_url", base_url, None)
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
            .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
                asset_id,
                coin: "MET",
                side: OrderSide::Long,
                entry_price: 0.23227,
                mark_price: stale_db_mark,
                quantity: 9634.0,
                committed_notional_usdc: 2058.7858,
                unrealized_pnl_usdc: 0.0,
                portfolio_weight: 0.5,
                protective_stop_px: 0.20111,
                take_profit_px: 0.25086,
                funding_accrued_usdc: 0.0,
                opened_at_ms: Some(now_ms() - 600_000),
            })
            .unwrap();
        db.update_position_tpsl_orders(
            pos_id,
            0.20111,
            OidUpdate::Set("hl-65605015926"),
            0.25086,
            OidUpdate::Set("hl-65648930301"),
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
            json!({
                "coin": "MET",
                "oid": 65605015926u64,
                "side": "A",
                "sz": "9634",
                "limitPx": "0.185",
                "reduceOnly": true,
                "isTrigger": true,
                "orderType": "Stop Limit",
                "triggerPx": "0.20111",
                "timestamp": 1000
            }),
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

        let snap = OpenOrdersSnapshot {
            orders: orders.clone(),
            metadata_complete: true,
        };
        let cancels = collect_coin_non_trigger_cancels(&snap, &met, keep);
        let cancel_oids: Vec<u64> = cancels.iter().map(|c| c.o).collect();
        assert_eq!(cancel_oids, vec![70001, 70003]);

        // R14-M7: incomplete metadata skips non-trigger cancels
        let incomplete_snap = OpenOrdersSnapshot {
            orders,
            metadata_complete: false,
        };
        assert!(collect_coin_non_trigger_cancels(&incomplete_snap, &met, keep).is_empty());

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
        let tpsl_snap = OpenOrdersSnapshot {
            orders: vec![pos_tpsl_limit],
            metadata_complete: true,
        };
        assert!(collect_coin_non_trigger_cancels(&tpsl_snap, &met, None).is_empty());
        assert!((parse_hl_f64(Some(&json!(" 0.2137 "))) - 0.2137).abs() < 1e-9);
        assert_eq!(extract_hl_oid(&json!({ "oid": " 70005 " })), Some(70005));

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
                "orderType": "Stop Limit",
                "limitPx": "0.185",
                "triggerPx": "0.20111",
                "sz": "9634"
            }),
            json!({
                "coin": "MET",
                "oid": 65648930301u64,
                "isTrigger": true,
                "orderType": "Take Profit Limit",
                "limitPx": "0.258",
                "triggerPx": "0.25086",
                "sz": "9634"
            }),
        ]));
        let open_orders_srv = open_orders_state.clone();
        let submitted_orders = Arc::new(Mutex::new(Vec::<Value>::new()));
        let submitted_srv = submitted_orders.clone();
        let cancelled_oids = Arc::new(Mutex::new(Vec::<u64>::new()));
        let cancelled_srv = cancelled_oids.clone();

        let srv = MockHttpServer::start_json(move |path, body| {
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

        let (_db, engine, pos_id) = setup_live_met_engine(&srv.base_url(), 0.20115, 0.2137);

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
    }

    #[test]
    fn test_close_or_cancel_ioc_deduplicates_clamped_prices_and_breaks_early_on_band_reject() {
        let reject_error_msg = Arc::new(Mutex::new(
            "Order could not immediately match against any resting orders.".to_string(),
        ));
        let reject_srv = reject_error_msg.clone();
        let submitted_orders = Arc::new(Mutex::new(Vec::<Value>::new()));
        let submitted_srv = submitted_orders.clone();

        let srv = MockHttpServer::start_json(move |path, body| {
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
                                "orderType": "Stop Limit",
                                "limitPx": "0.185",
                                "triggerPx": "0.20111",
                                "sz": "9634"
                            },
                            {
                                "coin": "MET",
                                "oid": 65648930301u64,
                                "isTrigger": true,
                                "orderType": "Take Profit Limit",
                                "limitPx": "0.258",
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

        let (_db, engine, pos_id) = setup_live_met_engine(&srv.base_url(), 0.20000, 0.20000);

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
    }

    #[test]
    fn test_passes_entry_l2_liquidity_gate_and_oracle_deviation() {
        let healthy = L2BookDepthSnapshot {
            best_bid: 100.0,
            best_ask: 100.05,
            spread_bps: 5.0,
            bid_depth_15bps_usdc: 500.0,
            ask_depth_15bps_usdc: 500.0,
        };
        assert!(passes_entry_l2_liquidity_gate(&healthy, 200.0, 20.0).is_ok());

        // Default floor: $150.0 vs 1.5 * $60 = $90 -> requires $150 on both sides
        let exact_floor = L2BookDepthSnapshot {
            best_bid: 100.0,
            best_ask: 100.20,
            spread_bps: 20.0,
            bid_depth_15bps_usdc: 150.0,
            ask_depth_15bps_usdc: 150.0,
        };
        assert!(passes_entry_l2_liquidity_gate(&exact_floor, 60.0, 20.0).is_ok());

        let wide = L2BookDepthSnapshot {
            spread_bps: 54.0,
            ..healthy
        };
        assert!(passes_entry_l2_liquidity_gate(&wide, 200.0, 20.0)
            .unwrap_err()
            .contains("ENTRY_SPREAD_TOO_WIDE"));

        let thin_ask = L2BookDepthSnapshot {
            ask_depth_15bps_usdc: 149.99,
            ..healthy
        };
        assert!(passes_entry_l2_liquidity_gate(&thin_ask, 60.0, 20.0)
            .unwrap_err()
            .contains("ENTRY_L2_DEPTH_TOO_THIN"));

        let thin_bid = L2BookDepthSnapshot {
            bid_depth_15bps_usdc: 599.9,
            ask_depth_15bps_usdc: 800.0,
            ..healthy
        };
        assert!(passes_entry_l2_liquidity_gate(&thin_bid, 400.0, 20.0)
            .unwrap_err()
            .contains("ENTRY_L2_DEPTH_TOO_THIN"));
        let ok_bid = L2BookDepthSnapshot {
            bid_depth_15bps_usdc: 600.0,
            ask_depth_15bps_usdc: 600.0,
            ..healthy
        };
        assert!(passes_entry_l2_liquidity_gate(&ok_bid, 400.0, 20.0).is_ok());

        // Custom multiplier/floor override works identically
        assert!(passes_entry_l2_liquidity_gate_with_thresholds(
            &L2BookDepthSnapshot {
                bid_depth_15bps_usdc: 25.0,
                ask_depth_15bps_usdc: 25.0,
                ..healthy
            },
            60.0,
            20.0,
            0.25,
            25.0
        )
        .is_ok());

        // Oracle deviation gate rejects invalid oracle & >1.5% divergence
        let mut met = sample_met_asset();
        assert!(passes_entry_oracle_deviation_gate(&met, 0.015).is_err());
        met.mark_price = 0.251;
        met.mid_price = 0.2509;
        assert!(passes_entry_oracle_deviation_gate(&met, 0.015).is_ok());
        met.oracle_price = 0.0;
        assert!(passes_entry_oracle_deviation_gate(&met, 0.015).is_err());
    }
}
