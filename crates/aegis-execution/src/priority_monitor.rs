//! 1-Second High-Frequency Surveillance Loop (`aegis-execution::priority_monitor`).
//!
//! Operates strictly in 3 R14-H2 phases (**Snapshot -> Unlocked HTTP IO -> Re-Acquire & Commit**):
//! 1. Advances all `WORKING` Maker Limit Chaser orders (`v_assets_ordered`) sharing a single `clearinghouseState`.
//! 2. Monitors all `OPEN` holdings (`v_assets_held`) via `RiskEngine::evaluate_1s_position_lifecycle`,
//!    with per-item error isolation (`R14-H3`), oracle-fallback basis switching (`P0-Exec-2`),
//!    non-blocking maker-first passive closes (`P1-Strat`), unified reduce-only close ladder (`R14-M2`)
//!    without ever widening stops (`R14-H5`), immediate partial-exit persistence (`R14-M3`),
//!    widened `userFillsByTime` lookup (`R14-M4`), and batched 39-action TP/SL synchronization (`Sec. 11`).

use crate::close_ladder::{
    place_passive_reduce_only_close_order, submit_reduce_only_close_ladder, CloseUrgency,
    PassiveClosePlacement, PassiveCloseTracker, PendingPassiveClose, ReduceOnlyCloseLadderRequest,
    DEFAULT_PASSIVE_CLOSE_REST_SEC,
};
use crate::http_client::HyperliquidHttpClient;
use crate::lock_guard::acquire_execution_lock;
use crate::order_chaser::{parse_hl_oid, ChaseStepOutcome, OrderChaser};
use crate::signing::CancelWire;
use crate::tpsl_sync::{sync_tpsl_batch_for_positions, TpslSyncRequest};
use crate::ws_client::WsStateCache;
use aegis_alerts::{format_position_closed_html, AlertEngine, PositionClosedAlert};
use aegis_core::{
    now_ms, parse_hl_f64, parse_hl_i64_opt, parse_hl_u64_opt, round_dp, AegisError, ConfigMap,
    ExchangeAsset, HeldAssetView, OrderSide, PriorityMonitorHealth, Result,
};
use aegis_db::DatabaseManager;
use aegis_risk::{RiskEngine, StopEvaluationInput};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

/// Minimum relative stop ratchet (`0.25% = 25 bps`) required to persist DB & replace HL trigger (`R14-M5`).
pub const MATERIAL_STOP_RATCHET_MIN_PCT: f64 = 0.0025;

/// Retry backoff (`15_000 ms`) between `/exchange` close attempts on illiquid unfilled stop breaches (`R14-H5`).
pub const UNFILLED_BREACH_RETRY_BACKOFF_MS: i64 = 15_000;

/// Fallback `userFillsByTime` lookback (`24h`) when `opened_at_ms` & `last_confirmed_open_ms` are absent (`R14-M4`).
pub const EXCHANGE_FLAT_FILL_FALLBACK_LOOKBACK_MS: i64 = 86_400_000;

const EXCHANGE_FLAT_MAX_LOOKBACK_MS: i64 = 7 * 86_400_000;
const EXCHANGE_FLAT_SLICE_MAX_GAP_MS: i64 = 60_000;
const HEALTH_PERSIST_HEARTBEAT_MS: i64 = 30_000;
const UNFILLED_BREACH_ALERT_THROTTLE_MS: i64 = 300_000;
const MAX_TICK_ERROR_ITEMS: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq)]
struct StopSyncDecision {
    persisted_stop_px: f64,
    sl_ratcheted_materially: bool,
    should_sync_tpsl: bool,
    count_ratchet: bool,
}

fn resolve_stop_sync_decision(
    current_stop_px: f64,
    eval_new_stop_px: f64,
    eval_stop_ratcheted: bool,
    missing_sl: bool,
    missing_tp: bool,
) -> StopSyncDecision {
    let first_stop_attach = current_stop_px <= 0.0 && eval_new_stop_px > 0.0;
    let sl_ratcheted_materially = eval_stop_ratcheted
        && current_stop_px > 0.0
        && ((eval_new_stop_px - current_stop_px).abs() / current_stop_px)
            >= MATERIAL_STOP_RATCHET_MIN_PCT;
    let persisted_stop_px = if first_stop_attach || sl_ratcheted_materially {
        eval_new_stop_px
    } else {
        current_stop_px
    };
    let should_sync_tpsl = missing_sl || missing_tp || sl_ratcheted_materially || first_stop_attach;
    let count_ratchet = sl_ratcheted_materially || first_stop_attach;
    StopSyncDecision {
        persisted_stop_px,
        sl_ratcheted_materially,
        should_sync_tpsl,
        count_ratchet,
    }
}

/// Switches 1s surveillance reference to `oracle_price` when mark-vs-oracle basis exceeds
/// `trigger_oracle_fallback_basis_bps` (`50 bps` default) (`P0-Exec-2`).
pub fn resolve_surveillance_trigger_price(
    asset_opt: Option<&ExchangeAsset>,
    live_mark: f64,
    trigger_oracle_fallback_basis_bps: f64,
) -> f64 {
    let Some(asset) = asset_opt else {
        return live_mark;
    };
    if asset.oracle_price.is_finite()
        && asset.oracle_price > 0.0
        && live_mark.is_finite()
        && live_mark > 0.0
    {
        let basis_bps = ((live_mark - asset.oracle_price).abs() / asset.oracle_price) * 10_000.0;
        if basis_bps > trigger_oracle_fallback_basis_bps {
            return asset.oracle_price;
        }
    }
    live_mark
}

/// Individual error record captured during a 1s surveillance tick (`R14-H3`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PriorityTickErrorItem {
    pub scope: String,
    pub position_id: Option<i64>,
    pub symbol: Option<String>,
    pub error: String,
}

/// Telemetry report emitted by each 1-second Priority Monitor tick (`R14-H3`).
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
    #[serde(default)]
    pub working_order_errors: usize,
    #[serde(default)]
    pub open_holding_errors: usize,
    #[serde(default)]
    pub tpsl_sync_errors: usize,
    #[serde(default)]
    pub batch_update_failed: bool,
    #[serde(default)]
    pub degraded: bool,
    #[serde(default)]
    pub consecutive_degraded_ticks: u32,
    #[serde(default)]
    pub errors: Vec<PriorityTickErrorItem>,
}

#[derive(Debug, Clone)]
struct UnfilledBreachState {
    first_breach_ms: i64,
    last_attempt_ms: i64,
    passive_close_oid: Option<String>,
    passive_close_px: Option<f64>,
    attempt_count: u32,
    last_alert_ms: i64,
    is_tp: bool,
    symbol: String,
}

#[derive(Debug, Default)]
struct SurveillanceHealthState {
    consecutive_error_ticks: u32,
    per_position_error_streaks: HashMap<i64, u32>,
    last_alert_emitted_ms: i64,
    unmonitored_alert_active: bool,
    last_persisted_status: String,
    last_persisted_breaches: Vec<String>,
    last_persisted_ms: i64,
}

#[derive(Clone)]
struct TriggeredExitItem {
    held: HeldAssetView,
    asset: Option<ExchangeAsset>,
    side: OrderSide,
    exit_price: f64,
    exit_reason: String,
    urgency: CloseUrgency,
}

#[derive(Clone)]
struct PendingTpslItem {
    held: HeldAssetView,
    asset: ExchangeAsset,
    side: OrderSide,
    live_mark: f64,
    stop_px: f64,
    tp_px: f64,
    replace_sl: bool,
}

/// 1-Second High-Frequency Priority Position & Active Order Monitor.
#[derive(Clone)]
pub struct PriorityMonitor {
    pub db: DatabaseManager,
    pub http: HyperliquidHttpClient,
    pub chaser: OrderChaser,
    pub alerts: AlertEngine,
    pub passive_closes: PassiveCloseTracker,
    unfilled_breaches: Arc<Mutex<HashMap<i64, UnfilledBreachState>>>,
    last_confirmed_open_ms: Arc<Mutex<HashMap<i64, i64>>>,
    health_state: Arc<Mutex<SurveillanceHealthState>>,
    alert_wakeup_tx: Option<SyncSender<usize>>,
    ws_cache: Option<Arc<WsStateCache>>,
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
            passive_closes: PassiveCloseTracker::new(),
            unfilled_breaches: Arc::new(Mutex::new(HashMap::new())),
            last_confirmed_open_ms: Arc::new(Mutex::new(HashMap::new())),
            health_state: Arc::new(Mutex::new(SurveillanceHealthState::default())),
            alert_wakeup_tx: None,
            ws_cache: None,
        }
    }

    /// Attaches an optional bounded wakeup sender for non-blocking background alert dispatch (`R14-L5`).
    pub fn with_alert_wakeup(mut self, wakeup_tx: SyncSender<usize>) -> Self {
        self.alert_wakeup_tx = Some(wakeup_tx);
        self
    }

    /// Attaches a shared [`WsStateCache`] so `allMids`, `l2Book`, `clearinghouseState`, and
    /// `openOrders` are read from fresh WebSocket streams with transparent REST fallback (`R14-H4`).
    pub fn with_ws_cache(mut self, ws_cache: Arc<WsStateCache>) -> Self {
        self.chaser = self.chaser.with_ws_cache(Arc::clone(&ws_cache));
        self.ws_cache = Some(ws_cache);
        self
    }

    fn wake_alert_worker(&self) {
        if let Some(ref tx) = self.alert_wakeup_tx {
            let _ = tx.try_send(1);
        }
    }

    fn enqueue_non_blocking_alert(
        &self,
        event_type: &str,
        severity: &str,
        html_message: &str,
        payload: Option<&Value>,
    ) {
        let _ = self
            .alerts
            .enqueue_html_alert(event_type, severity, html_message, payload);
        self.wake_alert_worker();
    }

    /// Records a top-level `run_1s_priority_tick()` failure (e.g., DB unreachable in caller loop) (`R14-H3`).
    pub fn record_tick_failure(&self, err: &AegisError) {
        let ts = now_ms();
        let cfg = self.db.load_config_or_default();
        let threshold = cfg
            .get_i64("priority_unmonitored_alert_ticks", 10)
            .clamp(3, 300) as u32;
        let cooldown_ms = cfg
            .get_i64("chase_reject_cooldown_sec", 300)
            .clamp(30, 3600)
            * 1000;

        let mut health_guard = self.health_state.lock();
        health_guard.consecutive_error_ticks =
            health_guard.consecutive_error_ticks.saturating_add(1);
        let streak = health_guard.consecutive_error_ticks;
        let should_alert = streak >= threshold
            && (!health_guard.unmonitored_alert_active
                || ts.saturating_sub(health_guard.last_alert_emitted_ms) >= cooldown_ms);
        if streak >= threshold {
            health_guard.unmonitored_alert_active = true;
        }
        if should_alert {
            health_guard.last_alert_emitted_ms = ts;
        }
        let active = health_guard.unmonitored_alert_active;
        drop(health_guard);

        let err_str = err.to_string();
        if should_alert {
            let html = format!(
                "<b>[CRITICAL] PRIORITY MONITOR UNMONITORED</b><br/>Consecutive degraded ticks: <code>{}</code><br/>Error: <code>{}</code>",
                streak,
                err_str.chars().take(240).collect::<String>()
            );
            let payload = json!({
                "consecutive_error_ticks": streak,
                "error": err_str,
                "timestamp_ms": ts,
            });
            self.enqueue_non_blocking_alert(
                "PRIORITY_MONITOR_UNMONITORED",
                "CRITICAL",
                &html,
                Some(&payload),
            );
            let _ = self.db.log_telemetry(
                "ERROR",
                "execution.priority_monitor",
                &format!("PriorityMonitor unmonitored after {streak} failing ticks: {err_str}"),
                Some(payload),
            );
        }

        let breaches: Vec<String> = self
            .unfilled_breaches
            .lock()
            .values()
            .map(|b| b.symbol.clone())
            .collect();
        let status = if active { "CRITICAL" } else { "DEGRADED" };
        let snapshot = PriorityMonitorHealth {
            last_tick_at_ms: ts,
            consecutive_error_ticks: streak,
            last_tick_error_count: 1,
            failed_symbols: Vec::new(),
            unfilled_stop_breach_symbols: breaches,
            unmonitored_alert_active: active,
            is_stale: false,
            status: status.to_string(),
        };
        if let Ok(json_str) = serde_json::to_string(&snapshot) {
            let _ = self.db.set_config_value(
                "priority_monitor_health_json",
                &json_str,
                Some("Cached 1s PriorityMonitor surveillance health snapshot JSON"),
            );
        }
    }

    /// Runs a single 1-second surveillance tick over `v_priority_watchlist`, `v_assets_ordered`, and `v_assets_held`
    /// using strict **Snapshot -> Unlocked IO -> Short Locked Commit** isolation (`R14-H2`, `R14-H3`).
    pub fn run_1s_priority_tick(&self) -> Result<PriorityTickReport> {
        let ts = now_ms();

        // Phase 1: Short lock snapshot of config, watchlist, and working orders
        let mut initial_ordered_err: Option<String> = None;
        let (cfg, watchlist, initial_ordered, initial_has_held, is_live_exchange) = {
            let _guard = acquire_execution_lock();
            let cfg = self.db.load_config()?;
            let watchlist = self.db.get_priority_watchlist()?;
            let ordered = match self.db.get_assets_ordered() {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error = %e, "PriorityMonitor get_assets_ordered failed; continuing to OPEN holdings");
                    initial_ordered_err = Some(e.to_string());
                    Vec::new()
                }
            };
            let has_held = self
                .db
                .get_assets_held()
                .map(|v| !v.is_empty())
                .unwrap_or(true);
            let is_live_exchange = crate::engine::is_live_exchange_active(&self.db, &cfg);
            (cfg, watchlist, ordered, has_held, is_live_exchange)
        };

        // Phase 2: Unlocked surveillance reads (`allMids` + single shared `clearinghouseState`)
        // Prefer fresh WsStateCache frames, falling back transparently to REST when stale/disconnected.
        let mids_opt = if !watchlist.is_empty() && is_live_exchange {
            self.ws_cache
                .as_ref()
                .and_then(|c| c.get_fresh_all_mids())
                .or_else(|| self.http.fetch_all_mids(&cfg).ok())
        } else {
            None
        };
        let wallet = cfg.get_str("wallet_address", "");
        let shared_ch_state = if is_live_exchange
            && !wallet.trim().is_empty()
            && (!initial_ordered.is_empty() || initial_has_held)
        {
            self.ws_cache
                .as_ref()
                .and_then(|c| c.get_fresh_clearinghouse_state())
                .or_else(|| self.http.fetch_clearinghouse_state(&wallet, &cfg).ok())
        } else {
            None
        };

        // Phase 3A: Short locked batch mid-price update
        let mut report = PriorityTickReport {
            tick_at_ms: ts,
            watchlist_count: watchlist.len(),
            ..Default::default()
        };
        if let Some(ref err_msg) = initial_ordered_err {
            report.working_order_errors += 1;
            Self::push_tick_error(&mut report, "get_assets_ordered", None, None, err_msg);
        }
        {
            let _guard = acquire_execution_lock();
            if let Some(ref mids) = mids_opt {
                let mid_updates: Vec<(i64, f64)> = watchlist
                    .iter()
                    .filter_map(|item| {
                        mids.get(&item.coin).copied().and_then(|live_mid| {
                            (live_mid.is_finite()
                                && live_mid > 0.0
                                && (live_mid - item.mid_price).abs() > 1e-12)
                                .then_some((item.asset_id, live_mid))
                        })
                    })
                    .collect();
                if let Err(e) = self.db.update_assets_live_mid_batch(&mid_updates) {
                    Self::push_tick_error(
                        &mut report,
                        "mid_batch_update",
                        None,
                        None,
                        &e.to_string(),
                    );
                }
            }
        }

        // Phase 3B: Advance WORKING orders UNLOCKED, isolating failures per item (`R14-H3`)
        report.working_orders_checked = initial_ordered.len();
        let active_working_ids: HashSet<i64> =
            initial_ordered.iter().map(|o| o.position_id).collect();
        for ord in &initial_ordered {
            match self
                .chaser
                .advance_working_order_with_ch_state(ord, shared_ch_state.as_ref())
            {
                Ok(ChaseStepOutcome::Chased { .. }) => {
                    report.orders_chased += 1;
                }
                Ok(ChaseStepOutcome::FilledOpen { .. }) => {
                    report.orders_promoted_to_open += 1;
                }
                Ok(ChaseStepOutcome::Resting { .. } | ChaseStepOutcome::Cancelled { .. }) => {}
                Err(e) => {
                    report.working_order_errors += 1;
                    tracing::warn!(
                        position_id = ord.position_id,
                        symbol = %ord.symbol,
                        error = %e,
                        "PriorityMonitor working order step failed (isolated)"
                    );
                    Self::push_tick_error(
                        &mut report,
                        "working_order",
                        Some(ord.position_id),
                        Some(ord.symbol.clone()),
                        &e.to_string(),
                    );
                }
            }
        }

        // Extract live coins from shared `clearinghouseState` for R14-M4 `last_confirmed_open_ms` tracking
        let live_ch_coins: HashSet<String> = shared_ch_state
            .as_ref()
            .and_then(|ch_val| ch_val.get("assetPositions").and_then(Value::as_array))
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| {
                        let p = item.get("position").unwrap_or(item);
                        let szi = parse_hl_f64(p.get("szi"));
                        (szi.abs() > 1e-9).then(|| {
                            p.get("coin")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .trim()
                                .to_ascii_uppercase()
                        })
                    })
                    .filter(|c| !c.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        // Phase 4A: Short locked snapshot & lifecycle evaluation of ALL OPEN holdings (`R14-H3`)
        let risk = RiskEngine::from_config_map(&cfg);
        let passive_close_rest_sec = cfg
            .get_i64("passive_close_rest_sec", DEFAULT_PASSIVE_CLOSE_REST_SEC)
            .clamp(0, 60);
        let emergency_ioc_breach_pct = cfg
            .get_f64("emergency_ioc_breach_pct", 0.015)
            .clamp(0.005, 0.05);
        let trigger_oracle_fallback_basis_bps = cfg
            .get_f64("trigger_oracle_fallback_basis_bps", 50.0)
            .clamp(10.0, 500.0);

        let mut held: Vec<HeldAssetView> = Vec::new();
        let mut active_open_ids: HashSet<i64> = HashSet::new();
        let mut assets_by_id: HashMap<i64, ExchangeAsset> = HashMap::new();
        let mut passive_checks: Vec<(
            HeldAssetView,
            Option<ExchangeAsset>,
            OrderSide,
            f64,
            PendingPassiveClose,
        )> = Vec::new();
        let mut triggered_exits: Vec<TriggeredExitItem> = Vec::new();
        let mut pos_updates: Vec<(i64, f64, f64)> = Vec::new();
        let mut tpsl_syncs: Vec<PendingTpslItem> = Vec::new();
        let mut failed_position_ids: HashSet<i64> = HashSet::new();

        // Warm sparse signal_timeframe ATR bars unlocked before entering the short locked Phase 4A block (`P0-1`, `R14-H2`)
        {
            let signal_tf = cfg.get_str("signal_timeframe", "4h");
            let wall_clock_norm = cfg.get_bool("wall_clock_normalize_indicators", true);
            let (atr_tf, query_limit, _, _) =
                crate::engine::atr_params_for_timeframe(&signal_tf, wall_clock_norm);
            if let Ok(held_pre) = self.db.get_assets_held() {
                for h in &held_pre {
                    let local_bars = self
                        .db
                        .get_ohlcv_bars(h.asset_id, &atr_tf, query_limit)
                        .unwrap_or_default();
                    if local_bars.len() < 16 {
                        let ref_px = if h.mark_price > 0.0 {
                            h.mark_price
                        } else {
                            h.entry_price
                        };
                        let _ = crate::engine::resolve_signal_matched_atr_unlocked(
                            &self.db,
                            &self.http,
                            None,
                            h.asset_id,
                            &cfg,
                            ref_px,
                            risk.limits.protective_stop_default_pct,
                        );
                    }
                }
            }
        }

        {
            let _guard = acquire_execution_lock();
            match self.db.get_assets_held() {
                Ok(h_vec) => held = h_vec,
                Err(e) => {
                    report.open_holding_errors += 1;
                    tracing::error!(error = %e, "PriorityMonitor get_assets_held failed");
                    Self::push_tick_error(
                        &mut report,
                        "get_assets_held",
                        None,
                        None,
                        &e.to_string(),
                    );
                }
            }
            if !live_ch_coins.is_empty() {
                let mut map = self.last_confirmed_open_ms.lock();
                for h in &held {
                    if live_ch_coins.contains(&h.coin.trim().to_ascii_uppercase()) {
                        map.insert(h.position_id, ts);
                    }
                }
            }
            report.open_holdings_checked = held.len();
            for h in &held {
                active_open_ids.insert(h.position_id);
                if let Some(pending) = self.passive_closes.get(h.position_id) {
                    let asset_opt = match self.db.get_asset_by_id(h.asset_id) {
                        Ok(a) => a,
                        Err(e) => {
                            report.open_holding_errors += 1;
                            failed_position_ids.insert(h.position_id);
                            Self::push_tick_error(
                                &mut report,
                                "open_holding_asset",
                                Some(h.position_id),
                                Some(h.symbol.clone()),
                                &e.to_string(),
                            );
                            continue;
                        }
                    };
                    if let Some(ref a) = asset_opt {
                        assets_by_id.insert(h.asset_id, a.clone());
                    }
                    let side = match OrderSide::parse(&h.side) {
                        Ok(s) => s,
                        Err(e) => {
                            report.open_holding_errors += 1;
                            failed_position_ids.insert(h.position_id);
                            Self::push_tick_error(
                                &mut report,
                                "open_holding_side",
                                Some(h.position_id),
                                Some(h.symbol.clone()),
                                &e.to_string(),
                            );
                            continue;
                        }
                    };
                    let live_mark = asset_opt
                        .as_ref()
                        .map(|a| a.mark_price)
                        .filter(|&p| p > 0.0)
                        .unwrap_or(h.mark_price);
                    let ref_px = resolve_surveillance_trigger_price(
                        asset_opt.as_ref(),
                        live_mark,
                        trigger_oracle_fallback_basis_bps,
                    );
                    passive_checks.push((h.clone(), asset_opt, side, ref_px, pending));
                    continue;
                }

                let asset_opt = match self.db.get_asset_by_id(h.asset_id) {
                    Ok(a) => a,
                    Err(e) => {
                        report.open_holding_errors += 1;
                        failed_position_ids.insert(h.position_id);
                        Self::push_tick_error(
                            &mut report,
                            "open_holding_asset",
                            Some(h.position_id),
                            Some(h.symbol.clone()),
                            &e.to_string(),
                        );
                        continue;
                    }
                };
                if let Some(ref a) = asset_opt {
                    assets_by_id.insert(h.asset_id, a.clone());
                }
                let side = match OrderSide::parse(&h.side) {
                    Ok(s) => s,
                    Err(e) => {
                        report.open_holding_errors += 1;
                        failed_position_ids.insert(h.position_id);
                        Self::push_tick_error(
                            &mut report,
                            "open_holding_side",
                            Some(h.position_id),
                            Some(h.symbol.clone()),
                            &e.to_string(),
                        );
                        continue;
                    }
                };
                let tick_size = asset_opt.as_ref().map(|a| a.tick_size).unwrap_or(0.01);
                let live_mark = asset_opt
                    .as_ref()
                    .map(|a| a.mark_price)
                    .filter(|&p| p > 0.0)
                    .unwrap_or(h.mark_price);
                let surveillance_ref_px = resolve_surveillance_trigger_price(
                    asset_opt.as_ref(),
                    live_mark,
                    trigger_oracle_fallback_basis_bps,
                );
                let atr_opt = crate::engine::resolve_signal_matched_atr_from_db(
                    &self.db,
                    h.asset_id,
                    &cfg,
                    surveillance_ref_px,
                    risk.limits.protective_stop_default_pct,
                );
                let held_duration_ms = ts
                    .saturating_sub(h.opened_at_ms.unwrap_or(h.created_at_ms))
                    .max(0);
                let eval = risk.evaluate_1s_position_lifecycle(&StopEvaluationInput {
                    side,
                    entry_price: h.entry_price,
                    mark_price: surveillance_ref_px,
                    quantity: h.quantity,
                    current_stop_px: h.protective_stop_px,
                    current_tp_px: h.take_profit_px,
                    highest_price_seen: h.highest_price_seen,
                    lowest_price_seen: h.lowest_price_seen,
                    atr_opt,
                    tick_size,
                    check_tp: true,
                    held_duration_ms: Some(held_duration_ms),
                    entry_r0: (h.entry_r0 > 0.0).then_some(h.entry_r0),
                });

                if eval.triggered {
                    let exit_reason = eval
                        .exit_reason
                        .clone()
                        .unwrap_or_else(|| "PROTECTIVE_STOP_BREACH".to_string());
                    let is_stop_breach = exit_reason.starts_with("PROTECTIVE_STOP");
                    let emergency_gap = is_stop_breach
                        && h.protective_stop_px > 0.0
                        && (((h.protective_stop_px - surveillance_ref_px) * side.sign())
                            / h.protective_stop_px)
                            > emergency_ioc_breach_pct;
                    let urgency =
                        if !is_live_exchange || passive_close_rest_sec == 0 || emergency_gap {
                            CloseUrgency::ImmediateIoc
                        } else {
                            CloseUrgency::PassiveFirst
                        };
                    triggered_exits.push(TriggeredExitItem {
                        held: h.clone(),
                        asset: asset_opt,
                        side,
                        exit_price: surveillance_ref_px,
                        exit_reason,
                        urgency,
                    });
                } else {
                    let missing_sl = h
                        .protective_stop_oid
                        .as_deref()
                        .map(str::trim)
                        .unwrap_or("")
                        .is_empty();
                    let missing_tp = h
                        .take_profit_oid
                        .as_deref()
                        .map(str::trim)
                        .unwrap_or("")
                        .is_empty()
                        || h.take_profit_px <= 0.0;
                    let decision = resolve_stop_sync_decision(
                        h.protective_stop_px,
                        eval.new_protective_stop_px,
                        eval.stop_ratcheted,
                        missing_sl,
                        missing_tp,
                    );
                    pos_updates.push((h.position_id, live_mark, decision.persisted_stop_px));
                    if decision.count_ratchet {
                        report.stops_ratcheted += 1;
                    }
                    if decision.should_sync_tpsl {
                        if let Some(asset) = asset_opt {
                            tpsl_syncs.push(PendingTpslItem {
                                held: h.clone(),
                                asset,
                                side,
                                live_mark,
                                stop_px: decision.persisted_stop_px,
                                tp_px: eval.take_profit_px,
                                replace_sl: decision.sl_ratcheted_materially,
                            });
                        }
                    }
                }
            }

            if !pos_updates.is_empty() {
                if let Err(e) = self
                    .db
                    .update_open_positions_mark_and_stop_batch(&pos_updates)
                {
                    report.batch_update_failed = true;
                    tracing::error!(error = %e, "update_open_positions_mark_and_stop_batch failed");
                    Self::push_tick_error(
                        &mut report,
                        "mark_stop_batch",
                        None,
                        None,
                        &e.to_string(),
                    );
                }
            }
        }

        // Phase 4B: Unlocked monitoring of pending PassiveFirst closes (`P1-Strat`)
        for (h, asset_opt, side, ref_px, pending) in passive_checks {
            match self.process_pending_passive_close(
                &h,
                asset_opt.as_ref(),
                side,
                ref_px,
                &pending,
                passive_close_rest_sec,
                emergency_ioc_breach_pct,
                shared_ch_state.as_ref(),
                &risk,
                &cfg,
            ) {
                Ok(true) => {
                    report.stops_triggered_closed += 1;
                }
                Ok(false) => {}
                Err(e) => {
                    report.open_holding_errors += 1;
                    failed_position_ids.insert(h.position_id);
                    Self::push_tick_error(
                        &mut report,
                        "passive_close",
                        Some(h.position_id),
                        Some(h.symbol.clone()),
                        &e.to_string(),
                    );
                }
            }
        }

        // Phase 4C: Unlocked execution of newly triggered exits (`R14-H5`, `R14-M2`, `R14-M3`, `P1-Strat`)
        for item in triggered_exits {
            match self.execute_exit_with_urgency(
                &item.held,
                item.asset.as_ref(),
                item.side,
                item.exit_price,
                &item.exit_reason,
                item.urgency,
                shared_ch_state.as_ref(),
                &risk,
                &cfg,
            ) {
                Ok(Some(_realized_pnl)) => {
                    self.unfilled_breaches.lock().remove(&item.held.position_id);
                    report.stops_triggered_closed += 1;
                }
                Ok(None) => {}
                Err(e) => {
                    report.open_holding_errors += 1;
                    failed_position_ids.insert(item.held.position_id);
                    Self::push_tick_error(
                        &mut report,
                        "triggered_exit",
                        Some(item.held.position_id),
                        Some(item.held.symbol.clone()),
                        &e.to_string(),
                    );
                }
            }
        }

        // Phase 4D: Unlocked batched TP/SL synchronization (`Sec. 11`, `P0-Risk` Defect #6, `R14-H3`)
        if !tpsl_syncs.is_empty() {
            let reqs: Vec<TpslSyncRequest<'_>> = tpsl_syncs
                .iter()
                .map(|t| TpslSyncRequest {
                    position_id: t.held.position_id,
                    asset: &t.asset,
                    side: t.side,
                    quantity: t.held.quantity,
                    mark_price: t.live_mark,
                    desired_stop_px: t.stop_px,
                    desired_tp_px: t.tp_px,
                    existing_sl_oid: t.held.protective_stop_oid.as_deref(),
                    existing_tp_oid: t.held.take_profit_oid.as_deref(),
                    replace_sl: t.replace_sl,
                    replace_tp: false,
                })
                .collect();
            let ws_open_orders = self
                .ws_cache
                .as_ref()
                .and_then(|c| c.get_fresh_open_orders())
                .filter(|s| s.metadata_complete);
            let batch_results = sync_tpsl_batch_for_positions(
                &self.http,
                &self.db,
                &reqs,
                None,
                ws_open_orders.as_ref(),
                &cfg,
            );
            for (idx, (pid, res)) in batch_results.into_iter().enumerate() {
                let sym = tpsl_syncs[idx].held.symbol.clone();
                match res {
                    Ok(rep) => {
                        if rep.sl_failed || rep.tp_failed {
                            report.tpsl_sync_errors += 1;
                            let err_joined = if rep.errors.is_empty() {
                                "TP/SL trigger leg sync failed".to_string()
                            } else {
                                rep.errors.join("; ")
                            };
                            Self::push_tick_error(
                                &mut report,
                                "tpsl_sync",
                                Some(pid),
                                Some(sym),
                                &err_joined,
                            );
                        }
                    }
                    Err(e) => {
                        report.tpsl_sync_errors += 1;
                        Self::push_tick_error(
                            &mut report,
                            "tpsl_sync",
                            Some(pid),
                            Some(sym),
                            &e.to_string(),
                        );
                    }
                }
            }
        }

        // Phase 5: Prune all tracking maps (`R14-L6`) & persist surveillance health (`R14-H3`)
        self.unfilled_breaches
            .lock()
            .retain(|pid, _| active_open_ids.contains(pid));
        self.last_confirmed_open_ms
            .lock()
            .retain(|pid, _| active_open_ids.contains(pid));
        self.passive_closes.prune_to_active(&active_open_ids);
        self.chaser.prune_tracking_maps(&active_working_ids);

        self.update_surveillance_health_and_persist(
            ts,
            &active_open_ids,
            &failed_position_ids,
            &mut report,
            &cfg,
        );

        Ok(report)
    }

    fn push_tick_error(
        report: &mut PriorityTickReport,
        scope: &str,
        position_id: Option<i64>,
        symbol: Option<String>,
        error: &str,
    ) {
        if report.errors.len() < MAX_TICK_ERROR_ITEMS {
            report.errors.push(PriorityTickErrorItem {
                scope: scope.to_string(),
                position_id,
                symbol,
                error: error.chars().take(240).collect(),
            });
        }
    }

    fn update_surveillance_health_and_persist(
        &self,
        ts: i64,
        active_open_ids: &HashSet<i64>,
        failed_position_ids: &HashSet<i64>,
        report: &mut PriorityTickReport,
        cfg: &ConfigMap,
    ) {
        let total_errs = report.working_order_errors
            + report.open_holding_errors
            + report.tpsl_sync_errors
            + usize::from(report.batch_update_failed);
        report.degraded = total_errs > 0;

        let threshold = cfg
            .get_i64("priority_unmonitored_alert_ticks", 10)
            .clamp(3, 300) as u32;
        let cooldown_ms = cfg
            .get_i64("chase_reject_cooldown_sec", 300)
            .clamp(30, 3600)
            * 1000;

        let unfilled_symbols: Vec<String> = {
            let guard = self.unfilled_breaches.lock();
            let mut syms: Vec<String> = guard.values().map(|v| v.symbol.clone()).collect();
            syms.sort();
            syms.dedup();
            syms
        };
        let mut failed_symbols: Vec<String> = report
            .errors
            .iter()
            .filter_map(|e| e.symbol.clone())
            .collect();
        failed_symbols.sort();
        failed_symbols.dedup();

        let (
            streak,
            active_unmonitored,
            emit_unmonitored,
            emit_recovered,
            should_persist,
            status_str,
        ) = {
            let mut hs = self.health_state.lock();
            hs.per_position_error_streaks
                .retain(|pid, _| active_open_ids.contains(pid));
            for &pid in active_open_ids {
                if failed_position_ids.contains(&pid) {
                    let entry = hs.per_position_error_streaks.entry(pid).or_insert(0);
                    *entry = entry.saturating_add(1);
                } else {
                    hs.per_position_error_streaks.remove(&pid);
                }
            }
            if report.degraded {
                hs.consecutive_error_ticks = hs.consecutive_error_ticks.saturating_add(1);
            } else {
                hs.consecutive_error_ticks = 0;
            }
            let max_pos_streak = hs
                .per_position_error_streaks
                .values()
                .copied()
                .max()
                .unwrap_or(0);
            let breached_threshold =
                hs.consecutive_error_ticks >= threshold || max_pos_streak >= threshold;
            let prev_active = hs.unmonitored_alert_active;

            let mut emit_unmonitored = false;
            let mut emit_recovered = false;
            if breached_threshold {
                hs.unmonitored_alert_active = true;
                if !prev_active || ts.saturating_sub(hs.last_alert_emitted_ms) >= cooldown_ms {
                    hs.last_alert_emitted_ms = ts;
                    emit_unmonitored = true;
                }
            } else if !report.degraded && prev_active {
                hs.unmonitored_alert_active = false;
                emit_recovered = true;
            }

            let status = if hs.unmonitored_alert_active {
                "CRITICAL".to_string()
            } else if report.degraded || !unfilled_symbols.is_empty() {
                "DEGRADED".to_string()
            } else {
                "HEALTHY".to_string()
            };

            let state_changed = status != hs.last_persisted_status
                || unfilled_symbols != hs.last_persisted_breaches;
            let heartbeat_due =
                ts.saturating_sub(hs.last_persisted_ms) >= HEALTH_PERSIST_HEARTBEAT_MS;
            let persist = state_changed || heartbeat_due;
            if persist {
                hs.last_persisted_status = status.clone();
                hs.last_persisted_breaches = unfilled_symbols.clone();
                hs.last_persisted_ms = ts;
            }
            (
                hs.consecutive_error_ticks,
                hs.unmonitored_alert_active,
                emit_unmonitored,
                emit_recovered,
                persist,
                status,
            )
        };

        report.consecutive_degraded_ticks = streak;

        if emit_unmonitored {
            let html = format!(
                "<b>[CRITICAL] PRIORITY MONITOR UNMONITORED</b><br/>Consecutive degraded ticks: <code>{}</code><br/>Failed symbols: <code>{}</code>",
                streak,
                if failed_symbols.is_empty() {
                    "global".to_string()
                } else {
                    failed_symbols.join(", ")
                }
            );
            let payload = json!({
                "consecutive_error_ticks": streak,
                "error_count": total_errs,
                "failed_symbols": failed_symbols,
                "timestamp_ms": ts,
            });
            self.enqueue_non_blocking_alert(
                "PRIORITY_MONITOR_UNMONITORED",
                "CRITICAL",
                &html,
                Some(&payload),
            );
            let _ = self.db.log_telemetry(
                "ERROR",
                "execution.priority_monitor",
                &format!("PriorityMonitor degraded for {streak} consecutive ticks"),
                Some(payload),
            );
        } else if emit_recovered {
            let html = "<b>[INFO] PRIORITY MONITOR RECOVERED</b><br/>1s surveillance loop returned to HEALTHY state.";
            self.enqueue_non_blocking_alert("PRIORITY_MONITOR_RECOVERED", "INFO", html, None);
        }

        if should_persist {
            let health_snap = PriorityMonitorHealth {
                last_tick_at_ms: ts,
                consecutive_error_ticks: streak,
                last_tick_error_count: total_errs,
                failed_symbols,
                unfilled_stop_breach_symbols: unfilled_symbols,
                unmonitored_alert_active: active_unmonitored,
                is_stale: false,
                status: status_str,
            };
            if let Ok(json_str) = serde_json::to_string(&health_snap) {
                let _ = self.db.set_config_value(
                    "priority_monitor_health_json",
                    &json_str,
                    Some("Cached 1s PriorityMonitor surveillance health snapshot JSON"),
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_pending_passive_close(
        &self,
        held: &HeldAssetView,
        asset_opt: Option<&ExchangeAsset>,
        side: OrderSide,
        surveillance_ref_px: f64,
        pending: &PendingPassiveClose,
        passive_close_rest_sec: i64,
        emergency_ioc_breach_pct: f64,
        shared_ch_state: Option<&Value>,
        risk: &RiskEngine,
        cfg: &ConfigMap,
    ) -> Result<bool> {
        let wallet = cfg.get_str("wallet_address", "");
        let pk = cfg.get_str("private_key", "");
        let ch_state = if let Some(s) = shared_ch_state {
            s.clone()
        } else if let Some(ws_ch) = self
            .ws_cache
            .as_ref()
            .and_then(|c| c.get_fresh_clearinghouse_state())
        {
            ws_ch
        } else {
            match self.http.fetch_clearinghouse_state(&wallet, cfg) {
                Ok(s) => s,
                Err(_) => return Ok(false),
            }
        };

        let live_szi = Self::extract_coin_szi(&ch_state, &held.coin);
        let lot = asset_opt.map(|a| a.lot_step_size).unwrap_or(0.01).max(1e-8);

        // 1. Resting passive maker order completely filled!
        if live_szi.abs() <= 1e-9 || live_szi.abs() <= lot * 0.5 {
            self.passive_closes.remove(held.position_id);
            let (eff_exit_price, fees_paid_usdc, live_close_oid) =
                self.resolve_exchange_flat_fill_details(held, side, pending.limit_px, risk, cfg);
            return self
                .commit_closed_position(
                    held,
                    side,
                    eff_exit_price,
                    fees_paid_usdc,
                    &pending.exit_reason,
                    live_close_oid.as_deref(),
                    risk,
                )
                .map(|opt| opt.is_some());
        }

        // 2. Partial maker fill -> immediately persist remainder (`R14-M3`)
        let live_abs = live_szi.abs();
        if live_abs + lot * 0.5 < pending.initial_qty {
            let closed_qty = (pending.initial_qty - live_abs).max(0.0);
            if closed_qty > 1e-9 {
                let partial_fee = round_dp(
                    pending.limit_px * closed_qty * (risk.limits.maker_fee_bps / 10_000.0),
                    4,
                );
                let _guard = acquire_execution_lock();
                if self
                    .db
                    .get_position_by_id(held.position_id)?
                    .is_some_and(|p| p.status.eq_ignore_ascii_case("OPEN"))
                {
                    let _ = self.db.record_partial_position_exit(
                        held.position_id,
                        closed_qty,
                        live_abs,
                        pending.limit_px,
                        partial_fee,
                        &pending.exit_reason,
                    );
                }
                self.passive_closes
                    .update_initial_qty(held.position_id, live_abs);
            }
        }

        // 3. Check expiry or emergency stop gap escalation
        let elapsed_ms = now_ms().saturating_sub(pending.placed_at_ms);
        let emergency_gap = !pending.is_tp
            && held.protective_stop_px > 0.0
            && (((held.protective_stop_px - surveillance_ref_px) * side.sign())
                / held.protective_stop_px)
                > emergency_ioc_breach_pct;

        if elapsed_ms < passive_close_rest_sec * 1000 && !emergency_gap {
            return Ok(false);
        }

        // Escalate: cancel resting passive order, re-verify live_szi, then run ImmediateIoc close ladder!
        if let Some(asset) = asset_opt {
            if let Some(oid) = pending.oid {
                let _ = self.http.cancel_orders_batch(
                    vec![CancelWire {
                        a: asset.asset_index,
                        o: oid,
                    }],
                    &pk,
                    Some(&self.db),
                    cfg,
                );
            }
        }
        self.passive_closes.remove(held.position_id);

        let fresh_ch = self
            .ws_cache
            .as_ref()
            .and_then(|c| c.get_fresh_clearinghouse_state())
            .or_else(|| self.http.fetch_clearinghouse_state(&wallet, cfg).ok())
            .unwrap_or(ch_state);
        self.execute_exit_with_urgency(
            held,
            asset_opt,
            side,
            surveillance_ref_px,
            &pending.exit_reason,
            CloseUrgency::ImmediateIoc,
            Some(&fresh_ch),
            risk,
            cfg,
        )
        .map(|opt| opt.is_some())
    }

    fn extract_coin_szi(ch_state: &Value, coin: &str) -> f64 {
        let Some(arr) = ch_state.get("assetPositions").and_then(Value::as_array) else {
            return 0.0;
        };
        for item in arr {
            let p = item.get("position").unwrap_or(item);
            let c = p.get("coin").and_then(Value::as_str).unwrap_or("").trim();
            if c.eq_ignore_ascii_case(coin) {
                return parse_hl_f64(p.get("szi"));
            }
        }
        0.0
    }

    /// Queries Hyperliquid `userFillsByTime` (falling back to `userFills`) using `last_confirmed_open_ms`
    /// or `opened_at_ms` (`R14-M4`), walking newest-to-oldest contiguous closing slices.
    fn resolve_exchange_flat_fill_details(
        &self,
        held: &HeldAssetView,
        side: OrderSide,
        fallback_exit_px: f64,
        risk: &RiskEngine,
        cfg: &ConfigMap,
    ) -> (f64, f64, Option<String>) {
        let now = now_ms();
        let wallet = cfg.get_str("wallet_address", "");
        let confirmed_ts = self
            .last_confirmed_open_ms
            .lock()
            .get(&held.position_id)
            .copied();
        let raw_start = confirmed_ts
            .or(held.opened_at_ms)
            .map(|t| t.saturating_sub(10_000))
            .unwrap_or_else(|| now.saturating_sub(EXCHANGE_FLAT_FILL_FALLBACK_LOOKBACK_MS));
        let min_bound = now.saturating_sub(EXCHANGE_FLAT_MAX_LOOKBACK_MS).max(0);
        let lookup_start_ms = raw_start.max(min_bound);

        let fills_res = self
            .http
            .fetch_user_fills_by_time(&wallet, lookup_start_ms, None, true, cfg)
            .or_else(|_| self.http.fetch_user_fills(&wallet, cfg));

        if let Ok(fills) = fills_res {
            let mut matching: Vec<(i64, f64, f64, f64, Option<u64>)> = Vec::new();
            for f in &fills {
                let fc = f.get("coin").and_then(Value::as_str).unwrap_or("").trim();
                if !fc.eq_ignore_ascii_case(&held.coin) {
                    continue;
                }
                let ft = parse_hl_i64_opt(f.get("time")).unwrap_or(0);
                if ft < lookup_start_ms {
                    continue;
                }
                let dir = f.get("dir").and_then(Value::as_str).unwrap_or("").trim();
                let f_side = f
                    .get("side")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_ascii_uppercase();
                let cpnl = parse_hl_f64(f.get("closedPnl"));

                let dir_matches = match side {
                    OrderSide::Long => {
                        dir.starts_with("Close Long")
                            || dir.starts_with("Long >")
                            || ((f_side == "A" || f_side == "SELL") && cpnl.abs() > 1e-9)
                            || (f_side.is_empty()
                                && (cpnl.abs() > 1e-9
                                    || dir.starts_with("Close")
                                    || dir.contains('>')))
                    }
                    OrderSide::Short => {
                        dir.starts_with("Close Short")
                            || dir.starts_with("Short >")
                            || ((f_side == "B" || f_side == "BUY") && cpnl.abs() > 1e-9)
                            || (f_side.is_empty()
                                && (cpnl.abs() > 1e-9
                                    || dir.starts_with("Close")
                                    || dir.contains('>')))
                    }
                };
                if !dir_matches {
                    continue;
                }
                let px = parse_hl_f64(f.get("px"));
                let sz = parse_hl_f64(f.get("sz"));
                let fee = parse_hl_f64(f.get("fee"));
                if px > 0.0 && sz > 0.0 {
                    let oid = parse_hl_u64_opt(f.get("oid"));
                    matching.push((ft, px, sz, fee, oid));
                }
            }

            // Sort newest-to-oldest and accumulate only contiguous slices (gap <= 60s)
            matching.sort_by_key(|a| std::cmp::Reverse(a.0));
            let mut sz_sum = 0.0_f64;
            let mut ntl_sum = 0.0_f64;
            let mut fee_sum = 0.0_f64;
            let mut last_oid: Option<u64> = None;
            let mut prev_ts: Option<i64> = None;

            for (ft, px, sz, fee, oid) in matching {
                if let Some(pts) = prev_ts {
                    if pts.saturating_sub(ft) > EXCHANGE_FLAT_SLICE_MAX_GAP_MS {
                        break;
                    }
                }
                prev_ts = Some(ft);
                sz_sum += sz;
                ntl_sum += px * sz;
                fee_sum += fee;
                if last_oid.is_none() {
                    last_oid = oid;
                }
                if sz_sum >= held.quantity * 0.99 {
                    break;
                }
            }

            if sz_sum > 0.0 {
                return (
                    round_dp(ntl_sum / sz_sum, 6),
                    round_dp(fee_sum, 4),
                    last_oid.map(|id| format!("hl-{id}")),
                );
            }
        }

        tracing::warn!(
            position_id = held.position_id,
            symbol = %held.symbol,
            lookup_start_ms,
            "userFillsByTime returned no matching closing slice for exchange-flat position; estimating taker fee fallback"
        );
        let est_fee = round_dp(
            fallback_exit_px * held.quantity * (risk.limits.taker_fee_bps / 10_000.0),
            4,
        );
        (fallback_exit_px, est_fee, None)
    }

    /// Executes an immediate protective stop or take-profit exit (`ImmediateIoc`), preserving
    /// backwards compatibility with direct callers.
    pub fn execute_protective_stop_exit(
        &self,
        held: &HeldAssetView,
        exit_price: f64,
        exit_reason: &str,
        risk: &RiskEngine,
    ) -> Result<Option<f64>> {
        let (cfg, asset_opt, side) = {
            let _guard = acquire_execution_lock();
            let cfg = self.db.load_config()?;
            let asset_opt = self.db.get_asset_by_id(held.asset_id)?;
            let side = OrderSide::parse(&held.side)?;
            (cfg, asset_opt, side)
        };
        self.execute_exit_with_urgency(
            held,
            asset_opt.as_ref(),
            side,
            exit_price,
            exit_reason,
            CloseUrgency::ImmediateIoc,
            None,
            risk,
            &cfg,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_exit_with_urgency(
        &self,
        held: &HeldAssetView,
        asset_opt: Option<&ExchangeAsset>,
        side: OrderSide,
        exit_price: f64,
        exit_reason: &str,
        urgency: CloseUrgency,
        pre_fetched_ch_state: Option<&Value>,
        risk: &RiskEngine,
        cfg: &ConfigMap,
    ) -> Result<Option<f64>> {
        let is_tp_exit = exit_reason.starts_with("TAKE_PROFIT");
        let taker_fee_bps = risk.limits.taker_fee_bps;
        let maker_fee_bps = risk.limits.maker_fee_bps;
        let mut fees_paid_usdc =
            round_dp(exit_price * held.quantity * (taker_fee_bps / 10_000.0), 4);
        let mut eff_exit_price = exit_price;
        let mut live_close_oid: Option<String> = None;

        let is_live_exchange = crate::engine::is_live_exchange_active(&self.db, cfg);

        if is_live_exchange {
            let pk = cfg.get_str("private_key", "");
            let wallet = cfg.get_str("wallet_address", "");
            if wallet.trim().is_empty() || pk.trim().is_empty() {
                tracing::error!(
                    position_id = held.position_id,
                    symbol = %held.symbol,
                    wallet_empty = wallet.trim().is_empty(),
                    pk_empty = pk.trim().is_empty(),
                    "wallet_address or private_key is empty in live mode; skipping live exit"
                );
                return Ok(None);
            }

            let ch_state = if let Some(s) = pre_fetched_ch_state {
                s.clone()
            } else if let Some(ws_ch) = self
                .ws_cache
                .as_ref()
                .and_then(|c| c.get_fresh_clearinghouse_state())
            {
                ws_ch
            } else {
                match self.http.fetch_clearinghouse_state(&wallet, cfg) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(
                            position_id = held.position_id,
                            symbol = %held.symbol,
                            error = %e,
                            "clearinghouseState unreachable during exit; skipping DB close"
                        );
                        return Ok(None);
                    }
                }
            };

            let live_szi = Self::extract_coin_szi(&ch_state, &held.coin);

            if live_szi.abs() <= 1e-9 {
                let (fx, fee, oid) =
                    self.resolve_exchange_flat_fill_details(held, side, exit_price, risk, cfg);
                eff_exit_price = fx;
                fees_paid_usdc = fee;
                live_close_oid = oid;
            } else if let Some(asset) = asset_opt {
                let l2_bbo = self
                    .ws_cache
                    .as_ref()
                    .and_then(|c| c.get_fresh_l2_bbo(&asset.coin))
                    .or_else(|| {
                        self.http
                            .fetch_l2_book_critical(&asset.coin, cfg)
                            .ok()
                            .flatten()
                    });

                // Check 15s unfilled breach retry backoff unless fresh book is crossable inside oracle band (`R14-H5`)
                let now = now_ms();
                if let Some(prev) = self
                    .unfilled_breaches
                    .lock()
                    .get(&held.position_id)
                    .cloned()
                {
                    let in_backoff =
                        now.saturating_sub(prev.last_attempt_ms) < UNFILLED_BREACH_RETRY_BACKOFF_MS;
                    if in_backoff
                        && !Self::bbo_crossable_inside_band(
                            asset,
                            live_szi < 0.0,
                            l2_bbo,
                            exit_price,
                        )
                    {
                        return Ok(None);
                    }
                }

                // P1-Strat: Attempt non-blocking maker-first passive close when urgency == PassiveFirst
                let mut effective_live_szi = live_szi;
                let mut already_filled_passively = false;
                if urgency == CloseUrgency::PassiveFirst {
                    let tp_cap = is_tp_exit
                        .then_some(held.take_profit_px)
                        .filter(|&p| p > 0.0);
                    match place_passive_reduce_only_close_order(
                        &self.http,
                        Some(&self.db),
                        &pk,
                        cfg,
                        asset,
                        held.position_id,
                        live_szi,
                        exit_price,
                        l2_bbo,
                        maker_fee_bps,
                        tp_cap,
                        if is_tp_exit { "v2-tp" } else { "v2-passive" },
                    ) {
                        Ok(PassiveClosePlacement::Filled {
                            fill_px,
                            filled_qty,
                            fee_usdc,
                            eoid,
                        }) => {
                            let lot = asset.lot_step_size.max(1e-8);
                            let rem = (live_szi.abs() - filled_qty).max(0.0);
                            if rem <= lot * 0.5 || rem <= 1e-9 {
                                eff_exit_price = fill_px;
                                fees_paid_usdc = round_dp(fee_usdc, 4);
                                live_close_oid = eoid;
                                already_filled_passively = true;
                            } else {
                                let _guard = acquire_execution_lock();
                                if self
                                    .db
                                    .get_position_by_id(held.position_id)?
                                    .is_some_and(|p| p.status.eq_ignore_ascii_case("OPEN"))
                                {
                                    let _ = self.db.record_partial_position_exit(
                                        held.position_id,
                                        filled_qty,
                                        rem,
                                        fill_px,
                                        round_dp(fee_usdc, 4),
                                        exit_reason,
                                    );
                                }
                                effective_live_szi = if live_szi < 0.0 { -rem } else { rem };
                            }
                        }
                        Ok(PassiveClosePlacement::Resting {
                            oid,
                            eoid,
                            cloid,
                            limit_px,
                        }) => {
                            self.passive_closes.insert(PendingPassiveClose {
                                position_id: held.position_id,
                                asset_id: held.asset_id,
                                oid,
                                eoid,
                                cloid,
                                limit_px,
                                initial_qty: live_szi.abs(),
                                placed_at_ms: now,
                                exit_reason: exit_reason.to_string(),
                                is_tp: is_tp_exit,
                            });
                            return Ok(None);
                        }
                        Ok(PassiveClosePlacement::Rejected(_)) | Err(_) => {
                            // Fall through immediately to ImmediateIoc close ladder
                        }
                    }
                }

                if !already_filled_passively {
                    let open_orders_snap = self
                        .ws_cache
                        .as_ref()
                        .and_then(|c| c.get_fresh_open_orders())
                        .filter(|s| s.metadata_complete)
                        .or_else(|| self.http.fetch_open_orders_with_metadata(&wallet, cfg).ok());
                    let cloid_prefix = if is_tp_exit {
                        "v2-tpexit"
                    } else {
                        "v2-stopexit"
                    };
                    let ladder_res = submit_reduce_only_close_ladder(
                        &self.http,
                        Some(&self.db),
                        &pk,
                        cfg,
                        &ReduceOnlyCloseLadderRequest {
                            asset,
                            position_id: Some(held.position_id),
                            live_szi: effective_live_szi,
                            reference_mark_px: exit_price,
                            l2_bbo,
                            taker_fee_bps,
                            cloid_prefix,
                            max_slippage_pct: None,
                            allow_passive_fallback: true,
                            existing_open_orders: open_orders_snap.as_ref(),
                        },
                    )?;

                    if ladder_res.fully_closed {
                        if let Some(vwap) = ladder_res.vwap_fill_price {
                            eff_exit_price = vwap;
                            fees_paid_usdc = ladder_res.total_fee_usdc;
                        }
                        live_close_oid = ladder_res.last_fill_eoid;

                        let mut cancels = Vec::new();
                        if let Some(oid) = parse_hl_oid(held.protective_stop_oid.as_deref()) {
                            cancels.push(CancelWire {
                                a: asset.asset_index,
                                o: oid,
                            });
                        }
                        if let Some(oid) = parse_hl_oid(held.take_profit_oid.as_deref()) {
                            cancels.push(CancelWire {
                                a: asset.asset_index,
                                o: oid,
                            });
                        }
                        if !cancels.is_empty() {
                            let _ =
                                self.http
                                    .cancel_orders_batch(cancels, &pk, Some(&self.db), cfg);
                        }
                    } else {
                        // R14-M3: Immediately persist any partial stop-exit fill!
                        if ladder_res.filled_qty > 1e-9 && ladder_res.remaining_qty > 1e-9 {
                            let partial_px = ladder_res.vwap_fill_price.unwrap_or(exit_price);
                            let _guard = acquire_execution_lock();
                            if self
                                .db
                                .get_position_by_id(held.position_id)?
                                .is_some_and(|p| p.status.eq_ignore_ascii_case("OPEN"))
                            {
                                let _ = self.db.record_partial_position_exit(
                                    held.position_id,
                                    ladder_res.filled_qty,
                                    ladder_res.remaining_qty,
                                    partial_px,
                                    ladder_res.total_fee_usdc,
                                    exit_reason,
                                );
                            }
                        }

                        // R14-H5: NEVER widen protective_stop_px or take_profit_px!
                        // Keep position OPEN with resting passive reduceOnly limit & alert operator.
                        self.record_unfilled_breach_and_alert(
                            held,
                            exit_price,
                            ladder_res.remaining_qty,
                            is_tp_exit,
                            ladder_res.resting_passive_oid,
                            ladder_res.resting_passive_px,
                        );
                        return Ok(None);
                    }
                }
            }
        }

        self.commit_closed_position(
            held,
            side,
            eff_exit_price,
            fees_paid_usdc,
            exit_reason,
            live_close_oid.as_deref(),
            risk,
        )
    }

    fn bbo_crossable_inside_band(
        asset: &ExchangeAsset,
        close_is_buy: bool,
        l2_bbo: Option<(f64, f64)>,
        ref_mark: f64,
    ) -> bool {
        let Some((bid, ask)) = l2_bbo else {
            return false;
        };
        let anchor = if asset.oracle_price > 0.0 {
            asset.oracle_price
        } else {
            ref_mark
        };
        if anchor <= 0.0 {
            return false;
        }
        let band_lo = anchor * (1.0 - aegis_core::HL_ORACLE_BAND_PCT);
        let band_hi = anchor * (1.0 + aegis_core::HL_ORACLE_BAND_PCT);
        if close_is_buy {
            ask > 0.0 && ask <= band_hi
        } else {
            bid > 0.0 && bid >= band_lo
        }
    }

    fn record_unfilled_breach_and_alert(
        &self,
        held: &HeldAssetView,
        mark_px: f64,
        remaining_qty: f64,
        is_tp: bool,
        resting_passive_oid: Option<String>,
        resting_passive_px: Option<f64>,
    ) {
        let now = now_ms();
        let (should_alert, first_breach_ms, attempt_count, entry_is_tp) = {
            let mut guard = self.unfilled_breaches.lock();
            let entry = guard
                .entry(held.position_id)
                .or_insert_with(|| UnfilledBreachState {
                    first_breach_ms: now,
                    last_attempt_ms: 0,
                    passive_close_oid: None,
                    passive_close_px: None,
                    attempt_count: 0,
                    last_alert_ms: 0,
                    is_tp,
                    symbol: held.symbol.clone(),
                });
            entry.last_attempt_ms = now;
            entry.attempt_count = entry.attempt_count.saturating_add(1);
            entry.is_tp = is_tp;
            if resting_passive_oid.is_some() {
                entry.passive_close_oid = resting_passive_oid.clone();
            }
            if resting_passive_px.is_some() {
                entry.passive_close_px = resting_passive_px;
            }
            let emit = entry.last_alert_ms == 0
                || now.saturating_sub(entry.last_alert_ms) >= UNFILLED_BREACH_ALERT_THROTTLE_MS;
            if emit {
                entry.last_alert_ms = now;
            }
            (
                emit,
                entry.first_breach_ms,
                entry.attempt_count,
                entry.is_tp,
            )
        };

        if should_alert {
            let event_type = if entry_is_tp {
                "TP_BREACH_UNFILLED_PASSIVE_RESTING"
            } else {
                "STOP_BREACH_UNFILLED_PASSIVE_RESTING"
            };
            let breach_age_sec = now.saturating_sub(first_breach_ms) / 1000;
            let passive_oid_str = resting_passive_oid.as_deref().unwrap_or("none");
            let passive_px_str = resting_passive_px
                .map(|p| format!("${p:.6}"))
                .unwrap_or_else(|| "n/a".to_string());
            let html = format!(
                "<b>[CRITICAL] {} ({})</b><br/>Side: <code>{}</code> | Stop: <code>${:.6}</code> | Mark: <code>${:.6}</code><br/>Remaining Qty: <code>{:.6}</code> | Resting Passive Limit: <code>{}</code> (OID: <code>{}</code>) | Attempts: <code>{}</code> (age {}s)<br/>Original stop preserved untouched; retrying on 15s backoff.",
                event_type,
                held.symbol,
                held.side,
                held.protective_stop_px,
                mark_px,
                remaining_qty,
                passive_px_str,
                passive_oid_str,
                attempt_count,
                breach_age_sec
            );
            let payload = json!({
                "position_id": held.position_id,
                "symbol": held.symbol,
                "side": held.side,
                "is_tp": entry_is_tp,
                "first_breach_ms": first_breach_ms,
                "breach_age_sec": breach_age_sec,
                "attempt_count": attempt_count,
                "protective_stop_px": held.protective_stop_px,
                "mark_price": mark_px,
                "remaining_qty": remaining_qty,
                "resting_passive_oid": resting_passive_oid,
                "resting_passive_px": resting_passive_px,
            });
            self.enqueue_non_blocking_alert(event_type, "CRITICAL", &html, Some(&payload));
            let _ = self.db.log_telemetry(
                "ERROR",
                "execution.priority_monitor",
                &format!(
                    "{} on {} (pos #{}, attempt #{}, age {}s): rem={:.6}, stop=${:.6} preserved untouched, passive={} ({})",
                    event_type,
                    held.symbol,
                    held.position_id,
                    attempt_count,
                    breach_age_sec,
                    remaining_qty,
                    held.protective_stop_px,
                    passive_px_str,
                    passive_oid_str
                ),
                Some(payload),
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_closed_position(
        &self,
        held: &HeldAssetView,
        side: OrderSide,
        eff_exit_price: f64,
        fees_paid_usdc: f64,
        exit_reason: &str,
        live_close_oid: Option<&str>,
        risk: &RiskEngine,
    ) -> Result<Option<f64>> {
        let is_tp_exit = exit_reason.starts_with("TAKE_PROFIT");
        let is_stagnation_exit = exit_reason.starts_with("STAGNATION_TIME_STOP");
        let (realized_pnl, cd_reason) = {
            let _guard = acquire_execution_lock();
            if !self
                .db
                .get_position_by_id(held.position_id)?
                .is_some_and(|p| p.status.eq_ignore_ascii_case("OPEN"))
            {
                return Ok(None);
            }
            let realized_pnl = self.db.close_position(
                held.position_id,
                eff_exit_price,
                fees_paid_usdc,
                exit_reason,
            )?;

            if let Some(eoid) = live_close_oid {
                let oid_num = eoid.strip_prefix("hl-").unwrap_or(eoid);
                let coid = format!("hl-closed-{}-{oid_num}", held.coin.to_ascii_lowercase());
                let _ = self
                    .db
                    .set_position_closed_exchange_ids(held.position_id, &coid, eoid);
            }

            let (cooldown_type, cooldown_sec, cd_reason) = if is_tp_exit {
                (
                    "CHURN_GUARD",
                    300,
                    format!(
                        "Take-profit target triggered on {} at ${:.4} (entry=${:.4}, PnL=${:+.2})",
                        held.symbol, eff_exit_price, held.entry_price, realized_pnl
                    ),
                )
            } else if is_stagnation_exit {
                (
                    "CHURN_GUARD",
                    600,
                    format!(
                        "Stagnation time-stop closed {} at ${:.4} (entry=${:.4}, PnL=${:+.2})",
                        held.symbol, eff_exit_price, held.entry_price, realized_pnl
                    ),
                )
            } else {
                (
                    "STOP_OUT",
                    risk.limits.stop_out_cooldown_sec.max(60),
                    format!(
                        "Protective stop triggered on {} at ${:.4} (entry=${:.4}, PnL=${:+.2})",
                        held.symbol, eff_exit_price, held.entry_price, realized_pnl
                    ),
                )
            };
            self.db.insert_asset_cooldown(
                held.asset_id,
                cooldown_type,
                &cd_reason,
                cooldown_sec,
            )?;

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
                .map(|s| {
                    (s.available_cash_usdc + held.committed_notional_usdc + realized_pnl).max(0.0)
                })
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
                cooldown_type: Some(cooldown_type.to_string()),
                cooldown_seconds: cooldown_sec,
                timestamp_ms: now_ms(),
            };
            let html = format_position_closed_html(&alert);
            let payload = serde_json::to_value(&alert).ok();
            self.enqueue_non_blocking_alert(
                "POSITION_CLOSED_COOLDOWN",
                "WARNING",
                &html,
                payload.as_ref(),
            );
            (realized_pnl, cd_reason)
        };
        let _ = cd_reason;
        Ok(Some(realized_pnl))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_db::{AssetUpsertInput, BarUpsertInput, OidUpdate};
    use aegis_test_support::mock_http::{MockHttpResponse, MockHttpServer};

    #[test]
    fn test_resolve_stop_sync_decision_gates_sub_25bps_db_writes() {
        for small_stop in [0.050349, 0.050250] {
            let dec = resolve_stop_sync_decision(0.050351, small_stop, true, false, false);
            assert_eq!(dec.persisted_stop_px, 0.050351);
            assert!(!dec.sl_ratcheted_materially);
            assert!(!dec.should_sync_tpsl);
            assert!(!dec.count_ratchet);

            let dec_missing_tp =
                resolve_stop_sync_decision(0.050351, small_stop, true, false, true);
            assert_eq!(dec_missing_tp.persisted_stop_px, 0.050351);
            assert!(!dec_missing_tp.sl_ratcheted_materially);
            assert!(dec_missing_tp.should_sync_tpsl);
            assert!(!dec_missing_tp.count_ratchet);
        }

        let material = resolve_stop_sync_decision(0.050351, 0.050220, true, false, false);
        assert_eq!(material.persisted_stop_px, 0.050220);
        assert!(material.sl_ratcheted_materially);
        assert!(material.should_sync_tpsl);
        assert!(material.count_ratchet);

        let attach = resolve_stop_sync_decision(0.0, 0.049, false, false, false);
        assert_eq!(attach.persisted_stop_px, 0.049);
        assert!(!attach.sl_ratcheted_materially);
        assert!(attach.should_sync_tpsl);
        assert!(attach.count_ratchet);
    }

    #[test]
    fn test_priority_tick_sub_25bps_ratchet_preserves_db_stop_and_skips_hl_replace() {
        let stop_trigger_px = Arc::new(Mutex::new("1720.0".to_string()));
        let stop_trigger_srv = Arc::clone(&stop_trigger_px);
        let mid_px = Arc::new(Mutex::new("1734.0".to_string()));
        let mid_px_srv = Arc::clone(&mid_px);
        let exchange_requests = Arc::new(Mutex::new(Vec::<Value>::new()));
        let exchange_srv = Arc::clone(&exchange_requests);

        let srv = MockHttpServer::start(move |req| {
            if req.path == "/info" {
                let typ = req
                    .json_body
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                match typ {
                    "allMids" => {
                        MockHttpResponse::ok_json(&json!({ "ETH": mid_px_srv.lock().clone() }))
                    }
                    "frontendOpenOrders" | "openOrders" => MockHttpResponse::ok_json(&json!([
                        {
                            "coin": "ETH",
                            "oid": 90001u64,
                            "isTrigger": true,
                            "orderType": "Stop Limit",
                            "triggerPx": stop_trigger_srv.lock().clone(),
                            "sz": "0.0253"
                        },
                        {
                            "coin": "ETH",
                            "oid": 90002u64,
                            "isTrigger": true,
                            "orderType": "Take Profit Limit",
                            "triggerPx": "1831.6",
                            "sz": "0.0253"
                        }
                    ])),
                    _ => MockHttpResponse::ok_json(&json!([])),
                }
            } else {
                exchange_srv.lock().push(req.json_body.clone());
                if req
                    .json_body
                    .pointer("/action/type")
                    .and_then(Value::as_str)
                    == Some("order")
                {
                    MockHttpResponse::ok_json(&json!({
                        "status": "ok",
                        "response": {
                            "type": "order",
                            "data": {
                                "statuses": [{ "waitingForTrigger": { "oid": 90003u64 } }]
                            }
                        }
                    }))
                } else {
                    MockHttpResponse::ok_json(&json!({ "status": "ok" }))
                }
            }
        });

        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let http = HyperliquidHttpClient::new(5);
        let alerts = AlertEngine::new(db.clone());
        let chaser = OrderChaser::new(db.clone(), http.clone(), alerts.clone());
        let monitor = PriorityMonitor::new(db.clone(), http, chaser, alerts);

        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_testnet_url", &srv.base_url(), None)
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

        let eth_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "ETH".to_string(),
                asset_index: 1,
                sz_decimals: 3,
                tick_size: 0.1,
                lot_step_size: 0.001,
                max_leverage: 25.0,
                mark_price: 1734.0,
                mid_price: 1734.0,
                oracle_price: 1734.0,
                funding_rate_hourly: 0.0000125,
                day_volume_usdc: 20_000_000.0,
                open_interest_usdc: 8_000_000.0,
                prev_day_price: 1700.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();

        let base_ts = now_ms() - 20 * 14_400_000;
        let bars: Vec<BarUpsertInput> = (0..20)
            .map(|i| BarUpsertInput {
                asset_id: eth_id,
                timeframe: "4h".to_string(),
                timestamp_ms: base_ts + i * 14_400_000,
                open: 1718.0,
                high: 1724.0,
                low: 1712.0,
                close: 1718.0,
                volume: 1000.0,
                funding_rate: 0.00001,
            })
            .collect();
        db.upsert_ohlcv_bars(&bars).unwrap();

        let (pos_id, _) = db
            .sync_open_position_from_exchange_v2(&aegis_db::ExchangeOpenPositionSyncInput {
                asset_id: eth_id,
                coin: "ETH",
                side: OrderSide::Long,
                entry_price: 1718.0,
                mark_price: 1734.0,
                quantity: 0.0253,
                committed_notional_usdc: 0.0253 * 1734.0,
                unrealized_pnl_usdc: 0.0,
                portfolio_weight: 0.05,
                protective_stop_px: 1720.0,
                take_profit_px: 1831.6,
                funding_accrued_usdc: 0.0,
                opened_at_ms: Some(now_ms() - 600_000),
            })
            .unwrap();
        db.update_position_tpsl_orders(
            pos_id,
            1720.0,
            OidUpdate::Set("hl-90001"),
            1831.6,
            OidUpdate::Set("hl-90002"),
        )
        .unwrap();

        let tick1 = monitor.run_1s_priority_tick().unwrap();
        assert_eq!(tick1.stops_ratcheted, 0);
        let pos1 = db.get_position_by_id(pos_id).unwrap().unwrap();
        assert!((pos1.protective_stop_px - 1720.0).abs() < 1e-6);
        assert!((pos1.highest_price_seen - 1734.0).abs() < 1e-6);
        assert!(exchange_requests.lock().is_empty());

        *mid_px.lock() = "1746.0".to_string();
        db.upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: "ETH".to_string(),
            asset_index: 1,
            sz_decimals: 3,
            tick_size: 0.1,
            lot_step_size: 0.001,
            max_leverage: 25.0,
            mark_price: 1746.0,
            mid_price: 1746.0,
            oracle_price: 1746.0,
            funding_rate_hourly: 0.0000125,
            day_volume_usdc: 20_000_000.0,
            open_interest_usdc: 8_000_000.0,
            prev_day_price: 1700.0,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: true,
        })
        .unwrap();
        let tick2 = monitor.run_1s_priority_tick().unwrap();
        assert_eq!(tick2.stops_ratcheted, 1);
        let pos2 = db.get_position_by_id(pos_id).unwrap().unwrap();
        assert!((pos2.protective_stop_px - 1731.4).abs() < 1e-6);
        assert!((pos2.highest_price_seen - 1746.0).abs() < 1e-6);
        assert_eq!(pos2.protective_stop_oid.as_deref(), Some("hl-90003"));
        let exch_reqs = exchange_requests.lock();
        assert!(exch_reqs
            .iter()
            .any(|r| r.pointer("/action/type").and_then(Value::as_str) == Some("order")));
    }

    #[test]
    fn test_r14_h5_unfilled_stop_breach_never_widens_stop_rests_passive_limit_and_alerts() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let bnb_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "BNB".to_string(),
                asset_index: 5,
                sz_decimals: 2,
                tick_size: 0.1,
                lot_step_size: 0.01,
                max_leverage: 15.0,
                mark_price: 580.0,
                mid_price: 580.0,
                oracle_price: 580.0,
                funding_rate_hourly: 0.0,
                day_volume_usdc: 10_000_000.0,
                open_interest_usdc: 5_000_000.0,
                prev_day_price: 600.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();

        let (pos_id, _) = db
            .sync_open_position_from_exchange_v2(&aegis_db::ExchangeOpenPositionSyncInput {
                asset_id: bnb_id,
                coin: "BNB",
                side: OrderSide::Long,
                entry_price: 600.0,
                mark_price: 580.0,
                quantity: 1.0,
                committed_notional_usdc: 600.0,
                unrealized_pnl_usdc: -20.0,
                portfolio_weight: 0.1,
                protective_stop_px: 585.0,
                take_profit_px: 640.0,
                funding_accrued_usdc: 0.0,
                opened_at_ms: Some(now_ms() - 1_200_000),
            })
            .unwrap();

        // Completely illiquid book (bid=500 << band_lo) where IOC never matches and passive GTC rests
        let srv = MockHttpServer::start(|req| {
            if req.path == "/info" {
                match req
                    .json_body
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                {
                    "allMids" => MockHttpResponse::ok_json(&json!({ "BNB": "580.0" })),
                    "clearinghouseState" => MockHttpResponse::ok_json(&json!({
                        "assetPositions": [{ "position": { "coin": "BNB", "szi": "1.0", "entryPx": "600.0" } }]
                    })),
                    "l2Book" => MockHttpResponse::ok_json(&json!({
                        "levels": [
                            [{ "px": "500.0", "sz": "1.0" }],
                            [{ "px": "660.0", "sz": "1.0" }]
                        ]
                    })),
                    _ => MockHttpResponse::ok_json(&json!([])),
                }
            } else {
                let tif = req
                    .json_body
                    .pointer("/action/orders/0/t/limit/tif")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if tif == "Ioc" {
                    MockHttpResponse::ok_json(&json!({
                        "status": "ok",
                        "response": { "type": "order", "data": { "statuses": [{ "error": "Order could not immediately match" }] } }
                    }))
                } else {
                    MockHttpResponse::ok_json(&json!({
                        "status": "ok",
                        "response": { "type": "order", "data": { "statuses": [{ "resting": { "oid": 77111u64 } }] } }
                    }))
                }
            }
        });

        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_testnet_url", &srv.base_url(), None)
            .unwrap();
        db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
            .unwrap();
        db.set_config_value("passive_close_rest_sec", "0", None)
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

        let http = HyperliquidHttpClient::new(5);
        let alerts = AlertEngine::new(db.clone());
        let chaser = OrderChaser::new(db.clone(), http.clone(), alerts.clone());
        let monitor = PriorityMonitor::new(db.clone(), http, chaser, alerts);

        let rep = monitor.run_1s_priority_tick().unwrap();
        assert_eq!(rep.stops_triggered_closed, 0);

        // Original protective_stop_px MUST remain untouched at 585.0 (NEVER widened!)
        let pos_after = db.get_position_by_id(pos_id).unwrap().unwrap();
        assert_eq!(pos_after.status, "OPEN");
        assert!((pos_after.protective_stop_px - 585.0).abs() < 1e-6);

        // Health JSON records unfilled_stop_breach_symbols and CRITICAL alert was enqueued
        let health_json = db
            .load_config()
            .unwrap()
            .get_str("priority_monitor_health_json", "")
            .to_string();
        let health: PriorityMonitorHealth = serde_json::from_str(&health_json).unwrap();
        assert_eq!(
            health.unfilled_stop_breach_symbols,
            vec!["BNB-USDC".to_string()]
        );
    }

    #[test]
    fn test_r14_m3_m4_partial_stop_exit_and_widened_user_fills_lookup_beyond_120s() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let sol_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "SOL".to_string(),
                asset_index: 3,
                sz_decimals: 2,
                tick_size: 0.01,
                lot_step_size: 0.01,
                max_leverage: 20.0,
                mark_price: 95.0,
                mid_price: 95.0,
                oracle_price: 95.0,
                funding_rate_hourly: 0.0,
                day_volume_usdc: 10_000_000.0,
                open_interest_usdc: 5_000_000.0,
                prev_day_price: 100.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();

        let opened_ts = now_ms() - 600_000;
        let fill_ts = now_ms() - 300_000; // 5 minutes ago (> 120s old!)
        let (pos_id, _) = db
            .sync_open_position_from_exchange_v2(&aegis_db::ExchangeOpenPositionSyncInput {
                asset_id: sol_id,
                coin: "SOL",
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 95.0,
                quantity: 10.0,
                committed_notional_usdc: 1000.0,
                unrealized_pnl_usdc: -50.0,
                portfolio_weight: 0.1,
                protective_stop_px: 96.0,
                take_profit_px: 110.0,
                funding_accrued_usdc: 0.0,
                opened_at_ms: Some(opened_ts),
            })
            .unwrap();

        let scenario = Arc::new(Mutex::new("partial_exit"));
        let scenario_srv = Arc::clone(&scenario);
        let srv = MockHttpServer::start(move |req| {
            let mode = *scenario_srv.lock();
            if req.path == "/info" {
                match req
                    .json_body
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                {
                    "allMids" => MockHttpResponse::ok_json(&json!({ "SOL": "95.0" })),
                    "clearinghouseState" => {
                        if mode == "partial_exit" {
                            MockHttpResponse::ok_json(&json!({
                                "assetPositions": [{ "position": { "coin": "SOL", "szi": "10.0", "entryPx": "100.0" } }]
                            }))
                        } else {
                            MockHttpResponse::ok_json(&json!({ "assetPositions": [] }))
                        }
                    }
                    "l2Book" => MockHttpResponse::ok_json(&json!({
                        "levels": [
                            [{ "px": "95.0", "sz": "6.0" }],
                            [{ "px": "95.1", "sz": "6.0" }]
                        ]
                    })),
                    "userFillsByTime" => MockHttpResponse::ok_json(&json!([
                        { "coin": "SOL", "time": fill_ts - 180_000, "dir": "Close Long", "px": "80.0", "sz": "4.0", "fee": "1.0", "oid": 100u64 },
                        { "coin": "SOL", "time": fill_ts, "dir": "Close Long", "px": "94.50", "sz": "4.0", "fee": "0.17", "oid": 200u64 }
                    ])),
                    _ => MockHttpResponse::ok_json(&json!([])),
                }
            } else {
                let tif = req
                    .json_body
                    .pointer("/action/orders/0/t/limit/tif")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if tif == "Ioc" && mode == "partial_exit" {
                    // Fill 6.0 of 10.0 on first IOC, then reject subsequent rungs
                    let sz = req
                        .json_body
                        .pointer("/action/orders/0/s")
                        .and_then(Value::as_str)
                        .unwrap_or("0");
                    if sz == "10" {
                        MockHttpResponse::ok_json(&json!({
                            "status": "ok",
                            "response": { "type": "order", "data": { "statuses": [{ "filled": { "oid": 300u64, "totalSz": "6.0", "avgPx": "95.0" } }] } }
                        }))
                    } else {
                        MockHttpResponse::ok_json(&json!({
                            "status": "ok",
                            "response": { "type": "order", "data": { "statuses": [{ "error": "Order could not immediately match" }] } }
                        }))
                    }
                } else {
                    MockHttpResponse::ok_json(&json!({
                        "status": "ok",
                        "response": { "type": "order", "data": { "statuses": [{ "resting": { "oid": 301u64 } }] } }
                    }))
                }
            }
        });

        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_testnet_url", &srv.base_url(), None)
            .unwrap();
        db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
            .unwrap();
        db.set_config_value("passive_close_rest_sec", "0", None)
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

        let http = HyperliquidHttpClient::new(5);
        let alerts = AlertEngine::new(db.clone());
        let chaser = OrderChaser::new(db.clone(), http.clone(), alerts.clone());
        let monitor = PriorityMonitor::new(db.clone(), http, chaser, alerts);

        // 1. R14-M3: Partial stop exit persists remaining 4.0 qty immediately
        monitor.run_1s_priority_tick().unwrap();
        let pos_partial = db.get_position_by_id(pos_id).unwrap().unwrap();
        assert_eq!(pos_partial.status, "OPEN");
        assert!((pos_partial.filled_quantity - 4.0).abs() < 1e-6);

        // 2. R14-M4: Remainder fills on exchange 300s ago (> 120s); contiguous slice resolves exact 94.50 + 0.17 fee
        *scenario.lock() = "exchange_flat";
        monitor.unfilled_breaches.lock().clear();
        monitor.last_confirmed_open_ms.lock().clear();
        let rep2 = monitor.run_1s_priority_tick().unwrap();
        assert_eq!(rep2.stops_triggered_closed, 1);
        let pos_closed = db.get_position_by_id(pos_id).unwrap().unwrap();
        assert_eq!(pos_closed.status, "CLOSED");
        assert!((pos_closed.exit_price.unwrap() - 94.50).abs() < 1e-6);
    }

    #[test]
    fn test_r14_h3_priority_tick_isolates_bad_item_and_alerts_after_threshold() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        db.set_config_value("priority_unmonitored_alert_ticks", "3", None)
            .unwrap();
        let sol_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "SOL".to_string(),
                asset_index: 3,
                sz_decimals: 2,
                tick_size: 0.01,
                lot_step_size: 0.01,
                max_leverage: 20.0,
                mark_price: 100.0,
                mid_price: 100.0,
                oracle_price: 100.0,
                funding_rate_hourly: 0.0,
                day_volume_usdc: 10_000_000.0,
                open_interest_usdc: 5_000_000.0,
                prev_day_price: 100.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();

        let (good_pid, _) = db
            .sync_open_position_from_exchange_v2(&aegis_db::ExchangeOpenPositionSyncInput {
                asset_id: sol_id,
                coin: "SOL",
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 100.0,
                quantity: 2.0,
                committed_notional_usdc: 200.0,
                unrealized_pnl_usdc: 0.0,
                portfolio_weight: 0.1,
                protective_stop_px: 95.0,
                take_profit_px: 110.0,
                funding_accrued_usdc: 0.0,
                opened_at_ms: Some(now_ms() - 10_000),
            })
            .unwrap();

        let http = HyperliquidHttpClient::new(5);
        let alerts = AlertEngine::new(db.clone());
        let chaser = OrderChaser::new(db.clone(), http.clone(), alerts.clone());
        let monitor = PriorityMonitor::new(db.clone(), http, chaser, alerts);

        // Simulate 3 consecutive top-level tick failures hitting alert threshold
        let err = AegisError::Database("simulated transient db error".to_string());
        monitor.record_tick_failure(&err);
        monitor.record_tick_failure(&err);
        monitor.record_tick_failure(&err);

        let health_raw = db
            .load_config()
            .unwrap()
            .get_str("priority_monitor_health_json", "")
            .to_string();
        let health: PriorityMonitorHealth = serde_json::from_str(&health_raw).unwrap();
        assert!(health.unmonitored_alert_active);
        assert_eq!(health.status, "CRITICAL");
        assert_eq!(health.consecutive_error_ticks, 3);

        // Healthy tick recovers status back to HEALTHY
        let rep = monitor.run_1s_priority_tick().unwrap();
        assert!(!rep.degraded);
        assert_eq!(rep.open_holdings_checked, 1);
        assert_eq!(
            db.get_position_by_id(good_pid).unwrap().unwrap().status,
            "OPEN"
        );

        let recovered_raw = db
            .load_config()
            .unwrap()
            .get_str("priority_monitor_health_json", "")
            .to_string();
        let recovered: PriorityMonitorHealth = serde_json::from_str(&recovered_raw).unwrap();
        assert!(!recovered.unmonitored_alert_active);
        assert_eq!(recovered.status, "HEALTHY");
    }

    #[test]
    fn test_p0_exec2_p1_strat_oracle_fallback_surveillance_and_maker_first_passive_close() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let sol_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "SOL".to_string(),
                asset_index: 3,
                sz_decimals: 2,
                tick_size: 0.01,
                lot_step_size: 0.01,
                max_leverage: 20.0,
                mark_price: 90.0, // > 50 bps basis dislocation vs oracle 100.0
                mid_price: 90.0,
                oracle_price: 100.0,
                funding_rate_hourly: 0.0,
                day_volume_usdc: 10_000_000.0,
                open_interest_usdc: 5_000_000.0,
                prev_day_price: 100.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();
        let sol = db.get_asset_by_id(sol_id).unwrap().unwrap();
        // Basis is 1000 bps > 50 bps -> must switch surveillance reference to oracle 100.0!
        assert!((resolve_surveillance_trigger_price(Some(&sol), 90.0, 50.0) - 100.0).abs() < 1e-6);

        // Create a position opened 6 hours ago with stagnation time-stop pinned to 5h -> PassiveFirst rests ALO limit, then fills!
        db.set_config_value("stagnation_time_stop_hours", "5.0", None)
            .unwrap();
        let (pos_id, _) = db
            .sync_open_position_from_exchange_v2(&aegis_db::ExchangeOpenPositionSyncInput {
                asset_id: sol_id,
                coin: "SOL",
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 100.0,
                quantity: 2.0,
                committed_notional_usdc: 200.0,
                unrealized_pnl_usdc: 0.0,
                portfolio_weight: 0.1,
                protective_stop_px: 92.0,
                take_profit_px: 112.0,
                funding_accrued_usdc: 0.0,
                opened_at_ms: Some(now_ms() - 6 * 3_600_000),
            })
            .unwrap();

        let phase = Arc::new(Mutex::new("rest_passive"));
        let phase_srv = Arc::clone(&phase);
        let fill_ts = now_ms();
        let srv = MockHttpServer::start(move |req| {
            let cur = *phase_srv.lock();
            if req.path == "/info" {
                match req
                    .json_body
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                {
                    "allMids" => MockHttpResponse::ok_json(&json!({ "SOL": "100.0" })),
                    "clearinghouseState" => {
                        if cur == "rest_passive" {
                            MockHttpResponse::ok_json(&json!({
                                "assetPositions": [{ "position": { "coin": "SOL", "szi": "2.0", "entryPx": "100.0" } }]
                            }))
                        } else {
                            MockHttpResponse::ok_json(&json!({ "assetPositions": [] }))
                        }
                    }
                    "l2Book" => MockHttpResponse::ok_json(&json!({
                        "levels": [
                            [{ "px": "99.95", "sz": "10.0" }],
                            [{ "px": "100.05", "sz": "10.0" }]
                        ]
                    })),
                    "userFillsByTime" => MockHttpResponse::ok_json(&json!([
                        { "coin": "SOL", "time": fill_ts, "dir": "Close Long", "px": "100.04", "sz": "2.0", "fee": "0.03", "oid": 8801u64 }
                    ])),
                    _ => MockHttpResponse::ok_json(&json!([])),
                }
            } else {
                MockHttpResponse::ok_json(&json!({
                    "status": "ok",
                    "response": { "type": "order", "data": { "statuses": [{ "resting": { "oid": 8801u64 } }] } }
                }))
            }
        });

        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_testnet_url", &srv.base_url(), None)
            .unwrap();
        db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
            .unwrap();
        db.set_config_value("passive_close_rest_sec", "8", None)
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

        let http = HyperliquidHttpClient::new(5);
        let alerts = AlertEngine::new(db.clone());
        let chaser = OrderChaser::new(db.clone(), http.clone(), alerts.clone());
        let monitor = PriorityMonitor::new(db.clone(), http, chaser, alerts);

        // Tick 1: Stagnation stop fires -> oracle fallback preserves 100.0 instead of spurious 90.0 SL ->
        // PassiveFirst rests ALO close at 100.04 and stays OPEN waiting passively.
        let rep1 = monitor.run_1s_priority_tick().unwrap();
        assert_eq!(rep1.stops_triggered_closed, 0);
        assert_eq!(monitor.passive_closes.len(), 1);
        assert_eq!(
            db.get_position_by_id(pos_id).unwrap().unwrap().status,
            "OPEN"
        );

        // Tick 2: Maker passive order filled on exchange -> closes DB with maker fill 100.04 & removes tracker!
        *phase.lock() = "filled";
        let rep2 = monitor.run_1s_priority_tick().unwrap();
        assert_eq!(rep2.stops_triggered_closed, 1);
        assert_eq!(monitor.passive_closes.len(), 0);
        let closed = db.get_position_by_id(pos_id).unwrap().unwrap();
        assert_eq!(closed.status, "CLOSED");
        assert!((closed.exit_price.unwrap() - 100.04).abs() < 1e-6);
    }
}
