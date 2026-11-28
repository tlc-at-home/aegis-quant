//! 1-Second Maker Limit Chaser & Order-to-Open Capital Lifecycle (`aegis-execution::order_chaser`).
//!
//! Enforces the Rev 3.1 strict Order-to-Open Capital Lifecycle:
//! - Places new entries as `WORKING` (`WORKING_ALO`), reserving `reserved_margin_usdc` without spending cash into `committed_notional_usdc`.
//! - Chases top-of-book at 1s frequency (`record_working_order_chase`) with cumulative initial-limit & oracle drift caps,
//!   live L2 spread aborts, native 1-tick wire-equivalence preservation, and `±25 bps` entry IOC escalation.
//! - Upon fill confirmation, unconditionally recomputes protective stop & TP from actual fill price,
//!   calls `promote_working_to_open`, attaches SL/TP via `tpsl_sync`, and enqueues non-blocking HTML alerts.

use crate::http_client::{HyperliquidHttpClient, OpenOrdersSnapshot};
use crate::lock_guard::acquire_execution_lock;
use crate::signing::{
    build_limit_order_wire, derive_hl_cloid_hex, effective_hl_tick_size, format_hl_wire_price,
    snap_to_lot, snap_to_tick,
};
use crate::tpsl_sync::{ensure_exchange_tpsl_orders_detailed, TpslSyncRequest};
use crate::ws_client::WsStateCache;
use aegis_alerts::{
    format_order_filled_open_html, format_working_order_placed_html, AlertEngine,
    OrderFilledOpenAlert, WorkingOrderPlacedAlert,
};
use aegis_core::hl_parse::{
    is_entry_fill_complete as core_is_entry_fill_complete,
    remaining_entry_qty_for_lot as core_remaining_entry_qty_for_lot, ENTRY_PARTIAL_GRACE_MS,
    HL_ORACLE_BAND_PCT,
};
use aegis_core::indicators::compute_one_tick_bps_opt;
use aegis_core::{
    now_ms, parse_hl_f64, round_dp, AegisError, ConfigMap, ExchangeAsset, OrderFsmState, OrderSide,
    OrderedAssetView, PositionStatus, Result,
};
use aegis_db::{DatabaseManager, WorkingOrderInput};
use aegis_risk::{
    compute_initial_protective_stop, compute_initial_take_profit, RiskEngine, RiskLimitsConfig,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

// Re-export consolidated HL OID parsers for internal modules (e.g., `reconcile.rs`, `priority_monitor.rs`) (`R14-L2`).
pub(crate) use aegis_core::{extract_hl_oid as extract_order_oid, parse_hl_oid};

/// Default multiplier applied to single-tick bps when computing effective chase threshold (`R14-M5`).
pub const ALO_ONE_TICK_THRESHOLD_MULT: f64 = 1.5;

/// Default maximum entry IOC slippage in basis points (`±25 bps`) (`P0-Exec-1`, `R14-M5`).
pub const ENTRY_IOC_DEFAULT_MAX_SLIPPAGE_BPS: f64 = 25.0;

const REJECT_WINDOW_MS: i64 = 900_000; // 15m sliding window
const EXPIRY_WINDOW_MS: i64 = 1_800_000; // 30m sliding window

enum CancelEntryOutcome {
    Proceed { saw_filled_hint: bool },
    Aborted,
}

#[derive(Debug, Default)]
struct AssetFailureCounters {
    reject_timestamps_ms: VecDeque<i64>,
    expiry_timestamps_ms: VecDeque<i64>,
}

enum UncertainSubmitProbe {
    Resting {
        eoid: Option<String>,
    },
    Filled {
        eoid: Option<String>,
        fill_px: f64,
        fill_qty: f64,
        est_fee: f64,
    },
    ConfirmedAbsent,
    ProbeUnreachable,
}

#[derive(Debug, Clone, Copy)]
struct ChaserConfig {
    max_chases: u32,
    chase_threshold_bps: f64,
    max_entry_spread_bps: f64,
    max_alo_chase_drift_bps: f64,
    max_alo_oracle_drift_bps: f64,
    chase_reject_cooldown_sec: i64,
    entry_ioc_max_slippage_bps: f64,
    hl_reject_backoff_threshold: usize,
    alo_expiry_backoff_threshold: usize,
    maker_fee_bps: f64,
    taker_fee_bps: f64,
}

impl ChaserConfig {
    fn from_config(cfg: &ConfigMap) -> Self {
        Self {
            max_chases: cfg.get_usize("max_alo_chase_ticks", 3).max(1) as u32,
            chase_threshold_bps: cfg.get_f64("alo_chase_threshold_bps", 8.0).max(0.5),
            max_entry_spread_bps: cfg.get_f64("max_entry_spread_bps", 10.0).max(1.0),
            max_alo_chase_drift_bps: cfg
                .get_f64("max_alo_chase_drift_bps", 60.0)
                .clamp(5.0, 300.0),
            max_alo_oracle_drift_bps: cfg
                .get_f64("max_alo_oracle_drift_bps", 150.0)
                .clamp(20.0, 500.0),
            chase_reject_cooldown_sec: cfg
                .get_i64("chase_reject_cooldown_sec", 300)
                .clamp(30, 3600),
            entry_ioc_max_slippage_bps: cfg
                .get_f64(
                    "entry_ioc_max_slippage_bps",
                    ENTRY_IOC_DEFAULT_MAX_SLIPPAGE_BPS,
                )
                .clamp(5.0, 200.0),
            hl_reject_backoff_threshold: cfg
                .get_usize("hl_reject_backoff_threshold", 3)
                .clamp(1, 20),
            alo_expiry_backoff_threshold: cfg
                .get_usize("alo_expiry_backoff_threshold", 2)
                .clamp(1, 10),
            maker_fee_bps: cfg.get_f64("maker_fee_bps", 1.5).max(0.0),
            taker_fee_bps: cfg.get_f64("taker_fee_bps", 4.5).max(0.0),
        }
    }
}

/// Anchors and clamps an initial ALO limit price onto the strictly passive side of `(bid, ask)`.
pub(crate) fn clamp_initial_alo_price(
    side: OrderSide,
    ref_mid: f64,
    bid: f64,
    ask: f64,
    tick: f64,
) -> f64 {
    let eff_tick = tick.max(1e-8);
    if side.is_buy() {
        let max_passive = (ask - eff_tick).max(eff_tick);
        let raw = bid.min(ref_mid).min(max_passive).max(eff_tick);
        let mut px = snap_to_tick(raw, eff_tick);
        if ask > eff_tick * 1.5 && px >= ask - eff_tick * 1e-6 {
            px = snap_to_tick((px - eff_tick).max(eff_tick), eff_tick);
        }
        px
    } else {
        let min_passive = bid + eff_tick;
        let raw = ask.max(ref_mid).max(min_passive).max(eff_tick);
        let mut px = snap_to_tick(raw, eff_tick);
        if px <= bid + eff_tick * 1e-6 {
            px = snap_to_tick(px + eff_tick, eff_tick);
        }
        px
    }
}

/// Computes a chased ALO limit price from `live_mid` and clamps it strictly passive against `(bid, ask)`.
pub(crate) fn clamp_chase_alo_price(
    side: OrderSide,
    live_mid: f64,
    bbo: Option<(f64, f64)>,
    tick: f64,
) -> f64 {
    let eff_tick = tick.max(1e-8);
    let mut px = if side.is_buy() {
        snap_to_tick((live_mid - eff_tick).max(eff_tick), eff_tick)
    } else {
        snap_to_tick(live_mid + eff_tick, eff_tick)
    };
    if let Some((bid, ask)) =
        bbo.filter(|(b, a)| b.is_finite() && a.is_finite() && *b > 0.0 && *a >= *b)
    {
        if side.is_buy() {
            let mut max_passive = snap_to_tick((ask - eff_tick).max(eff_tick), eff_tick);
            if ask > eff_tick * 1.5 && max_passive >= ask - eff_tick * 1e-6 {
                max_passive = snap_to_tick((max_passive - eff_tick).max(eff_tick), eff_tick);
            }
            px = snap_to_tick(px.min(max_passive).max(eff_tick), eff_tick);
        } else {
            let mut min_passive = snap_to_tick(bid + eff_tick, eff_tick);
            if min_passive <= bid + eff_tick * 1e-6 {
                min_passive = snap_to_tick(min_passive + eff_tick, eff_tick);
            }
            px = snap_to_tick(px.max(min_passive).max(eff_tick), eff_tick);
        }
    }
    px
}

/// Steps a rejected ALO chase price 1 extra tick passive relative to `rejected_px` and fresh `l2Book`.
pub(crate) fn step_more_passive_alo_price(
    side: OrderSide,
    rejected_px: f64,
    bbo: Option<(f64, f64)>,
    tick: f64,
) -> f64 {
    let eff_tick = tick.max(1e-8);
    if side.is_buy() {
        let mut px = snap_to_tick((rejected_px - eff_tick).max(eff_tick), eff_tick);
        if let Some((_bid, ask)) =
            bbo.filter(|(b, a)| b.is_finite() && a.is_finite() && *b > 0.0 && *a >= *b)
        {
            let bbo_two_ticks = snap_to_tick((ask - 2.0 * eff_tick).max(eff_tick), eff_tick);
            px = px.min(bbo_two_ticks).max(eff_tick);
            if ask > eff_tick * 1.5 && px >= ask - eff_tick * 1e-6 {
                px = snap_to_tick((px - eff_tick).max(eff_tick), eff_tick);
            }
        }
        snap_to_tick(px, eff_tick)
    } else {
        let mut px = snap_to_tick(rejected_px + eff_tick, eff_tick);
        if let Some((bid, _ask)) =
            bbo.filter(|(b, a)| b.is_finite() && a.is_finite() && *b > 0.0 && *a >= *b)
        {
            let bbo_two_ticks = snap_to_tick(bid + 2.0 * eff_tick, eff_tick);
            px = px.max(bbo_two_ticks);
            if px <= bid + eff_tick * 1e-6 {
                px = snap_to_tick(px + eff_tick, eff_tick);
            }
        }
        snap_to_tick(px, eff_tick)
    }
}

pub(crate) fn is_post_only_crossing_rejection(raw_error: Option<&str>) -> bool {
    let Some(err) = raw_error else {
        return false;
    };
    let lower = err.to_ascii_lowercase();
    lower.contains("badalopx")
        || lower.contains("post only")
        || lower.contains("post-only")
        || lower.contains("would immediately match")
        || lower.contains("would have immediately matched")
}

fn is_hard_non_transient_reject(reason: &str) -> bool {
    let lower = reason.to_ascii_lowercase();
    lower.contains("insufficient margin")
        || lower.contains("minimum value")
        || lower.contains("price too far from oracle")
        || lower.contains("exceeds max position")
        || lower.contains("max open interest")
        || lower.contains("reduce only")
}

fn ensure_wire_price_strictly_passive(
    side: OrderSide,
    candidate_px: f64,
    bbo: Option<(f64, f64)>,
    eff_tick: f64,
    sz_decimals: u32,
) -> f64 {
    let Some((bid, ask)) =
        bbo.filter(|(b, a)| b.is_finite() && a.is_finite() && *b > 0.0 && *a >= *b)
    else {
        return candidate_px;
    };
    let mut px = candidate_px;
    let wire_val = |p: f64| -> f64 {
        format_hl_wire_price(p, eff_tick, sz_decimals)
            .parse::<f64>()
            .unwrap_or(p)
    };
    let ask_wire = wire_val(ask);
    let bid_wire = wire_val(bid);
    for _ in 0..2 {
        let cur_wire = wire_val(px);
        if side.is_buy() && cur_wire >= ask_wire - 1e-12 && ask_wire > eff_tick * 1.5 {
            px = snap_to_tick((px - eff_tick).max(eff_tick), eff_tick);
        } else if !side.is_buy() && cur_wire <= bid_wire + 1e-12 {
            px = snap_to_tick(px + eff_tick, eff_tick);
        } else {
            break;
        }
    }
    px
}

fn extract_position_state_from_ch_value(
    ch_state: &Value,
    asset: &ExchangeAsset,
    side: OrderSide,
) -> Option<(f64, f64)> {
    let arr = ch_state.get("assetPositions").and_then(Value::as_array)?;
    for item in arr {
        let p = item.get("position").unwrap_or(item);
        let coin = p.get("coin").and_then(Value::as_str).unwrap_or("").trim();
        if !coin.eq_ignore_ascii_case(&asset.coin) {
            continue;
        }
        let szi = parse_hl_f64(p.get("szi"));
        if szi.abs() > 1e-9 && (szi > 0.0) == side.is_buy() {
            let qty = szi.abs();
            let entry_px = parse_hl_f64(p.get("entryPx"));
            let pos_val = parse_hl_f64(p.get("positionValue"));
            let fill_px = if entry_px > 0.0 {
                entry_px
            } else if pos_val > 0.0 {
                round_dp(pos_val / qty, 6)
            } else if asset.mark_price > 0.0 {
                asset.mark_price
            } else {
                asset.mid_price
            };
            if fill_px > 0.0 {
                return Some((fill_px, qty));
            }
        }
    }
    None
}

/// Entry routing urgency policy (`N4`).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum EntryUrgency {
    MakerOnly,
    #[default]
    ChaseToIoc,
}

/// Request payload to place a new `WORKING` (`WORKING_ALO`) order reserving margin.
#[derive(Debug, Clone)]
pub struct NewWorkingOrderRequest {
    pub asset: ExchangeAsset,
    pub side: OrderSide,
    pub requested_quantity: f64,
    pub reserved_margin_usdc: f64,
    pub portfolio_weight: f64,
    pub s1_weight_contrib: f64,
    pub s2_weight_contrib: f64,
    pub s3_weight_contrib: f64,
    pub protective_stop_px: f64,
    pub reason: String,
    pub bypass_entry_interval: bool,
    pub entry_urgency: EntryUrgency,
    pub is_scale_in: bool,
}

/// Result of a 1-second Maker Limit Chaser step on a `WORKING` order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChaseStepOutcome {
    Resting {
        position_id: i64,
        symbol: String,
        limit_price: f64,
        distance_from_mid_bps: f64,
    },
    Chased {
        position_id: i64,
        symbol: String,
        new_limit_price: f64,
        chase_count: u32,
        fsm_state: String,
    },
    FilledOpen {
        position_id: i64,
        asset_id: i64,
        symbol: String,
        side: String,
        fill_price: f64,
        filled_quantity: f64,
        committed_notional_usdc: f64,
        protective_stop_px: f64,
        fees_paid_usdc: f64,
    },
    Cancelled {
        position_id: i64,
        symbol: String,
        reason: String,
    },
}

/// 1-Second Maker Limit Chaser managing `WORKING` -> `OPEN` transitions.
#[derive(Clone)]
pub struct OrderChaser {
    pub db: DatabaseManager,
    pub http: HyperliquidHttpClient,
    pub alerts: AlertEngine,
    working_initial_limits: Arc<Mutex<HashMap<i64, f64>>>,
    working_entry_urgencies: Arc<Mutex<HashMap<i64, EntryUrgency>>>,
    asset_failure_counters: Arc<Mutex<HashMap<i64, AssetFailureCounters>>>,
    scale_in_anchors: Arc<Mutex<HashMap<i64, (f64, i64)>>>,
    alert_wakeup_tx: Option<SyncSender<usize>>,
    ws_cache: Option<Arc<WsStateCache>>,
}

impl OrderChaser {
    pub fn new(db: DatabaseManager, http: HyperliquidHttpClient, alerts: AlertEngine) -> Self {
        Self {
            db,
            http,
            alerts,
            working_initial_limits: Arc::new(Mutex::new(HashMap::new())),
            working_entry_urgencies: Arc::new(Mutex::new(HashMap::new())),
            asset_failure_counters: Arc::new(Mutex::new(HashMap::new())),
            scale_in_anchors: Arc::new(Mutex::new(HashMap::new())),
            alert_wakeup_tx: None,
            ws_cache: None,
        }
    }

    /// Returns the latest in-memory `(fill_price, ts_ms)` recorded when a `WORKING` scale-in merged into `open_position_id` (`R14-W2`).
    pub fn latest_scale_in_anchor(&self, open_position_id: i64) -> Option<(f64, i64)> {
        self.scale_in_anchors.lock().get(&open_position_id).copied()
    }

    /// Records a merged scale-in fill anchor `(fill_price, ts_ms)` for `open_position_id` (`R14-W2`).
    pub fn record_scale_in_anchor(&self, open_position_id: i64, fill_px: f64, ts_ms: i64) {
        if fill_px.is_finite() && fill_px > 0.0 {
            self.scale_in_anchors
                .lock()
                .insert(open_position_id, (fill_px, ts_ms));
        }
    }

    /// Prunes cached `scale_in_anchors` to currently `OPEN` position IDs (`R14-W2`).
    pub fn prune_scale_in_anchors(&self, active_open_ids: &HashSet<i64>) {
        self.scale_in_anchors
            .lock()
            .retain(|pid, _| active_open_ids.contains(pid));
    }

    /// Attaches an optional bounded wakeup sender for non-blocking background alert dispatch (`R14-L5`).
    pub fn with_alert_wakeup(mut self, wakeup_tx: SyncSender<usize>) -> Self {
        self.alert_wakeup_tx = Some(wakeup_tx);
        self
    }

    /// Attaches a shared [`WsStateCache`] so `l2Book` and `clearinghouseState` reads avoid REST
    /// polling when fresh WebSocket frames are available (`R14-H4` Phase 2).
    pub fn with_ws_cache(mut self, ws_cache: Arc<WsStateCache>) -> Self {
        self.ws_cache = Some(ws_cache);
        self
    }

    fn resolve_entry_urgency(&self, ordered: &OrderedAssetView, cfg: &ConfigMap) -> EntryUrgency {
        if let Some(&urg) = self
            .working_entry_urgencies
            .lock()
            .get(&ordered.position_id)
        {
            return urg;
        }
        if cfg.get_bool("maker_first_low_urgency_entries", false) {
            if let Ok(Some(reason)) = self
                .db
                .get_working_order_initial_reason(ordered.position_id)
            {
                if !matches!(
                    reason.as_str(),
                    "LIVE_BREAKOUT_SURGE" | "CAPITULATION_FLUSH" | "FLIP"
                ) {
                    return EntryUrgency::MakerOnly;
                }
            }
        }
        EntryUrgency::ChaseToIoc
    }

    fn adjust_exchange_state_for_scale_in(
        &self,
        working_position_id: Option<i64>,
        asset: &ExchangeAsset,
        side: OrderSide,
        ch_pos: Option<(f64, f64)>,
    ) -> Option<(f64, f64)> {
        let (total_px, total_qty) = ch_pos?;
        let open_row_opt = self
            .db
            .list_active_positions_for_asset(asset.id)
            .ok()
            .and_then(|rows| {
                rows.into_iter().find(|p| {
                    working_position_id.is_none_or(|wid| p.id != wid)
                        && PositionStatus::parse(&p.status) == PositionStatus::Open
                        && OrderSide::parse(&p.side).ok() == Some(side)
                })
            });
        let Some(open_row) = open_row_opt else {
            return Some((total_px, total_qty));
        };
        let base_qty = open_row.filled_quantity.max(0.0);
        let inc_qty = snap_to_lot(
            (total_qty - base_qty).max(0.0),
            asset.lot_step_size,
            asset.sz_decimals,
            true,
        );
        if inc_qty <= 1e-9 {
            return None;
        }
        let inc_px = if total_px > 0.0 && open_row.entry_price > 0.0 {
            let implied = (total_px * total_qty - open_row.entry_price * base_qty) / inc_qty;
            if implied.is_finite() && implied > 0.0 {
                implied
            } else {
                total_px
            }
        } else {
            total_px
        };
        Some((inc_px, inc_qty))
    }

    fn query_working_exchange_position_state(
        &self,
        working_position_id: Option<i64>,
        asset: &ExchangeAsset,
        side: OrderSide,
        cfg: &ConfigMap,
    ) -> Result<Option<(f64, f64)>> {
        let ch_pos = self.query_exchange_position_state(asset, side, cfg)?;
        Ok(self.adjust_exchange_state_for_scale_in(working_position_id, asset, side, ch_pos))
    }

    fn extract_working_position_state(
        &self,
        ch_val: &Value,
        working_position_id: i64,
        asset: &ExchangeAsset,
        side: OrderSide,
    ) -> Option<(f64, f64)> {
        let ch_pos = extract_position_state_from_ch_value(ch_val, asset, side);
        self.adjust_exchange_state_for_scale_in(Some(working_position_id), asset, side, ch_pos)
    }

    fn check_pre_ioc_heat_and_gross_cap(
        &self,
        ordered: &OrderedAssetView,
        asset: &ExchangeAsset,
        side: OrderSide,
        live_mid: f64,
        eff_tick: f64,
        cfg: &ConfigMap,
    ) -> Result<()> {
        let risk = RiskEngine::from_config_map(cfg);
        let eff_equity = crate::engine::resolve_effective_base_equity_usdc_for_db(&self.db, cfg);
        let (held, ordered_all, stored_stop) = {
            let _guard = acquire_execution_lock();
            let h = self.db.get_assets_held()?;
            let o = self.db.get_assets_ordered().unwrap_or_default();
            let s = self
                .db
                .get_position_by_id(ordered.position_id)
                .ok()
                .flatten()
                .map(|p| p.protective_stop_px)
                .unwrap_or(0.0);
            (h, o, s)
        };
        let exp = crate::engine::compute_active_exposure_and_heat_with_db(
            Some(&self.db),
            &held,
            &ordered_all,
            cfg.target_leverage_multiplier(),
            risk.limits.protective_stop_default_pct,
            Some(ordered.position_id),
        );
        let est_fill_px = if live_mid.is_finite() && live_mid > 0.0 {
            live_mid
        } else {
            ordered.limit_price
        };
        let prosp_qty = ordered.requested_quantity.max(0.0);
        let prosp_notional = est_fill_px * prosp_qty;
        let prosp_stop_px = if stored_stop.is_finite()
            && stored_stop > 0.0
            && match side {
                OrderSide::Long => stored_stop < est_fill_px,
                OrderSide::Short => stored_stop > est_fill_px,
            } {
            stored_stop
        } else {
            let atr_opt = crate::engine::resolve_signal_matched_atr_from_db(
                &self.db,
                asset.id,
                cfg,
                est_fill_px,
                risk.limits.protective_stop_default_pct,
            );
            compute_initial_protective_stop(side, est_fill_px, atr_opt, eff_tick, &risk.limits)
        };
        let prosp_stop_risk = (est_fill_px - prosp_stop_px).abs() * prosp_qty;
        risk.check_portfolio_heat_and_gross_at_fill(
            eff_equity,
            exp.total_gross_notional_usdc + prosp_notional,
            exp.total_stop_risk_usdc + prosp_stop_risk,
        )
    }

    fn fetch_fresh_l2_bbo(&self, coin: &str, cfg: &ConfigMap) -> Option<(f64, f64)> {
        if let Some(bbo) = self
            .ws_cache
            .as_ref()
            .and_then(|c| c.get_fresh_l2_bbo(coin))
            .filter(|(b, a)| b.is_finite() && a.is_finite() && *b > 0.0 && *a >= *b)
        {
            return Some(bbo);
        }
        self.http
            .fetch_l2_book(coin, cfg)
            .ok()
            .flatten()
            .filter(|(b, a)| b.is_finite() && a.is_finite() && *b > 0.0 && *a >= *b)
    }

    /// Prunes cached `working_initial_limits` and expired failure windows to prevent unbounded growth (`R14-L6`).
    pub fn prune_tracking_maps(&self, active_working_ids: &HashSet<i64>) {
        self.working_initial_limits
            .lock()
            .retain(|pid, _| active_working_ids.contains(pid));

        let now = now_ms();
        let mut counters = self.asset_failure_counters.lock();
        counters.retain(|_, c| {
            while c
                .reject_timestamps_ms
                .front()
                .is_some_and(|&t| now.saturating_sub(t) > REJECT_WINDOW_MS)
            {
                c.reject_timestamps_ms.pop_front();
            }
            while c
                .expiry_timestamps_ms
                .front()
                .is_some_and(|&t| now.saturating_sub(t) > EXPIRY_WINDOW_MS)
            {
                c.expiry_timestamps_ms.pop_front();
            }
            !c.reject_timestamps_ms.is_empty() || !c.expiry_timestamps_ms.is_empty()
        });
    }

    fn wake_alert_worker(&self) {
        if let Some(ref tx) = self.alert_wakeup_tx {
            let _ = tx.try_send(1);
        }
    }

    fn resolve_initial_limit_price(&self, position_id: i64, fallback_limit_px: f64) -> f64 {
        if let Some(&px) = self.working_initial_limits.lock().get(&position_id) {
            if px.is_finite() && px > 0.0 {
                return px;
            }
        }
        let resolved = self
            .db
            .get_working_order_initial_limit_price(position_id)
            .ok()
            .flatten()
            .filter(|&p| p.is_finite() && p > 0.0)
            .unwrap_or(fallback_limit_px);
        if resolved.is_finite() && resolved > 0.0 {
            self.working_initial_limits
                .lock()
                .insert(position_id, resolved);
        }
        resolved
    }

    fn record_hl_reject_and_maybe_cooldown(
        &self,
        asset_id: i64,
        symbol: &str,
        reason: &str,
        ccfg: &ChaserConfig,
    ) {
        let now = now_ms();
        let count = {
            let mut map = self.asset_failure_counters.lock();
            let entry = map.entry(asset_id).or_default();
            while entry
                .reject_timestamps_ms
                .front()
                .is_some_and(|&t| now.saturating_sub(t) > REJECT_WINDOW_MS)
            {
                entry.reject_timestamps_ms.pop_front();
            }
            entry.reject_timestamps_ms.push_back(now);
            entry.reject_timestamps_ms.len()
        };
        if is_hard_non_transient_reject(reason) || count >= ccfg.hl_reject_backoff_threshold {
            let cd_reason = format!(
                "CHURN_GUARD: Hyperliquid rejected {} ({count} rejects in 15m; latest='{reason}')",
                symbol
            );
            let _ = self.db.insert_asset_cooldown(
                asset_id,
                "CHURN_GUARD",
                &cd_reason,
                ccfg.chase_reject_cooldown_sec,
            );
        }
    }

    fn record_alo_expiry_and_maybe_cooldown(
        &self,
        asset_id: i64,
        symbol: &str,
        reason: &str,
        ccfg: &ChaserConfig,
    ) {
        let now = now_ms();
        let count = {
            let mut map = self.asset_failure_counters.lock();
            let entry = map.entry(asset_id).or_default();
            while entry
                .expiry_timestamps_ms
                .front()
                .is_some_and(|&t| now.saturating_sub(t) > EXPIRY_WINDOW_MS)
            {
                entry.expiry_timestamps_ms.pop_front();
            }
            entry.expiry_timestamps_ms.push_back(now);
            entry.expiry_timestamps_ms.len()
        };
        let cd_sec = if count >= ccfg.alo_expiry_backoff_threshold {
            ccfg.chase_reject_cooldown_sec
        } else {
            60
        };
        let cd_reason = format!(
            "CHURN_GUARD: {} on {} ({count} expiry/unfilled events in 30m)",
            reason, symbol
        );
        let _ = self
            .db
            .insert_asset_cooldown(asset_id, "CHURN_GUARD", &cd_reason, cd_sec);
    }

    /// Places a new `WORKING` (`WORKING_ALO`) order, reserving `reserved_margin_usdc`
    /// without deducting `committed_notional_usdc` yet, and enqueues a non-blocking HTML alert.
    pub fn place_working_order(&self, req: &NewWorkingOrderRequest) -> Result<i64> {
        let (cfg, ccfg, is_live_exchange, ref_mid, tick, qty, limits, scale_in_open_pos) = {
            let _guard = acquire_execution_lock();
            if self.db.is_asset_in_cooldown(req.asset.id)? {
                return Err(AegisError::Validation(format!(
                    "Asset '{}' is currently in active cooldown; blocking new WORKING order",
                    req.asset.symbol
                )));
            }
            let active_rows = self.db.list_active_positions_for_asset(req.asset.id)?;
            let scale_in_open_pos = if req.is_scale_in {
                let valid_open = active_rows.len() == 1
                    && PositionStatus::parse(&active_rows[0].status) == PositionStatus::Open
                    && OrderSide::parse(&active_rows[0].side).ok() == Some(req.side);
                if !valid_open {
                    return Err(AegisError::Validation(format!(
                        "Asset '{}' scale-in requires exactly one matching-side OPEN position",
                        req.asset.symbol
                    )));
                }
                Some(active_rows[0].clone())
            } else {
                if let Some(existing) = active_rows.first() {
                    return Err(AegisError::Validation(format!(
                        "Asset '{}' already has active position #{} (status={})",
                        req.asset.symbol, existing.id, existing.status
                    )));
                }
                None
            };

            let cfg = self.db.load_config()?;
            let ccfg = ChaserConfig::from_config(&cfg);
            let max_open_positions = cfg.get_usize("max_open_positions", 6).max(1);
            let min_entry_interval_sec = cfg.get_i64("min_entry_interval_sec", 150).max(0);

            // R14-H1: Count distinct active assets (OPEN U WORKING) regardless of cap_epoch_ms.
            let mut active_asset_ids: HashSet<i64> = self
                .db
                .get_assets_held()?
                .into_iter()
                .map(|h| h.asset_id)
                .collect();
            for o in self.db.get_assets_ordered()? {
                active_asset_ids.insert(o.asset_id);
            }
            let total_active = active_asset_ids.len();
            if !req.is_scale_in && total_active >= max_open_positions {
                return Err(AegisError::Validation(format!(
                    "MAX_OPEN_POSITIONS_REACHED: active={total_active} >= max_open_positions={max_open_positions}"
                )));
            }

            if min_entry_interval_sec > 0 && !req.bypass_entry_interval {
                if let Some(last_entry_ms) = self.db.last_position_entry_timestamp_ms()? {
                    let elapsed_ms = now_ms().saturating_sub(last_entry_ms);
                    if elapsed_ms < min_entry_interval_sec * 1000 {
                        return Err(AegisError::Validation(format!(
                            "MIN_ENTRY_INTERVAL_COOLDOWN: elapsed={}s < min_entry_interval_sec={}s",
                            elapsed_ms / 1000,
                            min_entry_interval_sec
                        )));
                    }
                }
            }

            let ref_mid = if req.asset.mid_price > 0.0 {
                req.asset.mid_price
            } else {
                req.asset.mark_price
            };
            if !ref_mid.is_finite() || ref_mid <= 0.0 {
                return Err(AegisError::Validation(format!(
                    "Asset '{}' has invalid reference price {ref_mid}",
                    req.asset.symbol
                )));
            }

            let tick = effective_hl_tick_size(ref_mid, req.asset.tick_size, req.asset.sz_decimals)
                .unwrap_or_else(|| req.asset.tick_size.max(1e-8));
            let qty = snap_to_lot(
                req.requested_quantity,
                req.asset.lot_step_size,
                req.asset.sz_decimals,
                true,
            );
            if qty <= 0.0 {
                return Err(AegisError::Validation(format!(
                    "Requested quantity {:.6} rounds to zero for '{}'",
                    req.requested_quantity, req.asset.symbol
                )));
            }

            let limits = RiskLimitsConfig::from_config_map(&cfg);
            let is_live_exchange = crate::engine::is_live_exchange_active(&self.db, &cfg);
            (
                cfg,
                ccfg,
                is_live_exchange,
                ref_mid,
                tick,
                qty,
                limits,
                scale_in_open_pos,
            )
        };

        let atr_opt = crate::engine::resolve_signal_matched_atr_unlocked(
            &self.db,
            &self.http,
            Some(&req.asset),
            req.asset.id,
            &cfg,
            ref_mid,
            limits.protective_stop_default_pct,
        );
        if let Some(ref open_pos) = scale_in_open_pos {
            crate::engine::check_scale_in_spacing_for_open_position(
                &self.db, self, open_pos, ref_mid, atr_opt, &cfg,
            )?;
        }
        let stop_px = if req.protective_stop_px.is_finite() && req.protective_stop_px > 0.0 {
            snap_to_tick(req.protective_stop_px, tick)
        } else {
            compute_initial_protective_stop(req.side, ref_mid, atr_opt, tick, &limits)
        };
        let tp_px = compute_initial_take_profit(req.side, ref_mid, atr_opt, tick, &limits);

        let mut limit_price = if req.side.is_buy() {
            snap_to_tick((ref_mid - tick).max(tick), tick)
        } else {
            snap_to_tick(ref_mid + tick, tick)
        };

        let coid = format!(
            "v2-{}-{}-{}",
            req.asset.coin.to_ascii_lowercase(),
            req.side.as_str().to_ascii_lowercase(),
            now_ms()
        );
        let expected_cloid = derive_hl_cloid_hex(&coid);

        let mut exchange_oid: Option<String> = None;
        let mut live_filled_immediately: Option<(f64, f64, f64)> = None;

        if is_live_exchange {
            let pk = cfg.get_str("private_key", "");
            if pk.trim().is_empty() {
                tracing::error!(
                    symbol = %req.asset.symbol,
                    "private_key is empty/missing in live mode; refusing to place WORKING order"
                );
                return Err(AegisError::SecurityViolation(format!(
                    "private_key is empty in live mode; refusing to place order on {}",
                    req.asset.symbol
                )));
            }
            let l2_bbo = self.fetch_fresh_l2_bbo(&req.asset.coin, &cfg);
            if let Some((bid, ask)) = l2_bbo {
                let mid_bbo = (bid + ask) * 0.5;
                if mid_bbo > 0.0 {
                    let spread_bps = ((ask - bid) / mid_bbo) * 10_000.0;
                    if spread_bps > ccfg.max_entry_spread_bps {
                        return Err(AegisError::Validation(format!(
                            "ENTRY_SPREAD_TOO_WIDE: {} spread={spread_bps:.2}bps > max_entry_spread_bps={:.2}bps",
                            req.asset.symbol, ccfg.max_entry_spread_bps
                        )));
                    }
                }
                limit_price = clamp_initial_alo_price(req.side, ref_mid, bid, ask, tick);
                limit_price = ensure_wire_price_strictly_passive(
                    req.side,
                    limit_price,
                    Some((bid, ask)),
                    tick,
                    req.asset.sz_decimals,
                );
            }

            let wire = build_limit_order_wire(
                req.asset.asset_index,
                req.side.is_buy(),
                limit_price,
                qty,
                tick,
                req.asset.lot_step_size,
                req.asset.sz_decimals,
                false,
                "Alo",
                Some(&coid),
            );
            match self.http.submit_order_wire(
                wire,
                qty,
                limit_price,
                ccfg.maker_fee_bps,
                &pk,
                Some(&self.db),
                &cfg,
            ) {
                Ok(ack) => {
                    if !ack.accepted {
                        let reason = ack.raw_error.unwrap_or_else(|| "REJECTED".to_string());
                        tracing::warn!(
                            symbol = %req.asset.symbol,
                            limit_price = limit_price,
                            status = %ack.status,
                            error = %reason,
                            "Hyperliquid rejected initial WORKING ALO order"
                        );
                        self.record_hl_reject_and_maybe_cooldown(
                            req.asset.id,
                            &req.asset.symbol,
                            &reason,
                            &ccfg,
                        );
                        return Err(AegisError::Execution(format!(
                            "Hyperliquid rejected WORKING ALO order on {}: {reason}",
                            req.asset.symbol
                        )));
                    }
                    exchange_oid = ack.exchange_order_id.clone();
                    if ack.status == "FILLED" && ack.filled_quantity > 0.0 {
                        live_filled_immediately =
                            Some((ack.avg_fill_price, ack.filled_quantity, ack.fee_usdc));
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        symbol = %req.asset.symbol,
                        limit_price = limit_price,
                        expected_cloid = %expected_cloid,
                        error = %err,
                        "Hyperliquid WORKING ALO submit_order_wire failed; probing cloid status (R14-M1)"
                    );
                    match self.probe_uncertain_working_submit_by_cloid(
                        None,
                        &req.asset,
                        req.side,
                        req.is_scale_in,
                        &expected_cloid,
                        qty,
                        ccfg.maker_fee_bps,
                        &cfg,
                    ) {
                        UncertainSubmitProbe::Resting { eoid } => {
                            exchange_oid = eoid;
                        }
                        UncertainSubmitProbe::Filled {
                            eoid,
                            fill_px,
                            fill_qty,
                            est_fee,
                        } => {
                            exchange_oid = eoid;
                            live_filled_immediately = Some((fill_px, fill_qty, est_fee));
                        }
                        UncertainSubmitProbe::ConfirmedAbsent => return Err(err),
                        UncertainSubmitProbe::ProbeUnreachable => {
                            let detail = format!(
                                "CHURN_GUARD: Uncertain WORKING ALO submit on {} (cloid={}); probe unreachable, pausing 60s",
                                req.asset.symbol, expected_cloid
                            );
                            let _ = self.db.insert_asset_cooldown(
                                req.asset.id,
                                "CHURN_GUARD",
                                &detail,
                                60,
                            );
                            let _ = self.db.log_telemetry(
                                "WARN",
                                "execution.order_chaser",
                                &detail,
                                None,
                            );
                            return Err(err);
                        }
                    }
                }
            }
        }

        let reserved_usdc = if req.reserved_margin_usdc > 0.0 {
            round_dp(req.reserved_margin_usdc, 4)
        } else {
            round_dp(limit_price * qty, 4)
        };

        let pos_id = {
            let _guard = acquire_execution_lock();
            let pos_id = self.db.create_working_order(&WorkingOrderInput {
                asset_id: req.asset.id,
                client_order_id: coid.clone(),
                side: req.side,
                limit_price,
                requested_quantity: qty,
                reserved_margin_usdc: reserved_usdc,
                portfolio_weight: req.portfolio_weight,
                s1_weight_contrib: req.s1_weight_contrib,
                s2_weight_contrib: req.s2_weight_contrib,
                s3_weight_contrib: req.s3_weight_contrib,
                protective_stop_px: stop_px,
                take_profit_px: Some(tp_px),
                exchange_order_id: exchange_oid.clone(),
                reason: req.reason.clone(),
            })?;

            self.working_initial_limits
                .lock()
                .insert(pos_id, limit_price);
            self.working_entry_urgencies
                .lock()
                .insert(pos_id, req.entry_urgency);
            let _ = self.db.record_position_entry_timestamp_ms(now_ms());
            let _ = self.db.set_asset_priority_watch(req.asset.id, true);

            if let Ok(snap) = self.db.get_latest_portfolio_snapshot() {
                let dist_bps = if ref_mid > 0.0 {
                    round_dp(((ref_mid - limit_price) / ref_mid) * 10_000.0, 2)
                } else {
                    0.0
                };
                let alert = WorkingOrderPlacedAlert {
                    symbol: req.asset.symbol.clone(),
                    side: req.side,
                    risk_tier: cfg.risk_tier(),
                    leverage_multiplier: Some(cfg.target_leverage_multiplier()),
                    fsm_state: OrderFsmState::WorkingAlo.as_str().to_string(),
                    limit_price,
                    requested_qty: qty,
                    reserved_margin_usdc: reserved_usdc,
                    available_cash_usdc: (snap.available_cash_usdc - reserved_usdc).max(0.0),
                    distance_from_mid_bps: dist_bps,
                    s1_weight: req.s1_weight_contrib,
                    s2_weight: req.s2_weight_contrib,
                    s3_weight: req.s3_weight_contrib,
                    timestamp_ms: now_ms(),
                };
                let html = format_working_order_placed_html(&alert);
                let payload = serde_json::to_value(&alert).ok();
                let _ = self.alerts.enqueue_html_alert(
                    "WORKING_ORDER_RESERVED",
                    "INFO",
                    &html,
                    payload.as_ref(),
                );
                self.wake_alert_worker();
            }
            pos_id
        };

        if let Some((fill_px, fill_qty, fee_usdc)) = live_filled_immediately {
            self.finalize_fill_to_open(
                pos_id,
                &req.asset,
                req.side,
                fill_px,
                fill_qty,
                fee_usdc,
                exchange_oid.as_deref(),
                "HL_IMMEDIATE_FILL_ON_SUBMIT",
                &cfg,
            )?;
        }

        Ok(pos_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn probe_uncertain_working_submit_by_cloid(
        &self,
        working_position_id: Option<i64>,
        asset: &ExchangeAsset,
        side: OrderSide,
        is_scale_in: bool,
        expected_cloid: &str,
        requested_qty: f64,
        maker_fee_bps: f64,
        cfg: &ConfigMap,
    ) -> UncertainSubmitProbe {
        let wallet = cfg.get_str("wallet_address", "");
        if wallet.trim().is_empty() {
            return UncertainSubmitProbe::ProbeUnreachable;
        }
        match self
            .http
            .fetch_order_status_by_cloid(&wallet, expected_cloid, cfg)
        {
            Ok(Some(st)) => {
                let st_lower = st.status.trim().to_ascii_lowercase();
                let eoid = (st.oid > 0).then(|| format!("hl-{}", st.oid));
                if st_lower == "filled"
                    || (st.filled_sz > 1e-9
                        && core_is_entry_fill_complete(
                            st.filled_sz,
                            requested_qty,
                            asset.lot_step_size,
                            asset.sz_decimals,
                        ))
                {
                    let fill_qty = if st.filled_sz > 1e-9 {
                        st.filled_sz
                    } else {
                        requested_qty
                    };
                    let fill_px = if st.avg_px > 0.0 {
                        st.avg_px
                    } else {
                        st.limit_px
                    };
                    let est_fee = round_dp(fill_px * fill_qty * (maker_fee_bps / 10_000.0), 4);
                    return UncertainSubmitProbe::Filled {
                        eoid,
                        fill_px,
                        fill_qty,
                        est_fee,
                    };
                }
                if matches!(
                    st_lower.as_str(),
                    "open" | "resting" | "triggered" | "waitingfortrigger" | "waitingforfill"
                ) {
                    return UncertainSubmitProbe::Resting { eoid };
                }
                UncertainSubmitProbe::ConfirmedAbsent
            }
            Ok(None) => {
                if let Ok(front) = self.http.fetch_frontend_open_orders(&wallet, cfg) {
                    for ord in &front {
                        if ord
                            .get("cloid")
                            .and_then(Value::as_str)
                            .is_some_and(|c| c.trim().eq_ignore_ascii_case(expected_cloid))
                        {
                            let eoid = extract_order_oid(ord).map(|o| format!("hl-{o}"));
                            return UncertainSubmitProbe::Resting { eoid };
                        }
                    }
                }
                let ch_res = if working_position_id.is_some() || is_scale_in {
                    self.query_working_exchange_position_state(
                        working_position_id,
                        asset,
                        side,
                        cfg,
                    )
                } else {
                    self.query_exchange_position_state(asset, side, cfg)
                };
                if let Ok(Some((fill_px, fill_qty))) = ch_res {
                    let est_fee = round_dp(fill_px * fill_qty * (maker_fee_bps / 10_000.0), 4);
                    return UncertainSubmitProbe::Filled {
                        eoid: None,
                        fill_px,
                        fill_qty,
                        est_fee,
                    };
                }
                UncertainSubmitProbe::ConfirmedAbsent
            }
            Err(_) => UncertainSubmitProbe::ProbeUnreachable,
        }
    }

    /// Advances a single `WORKING` order from `v_assets_ordered` during the 1-second surveillance loop.
    pub fn advance_working_order(&self, ordered: &OrderedAssetView) -> Result<ChaseStepOutcome> {
        self.advance_working_order_with_ch_state(ordered, None)
    }

    /// Advances a single `WORKING` order using an optional pre-fetched `clearinghouseState` snapshot
    /// shared across the 1s `PriorityMonitor` tick (`R14-H4`, `R14-H2`).
    pub fn advance_working_order_with_ch_state(
        &self,
        ordered: &OrderedAssetView,
        pre_fetched_ch_state: Option<&Value>,
    ) -> Result<ChaseStepOutcome> {
        let (cfg, ccfg, asset, pos, is_live_exchange) = {
            let _guard = acquire_execution_lock();
            let cfg = self.db.load_config()?;
            let ccfg = ChaserConfig::from_config(&cfg);
            let asset = self.db.get_asset_by_id(ordered.asset_id)?.ok_or_else(|| {
                AegisError::Database(format!("Asset {} not found", ordered.asset_id))
            })?;
            let pos = self
                .db
                .get_position_by_id(ordered.position_id)?
                .ok_or_else(|| {
                    AegisError::Database(format!("Position {} not found", ordered.position_id))
                })?;
            let is_live_exchange = crate::engine::is_live_exchange_active(&self.db, &cfg);
            (cfg, ccfg, asset, pos, is_live_exchange)
        };

        if pos.status != "WORKING" {
            self.working_initial_limits
                .lock()
                .remove(&ordered.position_id);
            return Ok(ChaseStepOutcome::Resting {
                position_id: ordered.position_id,
                symbol: ordered.symbol.clone(),
                limit_price: ordered.limit_price,
                distance_from_mid_bps: ordered.distance_from_mid_bps,
            });
        }

        if !cfg.trading_run_mode().allows_new_entries() {
            if is_live_exchange {
                let ccfg_ro = ChaserConfig::from_config(&cfg);
                let side_ro = OrderSide::parse(&ordered.side)?;
                if let Some(outcome) = self.try_finalize_existing_exchange_fill(
                    ordered,
                    &asset,
                    side_ro,
                    &ccfg_ro,
                    pre_fetched_ch_state,
                    &cfg,
                )? {
                    return Ok(outcome);
                }
            }
            return Ok(ChaseStepOutcome::Resting {
                position_id: ordered.position_id,
                symbol: ordered.symbol.clone(),
                limit_price: ordered.limit_price,
                distance_from_mid_bps: ordered.distance_from_mid_bps,
            });
        }

        let side = OrderSide::parse(&ordered.side)?;
        let live_mid = if asset.mid_price > 0.0 {
            asset.mid_price
        } else if ordered.mid_price > 0.0 {
            ordered.mid_price
        } else if asset.mark_price > 0.0 {
            asset.mark_price
        } else {
            ordered.limit_price
        };
        let eff_tick = effective_hl_tick_size(live_mid, asset.tick_size, asset.sz_decimals)
            .unwrap_or_else(|| asset.tick_size.max(1e-8));

        if is_live_exchange && cfg.get_str("private_key", "").trim().is_empty() {
            tracing::error!(
                position_id = ordered.position_id,
                symbol = %ordered.symbol,
                "private_key is empty/missing in live mode; keeping WORKING order resting"
            );
            return Ok(ChaseStepOutcome::Resting {
                position_id: ordered.position_id,
                symbol: ordered.symbol.clone(),
                limit_price: ordered.limit_price,
                distance_from_mid_bps: ordered.distance_from_mid_bps,
            });
        }

        if !is_live_exchange {
            if let Some(outcome) = self.advance_paper_working_order(
                ordered, &asset, side, live_mid, eff_tick, &ccfg, &cfg,
            )? {
                return Ok(outcome);
            }
        }

        // Fail-closed tick calculation (`R14-M8`): never chase on invalid tick/mid.
        let Some(one_tick_bps) = compute_one_tick_bps_opt(eff_tick, live_mid) else {
            tracing::warn!(
                position_id = ordered.position_id,
                symbol = %ordered.symbol,
                tick = eff_tick,
                live_mid = live_mid,
                "Invalid tick or mid price for WORKING order; keeping Resting fail-closed"
            );
            return Ok(ChaseStepOutcome::Resting {
                position_id: ordered.position_id,
                symbol: ordered.symbol.clone(),
                limit_price: ordered.limit_price,
                distance_from_mid_bps: ordered.distance_from_mid_bps,
            });
        };

        let effective_chase_threshold_bps = ccfg
            .chase_threshold_bps
            .max(one_tick_bps * ALO_ONE_TICK_THRESHOLD_MULT);
        let abs_dist_bps = if live_mid > 0.0 {
            ((live_mid - ordered.limit_price).abs() / live_mid) * 10_000.0
        } else {
            0.0
        };
        let entry_urgency = self.resolve_entry_urgency(ordered, &cfg);
        let is_maker_only = entry_urgency == EntryUrgency::MakerOnly;
        let rest_ttl_ms = if is_maker_only {
            cfg.get_i64("maker_only_rest_ttl_ms", 60_000)
                .clamp(5_000, 600_000)
        } else {
            ENTRY_PARTIAL_GRACE_MS
        };
        let is_expired = now_ms().saturating_sub(ordered.created_at_ms) >= rest_ttl_ms;
        let missing_exchange_order =
            is_live_exchange && ordered.exchange_order_id.as_deref() == Some("");

        if is_live_exchange
            && !is_expired
            && ((ordered.chase_count < ccfg.max_chases
                && abs_dist_bps < effective_chase_threshold_bps)
                || (is_maker_only && ordered.chase_count >= ccfg.max_chases))
        {
            if let Some(outcome) = self.try_finalize_existing_exchange_fill(
                ordered,
                &asset,
                side,
                &ccfg,
                pre_fetched_ch_state,
                &cfg,
            )? {
                return Ok(outcome);
            }
        }

        if is_live_exchange && is_expired {
            return self.handle_expired_working_order(
                ordered,
                &asset,
                side,
                &ccfg,
                pre_fetched_ch_state,
                is_maker_only,
                &cfg,
            );
        }

        if !is_live_exchange && is_maker_only && is_expired {
            let _guard = acquire_execution_lock();
            self.db
                .cancel_working_order(ordered.position_id, "HL_ALO_EXPIRED_CANCELLED")?;
            self.working_initial_limits
                .lock()
                .remove(&ordered.position_id);
            self.working_entry_urgencies
                .lock()
                .remove(&ordered.position_id);
            return Ok(ChaseStepOutcome::Cancelled {
                position_id: ordered.position_id,
                symbol: ordered.symbol.clone(),
                reason: "HL_ALO_EXPIRED_CANCELLED".to_string(),
            });
        }

        if ordered.chase_count >= ccfg.max_chases {
            if is_maker_only {
                return Ok(ChaseStepOutcome::Resting {
                    position_id: ordered.position_id,
                    symbol: ordered.symbol.clone(),
                    limit_price: ordered.limit_price,
                    distance_from_mid_bps: abs_dist_bps,
                });
            }
            if let Err(breach_err) = self
                .check_pre_ioc_heat_and_gross_cap(ordered, &asset, side, live_mid, eff_tick, &cfg)
            {
                tracing::warn!(
                    position_id = ordered.position_id,
                    symbol = %ordered.symbol,
                    error = %breach_err,
                    "Aborting WORKING order prior to CROSSING_IOC due to portfolio heat/gross cap"
                );
                return self.abort_working_order_with_cooldown(
                    ordered,
                    &asset,
                    side,
                    is_live_exchange,
                    ccfg.maker_fee_bps,
                    "HL_PRE_IOC_HEAT_CAP_CANCELLED",
                    0,
                    &cfg,
                );
            }
            return self.escalate_working_to_crossing_ioc(
                ordered,
                &asset,
                side,
                live_mid,
                abs_dist_bps,
                eff_tick,
                is_live_exchange,
                &ccfg,
                pre_fetched_ch_state,
                &cfg,
            );
        }

        if abs_dist_bps >= effective_chase_threshold_bps || missing_exchange_order {
            return self.chase_working_order(
                ordered,
                &asset,
                side,
                live_mid,
                abs_dist_bps,
                eff_tick,
                is_live_exchange,
                missing_exchange_order,
                &ccfg,
                pre_fetched_ch_state,
                &cfg,
            );
        }

        Ok(ChaseStepOutcome::Resting {
            position_id: ordered.position_id,
            symbol: ordered.symbol.clone(),
            limit_price: ordered.limit_price,
            distance_from_mid_bps: ordered.distance_from_mid_bps,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn advance_paper_working_order(
        &self,
        ordered: &OrderedAssetView,
        asset: &ExchangeAsset,
        side: OrderSide,
        live_mid: f64,
        eff_tick: f64,
        ccfg: &ChaserConfig,
        cfg: &ConfigMap,
    ) -> Result<Option<ChaseStepOutcome>> {
        let price_touched = match side {
            OrderSide::Long => live_mid <= ordered.limit_price * 1.0005,
            OrderSide::Short => live_mid >= ordered.limit_price * 0.9995,
        };
        let is_crossing_ioc = ordered.fsm_state == OrderFsmState::CrossingIoc.as_str();
        if !price_touched && !is_crossing_ioc {
            return Ok(None);
        }

        let fill_price = if is_crossing_ioc {
            snap_to_tick(live_mid, eff_tick)
        } else {
            ordered.limit_price
        };
        let fee_bps = if is_crossing_ioc {
            ccfg.taker_fee_bps
        } else {
            ccfg.maker_fee_bps
        };
        let filled_qty = ordered.requested_quantity;
        let fees_paid_usdc = round_dp(fill_price * filled_qty * (fee_bps / 10_000.0), 4);
        let reason = if is_crossing_ioc {
            "IOC_CROSSING_FILL_CONFIRMED"
        } else {
            "ALO_MAKER_FILL_CONFIRMED"
        };

        let (committed_usdc, stop_px) = self.finalize_fill_to_open(
            ordered.position_id,
            asset,
            side,
            fill_price,
            filled_qty,
            fees_paid_usdc,
            ordered.exchange_order_id.as_deref(),
            reason,
            cfg,
        )?;

        Ok(Some(ChaseStepOutcome::FilledOpen {
            position_id: ordered.position_id,
            asset_id: ordered.asset_id,
            symbol: ordered.symbol.clone(),
            side: side.as_str().to_string(),
            fill_price,
            filled_quantity: filled_qty,
            committed_notional_usdc: committed_usdc,
            protective_stop_px: stop_px,
            fees_paid_usdc,
        }))
    }

    fn evaluate_working_drift_and_spread_abort(
        &self,
        ordered: &OrderedAssetView,
        asset: &ExchangeAsset,
        candidate_px: f64,
        initial_limit_px: f64,
        l2_bbo: Option<(f64, f64)>,
        ccfg: &ChaserConfig,
    ) -> Option<String> {
        if initial_limit_px.is_finite() && initial_limit_px > 0.0 && candidate_px.is_finite() {
            let drift_bps = ((candidate_px - initial_limit_px).abs() / initial_limit_px) * 10_000.0;
            if drift_bps > ccfg.max_alo_chase_drift_bps {
                tracing::warn!(
                    position_id = ordered.position_id,
                    symbol = %ordered.symbol,
                    initial_limit_px,
                    candidate_px,
                    drift_bps,
                    max_drift_bps = ccfg.max_alo_chase_drift_bps,
                    "Aborting WORKING chase: cumulative drift exceeded max_alo_chase_drift_bps"
                );
                return Some("HL_ALO_CHASE_DRIFT_ABORT".to_string());
            }
        }
        if asset.oracle_price.is_finite() && asset.oracle_price > 0.0 && candidate_px.is_finite() {
            let oracle_drift_bps =
                ((candidate_px - asset.oracle_price).abs() / asset.oracle_price) * 10_000.0;
            if oracle_drift_bps > ccfg.max_alo_oracle_drift_bps {
                tracing::warn!(
                    position_id = ordered.position_id,
                    symbol = %ordered.symbol,
                    oracle_price = asset.oracle_price,
                    candidate_px,
                    oracle_drift_bps,
                    max_oracle_drift_bps = ccfg.max_alo_oracle_drift_bps,
                    "Aborting WORKING chase: oracle drift exceeded max_alo_oracle_drift_bps"
                );
                return Some("HL_ALO_ORACLE_DRIFT_ABORT".to_string());
            }
        }
        if let Some((bid, ask)) =
            l2_bbo.filter(|(b, a)| b.is_finite() && a.is_finite() && *b > 0.0 && *a >= *b)
        {
            let mid_bbo = (bid + ask) * 0.5;
            if mid_bbo > 0.0 {
                let spread_bps = ((ask - bid) / mid_bbo) * 10_000.0;
                if spread_bps > ccfg.max_entry_spread_bps {
                    tracing::warn!(
                        position_id = ordered.position_id,
                        symbol = %ordered.symbol,
                        spread_bps,
                        max_entry_spread_bps = ccfg.max_entry_spread_bps,
                        "Aborting WORKING chase: live L2 spread exceeded max_entry_spread_bps"
                    );
                    return Some("HL_ALO_SPREAD_ABORT".to_string());
                }
            }
        }
        None
    }

    #[allow(clippy::too_many_arguments)]
    fn abort_working_order_with_cooldown(
        &self,
        ordered: &OrderedAssetView,
        asset: &ExchangeAsset,
        side: OrderSide,
        is_live_exchange: bool,
        maker_fee_bps: f64,
        abort_reason: &str,
        cooldown_sec: i64,
        cfg: &ConfigMap,
    ) -> Result<ChaseStepOutcome> {
        let resting_outcome = || ChaseStepOutcome::Resting {
            position_id: ordered.position_id,
            symbol: ordered.symbol.clone(),
            limit_price: ordered.limit_price,
            distance_from_mid_bps: ordered.distance_from_mid_bps,
        };

        if is_live_exchange {
            if matches!(
                self.cancel_working_entry_oid(ordered, asset, cfg),
                CancelEntryOutcome::Aborted
            ) {
                return Ok(resting_outcome());
            }
            match self.query_working_exchange_position_state(
                Some(ordered.position_id),
                asset,
                side,
                cfg,
            ) {
                Ok(Some((fill_px, fill_qty))) if fill_qty > 1e-9 => {
                    let est_fee = round_dp(fill_px * fill_qty * (maker_fee_bps / 10_000.0), 4);
                    return self.finalize_known_fill_outcome(
                        ordered,
                        asset,
                        side,
                        fill_px,
                        fill_qty,
                        est_fee,
                        "HL_ALO_ABORT_RESCUED_FILL_OPEN",
                        cfg,
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        position_id = ordered.position_id,
                        symbol = %ordered.symbol,
                        error = %e,
                        "clearinghouseState unreachable after chase abort cancel; deferring cancel"
                    );
                    return Ok(resting_outcome());
                }
                Ok(_) => {}
            }
        }

        {
            let _guard = acquire_execution_lock();
            self.db
                .cancel_working_order(ordered.position_id, abort_reason)?;
            self.working_initial_limits
                .lock()
                .remove(&ordered.position_id);
            self.working_entry_urgencies
                .lock()
                .remove(&ordered.position_id);
            if cooldown_sec > 0 {
                let cd_msg = format!("CHURN_GUARD: {} aborted ({abort_reason})", ordered.symbol);
                let _ = self.db.insert_asset_cooldown(
                    ordered.asset_id,
                    "CHURN_GUARD",
                    &cd_msg,
                    cooldown_sec,
                );
            }
        }
        Ok(ChaseStepOutcome::Cancelled {
            position_id: ordered.position_id,
            symbol: ordered.symbol.clone(),
            reason: abort_reason.to_string(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn escalate_working_to_crossing_ioc(
        &self,
        ordered: &OrderedAssetView,
        asset: &ExchangeAsset,
        side: OrderSide,
        live_mid: f64,
        abs_dist_bps: f64,
        eff_tick: f64,
        is_live_exchange: bool,
        ccfg: &ChaserConfig,
        pre_fetched_ch_state: Option<&Value>,
        cfg: &ConfigMap,
    ) -> Result<ChaseStepOutcome> {
        let initial_limit_px =
            self.resolve_initial_limit_price(ordered.position_id, ordered.limit_price);

        let l2_bbo = if is_live_exchange {
            self.fetch_fresh_l2_bbo(&asset.coin, cfg)
        } else {
            None
        };

        let ioc_price = if is_live_exchange {
            let oracle_anchor = if asset.oracle_price.is_finite() && asset.oracle_price > 0.0 {
                asset.oracle_price
            } else {
                live_mid
            };
            let band_lo = (oracle_anchor * (1.0 - HL_ORACLE_BAND_PCT)).max(eff_tick);
            let band_hi = (oracle_anchor * (1.0 + HL_ORACLE_BAND_PCT)).max(band_lo);
            let anchor = l2_bbo
                .map(|(bid, ask)| if side.is_buy() { ask } else { bid })
                .filter(|&p| p > 0.0)
                .unwrap_or(live_mid);
            let slip = ccfg.entry_ioc_max_slippage_bps / 10_000.0;
            let raw = if side.is_buy() {
                anchor * (1.0 + slip)
            } else {
                (anchor * (1.0 - slip)).max(eff_tick)
            };
            snap_to_tick(raw.clamp(band_lo, band_hi), eff_tick)
        } else {
            snap_to_tick(live_mid, eff_tick)
        };

        if let Some(abort_reason) = self.evaluate_working_drift_and_spread_abort(
            ordered,
            asset,
            ioc_price,
            initial_limit_px,
            l2_bbo,
            ccfg,
        ) {
            return self.abort_working_order_with_cooldown(
                ordered,
                asset,
                side,
                is_live_exchange,
                ccfg.maker_fee_bps,
                &abort_reason,
                ccfg.chase_reject_cooldown_sec,
                cfg,
            );
        }

        if is_live_exchange {
            let pk = cfg.get_str("private_key", "");
            if matches!(
                self.cancel_working_entry_oid(ordered, asset, cfg),
                CancelEntryOutcome::Aborted
            ) {
                return Ok(ChaseStepOutcome::Resting {
                    position_id: ordered.position_id,
                    symbol: ordered.symbol.clone(),
                    limit_price: ordered.limit_price,
                    distance_from_mid_bps: ordered.distance_from_mid_bps,
                });
            }

            // Re-check exchange state (using pre-fetched state only if no cancel wire had to be sent)
            let had_resting_oid = parse_hl_oid(ordered.exchange_order_id.as_deref()).is_some();
            let live_pos_opt = if !had_resting_oid {
                if let Some(ch_val) = pre_fetched_ch_state {
                    Ok(self.extract_working_position_state(
                        ch_val,
                        ordered.position_id,
                        asset,
                        side,
                    ))
                } else {
                    self.query_working_exchange_position_state(
                        Some(ordered.position_id),
                        asset,
                        side,
                        cfg,
                    )
                }
            } else {
                self.query_working_exchange_position_state(
                    Some(ordered.position_id),
                    asset,
                    side,
                    cfg,
                )
            };

            let live_pos_opt = match live_pos_opt {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        symbol = %ordered.symbol,
                        error = %e,
                        "clearinghouseState unreachable before CROSSING_IOC escalation; keeping WORKING"
                    );
                    return Ok(ChaseStepOutcome::Resting {
                        position_id: ordered.position_id,
                        symbol: ordered.symbol.clone(),
                        limit_price: ordered.limit_price,
                        distance_from_mid_bps: ordered.distance_from_mid_bps,
                    });
                }
            };
            let (live_fill_px, live_qty) = live_pos_opt.unwrap_or((live_mid, 0.0));
            let rem_qty = core_remaining_entry_qty_for_lot(
                live_qty,
                ordered.requested_quantity,
                asset.lot_step_size,
                asset.sz_decimals,
            );
            if core_is_entry_fill_complete(
                live_qty,
                ordered.requested_quantity,
                asset.lot_step_size,
                asset.sz_decimals,
            ) {
                return self.finalize_known_fill_outcome(
                    ordered,
                    asset,
                    side,
                    live_fill_px,
                    live_qty,
                    round_dp(live_fill_px * live_qty * (ccfg.maker_fee_bps / 10_000.0), 4),
                    "HL_ALO_FILL_DETECTED_ON_EXCHANGE",
                    cfg,
                );
            }
            if rem_qty <= asset.lot_step_size * 0.5 || rem_qty <= 1e-9 {
                let _guard = acquire_execution_lock();
                self.db
                    .cancel_working_order(ordered.position_id, "HL_IOC_ZERO_REM_CANCELLED")?;
                self.working_initial_limits
                    .lock()
                    .remove(&ordered.position_id);
                self.working_entry_urgencies
                    .lock()
                    .remove(&ordered.position_id);
                return Ok(ChaseStepOutcome::Cancelled {
                    position_id: ordered.position_id,
                    symbol: ordered.symbol.clone(),
                    reason: "HL_IOC_ZERO_REM_CANCELLED".to_string(),
                });
            }

            let wire = build_limit_order_wire(
                asset.asset_index,
                side.is_buy(),
                ioc_price,
                rem_qty,
                eff_tick,
                asset.lot_step_size,
                asset.sz_decimals,
                false,
                "Ioc",
                Some(&format!("{}-ioc", ordered.client_order_id)),
            );
            match self.http.submit_order_wire(
                wire,
                rem_qty,
                ioc_price,
                ccfg.taker_fee_bps,
                &pk,
                Some(&self.db),
                cfg,
            ) {
                Ok(ack) if ack.accepted && ack.status == "FILLED" && ack.filled_quantity > 0.0 => {
                    let ioc_eoid = ack.exchange_order_id.or(ordered.exchange_order_id.clone());
                    let total_qty = live_qty + ack.filled_quantity;
                    let blended_px = if total_qty > 1e-9 {
                        (live_fill_px * live_qty + ack.avg_fill_price * ack.filled_quantity)
                            / total_qty
                    } else {
                        ack.avg_fill_price
                    };
                    let prior_fee = if live_qty > 1e-9 {
                        round_dp(live_fill_px * live_qty * (ccfg.maker_fee_bps / 10_000.0), 4)
                    } else {
                        0.0
                    };
                    let total_fee = round_dp(prior_fee + ack.fee_usdc, 4);
                    {
                        let _guard = acquire_execution_lock();
                        let _ = self.db.record_working_order_chase(
                            ordered.position_id,
                            blended_px,
                            OrderFsmState::CrossingIoc.as_str(),
                            ioc_eoid.as_deref(),
                            &format!(
                                "Escalated to CROSSING_IOC after {} ALO chases (filled {:.4} @ ${:.6})",
                                ordered.chase_count, total_qty, blended_px
                            ),
                        )?;
                    }
                    let (committed_usdc, stop_px) = self.finalize_fill_to_open(
                        ordered.position_id,
                        asset,
                        side,
                        blended_px,
                        total_qty,
                        total_fee,
                        ioc_eoid.as_deref(),
                        "HL_IOC_ESCALATION_FILLED",
                        cfg,
                    )?;
                    return Ok(ChaseStepOutcome::FilledOpen {
                        position_id: ordered.position_id,
                        asset_id: ordered.asset_id,
                        symbol: ordered.symbol.clone(),
                        side: side.as_str().to_string(),
                        fill_price: blended_px,
                        filled_quantity: total_qty,
                        committed_notional_usdc: committed_usdc,
                        protective_stop_px: stop_px,
                        fees_paid_usdc: total_fee,
                    });
                }
                Ok(ack) => {
                    tracing::warn!(
                        symbol = %ordered.symbol,
                        position_id = ordered.position_id,
                        chase_count = ordered.chase_count,
                        limit_price = ioc_price,
                        accepted = ack.accepted,
                        status = %ack.status,
                        error = ?ack.raw_error,
                        "Hyperliquid CROSSING_IOC order rejected or unfilled"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        symbol = %ordered.symbol,
                        position_id = ordered.position_id,
                        chase_count = ordered.chase_count,
                        limit_price = ioc_price,
                        error = %e,
                        "Hyperliquid CROSSING_IOC submit_order_wire failed"
                    );
                }
            }

            match self.query_working_exchange_position_state(
                Some(ordered.position_id),
                asset,
                side,
                cfg,
            ) {
                Ok(Some((post_px, post_qty))) if post_qty > 1e-9 => {
                    let est_fee = if post_qty <= live_qty + 1e-9 {
                        round_dp(post_px * post_qty * (ccfg.maker_fee_bps / 10_000.0), 4)
                    } else {
                        round_dp(post_px * post_qty * (ccfg.taker_fee_bps / 10_000.0), 4)
                    };
                    return self.finalize_known_fill_outcome(
                        ordered,
                        asset,
                        side,
                        post_px,
                        post_qty,
                        est_fee,
                        "HL_IOC_FILL_CONFIRMED_ON_STATE",
                        cfg,
                    );
                }
                Err(e) => {
                    if live_qty > 1e-9 {
                        return self.finalize_known_fill_outcome(
                            ordered,
                            asset,
                            side,
                            live_fill_px,
                            live_qty,
                            round_dp(live_fill_px * live_qty * (ccfg.maker_fee_bps / 10_000.0), 4),
                            "HL_ALO_PARTIAL_FILL_ON_EXCHANGE",
                            cfg,
                        );
                    }
                    tracing::warn!(
                        symbol = %ordered.symbol,
                        error = %e,
                        "clearinghouseState unreachable after IOC submit; deferring cancel"
                    );
                    return Ok(ChaseStepOutcome::Resting {
                        position_id: ordered.position_id,
                        symbol: ordered.symbol.clone(),
                        limit_price: ordered.limit_price,
                        distance_from_mid_bps: ordered.distance_from_mid_bps,
                    });
                }
                Ok(_) => {}
            }

            {
                let _guard = acquire_execution_lock();
                self.db
                    .cancel_working_order(ordered.position_id, "HL_IOC_UNFILLED_CANCELLED")?;
                self.working_initial_limits
                    .lock()
                    .remove(&ordered.position_id);
                self.working_entry_urgencies
                    .lock()
                    .remove(&ordered.position_id);
                self.record_alo_expiry_and_maybe_cooldown(
                    ordered.asset_id,
                    &ordered.symbol,
                    "HL_IOC_UNFILLED_CANCELLED",
                    ccfg,
                );
            }
            return Ok(ChaseStepOutcome::Cancelled {
                position_id: ordered.position_id,
                symbol: ordered.symbol.clone(),
                reason: "HL_IOC_UNFILLED_CANCELLED".to_string(),
            });
        }

        let ioc_eoid = ordered.exchange_order_id.clone();
        {
            let _guard = acquire_execution_lock();
            let _ = self.db.record_working_order_chase(
                ordered.position_id,
                ioc_price,
                OrderFsmState::CrossingIoc.as_str(),
                ioc_eoid.as_deref(),
                &format!(
                    "Escalated to CROSSING_IOC after {} ALO chases (drift {:.2} bps)",
                    ordered.chase_count, abs_dist_bps
                ),
            )?;
        }

        let filled_qty = ordered.requested_quantity;
        let fees_paid_usdc = round_dp(ioc_price * filled_qty * (ccfg.taker_fee_bps / 10_000.0), 4);
        let (committed_usdc, stop_px) = self.finalize_fill_to_open(
            ordered.position_id,
            asset,
            side,
            ioc_price,
            filled_qty,
            fees_paid_usdc,
            ioc_eoid.as_deref(),
            "IOC_ESCALATION_FILLED",
            cfg,
        )?;

        Ok(ChaseStepOutcome::FilledOpen {
            position_id: ordered.position_id,
            asset_id: ordered.asset_id,
            symbol: ordered.symbol.clone(),
            side: side.as_str().to_string(),
            fill_price: ioc_price,
            filled_quantity: filled_qty,
            committed_notional_usdc: committed_usdc,
            protective_stop_px: stop_px,
            fees_paid_usdc,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn chase_working_order(
        &self,
        ordered: &OrderedAssetView,
        asset: &ExchangeAsset,
        side: OrderSide,
        live_mid: f64,
        abs_dist_bps: f64,
        eff_tick: f64,
        is_live_exchange: bool,
        missing_exchange_order: bool,
        ccfg: &ChaserConfig,
        pre_fetched_ch_state: Option<&Value>,
        cfg: &ConfigMap,
    ) -> Result<ChaseStepOutcome> {
        let resting_outcome = || ChaseStepOutcome::Resting {
            position_id: ordered.position_id,
            symbol: ordered.symbol.clone(),
            limit_price: ordered.limit_price,
            distance_from_mid_bps: ordered.distance_from_mid_bps,
        };

        let initial_limit_px =
            self.resolve_initial_limit_price(ordered.position_id, ordered.limit_price);

        let l2_bbo = if is_live_exchange {
            self.fetch_fresh_l2_bbo(&asset.coin, cfg)
        } else {
            None
        };
        let mut new_limit_price = clamp_chase_alo_price(side, live_mid, l2_bbo, eff_tick);
        new_limit_price = ensure_wire_price_strictly_passive(
            side,
            new_limit_price,
            l2_bbo,
            eff_tick,
            asset.sz_decimals,
        );

        // Native ALO 1-tick wire-equivalence & 1-tick BBO spread queue preservation (`Sec. 11`)
        let old_wire_px = format_hl_wire_price(ordered.limit_price, eff_tick, asset.sz_decimals);
        let new_wire_px = format_hl_wire_price(new_limit_price, eff_tick, asset.sz_decimals);
        let one_tick_bbo_keep = l2_bbo.is_some_and(|(bid, ask)| {
            ask - bid <= eff_tick * 1.05
                && if side.is_buy() {
                    (ordered.limit_price - bid).abs() < eff_tick * 0.5
                } else {
                    (ordered.limit_price - ask).abs() < eff_tick * 0.5
                }
        });
        if !missing_exchange_order
            && ((new_limit_price - ordered.limit_price).abs() < eff_tick * 0.5
                || new_wire_px == old_wire_px
                || one_tick_bbo_keep)
        {
            return Ok(resting_outcome());
        }

        if let Some(abort_reason) = self.evaluate_working_drift_and_spread_abort(
            ordered,
            asset,
            new_limit_price,
            initial_limit_px,
            l2_bbo,
            ccfg,
        ) {
            return self.abort_working_order_with_cooldown(
                ordered,
                asset,
                side,
                is_live_exchange,
                ccfg.maker_fee_bps,
                &abort_reason,
                ccfg.chase_reject_cooldown_sec,
                cfg,
            );
        }

        let mut final_limit_price = new_limit_price;
        let mut new_eoid = ordered.exchange_order_id.clone();
        let mut chase_reason = format!(
            "1s top-of-book ALO chase #{}: mid ${:.6} drifted {:.2} bps",
            ordered.chase_count.saturating_add(1),
            live_mid,
            abs_dist_bps
        );

        if is_live_exchange {
            let pk = cfg.get_str("private_key", "");
            if matches!(
                self.cancel_working_entry_oid(ordered, asset, cfg),
                CancelEntryOutcome::Aborted
            ) {
                return Ok(resting_outcome());
            }

            let had_resting_oid = parse_hl_oid(ordered.exchange_order_id.as_deref()).is_some();
            let live_pos_opt = if !had_resting_oid {
                if let Some(ch_val) = pre_fetched_ch_state {
                    Ok(self.extract_working_position_state(
                        ch_val,
                        ordered.position_id,
                        asset,
                        side,
                    ))
                } else {
                    self.query_working_exchange_position_state(
                        Some(ordered.position_id),
                        asset,
                        side,
                        cfg,
                    )
                }
            } else {
                self.query_working_exchange_position_state(
                    Some(ordered.position_id),
                    asset,
                    side,
                    cfg,
                )
            };
            let live_pos_opt = match live_pos_opt {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        symbol = %ordered.symbol,
                        error = %e,
                        "clearinghouseState unreachable before ALO chase; keeping WORKING"
                    );
                    return Ok(resting_outcome());
                }
            };
            let (live_fill_px, live_qty) = live_pos_opt.unwrap_or((live_mid, 0.0));
            let rem_qty = core_remaining_entry_qty_for_lot(
                live_qty,
                ordered.requested_quantity,
                asset.lot_step_size,
                asset.sz_decimals,
            );
            if core_is_entry_fill_complete(
                live_qty,
                ordered.requested_quantity,
                asset.lot_step_size,
                asset.sz_decimals,
            ) {
                return self.finalize_known_fill_outcome(
                    ordered,
                    asset,
                    side,
                    live_fill_px,
                    live_qty,
                    round_dp(live_fill_px * live_qty * (ccfg.maker_fee_bps / 10_000.0), 4),
                    "HL_ALO_FILL_DETECTED_ON_EXCHANGE",
                    cfg,
                );
            }
            if rem_qty <= asset.lot_step_size * 0.5 || rem_qty <= 1e-9 {
                let _guard = acquire_execution_lock();
                self.db
                    .cancel_working_order(ordered.position_id, "HL_CHASE_ZERO_REM_CANCELLED")?;
                self.working_initial_limits
                    .lock()
                    .remove(&ordered.position_id);
                self.working_entry_urgencies
                    .lock()
                    .remove(&ordered.position_id);
                return Ok(ChaseStepOutcome::Cancelled {
                    position_id: ordered.position_id,
                    symbol: ordered.symbol.clone(),
                    reason: "HL_CHASE_ZERO_REM_CANCELLED".to_string(),
                });
            }

            let chase_coid = format!(
                "{}-c{}",
                ordered.client_order_id,
                ordered.chase_count.saturating_add(1)
            );
            let wire = build_limit_order_wire(
                asset.asset_index,
                side.is_buy(),
                new_limit_price,
                rem_qty,
                eff_tick,
                asset.lot_step_size,
                asset.sz_decimals,
                false,
                "Alo",
                Some(&chase_coid),
            );
            match self.http.submit_order_wire(
                wire,
                rem_qty,
                new_limit_price,
                ccfg.maker_fee_bps,
                &pk,
                Some(&self.db),
                cfg,
            ) {
                Ok(ack) if ack.accepted => {
                    if ack.status == "FILLED" && ack.filled_quantity > 0.0 {
                        return self.finalize_accepted_chase_fill(
                            ordered,
                            asset,
                            side,
                            live_fill_px,
                            live_qty,
                            ack.avg_fill_price,
                            ack.filled_quantity,
                            ack.fee_usdc,
                            ack.exchange_order_id.as_deref(),
                            ccfg.maker_fee_bps,
                            cfg,
                        );
                    }
                    if let Some(id) = ack.exchange_order_id {
                        new_eoid = Some(id);
                    }
                }
                submit_failure => {
                    let is_transport_err = submit_failure.is_err();
                    let (is_crossing_reject, reject_summary) = match &submit_failure {
                        Ok(ack) => {
                            tracing::warn!(
                                symbol = %ordered.symbol,
                                position_id = ordered.position_id,
                                chase_count = ordered.chase_count,
                                limit_price = new_limit_price,
                                status = %ack.status,
                                error = ?ack.raw_error,
                                "Hyperliquid rejected ALO chase order"
                            );
                            (
                                is_post_only_crossing_rejection(ack.raw_error.as_deref()),
                                ack.raw_error.clone().unwrap_or_else(|| ack.status.clone()),
                            )
                        }
                        Err(err) => {
                            tracing::warn!(
                                symbol = %ordered.symbol,
                                position_id = ordered.position_id,
                                chase_count = ordered.chase_count,
                                limit_price = new_limit_price,
                                error = %err,
                                "Hyperliquid ALO chase submit_order_wire failed; probing cloid before reject cancel"
                            );
                            (false, err.to_string())
                        }
                    };

                    if is_transport_err && !is_crossing_reject {
                        let expected_cloid = derive_hl_cloid_hex(&chase_coid);
                        match self.probe_uncertain_working_submit_by_cloid(
                            Some(ordered.position_id),
                            asset,
                            side,
                            false,
                            &expected_cloid,
                            rem_qty,
                            ccfg.maker_fee_bps,
                            cfg,
                        ) {
                            UncertainSubmitProbe::Filled {
                                eoid,
                                fill_px,
                                fill_qty,
                                est_fee,
                            } => {
                                return self.finalize_accepted_chase_fill(
                                    ordered,
                                    asset,
                                    side,
                                    live_fill_px,
                                    live_qty,
                                    fill_px,
                                    fill_qty,
                                    est_fee,
                                    eoid.as_deref(),
                                    ccfg.maker_fee_bps,
                                    cfg,
                                );
                            }
                            UncertainSubmitProbe::Resting { eoid } => {
                                let adopt_reason = format!(
                                    "1s ALO chase #{} adopted resting exchange order ({}) via cloid probe after submit error",
                                    ordered.chase_count.saturating_add(1),
                                    eoid.as_deref().unwrap_or("cloid")
                                );
                                let _guard = acquire_execution_lock();
                                let new_cc = self.db.record_working_order_chase(
                                    ordered.position_id,
                                    new_limit_price,
                                    OrderFsmState::ChasingAlo.as_str(),
                                    eoid.as_deref(),
                                    &adopt_reason,
                                )?;
                                return Ok(ChaseStepOutcome::Chased {
                                    position_id: ordered.position_id,
                                    symbol: ordered.symbol.clone(),
                                    new_limit_price,
                                    chase_count: new_cc,
                                    fsm_state: OrderFsmState::ChasingAlo.as_str().to_string(),
                                });
                            }
                            UncertainSubmitProbe::ProbeUnreachable => {
                                return Ok(resting_outcome());
                            }
                            UncertainSubmitProbe::ConfirmedAbsent => {}
                        }
                    }

                    if live_qty > 1e-9 {
                        return self.finalize_known_fill_outcome(
                            ordered,
                            asset,
                            side,
                            live_fill_px,
                            live_qty,
                            round_dp(live_fill_px * live_qty * (ccfg.maker_fee_bps / 10_000.0), 4),
                            "HL_ALO_PARTIAL_FILL_ON_EXCHANGE",
                            cfg,
                        );
                    }
                    match self.query_working_exchange_position_state(
                        Some(ordered.position_id),
                        asset,
                        side,
                        cfg,
                    ) {
                        Ok(Some((post_px, post_qty))) if post_qty > 1e-9 => {
                            return self.finalize_known_fill_outcome(
                                ordered,
                                asset,
                                side,
                                post_px,
                                post_qty,
                                round_dp(post_px * post_qty * (ccfg.maker_fee_bps / 10_000.0), 4),
                                "HL_ALO_FILL_DETECTED_ON_EXCHANGE",
                                cfg,
                            );
                        }
                        Err(_) => return Ok(resting_outcome()),
                        Ok(_) => {}
                    }

                    if !is_crossing_reject {
                        self.record_hl_reject_and_maybe_cooldown(
                            ordered.asset_id,
                            &ordered.symbol,
                            &reject_summary,
                            ccfg,
                        );
                        let _guard = acquire_execution_lock();
                        self.db.cancel_working_order(
                            ordered.position_id,
                            "HL_CHASE_REJECT_CANCELLED",
                        )?;
                        self.working_initial_limits
                            .lock()
                            .remove(&ordered.position_id);
                        self.working_entry_urgencies
                            .lock()
                            .remove(&ordered.position_id);
                        return Ok(ChaseStepOutcome::Cancelled {
                            position_id: ordered.position_id,
                            symbol: ordered.symbol.clone(),
                            reason: "HL_CHASE_REJECT_CANCELLED".to_string(),
                        });
                    }

                    new_eoid = Some(String::new());
                    chase_reason = format!(
                        "1s ALO chase #{} rejected ({}); queued retry/IOC escalation",
                        ordered.chase_count.saturating_add(1),
                        reject_summary
                    );

                    if ordered.chase_count.saturating_add(1) < ccfg.max_chases {
                        let fresh_bbo = self
                            .http
                            .fetch_l2_book(&asset.coin, cfg)
                            .ok()
                            .flatten()
                            .filter(|(b, a)| {
                                b.is_finite() && a.is_finite() && *b > 0.0 && *a >= *b
                            });
                        let requote_px = ensure_wire_price_strictly_passive(
                            side,
                            step_more_passive_alo_price(side, new_limit_price, fresh_bbo, eff_tick),
                            fresh_bbo,
                            eff_tick,
                            asset.sz_decimals,
                        );
                        let requote_coid = format!("{chase_coid}-r1");
                        let requote_wire = build_limit_order_wire(
                            asset.asset_index,
                            side.is_buy(),
                            requote_px,
                            rem_qty,
                            eff_tick,
                            asset.lot_step_size,
                            asset.sz_decimals,
                            false,
                            "Alo",
                            Some(&requote_coid),
                        );
                        match self.http.submit_order_wire(
                            requote_wire,
                            rem_qty,
                            requote_px,
                            ccfg.maker_fee_bps,
                            &pk,
                            Some(&self.db),
                            cfg,
                        ) {
                            Ok(re_ack) if re_ack.accepted => {
                                final_limit_price = requote_px;
                                if re_ack.status == "FILLED" && re_ack.filled_quantity > 0.0 {
                                    return self.finalize_accepted_chase_fill(
                                        ordered,
                                        asset,
                                        side,
                                        live_fill_px,
                                        live_qty,
                                        re_ack.avg_fill_price,
                                        re_ack.filled_quantity,
                                        re_ack.fee_usdc,
                                        re_ack.exchange_order_id.as_deref(),
                                        ccfg.maker_fee_bps,
                                        cfg,
                                    );
                                }
                                new_eoid = re_ack.exchange_order_id;
                                chase_reason = format!(
                                    "1s ALO chase #{} re-quoted passive @ {:.6} after post-only reject ({})",
                                    ordered.chase_count.saturating_add(1),
                                    requote_px,
                                    reject_summary
                                );
                            }
                            Ok(re_ack) => {
                                tracing::warn!(
                                    symbol = %ordered.symbol,
                                    position_id = ordered.position_id,
                                    chase_count = ordered.chase_count,
                                    limit_price = requote_px,
                                    status = %re_ack.status,
                                    error = ?re_ack.raw_error,
                                    "Hyperliquid rejected passive ALO re-quote; continuing chase toward IOC"
                                );
                                final_limit_price = requote_px;
                                chase_reason = format!(
                                    "1s ALO chase #{} rejected ({}) & passive re-quote @ {:.6} rejected ({}); queued retry/IOC escalation",
                                    ordered.chase_count.saturating_add(1),
                                    reject_summary,
                                    requote_px,
                                    re_ack.raw_error.unwrap_or(re_ack.status)
                                );
                            }
                            Err(re_err) => {
                                tracing::warn!(
                                    symbol = %ordered.symbol,
                                    position_id = ordered.position_id,
                                    chase_count = ordered.chase_count,
                                    limit_price = requote_px,
                                    error = %re_err,
                                    "Hyperliquid passive ALO re-quote submit_order_wire failed; probing cloid"
                                );
                                let expected_re_cloid = derive_hl_cloid_hex(&requote_coid);
                                match self.probe_uncertain_working_submit_by_cloid(
                                    Some(ordered.position_id),
                                    asset,
                                    side,
                                    false,
                                    &expected_re_cloid,
                                    rem_qty,
                                    ccfg.maker_fee_bps,
                                    cfg,
                                ) {
                                    UncertainSubmitProbe::Filled {
                                        eoid,
                                        fill_px,
                                        fill_qty,
                                        est_fee,
                                    } => {
                                        return self.finalize_accepted_chase_fill(
                                            ordered,
                                            asset,
                                            side,
                                            live_fill_px,
                                            live_qty,
                                            fill_px,
                                            fill_qty,
                                            est_fee,
                                            eoid.as_deref(),
                                            ccfg.maker_fee_bps,
                                            cfg,
                                        );
                                    }
                                    UncertainSubmitProbe::Resting { eoid } => {
                                        final_limit_price = requote_px;
                                        new_eoid = eoid;
                                        chase_reason = format!(
                                            "1s ALO chase #{} adopted resting passive re-quote @ {:.6} via cloid probe",
                                            ordered.chase_count.saturating_add(1),
                                            requote_px
                                        );
                                    }
                                    UncertainSubmitProbe::ProbeUnreachable => {
                                        return Ok(resting_outcome());
                                    }
                                    UncertainSubmitProbe::ConfirmedAbsent => {
                                        match self.query_working_exchange_position_state(
                                            Some(ordered.position_id),
                                            asset,
                                            side,
                                            cfg,
                                        ) {
                                            Ok(Some((post_px, post_qty))) if post_qty > 1e-9 => {
                                                return self.finalize_known_fill_outcome(
                                                    ordered,
                                                    asset,
                                                    side,
                                                    post_px,
                                                    post_qty,
                                                    round_dp(
                                                        post_px
                                                            * post_qty
                                                            * (ccfg.maker_fee_bps / 10_000.0),
                                                        4,
                                                    ),
                                                    "HL_ALO_FILL_DETECTED_ON_EXCHANGE",
                                                    cfg,
                                                );
                                            }
                                            Err(_) => return Ok(resting_outcome()),
                                            Ok(_) => {
                                                final_limit_price = requote_px;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let new_cc = {
            let _guard = acquire_execution_lock();
            self.db.record_working_order_chase(
                ordered.position_id,
                final_limit_price,
                OrderFsmState::ChasingAlo.as_str(),
                new_eoid.as_deref(),
                &chase_reason,
            )?
        };

        Ok(ChaseStepOutcome::Chased {
            position_id: ordered.position_id,
            symbol: ordered.symbol.clone(),
            new_limit_price: final_limit_price,
            chase_count: new_cc,
            fsm_state: OrderFsmState::ChasingAlo.as_str().to_string(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_accepted_chase_fill(
        &self,
        ordered: &OrderedAssetView,
        asset: &ExchangeAsset,
        side: OrderSide,
        live_fill_px: f64,
        live_qty: f64,
        ack_avg_fill_price: f64,
        ack_filled_qty: f64,
        ack_fee_usdc: f64,
        ack_exchange_order_id: Option<&str>,
        maker_fee_bps: f64,
        cfg: &ConfigMap,
    ) -> Result<ChaseStepOutcome> {
        let total_qty = live_qty + ack_filled_qty;
        let blended_px = if total_qty > 1e-9 {
            (live_fill_px * live_qty + ack_avg_fill_price * ack_filled_qty) / total_qty
        } else {
            ack_avg_fill_price
        };
        let prior_fee = if live_qty > 1e-9 {
            round_dp(live_fill_px * live_qty * (maker_fee_bps / 10_000.0), 4)
        } else {
            0.0
        };
        let total_fee = round_dp(prior_fee + ack_fee_usdc, 4);
        let (committed_usdc, stop_px) = self.finalize_fill_to_open(
            ordered.position_id,
            asset,
            side,
            blended_px,
            total_qty,
            total_fee,
            ack_exchange_order_id,
            "HL_ALO_CHASE_FILLED",
            cfg,
        )?;
        Ok(ChaseStepOutcome::FilledOpen {
            position_id: ordered.position_id,
            asset_id: ordered.asset_id,
            symbol: ordered.symbol.clone(),
            side: side.as_str().to_string(),
            fill_price: blended_px,
            filled_quantity: total_qty,
            committed_notional_usdc: committed_usdc,
            protective_stop_px: stop_px,
            fees_paid_usdc: total_fee,
        })
    }

    fn cancel_working_entry_oid(
        &self,
        ordered: &OrderedAssetView,
        asset: &ExchangeAsset,
        cfg: &ConfigMap,
    ) -> CancelEntryOutcome {
        let Some(old_oid) = parse_hl_oid(ordered.exchange_order_id.as_deref()) else {
            return CancelEntryOutcome::Proceed {
                saw_filled_hint: false,
            };
        };
        let pk = cfg.get_str("private_key", "");
        if pk.trim().is_empty() {
            tracing::error!(
                symbol = %ordered.symbol,
                oid = old_oid,
                "private_key is empty/missing in live mode; aborting WORKING order cancel"
            );
            return CancelEntryOutcome::Aborted;
        }
        match self
            .http
            .cancel_order_wire(asset.asset_index, old_oid, &pk, Some(&self.db), cfg)
        {
            Ok(resp) => {
                if resp.get("status").and_then(Value::as_str) != Some("ok") {
                    tracing::warn!(
                        symbol = %ordered.symbol,
                        oid = old_oid,
                        response = %resp,
                        "Hyperliquid cancel returned non-ok status; deferring chase/finalize"
                    );
                    return CancelEntryOutcome::Aborted;
                }
                if let Some(err_str) = resp
                    .pointer("/response/data/statuses/0/error")
                    .and_then(Value::as_str)
                {
                    let err_lower = err_str.to_ascii_lowercase();
                    if !err_lower.contains("filled")
                        && !err_lower.contains("canceled")
                        && !err_lower.contains("never placed")
                    {
                        tracing::warn!(
                            symbol = %ordered.symbol,
                            oid = old_oid,
                            error = %err_str,
                            "Hyperliquid cancel returned unexpected error; deferring chase/finalize"
                        );
                        return CancelEntryOutcome::Aborted;
                    }
                }
                let resp_lower = resp.to_string().to_ascii_lowercase();
                CancelEntryOutcome::Proceed {
                    saw_filled_hint: resp_lower.contains("filled"),
                }
            }
            Err(e) => {
                tracing::warn!(
                    symbol = %ordered.symbol,
                    oid = old_oid,
                    error = %e,
                    "cancel_order_wire failed; keeping WORKING to prevent over-fill"
                );
                CancelEntryOutcome::Aborted
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_expired_working_order(
        &self,
        ordered: &OrderedAssetView,
        asset: &ExchangeAsset,
        side: OrderSide,
        ccfg: &ChaserConfig,
        pre_fetched_ch_state: Option<&Value>,
        is_maker_only: bool,
        cfg: &ConfigMap,
    ) -> Result<ChaseStepOutcome> {
        let resting_outcome = || ChaseStepOutcome::Resting {
            position_id: ordered.position_id,
            symbol: ordered.symbol.clone(),
            limit_price: ordered.limit_price,
            distance_from_mid_bps: ordered.distance_from_mid_bps,
        };

        let pre_state = if let Some(ch_val) = pre_fetched_ch_state {
            Ok(self.extract_working_position_state(ch_val, ordered.position_id, asset, side))
        } else {
            self.query_working_exchange_position_state(Some(ordered.position_id), asset, side, cfg)
        };
        let pre_state = match pre_state {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    symbol = %ordered.symbol,
                    error = %e,
                    "clearinghouseState unreachable on expired WORKING order; keeping WORKING"
                );
                return Ok(resting_outcome());
            }
        };

        if let Some((mut fill_px, mut fill_qty)) = pre_state {
            let rem_qty = core_remaining_entry_qty_for_lot(
                fill_qty,
                ordered.requested_quantity,
                asset.lot_step_size,
                asset.sz_decimals,
            );
            if rem_qty > asset.lot_step_size * 0.5 && rem_qty > 1e-9 {
                match self.cancel_working_entry_oid(ordered, asset, cfg) {
                    CancelEntryOutcome::Aborted => return Ok(resting_outcome()),
                    CancelEntryOutcome::Proceed { .. } => {
                        if let Ok(Some((post_px, post_qty))) = self
                            .query_working_exchange_position_state(
                                Some(ordered.position_id),
                                asset,
                                side,
                                cfg,
                            )
                        {
                            fill_px = post_px;
                            fill_qty = post_qty;
                        }
                    }
                }
            }
            let reason = if core_is_entry_fill_complete(
                fill_qty,
                ordered.requested_quantity,
                asset.lot_step_size,
                asset.sz_decimals,
            ) {
                "HL_ALO_FILL_DETECTED_ON_EXCHANGE"
            } else {
                "HL_ALO_PARTIAL_FILL_EXPIRED_OPEN"
            };
            let est_fee = round_dp(fill_px * fill_qty * (ccfg.maker_fee_bps / 10_000.0), 4);
            return self.finalize_known_fill_outcome(
                ordered, asset, side, fill_px, fill_qty, est_fee, reason, cfg,
            );
        }

        match self.cancel_working_entry_oid(ordered, asset, cfg) {
            CancelEntryOutcome::Aborted => return Ok(resting_outcome()),
            CancelEntryOutcome::Proceed { saw_filled_hint } => {
                if saw_filled_hint {
                    match self.query_working_exchange_position_state(
                        Some(ordered.position_id),
                        asset,
                        side,
                        cfg,
                    ) {
                        Ok(Some((post_px, post_qty))) if post_qty > 1e-9 => {
                            let reason = if core_is_entry_fill_complete(
                                post_qty,
                                ordered.requested_quantity,
                                asset.lot_step_size,
                                asset.sz_decimals,
                            ) {
                                "HL_ALO_FILL_DETECTED_ON_EXCHANGE"
                            } else {
                                "HL_ALO_PARTIAL_FILL_EXPIRED_OPEN"
                            };
                            let est_fee =
                                round_dp(post_px * post_qty * (ccfg.maker_fee_bps / 10_000.0), 4);
                            return self.finalize_known_fill_outcome(
                                ordered, asset, side, post_px, post_qty, est_fee, reason, cfg,
                            );
                        }
                        Err(_) => return Ok(resting_outcome()),
                        Ok(_) => {}
                    }
                }
            }
        }

        {
            let _guard = acquire_execution_lock();
            self.db
                .cancel_working_order(ordered.position_id, "HL_ALO_EXPIRED_CANCELLED")?;
            self.working_initial_limits
                .lock()
                .remove(&ordered.position_id);
            self.working_entry_urgencies
                .lock()
                .remove(&ordered.position_id);
            if !is_maker_only {
                self.record_alo_expiry_and_maybe_cooldown(
                    ordered.asset_id,
                    &ordered.symbol,
                    "HL_ALO_EXPIRED_CANCELLED",
                    ccfg,
                );
            }
        }
        Ok(ChaseStepOutcome::Cancelled {
            position_id: ordered.position_id,
            symbol: ordered.symbol.clone(),
            reason: "HL_ALO_EXPIRED_CANCELLED".to_string(),
        })
    }

    fn try_finalize_existing_exchange_fill(
        &self,
        ordered: &OrderedAssetView,
        asset: &ExchangeAsset,
        side: OrderSide,
        ccfg: &ChaserConfig,
        pre_fetched_ch_state: Option<&Value>,
        cfg: &ConfigMap,
    ) -> Result<Option<ChaseStepOutcome>> {
        let existing = if let Some(ch_val) = pre_fetched_ch_state {
            self.extract_working_position_state(ch_val, ordered.position_id, asset, side)
        } else {
            self.query_working_exchange_position_state(Some(ordered.position_id), asset, side, cfg)
                .ok()
                .flatten()
        };
        let Some((mut fill_px, mut fill_qty)) = existing else {
            return Ok(None);
        };
        if !core_is_entry_fill_complete(
            fill_qty,
            ordered.requested_quantity,
            asset.lot_step_size,
            asset.sz_decimals,
        ) {
            return Ok(None);
        }
        let rem_qty = core_remaining_entry_qty_for_lot(
            fill_qty,
            ordered.requested_quantity,
            asset.lot_step_size,
            asset.sz_decimals,
        );
        if rem_qty > asset.lot_step_size * 0.5 && rem_qty > 1e-9 {
            match self.cancel_working_entry_oid(ordered, asset, cfg) {
                CancelEntryOutcome::Aborted => return Ok(None),
                CancelEntryOutcome::Proceed { .. } => {
                    if let Ok(Some((post_px, post_qty))) = self
                        .query_working_exchange_position_state(
                            Some(ordered.position_id),
                            asset,
                            side,
                            cfg,
                        )
                    {
                        fill_px = post_px;
                        fill_qty = post_qty;
                    }
                }
            }
        }
        let est_fee = round_dp(fill_px * fill_qty * (ccfg.maker_fee_bps / 10_000.0), 4);
        self.finalize_known_fill_outcome(
            ordered,
            asset,
            side,
            fill_px,
            fill_qty,
            est_fee,
            "HL_ALO_FILL_DETECTED_ON_EXCHANGE",
            cfg,
        )
        .map(Some)
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_known_fill_outcome(
        &self,
        ordered: &OrderedAssetView,
        asset: &ExchangeAsset,
        side: OrderSide,
        fill_px: f64,
        fill_qty: f64,
        fees_paid_usdc: f64,
        reason: &str,
        cfg: &ConfigMap,
    ) -> Result<ChaseStepOutcome> {
        let (committed_usdc, stop_px) = self.finalize_fill_to_open(
            ordered.position_id,
            asset,
            side,
            fill_px,
            fill_qty,
            fees_paid_usdc,
            ordered.exchange_order_id.as_deref(),
            reason,
            cfg,
        )?;
        Ok(ChaseStepOutcome::FilledOpen {
            position_id: ordered.position_id,
            asset_id: ordered.asset_id,
            symbol: ordered.symbol.clone(),
            side: side.as_str().to_string(),
            fill_price: fill_px,
            filled_quantity: fill_qty,
            committed_notional_usdc: committed_usdc,
            protective_stop_px: stop_px,
            fees_paid_usdc,
        })
    }

    fn query_exchange_position_state(
        &self,
        asset: &ExchangeAsset,
        side: OrderSide,
        cfg: &ConfigMap,
    ) -> Result<Option<(f64, f64)>> {
        let wallet = cfg.get_str("wallet_address", "");
        if wallet.trim().is_empty() {
            return Err(AegisError::Execution(
                "wallet_address is empty; cannot verify clearinghouseState".to_string(),
            ));
        }
        let ch_state = if let Some(ws_ch) = self
            .ws_cache
            .as_ref()
            .and_then(|c| c.get_fresh_clearinghouse_state())
        {
            ws_ch
        } else {
            self.http.fetch_clearinghouse_state(&wallet, cfg)?
        };
        Ok(extract_position_state_from_ch_value(&ch_state, asset, side))
    }

    /// Backwards-compatible wrapper delegating SL/TP sync to [`crate::tpsl_sync`].
    #[allow(clippy::too_many_arguments)]
    pub fn ensure_exchange_tpsl_orders(
        &self,
        position_id: i64,
        asset: &ExchangeAsset,
        side: OrderSide,
        quantity: f64,
        mark_price: f64,
        desired_stop_px: f64,
        desired_tp_px: f64,
        existing_sl_oid: Option<&str>,
        existing_tp_oid: Option<&str>,
        replace_sl: bool,
        replace_tp: bool,
        resting_exchange_oids: Option<&HashSet<u64>>,
        cfg: &ConfigMap,
    ) -> Result<(f64, Option<String>, f64, Option<String>)> {
        let rep = ensure_exchange_tpsl_orders_detailed(
            &self.http,
            &self.db,
            &TpslSyncRequest {
                position_id,
                asset,
                side,
                quantity,
                mark_price,
                desired_stop_px,
                desired_tp_px,
                existing_sl_oid,
                existing_tp_oid,
                replace_sl,
                replace_tp,
            },
            resting_exchange_oids,
            None,
            cfg,
        )?;
        Ok((rep.stop_px, rep.sl_oid, rep.tp_px, rep.tp_oid))
    }

    /// Snapshot-aware TP/SL sync preserving [`OpenOrdersSnapshot::metadata_complete`] (`R14-M7`).
    #[allow(clippy::too_many_arguments)]
    pub fn ensure_exchange_tpsl_orders_with_snapshot(
        &self,
        position_id: i64,
        asset: &ExchangeAsset,
        side: OrderSide,
        quantity: f64,
        mark_price: f64,
        desired_stop_px: f64,
        desired_tp_px: f64,
        existing_sl_oid: Option<&str>,
        existing_tp_oid: Option<&str>,
        replace_sl: bool,
        replace_tp: bool,
        open_orders_snapshot: Option<&OpenOrdersSnapshot>,
        cfg: &ConfigMap,
    ) -> Result<(f64, Option<String>, f64, Option<String>)> {
        let rep = ensure_exchange_tpsl_orders_detailed(
            &self.http,
            &self.db,
            &TpslSyncRequest {
                position_id,
                asset,
                side,
                quantity,
                mark_price,
                desired_stop_px,
                desired_tp_px,
                existing_sl_oid,
                existing_tp_oid,
                replace_sl,
                replace_tp,
            },
            None,
            open_orders_snapshot,
            cfg,
        )?;
        Ok((rep.stop_px, rep.sl_oid, rep.tp_px, rep.tp_oid))
    }

    /// Backwards-compatible slice wrapper for callers passing raw `frontendOpenOrders` slices.
    #[allow(clippy::too_many_arguments)]
    pub fn ensure_exchange_tpsl_orders_with_orders(
        &self,
        position_id: i64,
        asset: &ExchangeAsset,
        side: OrderSide,
        quantity: f64,
        mark_price: f64,
        desired_stop_px: f64,
        desired_tp_px: f64,
        existing_sl_oid: Option<&str>,
        existing_tp_oid: Option<&str>,
        replace_sl: bool,
        replace_tp: bool,
        open_orders_snapshot: Option<&[Value]>,
        cfg: &ConfigMap,
    ) -> Result<(f64, Option<String>, f64, Option<String>)> {
        let owned_snap = open_orders_snapshot.map(|orders| OpenOrdersSnapshot {
            orders: orders.to_vec(),
            metadata_complete: true,
        });
        self.ensure_exchange_tpsl_orders_with_snapshot(
            position_id,
            asset,
            side,
            quantity,
            mark_price,
            desired_stop_px,
            desired_tp_px,
            existing_sl_oid,
            existing_tp_oid,
            replace_sl,
            replace_tp,
            owned_snap.as_ref(),
            cfg,
        )
    }

    /// Unconditionally recomputes `protective_stop_px` and `take_profit_px` from actual `fill_price`
    /// (`P0-Exec-1`, `P1-Strat` without 2.0R override), promotes `WORKING -> OPEN`, attaches SL/TP triggers,
    /// and enqueues a non-blocking HTML alert. Returns `(committed_usdc, final_stop_px)`.
    #[allow(clippy::too_many_arguments)]
    fn finalize_fill_to_open(
        &self,
        position_id: i64,
        asset: &ExchangeAsset,
        side: OrderSide,
        fill_price: f64,
        filled_qty: f64,
        fees_paid_usdc: f64,
        exchange_order_id: Option<&str>,
        reason: &str,
        cfg: &ConfigMap,
    ) -> Result<(f64, f64)> {
        let limits = RiskLimitsConfig::from_config_map(cfg);
        let atr_opt = crate::engine::resolve_signal_matched_atr_unlocked(
            &self.db,
            &self.http,
            Some(asset),
            asset.id,
            cfg,
            fill_price,
            limits.protective_stop_default_pct,
        );
        let eff_tick = effective_hl_tick_size(fill_price, asset.tick_size, asset.sz_decimals)
            .unwrap_or_else(|| asset.tick_size.max(1e-8));

        let (
            target_pos_id,
            target_qty,
            committed_usdc,
            eff_stop_px,
            eff_tp_px,
            existing_sl_oid,
            existing_tp_oid,
        ) = {
            let _guard = acquire_execution_lock();
            let merge_target_opt = self
                .db
                .list_active_positions_for_asset(asset.id)?
                .into_iter()
                .find(|p| {
                    p.id != position_id
                        && PositionStatus::parse(&p.status) == PositionStatus::Open
                        && OrderSide::parse(&p.side).ok() == Some(side)
                });

            if let Some(open_pos) = merge_target_opt {
                self.db.merge_working_scale_in_into_open(
                    position_id,
                    open_pos.id,
                    fill_price,
                    filled_qty,
                    fees_paid_usdc,
                    exchange_order_id,
                    reason,
                )?;
                self.record_scale_in_anchor(open_pos.id, fill_price, now_ms());
                self.working_initial_limits.lock().remove(&position_id);
                self.working_entry_urgencies.lock().remove(&position_id);
                let merged = self.db.get_position_by_id(open_pos.id)?.ok_or_else(|| {
                    AegisError::Database(format!("Merged OPEN position #{} missing", open_pos.id))
                })?;
                let stop_px = if merged.protective_stop_px > 0.0 {
                    merged.protective_stop_px
                } else {
                    compute_initial_protective_stop(
                        side,
                        merged.entry_price,
                        atr_opt,
                        eff_tick,
                        &limits,
                    )
                };
                let tp_px = if merged.take_profit_px > 0.0 {
                    merged.take_profit_px
                } else {
                    compute_initial_take_profit(
                        side,
                        merged.entry_price,
                        atr_opt,
                        eff_tick,
                        &limits,
                    )
                };
                (
                    merged.id,
                    merged.filled_quantity,
                    merged.committed_notional_usdc,
                    stop_px,
                    tp_px,
                    merged.protective_stop_oid,
                    merged.take_profit_oid,
                )
            } else {
                let eff_stop_px =
                    compute_initial_protective_stop(side, fill_price, atr_opt, eff_tick, &limits);
                let eff_tp_px =
                    compute_initial_take_profit(side, fill_price, atr_opt, eff_tick, &limits);
                self.db.promote_working_to_open(
                    position_id,
                    fill_price,
                    filled_qty,
                    fees_paid_usdc,
                    eff_stop_px,
                    Some(eff_tp_px),
                    exchange_order_id,
                    reason,
                )?;
                self.working_initial_limits.lock().remove(&position_id);
                self.working_entry_urgencies.lock().remove(&position_id);
                (
                    position_id,
                    filled_qty,
                    round_dp(fill_price * filled_qty, 4),
                    eff_stop_px,
                    eff_tp_px,
                    None,
                    None,
                )
            }
        };

        // Unlocked HTTP TP/SL attachment / quantity re-sync (`R14-H2`)
        let (final_stop_px, _sl_oid, final_tp_px, _tp_oid) = self.ensure_exchange_tpsl_orders(
            target_pos_id,
            asset,
            side,
            target_qty,
            fill_price,
            eff_stop_px,
            eff_tp_px,
            existing_sl_oid.as_deref(),
            existing_tp_oid.as_deref(),
            true,
            true,
            None,
            cfg,
        )?;

        let eff_equity = crate::engine::resolve_effective_base_equity_usdc_for_db(&self.db, cfg);
        let _guard = acquire_execution_lock();
        if let Ok(held_after) = self.db.get_assets_held() {
            let ordered_after = self.db.get_assets_ordered().unwrap_or_default();
            let exp = crate::engine::compute_active_exposure_and_heat_with_db(
                Some(&self.db),
                &held_after,
                &ordered_after,
                cfg.target_leverage_multiplier(),
                limits.protective_stop_default_pct,
                None,
            );
            let open_notional = exp.total_gross_notional_usdc;
            let open_stop_risk = exp.total_stop_risk_usdc;
            let risk = RiskEngine::from_config_map(cfg);
            if let Err(breach_err) = risk.check_portfolio_heat_and_gross_at_fill(
                eff_equity,
                open_notional,
                open_stop_risk,
            ) {
                let msg = format!(
                    "Post-fill portfolio heat/gross breach telemetry for {} (#{target_pos_id}): {breach_err}",
                    asset.symbol
                );
                tracing::warn!(
                    position_id = target_pos_id,
                    symbol = %asset.symbol,
                    equity_usdc = eff_equity,
                    open_notional_usdc = open_notional,
                    open_stop_risk_usdc = open_stop_risk,
                    error = %breach_err,
                    "Post-fill portfolio heat/gross breach; queued for cap-enforcement trim"
                );
                let details = json!({
                    "position_id": target_pos_id,
                    "symbol": asset.symbol,
                    "equity_usdc": eff_equity,
                    "open_notional_usdc": open_notional,
                    "open_stop_risk_usdc": open_stop_risk,
                    "breach": breach_err.to_string(),
                });
                let _ =
                    self.db
                        .log_telemetry("WARN", "execution.order_chaser", &msg, Some(details));
            }
        }

        if let (Ok(Some(pos)), Ok(snap)) = (
            self.db.get_position_by_id(target_pos_id),
            self.db.get_latest_portfolio_snapshot(),
        ) {
            let avail_after = (snap.available_cash_usdc - committed_usdc).max(0.0);
            let alert = OrderFilledOpenAlert {
                symbol: asset.symbol.clone(),
                side,
                risk_tier: cfg.risk_tier(),
                leverage_multiplier: Some(cfg.target_leverage_multiplier()),
                trigger: reason.to_string(),
                fill_price,
                filled_qty: target_qty,
                committed_notional_usdc: committed_usdc,
                total_equity_usdc: snap.total_equity_usdc,
                available_cash_usdc: avail_after,
                protective_stop_px: final_stop_px,
                take_profit_px: final_tp_px,
                s1_weight: pos.s1_weight_contrib,
                s2_weight: pos.s2_weight_contrib,
                s3_weight: pos.s3_weight_contrib,
                timestamp_ms: now_ms(),
            };
            let html = format_order_filled_open_html(&alert);
            let payload = serde_json::to_value(&alert).ok();
            let _ = self.alerts.enqueue_html_alert(
                "ORDER_FILLED_OPEN",
                "INFO",
                &html,
                payload.as_ref(),
            );
            self.wake_alert_worker();
        }

        Ok((committed_usdc, final_stop_px))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_db::{AssetUpsertInput, BarUpsertInput};
    use aegis_test_support::mock_http::{MockHttpResponse, MockHttpServer};
    use serde_json::json;

    fn seed_test_asset(
        db: &DatabaseManager,
        coin: &str,
        idx: u32,
        px: f64,
        tick: f64,
    ) -> ExchangeAsset {
        let id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: coin.to_string(),
                asset_index: idx,
                sz_decimals: 2,
                tick_size: tick,
                lot_step_size: 0.01,
                max_leverage: 20.0,
                mark_price: px,
                mid_price: px,
                oracle_price: px,
                funding_rate_hourly: 0.0,
                day_volume_usdc: 25_000_000.0,
                open_interest_usdc: 10_000_000.0,
                prev_day_price: px * 0.99,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();
        db.get_asset_by_id(id).unwrap().unwrap()
    }

    #[test]
    fn test_one_tick_bps_and_effective_chase_threshold() {
        let tick = 1e-6;
        let mid = 0.002488;
        let one_tick_bps = compute_one_tick_bps_opt(tick, mid).unwrap();
        assert!((one_tick_bps - 4.01929).abs() < 0.01);
        let eff_thresh = 4.0_f64.max(one_tick_bps * 1.5);
        assert!(one_tick_bps < eff_thresh);
        assert!((eff_thresh - 6.0289).abs() < 0.01);
        assert!(compute_one_tick_bps_opt(tick, 0.0).is_none());
        assert!(compute_one_tick_bps_opt(0.0, mid).is_none());
    }

    #[test]
    fn test_clamp_initial_and_chase_alo_prices_stay_strictly_passive() {
        let tick = 1e-6;
        let buy_init = clamp_initial_alo_price(OrderSide::Long, 0.002495, 0.002488, 0.002489, tick);
        assert!((buy_init - 0.002488).abs() < 1e-9);
        assert!(buy_init < 0.002489);

        let sell_init =
            clamp_initial_alo_price(OrderSide::Short, 0.002485, 0.002488, 0.002489, tick);
        assert!((sell_init - 0.002489).abs() < 1e-9);
        assert!(sell_init > 0.002488);

        let buy_locked =
            clamp_initial_alo_price(OrderSide::Long, 0.002489, 0.002489, 0.002489, tick);
        assert!((buy_locked - 0.002488).abs() < 1e-9);
        let sell_locked =
            clamp_initial_alo_price(OrderSide::Short, 0.002489, 0.002489, 0.002489, tick);
        assert!((sell_locked - 0.002490).abs() < 1e-9);

        let buy_chase =
            clamp_chase_alo_price(OrderSide::Long, 0.002495, Some((0.002488, 0.002489)), tick);
        assert!((buy_chase - 0.002488).abs() < 1e-9);
        let sell_chase =
            clamp_chase_alo_price(OrderSide::Short, 0.002484, Some((0.002488, 0.002489)), tick);
        assert!((sell_chase - 0.002489).abs() < 1e-9);

        let sell_rq = step_more_passive_alo_price(
            OrderSide::Short,
            0.002489,
            Some((0.002489, 0.002490)),
            tick,
        );
        assert!((sell_rq - 0.002491).abs() < 1e-9);
        let buy_rq = step_more_passive_alo_price(
            OrderSide::Long,
            0.002488,
            Some((0.002487, 0.002488)),
            tick,
        );
        assert!((buy_rq - 0.002486).abs() < 1e-9);
    }

    #[test]
    fn test_post_only_crossing_rejection_detection() {
        assert!(is_post_only_crossing_rejection(Some("badAloPxRejected")));
        assert!(is_post_only_crossing_rejection(Some(
            "Post only order would have immediately matched, bbo was 0.002489@0.002490"
        )));
        assert!(is_post_only_crossing_rejection(Some(
            "Order would immediately match"
        )));
        assert!(!is_post_only_crossing_rejection(Some(
            "Insufficient margin"
        )));
        assert!(!is_post_only_crossing_rejection(None));
    }

    #[test]
    fn test_r14_m1_place_working_order_network_error_recovers_resting_and_filled_via_cloid() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let sol = seed_test_asset(&db, "SOL", 3, 180.0, 0.01);
        let eth = seed_test_asset(&db, "ETH", 1, 2000.0, 0.1);

        let scenario = Arc::new(Mutex::new("resting"));
        let scenario_srv = Arc::clone(&scenario);
        let srv = MockHttpServer::start(move |req| {
            let mode = *scenario_srv.lock();
            if req.path == "/exchange" {
                let is_trigger = req
                    .json_body
                    .pointer("/action/orders/0/t/trigger")
                    .is_some();
                if is_trigger {
                    return MockHttpResponse::ok_json(&json!({
                        "status": "ok",
                        "response": { "type": "order", "data": { "statuses": [{ "waitingForTrigger": { "oid": 88001u64 } }] } }
                    }));
                }
                return MockHttpResponse::json(500, &json!({ "error": "timeout" }));
            }
            match req
                .json_body
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("")
            {
                "l2Book" => MockHttpResponse::ok_json(&json!({
                    "levels": [
                        [{ "px": "180.00", "sz": "50.0" }],
                        [{ "px": "180.05", "sz": "50.0" }]
                    ]
                })),
                "orderStatus" => match mode {
                    "resting" => MockHttpResponse::ok_json(&json!({
                        "status": "order",
                        "order": {
                            "status": "open",
                            "order": { "oid": 44111u64, "origSz": "1.0", "sz": "1.0", "limitPx": "180.00" }
                        }
                    })),
                    "filled" => MockHttpResponse::ok_json(&json!({
                        "status": "order",
                        "order": {
                            "status": "filled",
                            "order": { "oid": 44222u64, "origSz": "0.5", "sz": "0.0", "limitPx": "2000.0", "avgPx": "1999.8" }
                        }
                    })),
                    _ => MockHttpResponse::ok_json(&json!({ "status": "unknownOid" })),
                },
                _ => MockHttpResponse::ok_json(&json!([])),
            }
        });

        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_testnet_url", &srv.base_url(), None)
            .unwrap();
        db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
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

        let chaser = OrderChaser::new(
            db.clone(),
            HyperliquidHttpClient::new(5),
            AlertEngine::new(db.clone()),
        );

        // 1. Recover resting WORKING order via cloid probe
        let pid1 = chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: sol,
                side: OrderSide::Long,
                requested_quantity: 1.0,
                reserved_margin_usdc: 180.0,
                portfolio_weight: 0.1,
                s1_weight_contrib: 0.1,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 170.0,
                reason: "R14_M1_RESTING".to_string(),
                bypass_entry_interval: true,
                entry_urgency: EntryUrgency::ChaseToIoc,
                is_scale_in: false,
            })
            .unwrap();
        let p1 = db.get_position_by_id(pid1).unwrap().unwrap();
        assert_eq!(p1.status, "WORKING");
        assert_eq!(p1.exchange_order_id.as_deref(), Some("hl-44111"));

        // 2. Recover immediate filled order via cloid probe -> promotes to OPEN with SL/TP
        *scenario.lock() = "filled";
        let pid2 = chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: eth,
                side: OrderSide::Long,
                requested_quantity: 0.5,
                reserved_margin_usdc: 1000.0,
                portfolio_weight: 0.1,
                s1_weight_contrib: 0.1,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 1900.0,
                reason: "R14_M1_FILLED".to_string(),
                bypass_entry_interval: true,
                entry_urgency: EntryUrgency::ChaseToIoc,
                is_scale_in: false,
            })
            .unwrap();
        let p2 = db.get_position_by_id(pid2).unwrap().unwrap();
        assert_eq!(p2.status, "OPEN");
        assert!((p2.entry_price - 1999.8).abs() < 1e-6);
    }

    #[test]
    fn test_p0_exec1_p1_strat_drift_abort_fill_stop_recompute_and_no_2r_override() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let sol = seed_test_asset(&db, "SOL", 3, 100.0, 0.01);
        let base_ts = now_ms() - 20 * 14_400_000;
        let bars: Vec<BarUpsertInput> = (0..20)
            .map(|i| BarUpsertInput {
                asset_id: sol.id,
                timeframe: "4h".to_string(),
                timestamp_ms: base_ts + i * 14_400_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 1000.0,
                funding_rate: 0.0,
            })
            .collect();
        db.upsert_ohlcv_bars(&bars).unwrap();

        let chaser = OrderChaser::new(
            db.clone(),
            HyperliquidHttpClient::new(5),
            AlertEngine::new(db.clone()),
        );

        // 1. Place WORKING order at ~99.99 (mid=100.0), then chase drifts > 60 bps (to 101.0 = +101 bps):
        //    must abort WORKING order and insert CHURN_GUARD cooldown!
        let pid1 = chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: sol.clone(),
                side: OrderSide::Long,
                requested_quantity: 1.0,
                reserved_margin_usdc: 100.0,
                portfolio_weight: 0.1,
                s1_weight_contrib: 0.1,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 95.6,
                reason: "DRIFT_ABORT_TEST".to_string(),
                bypass_entry_interval: true,
                entry_urgency: EntryUrgency::ChaseToIoc,
                is_scale_in: false,
            })
            .unwrap();
        db.update_asset_live_quote(sol.id, 101.0, 101.0).unwrap();
        let ord1 = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == pid1)
            .unwrap();
        let outcome = chaser.advance_working_order(&ord1).unwrap();
        assert!(matches!(
            outcome,
            ChaseStepOutcome::Cancelled { ref reason, .. } if reason == "HL_ALO_CHASE_DRIFT_ABORT"
        ));
        assert!(db.is_asset_in_cooldown(sol.id).unwrap());

        // 2. Clear cooldown, place order, fill at 105.0 (above initial 99.99):
        //    must recompute protective_stop_px from 105.0 (NOT keep stale 95.6!) and honor 2.8x ATR TP
        //    WITHOUT forcing a 2.0 * stop_dist override!
        let eth = seed_test_asset(&db, "ETH", 1, 100.0, 0.01);
        let eth_bars: Vec<BarUpsertInput> = (0..20)
            .map(|i| BarUpsertInput {
                asset_id: eth.id,
                timeframe: "4h".to_string(),
                timestamp_ms: base_ts + i * 14_400_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 1000.0,
                funding_rate: 0.0,
            })
            .collect();
        db.upsert_ohlcv_bars(&eth_bars).unwrap();

        let cfg = db.load_config().unwrap();
        let pid2 = chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: eth.clone(),
                side: OrderSide::Long,
                requested_quantity: 1.0,
                reserved_margin_usdc: 100.0,
                portfolio_weight: 0.1,
                s1_weight_contrib: 0.1,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 95.6,
                reason: "STOP_RECOMPUTE_TEST".to_string(),
                bypass_entry_interval: true,
                entry_urgency: EntryUrgency::ChaseToIoc,
                is_scale_in: false,
            })
            .unwrap();
        let (_comm, recomputed_stop) = chaser
            .finalize_fill_to_open(
                pid2,
                &eth,
                OrderSide::Long,
                105.0,
                1.0,
                0.01,
                None,
                "TEST_FILL",
                &cfg,
            )
            .unwrap();
        let pos2 = db.get_position_by_id(pid2).unwrap().unwrap();
        // ATR=2.0 -> stop_dist = 2.2 * 2.0 = 4.4 -> stop = 105.0 - 4.4 = 100.60 (NOT stale 95.6!)
        assert!((recomputed_stop - 100.60).abs() < 0.05);
        assert!((pos2.protective_stop_px - recomputed_stop).abs() < 1e-6);
        // ATR=2.0 -> tp_dist = 2.8 * 2.0 = 5.6 -> tp = 105.0 + 5.6 = 110.60 (NOT 105 + 2*4.4 = 113.8!)
        assert!((pos2.take_profit_px - 110.60).abs() < 0.05);
    }

    #[test]
    fn test_sec11_native_alo_1tick_wire_equivalence_preservation() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let pump = seed_test_asset(&db, "PUMP", 9, 1000.0, 0.1);

        // Mock L2 book with 1-tick spread (1000.0 / 1000.1)
        let srv = MockHttpServer::start(|req| {
            if req.path == "/info" {
                match req
                    .json_body
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                {
                    "l2Book" => MockHttpResponse::ok_json(&json!({
                        "levels": [
                            [{ "px": "1000.0", "sz": "10.0" }],
                            [{ "px": "1000.1", "sz": "10.0" }]
                        ]
                    })),
                    _ => MockHttpResponse::ok_json(&json!([])),
                }
            } else {
                MockHttpResponse::ok_json(&json!({
                    "status": "ok",
                    "response": { "type": "order", "data": { "statuses": [{ "resting": { "oid": 9991u64 } }] } }
                }))
            }
        });

        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_testnet_url", &srv.base_url(), None)
            .unwrap();
        db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
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

        let chaser = OrderChaser::new(
            db.clone(),
            HyperliquidHttpClient::new(5),
            AlertEngine::new(db.clone()),
        );
        let pid = chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: pump.clone(),
                side: OrderSide::Long,
                requested_quantity: 1.0,
                reserved_margin_usdc: 1000.0,
                portfolio_weight: 0.1,
                s1_weight_contrib: 0.1,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 950.0,
                reason: "1TICK_PRESERVE".to_string(),
                bypass_entry_interval: true,
                entry_urgency: EntryUrgency::ChaseToIoc,
                is_scale_in: false,
            })
            .unwrap();

        // Simulate minor allMids drift to 1000.9 while BBO stays 1000.0 / 1000.1 ->
        // already resting at bid 1000.0 -> must preserve queue with ZERO /exchange cancels!
        srv.clear_requests();
        db.update_asset_live_quote(pump.id, 1000.9, 1000.9).unwrap();
        let ord = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == pid)
            .unwrap();
        let res = chaser.advance_working_order(&ord).unwrap();
        assert!(matches!(res, ChaseStepOutcome::Resting { .. }));
        assert!(srv.requests().into_iter().all(|r| r.path != "/exchange"));
    }

    #[test]
    fn test_p0_exec1_repeated_hl_rejects_and_alo_expiries_insert_cooldown() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let doge = seed_test_asset(&db, "DOGE", 2, 0.25, 0.0001);
        let kpepe = seed_test_asset(&db, "kPEPE", 7, 0.015, 0.00001);

        let srv = MockHttpServer::start(|req| {
            if req.path == "/info" {
                match req
                    .json_body
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                {
                    "l2Book" => MockHttpResponse::ok_json(&json!({
                        "levels": [
                            [{ "px": "0.01499", "sz": "10000" }],
                            [{ "px": "0.01500", "sz": "10000" }]
                        ]
                    })),
                    _ => MockHttpResponse::ok_json(&json!([])),
                }
            } else {
                MockHttpResponse::ok_json(&json!({
                    "status": "ok",
                    "response": { "type": "order", "data": { "statuses": [{ "error": "Insufficient margin to place order" }] } }
                }))
            }
        });

        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_testnet_url", &srv.base_url(), None)
            .unwrap();
        db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
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

        let chaser = OrderChaser::new(
            db.clone(),
            HyperliquidHttpClient::new(5),
            AlertEngine::new(db.clone()),
        );

        // 1. Hard non-transient reject immediately activates CHURN_GUARD cooldown on kPEPE
        assert!(chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: kpepe.clone(),
                side: OrderSide::Long,
                requested_quantity: 1000.0,
                reserved_margin_usdc: 15.0,
                portfolio_weight: 0.05,
                s1_weight_contrib: 0.05,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 0.014,
                reason: "HARD_REJECT_TEST".to_string(),
                bypass_entry_interval: true,
                entry_urgency: EntryUrgency::ChaseToIoc,
                is_scale_in: false,
            })
            .is_err());
        assert!(db.is_asset_in_cooldown(kpepe.id).unwrap());

        // 2. Repeated ALO expiry window activates cooldown on DOGE
        let cfg = db.load_config().unwrap();
        let ccfg = ChaserConfig::from_config(&cfg);
        chaser.record_alo_expiry_and_maybe_cooldown(
            doge.id,
            &doge.symbol,
            "HL_ALO_EXPIRED_CANCELLED",
            &ccfg,
        );
        assert!(db.is_asset_in_cooldown(doge.id).unwrap());
    }

    #[test]
    fn test_ambiguous_post_chase_working_order_recovers_resting_and_filled_by_cloid() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let sol = seed_test_asset(&db, "SOL", 3, 100.0, 0.01);
        let eth = seed_test_asset(&db, "ETH", 1, 100.0, 0.01);

        let scenario: Arc<Mutex<&'static str>> = Arc::new(Mutex::new("open"));
        let captured_cloid: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let sc_clone = Arc::clone(&scenario);
        let cap_clone = Arc::clone(&captured_cloid);

        let srv = MockHttpServer::start(move |req| {
            if req.path == "/exchange" {
                let act_type = req
                    .json_body
                    .pointer("/action/type")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if act_type == "cancel" {
                    return MockHttpResponse::ok_json(&json!({
                        "status": "ok",
                        "response": { "type": "cancel", "data": { "statuses": ["success"] } }
                    }));
                }
                if let Some(orders) = req
                    .json_body
                    .pointer("/action/orders")
                    .and_then(Value::as_array)
                {
                    if orders
                        .first()
                        .and_then(|o| o.pointer("/t/trigger"))
                        .is_some()
                    {
                        let st: Vec<Value> = orders
                            .iter()
                            .enumerate()
                            .map(|(i, _)| json!({ "resting": { "oid": 99000 + i } }))
                            .collect();
                        return MockHttpResponse::ok_json(&json!({
                            "status": "ok",
                            "response": { "type": "order", "data": { "statuses": st } }
                        }));
                    }
                    let cloid = orders
                        .first()
                        .and_then(|o| o.get("c"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    let px_str = orders
                        .first()
                        .and_then(|o| o.get("p"))
                        .and_then(Value::as_str)
                        .unwrap_or("99.99");
                    // Initial order succeeds resting on 99.99; chase (different price) simulates timeout
                    if px_str == "99.99" {
                        return MockHttpResponse::ok_json(&json!({
                            "status": "ok",
                            "response": { "type": "order", "data": { "statuses": [{ "resting": { "oid": 1001 } }] } }
                        }));
                    }
                    *cap_clone.lock() = Some(cloid);
                    return MockHttpResponse::text(500, "text/plain", "ambiguous chase timeout");
                }
                return MockHttpResponse::ok_json(&json!({ "status": "ok" }));
            }

            let typ = req
                .json_body
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("");
            match typ {
                "l2Book" => MockHttpResponse::ok_json(&json!({
                    "levels": [
                        [{ "px": "99.99", "sz": "100" }],
                        [{ "px": "100.01", "sz": "100" }]
                    ]
                })),
                "clearinghouseState" => MockHttpResponse::ok_json(&json!({
                    "assetPositions": []
                })),
                "openOrders" | "frontendOpenOrders" => MockHttpResponse::ok_json(&json!([])),
                "orderStatus" => {
                    let oid_q = req
                        .json_body
                        .get("oid")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if cap_clone.lock().as_deref() == Some(oid_q) {
                        if *sc_clone.lock() == "open" {
                            return MockHttpResponse::ok_json(&json!({
                                "status": "order",
                                "order": {
                                    "status": "open",
                                    "order": {
                                        "coin": "SOL",
                                        "oid": 88123u64,
                                        "limitPx": "100.18",
                                        "origSz": "1.0",
                                        "sz": "1.0",
                                        "cloid": oid_q
                                    }
                                }
                            }));
                        } else {
                            return MockHttpResponse::ok_json(&json!({
                                "status": "order",
                                "order": {
                                    "status": "filled",
                                    "order": {
                                        "coin": "ETH",
                                        "oid": 88124u64,
                                        "limitPx": "100.18",
                                        "avgPx": "100.15",
                                        "origSz": "1.0",
                                        "sz": "0.0",
                                        "cloid": oid_q
                                    }
                                }
                            }));
                        }
                    }
                    MockHttpResponse::ok_json(&json!({ "status": "unknownOid" }))
                }
                _ => MockHttpResponse::ok_json(&json!([])),
            }
        });

        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_testnet_url", &srv.base_url(), None)
            .unwrap();
        db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
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

        let chaser = OrderChaser::new(
            db.clone(),
            HyperliquidHttpClient::new(5),
            AlertEngine::new(db.clone()),
        );

        // 1. Ambiguous chase POST recovers resting OID hl-88123 instead of HL_CHASE_REJECT_CANCELLED
        let pid1 = chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: sol.clone(),
                side: OrderSide::Long,
                requested_quantity: 1.0,
                reserved_margin_usdc: 100.0,
                portfolio_weight: 0.1,
                s1_weight_contrib: 0.1,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 95.0,
                reason: "CHASE_CLOID_OPEN".to_string(),
                bypass_entry_interval: true,
                entry_urgency: EntryUrgency::ChaseToIoc,
                is_scale_in: false,
            })
            .unwrap();
        db.update_asset_live_quote(sol.id, 100.20, 100.20).unwrap();
        let ord1 = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == pid1)
            .unwrap();
        let out1 = chaser.advance_working_order(&ord1).unwrap();
        assert!(matches!(out1, ChaseStepOutcome::Chased { .. }));
        let p1 = db.get_position_by_id(pid1).unwrap().unwrap();
        assert_eq!(p1.status, "WORKING");
        assert_eq!(p1.exchange_order_id.as_deref(), Some("hl-88123"));

        // 2. Ambiguous chase POST recovers filled trade via cloid and promotes to OPEN
        let pid2 = chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: eth.clone(),
                side: OrderSide::Long,
                requested_quantity: 1.0,
                reserved_margin_usdc: 100.0,
                portfolio_weight: 0.1,
                s1_weight_contrib: 0.1,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 95.0,
                reason: "CHASE_CLOID_FILL".to_string(),
                bypass_entry_interval: true,
                entry_urgency: EntryUrgency::ChaseToIoc,
                is_scale_in: false,
            })
            .unwrap();
        *scenario.lock() = "filled";
        db.update_asset_live_quote(eth.id, 100.20, 100.20).unwrap();
        let ord2 = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == pid2)
            .unwrap();
        let out2 = chaser.advance_working_order(&ord2).unwrap();
        assert!(matches!(
            out2,
            ChaseStepOutcome::FilledOpen { fill_price, .. } if (fill_price - 100.15).abs() < 1e-6
        ));
        let p2 = db.get_position_by_id(pid2).unwrap().unwrap();
        assert_eq!(p2.status, "OPEN");
    }

    #[test]
    fn test_pre_ioc_heat_cap_cancels_cleanly_and_post_fill_emits_warn_telemetry() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        db.set_config_value("initial_paper_equity_usdc", "1000.0", None)
            .unwrap();
        db.set_config_value("max_portfolio_heat_pct", "0.12", None)
            .unwrap();
        db.set_config_value("max_alo_chase_ticks", "1", None)
            .unwrap();

        let btc = seed_test_asset(&db, "BTC", 0, 100.0, 0.01);
        let eth = seed_test_asset(&db, "ETH", 1, 10.0, 0.01);
        let sol = seed_test_asset(&db, "SOL", 3, 10.0, 0.01);

        // Existing OPEN position uses 10% heat ($100 stop risk on $1000 equity)
        let btc_pid = db
            .create_working_order(&aegis_db::WorkingOrderInput {
                asset_id: btc.id,
                client_order_id: "v2-btc-heat-base".to_string(),
                side: OrderSide::Long,
                limit_price: 100.0,
                requested_quantity: 10.0,
                reserved_margin_usdc: 500.0,
                portfolio_weight: 0.25,
                s1_weight_contrib: 0.25,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 90.0,
                take_profit_px: Some(120.0),
                exchange_order_id: None,
                reason: "BASE_HEAT".to_string(),
            })
            .unwrap();
        db.promote_working_to_open(
            btc_pid,
            100.0,
            10.0,
            0.4,
            90.0,
            Some(120.0),
            None,
            "BASE_OPEN",
        )
        .unwrap();

        let chaser = OrderChaser::new(
            db.clone(),
            HyperliquidHttpClient::new(5),
            AlertEngine::new(db.clone()),
        );

        // 1. Pre-IOC heat cap abort: ETH stop risk = (10 - 7) * 10 = $30 (+0.03 heat -> 0.13 > 0.12)
        let eth_pid = chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: eth.clone(),
                side: OrderSide::Long,
                requested_quantity: 10.0,
                reserved_margin_usdc: 50.0,
                portfolio_weight: 0.10,
                s1_weight_contrib: 0.10,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 7.0,
                reason: "PRE_IOC_HEAT_TEST".to_string(),
                bypass_entry_interval: true,
                entry_urgency: EntryUrgency::ChaseToIoc,
                is_scale_in: false,
            })
            .unwrap();
        db.record_working_order_chase(
            eth_pid,
            9.99,
            OrderFsmState::ChasingAlo.as_str(),
            None,
            "chase 1",
        )
        .unwrap();
        db.update_asset_live_quote(eth.id, 10.05, 10.05).unwrap();
        let eth_ord = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == eth_pid)
            .unwrap();
        let outcome = chaser.advance_working_order(&eth_ord).unwrap();
        assert!(matches!(
            outcome,
            ChaseStepOutcome::Cancelled { ref reason, .. } if reason == "HL_PRE_IOC_HEAT_CAP_CANCELLED"
        ));
        assert!(!db.is_asset_in_cooldown(eth.id).unwrap());

        // 2. Post-fill heat check emits structured WARN telemetry without dropping confirmed fill
        let sol_pid = chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: sol.clone(),
                side: OrderSide::Long,
                requested_quantity: 10.0,
                reserved_margin_usdc: 50.0,
                portfolio_weight: 0.10,
                s1_weight_contrib: 0.10,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 7.0,
                reason: "POST_FILL_HEAT_WARN".to_string(),
                bypass_entry_interval: true,
                entry_urgency: EntryUrgency::ChaseToIoc,
                is_scale_in: false,
            })
            .unwrap();
        // Tighten heat limit to simulate concurrent fill narrowing headroom right at finalization
        db.set_config_value("max_portfolio_heat_pct", "0.09", None)
            .unwrap();
        let cfg = db.load_config().unwrap();
        chaser
            .finalize_fill_to_open(
                sol_pid,
                &sol,
                OrderSide::Long,
                10.0,
                10.0,
                0.05,
                None,
                "CONCURRENT_FILL",
                &cfg,
            )
            .unwrap();
        let logs = db.list_recent_telemetry(20).unwrap();
        assert!(logs.iter().any(|l| {
            l.level == "WARN"
                && l.component == "execution.order_chaser"
                && l.message.contains("Post-fill portfolio heat/gross breach")
        }));
    }

    #[test]
    fn test_n4_maker_only_rests_until_ttl_without_ioc_and_clean_expiry() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        db.set_config_value("max_alo_chase_ticks", "1", None)
            .unwrap();
        db.set_config_value("maker_first_low_urgency_entries", "true", None)
            .unwrap();
        db.set_config_value("maker_only_rest_ttl_ms", "30000", None)
            .unwrap();

        let sol = seed_test_asset(&db, "SOL", 3, 100.0, 0.01);
        let eth = seed_test_asset(&db, "ETH", 1, 100.0, 0.01);

        let chaser = OrderChaser::new(
            db.clone(),
            HyperliquidHttpClient::new(5),
            AlertEngine::new(db.clone()),
        );

        // 1. MakerOnly rests cleanly when chase_count >= max_chases and age < TTL
        let pid1 = chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: sol.clone(),
                side: OrderSide::Long,
                requested_quantity: 1.0,
                reserved_margin_usdc: 100.0,
                portfolio_weight: 0.10,
                s1_weight_contrib: 0.10,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 95.0,
                reason: "OPEN_LONG".to_string(),
                bypass_entry_interval: true,
                entry_urgency: EntryUrgency::MakerOnly,
                is_scale_in: false,
            })
            .unwrap();
        db.record_working_order_chase(
            pid1,
            99.99,
            OrderFsmState::ChasingAlo.as_str(),
            None,
            "chase 1",
        )
        .unwrap();
        db.update_asset_live_quote(sol.id, 100.20, 100.20).unwrap();

        let mut ord1 = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == pid1)
            .unwrap();
        let rest_outcome = chaser.advance_working_order(&ord1).unwrap();
        assert!(matches!(rest_outcome, ChaseStepOutcome::Resting { .. }));
        let p1_mid = db.get_position_by_id(pid1).unwrap().unwrap();
        assert_eq!(p1_mid.status, "WORKING");

        // Once created_at_ms ages beyond maker_only_rest_ttl_ms (30_000ms), clean cancel with NO cooldown
        ord1.created_at_ms -= 35_000;
        let exp_outcome = chaser.advance_working_order(&ord1).unwrap();
        assert!(matches!(
            exp_outcome,
            ChaseStepOutcome::Cancelled { ref reason, .. } if reason == "HL_ALO_EXPIRED_CANCELLED"
        ));
        assert!(!db.is_asset_in_cooldown(sol.id).unwrap());

        // 2. High-urgency ChaseToIoc still escalates to IOC at chase_count >= max_chases
        let pid2 = chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: eth.clone(),
                side: OrderSide::Long,
                requested_quantity: 1.0,
                reserved_margin_usdc: 100.0,
                portfolio_weight: 0.10,
                s1_weight_contrib: 0.10,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 95.0,
                reason: "LIVE_BREAKOUT_SURGE".to_string(),
                bypass_entry_interval: true,
                entry_urgency: EntryUrgency::ChaseToIoc,
                is_scale_in: false,
            })
            .unwrap();
        db.record_working_order_chase(
            pid2,
            99.99,
            OrderFsmState::ChasingAlo.as_str(),
            None,
            "chase 1",
        )
        .unwrap();
        db.update_asset_live_quote(eth.id, 100.20, 100.20).unwrap();
        let ord2 = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == pid2)
            .unwrap();
        let fill_outcome = chaser.advance_working_order(&ord2).unwrap();
        assert!(matches!(fill_outcome, ChaseStepOutcome::FilledOpen { .. }));
    }
}
