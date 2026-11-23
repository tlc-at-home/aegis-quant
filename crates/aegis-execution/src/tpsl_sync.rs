//! Exchange-side SL/TP trigger synchronization with submit-before-cancel protection,
//! tri-state `OidUpdate` persistence, legacy Market-to-Limit upgrade, and 39-action batching
//! (`aegis-execution::tpsl_sync`, `P0-Risk` Defect #6, `R14-M26`, `R14-M7`, `Sec. 11`).

use crate::http_client::{ExchangeOrderAck, HyperliquidHttpClient, OpenOrdersSnapshot};
use crate::lock_guard::acquire_execution_lock;
use crate::signing::{
    build_protective_stop_order_wire_with_band_bps, build_take_profit_order_wire,
    derive_hl_cloid_hex, effective_hl_tick_size, format_hl_wire_price, format_hl_wire_size,
    format_hl_wire_size_floor, snap_to_tick, CancelWire, OrderWire,
    HL_SL_TRIGGER_LIMIT_SLIPPAGE_PCT,
};
use aegis_core::{
    classify_hl_trigger_is_tp, extract_hl_oid, is_hl_trigger_order, now_ms, parse_hl_f64,
    parse_hl_oid, ConfigMap, ExchangeAsset, OrderSide, Result,
};
use aegis_db::{DatabaseManager, OidUpdate};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Parsed live Hyperliquid trigger order metadata from `frontendOpenOrders`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TriggerOrderInfo {
    pub oid: u64,
    pub is_tp: bool,
    pub is_market_order: bool,
    pub trigger_px: f64,
    pub qty: f64,
}

/// Request specification for synchronizing SL and TP triggers on a single `OPEN` position.
#[derive(Debug, Clone)]
pub struct TpslSyncRequest<'a> {
    pub position_id: i64,
    pub asset: &'a ExchangeAsset,
    pub side: OrderSide,
    pub quantity: f64,
    pub mark_price: f64,
    pub desired_stop_px: f64,
    pub desired_tp_px: f64,
    pub existing_sl_oid: Option<&'a str>,
    pub existing_tp_oid: Option<&'a str>,
    pub replace_sl: bool,
    pub replace_tp: bool,
}

/// Detailed outcome of synchronizing SL and TP trigger orders for a position.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TpslSyncReport {
    pub position_id: i64,
    pub stop_px: f64,
    pub sl_oid: Option<String>,
    pub tp_px: f64,
    pub tp_oid: Option<String>,
    pub sl_failed: bool,
    pub tp_failed: bool,
    pub errors: Vec<String>,
}

struct ParsedOpenOrders {
    have_resting_snapshot: bool,
    has_trigger_metadata: bool,
    resting_oids: HashSet<u64>,
    cloid_to_oid: HashMap<String, u64>,
    triggers_by_coin: HashMap<String, Vec<TriggerOrderInfo>>,
    raw_orders: Vec<Value>,
}

impl ParsedOpenOrders {
    fn from_snapshot(snap: &OpenOrdersSnapshot) -> Self {
        let mut out = Self {
            have_resting_snapshot: true,
            has_trigger_metadata: snap.metadata_complete,
            resting_oids: HashSet::new(),
            cloid_to_oid: HashMap::new(),
            triggers_by_coin: HashMap::new(),
            raw_orders: snap.orders.clone(),
        };
        for ord in &snap.orders {
            let Some(oid) = extract_hl_oid(ord) else {
                continue;
            };
            out.resting_oids.insert(oid);
            if let Some(cloid) = ord.get("cloid").and_then(Value::as_str) {
                out.cloid_to_oid
                    .insert(cloid.trim().to_ascii_lowercase(), oid);
            }
            if snap.metadata_complete && is_hl_trigger_order(ord) {
                if let Some(is_tp) = classify_hl_trigger_is_tp(ord) {
                    let coin = ord
                        .get("coin")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_ascii_uppercase();
                    if !coin.is_empty() {
                        let is_market_order = ord
                            .get("orderType")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_ascii_lowercase()
                            .contains("market");
                        out.triggers_by_coin
                            .entry(coin)
                            .or_default()
                            .push(TriggerOrderInfo {
                                oid,
                                is_tp,
                                is_market_order,
                                trigger_px: parse_hl_f64(ord.get("triggerPx")),
                                qty: parse_hl_f64(ord.get("origSz"))
                                    .max(parse_hl_f64(ord.get("sz"))),
                            });
                    }
                }
            }
        }
        out
    }

    fn empty_missing() -> Self {
        Self {
            have_resting_snapshot: false,
            has_trigger_metadata: false,
            resting_oids: HashSet::new(),
            cloid_to_oid: HashMap::new(),
            triggers_by_coin: HashMap::new(),
            raw_orders: Vec::new(),
        }
    }
}

fn resolve_target_tpsl_prices(
    asset: &ExchangeAsset,
    side: OrderSide,
    mark_price: f64,
    desired_stop_px: f64,
    desired_tp_px: f64,
    cfg: &ConfigMap,
) -> (f64, f64) {
    let ref_mark = if mark_price.is_finite() && mark_price > 0.0 {
        mark_price
    } else if asset.mark_price.is_finite() && asset.mark_price > 0.0 {
        asset.mark_price
    } else {
        asset.mid_price
    };
    let tick = effective_hl_tick_size(
        if ref_mark > 0.0 {
            ref_mark
        } else {
            desired_stop_px.max(desired_tp_px).max(1.0)
        },
        asset.tick_size,
        asset.sz_decimals,
    )
    .unwrap_or_else(|| asset.tick_size.max(1e-8));
    let default_sl_pct = cfg
        .get_f64("protective_stop_default_pct", 0.04)
        .clamp(0.005, 0.35);
    let default_tp_pct = cfg
        .get_f64("take_profit_default_pct", 0.055)
        .clamp(0.01, 0.80);

    let stop_px = if desired_stop_px.is_finite()
        && desired_stop_px > 0.0
        && (ref_mark <= 0.0
            || match side {
                OrderSide::Long => desired_stop_px < ref_mark - tick * 0.5,
                OrderSide::Short => desired_stop_px > ref_mark + tick * 0.5,
            }) {
        snap_to_tick(desired_stop_px, tick)
    } else if ref_mark > 0.0 {
        match side {
            OrderSide::Long => snap_to_tick((ref_mark * (1.0 - default_sl_pct)).max(tick), tick),
            OrderSide::Short => snap_to_tick(ref_mark * (1.0 + default_sl_pct), tick),
        }
    } else {
        0.0
    };

    let tp_px = if desired_tp_px.is_finite()
        && desired_tp_px > 0.0
        && (ref_mark <= 0.0
            || match side {
                OrderSide::Long => desired_tp_px > ref_mark + tick * 0.5,
                OrderSide::Short => desired_tp_px < ref_mark - tick * 0.5,
            }) {
        snap_to_tick(desired_tp_px, tick)
    } else if ref_mark > 0.0 {
        match side {
            OrderSide::Long => snap_to_tick(ref_mark * (1.0 + default_tp_pct), tick),
            OrderSide::Short => snap_to_tick((ref_mark * (1.0 - default_tp_pct)).max(tick), tick),
        }
    } else {
        0.0
    };

    (stop_px, tp_px)
}

#[derive(Debug, Clone)]
enum LegResolution {
    NotRequested,
    CanonicalKept {
        eoid: String,
    },
    NeedsReplace {
        wire: OrderWire,
        expected_cloid: String,
        old_oids_to_cancel: Vec<u64>,
        trigger_px: f64,
        quantity: f64,
    },
    InvalidKeepExisting {
        existing_oid: Option<String>,
        error: String,
    },
}

#[allow(clippy::too_many_arguments)]
fn plan_single_trigger_leg(
    is_tp: bool,
    position_id: i64,
    asset: &ExchangeAsset,
    side: OrderSide,
    quantity: f64,
    trigger_px: f64,
    existing_oid_str: Option<&str>,
    force_replace: bool,
    parsed: &ParsedOpenOrders,
    sl_band_bps: f64,
    dup_cancels: &mut Vec<CancelWire>,
) -> LegResolution {
    if !trigger_px.is_finite() || trigger_px <= 0.0 || !quantity.is_finite() || quantity <= 0.0 {
        return LegResolution::NotRequested;
    }

    let tick = effective_hl_tick_size(trigger_px, asset.tick_size, asset.sz_decimals)
        .unwrap_or_else(|| asset.tick_size.max(1e-8));
    let existing_num_oid = parse_hl_oid(existing_oid_str).or_else(|| {
        existing_oid_str
            .map(|s| s.trim().to_ascii_lowercase())
            .and_then(|c| parsed.cloid_to_oid.get(&c).copied())
    });

    let coin_upper = asset.coin.trim().to_ascii_uppercase();
    let leg_triggers: Vec<TriggerOrderInfo> = parsed
        .triggers_by_coin
        .get(&coin_upper)
        .into_iter()
        .flatten()
        .copied()
        .filter(|t| t.is_tp == is_tp)
        .collect();

    let exp_px_str = format_hl_wire_price(trigger_px, tick, asset.sz_decimals);
    let exp_sz_str = {
        let floored = format_hl_wire_size_floor(quantity, asset.lot_step_size, asset.sz_decimals);
        if floored == "0" {
            format_hl_wire_size(quantity, asset.lot_step_size, asset.sz_decimals)
        } else {
            floored
        }
    };
    let matches_canonical = |t: &TriggerOrderInfo| -> bool {
        if t.is_market_order {
            // Auto-upgrade legacy "Stop Market" / "Take Profit Market" orders to Limit!
            return false;
        }
        let px_ok = (t.trigger_px > 0.0
            && format_hl_wire_price(t.trigger_px, tick, asset.sz_decimals) == exp_px_str)
            || (t.trigger_px > 0.0 && (t.trigger_px - trigger_px).abs() <= tick * 1.5);
        let sz_ok = (t.qty > 0.0
            && format_hl_wire_size_floor(t.qty, asset.lot_step_size, asset.sz_decimals)
                == exp_sz_str)
            || (t.qty > 0.0 && (t.qty - quantity).abs() <= asset.lot_step_size.max(1e-8) * 1.5);
        px_ok && sz_ok
    };

    if !force_replace {
        if parsed.have_resting_snapshot && parsed.has_trigger_metadata {
            let canonical_oid = existing_num_oid
                .filter(|oid| {
                    leg_triggers
                        .iter()
                        .any(|t| t.oid == *oid && matches_canonical(t))
                })
                .or_else(|| {
                    leg_triggers
                        .iter()
                        .find(|t| matches_canonical(t))
                        .map(|t| t.oid)
                });
            if let Some(keep_oid) = canonical_oid {
                let mut seen: HashSet<u64> = HashSet::new();
                for t in &leg_triggers {
                    if t.oid != keep_oid && seen.insert(t.oid) {
                        dup_cancels.push(CancelWire {
                            a: asset.asset_index,
                            o: t.oid,
                        });
                    }
                }
                return LegResolution::CanonicalKept {
                    eoid: format!("hl-{keep_oid}"),
                };
            }
        } else if let Some(oid) = existing_num_oid {
            if !parsed.have_resting_snapshot || parsed.resting_oids.contains(&oid) {
                return LegResolution::CanonicalKept {
                    eoid: format!("hl-{oid}"),
                };
            }
        }
    }

    // Collect old trigger OIDs to retire ONLY AFTER replacement submission succeeds (Defect #6).
    let mut old_oids_to_cancel: Vec<u64> = Vec::new();
    let mut cancel_seen: HashSet<u64> = HashSet::new();
    for t in &leg_triggers {
        if cancel_seen.insert(t.oid) {
            old_oids_to_cancel.push(t.oid);
        }
    }
    if let Some(old_oid) = existing_num_oid {
        if (!parsed.have_resting_snapshot || parsed.resting_oids.contains(&old_oid))
            && cancel_seen.insert(old_oid)
        {
            old_oids_to_cancel.push(old_oid);
        }
    }

    let coid = format!(
        "v2-{}-{position_id}-{}",
        if is_tp { "tp" } else { "sl" },
        now_ms()
    );
    let expected_cloid = derive_hl_cloid_hex(&coid);
    let wire = if is_tp {
        build_take_profit_order_wire(
            asset.asset_index,
            side,
            quantity,
            trigger_px,
            asset.tick_size,
            asset.lot_step_size,
            asset.sz_decimals,
            Some(&coid),
        )
    } else {
        build_protective_stop_order_wire_with_band_bps(
            asset.asset_index,
            side,
            quantity,
            trigger_px,
            asset.tick_size,
            asset.lot_step_size,
            asset.sz_decimals,
            sl_band_bps,
            Some(&coid),
        )
    };

    if wire.s == "0" || wire.p == "0" {
        return LegResolution::InvalidKeepExisting {
            existing_oid: existing_oid_str.map(str::to_string),
            error: format!(
                "Invalid trigger wire for {} {} (p={}, s={})",
                asset.symbol,
                if is_tp { "TP" } else { "SL" },
                wire.p,
                wire.s
            ),
        };
    }

    LegResolution::NeedsReplace {
        wire,
        expected_cloid,
        old_oids_to_cancel,
        trigger_px,
        quantity,
    }
}

fn status_label_is_active_resting(status: &str) -> bool {
    let lower = status.trim().to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "open" | "resting" | "triggered" | "waitingfortrigger" | "waitingforfill"
    )
}

fn resolve_accepted_trigger_eoid(
    http: &HyperliquidHttpClient,
    ack: &ExchangeOrderAck,
    wallet: &str,
    item: &PendingReplacementWire,
    parsed: &ParsedOpenOrders,
    cfg: &ConfigMap,
) -> String {
    if let Some(ref eoid) = ack.exchange_order_id {
        return eoid.clone();
    }
    if !wallet.is_empty() {
        if let Ok(Some(st)) = http.fetch_order_status_by_cloid(wallet, &item.expected_cloid, cfg) {
            if st.oid > 0 {
                return format!("hl-{}", st.oid);
            }
        }
        if let Ok(front_orders) = http.fetch_frontend_open_orders(wallet, cfg) {
            if let Some(eoid) = find_cloid_or_trigger_eoid(
                &front_orders,
                &item.coin,
                item.is_tp,
                &item.expected_cloid,
            ) {
                return eoid;
            }
        }
    }
    if let Some(eoid) = find_cloid_or_trigger_eoid(
        &parsed.raw_orders,
        &item.coin,
        item.is_tp,
        &item.expected_cloid,
    ) {
        return eoid;
    }
    item.expected_cloid.clone()
}

fn find_cloid_or_trigger_eoid(
    orders: &[Value],
    coin: &str,
    is_tp: bool,
    expected_cloid: &str,
) -> Option<String> {
    for ord in orders {
        let cloid_match = ord
            .get("cloid")
            .and_then(Value::as_str)
            .is_some_and(|c| c.trim().eq_ignore_ascii_case(expected_cloid));
        if cloid_match {
            if let Some(oid) = extract_hl_oid(ord) {
                return Some(format!("hl-{oid}"));
            }
        }
    }
    for ord in orders {
        let coin_match = ord
            .get("coin")
            .and_then(Value::as_str)
            .is_some_and(|c| c.trim().eq_ignore_ascii_case(coin));
        if coin_match && is_hl_trigger_order(ord) && classify_hl_trigger_is_tp(ord) == Some(is_tp) {
            if let Some(oid) = extract_hl_oid(ord) {
                return Some(format!("hl-{oid}"));
            }
        }
    }
    None
}

fn probe_cloid_after_submit_timeout(
    http: &HyperliquidHttpClient,
    wallet: &str,
    expected_cloid: &str,
    cfg: &ConfigMap,
) -> Option<String> {
    if wallet.trim().is_empty() {
        return None;
    }
    match http.fetch_order_status_by_cloid(wallet, expected_cloid, cfg) {
        Ok(Some(st)) if status_label_is_active_resting(&st.status) => {
            if st.oid > 0 {
                Some(format!("hl-{}", st.oid))
            } else {
                Some(expected_cloid.to_string())
            }
        }
        _ => None,
    }
}

#[derive(Debug, Clone)]
enum LegOutcome {
    NotRequested,
    KeptCanonical(String),
    Replaced(String),
    FailedKeepExisting(Option<String>, String),
}

impl LegOutcome {
    fn to_oid_update(&self) -> OidUpdate<'_> {
        match self {
            Self::KeptCanonical(s) | Self::Replaced(s) if !s.trim().is_empty() => {
                OidUpdate::Set(s.as_str())
            }
            _ => OidUpdate::Keep,
        }
    }

    fn effective_oid(&self) -> Option<String> {
        match self {
            Self::NotRequested => None,
            Self::KeptCanonical(s) | Self::Replaced(s) => Some(s.clone()),
            Self::FailedKeepExisting(prev, _) => prev.clone(),
        }
    }

    fn failed(&self) -> bool {
        matches!(self, Self::FailedKeepExisting(_, _))
    }

    fn error_msg(&self) -> Option<&str> {
        match self {
            Self::FailedKeepExisting(_, msg) => Some(msg.as_str()),
            _ => None,
        }
    }
}

/// Synchronizes exchange SL/TP trigger orders for a single `OPEN` position with submit-before-cancel
/// protection (`P0-Risk` Defect #6), tri-state `OidUpdate` persistence (`R14-M26`), and `OpenOrdersSnapshot`
/// metadata awareness (`R14-M7`).
pub fn ensure_exchange_tpsl_orders_detailed(
    http: &HyperliquidHttpClient,
    db: &DatabaseManager,
    req: &TpslSyncRequest<'_>,
    resting_exchange_oids: Option<&HashSet<u64>>,
    open_orders_snapshot: Option<&OpenOrdersSnapshot>,
    cfg: &ConfigMap,
) -> Result<TpslSyncReport> {
    let batch = sync_tpsl_batch_for_positions(
        http,
        db,
        std::slice::from_ref(req),
        resting_exchange_oids,
        open_orders_snapshot,
        cfg,
    );
    match batch.into_iter().next() {
        Some((_, res)) => res,
        None => Ok(TpslSyncReport {
            position_id: req.position_id,
            stop_px: req.desired_stop_px,
            sl_oid: req.existing_sl_oid.map(str::to_string),
            tp_px: req.desired_tp_px,
            tp_oid: req.existing_tp_oid.map(str::to_string),
            sl_failed: false,
            tp_failed: false,
            errors: Vec::new(),
        }),
    }
}

struct PlannedPosition<'a> {
    req: &'a TpslSyncRequest<'a>,
    stop_px: f64,
    tp_px: f64,
    sl_plan: LegResolution,
    tp_plan: LegResolution,
}

struct PendingReplacementWire {
    pos_idx: usize,
    is_tp: bool,
    asset_index: u32,
    coin: String,
    symbol: String,
    wire: OrderWire,
    expected_cloid: String,
    old_oids_to_cancel: Vec<u64>,
    existing_oid: Option<String>,
    trigger_px: f64,
    quantity: f64,
}

/// Synchronizes SL/TP trigger orders for 1 or more `OPEN` positions in a single shared `OpenOrdersSnapshot`
/// pass, batching replacement wires (`<= 39` per `/exchange` request) and cancelling retired OIDs ONLY
/// after replacement acceptance (`Sec. 11`, Defect #6, `R14-H2`).
pub fn sync_tpsl_batch_for_positions(
    http: &HyperliquidHttpClient,
    db: &DatabaseManager,
    reqs: &[TpslSyncRequest<'_>],
    resting_exchange_oids: Option<&HashSet<u64>>,
    open_orders_snapshot: Option<&OpenOrdersSnapshot>,
    cfg: &ConfigMap,
) -> Vec<(i64, Result<TpslSyncReport>)> {
    if reqs.is_empty() {
        return Vec::new();
    }

    let is_live_exchange = crate::engine::is_live_exchange_active(db, cfg);
    if !is_live_exchange {
        let mut results = Vec::with_capacity(reqs.len());
        let _guard = acquire_execution_lock();
        for req in reqs {
            let (stop_px, tp_px) = resolve_target_tpsl_prices(
                req.asset,
                req.side,
                req.mark_price,
                req.desired_stop_px,
                req.desired_tp_px,
                cfg,
            );
            let sl_oid = format!("paper-sl-{}", req.position_id);
            let tp_oid = format!("paper-tp-{}", req.position_id);
            let db_res = db.update_position_tpsl_orders(
                req.position_id,
                stop_px,
                OidUpdate::Set(&sl_oid),
                tp_px,
                OidUpdate::Set(&tp_oid),
            );
            results.push((
                req.position_id,
                db_res.map(|()| TpslSyncReport {
                    position_id: req.position_id,
                    stop_px,
                    sl_oid: Some(sl_oid),
                    tp_px,
                    tp_oid: Some(tp_oid),
                    sl_failed: false,
                    tp_failed: false,
                    errors: Vec::new(),
                }),
            ));
        }
        return results;
    }

    let pk = cfg.get_str("private_key", "");
    if pk.trim().is_empty() {
        tracing::warn!(
            count = reqs.len(),
            "private_key is empty/missing in live mode; skipping TP/SL exchange sync without writing paper OIDs"
        );
        return reqs
            .iter()
            .map(|req| {
                let (stop_px, tp_px) = resolve_target_tpsl_prices(
                    req.asset,
                    req.side,
                    req.mark_price,
                    req.desired_stop_px,
                    req.desired_tp_px,
                    cfg,
                );
                (
                    req.position_id,
                    Ok(TpslSyncReport {
                        position_id: req.position_id,
                        stop_px,
                        sl_oid: req.existing_sl_oid.map(str::to_string),
                        tp_px,
                        tp_oid: req.existing_tp_oid.map(str::to_string),
                        sl_failed: false,
                        tp_failed: false,
                        errors: Vec::new(),
                    }),
                )
            })
            .collect();
    }

    let wallet = cfg.get_str("wallet_address", "");
    let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);
    let sl_band_bps = cfg
        .get_f64(
            "sl_trigger_limit_band_bps",
            HL_SL_TRIGGER_LIMIT_SLIPPAGE_PCT * 10_000.0,
        )
        .clamp(20.0, crate::signing::HL_ORACLE_BAND_PCT * 10_000.0);

    // Fetch OpenOrdersSnapshot once across all positions when not supplied by caller.
    let parsed = if let Some(snap) = open_orders_snapshot {
        ParsedOpenOrders::from_snapshot(snap)
    } else if let Some(set) = resting_exchange_oids {
        if set.is_empty() {
            ParsedOpenOrders {
                have_resting_snapshot: true,
                has_trigger_metadata: true,
                resting_oids: HashSet::new(),
                cloid_to_oid: HashMap::new(),
                triggers_by_coin: HashMap::new(),
                raw_orders: Vec::new(),
            }
        } else if !wallet.trim().is_empty() {
            match http.fetch_open_orders_with_metadata(&wallet, cfg) {
                Ok(snap) => ParsedOpenOrders::from_snapshot(&snap),
                Err(_) => ParsedOpenOrders {
                    have_resting_snapshot: true,
                    has_trigger_metadata: false,
                    resting_oids: set.clone(),
                    cloid_to_oid: HashMap::new(),
                    triggers_by_coin: HashMap::new(),
                    raw_orders: Vec::new(),
                },
            }
        } else {
            ParsedOpenOrders {
                have_resting_snapshot: true,
                has_trigger_metadata: false,
                resting_oids: set.clone(),
                cloid_to_oid: HashMap::new(),
                triggers_by_coin: HashMap::new(),
                raw_orders: Vec::new(),
            }
        }
    } else if !wallet.trim().is_empty() {
        match http.fetch_open_orders_with_metadata(&wallet, cfg) {
            Ok(snap) => ParsedOpenOrders::from_snapshot(&snap),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to fetch open orders snapshot before TP/SL reconciliation"
                );
                ParsedOpenOrders::empty_missing()
            }
        }
    } else {
        ParsedOpenOrders::empty_missing()
    };

    let mut dup_cancels: Vec<CancelWire> = Vec::new();
    let mut planned: Vec<PlannedPosition<'_>> = Vec::with_capacity(reqs.len());
    let mut sl_outcomes: Vec<LegOutcome> = Vec::with_capacity(reqs.len());
    let mut tp_outcomes: Vec<LegOutcome> = Vec::with_capacity(reqs.len());
    let mut pending_wires: Vec<PendingReplacementWire> = Vec::new();

    for (pos_idx, req) in reqs.iter().enumerate() {
        let (stop_px, tp_px) = resolve_target_tpsl_prices(
            req.asset,
            req.side,
            req.mark_price,
            req.desired_stop_px,
            req.desired_tp_px,
            cfg,
        );
        let sl_plan = plan_single_trigger_leg(
            false,
            req.position_id,
            req.asset,
            req.side,
            req.quantity,
            stop_px,
            req.existing_sl_oid,
            req.replace_sl,
            &parsed,
            sl_band_bps,
            &mut dup_cancels,
        );
        let tp_plan = plan_single_trigger_leg(
            true,
            req.position_id,
            req.asset,
            req.side,
            req.quantity,
            tp_px,
            req.existing_tp_oid,
            req.replace_tp,
            &parsed,
            sl_band_bps,
            &mut dup_cancels,
        );

        let sl_out = match &sl_plan {
            LegResolution::NotRequested => LegOutcome::NotRequested,
            LegResolution::CanonicalKept { eoid } => LegOutcome::KeptCanonical(eoid.clone()),
            LegResolution::InvalidKeepExisting {
                existing_oid,
                error,
            } => LegOutcome::FailedKeepExisting(existing_oid.clone(), error.clone()),
            LegResolution::NeedsReplace {
                wire,
                expected_cloid,
                old_oids_to_cancel,
                trigger_px,
                quantity,
            } => {
                pending_wires.push(PendingReplacementWire {
                    pos_idx,
                    is_tp: false,
                    asset_index: req.asset.asset_index,
                    coin: req.asset.coin.clone(),
                    symbol: req.asset.symbol.clone(),
                    wire: wire.clone(),
                    expected_cloid: expected_cloid.clone(),
                    old_oids_to_cancel: old_oids_to_cancel.clone(),
                    existing_oid: req.existing_sl_oid.map(str::to_string),
                    trigger_px: *trigger_px,
                    quantity: *quantity,
                });
                LegOutcome::FailedKeepExisting(
                    req.existing_sl_oid.map(str::to_string),
                    "PENDING".to_string(),
                )
            }
        };
        let tp_out = match &tp_plan {
            LegResolution::NotRequested => LegOutcome::NotRequested,
            LegResolution::CanonicalKept { eoid } => LegOutcome::KeptCanonical(eoid.clone()),
            LegResolution::InvalidKeepExisting {
                existing_oid,
                error,
            } => LegOutcome::FailedKeepExisting(existing_oid.clone(), error.clone()),
            LegResolution::NeedsReplace {
                wire,
                expected_cloid,
                old_oids_to_cancel,
                trigger_px,
                quantity,
            } => {
                pending_wires.push(PendingReplacementWire {
                    pos_idx,
                    is_tp: true,
                    asset_index: req.asset.asset_index,
                    coin: req.asset.coin.clone(),
                    symbol: req.asset.symbol.clone(),
                    wire: wire.clone(),
                    expected_cloid: expected_cloid.clone(),
                    old_oids_to_cancel: old_oids_to_cancel.clone(),
                    existing_oid: req.existing_tp_oid.map(str::to_string),
                    trigger_px: *trigger_px,
                    quantity: *quantity,
                });
                LegOutcome::FailedKeepExisting(
                    req.existing_tp_oid.map(str::to_string),
                    "PENDING".to_string(),
                )
            }
        };

        sl_outcomes.push(sl_out);
        tp_outcomes.push(tp_out);
        planned.push(PlannedPosition {
            req,
            stop_px,
            tp_px,
            sl_plan,
            tp_plan,
        });
    }

    let _ = &planned; // keeps ownership clear for req/stop_px/tp_px below
    let mut confirmed_cancels: Vec<CancelWire> = dup_cancels;
    let mut cancel_seen: HashSet<(u32, u64)> =
        confirmed_cancels.iter().map(|c| (c.a, c.o)).collect();

    let mut add_retired_cancels = |asset_idx: u32, old_oids: &[u64], new_eoid: &str| {
        let new_num = parse_hl_oid(Some(new_eoid));
        for &old_oid in old_oids {
            if Some(old_oid) != new_num && cancel_seen.insert((asset_idx, old_oid)) {
                confirmed_cancels.push(CancelWire {
                    a: asset_idx,
                    o: old_oid,
                });
            }
        }
    };

    if !pending_wires.is_empty() {
        let use_position_tpsl_grouping = reqs.len() == 1
            && pending_wires.len() == 2
            && !pending_wires[0].is_tp
            && pending_wires[1].is_tp;
        let wires: Vec<OrderWire> = pending_wires.iter().map(|p| p.wire.clone()).collect();
        let meta: Vec<(f64, f64, f64)> = pending_wires
            .iter()
            .map(|p| (p.quantity, p.trigger_px, taker_fee_bps))
            .collect();
        let primary_grouping = if use_position_tpsl_grouping || pending_wires.len() == 1 {
            "positionTpsl"
        } else {
            "na"
        };

        match http.submit_orders_batch(wires, &meta, primary_grouping, &pk, Some(db), cfg) {
            Ok(acks) => {
                for (item, ack) in pending_wires.iter().zip(acks) {
                    let final_ack = if !ack.accepted {
                        // Retry individually via submit_order_wire (falls back from positionTpsl -> "na")
                        match http.submit_order_wire(
                            item.wire.clone(),
                            item.quantity,
                            item.trigger_px,
                            taker_fee_bps,
                            &pk,
                            Some(db),
                            cfg,
                        ) {
                            Ok(fb_ack) => fb_ack,
                            Err(e) => {
                                let probed = probe_cloid_after_submit_timeout(
                                    http,
                                    &wallet,
                                    &item.expected_cloid,
                                    cfg,
                                );
                                if let Some(new_eoid) = probed {
                                    add_retired_cancels(
                                        item.asset_index,
                                        &item.old_oids_to_cancel,
                                        &new_eoid,
                                    );
                                    let target = if item.is_tp {
                                        &mut tp_outcomes[item.pos_idx]
                                    } else {
                                        &mut sl_outcomes[item.pos_idx]
                                    };
                                    *target = LegOutcome::Replaced(new_eoid);
                                } else {
                                    let msg = format!(
                                        "Failed to submit {} {} trigger (kept existing protection): {e}",
                                        item.symbol,
                                        if item.is_tp { "TP" } else { "SL" }
                                    );
                                    tracing::warn!(symbol = %item.symbol, error = %e, "{msg}");
                                    let target = if item.is_tp {
                                        &mut tp_outcomes[item.pos_idx]
                                    } else {
                                        &mut sl_outcomes[item.pos_idx]
                                    };
                                    *target = LegOutcome::FailedKeepExisting(
                                        item.existing_oid.clone(),
                                        msg,
                                    );
                                }
                                continue;
                            }
                        }
                    } else {
                        ack
                    };

                    if final_ack.accepted {
                        let new_eoid = resolve_accepted_trigger_eoid(
                            http, &final_ack, &wallet, item, &parsed, cfg,
                        );
                        add_retired_cancels(item.asset_index, &item.old_oids_to_cancel, &new_eoid);
                        let target = if item.is_tp {
                            &mut tp_outcomes[item.pos_idx]
                        } else {
                            &mut sl_outcomes[item.pos_idx]
                        };
                        *target = LegOutcome::Replaced(new_eoid);
                    } else {
                        let err_txt = final_ack
                            .raw_error
                            .unwrap_or_else(|| final_ack.status.clone());
                        let msg = format!(
                            "Hyperliquid rejected {} {} trigger (kept existing protection): {}",
                            item.symbol,
                            if item.is_tp { "TP" } else { "SL" },
                            err_txt
                        );
                        tracing::warn!(
                            symbol = %item.symbol,
                            leg = if item.is_tp { "TP" } else { "SL" },
                            trigger_px = item.trigger_px,
                            error = %err_txt,
                            "Hyperliquid rejected replacement trigger; preserving existing OIDs"
                        );
                        let target = if item.is_tp {
                            &mut tp_outcomes[item.pos_idx]
                        } else {
                            &mut sl_outcomes[item.pos_idx]
                        };
                        *target = LegOutcome::FailedKeepExisting(item.existing_oid.clone(), msg);
                    }
                }
            }
            Err(batch_err) => {
                // Network timeout / transport error: probe each cloid via orderStatus before deciding!
                tracing::warn!(
                    error = %batch_err,
                    count = pending_wires.len(),
                    "Trigger batch submit failed; probing orderStatus by cloid before cancelling old OIDs"
                );
                for item in &pending_wires {
                    if let Some(new_eoid) =
                        probe_cloid_after_submit_timeout(http, &wallet, &item.expected_cloid, cfg)
                    {
                        add_retired_cancels(item.asset_index, &item.old_oids_to_cancel, &new_eoid);
                        let target = if item.is_tp {
                            &mut tp_outcomes[item.pos_idx]
                        } else {
                            &mut sl_outcomes[item.pos_idx]
                        };
                        *target = LegOutcome::Replaced(new_eoid);
                    } else {
                        let msg = format!(
                            "Trigger submit failed and cloid probe unconfirmed for {} {} (kept existing protection): {batch_err}",
                            item.symbol,
                            if item.is_tp { "TP" } else { "SL" }
                        );
                        let target = if item.is_tp {
                            &mut tp_outcomes[item.pos_idx]
                        } else {
                            &mut sl_outcomes[item.pos_idx]
                        };
                        *target = LegOutcome::FailedKeepExisting(item.existing_oid.clone(), msg);
                    }
                }
            }
        }
    }

    // Cancel retired/duplicate OIDs ONLY AFTER replacement triggers are confirmed resting!
    if !confirmed_cancels.is_empty() {
        if let Err(e) = http.cancel_orders_batch(confirmed_cancels, &pk, Some(db), cfg) {
            tracing::warn!(
                error = %e,
                "Failed to cancel retired trigger orders after replacement; position remains protected"
            );
        }
    }

    // Short locked DB commit using tri-state OidUpdate (R14-M26).
    let mut results = Vec::with_capacity(planned.len());
    let _guard = acquire_execution_lock();
    for (idx, p) in planned.iter().enumerate() {
        let sl_out = &sl_outcomes[idx];
        let tp_out = &tp_outcomes[idx];
        let mut errors = Vec::new();
        if let Some(e) = sl_out.error_msg() {
            errors.push(e.to_string());
        }
        if let Some(e) = tp_out.error_msg() {
            errors.push(e.to_string());
        }

        // Never overwrite persisted stop/TP price when the replacement failed (R14-M26).
        let db_stop_px = if sl_out.failed() && p.req.replace_sl {
            0.0
        } else {
            p.stop_px
        };
        let db_tp_px = if tp_out.failed() && p.req.replace_tp {
            0.0
        } else {
            p.tp_px
        };
        let _ = (&p.sl_plan, &p.tp_plan);

        let db_res = db.update_position_tpsl_orders(
            p.req.position_id,
            db_stop_px,
            sl_out.to_oid_update(),
            db_tp_px,
            tp_out.to_oid_update(),
        );
        results.push((
            p.req.position_id,
            db_res.map(|()| TpslSyncReport {
                position_id: p.req.position_id,
                stop_px: p.stop_px,
                sl_oid: sl_out.effective_oid(),
                tp_px: p.tp_px,
                tp_oid: tp_out.effective_oid(),
                sl_failed: sl_out.failed(),
                tp_failed: tp_out.failed(),
                errors,
            }),
        ));
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_db::{AssetUpsertInput, WorkingOrderInput};
    use aegis_test_support::mock_http::{MockHttpResponse, MockHttpServer};
    use parking_lot::Mutex;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn test_defect6_tpsl_replace_before_cancel_preserves_protection_on_submit_failure_and_timeout()
    {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let sol_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "SOL".to_string(),
                asset_index: 3,
                sz_decimals: 2,
                tick_size: 0.01,
                lot_step_size: 0.01,
                max_leverage: 20.0,
                mark_price: 180.0,
                mid_price: 180.0,
                oracle_price: 180.0,
                funding_rate_hourly: 0.0,
                day_volume_usdc: 10_000_000.0,
                open_interest_usdc: 5_000_000.0,
                prev_day_price: 175.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();
        let sol = db.get_asset_by_id(sol_id).unwrap().unwrap();
        let pid = db
            .create_working_order(&WorkingOrderInput {
                asset_id: sol_id,
                client_order_id: "v2-sol-defect6".to_string(),
                side: OrderSide::Long,
                limit_price: 180.0,
                requested_quantity: 5.0,
                reserved_margin_usdc: 450.0,
                portfolio_weight: 0.1,
                s1_weight_contrib: 0.1,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 170.0,
                take_profit_px: Some(196.0),
                exchange_order_id: Some("hl-10".to_string()),
                reason: "TEST".to_string(),
            })
            .unwrap();
        db.promote_working_to_open(
            pid,
            180.0,
            5.0,
            0.1,
            170.0,
            Some(196.0),
            Some("hl-10"),
            "FILLED",
        )
        .unwrap();
        db.update_position_tpsl_orders(
            pid,
            170.0,
            OidUpdate::Set("hl-11111"),
            196.0,
            OidUpdate::Set("hl-22222"),
        )
        .unwrap();

        let mode = Arc::new(Mutex::new("reject_replace"));
        let mode_srv = Arc::clone(&mode);
        let cancelled = Arc::new(Mutex::new(Vec::<u64>::new()));
        let cancelled_srv = Arc::clone(&cancelled);
        let submitted = Arc::new(Mutex::new(Vec::<String>::new()));
        let submitted_srv = Arc::clone(&submitted);

        let srv = MockHttpServer::start(move |req| {
            let cur_mode = *mode_srv.lock();
            if req.path == "/info" {
                match req
                    .json_body
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                {
                    "frontendOpenOrders" => MockHttpResponse::ok_json(&json!([
                        {
                            "coin": "SOL",
                            "oid": 11111u64,
                            "isTrigger": true,
                            "orderType": "Stop Limit",
                            "triggerPx": "170.0",
                            "sz": "5.0"
                        },
                        {
                            "coin": "SOL",
                            "oid": 22222u64,
                            "isTrigger": true,
                            "orderType": "Take Profit Limit",
                            "triggerPx": "196.0",
                            "sz": "5.0"
                        }
                    ])),
                    "orderStatus" => {
                        if cur_mode == "timeout_confirmed" {
                            MockHttpResponse::ok_json(&json!({
                                "status": "order",
                                "order": {
                                    "status": "open",
                                    "order": { "oid": 55555u64, "sz": "5.0", "origSz": "5.0", "limitPx": "173.0" }
                                }
                            }))
                        } else {
                            MockHttpResponse::ok_json(&json!({ "status": "unknownOid" }))
                        }
                    }
                    _ => MockHttpResponse::ok_json(&json!([])),
                }
            } else {
                let typ = req
                    .json_body
                    .pointer("/action/type")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if typ == "cancel" {
                    if let Some(arr) = req
                        .json_body
                        .pointer("/action/cancels")
                        .and_then(Value::as_array)
                    {
                        for c in arr {
                            if let Some(o) = c.get("o").and_then(Value::as_u64) {
                                cancelled_srv.lock().push(o);
                            }
                        }
                    }
                    return MockHttpResponse::ok_json(&json!({ "status": "ok" }));
                }
                submitted_srv.lock().push(cur_mode.to_string());
                match cur_mode {
                    "reject_replace" => MockHttpResponse::ok_json(&json!({
                        "status": "ok",
                        "response": { "type": "order", "data": { "statuses": [{ "error": "Insufficient liquidity" }] } }
                    })),
                    "timeout_confirmed" | "timeout_unknown" => {
                        MockHttpResponse::json(500, &json!({ "error": "gateway down" }))
                    }
                    _ => MockHttpResponse::ok_json(&json!({
                        "status": "ok",
                        "response": { "type": "order", "data": { "statuses": [{ "waitingForTrigger": { "oid": 33333u64 } }] } }
                    })),
                }
            }
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");
        cfg.insert(
            "wallet_address",
            "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
        );
        cfg.insert(
            "private_key",
            "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
        );

        let http = HyperliquidHttpClient::new(5);

        // 1. Replacement rejected -> old SL hl-11111 MUST NOT be cancelled and MUST NOT be wiped in DB!
        let rep1 = ensure_exchange_tpsl_orders_detailed(
            &http,
            &db,
            &TpslSyncRequest {
                position_id: pid,
                asset: &sol,
                side: OrderSide::Long,
                quantity: 5.0,
                mark_price: 180.0,
                desired_stop_px: 173.0,
                desired_tp_px: 196.0,
                existing_sl_oid: Some("hl-11111"),
                existing_tp_oid: Some("hl-22222"),
                replace_sl: true,
                replace_tp: false,
            },
            None,
            None,
            &cfg,
        )
        .unwrap();
        assert!(rep1.sl_failed);
        assert!(cancelled.lock().is_empty());
        let p1 = db.get_position_by_id(pid).unwrap().unwrap();
        assert_eq!(p1.protective_stop_oid.as_deref(), Some("hl-11111"));
        assert!((p1.protective_stop_px - 170.0).abs() < 1e-6);

        // 2. Transport error + orderStatus unknownOid -> old SL still NOT cancelled and NOT wiped
        *mode.lock() = "timeout_unknown";
        let rep2 = ensure_exchange_tpsl_orders_detailed(
            &http,
            &db,
            &TpslSyncRequest {
                position_id: pid,
                asset: &sol,
                side: OrderSide::Long,
                quantity: 5.0,
                mark_price: 180.0,
                desired_stop_px: 173.0,
                desired_tp_px: 196.0,
                existing_sl_oid: Some("hl-11111"),
                existing_tp_oid: Some("hl-22222"),
                replace_sl: true,
                replace_tp: false,
            },
            None,
            None,
            &cfg,
        )
        .unwrap();
        assert!(rep2.sl_failed);
        assert!(cancelled.lock().is_empty());
        assert_eq!(
            db.get_position_by_id(pid)
                .unwrap()
                .unwrap()
                .protective_stop_oid
                .as_deref(),
            Some("hl-11111")
        );

        // 3. Transport error + orderStatus confirms resting hl-55555 -> recovers hl-55555 AND THEN cancels hl-11111
        *mode.lock() = "timeout_confirmed";
        let rep3 = ensure_exchange_tpsl_orders_detailed(
            &http,
            &db,
            &TpslSyncRequest {
                position_id: pid,
                asset: &sol,
                side: OrderSide::Long,
                quantity: 5.0,
                mark_price: 180.0,
                desired_stop_px: 173.0,
                desired_tp_px: 196.0,
                existing_sl_oid: Some("hl-11111"),
                existing_tp_oid: Some("hl-22222"),
                replace_sl: true,
                replace_tp: false,
            },
            None,
            None,
            &cfg,
        )
        .unwrap();
        assert!(!rep3.sl_failed);
        assert_eq!(rep3.sl_oid.as_deref(), Some("hl-55555"));
        assert_eq!(*cancelled.lock(), vec![11111u64]);
        let p3 = db.get_position_by_id(pid).unwrap().unwrap();
        assert_eq!(p3.protective_stop_oid.as_deref(), Some("hl-55555"));
        assert!((p3.protective_stop_px - 173.0).abs() < 1e-6);
    }

    #[test]
    fn test_p0_exec2_auto_upgrades_legacy_stop_market_to_stop_limit() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let sol_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "SOL".to_string(),
                asset_index: 3,
                sz_decimals: 2,
                tick_size: 0.01,
                lot_step_size: 0.01,
                max_leverage: 20.0,
                mark_price: 180.0,
                mid_price: 180.0,
                oracle_price: 180.0,
                funding_rate_hourly: 0.0,
                day_volume_usdc: 10_000_000.0,
                open_interest_usdc: 5_000_000.0,
                prev_day_price: 175.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();
        let sol = db.get_asset_by_id(sol_id).unwrap().unwrap();
        let pid = db
            .create_working_order(&WorkingOrderInput {
                asset_id: sol_id,
                client_order_id: "v2-sol-upgrade".to_string(),
                side: OrderSide::Long,
                limit_price: 180.0,
                requested_quantity: 5.0,
                reserved_margin_usdc: 450.0,
                portfolio_weight: 0.1,
                s1_weight_contrib: 0.1,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 170.0,
                take_profit_px: Some(196.0),
                exchange_order_id: Some("hl-10".to_string()),
                reason: "TEST".to_string(),
            })
            .unwrap();
        db.promote_working_to_open(
            pid,
            180.0,
            5.0,
            0.1,
            170.0,
            Some(196.0),
            Some("hl-10"),
            "FILLED",
        )
        .unwrap();

        let srv = MockHttpServer::start(|req| {
            if req.path == "/exchange" {
                let n = req
                    .json_body
                    .pointer("/action/orders")
                    .and_then(Value::as_array)
                    .map(Vec::len)
                    .unwrap_or(1);
                let statuses: Vec<Value> = (0..n)
                    .map(|i| json!({ "waitingForTrigger": { "oid": (77000 + i) as u64 } }))
                    .collect();
                MockHttpResponse::ok_json(&json!({
                    "status": "ok",
                    "response": { "type": "order", "data": { "statuses": statuses } }
                }))
            } else {
                MockHttpResponse::ok_json(&json!([]))
            }
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");
        cfg.insert(
            "wallet_address",
            "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
        );
        cfg.insert(
            "private_key",
            "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
        );

        // Resting openOrders contain legacy "Stop Market" (90001) and "Take Profit Market" (90002)
        let snap = OpenOrdersSnapshot {
            metadata_complete: true,
            orders: vec![
                json!({ "coin": "SOL", "oid": 90001u64, "isTrigger": true, "orderType": "Stop Market", "triggerPx": "170.0", "sz": "5.0" }),
                json!({ "coin": "SOL", "oid": 90002u64, "isTrigger": true, "orderType": "Take Profit Market", "triggerPx": "196.0", "sz": "5.0" }),
            ],
        };

        let http = HyperliquidHttpClient::new(5);
        let rep = ensure_exchange_tpsl_orders_detailed(
            &http,
            &db,
            &TpslSyncRequest {
                position_id: pid,
                asset: &sol,
                side: OrderSide::Long,
                quantity: 5.0,
                mark_price: 180.0,
                desired_stop_px: 170.0,
                desired_tp_px: 196.0,
                existing_sl_oid: Some("hl-90001"),
                existing_tp_oid: Some("hl-90002"),
                replace_sl: false,
                replace_tp: false,
            },
            None,
            Some(&snap),
            &cfg,
        )
        .unwrap();

        assert!(!rep.sl_failed && !rep.tp_failed);
        assert_eq!(rep.sl_oid.as_deref(), Some("hl-77000"));
        assert_eq!(rep.tp_oid.as_deref(), Some("hl-77001"));
        // Verify both orders submitted in 1 positionTpsl batch with isMarket=false, followed by 1 cancel batch
        let ex_reqs: Vec<_> = srv
            .requests()
            .into_iter()
            .filter(|r| r.path == "/exchange")
            .collect();
        assert_eq!(ex_reqs.len(), 2);
        assert_eq!(
            ex_reqs[0]
                .json_body
                .pointer("/action/grouping")
                .and_then(Value::as_str),
            Some("positionTpsl")
        );
        assert_eq!(
            ex_reqs[0]
                .json_body
                .pointer("/action/orders/0/t/trigger/isMarket")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            ex_reqs[0]
                .json_body
                .pointer("/action/orders/1/t/trigger/isMarket")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            ex_reqs[1]
                .json_body
                .pointer("/action/type")
                .and_then(Value::as_str),
            Some("cancel")
        );
    }
}
