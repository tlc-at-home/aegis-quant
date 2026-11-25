//! Unified reduce-only close ladder & non-blocking maker-first passive close tracker
//! (`aegis-execution::close_ladder`, `R14-M2`, `R14-M5`, `R14-M7`, `P1-Strat`).

use crate::http_client::{ExchangeOrderAck, HyperliquidHttpClient, OpenOrdersSnapshot};
use crate::order_chaser::is_post_only_crossing_rejection;
use crate::signing::{
    build_limit_order_wire, derive_hl_cloid_hex, effective_hl_tick_size, format_hl_wire_price,
    format_hl_wire_size_floor, snap_to_tick, CancelWire, LimitOrderType, OrderTypeWire, OrderWire,
    HL_ORACLE_BAND_PCT,
};
use aegis_core::{
    extract_hl_oid, is_hl_trigger_order, now_ms, parse_hl_f64, round_dp, ConfigMap, ExchangeAsset,
    Result,
};
use aegis_db::DatabaseManager;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Progressive IOC close-ladder slippage steps relative to BBO anchor (`0.0%, 0.8%, 2.0%`) (`R14-M5`).
pub const CLOSE_LADDER_SLIPPAGE_STEPS: [f64; 3] = [0.0, 0.008, 0.020];

/// Default passive maker-first reduce-only rest duration in seconds before IOC escalation (`P1-Strat`).
pub const DEFAULT_PASSIVE_CLOSE_REST_SEC: i64 = 8;

/// Urgency lane controlling whether an exit attempts a maker-first resting passive order before IOC escalation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloseUrgency {
    /// Rest one strictly passive reduce-only limit order for `passive_close_rest_sec` before IOC escalation.
    PassiveFirst,
    /// Immediately execute the reduce-only IOC ladder (emergency stop gap, auto-deleverage, cutover flatten).
    ImmediateIoc,
}

/// Request payload for [`submit_reduce_only_close_ladder`] (`R14-M2`).
pub struct ReduceOnlyCloseLadderRequest<'a> {
    pub asset: &'a ExchangeAsset,
    pub position_id: Option<i64>,
    pub live_szi: f64,
    /// Pre-close absolute open position quantity for partial reductions (`ACTION_REDUCE`),
    /// so `clearinghouseState` recovery compares against the pre-reduce size rather than `live_szi`.
    pub pre_close_position_abs_qty: Option<f64>,
    pub reference_mark_px: f64,
    pub l2_bbo: Option<(f64, f64)>,
    pub taker_fee_bps: f64,
    pub cloid_prefix: &'a str,
    /// Optional explicit slippage cap (`None` uses [`HL_ORACLE_BAND_PCT`] `0.038`).
    pub max_slippage_pct: Option<f64>,
    /// Whether to rest one passive reduce-only `Gtc` order inside oracle band when IOC leaves an illiquid remainder.
    pub allow_passive_fallback: bool,
    pub existing_open_orders: Option<&'a OpenOrdersSnapshot>,
}

/// Execution outcome of [`submit_reduce_only_close_ladder`] (`R14-M2`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReduceOnlyCloseLadderResult {
    pub fully_closed: bool,
    pub initial_qty: f64,
    pub filled_qty: f64,
    pub remaining_qty: f64,
    pub vwap_fill_price: Option<f64>,
    pub total_fee_usdc: f64,
    pub last_fill_eoid: Option<String>,
    pub resting_passive_oid: Option<String>,
    pub resting_passive_px: Option<f64>,
    pub kept_existing_passive: bool,
    pub mutated_open_orders: bool,
    pub attempts_submitted: usize,
    pub broke_early_at_band: bool,
}

/// Accumulates partial/full reduce-only close fills across ladder rungs and passive fallbacks.
#[derive(Debug, Clone)]
pub struct CloseFillAccumulator {
    pub close_eoid_opt: Option<String>,
    pub live_fee_usdc: f64,
    pub fill_notional_sum: f64,
    pub fill_qty_sum: f64,
    pub rem_qty: f64,
}

impl CloseFillAccumulator {
    pub fn new(rem_qty: f64) -> Self {
        Self {
            close_eoid_opt: None,
            live_fee_usdc: 0.0,
            fill_notional_sum: 0.0,
            fill_qty_sum: 0.0,
            rem_qty: if rem_qty.is_finite() {
                rem_qty.abs()
            } else {
                0.0
            },
        }
    }

    pub fn record_fill(
        &mut self,
        ack: ExchangeOrderAck,
        fallback_px: f64,
        lot_step_size: f64,
    ) -> bool {
        if ack.accepted && ack.status == "FILLED" && ack.filled_quantity > 0.0 {
            let fill_px = if ack.avg_fill_price.is_finite() && ack.avg_fill_price > 0.0 {
                ack.avg_fill_price
            } else {
                fallback_px
            };
            self.live_fee_usdc += ack.fee_usdc;
            self.fill_notional_sum += fill_px * ack.filled_quantity;
            self.fill_qty_sum += ack.filled_quantity;
            if ack.exchange_order_id.is_some() {
                self.close_eoid_opt = ack.exchange_order_id;
            }
            self.rem_qty = (self.rem_qty - ack.filled_quantity).max(0.0);
            self.rem_qty <= lot_step_size * 0.5 || self.rem_qty <= 1e-9
        } else {
            false
        }
    }

    pub fn vwap_fill_price(&self) -> Option<f64> {
        if self.fill_qty_sum > 1e-9 && self.fill_notional_sum > 0.0 {
            Some(round_dp(self.fill_notional_sum / self.fill_qty_sum, 6))
        } else {
            None
        }
    }
}

/// Returns `true` when Hyperliquid rejected an IOC order due to zero matching liquidity in the book.
pub fn is_ioc_no_match_error(raw_error: Option<&str>) -> bool {
    let Some(err) = raw_error else {
        return false;
    };
    let lower = err.to_ascii_lowercase();
    lower.contains("could not immediately match") || lower.contains("ioccancelrejected")
}

/// Computes the strictly passive reduce-only limit price inside `[band_lo, band_hi]`.
pub fn compute_passive_close_px(
    asset: &ExchangeAsset,
    close_is_buy: bool,
    l2_bbo: Option<(f64, f64)>,
    mark_px: f64,
    band_lo: f64,
    band_hi: f64,
) -> (f64, String) {
    let eff_tick = effective_hl_tick_size(mark_px, asset.tick_size, asset.sz_decimals)
        .unwrap_or_else(|| asset.tick_size.max(1e-8));
    let valid_bbo = l2_bbo.filter(|(b, a)| b.is_finite() && a.is_finite() && *b > 0.0 && *a >= *b);
    let passive_raw = if close_is_buy {
        match valid_bbo {
            Some((bid, ask)) => {
                // Step inside spread toward bid+tick while remaining strictly < ask when spread >= 2 ticks
                if ask - bid > eff_tick * 1.5 {
                    (bid + eff_tick).min((ask - eff_tick).max(eff_tick))
                } else {
                    bid.min((ask - eff_tick).max(eff_tick))
                }
            }
            None => mark_px,
        }
    } else {
        match valid_bbo {
            Some((bid, ask)) => {
                if ask - bid > eff_tick * 1.5 {
                    (ask - eff_tick).max(bid + eff_tick)
                } else {
                    ask.max(bid + eff_tick)
                }
            }
            None => mark_px,
        }
    };
    let (lo, hi) = if band_lo <= band_hi {
        (band_lo, band_hi)
    } else {
        (band_hi, band_lo)
    };
    let clamped = passive_raw.clamp(lo, hi);
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

/// Finds the newest matching resting passive reduce-only limit order OID on `asset.coin`.
pub fn find_newest_matching_passive_close_oid(
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
            let oid = extract_hl_oid(ord)?;
            let ts = ord.get("timestamp").and_then(Value::as_i64).unwrap_or(0);
            Some((ts, oid))
        })
        .max()
        .map(|(_, oid)| oid)
}

/// Collects non-trigger resting order cancels for `asset.coin`, excluding `keep_oid`.
///
/// Enforces `R14-M7`: returns an empty list and logs a warning whenever `metadata_complete == false`
/// (e.g. `openOrders` fallback lacking `isTrigger`/`orderType`), preventing accidental SL/TP deletion.
pub fn collect_coin_non_trigger_cancels(
    snap: &OpenOrdersSnapshot,
    asset: &ExchangeAsset,
    keep_oid: Option<u64>,
) -> Vec<CancelWire> {
    if !snap.metadata_complete {
        tracing::warn!(
            symbol = %asset.symbol,
            "Skipping non-trigger orphan cancels because openOrders metadata_complete=false (R14-M7)"
        );
        return Vec::new();
    }
    let mut cancels = Vec::new();
    for ord in &snap.orders {
        let c = ord.get("coin").and_then(Value::as_str).unwrap_or("").trim();
        if !c.eq_ignore_ascii_case(&asset.coin) || is_hl_trigger_order(ord) {
            continue;
        }
        if let Some(oid) = extract_hl_oid(ord) {
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

/// Checks whether a Hyperliquid `/exchange` cancel batch response succeeded or returned terminal/absent states.
pub fn cancel_batch_response_ok(resp: &Value) -> std::result::Result<(), String> {
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

/// Executes the unified, oracle-band-anchored, unlocked-IO reduce-only close ladder (`R14-M2`).
pub fn submit_reduce_only_close_ladder(
    http: &HyperliquidHttpClient,
    db: Option<&DatabaseManager>,
    pk: &str,
    cfg: &ConfigMap,
    req: &ReduceOnlyCloseLadderRequest<'_>,
) -> Result<ReduceOnlyCloseLadderResult> {
    let initial_qty = if req.live_szi.is_finite() {
        req.live_szi.abs()
    } else {
        0.0
    };
    let lot = req.asset.lot_step_size.max(1e-8);
    if initial_qty <= 1e-9 || initial_qty <= lot * 0.5 {
        return Ok(ReduceOnlyCloseLadderResult {
            fully_closed: true,
            initial_qty,
            filled_qty: 0.0,
            remaining_qty: 0.0,
            vwap_fill_price: None,
            total_fee_usdc: 0.0,
            last_fill_eoid: None,
            resting_passive_oid: None,
            resting_passive_px: None,
            kept_existing_passive: false,
            mutated_open_orders: false,
            attempts_submitted: 0,
            broke_early_at_band: false,
        });
    }

    let close_is_buy = req.live_szi < 0.0;
    let oracle_anchor = if req.asset.oracle_price.is_finite() && req.asset.oracle_price > 0.0 {
        req.asset.oracle_price
    } else if req.reference_mark_px.is_finite() && req.reference_mark_px > 0.0 {
        req.reference_mark_px
    } else {
        req.asset.mark_price.max(1e-8)
    };
    let tick = effective_hl_tick_size(oracle_anchor, req.asset.tick_size, req.asset.sz_decimals)
        .unwrap_or_else(|| req.asset.tick_size.max(1e-8));
    let eff_band = req
        .max_slippage_pct
        .filter(|v| v.is_finite() && *v > 0.0)
        .map(|v| v.min(HL_ORACLE_BAND_PCT))
        .unwrap_or(HL_ORACLE_BAND_PCT);
    let band_lo = (oracle_anchor * (1.0 - eff_band)).max(tick);
    let band_hi = (oracle_anchor * (1.0 + eff_band)).max(band_lo);

    let valid_bbo = req
        .l2_bbo
        .filter(|(b, a)| b.is_finite() && a.is_finite() && *b > 0.0 && *a >= *b);
    let book_crossable_in_band = match valid_bbo {
        Some((bid, ask)) => {
            if close_is_buy {
                ask <= band_hi + tick * 0.5
            } else {
                bid >= band_lo - tick * 0.5
            }
        }
        None => true,
    };

    let (passive_px, passive_wire_px) = compute_passive_close_px(
        req.asset,
        close_is_buy,
        valid_bbo,
        oracle_anchor,
        band_lo,
        band_hi,
    );

    let wire_sz_str =
        format_hl_wire_size_floor(initial_qty, req.asset.lot_step_size, req.asset.sz_decimals);
    let target_qty = wire_sz_str
        .parse::<f64>()
        .ok()
        .filter(|&q| q > 0.0)
        .unwrap_or(initial_qty);

    let mut fills = CloseFillAccumulator::new(initial_qty);
    let mut fully_closed = false;
    let mut kept_existing_passive = false;
    let mut resting_passive_oid: Option<String> = None;
    let mut resting_passive_px: Option<f64> = None;
    let mut mutated_open_orders = false;
    let mut attempts_submitted = 0usize;
    let mut broke_early_at_band = false;

    if !book_crossable_in_band {
        let keep_passive_oid = if req.allow_passive_fallback && wire_sz_str != "0" {
            req.existing_open_orders.and_then(|snap| {
                find_newest_matching_passive_close_oid(
                    &snap.orders,
                    req.asset,
                    close_is_buy,
                    target_qty,
                    passive_px,
                    &passive_wire_px,
                )
            })
        } else {
            None
        };

        if let Some(snap) = req.existing_open_orders {
            let cancels = collect_coin_non_trigger_cancels(snap, req.asset, keep_passive_oid);
            if !cancels.is_empty() {
                mutated_open_orders = true;
                let _ = http.cancel_orders_batch(cancels, pk, db, cfg);
            }
        }

        if let Some(keep_oid) = keep_passive_oid {
            kept_existing_passive = true;
            resting_passive_oid = Some(format!("hl-{keep_oid}"));
            resting_passive_px = Some(passive_px);
            tracing::info!(
                position_id = ?req.position_id,
                symbol = %req.asset.symbol,
                oid = keep_oid,
                passive_px,
                initial_qty,
                "Preserving existing resting passive reduceOnly close order on illiquid book"
            );
        }
    } else {
        if let Some(snap) = req.existing_open_orders {
            let cancels = collect_coin_non_trigger_cancels(snap, req.asset, None);
            if !cancels.is_empty() {
                mutated_open_orders = true;
                let _ = http.cancel_orders_batch(cancels, pk, db, cfg);
            }
        }

        let bbo_anchor = valid_bbo
            .map(|(bid, ask)| if close_is_buy { ask } else { bid })
            .filter(|&px| px > 0.0)
            .unwrap_or(oracle_anchor);
        let band_limit_px = if close_is_buy { band_hi } else { band_lo };
        let band_limit_wire = format_hl_wire_price(band_limit_px, tick, req.asset.sz_decimals);
        let mut rejected_wire_prices: HashSet<String> = HashSet::new();

        for (attempt, (ref_anchor, slip)) in [
            (bbo_anchor, CLOSE_LADDER_SLIPPAGE_STEPS[0]),
            (bbo_anchor, CLOSE_LADDER_SLIPPAGE_STEPS[1]),
            (bbo_anchor, CLOSE_LADDER_SLIPPAGE_STEPS[2]),
            (oracle_anchor, eff_band),
        ]
        .into_iter()
        .enumerate()
        {
            if fills.rem_qty <= lot * 0.5 || fills.rem_qty <= 1e-9 {
                fully_closed = true;
                break;
            }
            let wire_sz = format_hl_wire_size_floor(
                fills.rem_qty,
                req.asset.lot_step_size,
                req.asset.sz_decimals,
            );
            if wire_sz == "0" {
                fully_closed = fills.fill_qty_sum > 0.0;
                break;
            }
            let eff_slip = slip.min(eff_band);
            let raw_limit_px = if close_is_buy {
                ref_anchor * (1.0 + eff_slip)
            } else {
                (ref_anchor * (1.0 - eff_slip)).max(tick)
            };
            let limit_px = raw_limit_px.clamp(band_lo, band_hi);
            let wire_px = format_hl_wire_price(limit_px, tick, req.asset.sz_decimals);
            if rejected_wire_prices.contains(&wire_px) {
                continue;
            }
            let at_band_limit = if close_is_buy {
                raw_limit_px >= band_hi - tick * 0.5 || wire_px == band_limit_wire
            } else {
                raw_limit_px <= band_lo + tick * 0.5 || wire_px == band_limit_wire
            };
            let coid = format!(
                "{}-{}-{attempt}-{}",
                req.cloid_prefix,
                req.position_id.unwrap_or(0),
                now_ms()
            );
            let cloid_hex = derive_hl_cloid_hex(&coid);
            let wire = OrderWire {
                a: req.asset.asset_index,
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
                c: Some(cloid_hex.clone()),
            };

            mutated_open_orders = true;
            attempts_submitted += 1;
            match http.submit_order_wire(
                wire,
                fills.rem_qty,
                limit_px,
                req.taker_fee_bps,
                pk,
                db,
                cfg,
            ) {
                Ok(ack) => {
                    let accepted = ack.accepted;
                    let raw_err = ack.raw_error.clone();
                    if fills.record_fill(ack, limit_px, lot) {
                        fully_closed = true;
                        break;
                    }
                    if !accepted {
                        rejected_wire_prices.insert(wire_px);
                        if is_ioc_no_match_error(raw_err.as_deref()) && at_band_limit {
                            broke_early_at_band = true;
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        position_id = ?req.position_id,
                        symbol = %req.asset.symbol,
                        attempt,
                        cloid = %cloid_hex,
                        error = %e,
                        "Hyperliquid reduceOnly IOC close submission error; probing cloid/state"
                    );
                    let wallet = cfg.get_str("wallet_address", "");
                    let trimmed_wallet = wallet.trim();
                    if !trimmed_wallet.is_empty() {
                        match http.fetch_order_status_by_cloid(trimmed_wallet, &cloid_hex, cfg) {
                            Ok(Some(st)) => {
                                if st.filled_sz > 1e-9 {
                                    let fill_px = if st.avg_px > 0.0 {
                                        st.avg_px
                                    } else if st.limit_px > 0.0 {
                                        st.limit_px
                                    } else {
                                        limit_px
                                    };
                                    let est_fee = round_dp(
                                        fill_px * st.filled_sz * (req.taker_fee_bps / 10_000.0),
                                        4,
                                    );
                                    let eoid = (st.oid > 0).then(|| format!("hl-{}", st.oid));
                                    let ack = ExchangeOrderAck {
                                        accepted: true,
                                        status: "FILLED".to_string(),
                                        exchange_order_id: eoid,
                                        filled_quantity: st.filled_sz,
                                        avg_fill_price: fill_px,
                                        fee_usdc: est_fee,
                                        raw_error: None,
                                    };
                                    if fills.record_fill(ack, limit_px, lot) {
                                        fully_closed = true;
                                        break;
                                    }
                                }
                            }
                            Ok(None) | Err(_) => {
                                if let Ok(ch_state) =
                                    http.fetch_clearinghouse_state(trimmed_wallet, cfg)
                                {
                                    let mut live_szi = 0.0_f64;
                                    if let Some(arr) =
                                        ch_state.get("assetPositions").and_then(Value::as_array)
                                    {
                                        for item in arr {
                                            let p = item.get("position").unwrap_or(item);
                                            let coin = p
                                                .get("coin")
                                                .and_then(Value::as_str)
                                                .unwrap_or("")
                                                .trim();
                                            if coin.eq_ignore_ascii_case(&req.asset.coin) {
                                                live_szi = parse_hl_f64(p.get("szi"));
                                                break;
                                            }
                                        }
                                    }
                                    let base_pos_qty = req
                                        .pre_close_position_abs_qty
                                        .filter(|q| q.is_finite() && *q > 0.0)
                                        .map(|q| q.max(initial_qty))
                                        .unwrap_or(initial_qty);
                                    let expected_pre_rung_pos_qty =
                                        (base_pos_qty - fills.fill_qty_sum).max(0.0);
                                    let same_dir = live_szi.abs() > 1e-9
                                        && (live_szi > 0.0) == (req.live_szi > 0.0);
                                    let filled_delta = if !same_dir {
                                        fills.rem_qty
                                    } else if live_szi.abs() < expected_pre_rung_pos_qty - lot * 0.5
                                    {
                                        (expected_pre_rung_pos_qty - live_szi.abs())
                                            .clamp(0.0, fills.rem_qty)
                                    } else {
                                        0.0
                                    };
                                    if filled_delta > 1e-9 {
                                        let est_fee = round_dp(
                                            limit_px
                                                * filled_delta
                                                * (req.taker_fee_bps / 10_000.0),
                                            4,
                                        );
                                        let ack = ExchangeOrderAck {
                                            accepted: true,
                                            status: "FILLED".to_string(),
                                            exchange_order_id: None,
                                            filled_quantity: filled_delta,
                                            avg_fill_price: limit_px,
                                            fee_usdc: est_fee,
                                            raw_error: None,
                                        };
                                        if fills.record_fill(ack, limit_px, lot) {
                                            fully_closed = true;
                                            break;
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

    if !fully_closed
        && req.allow_passive_fallback
        && !kept_existing_passive
        && fills.rem_qty > lot * 0.5
        && fills.rem_qty > 1e-9
    {
        let passive_sz = format_hl_wire_size_floor(
            fills.rem_qty,
            req.asset.lot_step_size,
            req.asset.sz_decimals,
        );
        if passive_sz != "0" {
            let coid = format!(
                "{}-passive-{}-{}",
                req.cloid_prefix,
                req.position_id.unwrap_or(0),
                now_ms()
            );
            let passive_wire = OrderWire {
                a: req.asset.asset_index,
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
                c: Some(derive_hl_cloid_hex(&coid)),
            };
            mutated_open_orders = true;
            attempts_submitted += 1;
            if let Ok(ack) = http.submit_order_wire(
                passive_wire,
                fills.rem_qty,
                passive_px,
                req.taker_fee_bps,
                pk,
                db,
                cfg,
            ) {
                let accepted = ack.accepted;
                let status = ack.status.clone();
                let eoid = ack.exchange_order_id.clone();
                if fills.record_fill(ack, passive_px, lot) {
                    fully_closed = true;
                } else if accepted && status != "FILLED" {
                    resting_passive_oid = eoid;
                    resting_passive_px = Some(passive_px);
                }
            }
        }
    }

    Ok(ReduceOnlyCloseLadderResult {
        fully_closed,
        initial_qty,
        filled_qty: round_dp(fills.fill_qty_sum, 8),
        remaining_qty: round_dp(fills.rem_qty, 8),
        vwap_fill_price: fills.vwap_fill_price(),
        total_fee_usdc: round_dp(fills.live_fee_usdc, 4),
        last_fill_eoid: fills.close_eoid_opt,
        resting_passive_oid,
        resting_passive_px,
        kept_existing_passive,
        mutated_open_orders,
        attempts_submitted,
        broke_early_at_band,
    })
}

/// Active non-blocking maker-first reduce-only close tracked across 1s `PriorityMonitor` ticks (`P1-Strat`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingPassiveClose {
    pub position_id: i64,
    pub asset_id: i64,
    pub oid: Option<u64>,
    pub eoid: Option<String>,
    pub cloid: String,
    pub limit_px: f64,
    pub initial_qty: f64,
    pub placed_at_ms: i64,
    pub exit_reason: String,
    pub is_tp: bool,
}

/// Shared thread-safe tracker for non-blocking `PassiveFirst` reduce-only closes (`P1-Strat`, `R14-L6`).
#[derive(Debug, Clone, Default)]
pub struct PassiveCloseTracker {
    entries: Arc<Mutex<HashMap<i64, PendingPassiveClose>>>,
}

impl PassiveCloseTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, position_id: i64) -> Option<PendingPassiveClose> {
        self.entries.lock().get(&position_id).cloned()
    }

    pub fn insert(&self, entry: PendingPassiveClose) {
        self.entries.lock().insert(entry.position_id, entry);
    }

    pub fn remove(&self, position_id: i64) -> Option<PendingPassiveClose> {
        self.entries.lock().remove(&position_id)
    }

    pub fn update_initial_qty(&self, position_id: i64, new_qty: f64) {
        if let Some(entry) = self.entries.lock().get_mut(&position_id) {
            entry.initial_qty = new_qty;
        }
    }

    pub fn prune_to_active(&self, active_open_ids: &HashSet<i64>) {
        self.entries
            .lock()
            .retain(|pid, _| active_open_ids.contains(pid));
    }

    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }
}

/// Outcome of placing a strictly passive reduce-only maker close order (`P1-Strat`).
#[derive(Debug, Clone)]
pub enum PassiveClosePlacement {
    Filled {
        fill_px: f64,
        filled_qty: f64,
        fee_usdc: f64,
        eoid: Option<String>,
    },
    Resting {
        oid: Option<u64>,
        eoid: Option<String>,
        cloid: String,
        limit_px: f64,
    },
    Rejected(String),
}

/// Places ONE strictly passive reduce-only `Alo` close order (1-tick passive fallback on crossing reject,
/// then passive `Gtc` inside oracle band), never sleeping inside the 1s loop (`P1-Strat`).
#[allow(clippy::too_many_arguments)]
pub fn place_passive_reduce_only_close_order(
    http: &HyperliquidHttpClient,
    db: Option<&DatabaseManager>,
    pk: &str,
    cfg: &ConfigMap,
    asset: &ExchangeAsset,
    position_id: i64,
    live_szi: f64,
    reference_mark_px: f64,
    l2_bbo: Option<(f64, f64)>,
    maker_fee_bps: f64,
    tp_floor_or_cap_px: Option<f64>,
    cloid_prefix: &str,
) -> Result<PassiveClosePlacement> {
    let close_is_buy = live_szi < 0.0;
    let qty = live_szi.abs();
    let oracle_anchor = if asset.oracle_price.is_finite() && asset.oracle_price > 0.0 {
        asset.oracle_price
    } else if reference_mark_px.is_finite() && reference_mark_px > 0.0 {
        reference_mark_px
    } else {
        asset.mark_price.max(1e-8)
    };
    let eff_tick = effective_hl_tick_size(oracle_anchor, asset.tick_size, asset.sz_decimals)
        .unwrap_or_else(|| asset.tick_size.max(1e-8));
    let band_lo = (oracle_anchor * (1.0 - HL_ORACLE_BAND_PCT)).max(eff_tick);
    let band_hi = (oracle_anchor * (1.0 + HL_ORACLE_BAND_PCT)).max(band_lo);

    let (mut passive_px, _) =
        compute_passive_close_px(asset, close_is_buy, l2_bbo, oracle_anchor, band_lo, band_hi);
    if let Some(tp_target) = tp_floor_or_cap_px.filter(|v| v.is_finite() && *v > 0.0) {
        if close_is_buy {
            // Closing SHORT TP: never buy back worse (higher) than tp_target
            passive_px = passive_px.min(tp_target).clamp(band_lo, band_hi);
        } else {
            // Closing LONG TP: never sell worse (lower) than tp_target
            passive_px = passive_px.max(tp_target).clamp(band_lo, band_hi);
        }
    }

    let valid_bbo = l2_bbo.filter(|(b, a)| b.is_finite() && a.is_finite() && *b > 0.0 && *a >= *b);
    if let Some((bid, ask)) = valid_bbo {
        if close_is_buy && passive_px >= ask - eff_tick * 1e-6 {
            passive_px = ((ask - eff_tick).max(eff_tick)).clamp(band_lo, band_hi);
        } else if !close_is_buy && passive_px <= bid + eff_tick * 1e-6 {
            passive_px = (bid + eff_tick).clamp(band_lo, band_hi);
        }
    }

    let coid = format!("{cloid_prefix}-p0-{position_id}-{}", now_ms());
    let expected_cloid = derive_hl_cloid_hex(&coid);
    let wire = build_limit_order_wire(
        asset.asset_index,
        close_is_buy,
        passive_px,
        qty,
        eff_tick,
        asset.lot_step_size,
        asset.sz_decimals,
        true,
        "Alo",
        Some(&coid),
    );
    if wire.s == "0" {
        return Ok(PassiveClosePlacement::Rejected(
            "ZERO_WIRE_SIZE".to_string(),
        ));
    }

    match http.submit_order_wire(wire, qty, passive_px, maker_fee_bps, pk, db, cfg) {
        Ok(ack) if ack.accepted => {
            if ack.status == "FILLED" && ack.filled_quantity > 0.0 {
                return Ok(PassiveClosePlacement::Filled {
                    fill_px: ack.avg_fill_price,
                    filled_qty: ack.filled_quantity,
                    fee_usdc: ack.fee_usdc,
                    eoid: ack.exchange_order_id,
                });
            }
            let oid = aegis_core::parse_hl_oid(ack.exchange_order_id.as_deref());
            Ok(PassiveClosePlacement::Resting {
                oid,
                eoid: ack.exchange_order_id,
                cloid: expected_cloid,
                limit_px: passive_px,
            })
        }
        Ok(ack) if is_post_only_crossing_rejection(ack.raw_error.as_deref()) => {
            let fallback_px = if close_is_buy {
                (passive_px - eff_tick)
                    .max(eff_tick)
                    .clamp(band_lo, band_hi)
            } else {
                (passive_px + eff_tick).clamp(band_lo, band_hi)
            };
            let fb_coid = format!("{cloid_prefix}-p1-{position_id}-{}", now_ms());
            let fb_cloid = derive_hl_cloid_hex(&fb_coid);
            let fb_wire = build_limit_order_wire(
                asset.asset_index,
                close_is_buy,
                fallback_px,
                qty,
                eff_tick,
                asset.lot_step_size,
                asset.sz_decimals,
                true,
                "Gtc",
                Some(&fb_coid),
            );
            match http.submit_order_wire(fb_wire, qty, fallback_px, maker_fee_bps, pk, db, cfg) {
                Ok(fb_ack) if fb_ack.accepted => {
                    if fb_ack.status == "FILLED" && fb_ack.filled_quantity > 0.0 {
                        return Ok(PassiveClosePlacement::Filled {
                            fill_px: fb_ack.avg_fill_price,
                            filled_qty: fb_ack.filled_quantity,
                            fee_usdc: fb_ack.fee_usdc,
                            eoid: fb_ack.exchange_order_id,
                        });
                    }
                    let oid = aegis_core::parse_hl_oid(fb_ack.exchange_order_id.as_deref());
                    Ok(PassiveClosePlacement::Resting {
                        oid,
                        eoid: fb_ack.exchange_order_id,
                        cloid: fb_cloid,
                        limit_px: fallback_px,
                    })
                }
                Ok(fb_ack) => Ok(PassiveClosePlacement::Rejected(
                    fb_ack.raw_error.unwrap_or(fb_ack.status),
                )),
                Err(e) => Err(e),
            }
        }
        Ok(ack) => Ok(PassiveClosePlacement::Rejected(
            ack.raw_error.unwrap_or(ack.status),
        )),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_test_support::mock_http::{MockHttpResponse, MockHttpServer};
    use serde_json::json;

    fn sample_sol_asset() -> ExchangeAsset {
        ExchangeAsset {
            id: 1,
            symbol: "SOL-USDC".to_string(),
            coin: "SOL".to_string(),
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
            prev_day_price: 99.0,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: true,
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }

    #[test]
    fn test_r14_m2_unified_close_ladder_dedupes_wire_prices_breaks_at_band_and_preserves_passive() {
        let asset = sample_sol_asset();
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";

        // 1. Wide illiquid book (bid 95.0 < band_lo 96.20, ask 105.0 > band_hi 103.80):
        //    skips un-crossable IOC attempts and rests ONE passive GTC clamped to band_hi 103.80!
        let srv = MockHttpServer::start(|req| {
            let tif = req
                .json_body
                .pointer("/action/orders/0/t/limit/tif")
                .and_then(Value::as_str)
                .unwrap_or("");
            if tif == "Ioc" {
                MockHttpResponse::ok_json(&json!({
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": { "statuses": [{ "error": "Order could not immediately match" }] }
                    }
                }))
            } else {
                MockHttpResponse::ok_json(&json!({
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": { "statuses": [{ "resting": { "oid": 55501u64 } }] }
                    }
                }))
            }
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");

        let client = HyperliquidHttpClient::new(5);
        let res = submit_reduce_only_close_ladder(
            &client,
            None,
            pk,
            &cfg,
            &ReduceOnlyCloseLadderRequest {
                asset: &asset,
                position_id: Some(77),
                live_szi: 5.0, // closing LONG -> SELL
                pre_close_position_abs_qty: None,
                reference_mark_px: 100.0,
                l2_bbo: Some((95.0, 105.0)), // bid 95.0 < band_lo 96.20 (un-crossable!)
                taker_fee_bps: 4.5,
                cloid_prefix: "v2-close",
                max_slippage_pct: None,
                allow_passive_fallback: true,
                existing_open_orders: None,
            },
        )
        .unwrap();

        assert!(!res.fully_closed);
        // Since bid 95.0 < band_lo 96.20, book_crossable_in_band is false -> 0 IOC attempts + 1 passive GTC
        assert_eq!(res.attempts_submitted, 1);
        assert_eq!(res.resting_passive_oid.as_deref(), Some("hl-55501"));
        assert!((res.resting_passive_px.unwrap() - 103.80).abs() < 1e-6);

        // 2. When BBO IS barely inside band (bid = 96.20, ask = 100.50), rungs 0..=3 all clamp to 96.20:
        //    only 1 unique IOC price is submitted, breaks early at band, then 1 passive GTC rests.
        srv.clear_requests();
        let res2 = submit_reduce_only_close_ladder(
            &client,
            None,
            pk,
            &cfg,
            &ReduceOnlyCloseLadderRequest {
                asset: &asset,
                position_id: Some(78),
                live_szi: 5.0,
                pre_close_position_abs_qty: None,
                reference_mark_px: 100.0,
                l2_bbo: Some((96.20, 100.50)),
                taker_fee_bps: 4.5,
                cloid_prefix: "v2-close",
                max_slippage_pct: None,
                allow_passive_fallback: true,
                existing_open_orders: None,
            },
        )
        .unwrap();
        assert!(!res2.fully_closed);
        assert!(res2.broke_early_at_band);
        assert_eq!(res2.attempts_submitted, 2); // 1 deduped IOC + 1 passive GTC

        // 3. Preserves existing matching passive close order instead of churn-canceling it
        srv.clear_requests();
        let (passive_px, passive_wire_px) =
            compute_passive_close_px(&asset, false, Some((95.0, 105.0)), 100.0, 96.20, 103.80);
        assert!((passive_px - 103.80).abs() < 1e-6);
        let existing_snap = OpenOrdersSnapshot {
            metadata_complete: true,
            orders: vec![json!({
                "coin": "SOL",
                "oid": 88801u64,
                "side": "A",
                "limitPx": passive_wire_px,
                "sz": "5.0",
                "reduceOnly": true,
                "orderType": "Limit",
                "isTrigger": false,
                "timestamp": 1000i64
            })],
        };
        let res3 = submit_reduce_only_close_ladder(
            &client,
            None,
            pk,
            &cfg,
            &ReduceOnlyCloseLadderRequest {
                asset: &asset,
                position_id: Some(79),
                live_szi: 5.0,
                pre_close_position_abs_qty: None,
                reference_mark_px: 100.0,
                l2_bbo: Some((95.0, 105.0)),
                taker_fee_bps: 4.5,
                cloid_prefix: "v2-close",
                max_slippage_pct: None,
                allow_passive_fallback: true,
                existing_open_orders: Some(&existing_snap),
            },
        )
        .unwrap();
        assert!(res3.kept_existing_passive);
        assert_eq!(res3.attempts_submitted, 0);
        assert_eq!(srv.request_count(), 0);
        assert_eq!(res3.resting_passive_oid.as_deref(), Some("hl-88801"));
    }

    #[test]
    fn test_r14_m7_collect_cancels_skips_when_metadata_incomplete() {
        let asset = sample_sol_asset();
        let snap_incomplete = OpenOrdersSnapshot {
            metadata_complete: false,
            orders: vec![json!({
                "coin": "SOL",
                "oid": 99001u64,
                "limitPx": "95.0",
                "sz": "5.0"
            })],
        };
        assert!(collect_coin_non_trigger_cancels(&snap_incomplete, &asset, None).is_empty());
    }

    #[test]
    fn test_p1_strat_passive_close_tracker_prune_and_maker_placement() {
        let tracker = PassiveCloseTracker::new();
        tracker.insert(PendingPassiveClose {
            position_id: 10,
            asset_id: 1,
            oid: Some(111),
            eoid: Some("hl-111".to_string()),
            cloid: "0x111".to_string(),
            limit_px: 100.0,
            initial_qty: 2.0,
            placed_at_ms: 1000,
            exit_reason: "TAKE_PROFIT_LONG".to_string(),
            is_tp: true,
        });
        tracker.insert(PendingPassiveClose {
            position_id: 20,
            asset_id: 1,
            oid: Some(222),
            eoid: Some("hl-222".to_string()),
            cloid: "0x222".to_string(),
            limit_px: 99.0,
            initial_qty: 2.0,
            placed_at_ms: 1000,
            exit_reason: "STAGNATION_TIME_STOP_LONG".to_string(),
            is_tp: false,
        });
        let mut active = HashSet::new();
        active.insert(10);
        tracker.prune_to_active(&active);
        assert_eq!(tracker.len(), 1);
        assert!(tracker.get(10).is_some());
        assert!(tracker.get(20).is_none());
    }

    #[test]
    fn test_ambiguous_post_ioc_rung_probes_cloid_and_credits_partial_fill() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let asset = sample_sol_asset();
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let captured_cloid: Arc<parking_lot::Mutex<Option<String>>> =
            Arc::new(parking_lot::Mutex::new(None));
        let exchange_calls = Arc::new(AtomicUsize::new(0));
        let cloid_queries = Arc::new(AtomicUsize::new(0));

        let cap_clone = Arc::clone(&captured_cloid);
        let ex_clone = Arc::clone(&exchange_calls);
        let cq_clone = Arc::clone(&cloid_queries);

        let srv = MockHttpServer::start(move |req| {
            if req.path == "/exchange" {
                let n = ex_clone.fetch_add(1, Ordering::SeqCst);
                let sz = req
                    .json_body
                    .pointer("/action/orders/0/s")
                    .and_then(Value::as_str)
                    .unwrap_or("0")
                    .to_string();
                let cloid = req
                    .json_body
                    .pointer("/action/orders/0/c")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                if n == 0 {
                    *cap_clone.lock() = Some(cloid);
                    return MockHttpResponse::text(500, "text/plain", "gateway timeout");
                }
                // Rung 1 must request ONLY the remaining 0.6 (after probing 0.4 filled on Rung 0!)
                return MockHttpResponse::ok_json(&json!({
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": {
                            "statuses": [{
                                "filled": {
                                    "totalSz": sz,
                                    "avgPx": "99.7",
                                    "oid": 7002
                                }
                            }]
                        }
                    }
                }));
            }
            let typ = req
                .json_body
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("");
            if typ == "orderStatus" {
                cq_clone.fetch_add(1, Ordering::SeqCst);
                let oid_q = req
                    .json_body
                    .get("oid")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if cap_clone.lock().as_deref() == Some(oid_q) {
                    return MockHttpResponse::ok_json(&json!({
                        "status": "order",
                        "order": {
                            "status": "canceled",
                            "order": {
                                "coin": "SOL",
                                "oid": 7001u64,
                                "limitPx": "99.9",
                                "avgPx": "99.9",
                                "origSz": "1.0",
                                "sz": "0.6",
                                "cloid": oid_q
                            }
                        }
                    }));
                }
            }
            MockHttpResponse::ok_json(&json!({ "status": "unknownOid" }))
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");
        cfg.insert(
            "wallet_address",
            "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
        );

        let http = HyperliquidHttpClient::new(5);
        let res = submit_reduce_only_close_ladder(
            &http,
            None,
            pk,
            &cfg,
            &ReduceOnlyCloseLadderRequest {
                asset: &asset,
                position_id: Some(7),
                live_szi: 1.0,
                pre_close_position_abs_qty: None,
                reference_mark_px: 100.0,
                l2_bbo: Some((99.95, 100.05)),
                taker_fee_bps: 4.5,
                cloid_prefix: "v2-close",
                max_slippage_pct: None,
                allow_passive_fallback: false,
                existing_open_orders: None,
            },
        )
        .unwrap();

        assert!(res.fully_closed);
        assert_eq!(cloid_queries.load(Ordering::SeqCst), 1);
        assert_eq!(exchange_calls.load(Ordering::SeqCst), 2);
        assert!((res.filled_qty - 1.0).abs() < 1e-6);
        assert!((res.remaining_qty - 0.0).abs() < 1e-6);
        // VWAP = (0.4 * 99.9 + 0.6 * 99.7) / 1.0 = 99.78
        assert!((res.vwap_fill_price.unwrap() - 99.78).abs() < 1e-4);
    }

    #[test]
    fn test_partial_reduce_double_fault_uses_pre_close_position_abs_qty() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let asset = sample_sol_asset();
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let exchange_calls = Arc::new(AtomicUsize::new(0));
        let ch_queries = Arc::new(AtomicUsize::new(0));
        let last_sz = Arc::new(parking_lot::Mutex::new(String::new()));

        let ex_clone = Arc::clone(&exchange_calls);
        let ch_clone = Arc::clone(&ch_queries);
        let sz_clone = Arc::clone(&last_sz);

        // Pre-reduce open position is 10.0 SOL; ACTION_REDUCE targets 4.0 SOL.
        // Rung 0 fills 1.5 SOL (exchange drops 10.0 -> 8.5) then both /exchange and orderStatus fail.
        // clearinghouseState sees szi=8.5, so Rung 1 must submit ONLY the remaining 2.5 SOL.
        let srv = MockHttpServer::start(move |req| {
            if req.path == "/exchange" {
                let n = ex_clone.fetch_add(1, Ordering::SeqCst);
                let sz = req
                    .json_body
                    .pointer("/action/orders/0/s")
                    .and_then(Value::as_str)
                    .unwrap_or("0")
                    .to_string();
                *sz_clone.lock() = sz.clone();
                if n == 0 {
                    return MockHttpResponse::text(500, "text/plain", "gateway timeout");
                }
                return MockHttpResponse::ok_json(&json!({
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": {
                            "statuses": [{
                                "filled": {
                                    "totalSz": sz,
                                    "avgPx": "99.8",
                                    "oid": 8002
                                }
                            }]
                        }
                    }
                }));
            }
            let typ = req
                .json_body
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("");
            if typ == "clearinghouseState" {
                ch_clone.fetch_add(1, Ordering::SeqCst);
                return MockHttpResponse::ok_json(&json!({
                    "assetPositions": [{
                        "position": {
                            "coin": "SOL",
                            "szi": "8.5"
                        }
                    }]
                }));
            }
            MockHttpResponse::ok_json(&json!({ "status": "unknownOid" }))
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");
        cfg.insert(
            "wallet_address",
            "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
        );

        let http = HyperliquidHttpClient::new(5);
        let res = submit_reduce_only_close_ladder(
            &http,
            None,
            pk,
            &cfg,
            &ReduceOnlyCloseLadderRequest {
                asset: &asset,
                position_id: Some(88),
                live_szi: 4.0,
                pre_close_position_abs_qty: Some(10.0),
                reference_mark_px: 100.0,
                l2_bbo: Some((99.95, 100.05)),
                taker_fee_bps: 4.5,
                cloid_prefix: "v2-reduce",
                max_slippage_pct: None,
                allow_passive_fallback: false,
                existing_open_orders: None,
            },
        )
        .unwrap();

        assert!(res.fully_closed);
        assert_eq!(ch_queries.load(Ordering::SeqCst), 1);
        assert_eq!(exchange_calls.load(Ordering::SeqCst), 2);
        assert_eq!(&*last_sz.lock(), "2.5");
        assert!((res.filled_qty - 4.0).abs() < 1e-6);
        assert!((res.remaining_qty - 0.0).abs() < 1e-6);
    }
}
