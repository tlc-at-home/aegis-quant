//! 1-Second Maker Limit Chaser & Order-to-Open Capital Lifecycle (`aegis-execution::order_chaser`).
//!
//! Enforces the Rev 3.1 strict Order-to-Open Capital Lifecycle:
//! - Places new entries as `WORKING` (`WORKING_ALO`), reserving `reserved_margin_usdc` without spending cash into `committed_notional_usdc`.
//! - Chases top-of-book at 1s frequency (`record_working_order_chase`), escalating to `CROSSING_IOC` after `max_alo_chase_ticks`.
//! - Upon fill confirmation, calls `promote_working_to_open` (moving funds from `reserved_margin_usdc` into `committed_notional_usdc`),
//!   places an exchange-side protective stop order, and enqueues a rich HTML Telegram alert.

use crate::http_client::HyperliquidHttpClient;
use crate::signing::{
    build_limit_order_wire, build_protective_stop_order_wire, build_take_profit_order_wire,
    derive_hl_cloid_hex, format_hl_wire_price, format_hl_wire_size, format_hl_wire_size_floor,
    snap_to_lot, snap_to_tick, CancelWire, HL_ORACLE_BAND_PCT,
};
use aegis_alerts::{AlertEngine, OrderFilledOpenAlert, WorkingOrderPlacedAlert};
use aegis_core::{
    now_ms, round_dp, AegisError, ConfigMap, ExchangeAsset, OrderFsmState, OrderSide,
    OrderedAssetView, Result,
};
use aegis_db::{DatabaseManager, WorkingOrderInput};
use aegis_risk::{
    compute_atr_from_bars, compute_initial_protective_stop, compute_initial_take_profit,
    RiskLimitsConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy)]
struct TriggerOrderInfo {
    oid: u64,
    is_tp: bool,
    trigger_px: f64,
    qty: f64,
}

pub(crate) fn parse_hl_oid(raw: Option<&str>) -> Option<u64> {
    raw.and_then(|s| s.trim().strip_prefix("hl-").or_else(|| Some(s.trim())))
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&v| v > 0)
}

pub(crate) fn extract_order_oid(ord: &Value) -> Option<u64> {
    ord.get("oid")
        .and_then(Value::as_u64)
        .or_else(|| {
            ord.get("oid")
                .and_then(Value::as_str)
                .and_then(|s| s.trim().parse::<u64>().ok())
        })
        .filter(|&v| v > 0)
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
}

impl OrderChaser {
    pub fn new(db: DatabaseManager, http: HyperliquidHttpClient, alerts: AlertEngine) -> Self {
        Self { db, http, alerts }
    }

    /// Places a new `WORKING` (`WORKING_ALO`) order, reserving `reserved_margin_usdc`
    /// without deducting `committed_notional_usdc` yet, and notifies Telegram.
    pub fn place_working_order(&self, req: &NewWorkingOrderRequest) -> Result<i64> {
        if self.db.is_asset_in_cooldown(req.asset.id)? {
            return Err(AegisError::Validation(format!(
                "Asset '{}' is currently in active cooldown; blocking new WORKING order",
                req.asset.symbol
            )));
        }
        if let Some(existing) = self.db.get_active_position_for_asset(req.asset.id)? {
            return Err(AegisError::Validation(format!(
                "Asset '{}' already has active position #{} (status={})",
                req.asset.symbol, existing.id, existing.status
            )));
        }

        let cfg = self.db.load_config_or_default();
        let max_open_positions = cfg.get_usize("max_open_positions", 6).max(1);
        let min_entry_interval_sec = cfg.get_i64("min_entry_interval_sec", 150).max(0);
        let cap_epoch_ms = if cfg.execution_mode().is_live_or_testnet() {
            cfg.get_i64("risk_capital_cap_updated_at_ms", 0)
        } else {
            0
        };

        let active_held_cnt = self
            .db
            .get_assets_held()
            .map(|v| {
                v.iter()
                    .filter(|h| crate::engine::is_active_cap_holding(h, cap_epoch_ms))
                    .count()
            })
            .unwrap_or(0);
        let active_ord_cnt = self
            .db
            .get_assets_ordered()
            .map(|v| {
                v.iter()
                    .filter(|o| crate::engine::is_active_cap_order(o, cap_epoch_ms))
                    .count()
            })
            .unwrap_or(0);
        let total_active = active_held_cnt + active_ord_cnt;
        if total_active >= max_open_positions {
            return Err(AegisError::Validation(format!(
                "MAX_OPEN_POSITIONS_REACHED: active={total_active} >= max_open_positions={max_open_positions}"
            )));
        }

        if min_entry_interval_sec > 0 && !req.bypass_entry_interval {
            if let Ok(Some(last_entry_ms)) = self.db.last_position_entry_timestamp_ms() {
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
        if ref_mid <= 0.0 {
            return Err(AegisError::Validation(format!(
                "Asset '{}' has invalid reference price {ref_mid}",
                req.asset.symbol
            )));
        }

        let tick = req.asset.tick_size.max(1e-8);
        let limit_price = if req.side.is_buy() {
            snap_to_tick((ref_mid - tick).max(tick), tick)
        } else {
            snap_to_tick(ref_mid + tick, tick)
        };
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

        let default_stop_pct = cfg
            .get_f64("protective_stop_default_pct", 0.04)
            .clamp(0.005, 0.25);
        let stop_px = if req.protective_stop_px > 0.0 {
            snap_to_tick(req.protective_stop_px, tick)
        } else if req.side.is_buy() {
            snap_to_tick(limit_price * (1.0 - default_stop_pct), tick)
        } else {
            snap_to_tick(limit_price * (1.0 + default_stop_pct), tick)
        };

        let coid = format!(
            "v2-{}-{}-{}",
            req.asset.coin.to_ascii_lowercase(),
            req.side.as_str().to_ascii_lowercase(),
            now_ms()
        );

        let mut exchange_oid: Option<String> = None;
        let mut live_filled_immediately: Option<(f64, f64, f64)> = None;

        let is_live_exchange = crate::engine::is_live_exchange_active(&self.db, &cfg);
        let max_entry_spread_bps = cfg.get_f64("max_entry_spread_bps", 45.0).max(1.0);

        if is_live_exchange {
            if let Ok(Some((bid, ask))) = self.http.fetch_l2_book(&req.asset.coin, &cfg) {
                let mid_bbo = (bid + ask) * 0.5;
                if mid_bbo > 0.0 && ask >= bid {
                    let spread_bps = ((ask - bid) / mid_bbo) * 10_000.0;
                    if spread_bps > max_entry_spread_bps {
                        return Err(AegisError::Validation(format!(
                            "ENTRY_SPREAD_TOO_WIDE: {} spread={spread_bps:.2}bps > max_entry_spread_bps={max_entry_spread_bps:.2}bps",
                            req.asset.symbol
                        )));
                    }
                }
            }
            let pk = cfg.get_str("private_key", "");
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
            let maker_fee_bps = cfg.get_f64("maker_fee_bps", 1.5);
            let ack = self.http.submit_order_wire(
                wire,
                qty,
                limit_price,
                maker_fee_bps,
                &pk,
                Some(&self.db),
                &cfg,
            )?;
            if !ack.accepted {
                return Err(AegisError::Execution(format!(
                    "Hyperliquid rejected WORKING ALO order on {}: {}",
                    req.asset.symbol,
                    ack.raw_error.unwrap_or_else(|| "REJECTED".to_string())
                )));
            }
            exchange_oid = ack.exchange_order_id.clone();
            if ack.status == "FILLED" && ack.filled_quantity > 0.0 {
                live_filled_immediately =
                    Some((ack.avg_fill_price, ack.filled_quantity, ack.fee_usdc));
            }
        }

        let reserved_usdc = if req.reserved_margin_usdc > 0.0 {
            round_dp(req.reserved_margin_usdc, 4)
        } else {
            round_dp(limit_price * qty, 4)
        };

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
            exchange_order_id: exchange_oid.clone(),
            reason: req.reason.clone(),
        })?;

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
            let _ = self.alerts.notify_working_order_placed(&alert);
        }

        // If the exchange filled the order immediately upon placement, promote it WORKING -> OPEN now
        if let Some((fill_px, fill_qty, fee_usdc)) = live_filled_immediately {
            self.finalize_fill_to_open(
                pos_id,
                &req.asset,
                req.side,
                fill_px,
                fill_qty,
                fee_usdc,
                stop_px,
                exchange_oid.as_deref(),
                "HL_IMMEDIATE_FILL_ON_SUBMIT",
                &cfg,
            )?;
        }

        Ok(pos_id)
    }

    /// Advances a single `WORKING` order from `v_assets_ordered` during the 1-second surveillance loop.
    pub fn advance_working_order(&self, ordered: &OrderedAssetView) -> Result<ChaseStepOutcome> {
        let cfg = self.db.load_config_or_default();
        let max_chases = cfg.get_usize("max_alo_chase_ticks", 5).max(1) as u32;
        let chase_threshold_bps = cfg.get_f64("alo_chase_threshold_bps", 4.0).max(0.5);
        let maker_fee_bps = cfg.get_f64("maker_fee_bps", 1.5);
        let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);
        let default_stop_pct = cfg
            .get_f64("protective_stop_default_pct", 0.04)
            .clamp(0.005, 0.25);

        let asset = self
            .db
            .get_asset_by_id(ordered.asset_id)?
            .ok_or_else(|| AegisError::Database(format!("Asset {} not found", ordered.asset_id)))?;
        let pos = self
            .db
            .get_position_by_id(ordered.position_id)?
            .ok_or_else(|| {
                AegisError::Database(format!("Position {} not found", ordered.position_id))
            })?;

        if pos.status != "WORKING" {
            return Ok(ChaseStepOutcome::Resting {
                position_id: ordered.position_id,
                symbol: ordered.symbol.clone(),
                limit_price: ordered.limit_price,
                distance_from_mid_bps: ordered.distance_from_mid_bps,
            });
        }

        let side = OrderSide::parse(&ordered.side);
        let tick = asset.tick_size.max(1e-8);
        let live_mid = if asset.mid_price > 0.0 {
            asset.mid_price
        } else if ordered.mid_price > 0.0 {
            ordered.mid_price
        } else if asset.mark_price > 0.0 {
            asset.mark_price
        } else {
            ordered.limit_price
        };

        let is_live_exchange = crate::engine::is_live_exchange_active(&self.db, &cfg);

        // Only use simulated price_touched / paper IOC promotion in PAPER mode or isolated unit tests!
        // In live Testnet/Mainnet mode, orders are only promoted WORKING -> OPEN when Hyperliquid confirms a fill.
        if !is_live_exchange {
            let price_touched = match side {
                OrderSide::Long => live_mid <= ordered.limit_price * 1.0005,
                OrderSide::Short => live_mid >= ordered.limit_price * 0.9995,
            };
            let is_crossing_ioc = ordered.fsm_state == OrderFsmState::CrossingIoc.as_str();

            if price_touched || is_crossing_ioc {
                let fill_price = if is_crossing_ioc {
                    snap_to_tick(live_mid, tick)
                } else {
                    ordered.limit_price
                };
                let fee_bps = if is_crossing_ioc {
                    taker_fee_bps
                } else {
                    maker_fee_bps
                };
                let filled_qty = ordered.requested_quantity;
                let fees_paid_usdc = round_dp(fill_price * filled_qty * (fee_bps / 10_000.0), 4);
                let stop_px = if pos.protective_stop_px > 0.0 {
                    pos.protective_stop_px
                } else if side.is_buy() {
                    snap_to_tick(fill_price * (1.0 - default_stop_pct), tick)
                } else {
                    snap_to_tick(fill_price * (1.0 + default_stop_pct), tick)
                };
                let reason = if is_crossing_ioc {
                    "IOC_CROSSING_FILL_CONFIRMED"
                } else {
                    "ALO_MAKER_FILL_CONFIRMED"
                };

                let committed_usdc = self.finalize_fill_to_open(
                    ordered.position_id,
                    &asset,
                    side,
                    fill_price,
                    filled_qty,
                    fees_paid_usdc,
                    stop_px,
                    ordered.exchange_order_id.as_deref(),
                    reason,
                    &cfg,
                )?;

                return Ok(ChaseStepOutcome::FilledOpen {
                    position_id: ordered.position_id,
                    asset_id: ordered.asset_id,
                    symbol: ordered.symbol.clone(),
                    side: side.as_str().to_string(),
                    fill_price,
                    filled_quantity: filled_qty,
                    committed_notional_usdc: committed_usdc,
                    protective_stop_px: stop_px,
                    fees_paid_usdc,
                });
            }
        }

        let abs_dist_bps = if live_mid > 0.0 {
            ((live_mid - ordered.limit_price).abs() / live_mid) * 10_000.0
        } else {
            0.0
        };

        // Check for immediate exchange fills even when resting (< chase_threshold_bps) so resting ALO fills
        // are promoted with full sleeve metadata before 15s reconciliation runs.
        if is_live_exchange
            && ordered.chase_count < max_chases
            && abs_dist_bps < chase_threshold_bps
        {
            if let Some(outcome) = self.try_finalize_existing_exchange_fill(
                ordered,
                pos.protective_stop_px,
                &asset,
                side,
                default_stop_pct,
                maker_fee_bps,
                tick,
                &cfg,
            )? {
                return Ok(outcome);
            }
        }

        // Escalate to IOC crossing after reaching max_chases
        if ordered.chase_count >= max_chases {
            let ioc_price = if is_live_exchange {
                let band_lo = (live_mid * (1.0 - HL_ORACLE_BAND_PCT)).max(tick);
                let band_hi = live_mid * (1.0 + HL_ORACLE_BAND_PCT);
                let raw = if side.is_buy() {
                    live_mid * 1.02
                } else {
                    (live_mid * 0.98).max(tick)
                };
                snap_to_tick(raw.clamp(band_lo, band_hi), tick)
            } else {
                snap_to_tick(live_mid, tick)
            };

            if is_live_exchange {
                let pk = cfg.get_str("private_key", "");
                if let Some(old_oid) = parse_hl_oid(ordered.exchange_order_id.as_deref()) {
                    let cancel_res = self.http.cancel_order_wire(
                        asset.asset_index,
                        old_oid,
                        &pk,
                        Some(&self.db),
                        &cfg,
                    );
                    if let Ok(ref resp) = cancel_res {
                        let resp_lower = resp.to_string().to_ascii_lowercase();
                        if resp_lower.contains("filled") {
                            if let Some(outcome) = self.try_finalize_existing_exchange_fill(
                                ordered,
                                pos.protective_stop_px,
                                &asset,
                                side,
                                default_stop_pct,
                                maker_fee_bps,
                                tick,
                                &cfg,
                            )? {
                                return Ok(outcome);
                            }
                        }
                    }
                }

                let live_pos_opt = match self.query_exchange_position_state(&asset, side, &cfg) {
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
                let rem_qty = snap_to_lot(
                    (ordered.requested_quantity - live_qty).max(0.0),
                    asset.lot_step_size,
                    asset.sz_decimals,
                    true,
                );
                if live_qty > 1e-9 && (rem_qty <= asset.lot_step_size * 0.5 || rem_qty <= 1e-9) {
                    return self.finalize_known_fill_outcome(
                        ordered,
                        pos.protective_stop_px,
                        &asset,
                        side,
                        live_fill_px,
                        live_qty,
                        round_dp(live_fill_px * live_qty * (maker_fee_bps / 10_000.0), 4),
                        default_stop_pct,
                        tick,
                        "HL_ALO_FILL_DETECTED_ON_EXCHANGE",
                        &cfg,
                    );
                }

                let wire = build_limit_order_wire(
                    asset.asset_index,
                    side.is_buy(),
                    ioc_price,
                    rem_qty,
                    tick,
                    asset.lot_step_size,
                    asset.sz_decimals,
                    false,
                    "Ioc",
                    Some(&format!("{}-ioc", ordered.client_order_id)),
                );
                if let Ok(ack) = self.http.submit_order_wire(
                    wire,
                    rem_qty,
                    ioc_price,
                    taker_fee_bps,
                    &pk,
                    Some(&self.db),
                    &cfg,
                ) {
                    if ack.accepted && ack.status == "FILLED" && ack.filled_quantity > 0.0 {
                        let ioc_eoid = ack.exchange_order_id.or(ordered.exchange_order_id.clone());
                        let total_qty = live_qty + ack.filled_quantity;
                        let blended_px = if total_qty > 1e-9 {
                            (live_fill_px * live_qty + ack.avg_fill_price * ack.filled_quantity)
                                / total_qty
                        } else {
                            ack.avg_fill_price
                        };
                        let prior_fee = if live_qty > 1e-9 {
                            round_dp(live_fill_px * live_qty * (maker_fee_bps / 10_000.0), 4)
                        } else {
                            0.0
                        };
                        let total_fee = round_dp(prior_fee + ack.fee_usdc, 4);
                        let _ = self.db.record_working_order_chase(
                            ordered.position_id,
                            blended_px,
                            OrderFsmState::CrossingIoc.as_str(),
                            ioc_eoid.as_deref(),
                            &format!(
                                "Escalated to CROSSING_IOC after {} ALO chases (filled {:.4} @ ${:.4})",
                                ordered.chase_count, total_qty, blended_px
                            ),
                        )?;
                        let stop_px = if pos.protective_stop_px > 0.0 {
                            pos.protective_stop_px
                        } else if side.is_buy() {
                            snap_to_tick(blended_px * (1.0 - default_stop_pct), tick)
                        } else {
                            snap_to_tick(blended_px * (1.0 + default_stop_pct), tick)
                        };
                        let committed_usdc = self.finalize_fill_to_open(
                            ordered.position_id,
                            &asset,
                            side,
                            blended_px,
                            total_qty,
                            total_fee,
                            stop_px,
                            ioc_eoid.as_deref(),
                            "HL_IOC_ESCALATION_FILLED",
                            &cfg,
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
                }
                // Re-query clearinghouseState after IOC timeout/non-fill before marking CANCELLED
                match self.query_exchange_position_state(&asset, side, &cfg) {
                    Ok(Some((post_px, post_qty))) if post_qty > 1e-9 => {
                        return self.finalize_known_fill_outcome(
                            ordered,
                            pos.protective_stop_px,
                            &asset,
                            side,
                            post_px,
                            post_qty,
                            round_dp(post_px * post_qty * (taker_fee_bps / 10_000.0), 4),
                            default_stop_pct,
                            tick,
                            "HL_IOC_FILL_CONFIRMED_ON_STATE",
                            &cfg,
                        );
                    }
                    Err(e) => {
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
                // Live IOC did not fill on Hyperliquid -> cancel the WORKING order so no phantom order remains
                self.db
                    .cancel_working_order(ordered.position_id, "HL_IOC_UNFILLED_CANCELLED")?;
                return Ok(ChaseStepOutcome::Cancelled {
                    position_id: ordered.position_id,
                    symbol: ordered.symbol.clone(),
                    reason: "HL_IOC_UNFILLED_CANCELLED".to_string(),
                });
            }

            let ioc_eoid = ordered.exchange_order_id.clone();
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

            let filled_qty = ordered.requested_quantity;
            let fees_paid_usdc = round_dp(ioc_price * filled_qty * (taker_fee_bps / 10_000.0), 4);
            let stop_px = if pos.protective_stop_px > 0.0 {
                pos.protective_stop_px
            } else if side.is_buy() {
                snap_to_tick(ioc_price * (1.0 - default_stop_pct), tick)
            } else {
                snap_to_tick(ioc_price * (1.0 + default_stop_pct), tick)
            };

            let committed_usdc = self.finalize_fill_to_open(
                ordered.position_id,
                &asset,
                side,
                ioc_price,
                filled_qty,
                fees_paid_usdc,
                stop_px,
                ioc_eoid.as_deref(),
                "IOC_ESCALATION_FILLED",
                &cfg,
            )?;

            return Ok(ChaseStepOutcome::FilledOpen {
                position_id: ordered.position_id,
                asset_id: ordered.asset_id,
                symbol: ordered.symbol.clone(),
                side: side.as_str().to_string(),
                fill_price: ioc_price,
                filled_quantity: filled_qty,
                committed_notional_usdc: committed_usdc,
                protective_stop_px: stop_px,
                fees_paid_usdc,
            });
        }

        // Perform a 1s top-of-book ALO chase step if mid drifted >= chase_threshold_bps
        if abs_dist_bps >= chase_threshold_bps {
            let new_limit_price = if side.is_buy() {
                snap_to_tick((live_mid - tick).max(tick), tick)
            } else {
                snap_to_tick(live_mid + tick, tick)
            };
            let mut new_eoid = ordered.exchange_order_id.clone();

            if is_live_exchange {
                let pk = cfg.get_str("private_key", "");
                if let Some(old_oid) = parse_hl_oid(ordered.exchange_order_id.as_deref()) {
                    let cancel_res = self.http.cancel_order_wire(
                        asset.asset_index,
                        old_oid,
                        &pk,
                        Some(&self.db),
                        &cfg,
                    );
                    if let Ok(ref resp) = cancel_res {
                        let resp_lower = resp.to_string().to_ascii_lowercase();
                        if resp_lower.contains("filled") {
                            if let Some(outcome) = self.try_finalize_existing_exchange_fill(
                                ordered,
                                pos.protective_stop_px,
                                &asset,
                                side,
                                default_stop_pct,
                                maker_fee_bps,
                                tick,
                                &cfg,
                            )? {
                                return Ok(outcome);
                            }
                        }
                    }
                }
                let live_pos_opt = match self.query_exchange_position_state(&asset, side, &cfg) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(
                            symbol = %ordered.symbol,
                            error = %e,
                            "clearinghouseState unreachable before ALO chase; keeping WORKING"
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
                let rem_qty = snap_to_lot(
                    (ordered.requested_quantity - live_qty).max(0.0),
                    asset.lot_step_size,
                    asset.sz_decimals,
                    true,
                );
                if live_qty > 1e-9 && (rem_qty <= asset.lot_step_size * 0.5 || rem_qty <= 1e-9) {
                    return self.finalize_known_fill_outcome(
                        ordered,
                        pos.protective_stop_px,
                        &asset,
                        side,
                        live_fill_px,
                        live_qty,
                        round_dp(live_fill_px * live_qty * (maker_fee_bps / 10_000.0), 4),
                        default_stop_pct,
                        tick,
                        "HL_ALO_FILL_DETECTED_ON_EXCHANGE",
                        &cfg,
                    );
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
                    tick,
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
                    maker_fee_bps,
                    &pk,
                    Some(&self.db),
                    &cfg,
                ) {
                    Ok(ack) if ack.accepted => {
                        if ack.status == "FILLED" && ack.filled_quantity > 0.0 {
                            let total_qty = live_qty + ack.filled_quantity;
                            let blended_px = if total_qty > 1e-9 {
                                (live_fill_px * live_qty + ack.avg_fill_price * ack.filled_quantity)
                                    / total_qty
                            } else {
                                ack.avg_fill_price
                            };
                            let prior_fee = if live_qty > 1e-9 {
                                round_dp(live_fill_px * live_qty * (maker_fee_bps / 10_000.0), 4)
                            } else {
                                0.0
                            };
                            let total_fee = round_dp(prior_fee + ack.fee_usdc, 4);
                            let stop_px = if pos.protective_stop_px > 0.0 {
                                pos.protective_stop_px
                            } else if side.is_buy() {
                                snap_to_tick(blended_px * (1.0 - default_stop_pct), tick)
                            } else {
                                snap_to_tick(blended_px * (1.0 + default_stop_pct), tick)
                            };
                            let committed_usdc = self.finalize_fill_to_open(
                                ordered.position_id,
                                &asset,
                                side,
                                blended_px,
                                total_qty,
                                total_fee,
                                stop_px,
                                ack.exchange_order_id.as_deref(),
                                "HL_ALO_CHASE_FILLED",
                                &cfg,
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
                        if let Some(id) = ack.exchange_order_id {
                            new_eoid = Some(id);
                        }
                    }
                    _ => {
                        if live_qty > 1e-9 {
                            return self.finalize_known_fill_outcome(
                                ordered,
                                pos.protective_stop_px,
                                &asset,
                                side,
                                live_fill_px,
                                live_qty,
                                round_dp(live_fill_px * live_qty * (maker_fee_bps / 10_000.0), 4),
                                default_stop_pct,
                                tick,
                                "HL_ALO_PARTIAL_FILL_ON_EXCHANGE",
                                &cfg,
                            );
                        }
                        match self.query_exchange_position_state(&asset, side, &cfg) {
                            Ok(Some((post_px, post_qty))) if post_qty > 1e-9 => {
                                return self.finalize_known_fill_outcome(
                                    ordered,
                                    pos.protective_stop_px,
                                    &asset,
                                    side,
                                    post_px,
                                    post_qty,
                                    round_dp(post_px * post_qty * (maker_fee_bps / 10_000.0), 4),
                                    default_stop_pct,
                                    tick,
                                    "HL_ALO_FILL_DETECTED_ON_EXCHANGE",
                                    &cfg,
                                );
                            }
                            Err(_) => {
                                return Ok(ChaseStepOutcome::Resting {
                                    position_id: ordered.position_id,
                                    symbol: ordered.symbol.clone(),
                                    limit_price: ordered.limit_price,
                                    distance_from_mid_bps: ordered.distance_from_mid_bps,
                                });
                            }
                            Ok(_) => {}
                        }
                        self.db.cancel_working_order(
                            ordered.position_id,
                            "HL_CHASE_REJECTED_CANCELLED",
                        )?;
                        return Ok(ChaseStepOutcome::Cancelled {
                            position_id: ordered.position_id,
                            symbol: ordered.symbol.clone(),
                            reason: "HL_CHASE_REJECTED_CANCELLED".to_string(),
                        });
                    }
                }
            }

            let new_cc = self.db.record_working_order_chase(
                ordered.position_id,
                new_limit_price,
                OrderFsmState::ChasingAlo.as_str(),
                new_eoid.as_deref(),
                &format!(
                    "1s top-of-book ALO chase #{}: mid ${:.4} drifted {:.2} bps",
                    ordered.chase_count.saturating_add(1),
                    live_mid,
                    abs_dist_bps
                ),
            )?;

            return Ok(ChaseStepOutcome::Chased {
                position_id: ordered.position_id,
                symbol: ordered.symbol.clone(),
                new_limit_price,
                chase_count: new_cc,
                fsm_state: OrderFsmState::ChasingAlo.as_str().to_string(),
            });
        }

        Ok(ChaseStepOutcome::Resting {
            position_id: ordered.position_id,
            symbol: ordered.symbol.clone(),
            limit_price: ordered.limit_price,
            distance_from_mid_bps: ordered.distance_from_mid_bps,
        })
    }

    /// Promotes a `WORKING` order immediately if `clearinghouseState` already shows a filled matching position.
    #[allow(clippy::too_many_arguments)]
    fn try_finalize_existing_exchange_fill(
        &self,
        ordered: &OrderedAssetView,
        existing_stop_px: f64,
        asset: &ExchangeAsset,
        side: OrderSide,
        default_stop_pct: f64,
        maker_fee_bps: f64,
        tick: f64,
        cfg: &ConfigMap,
    ) -> Result<Option<ChaseStepOutcome>> {
        let Some((fill_px, fill_qty)) =
            self.check_if_working_order_filled_on_exchange(asset, side, cfg)
        else {
            return Ok(None);
        };
        let est_fee = round_dp(fill_px * fill_qty * (maker_fee_bps / 10_000.0), 4);
        self.finalize_known_fill_outcome(
            ordered,
            existing_stop_px,
            asset,
            side,
            fill_px,
            fill_qty,
            est_fee,
            default_stop_pct,
            tick,
            "HL_ALO_FILL_DETECTED_ON_EXCHANGE",
            cfg,
        )
        .map(Some)
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_known_fill_outcome(
        &self,
        ordered: &OrderedAssetView,
        existing_stop_px: f64,
        asset: &ExchangeAsset,
        side: OrderSide,
        fill_px: f64,
        fill_qty: f64,
        fees_paid_usdc: f64,
        default_stop_pct: f64,
        tick: f64,
        reason: &str,
        cfg: &ConfigMap,
    ) -> Result<ChaseStepOutcome> {
        let stop_px = if existing_stop_px > 0.0 {
            existing_stop_px
        } else if side.is_buy() {
            snap_to_tick(fill_px * (1.0 - default_stop_pct), tick)
        } else {
            snap_to_tick(fill_px * (1.0 + default_stop_pct), tick)
        };
        let committed_usdc = self.finalize_fill_to_open(
            ordered.position_id,
            asset,
            side,
            fill_px,
            fill_qty,
            fees_paid_usdc,
            stop_px,
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

    /// Queries Hyperliquid `clearinghouseState` returning `Ok(Some((fill_px, qty)))` if a matching-side
    /// position exists on `asset.coin`, `Ok(None)` if flat, or `Err(...)` if the exchange query failed.
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
        let ch_state = self.http.fetch_clearinghouse_state(&wallet, cfg)?;
        let parse_f = |v: Option<&Value>| -> f64 {
            v.and_then(|x| {
                x.as_f64()
                    .or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .filter(|val| val.is_finite())
            .unwrap_or(0.0)
        };
        let Some(arr) = ch_state.get("assetPositions").and_then(Value::as_array) else {
            return Ok(None);
        };
        for item in arr {
            let p = item.get("position").unwrap_or(item);
            let coin = p.get("coin").and_then(Value::as_str).unwrap_or("").trim();
            if !coin.eq_ignore_ascii_case(&asset.coin) {
                continue;
            }
            let szi = parse_f(p.get("szi"));
            if szi.abs() > 1e-9 && (szi > 0.0) == side.is_buy() {
                let qty = szi.abs();
                let entry_px = parse_f(p.get("entryPx"));
                let pos_val = parse_f(p.get("positionValue"));
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
                    return Ok(Some((fill_px, qty)));
                }
            }
        }
        Ok(None)
    }

    /// Queries Hyperliquid `clearinghouseState` to check if a `WORKING` order on `asset.coin`
    /// already filled on-chain before cancelling/re-submitting a chase/IOC order.
    fn check_if_working_order_filled_on_exchange(
        &self,
        asset: &ExchangeAsset,
        side: OrderSide,
        cfg: &ConfigMap,
    ) -> Option<(f64, f64)> {
        self.query_exchange_position_state(asset, side, cfg)
            .ok()
            .flatten()
    }

    /// Ensures an active `OPEN` position has valid Stop-Loss (`tpsl = "sl"`) and Take-Profit (`tpsl = "tp"`)
    /// prices and exchange trigger orders (or paper IDs in PAPER/test mode), persisting them to MariaDB.
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
        self.ensure_exchange_tpsl_orders_inner(
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
            resting_exchange_oids,
            None,
            cfg,
        )
    }

    /// Ensures SL/TP trigger orders using a pre-fetched `frontendOpenOrders` snapshot so callers such as
    /// `CutoverReconciler` preserve `cloid` resolution and duplicate trigger cleanup without extra HTTP polls.
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
        self.ensure_exchange_tpsl_orders_inner(
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
            None,
            open_orders_snapshot,
            cfg,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn ensure_exchange_tpsl_orders_inner(
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
        open_orders_snapshot: Option<&[Value]>,
        cfg: &ConfigMap,
    ) -> Result<(f64, Option<String>, f64, Option<String>)> {
        let tick = asset.tick_size.max(1e-8);
        let ref_mark = if mark_price > 0.0 {
            mark_price
        } else if asset.mark_price > 0.0 {
            asset.mark_price
        } else {
            asset.mid_price
        };
        let default_sl_pct = cfg
            .get_f64("protective_stop_default_pct", 0.04)
            .clamp(0.005, 0.35);
        let default_tp_pct = cfg
            .get_f64("take_profit_default_pct", 0.08)
            .clamp(0.01, 0.80);

        let stop_px = if desired_stop_px > 0.0
            && (ref_mark <= 0.0
                || match side {
                    OrderSide::Long => desired_stop_px < ref_mark - tick * 0.5,
                    OrderSide::Short => desired_stop_px > ref_mark + tick * 0.5,
                }) {
            snap_to_tick(desired_stop_px, tick)
        } else if ref_mark > 0.0 {
            match side {
                OrderSide::Long => {
                    snap_to_tick((ref_mark * (1.0 - default_sl_pct)).max(tick), tick)
                }
                OrderSide::Short => snap_to_tick(ref_mark * (1.0 + default_sl_pct), tick),
            }
        } else {
            0.0
        };

        let tp_px = if desired_tp_px > 0.0
            && (ref_mark <= 0.0
                || match side {
                    OrderSide::Long => desired_tp_px > ref_mark + tick * 0.5,
                    OrderSide::Short => desired_tp_px < ref_mark - tick * 0.5,
                }) {
            snap_to_tick(desired_tp_px, tick)
        } else if ref_mark > 0.0 {
            match side {
                OrderSide::Long => snap_to_tick(ref_mark * (1.0 + default_tp_pct), tick),
                OrderSide::Short => {
                    snap_to_tick((ref_mark * (1.0 - default_tp_pct)).max(tick), tick)
                }
            }
        } else {
            0.0
        };

        let is_live_exchange = crate::engine::is_live_exchange_active(&self.db, cfg);

        if !is_live_exchange {
            let sl_oid = Some(format!("paper-sl-{position_id}"));
            let tp_oid = Some(format!("paper-tp-{position_id}"));
            self.db.update_position_tpsl_orders(
                position_id,
                stop_px,
                sl_oid.as_deref(),
                tp_px,
                tp_oid.as_deref(),
            )?;
            return Ok((stop_px, sl_oid, tp_px, tp_oid));
        }

        let pk = cfg.get_str("private_key", "");
        let wallet = cfg.get_str("wallet_address", "");
        let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);

        let mut resting_oids: HashSet<u64> = HashSet::new();
        let mut cloid_to_oid: HashMap<String, u64> = HashMap::new();
        let mut trigger_orders: Vec<TriggerOrderInfo> = Vec::new();
        let mut have_resting_snapshot = false;
        let mut has_trigger_metadata = false;

        let parse_f = |v: Option<&Value>| -> f64 {
            v.and_then(|x| {
                x.as_f64()
                    .or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .filter(|val| val.is_finite())
            .unwrap_or(0.0)
        };
        let collect_orders =
            |orders: &[Value],
             resting_oids: &mut HashSet<u64>,
             cloid_to_oid: &mut HashMap<String, u64>,
             trigger_orders: &mut Vec<TriggerOrderInfo>| {
                for ord in orders {
                    let Some(oid) = extract_order_oid(ord) else {
                        continue;
                    };
                    resting_oids.insert(oid);
                    if let Some(cloid) = ord.get("cloid").and_then(Value::as_str) {
                        cloid_to_oid.insert(cloid.trim().to_ascii_lowercase(), oid);
                    }
                    let coin_match = ord
                        .get("coin")
                        .and_then(Value::as_str)
                        .is_some_and(|c| c.trim().eq_ignore_ascii_case(&asset.coin));
                    let is_trigger = ord
                        .get("isTrigger")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    if coin_match && is_trigger {
                        let order_type = ord
                            .get("orderType")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_ascii_lowercase();
                        let is_tp_flag = if order_type.contains("take profit") || order_type == "tp"
                        {
                            Some(true)
                        } else if order_type.contains("stop") || order_type == "sl" {
                            Some(false)
                        } else {
                            None
                        };
                        if let Some(is_tp_val) = is_tp_flag {
                            trigger_orders.push(TriggerOrderInfo {
                                oid,
                                is_tp: is_tp_val,
                                trigger_px: parse_f(ord.get("triggerPx")),
                                qty: parse_f(ord.get("origSz")).max(parse_f(ord.get("sz"))),
                            });
                        }
                    }
                }
            };

        if let Some(orders) = open_orders_snapshot {
            have_resting_snapshot = true;
            has_trigger_metadata = true;
            collect_orders(
                orders,
                &mut resting_oids,
                &mut cloid_to_oid,
                &mut trigger_orders,
            );
        } else if let Some(set) = resting_exchange_oids {
            have_resting_snapshot = true;
            resting_oids.extend(set.iter().copied());
            if set.is_empty() {
                has_trigger_metadata = true;
            } else if !wallet.is_empty() {
                if let Ok(orders) = self
                    .http
                    .fetch_frontend_open_orders(&wallet, cfg)
                    .or_else(|_| self.http.fetch_open_orders(&wallet, cfg))
                {
                    has_trigger_metadata = true;
                    collect_orders(
                        &orders,
                        &mut resting_oids,
                        &mut cloid_to_oid,
                        &mut trigger_orders,
                    );
                }
            }
        } else if !wallet.is_empty() {
            match self
                .http
                .fetch_frontend_open_orders(&wallet, cfg)
                .or_else(|_| self.http.fetch_open_orders(&wallet, cfg))
            {
                Ok(orders) => {
                    have_resting_snapshot = true;
                    has_trigger_metadata = true;
                    collect_orders(
                        &orders,
                        &mut resting_oids,
                        &mut cloid_to_oid,
                        &mut trigger_orders,
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        symbol = %asset.symbol,
                        error = %e,
                        "Failed to fetch frontendOpenOrders before TP/SL reconciliation"
                    );
                }
            }
        }

        let sl_oid = if stop_px > 0.0 && quantity > 0.0 {
            self.ensure_single_trigger_leg(
                false,
                position_id,
                asset,
                side,
                quantity,
                stop_px,
                existing_sl_oid,
                replace_sl,
                have_resting_snapshot,
                has_trigger_metadata,
                &resting_oids,
                &cloid_to_oid,
                &trigger_orders,
                &pk,
                &wallet,
                taker_fee_bps,
                cfg,
            )
        } else {
            None
        };

        let tp_oid = if tp_px > 0.0 && quantity > 0.0 {
            self.ensure_single_trigger_leg(
                true,
                position_id,
                asset,
                side,
                quantity,
                tp_px,
                existing_tp_oid,
                replace_tp,
                have_resting_snapshot,
                has_trigger_metadata,
                &resting_oids,
                &cloid_to_oid,
                &trigger_orders,
                &pk,
                &wallet,
                taker_fee_bps,
                cfg,
            )
        } else {
            None
        };

        self.db.update_position_tpsl_orders(
            position_id,
            stop_px,
            sl_oid.as_deref(),
            tp_px,
            tp_oid.as_deref(),
        )?;

        Ok((stop_px, sl_oid, tp_px, tp_oid))
    }

    #[allow(clippy::too_many_arguments)]
    fn ensure_single_trigger_leg(
        &self,
        is_tp: bool,
        position_id: i64,
        asset: &ExchangeAsset,
        side: OrderSide,
        quantity: f64,
        trigger_px: f64,
        existing_oid_str: Option<&str>,
        force_replace: bool,
        have_resting_snapshot: bool,
        has_trigger_metadata: bool,
        resting_oids: &HashSet<u64>,
        cloid_to_oid: &HashMap<String, u64>,
        trigger_orders: &[TriggerOrderInfo],
        pk: &str,
        wallet: &str,
        taker_fee_bps: f64,
        cfg: &ConfigMap,
    ) -> Option<String> {
        let leg_label = if is_tp { "TP" } else { "SL" };
        let tick = asset.tick_size.max(1e-8);
        let existing_num_oid = parse_hl_oid(existing_oid_str).or_else(|| {
            existing_oid_str
                .map(|s| s.trim().to_ascii_lowercase())
                .and_then(|c| cloid_to_oid.get(&c).copied())
        });

        let leg_triggers: Vec<&TriggerOrderInfo> =
            trigger_orders.iter().filter(|t| t.is_tp == is_tp).collect();
        let exp_px_str = format_hl_wire_price(trigger_px, tick, asset.sz_decimals);
        let exp_sz_str = {
            let floored =
                format_hl_wire_size_floor(quantity, asset.lot_step_size, asset.sz_decimals);
            if floored == "0" {
                format_hl_wire_size(quantity, asset.lot_step_size, asset.sz_decimals)
            } else {
                floored
            }
        };
        let matches_canonical = |t: &TriggerOrderInfo| -> bool {
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
            if have_resting_snapshot && has_trigger_metadata {
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
                    let mut dup_cancels: Vec<CancelWire> = Vec::new();
                    let mut seen: HashSet<u64> = HashSet::new();
                    for t in &leg_triggers {
                        if t.oid != keep_oid && seen.insert(t.oid) {
                            dup_cancels.push(CancelWire {
                                a: asset.asset_index,
                                o: t.oid,
                            });
                        }
                    }
                    if !dup_cancels.is_empty() {
                        let _ = self
                            .http
                            .cancel_orders_batch(dup_cancels, pk, Some(&self.db), cfg);
                    }
                    return Some(format!("hl-{keep_oid}"));
                }
            } else if let Some(oid) = existing_num_oid {
                if !have_resting_snapshot || resting_oids.contains(&oid) {
                    return Some(format!("hl-{oid}"));
                }
            }
        }

        // Cancel all stale/duplicate trigger orders for this (coin, leg) and any tracked old_oid before replacing
        let mut stale_cancels: Vec<CancelWire> = Vec::new();
        let mut cancel_seen: HashSet<u64> = HashSet::new();
        for t in &leg_triggers {
            if cancel_seen.insert(t.oid) {
                stale_cancels.push(CancelWire {
                    a: asset.asset_index,
                    o: t.oid,
                });
            }
        }
        if let Some(old_oid) = existing_num_oid {
            if (!have_resting_snapshot || resting_oids.contains(&old_oid))
                && cancel_seen.insert(old_oid)
            {
                stale_cancels.push(CancelWire {
                    a: asset.asset_index,
                    o: old_oid,
                });
            }
        }
        if !stale_cancels.is_empty() {
            let _ = self
                .http
                .cancel_orders_batch(stale_cancels, pk, Some(&self.db), cfg);
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
            build_protective_stop_order_wire(
                asset.asset_index,
                side,
                quantity,
                trigger_px,
                asset.tick_size,
                asset.lot_step_size,
                asset.sz_decimals,
                Some(&coid),
            )
        };

        match self.http.submit_order_wire(
            wire,
            quantity,
            trigger_px,
            taker_fee_bps,
            pk,
            Some(&self.db),
            cfg,
        ) {
            Ok(ack) if ack.accepted => {
                if let Some(eoid) = ack.exchange_order_id {
                    return Some(eoid);
                }
                if !wallet.is_empty() {
                    if let Ok(front_orders) = self.http.fetch_frontend_open_orders(wallet, cfg) {
                        for ord in &front_orders {
                            let cloid_match = ord
                                .get("cloid")
                                .and_then(Value::as_str)
                                .is_some_and(|c| c.trim().eq_ignore_ascii_case(&expected_cloid));
                            if cloid_match {
                                if let Some(oid) = extract_order_oid(ord) {
                                    return Some(format!("hl-{oid}"));
                                }
                            }
                        }
                        for ord in &front_orders {
                            let coin_match = ord
                                .get("coin")
                                .and_then(Value::as_str)
                                .is_some_and(|c| c.trim().eq_ignore_ascii_case(&asset.coin));
                            let is_trigger = ord
                                .get("isTrigger")
                                .and_then(Value::as_bool)
                                .unwrap_or(false);
                            let type_match = ord
                                .get("orderType")
                                .and_then(Value::as_str)
                                .is_some_and(|t| {
                                    let lower = t.to_ascii_lowercase();
                                    if is_tp {
                                        lower.contains("take profit")
                                    } else {
                                        lower.contains("stop") && !lower.contains("take profit")
                                    }
                                });
                            if coin_match && is_trigger && type_match {
                                if let Some(oid) = extract_order_oid(ord) {
                                    return Some(format!("hl-{oid}"));
                                }
                            }
                        }
                    }
                }
                Some(expected_cloid)
            }
            Ok(ack) => {
                tracing::warn!(
                    symbol = %asset.symbol,
                    leg = leg_label,
                    trigger_px = trigger_px,
                    error = ?ack.raw_error,
                    "Hyperliquid rejected exchange trigger order"
                );
                None
            }
            Err(e) => {
                tracing::warn!(
                    symbol = %asset.symbol,
                    leg = leg_label,
                    trigger_px = trigger_px,
                    error = %e,
                    "Failed to submit exchange trigger order"
                );
                None
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_fill_to_open(
        &self,
        position_id: i64,
        asset: &ExchangeAsset,
        side: OrderSide,
        fill_price: f64,
        filled_qty: f64,
        fees_paid_usdc: f64,
        stop_px: f64,
        exchange_order_id: Option<&str>,
        reason: &str,
        cfg: &ConfigMap,
    ) -> Result<f64> {
        let bars_1h = self
            .db
            .get_ohlcv_bars(asset.id, "1h", 20)
            .unwrap_or_default();
        let limits = RiskLimitsConfig::from_config_map(cfg);
        let atr_opt = (!bars_1h.is_empty()).then(|| {
            compute_atr_from_bars(&bars_1h, 14, fill_price, limits.protective_stop_default_pct)
        });
        let tick = asset.tick_size.max(1e-8);
        let eff_stop_px = if stop_px > 0.0
            && match side {
                OrderSide::Long => stop_px < fill_price - tick * 0.5,
                OrderSide::Short => stop_px > fill_price + tick * 0.5,
            } {
            snap_to_tick(stop_px, tick)
        } else {
            compute_initial_protective_stop(side, fill_price, atr_opt, tick, &limits)
        };
        let atr_tp_px = compute_initial_take_profit(side, fill_price, atr_opt, tick, &limits);
        let min_2r_dist = (fill_price - eff_stop_px).abs() * 2.0;
        let eff_tp_dist = (atr_tp_px - fill_price).abs().max(min_2r_dist);
        let eff_tp_px = match side {
            OrderSide::Long => snap_to_tick(fill_price + eff_tp_dist, tick),
            OrderSide::Short => snap_to_tick((fill_price - eff_tp_dist).max(tick), tick),
        };

        self.db.promote_working_to_open(
            position_id,
            fill_price,
            filled_qty,
            fees_paid_usdc,
            eff_stop_px,
            exchange_order_id,
            reason,
        )?;

        let (final_stop_px, _sl_oid, final_tp_px, _tp_oid) = self.ensure_exchange_tpsl_orders(
            position_id,
            asset,
            side,
            filled_qty,
            fill_price,
            eff_stop_px,
            eff_tp_px,
            None,
            None,
            true,
            true,
            None,
            cfg,
        )?;

        let committed_usdc = round_dp(fill_price * filled_qty, 4);

        if let (Ok(Some(pos)), Ok(snap)) = (
            self.db.get_position_by_id(position_id),
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
                filled_qty,
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
            let _ = self.alerts.notify_order_filled_open(&alert);
        }

        Ok(committed_usdc)
    }
}
