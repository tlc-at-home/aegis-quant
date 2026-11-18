//! 1-Second Maker Limit Chaser & Order-to-Open Capital Lifecycle (`aegis-execution::order_chaser`).
//!
//! Enforces the Rev 3.1 strict Order-to-Open Capital Lifecycle:
//! - Places new entries as `WORKING` (`WORKING_ALO`), reserving `reserved_margin_usdc` without spending cash into `committed_notional_usdc`.
//! - Chases top-of-book at 1s frequency (`record_working_order_chase`), escalating to `CROSSING_IOC` after `max_alo_chase_ticks`.
//! - Upon fill confirmation, calls `promote_working_to_open` (moving funds from `reserved_margin_usdc` into `committed_notional_usdc`),
//!   places an exchange-side protective stop order, and enqueues a rich HTML Telegram alert.

use crate::http_client::HyperliquidHttpClient;
use crate::signing::{
    build_limit_order_wire, build_protective_stop_order_wire, snap_to_lot, snap_to_tick,
};
use aegis_alerts::{AlertEngine, OrderFilledOpenAlert, WorkingOrderPlacedAlert};
use aegis_core::{
    now_ms, round_dp, AegisError, ConfigMap, ExchangeAsset, OrderFsmState, OrderSide,
    OrderedAssetView, Result,
};
use aegis_db::{DatabaseManager, WorkingOrderInput};
use serde::{Deserialize, Serialize};

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

        let is_live_exchange = cfg.execution_mode().is_live_or_testnet()
            && !cfg.get_str("private_key", "").is_empty()
            && !self.db.is_ephemeral_test_db();

        if is_live_exchange {
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

        let is_live_exchange = cfg.execution_mode().is_live_or_testnet()
            && !cfg.get_str("private_key", "").is_empty()
            && !self.db.is_ephemeral_test_db();

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

        // Escalate to IOC crossing after reaching max_chases
        if ordered.chase_count >= max_chases {
            let ioc_price = if is_live_exchange {
                if side.is_buy() {
                    snap_to_tick(live_mid * 1.02, tick)
                } else {
                    snap_to_tick((live_mid * 0.98).max(tick), tick)
                }
            } else {
                snap_to_tick(live_mid, tick)
            };

            if is_live_exchange {
                let pk = cfg.get_str("private_key", "");
                if let Some(old_oid) = ordered
                    .exchange_order_id
                    .as_deref()
                    .and_then(|s| s.strip_prefix("hl-").or(Some(s)))
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    let _ = self.http.cancel_order_wire(
                        asset.asset_index,
                        old_oid,
                        &pk,
                        Some(&self.db),
                        &cfg,
                    );
                }
                let wire = build_limit_order_wire(
                    asset.asset_index,
                    side.is_buy(),
                    ioc_price,
                    ordered.requested_quantity,
                    tick,
                    asset.lot_step_size,
                    asset.sz_decimals,
                    false,
                    "Ioc",
                    Some(&format!("{}-ioc", ordered.client_order_id)),
                );
                if let Ok(ack) = self.http.submit_order_wire(
                    wire,
                    ordered.requested_quantity,
                    ioc_price,
                    taker_fee_bps,
                    &pk,
                    Some(&self.db),
                    &cfg,
                ) {
                    if ack.accepted && ack.status == "FILLED" && ack.filled_quantity > 0.0 {
                        let ioc_eoid = ack.exchange_order_id.or(ordered.exchange_order_id.clone());
                        let _ = self.db.record_working_order_chase(
                            ordered.position_id,
                            ack.avg_fill_price,
                            OrderFsmState::CrossingIoc.as_str(),
                            ioc_eoid.as_deref(),
                            &format!(
                                "Escalated to CROSSING_IOC after {} ALO chases (filled {:.4} @ ${:.4})",
                                ordered.chase_count, ack.filled_quantity, ack.avg_fill_price
                            ),
                        )?;
                        let stop_px = if pos.protective_stop_px > 0.0 {
                            pos.protective_stop_px
                        } else if side.is_buy() {
                            snap_to_tick(ack.avg_fill_price * (1.0 - default_stop_pct), tick)
                        } else {
                            snap_to_tick(ack.avg_fill_price * (1.0 + default_stop_pct), tick)
                        };
                        let committed_usdc = self.finalize_fill_to_open(
                            ordered.position_id,
                            &asset,
                            side,
                            ack.avg_fill_price,
                            ack.filled_quantity,
                            ack.fee_usdc,
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
                            fill_price: ack.avg_fill_price,
                            filled_quantity: ack.filled_quantity,
                            committed_notional_usdc: committed_usdc,
                            protective_stop_px: stop_px,
                            fees_paid_usdc: ack.fee_usdc,
                        });
                    }
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
                if let Some(old_oid) = ordered
                    .exchange_order_id
                    .as_deref()
                    .and_then(|s| s.strip_prefix("hl-").or(Some(s)))
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    let _ = self.http.cancel_order_wire(
                        asset.asset_index,
                        old_oid,
                        &pk,
                        Some(&self.db),
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
                    ordered.requested_quantity,
                    tick,
                    asset.lot_step_size,
                    asset.sz_decimals,
                    false,
                    "Alo",
                    Some(&chase_coid),
                );
                match self.http.submit_order_wire(
                    wire,
                    ordered.requested_quantity,
                    new_limit_price,
                    maker_fee_bps,
                    &pk,
                    Some(&self.db),
                    &cfg,
                ) {
                    Ok(ack) if ack.accepted => {
                        if ack.status == "FILLED" && ack.filled_quantity > 0.0 {
                            let stop_px = if pos.protective_stop_px > 0.0 {
                                pos.protective_stop_px
                            } else if side.is_buy() {
                                snap_to_tick(ack.avg_fill_price * (1.0 - default_stop_pct), tick)
                            } else {
                                snap_to_tick(ack.avg_fill_price * (1.0 + default_stop_pct), tick)
                            };
                            let committed_usdc = self.finalize_fill_to_open(
                                ordered.position_id,
                                &asset,
                                side,
                                ack.avg_fill_price,
                                ack.filled_quantity,
                                ack.fee_usdc,
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
                                fill_price: ack.avg_fill_price,
                                filled_quantity: ack.filled_quantity,
                                committed_notional_usdc: committed_usdc,
                                protective_stop_px: stop_px,
                                fees_paid_usdc: ack.fee_usdc,
                            });
                        }
                        if let Some(id) = ack.exchange_order_id {
                            new_eoid = Some(id);
                        }
                    }
                    _ => {
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
        self.db.promote_working_to_open(
            position_id,
            fill_price,
            filled_qty,
            fees_paid_usdc,
            stop_px,
            exchange_order_id,
            reason,
        )?;

        // Submit exchange-side protective trigger stop order if on Testnet/Live
        if cfg.execution_mode().is_live_or_testnet() {
            let pk = cfg.get_str("private_key", "");
            if !pk.is_empty() && stop_px > 0.0 {
                let stop_wire = build_protective_stop_order_wire(
                    asset.asset_index,
                    side,
                    filled_qty,
                    stop_px,
                    asset.tick_size,
                    asset.lot_step_size,
                    asset.sz_decimals,
                    Some(&format!("v2-sl-{position_id}-{}", now_ms())),
                );
                let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);
                let _ = self.http.submit_order_wire(
                    stop_wire,
                    filled_qty,
                    stop_px,
                    taker_fee_bps,
                    &pk,
                    Some(&self.db),
                    cfg,
                );
            }
        }

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
                protective_stop_px: stop_px,
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
