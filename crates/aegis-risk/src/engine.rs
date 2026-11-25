//! Primary Risk & Order-to-Open Capital Sizing Engine (`aegis-risk::engine`).
//!
//! Coordinates:
//! 1. Order-to-Open Capital Reservation (`WORKING` order placement reserves `reserved_margin_usdc`, `committed_notional_usdc = 0.0`).
//! 2. Order-to-Open Capital Commitment (`WORKING -> OPEN` transition releases `reserved_margin_usdc` and commits `committed_notional_usdc` from `available_cash_usdc`).
//! 3. Initial Protective Stop & 1s High-Water Chandelier/ATR Trailing Stop Ratchet.

use crate::limits::{
    compute_capital_ledger, compute_capital_ledger_from_views, compute_committed_notional_usdc,
    compute_reserved_margin_usdc, round_to_lot, round_to_tick, CapitalLedgerState,
    RiskLimitsConfig,
};
use crate::stops::{
    compute_initial_protective_stop, compute_initial_take_profit, evaluate_1s_position_lifecycle,
    evaluate_1s_position_tpsl, evaluate_1s_protective_stop, StopEvaluationInput,
    StopRatchetEvaluation,
};
use aegis_core::{
    round_dp, AegisError, ConfigMap, ExchangeAsset, HeldAssetView, OrderSide, OrderedAssetView,
    PositionRecord, Result, SleeveSignal,
};
use serde::{Deserialize, Serialize};

/// Sizing and margin-reservation plan for a new `WORKING` Maker Limit Chaser order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkingOrderSizingPlan {
    pub approved: bool,
    pub rejection_reason: Option<String>,
    pub asset_id: i64,
    pub symbol: String,
    pub side: OrderSide,
    pub limit_price: f64,
    pub requested_quantity: f64,
    pub target_notional_usdc: f64,
    pub reserved_margin_usdc: f64,
    pub committed_notional_usdc: f64,
    pub portfolio_weight: f64,
    pub s1_weight_contrib: f64,
    pub s2_weight_contrib: f64,
    pub s3_weight_contrib: f64,
    pub protective_stop_px: f64,
    pub stop_distance_pct: f64,
    pub take_profit_px: f64,
    pub tp_distance_pct: f64,
    /// Per-trade equity risk budget in USDC (`equity * risk_per_trade_equity_pct`), `0.0` when disabled.
    #[serde(default)]
    pub risk_budget_usdc: f64,
}

/// Summary of the `WORKING -> OPEN` capital commitment transition when an exchange fill is confirmed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenCommitmentSummary {
    pub fill_price: f64,
    pub filled_quantity: f64,
    pub released_reserved_margin_usdc: f64,
    pub committed_notional_usdc: f64,
    pub available_cash_after_usdc: f64,
    pub protective_stop_px: f64,
    pub take_profit_px: f64,
    pub fees_paid_usdc: f64,
}

/// Primary Aegis Quant v2.0 Risk & Capital Sizing Engine.
#[derive(Debug, Clone, Default)]
pub struct RiskEngine {
    pub limits: RiskLimitsConfig,
}

impl RiskEngine {
    pub fn new(limits: RiskLimitsConfig) -> Self {
        Self { limits }
    }

    pub fn from_config_map(cfg: &ConfigMap) -> Self {
        Self {
            limits: RiskLimitsConfig::from_config_map(cfg),
        }
    }

    /// Computes the current `CapitalLedgerState` from base equity and active `PositionRecord` rows.
    pub fn evaluate_ledger(
        &self,
        base_cash_equity_usdc: f64,
        positions: &[PositionRecord],
    ) -> CapitalLedgerState {
        compute_capital_ledger(
            base_cash_equity_usdc,
            positions,
            self.limits.target_leverage_multiplier,
        )
    }

    /// Computes the current `CapitalLedgerState` directly from `v_assets_held` and `v_assets_ordered` view rows.
    pub fn evaluate_ledger_from_views(
        &self,
        base_cash_equity_usdc: f64,
        held: &[HeldAssetView],
        ordered: &[OrderedAssetView],
    ) -> CapitalLedgerState {
        compute_capital_ledger_from_views(
            base_cash_equity_usdc,
            held,
            ordered,
            self.limits.target_leverage_multiplier,
        )
    }

    /// Sizes a new `WORKING` order from a `SleeveSignal` and the current `CapitalLedgerState`.
    /// Reserves `reserved_margin_usdc` while keeping `committed_notional_usdc = 0.0` until fill confirmation.
    pub fn size_working_order(
        &self,
        asset: &ExchangeAsset,
        signal: &SleeveSignal,
        ledger: &CapitalLedgerState,
        atr_opt: Option<f64>,
    ) -> WorkingOrderSizingPlan {
        self.size_working_order_from_weights(
            asset,
            signal.target_weight,
            signal.current_weight,
            signal.w_s1_carry_mom,
            signal.w_s2_trend_breakout,
            signal.w_s3_event_reversion,
            ledger,
            atr_opt,
        )
    }

    /// Sizes a new `WORKING` order from explicit target/current weights and sleeve attribution weights.
    #[allow(clippy::too_many_arguments)]
    pub fn size_working_order_from_weights(
        &self,
        asset: &ExchangeAsset,
        target_weight: f64,
        current_weight: f64,
        w_s1_carry_mom: f64,
        w_s2_trend_breakout: f64,
        w_s3_event_reversion: f64,
        ledger: &CapitalLedgerState,
        atr_opt: Option<f64>,
    ) -> WorkingOrderSizingPlan {
        if !target_weight.is_finite()
            || !current_weight.is_finite()
            || !asset.mark_price.is_finite()
            || !ledger.total_equity_usdc.is_finite()
            || !ledger.available_cash_usdc.is_finite()
            || !ledger.reserved_order_cash_usdc.is_finite()
            || !ledger.committed_notional_usdc.is_finite()
        {
            return WorkingOrderSizingPlan {
                approved: false,
                rejection_reason: Some("NON_FINITE_RISK_INPUT".to_string()),
                asset_id: asset.id,
                symbol: asset.symbol.clone(),
                side: OrderSide::Long,
                limit_price: 0.0,
                requested_quantity: 0.0,
                target_notional_usdc: 0.0,
                reserved_margin_usdc: 0.0,
                committed_notional_usdc: 0.0,
                portfolio_weight: 0.0,
                s1_weight_contrib: 0.0,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 0.0,
                stop_distance_pct: 0.0,
                take_profit_px: 0.0,
                tp_distance_pct: 0.0,
                risk_budget_usdc: 0.0,
            };
        }

        let clamped_target_w = target_weight.clamp(
            -self.limits.max_single_asset_weight,
            self.limits.max_single_asset_weight,
        );
        let delta_w = clamped_target_w - current_weight;
        let side = if delta_w >= 0.0 {
            OrderSide::Long
        } else {
            OrderSide::Short
        };

        let raw_px = if asset.mid_price.is_finite() && asset.mid_price > 0.0 {
            asset.mid_price
        } else {
            asset.mark_price
        };
        let tick_lot_ok = asset.tick_size.is_finite()
            && asset.tick_size > 0.0
            && asset.lot_step_size.is_finite()
            && asset.lot_step_size > 0.0;
        let limit_price = if tick_lot_ok {
            round_to_tick(raw_px, asset.tick_size, side.is_buy())
        } else {
            0.0
        };

        let reject = |reason: String| WorkingOrderSizingPlan {
            approved: false,
            rejection_reason: Some(reason),
            asset_id: asset.id,
            symbol: asset.symbol.clone(),
            side,
            limit_price,
            requested_quantity: 0.0,
            target_notional_usdc: 0.0,
            reserved_margin_usdc: 0.0,
            committed_notional_usdc: 0.0,
            portfolio_weight: 0.0,
            s1_weight_contrib: 0.0,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.0,
            stop_distance_pct: 0.0,
            take_profit_px: 0.0,
            tp_distance_pct: 0.0,
            risk_budget_usdc: 0.0,
        };

        if !tick_lot_ok {
            return reject("INVALID_TICK_OR_LOT_SIZE".to_string());
        }

        if limit_price <= 0.0 {
            return reject("INVALID_MARK_OR_MID_PRICE".to_string());
        }

        if ledger.is_ruin_halted(self.limits.min_equity_halt_usdc)
            || ledger.total_equity_usdc <= 0.0
        {
            tracing::error!(
                asset_id = asset.id,
                symbol = %asset.symbol,
                total_equity_usdc = ledger.total_equity_usdc,
                min_equity_halt_usdc = self.limits.min_equity_halt_usdc,
                "Rejecting working order: RUIN_HALT_EQUITY_BELOW_FLOOR"
            );
            return reject("RUIN_HALT_EQUITY_BELOW_FLOOR".to_string());
        }

        let eff_lev = if self.limits.target_leverage_multiplier.is_finite() {
            self.limits.target_leverage_multiplier.max(0.10)
        } else {
            1.0
        };
        let eff_min_delta = (self.limits.min_rebalance_delta_weight * eff_lev)
            .max(1e-4)
            .min(self.limits.max_single_asset_weight.max(1e-4));
        if delta_w.abs() < eff_min_delta {
            return reject(format!(
                "DELTA_BELOW_THRESHOLD: |delta_w|={:.4} < min={:.4}",
                delta_w.abs(),
                eff_min_delta
            ));
        }

        let equity = ledger.total_equity_usdc;
        let max_gross_notional = equity * self.limits.max_gross_leverage_x;
        let existing_reserved_notional = ledger.reserved_order_cash_usdc.max(0.0) * eff_lev;
        let remaining_gross_notional =
            (max_gross_notional - ledger.committed_notional_usdc - existing_reserved_notional)
                .max(0.0);

        if remaining_gross_notional < self.limits.min_order_notional_usdc {
            return reject(format!(
                "MAX_GROSS_LEVERAGE_REACHED: remaining_notional={remaining_gross_notional:.2} < min={:.2}",
                self.limits.min_order_notional_usdc
            ));
        }

        // Compute protective stop BEFORE notional sizing for risk-parity sizing (`P1-Strat`).
        let protective_stop_px = compute_initial_protective_stop(
            side,
            limit_price,
            atr_opt,
            asset.tick_size,
            &self.limits,
        );
        let stop_distance_frac = if protective_stop_px.is_finite() && protective_stop_px > 0.0 {
            (limit_price - protective_stop_px).abs() / limit_price
        } else {
            0.0
        };
        if !stop_distance_frac.is_finite() || stop_distance_frac <= 1e-6 {
            return reject("INVALID_STOP_DISTANCE".to_string());
        }

        let weight_cap_notional = equity * self.limits.max_single_asset_weight;
        let risk_parity_notional = if self.limits.risk_per_trade_equity_pct.is_finite()
            && self.limits.risk_per_trade_equity_pct > 0.0
        {
            (equity * self.limits.risk_per_trade_equity_pct) / stop_distance_frac
        } else {
            f64::INFINITY
        };
        let existing_notional = if current_weight * side.sign() > 0.0 {
            equity * current_weight.abs()
        } else {
            0.0
        };
        let max_add_cap =
            (weight_cap_notional.min(risk_parity_notional) - existing_notional).max(0.0);

        let avail_cash = ledger.available_cash_usdc.max(0.0);
        let max_notional_from_cash = avail_cash * eff_lev;
        let asset_max_lev = if asset.max_leverage.is_finite() {
            asset.max_leverage.max(1.0)
        } else {
            1.0
        };
        let max_asset_lev_notional = avail_cash * asset_max_lev;
        let desired_notional = equity * delta_w.abs();
        let capped_notional = desired_notional
            .min(remaining_gross_notional)
            .min(max_notional_from_cash)
            .min(max_asset_lev_notional)
            .min(max_add_cap);

        let raw_qty = capped_notional / limit_price;
        let requested_quantity = round_to_lot(raw_qty, asset.lot_step_size);
        let target_notional_usdc = round_dp(requested_quantity * limit_price, 4);

        if requested_quantity <= 0.0 || target_notional_usdc < self.limits.min_order_notional_usdc {
            return reject(format!(
                "BELOW_MIN_ORDER_NOTIONAL: notional={target_notional_usdc:.2} < min={:.2}",
                self.limits.min_order_notional_usdc
            ));
        }

        let reserved_margin_usdc = compute_reserved_margin_usdc(
            target_notional_usdc,
            self.limits.target_leverage_multiplier,
        );
        if reserved_margin_usdc > ledger.available_cash_usdc + 1e-4 {
            return reject(format!(
                "INSUFFICIENT_AVAILABLE_CASH: required_margin={reserved_margin_usdc:.2} > available_cash={:.2}",
                ledger.available_cash_usdc
            ));
        }

        let stop_distance_pct = round_dp(stop_distance_frac * 100.0, 2);
        let take_profit_px =
            compute_initial_take_profit(side, limit_price, atr_opt, asset.tick_size, &self.limits);
        let tp_distance_pct = if take_profit_px > 0.0 {
            round_dp(
                ((take_profit_px - limit_price).abs() / limit_price) * 100.0,
                2,
            )
        } else {
            0.0
        };
        let risk_budget_usdc = if self.limits.risk_per_trade_equity_pct.is_finite()
            && self.limits.risk_per_trade_equity_pct > 0.0
        {
            round_dp(equity * self.limits.risk_per_trade_equity_pct, 4)
        } else {
            0.0
        };

        let achieved_weight = round_dp((target_notional_usdc / equity) * side.sign(), 4);
        let safe_s1 = if w_s1_carry_mom.is_finite() {
            w_s1_carry_mom
        } else {
            0.0
        };
        let safe_s2 = if w_s2_trend_breakout.is_finite() {
            w_s2_trend_breakout
        } else {
            0.0
        };
        let safe_s3 = if w_s3_event_reversion.is_finite() {
            w_s3_event_reversion
        } else {
            0.0
        };
        let raw_sleeve_sum = (safe_s1.abs() + safe_s2.abs() + safe_s3.abs()).max(1e-8);
        let scale = achieved_weight.abs() / raw_sleeve_sum;
        let s1_weight_contrib = round_dp(safe_s1 * scale, 4);
        let s2_weight_contrib = round_dp(safe_s2 * scale, 4);
        let s3_weight_contrib = round_dp(safe_s3 * scale, 4);

        WorkingOrderSizingPlan {
            approved: true,
            rejection_reason: None,
            asset_id: asset.id,
            symbol: asset.symbol.clone(),
            side,
            limit_price,
            requested_quantity,
            target_notional_usdc,
            reserved_margin_usdc,
            committed_notional_usdc: 0.0, // WORKING order strictly reserves margin; 0.0 committed until OPEN
            portfolio_weight: achieved_weight,
            s1_weight_contrib,
            s2_weight_contrib,
            s3_weight_contrib,
            protective_stop_px,
            stop_distance_pct,
            take_profit_px,
            tp_distance_pct,
            risk_budget_usdc,
        }
    }

    /// Promotes a `WORKING` order to `OPEN` upon exchange fill confirmation:
    /// releases `reserved_margin_usdc`, commits `committed_notional_usdc` from `available_cash_usdc`,
    /// computes exchange fees, and sets the initial protective stop price.
    #[allow(clippy::too_many_arguments)]
    pub fn confirm_order_fill_to_open(
        &self,
        side: OrderSide,
        fill_price: f64,
        filled_quantity: f64,
        prior_reserved_margin_usdc: f64,
        is_maker: bool,
        tick_size: f64,
        atr_opt: Option<f64>,
        ledger: &CapitalLedgerState,
    ) -> OpenCommitmentSummary {
        let committed_notional_usdc = compute_committed_notional_usdc(fill_price, filled_quantity);
        let fee_bps = if is_maker {
            self.limits.maker_fee_bps
        } else {
            self.limits.taker_fee_bps
        };
        let fees_paid_usdc = round_dp(committed_notional_usdc * (fee_bps / 10_000.0), 4);
        let protective_stop_px =
            compute_initial_protective_stop(side, fill_price, atr_opt, tick_size, &self.limits);
        let take_profit_px =
            compute_initial_take_profit(side, fill_price, atr_opt, tick_size, &self.limits);

        let committed_margin = compute_reserved_margin_usdc(
            committed_notional_usdc,
            self.limits.target_leverage_multiplier,
        );
        let available_cash_after_usdc = round_dp(
            (ledger.available_cash_usdc + prior_reserved_margin_usdc
                - committed_margin
                - fees_paid_usdc)
                .max(0.0),
            4,
        );

        OpenCommitmentSummary {
            fill_price,
            filled_quantity,
            released_reserved_margin_usdc: round_dp(prior_reserved_margin_usdc, 4),
            committed_notional_usdc,
            available_cash_after_usdc,
            protective_stop_px,
            take_profit_px,
            fees_paid_usdc,
        }
    }

    /// Evaluates the 1-second High-Water Chandelier/ATR trailing stop ratchet on an `OPEN` position.
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_1s_stop_ratchet(
        &self,
        side: OrderSide,
        entry_price: f64,
        mark_price: f64,
        quantity: f64,
        current_stop_px: f64,
        highest_price_seen: f64,
        lowest_price_seen: f64,
        atr_opt: Option<f64>,
        tick_size: f64,
    ) -> StopRatchetEvaluation {
        evaluate_1s_protective_stop(
            side,
            entry_price,
            mark_price,
            quantity,
            current_stop_px,
            highest_price_seen,
            lowest_price_seen,
            atr_opt,
            tick_size,
            &self.limits,
        )
    }

    /// Evaluates both the 1-second High-Water Chandelier/ATR trailing stop ratchet AND take-profit target on an `OPEN` position.
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_1s_stop_and_tp_ratchet(
        &self,
        side: OrderSide,
        entry_price: f64,
        mark_price: f64,
        quantity: f64,
        current_stop_px: f64,
        current_tp_px: f64,
        highest_price_seen: f64,
        lowest_price_seen: f64,
        atr_opt: Option<f64>,
        tick_size: f64,
    ) -> StopRatchetEvaluation {
        evaluate_1s_position_tpsl(
            side,
            entry_price,
            mark_price,
            quantity,
            current_stop_px,
            current_tp_px,
            highest_price_seen,
            lowest_price_seen,
            atr_opt,
            tick_size,
            &self.limits,
        )
    }

    /// Evaluates full 1-second position lifecycle (`SL -> TP -> Stagnation Time-Stop -> Chandelier/BE Ratchet`).
    pub fn evaluate_1s_position_lifecycle(
        &self,
        input: &StopEvaluationInput,
    ) -> StopRatchetEvaluation {
        evaluate_1s_position_lifecycle(input, &self.limits)
    }

    /// Validates aggregate gross leverage (`<= max_gross_leverage_x`) and open stop-loss portfolio heat
    /// (`open_stop_risk_usdc / equity_usdc <= max_portfolio_heat_pct`) prior to admitting a fill.
    pub fn check_portfolio_heat_and_gross_at_fill(
        &self,
        equity_usdc: f64,
        open_notional_usdc: f64,
        open_stop_risk_usdc: f64,
    ) -> Result<()> {
        if !equity_usdc.is_finite()
            || equity_usdc <= 0.0
            || !open_notional_usdc.is_finite()
            || open_notional_usdc < 0.0
            || !open_stop_risk_usdc.is_finite()
            || open_stop_risk_usdc < 0.0
        {
            return Err(AegisError::Validation(format!(
                "Invalid portfolio heat/gross check inputs: equity={equity_usdc}, open_notional={open_notional_usdc}, open_stop_risk={open_stop_risk_usdc}"
            )));
        }

        let gross_lev = open_notional_usdc / equity_usdc;
        let max_gross = if self.limits.max_gross_leverage_x.is_finite()
            && self.limits.max_gross_leverage_x > 0.0
        {
            self.limits.max_gross_leverage_x
        } else {
            2.50
        };
        if gross_lev > max_gross + 1e-9 {
            return Err(AegisError::Validation(format!(
                "PORTFOLIO_GROSS_LEVERAGE_EXCEEDED: gross={gross_lev:.4}x > max={max_gross:.4}x"
            )));
        }

        let heat_pct = open_stop_risk_usdc / equity_usdc;
        let max_heat = if self.limits.max_portfolio_heat_pct.is_finite()
            && self.limits.max_portfolio_heat_pct > 0.0
        {
            self.limits.max_portfolio_heat_pct
        } else {
            0.12
        };
        if heat_pct > max_heat + 1e-9 {
            return Err(AegisError::Validation(format!(
                "PORTFOLIO_HEAT_EXCEEDED: heat={heat_pct:.4} > max={max_heat:.4}"
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_core::RiskTier;

    fn sample_sol_asset() -> ExchangeAsset {
        ExchangeAsset {
            id: 2,
            symbol: "SOL-USDC".to_string(),
            coin: "SOL".to_string(),
            asset_index: 2,
            sz_decimals: 2,
            tick_size: 0.01,
            lot_step_size: 0.01,
            max_leverage: 20.0,
            mark_price: 180.0,
            mid_price: 180.0,
            oracle_price: 180.0,
            funding_rate_hourly: 0.00001,
            day_volume_usdc: 25_000_000.0,
            open_interest_usdc: 8_000_000.0,
            prev_day_price: 170.0,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: true,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn test_order_to_open_capital_reservation_and_commitment() {
        let risk = RiskEngine::default();
        assert_eq!(risk.limits.risk_tier, RiskTier::Extreme);
        assert!((risk.limits.target_leverage_multiplier - 1.85).abs() < 1e-9);

        let sol = sample_sol_asset();
        let initial_ledger = risk.evaluate_ledger(10_000.0, &[]);
        assert_eq!(initial_ledger.available_cash_usdc, 10_000.0);
        assert_eq!(initial_ledger.reserved_order_cash_usdc, 0.0);
        assert_eq!(initial_ledger.committed_notional_usdc, 0.0);

        // 1. Place WORKING order for 20% portfolio weight ($2,000 notional)
        let plan = risk.size_working_order_from_weights(
            &sol,
            0.20,
            0.0,
            0.12,
            0.08,
            0.0,
            &initial_ledger,
            Some(3.0),
        );
        assert!(plan.approved);
        assert_eq!(plan.side, OrderSide::Long);
        assert!(plan.reserved_margin_usdc > 0.0);
        assert_eq!(plan.committed_notional_usdc, 0.0);
        assert!(plan.protective_stop_px < plan.limit_price);
        assert!(plan.take_profit_px > plan.limit_price);

        let working_pos = PositionRecord {
            id: 1,
            asset_id: sol.id,
            client_order_id: "oid-1".to_string(),
            side: "LONG".to_string(),
            status: "WORKING".to_string(),
            fsm_state: "WORKING_ALO".to_string(),
            entry_price: plan.limit_price,
            current_price: plan.limit_price,
            exit_price: None,
            requested_quantity: plan.requested_quantity,
            filled_quantity: 0.0,
            reserved_margin_usdc: plan.reserved_margin_usdc,
            committed_notional_usdc: 0.0,
            portfolio_weight: plan.portfolio_weight,
            s1_weight_contrib: plan.s1_weight_contrib,
            s2_weight_contrib: plan.s2_weight_contrib,
            s3_weight_contrib: plan.s3_weight_contrib,
            protective_stop_px: plan.protective_stop_px,
            entry_r0: (plan.limit_price - plan.protective_stop_px).abs(),
            protective_stop_oid: None,
            take_profit_px: plan.take_profit_px,
            take_profit_oid: None,
            highest_price_seen: plan.limit_price,
            lowest_price_seen: plan.limit_price,
            chase_count: 0,
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            funding_accrued_usdc: 0.0,
            slippage_paid_usdc: 0.0,
            exchange_order_id: None,
            created_at_ms: 1_700_000_000_000,
            opened_at_ms: None,
            closed_at_ms: None,
            updated_at_ms: 1_700_000_000_000,
            exit_reason: None,
        };

        let working_ledger = risk.evaluate_ledger(10_000.0, std::slice::from_ref(&working_pos));
        assert_eq!(
            working_ledger.reserved_order_cash_usdc,
            plan.reserved_margin_usdc
        );
        assert_eq!(working_ledger.committed_notional_usdc, 0.0);
        assert_eq!(working_ledger.working_order_count, 1);

        // 2. Promote WORKING -> OPEN on fill confirmation
        let commit = risk.confirm_order_fill_to_open(
            OrderSide::Long,
            plan.limit_price,
            plan.requested_quantity,
            plan.reserved_margin_usdc,
            true,
            sol.tick_size,
            Some(3.0),
            &working_ledger,
        );
        assert_eq!(
            commit.released_reserved_margin_usdc,
            plan.reserved_margin_usdc
        );
        assert!((commit.committed_notional_usdc - plan.target_notional_usdc).abs() < 1e-2);

        let mut open_pos = working_pos;
        open_pos.status = "OPEN".to_string();
        open_pos.fsm_state = "FILLED_OPEN".to_string();
        open_pos.filled_quantity = plan.requested_quantity;
        open_pos.reserved_margin_usdc = 0.0;
        open_pos.committed_notional_usdc = commit.committed_notional_usdc;

        let open_ledger = risk.evaluate_ledger(10_000.0, &[open_pos]);
        assert_eq!(open_ledger.reserved_order_cash_usdc, 0.0);
        assert_eq!(
            open_ledger.committed_notional_usdc,
            commit.committed_notional_usdc
        );
        assert_eq!(open_ledger.open_long_count, 1);
    }

    #[test]
    fn test_1s_chandelier_stop_ratchet_and_breach() {
        let risk = RiskEngine::default();
        let entry_px = 180.0;
        let atr = 3.0; // stop distance = 2.2 * 3.0 = 6.6 -> initial stop = 173.40
        let init_stop = compute_initial_protective_stop(
            OrderSide::Long,
            entry_px,
            Some(atr),
            0.01,
            &risk.limits,
        );
        assert!((init_stop - 173.40).abs() < 1e-4);

        // Price advances to 190.0 -> stop ratchets upward and locks profit above entry
        let eval1 = risk.evaluate_1s_stop_ratchet(
            OrderSide::Long,
            entry_px,
            190.0,
            10.0,
            init_stop,
            entry_px,
            entry_px,
            Some(atr),
            0.01,
        );
        assert!(!eval1.triggered);
        assert!(eval1.stop_ratcheted);
        assert!(eval1.new_protective_stop_px > entry_px);
        assert_eq!(eval1.highest_price_seen, 190.0);

        // Price dips slightly to 188.0 -> stop never moves backward
        let eval2 = risk.evaluate_1s_stop_ratchet(
            OrderSide::Long,
            entry_px,
            188.0,
            10.0,
            eval1.new_protective_stop_px,
            eval1.highest_price_seen,
            eval1.lowest_price_seen,
            Some(atr),
            0.01,
        );
        assert!(!eval2.triggered);
        assert_eq!(eval2.new_protective_stop_px, eval1.new_protective_stop_px);

        // Price breaches the ratcheted stop -> immediate exit + 1800s post-stop cooldown
        let eval3 = risk.evaluate_1s_stop_ratchet(
            OrderSide::Long,
            entry_px,
            eval2.new_protective_stop_px - 0.50,
            10.0,
            eval2.new_protective_stop_px,
            eval2.highest_price_seen,
            eval2.lowest_price_seen,
            Some(atr),
            0.01,
        );
        assert!(eval3.triggered);
        assert_eq!(eval3.exit_reason.as_deref(), Some("PROTECTIVE_STOP_LONG"));
        assert_eq!(eval3.cooldown_duration_ms, 1_800_000);
    }

    #[test]
    fn test_nan_and_non_finite_inputs_rejected_immediately() {
        let risk = RiskEngine::default();
        let mut sol = sample_sol_asset();
        let ledger = risk.evaluate_ledger(10_000.0, &[]);

        let nan_target = risk.size_working_order_from_weights(
            &sol,
            f64::NAN,
            0.0,
            0.1,
            0.0,
            0.0,
            &ledger,
            Some(3.0),
        );
        assert!(!nan_target.approved);
        assert_eq!(nan_target.requested_quantity, 0.0);

        let nan_current = risk.size_working_order_from_weights(
            &sol,
            0.2,
            f64::NAN,
            0.1,
            0.0,
            0.0,
            &ledger,
            Some(3.0),
        );
        assert!(!nan_current.approved);
        assert_eq!(nan_current.requested_quantity, 0.0);

        sol.mark_price = f64::NAN;
        let nan_mark =
            risk.size_working_order_from_weights(&sol, 0.2, 0.0, 0.1, 0.0, 0.0, &ledger, Some(3.0));
        assert!(!nan_mark.approved);

        let mut bad_ledger = ledger.clone();
        bad_ledger.total_equity_usdc = f64::INFINITY;
        let sol_ok = sample_sol_asset();
        let inf_eq = risk.size_working_order_from_weights(
            &sol_ok,
            0.2,
            0.0,
            0.1,
            0.0,
            0.0,
            &bad_ledger,
            Some(3.0),
        );
        assert!(!inf_eq.approved);

        let mut nan_cash_ledger = ledger;
        nan_cash_ledger.available_cash_usdc = f64::NAN;
        let nan_cash = risk.size_working_order_from_weights(
            &sol_ok,
            0.2,
            0.0,
            0.1,
            0.0,
            0.0,
            &nan_cash_ledger,
            Some(3.0),
        );
        assert!(!nan_cash.approved);
    }

    #[test]
    fn test_conservative_sub_1x_leverage_and_asset_max_leverage_cap() {
        let mut limits = RiskLimitsConfig {
            risk_tier: RiskTier::Conservative,
            target_leverage_multiplier: 0.65,
            max_single_asset_weight: 1.0,
            max_gross_leverage_x: 2.50,
            risk_per_trade_equity_pct: 0.0,
            ..RiskLimitsConfig::default()
        };
        let risk = RiskEngine::new(limits.clone());
        let sol = sample_sol_asset();
        let ledger = risk.evaluate_ledger(10_000.0, &[]);

        // Request 90% weight -> Conservative 0.65x caps notional to $6,500 and reserves $10,000 margin
        let plan_cap = risk.size_working_order_from_weights(
            &sol,
            0.90,
            0.0,
            0.90,
            0.0,
            0.0,
            &ledger,
            Some(3.0),
        );
        assert!(plan_cap.approved);
        assert!(plan_cap.target_notional_usdc <= 6_500.0 + 1e-2);
        assert!(plan_cap.target_notional_usdc >= 6_480.0);
        assert!(plan_cap.reserved_margin_usdc <= 10_000.0 + 1e-4);

        // Two-step sizing under 0.65x: first order 0.40 ($4,000 notional, $6,153.85 margin), second capped at $2,500
        let plan1 = risk.size_working_order_from_weights(
            &sol,
            0.40,
            0.0,
            0.40,
            0.0,
            0.0,
            &ledger,
            Some(3.0),
        );
        assert!(plan1.approved);
        assert!((plan1.target_notional_usdc - 4_000.0).abs() < 20.0);

        let mut ledger_after_1 = ledger;
        ledger_after_1.reserved_order_cash_usdc = plan1.reserved_margin_usdc;
        ledger_after_1.available_cash_usdc =
            (ledger_after_1.total_equity_usdc - plan1.reserved_margin_usdc).max(0.0);

        let plan2 = risk.size_working_order_from_weights(
            &sol,
            0.40,
            0.0,
            0.40,
            0.0,
            0.0,
            &ledger_after_1,
            Some(3.0),
        );
        assert!(plan2.approved);
        assert!(
            plan1.target_notional_usdc + plan2.target_notional_usdc <= 6_500.0 + 1.0,
            "Combined Conservative 0.65x notional exceeded $6,500: {}",
            plan1.target_notional_usdc + plan2.target_notional_usdc
        );

        // Per-asset max_leverage cap (e.g. target 2.50x, asset.max_leverage = 2.0, available_cash = $1,000)
        limits.risk_tier = RiskTier::Custom;
        limits.target_leverage_multiplier = 2.50;
        limits.max_single_asset_weight = 0.50;
        let custom_risk = RiskEngine::new(limits);
        let mut low_lev = sample_sol_asset();
        low_lev.max_leverage = 2.0;
        let cash_tight_ledger = CapitalLedgerState {
            total_equity_usdc: 10_000.0,
            available_cash_usdc: 1_000.0,
            reserved_order_cash_usdc: 0.0,
            committed_notional_usdc: 0.0,
            ..CapitalLedgerState::default()
        };
        let plan_asset_cap = custom_risk.size_working_order_from_weights(
            &low_lev,
            0.30,
            0.0,
            0.30,
            0.0,
            0.0,
            &cash_tight_ledger,
            Some(3.0),
        );
        assert!(plan_asset_cap.approved);
        assert!(
            plan_asset_cap.target_notional_usdc <= 2_000.0 + 1e-2,
            "Asset max_leverage=2.0 should cap $1k cash order at $2,000, got {}",
            plan_asset_cap.target_notional_usdc
        );
    }

    #[test]
    fn test_risk_parity_sizing_interacts_with_weight_and_gross_caps() {
        // 1. Default Extreme tier: 25% single-asset cap, 2.0% per-trade risk budget
        let extreme = RiskEngine::default();
        assert!((extreme.limits.max_single_asset_weight - 0.25).abs() < 1e-9);
        assert!((extreme.limits.risk_per_trade_equity_pct - 0.02).abs() < 1e-9);

        let mut asset = sample_sol_asset();
        asset.mark_price = 100.0;
        asset.mid_price = 100.0;
        asset.oracle_price = 100.0;
        asset.tick_size = 0.01;
        asset.lot_step_size = 0.01;
        let extreme_ledger = extreme.evaluate_ledger(1_000.0, &[]);

        // On 4% stop (atr = None): risk-parity = $20 / 0.04 = $500, capped by max_single_asset_weight (0.25) to $250
        let ext_med = extreme.size_working_order_from_weights(
            &asset,
            0.25,
            0.0,
            0.25,
            0.0,
            0.0,
            &extreme_ledger,
            None,
        );
        assert!(ext_med.approved);
        assert!((ext_med.risk_budget_usdc - 20.0).abs() < 1e-6);
        assert!((ext_med.target_notional_usdc - 250.0).abs() < 1.5);

        // On 10% stop (atr = 10.0 / 2.2): risk-parity = $20 / 0.10 = $200 (< $250 weight cap)
        let ext_wide = extreme.size_working_order_from_weights(
            &asset,
            0.25,
            0.0,
            0.25,
            0.0,
            0.0,
            &extreme_ledger,
            Some(10.0 / 2.2),
        );
        assert!(ext_wide.approved);
        assert!((ext_wide.target_notional_usdc - 200.0).abs() < 1.5);

        // 2. Conservative tier (0.65x): 15% single-asset cap, 0.5% per-trade risk budget
        let mut cons_cfg = ConfigMap::new();
        cons_cfg.insert("risk_tier", "conservative");
        let risk = RiskEngine::from_config_map(&cons_cfg);
        assert!((risk.limits.max_single_asset_weight - 0.15).abs() < 1e-9);
        assert!((risk.limits.risk_per_trade_equity_pct - 0.005).abs() < 1e-9);
        let ledger = risk.evaluate_ledger(1_000.0, &[]);

        // Tight 2% stop (stop_dist = 2.0 -> atr = 2.0 / 2.2):
        // Risk-parity = $5 / 0.02 = $250, capped by max_single_asset_weight (0.15) to $150!
        let tight = risk.size_working_order_from_weights(
            &asset,
            0.15,
            0.0,
            0.15,
            0.0,
            0.0,
            &ledger,
            Some(2.0 / 2.2),
        );
        assert!(tight.approved);
        assert!((tight.risk_budget_usdc - 5.0).abs() < 1e-6);
        assert!((tight.target_notional_usdc - 150.0).abs() < 1.5);

        // Medium 4% stop (atr = None -> default 4%):
        // Risk-parity = $5 / 0.04 = $125 (< $150 weight cap), exact 0.50% equity risk!
        let med =
            risk.size_working_order_from_weights(&asset, 0.15, 0.0, 0.15, 0.0, 0.0, &ledger, None);
        assert!(med.approved);
        assert!((med.target_notional_usdc - 125.0).abs() < 1.5);

        // Wide 8% stop (atr = 8.0 / 2.2):
        // Risk-parity = $5 / 0.08 = $62.50 (< $150 weight cap), exact 0.50% equity risk!
        let wide = risk.size_working_order_from_weights(
            &asset,
            0.15,
            0.0,
            0.15,
            0.0,
            0.0,
            &ledger,
            Some(8.0 / 2.2),
        );
        assert!(wide.approved);
        assert!((wide.target_notional_usdc - 62.5).abs() < 1.5);

        // ACTION_INCREASE with existing same-side weight 0.08 ($80) on 4% stop ($125 cap):
        // Remaining add cap = $125 - $80 = $45!
        let inc =
            risk.size_working_order_from_weights(&asset, 0.15, 0.08, 0.15, 0.0, 0.0, &ledger, None);
        assert!(inc.approved);
        assert!(
            inc.target_notional_usdc <= 45.5 && inc.target_notional_usdc >= 43.0,
            "ACTION_INCREASE must account for existing $80 notional against $125 risk-parity cap, got {}",
            inc.target_notional_usdc
        );
    }

    #[test]
    fn test_eff_min_delta_tier_scaling_and_portfolio_heat_gross_check() {
        let asset = sample_sol_asset();
        // 1. Extreme tier (1.85x): eff_min_delta = 0.025 * 1.85 = 0.04625 -> delta 0.04 rejected, 0.05 approved
        let ext = RiskEngine::default();
        let ext_ledger = ext.evaluate_ledger(10_000.0, &[]);
        let too_small_ext = ext.size_working_order_from_weights(
            &asset,
            0.04,
            0.0,
            0.04,
            0.0,
            0.0,
            &ext_ledger,
            None,
        );
        assert!(!too_small_ext.approved);
        assert!(too_small_ext
            .rejection_reason
            .as_deref()
            .is_some_and(|r| r.contains("DELTA_BELOW_THRESHOLD")));
        let big_enough_ext = ext.size_working_order_from_weights(
            &asset,
            0.05,
            0.0,
            0.05,
            0.0,
            0.0,
            &ext_ledger,
            None,
        );
        assert!(big_enough_ext.approved);

        // Conservative tier (0.65x): eff_min_delta = 0.025 * 0.65 = 0.01625 -> delta 0.02 approved
        let mut cons_cfg = ConfigMap::new();
        cons_cfg.insert("risk_tier", "conservative");
        let cons = RiskEngine::from_config_map(&cons_cfg);
        let cons_ledger = cons.evaluate_ledger(10_000.0, &[]);
        let ok_cons = cons.size_working_order_from_weights(
            &asset,
            0.02,
            0.0,
            0.02,
            0.0,
            0.0,
            &cons_ledger,
            None,
        );
        assert!(ok_cons.approved);

        // Small max_single_asset_weight (0.01) caps eff_min_delta to 0.01 so delta=0.01 is not blocked
        let tiny_cap = RiskEngine::new(RiskLimitsConfig {
            max_single_asset_weight: 0.01,
            ..RiskLimitsConfig::default()
        });
        let tiny_ok = tiny_cap.size_working_order_from_weights(
            &asset,
            0.01,
            0.0,
            0.01,
            0.0,
            0.0,
            &ext_ledger,
            None,
        );
        assert!(tiny_ok.approved);

        // 2. check_portfolio_heat_and_gross_at_fill: boundary passes, breach fails
        assert!(ext
            .check_portfolio_heat_and_gross_at_fill(10_000.0, 25_000.0, 1_200.0)
            .is_ok());
        assert!(ext
            .check_portfolio_heat_and_gross_at_fill(10_000.0, 25_001.0, 1_000.0)
            .is_err());
        assert!(ext
            .check_portfolio_heat_and_gross_at_fill(10_000.0, 20_000.0, 1_201.0)
            .is_err());
        assert!(ext
            .check_portfolio_heat_and_gross_at_fill(0.0, 0.0, 0.0)
            .is_err());
        assert!(ext
            .check_portfolio_heat_and_gross_at_fill(10_000.0, f64::NAN, 100.0)
            .is_err());
        assert!(ext
            .check_portfolio_heat_and_gross_at_fill(10_000.0, 100.0, -1.0)
            .is_err());
    }

    #[test]
    fn test_r14_m8_zero_nan_tick_and_lot_size_fails_closed() {
        let risk = RiskEngine::default();
        let ledger = risk.evaluate_ledger(10_000.0, &[]);

        for bad_tick in [0.0, -0.01, f64::NAN, f64::INFINITY] {
            let mut asset = sample_sol_asset();
            asset.tick_size = bad_tick;
            let plan = risk.size_working_order_from_weights(
                &asset,
                0.10,
                0.0,
                0.10,
                0.0,
                0.0,
                &ledger,
                Some(3.0),
            );
            assert!(!plan.approved);
            assert_eq!(
                plan.rejection_reason.as_deref(),
                Some("INVALID_TICK_OR_LOT_SIZE")
            );
        }

        for bad_lot in [0.0, -0.01, f64::NAN, f64::INFINITY] {
            let mut asset = sample_sol_asset();
            asset.lot_step_size = bad_lot;
            let plan = risk.size_working_order_from_weights(
                &asset,
                0.10,
                0.0,
                0.10,
                0.0,
                0.0,
                &ledger,
                Some(3.0),
            );
            assert!(!plan.approved);
            assert_eq!(
                plan.rejection_reason.as_deref(),
                Some("INVALID_TICK_OR_LOT_SIZE")
            );
        }
    }

    #[test]
    fn test_r14_m12_true_negative_equity_and_ruin_halt_gate() {
        let risk = RiskEngine::default();
        let sol = sample_sol_asset();

        // Below $50 ruin halt floor -> rejected with RUIN_HALT_EQUITY_BELOW_FLOOR
        let low_ledger = risk.evaluate_ledger(40.0, &[]);
        assert_eq!(low_ledger.total_equity_usdc, 40.0);
        assert!(low_ledger.is_ruin_halted(risk.limits.min_equity_halt_usdc));
        let low_plan = risk.size_working_order_from_weights(
            &sol,
            0.10,
            0.0,
            0.10,
            0.0,
            0.0,
            &low_ledger,
            Some(3.0),
        );
        assert!(!low_plan.approved);
        assert_eq!(
            low_plan.rejection_reason.as_deref(),
            Some("RUIN_HALT_EQUITY_BELOW_FLOOR")
        );

        // Negative equity -> tracked truthfully without $1.0 floor and rejected
        let neg_ledger = risk.evaluate_ledger(-250.0, &[]);
        assert_eq!(neg_ledger.total_equity_usdc, -250.0);
        assert!(neg_ledger.is_insolvent);
        let neg_plan = risk.size_working_order_from_weights(
            &sol,
            0.10,
            0.0,
            0.10,
            0.0,
            0.0,
            &neg_ledger,
            Some(3.0),
        );
        assert!(!neg_plan.approved);
        assert_eq!(
            neg_plan.rejection_reason.as_deref(),
            Some("RUIN_HALT_EQUITY_BELOW_FLOOR")
        );
    }
}
