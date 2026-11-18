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
    compute_initial_protective_stop, evaluate_1s_protective_stop, StopRatchetEvaluation,
};
use aegis_core::{
    round_dp, ConfigMap, ExchangeAsset, HeldAssetView, OrderSide, OrderedAssetView, PositionRecord,
    SleeveSignal,
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

        let raw_px = if asset.mid_price > 0.0 {
            asset.mid_price
        } else {
            asset.mark_price
        };
        let limit_price = round_to_tick(raw_px, asset.tick_size, side.is_buy());

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
        };

        if limit_price <= 0.0 {
            return reject("INVALID_MARK_OR_MID_PRICE".to_string());
        }

        if delta_w.abs() < self.limits.min_rebalance_delta_weight {
            return reject(format!(
                "DELTA_BELOW_THRESHOLD: |delta_w|={:.4} < min={:.4}",
                delta_w.abs(),
                self.limits.min_rebalance_delta_weight
            ));
        }

        let equity = ledger.total_equity_usdc.max(1.0);
        let max_gross_notional = equity * self.limits.max_gross_leverage_x;
        let existing_reserved_notional =
            ledger.reserved_order_cash_usdc * self.limits.target_leverage_multiplier.max(1.0);
        let remaining_gross_notional =
            (max_gross_notional - ledger.committed_notional_usdc - existing_reserved_notional)
                .max(0.0);

        if remaining_gross_notional < self.limits.min_order_notional_usdc {
            return reject(format!(
                "MAX_GROSS_LEVERAGE_REACHED: remaining_notional={remaining_gross_notional:.2} < min={:.2}",
                self.limits.min_order_notional_usdc
            ));
        }

        let max_notional_from_cash =
            ledger.available_cash_usdc * self.limits.target_leverage_multiplier.max(1.0);
        let desired_notional = equity * delta_w.abs();
        let capped_notional = desired_notional
            .min(remaining_gross_notional)
            .min(max_notional_from_cash)
            .min(equity * self.limits.max_single_asset_weight);

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

        let protective_stop_px = compute_initial_protective_stop(
            side,
            limit_price,
            atr_opt,
            asset.tick_size,
            &self.limits,
        );
        let stop_distance_pct = if protective_stop_px > 0.0 {
            round_dp(
                ((limit_price - protective_stop_px).abs() / limit_price) * 100.0,
                2,
            )
        } else {
            0.0
        };

        let achieved_weight = round_dp((target_notional_usdc / equity) * side.sign(), 4);
        let raw_sleeve_sum =
            (w_s1_carry_mom.abs() + w_s2_trend_breakout.abs() + w_s3_event_reversion.abs())
                .max(1e-8);
        let scale = achieved_weight.abs() / raw_sleeve_sum;
        let s1_weight_contrib = round_dp(w_s1_carry_mom * scale, 4);
        let s2_weight_contrib = round_dp(w_s2_trend_breakout * scale, 4);
        let s3_weight_contrib = round_dp(w_s3_event_reversion * scale, 4);

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
            protective_stop_oid: None,
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
}
