//! Risk limits, tick/lot precision rounding, and Order-to-Open capital ledger accounting (`aegis-risk::limits`).
//!
//! Enforces the strict Order-to-Open Capital Lifecycle:
//! - `WORKING` orders reserve margin (`reserved_margin_usdc`), keeping `committed_notional_usdc = 0.0`.
//! - `OPEN` holdings commit capital (`committed_notional_usdc`) deducted from `available_cash_usdc`, releasing `reserved_margin_usdc = 0.0`.

use aegis_core::{
    round_dp, ConfigMap, HeldAssetView, OrderSide, OrderedAssetView, PositionRecord,
    PositionStatus, RiskTier,
};
use serde::{Deserialize, Serialize};

/// Risk limits and sizing configuration loaded from `system_config` or defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskLimitsConfig {
    pub risk_tier: RiskTier,
    pub target_leverage_multiplier: f64,
    pub max_gross_leverage_x: f64,
    pub max_single_asset_weight: f64,
    pub min_rebalance_delta_weight: f64,
    pub min_order_notional_usdc: f64,
    pub protective_stop_atr_mult: f64,
    pub protective_stop_default_pct: f64,
    pub take_profit_atr_mult: f64,
    pub take_profit_default_pct: f64,
    pub stop_out_cooldown_sec: i64,
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
}

impl Default for RiskLimitsConfig {
    fn default() -> Self {
        Self {
            risk_tier: RiskTier::Extreme,
            target_leverage_multiplier: 1.85,
            max_gross_leverage_x: 2.50,
            max_single_asset_weight: 0.30,
            min_rebalance_delta_weight: 0.025,
            min_order_notional_usdc: 12.0,
            protective_stop_atr_mult: 2.2,
            protective_stop_default_pct: 0.04,
            take_profit_atr_mult: 2.8,
            take_profit_default_pct: 0.055,
            stop_out_cooldown_sec: 1800,
            maker_fee_bps: 1.5,
            taker_fee_bps: 4.5,
        }
    }
}

impl RiskLimitsConfig {
    pub fn from_config_map(cfg: &ConfigMap) -> Self {
        let risk_tier = cfg.risk_tier();
        let target_leverage_multiplier = cfg.target_leverage_multiplier();
        Self {
            risk_tier,
            target_leverage_multiplier,
            max_gross_leverage_x: cfg.get_f64("max_gross_leverage_x", 2.50).clamp(0.20, 5.00),
            max_single_asset_weight: cfg
                .get_f64("max_single_asset_weight", 0.30)
                .clamp(0.01, 1.00),
            min_rebalance_delta_weight: cfg
                .get_f64("min_rebalance_delta_weight", 0.025)
                .clamp(0.005, 0.25),
            min_order_notional_usdc: cfg.get_f64("min_order_notional_usdc", 12.0).max(10.0),
            protective_stop_atr_mult: cfg
                .get_f64("protective_stop_atr_mult", 2.2)
                .clamp(0.5, 10.0),
            protective_stop_default_pct: cfg
                .get_f64("protective_stop_default_pct", 0.04)
                .clamp(0.005, 0.35),
            take_profit_atr_mult: cfg.get_f64("take_profit_atr_mult", 2.8).clamp(1.0, 20.0),
            take_profit_default_pct: cfg
                .get_f64("take_profit_default_pct", 0.055)
                .clamp(0.01, 0.80),
            stop_out_cooldown_sec: cfg.get_i64("stop_out_cooldown_sec", 1800).max(60),
            maker_fee_bps: cfg.get_f64("maker_fee_bps", 1.5).max(0.0),
            taker_fee_bps: cfg.get_f64("taker_fee_bps", 4.5).max(0.0),
        }
    }
}

/// Real-time capital accounting snapshot enforcing the Order-to-Open Capital Lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapitalLedgerState {
    pub total_equity_usdc: f64,
    pub available_cash_usdc: f64,
    pub reserved_order_cash_usdc: f64,
    pub committed_notional_usdc: f64,
    pub net_notional_usdc: f64,
    pub unrealized_pnl_usdc: f64,
    pub gross_leverage_x: f64,
    pub net_leverage_x: f64,
    pub working_order_count: i32,
    pub open_long_count: i32,
    pub open_short_count: i32,
}

impl Default for CapitalLedgerState {
    fn default() -> Self {
        Self {
            total_equity_usdc: 10_000.0,
            available_cash_usdc: 10_000.0,
            reserved_order_cash_usdc: 0.0,
            committed_notional_usdc: 0.0,
            net_notional_usdc: 0.0,
            unrealized_pnl_usdc: 0.0,
            gross_leverage_x: 0.0,
            net_leverage_x: 0.0,
            working_order_count: 0,
            open_long_count: 0,
            open_short_count: 0,
        }
    }
}

/// Rounds a price to the exchange `tick_size` step (`round_down = true` floors to tick, `false` ceilings to tick).
#[inline]
pub fn round_to_tick(price: f64, tick_size: f64, round_down: bool) -> f64 {
    if !price.is_finite() || price <= 0.0 {
        return 0.0;
    }
    let step = if tick_size.is_finite() && tick_size > 0.0 {
        tick_size
    } else {
        0.0001
    };
    let units = price / step;
    let rounded = if round_down {
        (units + 1e-9).floor() * step
    } else {
        (units - 1e-9).ceil() * step
    };
    round_dp(rounded.max(step), 8)
}

/// Rounds an order quantity down to the exchange `lot_step_size`.
#[inline]
pub fn round_to_lot(quantity: f64, lot_step_size: f64) -> f64 {
    if !quantity.is_finite() || quantity <= 0.0 {
        return 0.0;
    }
    let step = if lot_step_size.is_finite() && lot_step_size > 0.0 {
        lot_step_size
    } else {
        0.01
    };
    let rounded = ((quantity / step) + 1e-9).floor() * step;
    round_dp(rounded.max(0.0), 8)
}

/// Computes the margin (`reserved_margin_usdc`) reserved when placing a `WORKING` order.
#[inline]
pub fn compute_reserved_margin_usdc(order_notional_usdc: f64, leverage_multiplier: f64) -> f64 {
    if !order_notional_usdc.is_finite() || order_notional_usdc <= 0.0 {
        return 0.0;
    }
    let eff_lev = if leverage_multiplier.is_finite() {
        leverage_multiplier.clamp(0.1, 20.0)
    } else {
        1.0
    };
    round_dp(order_notional_usdc / eff_lev, 4)
}

/// Computes the committed notional (`committed_notional_usdc`) spent when a `WORKING` order is confirmed `OPEN`.
#[inline]
pub fn compute_committed_notional_usdc(fill_price: f64, filled_quantity: f64) -> f64 {
    if !fill_price.is_finite()
        || !filled_quantity.is_finite()
        || fill_price <= 0.0
        || filled_quantity <= 0.0
    {
        return 0.0;
    }
    round_dp(fill_price * filled_quantity.abs(), 4)
}

/// Computes the unified `CapitalLedgerState` from a base cash/realized equity floor and active `PositionRecord` rows.
/// - `WORKING` orders add to `reserved_order_cash_usdc` without adding to `committed_notional_usdc`.
/// - `OPEN` positions add to `committed_notional_usdc` and `unrealized_pnl_usdc`.
pub fn compute_capital_ledger(
    base_cash_equity_usdc: f64,
    positions: &[PositionRecord],
    leverage_multiplier: f64,
) -> CapitalLedgerState {
    let base_eq = if base_cash_equity_usdc.is_finite() {
        base_cash_equity_usdc.max(1.0)
    } else {
        1.0
    };
    let eff_lev = if leverage_multiplier.is_finite() {
        leverage_multiplier.clamp(0.1, 20.0)
    } else {
        1.0
    };

    let mut reserved_order_cash_usdc = 0.0;
    let mut committed_notional_usdc = 0.0;
    let mut net_notional_usdc = 0.0;
    let mut unrealized_pnl_usdc = 0.0;
    let mut working_order_count = 0;
    let mut open_long_count = 0;
    let mut open_short_count = 0;

    for pos in positions {
        match PositionStatus::parse(&pos.status) {
            PositionStatus::Working => {
                working_order_count += 1;
                let res = if pos.reserved_margin_usdc > 0.0 {
                    pos.reserved_margin_usdc
                } else {
                    compute_reserved_margin_usdc(pos.entry_price * pos.requested_quantity, eff_lev)
                };
                reserved_order_cash_usdc += res;
            }
            PositionStatus::Open => {
                let side = OrderSide::parse(&pos.side);
                match side {
                    OrderSide::Long => open_long_count += 1,
                    OrderSide::Short => open_short_count += 1,
                }
                let comm = if pos.committed_notional_usdc > 0.0 {
                    pos.committed_notional_usdc
                } else {
                    compute_committed_notional_usdc(pos.entry_price, pos.filled_quantity)
                };
                committed_notional_usdc += comm;
                net_notional_usdc += comm * side.sign();
                unrealized_pnl_usdc += pos.unrealized_pnl_usdc;
            }
            PositionStatus::Closed | PositionStatus::Cancelled => {}
        }
    }

    let total_equity_usdc = round_dp((base_eq + unrealized_pnl_usdc).max(1.0), 4);
    let committed_margin_used = committed_notional_usdc / eff_lev;
    let available_cash_usdc = round_dp(
        (total_equity_usdc - committed_margin_used - reserved_order_cash_usdc).max(0.0),
        4,
    );
    let gross_leverage_x = round_dp(committed_notional_usdc / total_equity_usdc, 4);
    let net_leverage_x = round_dp(net_notional_usdc / total_equity_usdc, 4);

    CapitalLedgerState {
        total_equity_usdc,
        available_cash_usdc,
        reserved_order_cash_usdc: round_dp(reserved_order_cash_usdc, 4),
        committed_notional_usdc: round_dp(committed_notional_usdc, 4),
        net_notional_usdc: round_dp(net_notional_usdc, 4),
        unrealized_pnl_usdc: round_dp(unrealized_pnl_usdc, 4),
        gross_leverage_x,
        net_leverage_x,
        working_order_count,
        open_long_count,
        open_short_count,
    }
}

/// Computes `CapitalLedgerState` directly from the MariaDB `v_assets_held` and `v_assets_ordered` view structs.
pub fn compute_capital_ledger_from_views(
    base_cash_equity_usdc: f64,
    held_assets: &[HeldAssetView],
    ordered_assets: &[OrderedAssetView],
    leverage_multiplier: f64,
) -> CapitalLedgerState {
    let base_eq = if base_cash_equity_usdc.is_finite() {
        base_cash_equity_usdc.max(1.0)
    } else {
        1.0
    };
    let eff_lev = if leverage_multiplier.is_finite() {
        leverage_multiplier.clamp(0.1, 20.0)
    } else {
        1.0
    };

    let reserved_order_cash_usdc: f64 = ordered_assets
        .iter()
        .map(|o| {
            if o.reserved_margin_usdc > 0.0 {
                o.reserved_margin_usdc
            } else {
                compute_reserved_margin_usdc(o.limit_price * o.requested_quantity, eff_lev)
            }
        })
        .sum();

    let mut committed_notional_usdc = 0.0;
    let mut net_notional_usdc = 0.0;
    let mut unrealized_pnl_usdc = 0.0;
    let mut open_long_count = 0;
    let mut open_short_count = 0;

    for h in held_assets {
        let side = OrderSide::parse(&h.side);
        match side {
            OrderSide::Long => open_long_count += 1,
            OrderSide::Short => open_short_count += 1,
        }
        let comm = if h.committed_notional_usdc > 0.0 {
            h.committed_notional_usdc
        } else {
            compute_committed_notional_usdc(h.entry_price, h.quantity)
        };
        committed_notional_usdc += comm;
        net_notional_usdc += comm * side.sign();
        unrealized_pnl_usdc += h.unrealized_pnl_usdc;
    }

    let total_equity_usdc = round_dp((base_eq + unrealized_pnl_usdc).max(1.0), 4);
    let committed_margin_used = committed_notional_usdc / eff_lev;
    let available_cash_usdc = round_dp(
        (total_equity_usdc - committed_margin_used - reserved_order_cash_usdc).max(0.0),
        4,
    );
    let gross_leverage_x = round_dp(committed_notional_usdc / total_equity_usdc, 4);
    let net_leverage_x = round_dp(net_notional_usdc / total_equity_usdc, 4);

    CapitalLedgerState {
        total_equity_usdc,
        available_cash_usdc,
        reserved_order_cash_usdc: round_dp(reserved_order_cash_usdc, 4),
        committed_notional_usdc: round_dp(committed_notional_usdc, 4),
        net_notional_usdc: round_dp(net_notional_usdc, 4),
        unrealized_pnl_usdc: round_dp(unrealized_pnl_usdc, 4),
        gross_leverage_x,
        net_leverage_x,
        working_order_count: ordered_assets.len() as i32,
        open_long_count,
        open_short_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_limits_defaults_and_config_map_clamping() {
        let defaults = RiskLimitsConfig::default();
        assert!((defaults.protective_stop_atr_mult - 2.2).abs() < 1e-9);
        assert!((defaults.protective_stop_default_pct - 0.04).abs() < 1e-9);
        assert!((defaults.take_profit_atr_mult - 2.8).abs() < 1e-9);
        assert!((defaults.take_profit_default_pct - 0.055).abs() < 1e-9);

        let empty_cfg = ConfigMap::new();
        let from_empty = RiskLimitsConfig::from_config_map(&empty_cfg);
        assert!((from_empty.take_profit_atr_mult - 2.8).abs() < 1e-9);
        assert!((from_empty.take_profit_default_pct - 0.055).abs() < 1e-9);

        let mut custom = ConfigMap::new();
        custom.insert("take_profit_atr_mult", "0.2");
        custom.insert("take_profit_default_pct", "1.5");
        let clamped_low_high = RiskLimitsConfig::from_config_map(&custom);
        assert!((clamped_low_high.take_profit_atr_mult - 1.0).abs() < 1e-9);
        assert!((clamped_low_high.take_profit_default_pct - 0.80).abs() < 1e-9);
    }

    #[test]
    fn test_tick_and_lot_rounding() {
        assert_eq!(round_to_tick(100.123, 0.05, true), 100.10);
        assert_eq!(round_to_tick(100.123, 0.05, false), 100.15);
        assert_eq!(round_to_tick(100.10, 0.05, true), 100.10);
        assert_eq!(round_to_tick(100.10, 0.05, false), 100.10);
        assert_eq!(round_to_tick(f64::NAN, 0.01, true), 0.0);
        assert_eq!(round_to_lot(1.299, 0.1), 1.2);
        assert_eq!(round_to_lot(-5.0, 0.1), 0.0);
    }
}
