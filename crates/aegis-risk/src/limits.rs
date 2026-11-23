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
    #[serde(default = "default_breakeven_trigger_r")]
    pub breakeven_trigger_r: f64,
    #[serde(default = "default_breakeven_lock_r")]
    pub breakeven_lock_r: f64,
    #[serde(default = "default_stagnation_time_stop_hours")]
    pub stagnation_time_stop_hours: f64,
    #[serde(default = "default_stagnation_min_mfe_r")]
    pub stagnation_min_mfe_r: f64,
    #[serde(default = "default_risk_per_trade_equity_pct")]
    pub risk_per_trade_equity_pct: f64,
    #[serde(default = "default_min_equity_halt_usdc")]
    pub min_equity_halt_usdc: f64,
}

fn default_breakeven_trigger_r() -> f64 {
    0.40
}

fn default_breakeven_lock_r() -> f64 {
    0.05
}

fn default_stagnation_time_stop_hours() -> f64 {
    5.0
}

fn default_stagnation_min_mfe_r() -> f64 {
    0.35
}

fn default_risk_per_trade_equity_pct() -> f64 {
    0.005
}

fn default_min_equity_halt_usdc() -> f64 {
    50.0
}

impl Default for RiskLimitsConfig {
    fn default() -> Self {
        Self {
            risk_tier: RiskTier::Extreme,
            target_leverage_multiplier: 1.85,
            max_gross_leverage_x: 2.50,
            max_single_asset_weight: 0.15,
            min_rebalance_delta_weight: 0.025,
            min_order_notional_usdc: 12.0,
            protective_stop_atr_mult: 2.2,
            protective_stop_default_pct: 0.04,
            take_profit_atr_mult: 2.8,
            take_profit_default_pct: 0.055,
            stop_out_cooldown_sec: 1800,
            maker_fee_bps: 1.5,
            taker_fee_bps: 4.5,
            breakeven_trigger_r: 0.40,
            breakeven_lock_r: 0.05,
            stagnation_time_stop_hours: 5.0,
            stagnation_min_mfe_r: 0.35,
            risk_per_trade_equity_pct: 0.005,
            min_equity_halt_usdc: 50.0,
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
                .get_f64("max_single_asset_weight", 0.15)
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
            breakeven_trigger_r: cfg.get_f64("breakeven_trigger_r", 0.40).clamp(0.15, 1.50),
            breakeven_lock_r: cfg.get_f64("breakeven_lock_r", 0.05).clamp(0.0, 0.50),
            stagnation_time_stop_hours: cfg
                .get_f64("stagnation_time_stop_hours", 5.0)
                .clamp(0.0, 72.0),
            stagnation_min_mfe_r: cfg.get_f64("stagnation_min_mfe_r", 0.35).clamp(0.05, 2.0),
            risk_per_trade_equity_pct: cfg
                .get_f64("risk_per_trade_equity_pct", 0.005)
                .clamp(0.0, 0.05),
            min_equity_halt_usdc: cfg
                .get_f64("min_equity_halt_usdc", 50.0)
                .clamp(1.0, 100_000.0),
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
    #[serde(default)]
    pub is_insolvent: bool,
}

impl CapitalLedgerState {
    /// Returns `true` when equity is non-finite, insolvent (`<= 0`), or strictly below `min_equity_halt_usdc` (`R14-M12`).
    pub fn is_ruin_halted(&self, min_equity_halt_usdc: f64) -> bool {
        let floor = if min_equity_halt_usdc.is_finite() && min_equity_halt_usdc > 0.0 {
            min_equity_halt_usdc
        } else {
            50.0
        };
        !self.total_equity_usdc.is_finite()
            || self.is_insolvent
            || self.total_equity_usdc <= 0.0
            || self.total_equity_usdc < floor
    }
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
            is_insolvent: false,
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

#[allow(clippy::too_many_arguments)]
fn finalize_capital_ledger(
    base_cash_equity_usdc: f64,
    eff_lev: f64,
    reserved_order_cash_usdc: f64,
    committed_notional_usdc: f64,
    net_notional_usdc: f64,
    unrealized_pnl_usdc: f64,
    working_order_count: i32,
    open_long_count: i32,
    open_short_count: i32,
) -> CapitalLedgerState {
    let base_finite = base_cash_equity_usdc.is_finite();
    let base_eq = if base_finite {
        base_cash_equity_usdc
    } else {
        0.0
    };
    let safe_upnl = if unrealized_pnl_usdc.is_finite() {
        unrealized_pnl_usdc
    } else {
        0.0
    };
    let raw_total_eq = base_eq + safe_upnl;
    let is_insolvent = !base_finite || !raw_total_eq.is_finite() || raw_total_eq <= 0.0;
    let total_equity_usdc = if raw_total_eq.is_finite() {
        round_dp(raw_total_eq, 4)
    } else {
        0.0
    };
    let committed_margin_used = committed_notional_usdc / eff_lev;
    let available_cash_usdc = round_dp(
        (total_equity_usdc - committed_margin_used - reserved_order_cash_usdc).max(0.0),
        4,
    );
    let (gross_leverage_x, net_leverage_x) = if total_equity_usdc > 1e-6 {
        (
            round_dp(committed_notional_usdc / total_equity_usdc, 4),
            round_dp(net_notional_usdc / total_equity_usdc, 4),
        )
    } else if committed_notional_usdc > 0.0 {
        let net_sign = if net_notional_usdc > 0.0 {
            1.0
        } else if net_notional_usdc < 0.0 {
            -1.0
        } else {
            0.0
        };
        (999.99, net_sign * 999.99)
    } else {
        (0.0, 0.0)
    };

    CapitalLedgerState {
        total_equity_usdc,
        available_cash_usdc,
        reserved_order_cash_usdc: round_dp(reserved_order_cash_usdc, 4),
        committed_notional_usdc: round_dp(committed_notional_usdc, 4),
        net_notional_usdc: round_dp(net_notional_usdc, 4),
        unrealized_pnl_usdc: round_dp(safe_upnl, 4),
        gross_leverage_x,
        net_leverage_x,
        working_order_count,
        open_long_count,
        open_short_count,
        is_insolvent,
    }
}

/// Computes the unified `CapitalLedgerState` from a base cash/realized equity floor and active `PositionRecord` rows.
/// - `WORKING` orders add to `reserved_order_cash_usdc` without adding to `committed_notional_usdc`.
/// - `OPEN` positions add to `committed_notional_usdc` and `unrealized_pnl_usdc`.
pub fn compute_capital_ledger(
    base_cash_equity_usdc: f64,
    positions: &[PositionRecord],
    leverage_multiplier: f64,
) -> CapitalLedgerState {
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
                let Ok(side) = OrderSide::parse(&pos.side) else {
                    tracing::error!(
                        position_id = pos.id,
                        side = %pos.side,
                        "Skipping position with invalid OrderSide in compute_capital_ledger"
                    );
                    continue;
                };
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

    finalize_capital_ledger(
        base_cash_equity_usdc,
        eff_lev,
        reserved_order_cash_usdc,
        committed_notional_usdc,
        net_notional_usdc,
        unrealized_pnl_usdc,
        working_order_count,
        open_long_count,
        open_short_count,
    )
}

/// Computes `CapitalLedgerState` directly from the MariaDB `v_assets_held` and `v_assets_ordered` view structs.
pub fn compute_capital_ledger_from_views(
    base_cash_equity_usdc: f64,
    held_assets: &[HeldAssetView],
    ordered_assets: &[OrderedAssetView],
    leverage_multiplier: f64,
) -> CapitalLedgerState {
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
        let Ok(side) = OrderSide::parse(&h.side) else {
            tracing::error!(
                position_id = h.position_id,
                side = %h.side,
                "Skipping held asset with invalid OrderSide in compute_capital_ledger_from_views"
            );
            continue;
        };
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

    finalize_capital_ledger(
        base_cash_equity_usdc,
        eff_lev,
        reserved_order_cash_usdc,
        committed_notional_usdc,
        net_notional_usdc,
        unrealized_pnl_usdc,
        ordered_assets.len() as i32,
        open_long_count,
        open_short_count,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_limits_defaults_and_config_map_clamping() {
        let defaults = RiskLimitsConfig::default();
        assert!((defaults.max_single_asset_weight - 0.15).abs() < 1e-9);
        assert!((defaults.protective_stop_atr_mult - 2.2).abs() < 1e-9);
        assert!((defaults.protective_stop_default_pct - 0.04).abs() < 1e-9);
        assert!((defaults.take_profit_atr_mult - 2.8).abs() < 1e-9);
        assert!((defaults.take_profit_default_pct - 0.055).abs() < 1e-9);
        assert!((defaults.breakeven_trigger_r - 0.40).abs() < 1e-9);
        assert!((defaults.breakeven_lock_r - 0.05).abs() < 1e-9);
        assert!((defaults.stagnation_time_stop_hours - 5.0).abs() < 1e-9);
        assert!((defaults.stagnation_min_mfe_r - 0.35).abs() < 1e-9);
        assert!((defaults.risk_per_trade_equity_pct - 0.005).abs() < 1e-9);
        assert!((defaults.min_equity_halt_usdc - 50.0).abs() < 1e-9);

        let empty_cfg = ConfigMap::new();
        let from_empty = RiskLimitsConfig::from_config_map(&empty_cfg);
        assert!((from_empty.max_single_asset_weight - 0.15).abs() < 1e-9);
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

    #[test]
    fn test_r14_m12_ledger_tracks_negative_equity_and_ruin_halt() {
        let neg_flat = compute_capital_ledger(-125.0, &[], 1.85);
        assert_eq!(neg_flat.total_equity_usdc, -125.0);
        assert!(neg_flat.is_insolvent);
        assert!(neg_flat.is_ruin_halted(50.0));
        assert_eq!(neg_flat.gross_leverage_x, 0.0);

        let pos_below_floor = compute_capital_ledger(40.0, &[], 1.85);
        assert_eq!(pos_below_floor.total_equity_usdc, 40.0);
        assert!(!pos_below_floor.is_insolvent);
        assert!(pos_below_floor.is_ruin_halted(50.0));

        let view_held = HeldAssetView {
            position_id: 1,
            asset_id: 1,
            symbol: "BTC-USDC".to_string(),
            coin: "BTC".to_string(),
            client_order_id: "c1".to_string(),
            side: "LONG".to_string(),
            quantity: 0.01,
            committed_notional_usdc: 1000.0,
            portfolio_weight: 0.5,
            entry_price: 100_000.0,
            mark_price: 50_000.0,
            protective_stop_px: 95_000.0,
            protective_stop_oid: None,
            stop_distance_pct: None,
            take_profit_px: 110_000.0,
            take_profit_oid: None,
            tp_distance_pct: None,
            highest_price_seen: 100_000.0,
            lowest_price_seen: 50_000.0,
            unrealized_pnl_usdc: -500.0,
            unrealized_pnl_pct: -50.0,
            realized_pnl_usdc: 0.0,
            funding_accrued_usdc: 0.0,
            fees_paid_usdc: 0.0,
            s1_weight_contrib: 0.5,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            latest_target_weight: 0.5,
            latest_delta_weight: 0.0,
            created_at_ms: 0,
            opened_at_ms: Some(1000),
            updated_at_ms: 1000,
        };
        let view_insolvent = compute_capital_ledger_from_views(200.0, &[view_held], &[], 1.85);
        assert_eq!(view_insolvent.total_equity_usdc, -300.0);
        assert!(view_insolvent.is_insolvent);
        assert!(view_insolvent.is_ruin_halted(50.0));
        assert_eq!(view_insolvent.gross_leverage_x, 999.99);
    }
}
