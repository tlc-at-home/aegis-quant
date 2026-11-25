//! Initial Protective Stop & 1-Second High-Water Chandelier/ATR Trailing Stop Ratchet (`aegis-risk::stops`).

use crate::limits::{round_to_tick, RiskLimitsConfig};
use aegis_core::indicators::compute_atr as core_compute_atr;
use aegis_core::{round_dp, OhlcvBar, OrderSide};
use serde::{Deserialize, Serialize};

/// Input bundle for single-source 1-second position lifecycle evaluation (`evaluate_1s_position_lifecycle`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StopEvaluationInput {
    pub side: OrderSide,
    pub entry_price: f64,
    pub mark_price: f64,
    pub quantity: f64,
    pub current_stop_px: f64,
    pub current_tp_px: f64,
    pub highest_price_seen: f64,
    pub lowest_price_seen: f64,
    pub atr_opt: Option<f64>,
    pub tick_size: f64,
    pub check_tp: bool,
    pub held_duration_ms: Option<i64>,
    #[serde(default)]
    pub entry_r0: Option<f64>,
}

/// Result of a 1-second protective stop ratchet & take-profit evaluation on an `OPEN` position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopRatchetEvaluation {
    pub triggered: bool,
    pub new_protective_stop_px: f64,
    pub stop_ratcheted: bool,
    pub take_profit_px: f64,
    pub highest_price_seen: f64,
    pub lowest_price_seen: f64,
    pub stop_distance_pct: f64,
    pub tp_distance_pct: f64,
    pub unrealized_pnl_usdc: f64,
    pub exit_reason: Option<String>,
    pub cooldown_duration_ms: i64,
}

#[inline]
fn tick_step(tick_size: f64) -> f64 {
    if tick_size.is_finite() && tick_size > 0.0 {
        tick_size.max(1e-8)
    } else {
        1e-4
    }
}

/// Computes Average True Range (ATR) over `period` OHLCV bars with a safe price-fraction floor.
/// Delegates to `aegis_core::indicators::compute_atr` (`R14-M28`).
pub fn compute_atr_from_bars(
    bars: &[OhlcvBar],
    period: usize,
    reference_price: f64,
    default_pct: f64,
) -> f64 {
    let fallback_pct = default_pct.max(0.005) / 2.2;
    core_compute_atr(
        bars,
        period,
        reference_price,
        Some(0.004),
        Some(fallback_pct),
    )
}

/// Computes the effective stop distance in price units given `entry_price`, optional `atr_value`, and `RiskLimitsConfig`.
pub fn resolve_stop_distance(
    reference_price: f64,
    atr_opt: Option<f64>,
    tick_size: f64,
    limits: &RiskLimitsConfig,
) -> f64 {
    let ref_px = if reference_price.is_finite() {
        reference_price.max(1e-8)
    } else {
        1.0
    };
    let step = tick_step(tick_size);
    let min_stop_dist = (ref_px * 0.008).max(step * 2.0);
    let raw_dist = match atr_opt.filter(|v| v.is_finite() && *v > 0.0) {
        Some(atr) => atr * limits.protective_stop_atr_mult,
        None => ref_px * limits.protective_stop_default_pct,
    };
    raw_dist.max(min_stop_dist)
}

/// Computes the effective take-profit distance in price units given `reference_price`, optional `atr_value`, and `RiskLimitsConfig`.
pub fn resolve_take_profit_distance(
    reference_price: f64,
    atr_opt: Option<f64>,
    tick_size: f64,
    limits: &RiskLimitsConfig,
) -> f64 {
    let ref_px = if reference_price.is_finite() {
        reference_price.max(1e-8)
    } else {
        1.0
    };
    let step = tick_step(tick_size);
    let min_tp_dist = (ref_px * 0.012).max(step * 4.0);
    let raw_dist = match atr_opt.filter(|v| v.is_finite() && *v > 0.0) {
        Some(atr) => atr * limits.take_profit_atr_mult,
        None => ref_px * limits.take_profit_default_pct,
    };
    let stop_floor = resolve_stop_distance(ref_px, atr_opt, tick_size, limits) * 1.25;
    raw_dist.max(min_tp_dist).max(stop_floor)
}

/// Computes the initial protective stop price for a newly placed `WORKING` or newly filled `OPEN` position.
pub fn compute_initial_protective_stop(
    side: OrderSide,
    entry_price: f64,
    atr_opt: Option<f64>,
    tick_size: f64,
    limits: &RiskLimitsConfig,
) -> f64 {
    if !entry_price.is_finite() || entry_price <= 0.0 {
        return 0.0;
    }
    let dist = resolve_stop_distance(entry_price, atr_opt, tick_size, limits);
    let step = tick_step(tick_size);
    match side {
        OrderSide::Long => round_to_tick((entry_price - dist).max(step), tick_size, true),
        OrderSide::Short => round_to_tick(entry_price + dist, tick_size, false),
    }
}

/// Computes the initial take-profit target price for a newly placed `WORKING` or newly filled `OPEN` position.
pub fn compute_initial_take_profit(
    side: OrderSide,
    entry_price: f64,
    atr_opt: Option<f64>,
    tick_size: f64,
    limits: &RiskLimitsConfig,
) -> f64 {
    if !entry_price.is_finite() || entry_price <= 0.0 {
        return 0.0;
    }
    let dist = resolve_take_profit_distance(entry_price, atr_opt, tick_size, limits);
    let step = tick_step(tick_size);
    match side {
        OrderSide::Long => round_to_tick(entry_price + dist, tick_size, false),
        OrderSide::Short => round_to_tick((entry_price - dist).max(step), tick_size, true),
    }
}

/// Returns `(trail_mult, lock_mult)` in multiples of `stop_dist` for stepped Chandelier profit-locking:
/// - `< breakeven_trigger_r * stop_dist` (`0.40R` default): `None` (initial stop untouched)
/// - `[breakeven_trigger_r, 0.60R)`: `Some((0.85, breakeven_lock_r))` (`+0.05R` lock)
/// - `[0.60R, 1.00R)`: `Some((0.75, 0.10))`
/// - `[1.00R, 1.60R)`: `Some((0.55, 0.45))`
/// - `>= 1.60R`: `Some((0.40, 1.00))`
#[inline]
fn stepped_chandelier_params(
    excursion: f64,
    stop_dist: f64,
    limits: &RiskLimitsConfig,
) -> Option<(f64, f64)> {
    let tol = stop_dist * 1e-9;
    let be_trigger = if limits.breakeven_trigger_r.is_finite() {
        limits.breakeven_trigger_r.clamp(0.15, 1.50)
    } else {
        0.40
    };
    let be_lock = if limits.breakeven_lock_r.is_finite() {
        limits.breakeven_lock_r.clamp(0.0, 0.50)
    } else {
        0.05
    };
    if excursion + tol < 0.6 * stop_dist {
        if excursion + tol >= be_trigger * stop_dist {
            Some((0.85, be_lock))
        } else {
            None
        }
    } else if excursion + tol < 1.0 * stop_dist {
        if excursion + tol >= be_trigger * stop_dist {
            Some((0.75, 0.10_f64.max(be_lock)))
        } else {
            None
        }
    } else if excursion + tol < 1.6 * stop_dist {
        Some((0.55, 0.45))
    } else {
        Some((0.40, 1.00))
    }
}

/// Evaluates the 1-second High-Water Chandelier/ATR trailing stop ratchet on an `OPEN` holding.
/// - Ratchets the stop monotonically in the direction of profit (`new_stop >= current_stop` for `LONG`,
///   `new_stop <= current_stop` for `SHORT`).
/// - Applies stepped profit locks and progressively tighter trailing stops at `0.4x`, `0.6x`, `1.0x`, and `1.6x` stop distance.
/// - Detects immediate stop breach when `mark_price <= active_stop` (`LONG`) or `mark_price >= active_stop` (`SHORT`).
#[allow(clippy::too_many_arguments)]
pub fn evaluate_1s_protective_stop(
    side: OrderSide,
    entry_price: f64,
    mark_price: f64,
    quantity: f64,
    current_stop_px: f64,
    highest_price_seen: f64,
    lowest_price_seen: f64,
    atr_opt: Option<f64>,
    tick_size: f64,
    limits: &RiskLimitsConfig,
) -> StopRatchetEvaluation {
    let input = StopEvaluationInput {
        side,
        entry_price,
        mark_price,
        quantity,
        current_stop_px,
        current_tp_px: 0.0,
        highest_price_seen,
        lowest_price_seen,
        atr_opt,
        tick_size,
        check_tp: false,
        held_duration_ms: None,
        entry_r0: None,
    };
    evaluate_1s_position_lifecycle(&input, limits)
}

/// Evaluates both the 1-second High-Water Chandelier/ATR trailing stop ratchet AND the take-profit target on an `OPEN` holding.
#[allow(clippy::too_many_arguments)]
pub fn evaluate_1s_position_tpsl(
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
    limits: &RiskLimitsConfig,
) -> StopRatchetEvaluation {
    let input = StopEvaluationInput {
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
        check_tp: true,
        held_duration_ms: None,
        entry_r0: None,
    };
    evaluate_1s_position_lifecycle(&input, limits)
}

/// Single-source 1-second position lifecycle evaluation (`P1-Strat`):
/// Precedence:
/// 1. Protective stop breach (`PROTECTIVE_STOP_LONG` | `PROTECTIVE_STOP_SHORT`)
/// 2. Take-profit hit (`TAKE_PROFIT_LONG` | `TAKE_PROFIT_SHORT`)
/// 3. Stagnation time-stop (`STAGNATION_TIME_STOP_LONG` | `STAGNATION_TIME_STOP_SHORT`, 600s cooldown)
/// 4. Monotonic Chandelier / breakeven stop ratchet
pub fn evaluate_1s_position_lifecycle(
    input: &StopEvaluationInput,
    limits: &RiskLimitsConfig,
) -> StopRatchetEvaluation {
    let side = input.side;
    let entry_price = input.entry_price;
    let mark_price = input.mark_price;
    let quantity = input.quantity;
    let current_stop_px = input.current_stop_px;
    let current_tp_px = input.current_tp_px;
    let highest_price_seen = input.highest_price_seen;
    let lowest_price_seen = input.lowest_price_seen;
    let atr_opt = input.atr_opt;
    let tick_size = input.tick_size;
    let check_tp = input.check_tp;

    if !mark_price.is_finite()
        || mark_price <= 0.0
        || !entry_price.is_finite()
        || entry_price <= 0.0
    {
        return StopRatchetEvaluation {
            triggered: false,
            new_protective_stop_px: if current_stop_px.is_finite() && current_stop_px > 0.0 {
                current_stop_px
            } else {
                0.0
            },
            stop_ratcheted: false,
            take_profit_px: if current_tp_px.is_finite() && current_tp_px > 0.0 {
                current_tp_px
            } else {
                0.0
            },
            highest_price_seen: if highest_price_seen.is_finite() {
                highest_price_seen
            } else {
                0.0
            },
            lowest_price_seen: if lowest_price_seen.is_finite() {
                lowest_price_seen
            } else {
                0.0
            },
            stop_distance_pct: 0.0,
            tp_distance_pct: 0.0,
            unrealized_pnl_usdc: 0.0,
            exit_reason: None,
            cooldown_duration_ms: 0,
        };
    }

    let ref_mark = mark_price;
    let qty = if quantity.is_finite() {
        quantity.abs()
    } else {
        0.0
    };
    let unrealized_pnl_usdc = round_dp((ref_mark - entry_price) * qty * side.sign(), 4);
    let stop_dist = resolve_stop_distance(ref_mark, atr_opt, tick_size, limits);
    let r0 = input
        .entry_r0
        .filter(|r| r.is_finite() && *r > 1e-12)
        .unwrap_or(stop_dist);
    let init_stop = compute_initial_protective_stop(side, entry_price, atr_opt, tick_size, limits);
    let init_tp = compute_initial_take_profit(side, entry_price, atr_opt, tick_size, limits);
    let step = tick_step(tick_size);
    let min_buf = (stop_dist * 0.25).max(step * 4.0);

    let active_stop = if current_stop_px.is_finite() && current_stop_px > 0.0 {
        current_stop_px
    } else {
        init_stop
    };
    let active_tp = if current_tp_px.is_finite() && current_tp_px > 0.0 {
        current_tp_px
    } else {
        init_tp
    };
    let new_highest = (if highest_price_seen.is_finite() {
        highest_price_seen
    } else {
        entry_price
    })
    .max(entry_price)
    .max(ref_mark);
    let new_lowest = if lowest_price_seen.is_finite() && lowest_price_seen > 0.0 {
        lowest_price_seen.min(entry_price).min(ref_mark)
    } else {
        entry_price.min(ref_mark)
    };

    let sl_hit = match side {
        OrderSide::Long => active_stop > 0.0 && ref_mark <= active_stop,
        OrderSide::Short => active_stop > 0.0 && ref_mark >= active_stop,
    };
    if sl_hit {
        let tp_dist_pct = round_dp(((active_tp - ref_mark).abs() / ref_mark) * 100.0, 2);
        let reason = match side {
            OrderSide::Long => "PROTECTIVE_STOP_LONG",
            OrderSide::Short => "PROTECTIVE_STOP_SHORT",
        };
        return StopRatchetEvaluation {
            triggered: true,
            new_protective_stop_px: active_stop,
            stop_ratcheted: false,
            take_profit_px: active_tp,
            highest_price_seen: new_highest,
            lowest_price_seen: new_lowest,
            stop_distance_pct: 0.0,
            tp_distance_pct: tp_dist_pct,
            unrealized_pnl_usdc,
            exit_reason: Some(reason.to_string()),
            cooldown_duration_ms: limits.stop_out_cooldown_sec * 1000,
        };
    }

    let tp_hit = check_tp
        && active_tp > 0.0
        && match side {
            OrderSide::Long => ref_mark >= active_tp,
            OrderSide::Short => ref_mark <= active_tp,
        };
    if tp_hit {
        let stop_dist_pct = round_dp(((ref_mark - active_stop).abs() / ref_mark) * 100.0, 2);
        let reason = match side {
            OrderSide::Long => "TAKE_PROFIT_LONG",
            OrderSide::Short => "TAKE_PROFIT_SHORT",
        };
        return StopRatchetEvaluation {
            triggered: true,
            new_protective_stop_px: active_stop,
            stop_ratcheted: false,
            take_profit_px: active_tp,
            highest_price_seen: new_highest,
            lowest_price_seen: new_lowest,
            stop_distance_pct: stop_dist_pct,
            tp_distance_pct: 0.0,
            unrealized_pnl_usdc,
            exit_reason: Some(reason.to_string()),
            cooldown_duration_ms: 0,
        };
    }

    let excursion = match side {
        OrderSide::Long => (new_highest - entry_price).max(0.0),
        OrderSide::Short => (entry_price - new_lowest).max(0.0),
    };

    // 3. Stagnation Time-Stop (`P1-Strat`): exit low-MFE drifting positions after `stagnation_time_stop_hours`
    if limits.stagnation_time_stop_hours.is_finite() && limits.stagnation_time_stop_hours > 0.0 {
        let min_held_ms = (limits.stagnation_time_stop_hours * 3_600_000.0).round() as i64;
        if input
            .held_duration_ms
            .is_some_and(|held_ms| held_ms >= min_held_ms)
        {
            let mfe_r = excursion / r0.max(1e-8);
            let cur_r = ((ref_mark - entry_price) * side.sign()) / r0.max(1e-8);
            if mfe_r < limits.stagnation_min_mfe_r && cur_r <= 0.15 {
                let stop_dist_pct = round_dp(
                    (((ref_mark - active_stop) * side.sign()).max(0.0) / ref_mark) * 100.0,
                    2,
                );
                let tp_dist_pct = round_dp(
                    (((active_tp - ref_mark) * side.sign()).max(0.0) / ref_mark) * 100.0,
                    2,
                );
                let reason = match side {
                    OrderSide::Long => "STAGNATION_TIME_STOP_LONG",
                    OrderSide::Short => "STAGNATION_TIME_STOP_SHORT",
                };
                return StopRatchetEvaluation {
                    triggered: true,
                    new_protective_stop_px: active_stop,
                    stop_ratcheted: false,
                    take_profit_px: active_tp,
                    highest_price_seen: new_highest,
                    lowest_price_seen: new_lowest,
                    stop_distance_pct: stop_dist_pct,
                    tp_distance_pct: tp_dist_pct,
                    unrealized_pnl_usdc,
                    exit_reason: Some(reason.to_string()),
                    cooldown_duration_ms: 600_000,
                };
            }
        }
    }

    // 4. Monotonic Chandelier / Breakeven Ratchet
    let (ratcheted_stop, stop_ratcheted) = match side {
        OrderSide::Long => {
            let max_safe_stop = (ref_mark - min_buf).max(step);
            let mut ratcheted = active_stop;
            if let Some((trail_mult, lock_mult)) = stepped_chandelier_params(excursion, r0, limits)
            {
                if new_highest > entry_price + step && active_stop < max_safe_stop {
                    let profit_lock = entry_price + lock_mult * r0;
                    let candidate = (new_highest - trail_mult * stop_dist).max(profit_lock);
                    let capped_candidate = candidate.min(max_safe_stop);
                    let rounded_candidate = round_to_tick(capped_candidate, tick_size, true);
                    if rounded_candidate > active_stop + step * 0.5
                        && rounded_candidate <= max_safe_stop + 1e-12
                    {
                        ratcheted = rounded_candidate;
                    }
                }
            }
            (ratcheted, ratcheted > active_stop + step * 0.5)
        }
        OrderSide::Short => {
            let min_safe_stop = ref_mark + min_buf;
            let mut ratcheted = active_stop;
            if let Some((trail_mult, lock_mult)) = stepped_chandelier_params(excursion, r0, limits)
            {
                if new_lowest > 0.0
                    && new_lowest < entry_price - step
                    && active_stop > min_safe_stop
                {
                    let profit_lock = (entry_price - lock_mult * r0).max(step);
                    let candidate = (new_lowest + trail_mult * stop_dist).min(profit_lock);
                    let capped_candidate = candidate.max(min_safe_stop);
                    let rounded_candidate = round_to_tick(capped_candidate, tick_size, false);
                    if rounded_candidate + step * 0.5 < active_stop
                        && rounded_candidate >= min_safe_stop - 1e-12
                    {
                        ratcheted = rounded_candidate;
                    }
                }
            }
            (ratcheted, ratcheted + step * 0.5 < active_stop)
        }
    };

    let dist_pct = round_dp(
        (((ref_mark - ratcheted_stop) * side.sign()).max(0.0) / ref_mark) * 100.0,
        2,
    );
    let tp_dist_pct = round_dp(
        (((active_tp - ref_mark) * side.sign()).max(0.0) / ref_mark) * 100.0,
        2,
    );

    StopRatchetEvaluation {
        triggered: false,
        new_protective_stop_px: ratcheted_stop,
        stop_ratcheted,
        take_profit_px: active_tp,
        highest_price_seen: new_highest,
        lowest_price_seen: new_lowest,
        stop_distance_pct: dist_pct,
        tp_distance_pct: tp_dist_pct,
        unrealized_pnl_usdc,
        exit_reason: None,
        cooldown_duration_ms: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_non_finite_and_zero_mark_guard_preserves_state() {
        let limits = RiskLimitsConfig::default();
        for bad_mark in [0.0, -10.0, f64::NAN, f64::INFINITY] {
            let eval = evaluate_1s_position_tpsl(
                OrderSide::Long,
                100.0,
                bad_mark,
                5.0,
                104.0,
                115.0,
                108.0,
                99.0,
                Some(2.0),
                0.01,
                &limits,
            );
            assert!(!eval.triggered);
            assert!(!eval.stop_ratcheted);
            assert_eq!(eval.new_protective_stop_px, 104.0);
            assert_eq!(eval.take_profit_px, 115.0);
            assert_eq!(eval.highest_price_seen, 108.0);
            assert_eq!(eval.lowest_price_seen, 99.0);
        }

        let bad_entry = evaluate_1s_position_tpsl(
            OrderSide::Short,
            f64::NAN,
            100.0,
            5.0,
            105.0,
            90.0,
            101.0,
            98.0,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(!bad_entry.triggered);
        assert_eq!(bad_entry.new_protective_stop_px, 105.0);
        assert_eq!(bad_entry.take_profit_px, 90.0);
    }

    #[test]
    fn test_underwater_position_never_snaps_trailing_stop_on_atr_compression() {
        let limits = RiskLimitsConfig::default();
        // LONG underwater: entry 100, current mark 96, active stop 92, highest_price_seen never exceeded entry
        // Even if ATR shrinks sharply (stop_dist = 1.0 -> 100 - 1 = 99 > mark 96), stop must NOT snap to mark
        let long_eval = evaluate_1s_position_tpsl(
            OrderSide::Long,
            100.0,
            96.0,
            10.0,
            92.0,
            115.0,
            100.0,
            95.5,
            Some(0.45),
            0.01,
            &limits,
        );
        assert!(!long_eval.triggered);
        assert!(!long_eval.stop_ratcheted);
        assert_eq!(long_eval.new_protective_stop_px, 92.0);

        // SHORT underwater: entry 100, current mark 104, active stop 108, lowest_price_seen never went below entry
        let short_eval = evaluate_1s_position_tpsl(
            OrderSide::Short,
            100.0,
            104.0,
            10.0,
            108.0,
            85.0,
            104.5,
            100.0,
            Some(0.45),
            0.01,
            &limits,
        );
        assert!(!short_eval.triggered);
        assert!(!short_eval.stop_ratcheted);
        assert_eq!(short_eval.new_protective_stop_px, 108.0);
    }

    #[test]
    fn test_in_profit_ratchet_enforces_min_safety_buffer() {
        let limits = RiskLimitsConfig::default();
        // LONG: entry 100, highest 105 (excursion 5.0 in [1.0R, 1.6R) -> trail 0.55*4.4 = 2.42 -> 102.58),
        // pulled back to mark 101, atr 2.0 (stop_dist = 4.4, min_buf = 1.1) -> must clamp to 101 - 1.1 = 99.9!
        let eval_long = evaluate_1s_position_tpsl(
            OrderSide::Long,
            100.0,
            101.0,
            10.0,
            95.6,
            120.0,
            105.0,
            99.5,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(!eval_long.triggered);
        assert!(eval_long.stop_ratcheted);
        assert!(eval_long.new_protective_stop_px <= 101.0 - 1.1 + 1e-9);
        assert!(eval_long.new_protective_stop_px > 95.6);

        // If active_stop is already inside min_buf (e.g. 100.2 > 99.9), do NOT tighten further
        let eval_skip = evaluate_1s_position_tpsl(
            OrderSide::Long,
            100.0,
            101.0,
            10.0,
            100.2,
            120.0,
            105.0,
            99.5,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(!eval_skip.triggered);
        assert!(!eval_skip.stop_ratcheted);
        assert_eq!(eval_skip.new_protective_stop_px, 100.2);

        // SHORT: entry 100, lowest 95 (in profit), bounce back to mark 99, atr 2.0 (stop_dist = 4.4, min_buf = 1.1)
        let eval_short = evaluate_1s_position_tpsl(
            OrderSide::Short,
            100.0,
            99.0,
            10.0,
            104.4,
            80.0,
            100.5,
            95.0,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(!eval_short.triggered);
        assert!(eval_short.stop_ratcheted);
        assert!(eval_short.new_protective_stop_px >= 99.0 + 1.1 - 1e-9);
        assert!(eval_short.new_protective_stop_px < 104.4);
    }

    #[test]
    fn test_short_active_stop_trigger_guard() {
        let limits = RiskLimitsConfig::default();
        // With current_stop_px = 0.0, fallback init_stop > entry protects SHORT from firing at mark == entry
        let eval_init = evaluate_1s_position_tpsl(
            OrderSide::Short,
            100.0,
            100.0,
            5.0,
            0.0,
            85.0,
            100.0,
            100.0,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(!eval_init.triggered);
        assert!(eval_init.new_protective_stop_px > 100.0);

        // With active_stop > 0.0 and mark >= active_stop, SHORT triggers
        let eval_hit = evaluate_1s_position_tpsl(
            OrderSide::Short,
            100.0,
            105.0,
            5.0,
            104.5,
            85.0,
            105.0,
            100.0,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(eval_hit.triggered);
        assert_eq!(
            eval_hit.exit_reason.as_deref(),
            Some("PROTECTIVE_STOP_SHORT")
        );
    }

    #[test]
    fn test_resolve_take_profit_distance_2_8x_atr_and_fallback() {
        let limits = RiskLimitsConfig::default();
        // With ATR=2.0 at ref_px=100: stop_dist=4.4, stop_floor=4.4*1.25=5.5, raw_tp=2.0*2.8=5.6 (not overridden!)
        let tp_atr = resolve_take_profit_distance(100.0, Some(2.0), 0.01, &limits);
        assert!((tp_atr - 5.6).abs() < 1e-9, "got {tp_atr}");

        let long_tp = compute_initial_take_profit(OrderSide::Long, 100.0, Some(2.0), 0.01, &limits);
        let short_tp =
            compute_initial_take_profit(OrderSide::Short, 100.0, Some(2.0), 0.01, &limits);
        assert!((long_tp - 105.60).abs() < 1e-9);
        assert!((short_tp - 94.40).abs() < 1e-9);

        // With ATR=None: default_pct=5.5% > stop_floor (4.0% * 1.25 = 5.0%)
        let tp_fallback = resolve_take_profit_distance(100.0, None, 0.01, &limits);
        assert!((tp_fallback - 5.5).abs() < 1e-9, "got {tp_fallback}");

        // Tiny ATR hits min_tp_dist = 1.2% (1.2 at 100.0) when stop_floor = 0.8% * 1.25 = 1.0% < 1.2%
        let tp_floor = resolve_take_profit_distance(100.0, Some(0.1), 0.01, &limits);
        assert!((tp_floor - 1.2).abs() < 1e-9, "got {tp_floor}");

        // 2.8x ATR TP triggers cleanly in evaluate_1s_position_tpsl
        let tp_hit = evaluate_1s_position_tpsl(
            OrderSide::Long,
            100.0,
            105.60,
            5.0,
            95.60,
            0.0,
            105.60,
            99.8,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(tp_hit.triggered);
        assert_eq!(tp_hit.exit_reason.as_deref(), Some("TAKE_PROFIT_LONG"));
        assert!((tp_hit.take_profit_px - 105.60).abs() < 1e-9);
    }

    #[test]
    fn test_stepped_chandelier_all_tiers_and_tick_rounding_long_and_short() {
        let limits = RiskLimitsConfig::default();
        // entry=100, atr=2.0 -> stop_dist=4.4, min_buf=1.1
        // Tier 0 (< 0.40R = 1.76): excursion = 0.30R (1.32) -> initial stop untouched
        let l0 = evaluate_1s_position_tpsl(
            OrderSide::Long,
            100.0,
            101.32,
            5.0,
            95.60,
            120.0,
            101.32,
            99.5,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(!l0.stop_ratcheted);
        assert_eq!(l0.new_protective_stop_px, 95.60);

        let s0 = evaluate_1s_position_tpsl(
            OrderSide::Short,
            100.0,
            98.68,
            5.0,
            104.40,
            80.0,
            100.5,
            98.68,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(!s0.stop_ratcheted);
        assert_eq!(s0.new_protective_stop_px, 104.40);

        // Breakeven Tier ([0.40R, 0.60R)): excursion=0.45R (1.98) -> lock entry +- 0.05R = +-0.22 (100.22 / 99.78)
        let l_be = evaluate_1s_position_tpsl(
            OrderSide::Long,
            100.0,
            101.98,
            5.0,
            95.60,
            120.0,
            101.98,
            99.5,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(l_be.stop_ratcheted);
        assert!((l_be.new_protective_stop_px - 100.22).abs() < 1e-9);

        let s_be = evaluate_1s_position_tpsl(
            OrderSide::Short,
            100.0,
            98.02,
            5.0,
            104.40,
            80.0,
            100.5,
            98.02,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(s_be.stop_ratcheted);
        assert!((s_be.new_protective_stop_px - 99.78).abs() < 1e-9);

        // Tier 1 ([0.6R, 1.0R)): excursion=0.70R (3.08) -> lock entry +- 0.10R = +-0.44 (100.44 / 99.56)
        let l1 = evaluate_1s_position_tpsl(
            OrderSide::Long,
            100.0,
            103.08,
            5.0,
            95.60,
            120.0,
            103.08,
            99.5,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(l1.stop_ratcheted);
        assert!((l1.new_protective_stop_px - 100.44).abs() < 1e-9);

        let s1 = evaluate_1s_position_tpsl(
            OrderSide::Short,
            100.0,
            96.92,
            5.0,
            104.40,
            80.0,
            100.5,
            96.92,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(s1.stop_ratcheted);
        assert!((s1.new_protective_stop_px - 99.56).abs() < 1e-9);

        // Coarse tick rounding (0.05): LONG floors 100.44 -> 100.40, SHORT ceilings 99.56 -> 99.60
        let l1_tick = evaluate_1s_position_tpsl(
            OrderSide::Long,
            100.0,
            103.08,
            5.0,
            95.60,
            120.0,
            103.08,
            99.5,
            Some(2.0),
            0.05,
            &limits,
        );
        assert!((l1_tick.new_protective_stop_px - 100.40).abs() < 1e-9);
        let s1_tick = evaluate_1s_position_tpsl(
            OrderSide::Short,
            100.0,
            96.92,
            5.0,
            104.40,
            80.0,
            100.5,
            96.92,
            Some(2.0),
            0.05,
            &limits,
        );
        assert!((s1_tick.new_protective_stop_px - 99.60).abs() < 1e-9);

        // Tier 2 ([1.0R, 1.6R)): excursion=1.40R (6.16) -> trail 0.55*4.4=2.42 -> 103.74 / 96.26
        let l2 = evaluate_1s_position_tpsl(
            OrderSide::Long,
            100.0,
            106.16,
            5.0,
            l1.new_protective_stop_px,
            120.0,
            106.16,
            99.5,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(l2.stop_ratcheted);
        assert!((l2.new_protective_stop_px - 103.74).abs() < 1e-9);

        let s2 = evaluate_1s_position_tpsl(
            OrderSide::Short,
            100.0,
            93.84,
            5.0,
            s1.new_protective_stop_px,
            80.0,
            100.5,
            93.84,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(s2.stop_ratcheted);
        assert!((s2.new_protective_stop_px - 96.26).abs() < 1e-9);

        // Tier 3 (>= 1.6R): excursion=1.70R (7.48) -> trail 0.40*4.4=1.76 -> 105.72 / 94.28
        let l3 = evaluate_1s_position_tpsl(
            OrderSide::Long,
            100.0,
            107.48,
            5.0,
            l2.new_protective_stop_px,
            120.0,
            107.48,
            99.5,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(l3.stop_ratcheted);
        assert!((l3.new_protective_stop_px - 105.72).abs() < 1e-9);

        let s3 = evaluate_1s_position_tpsl(
            OrderSide::Short,
            100.0,
            92.52,
            5.0,
            s2.new_protective_stop_px,
            80.0,
            100.5,
            92.52,
            Some(2.0),
            0.01,
            &limits,
        );
        assert!(s3.stop_ratcheted);
        assert!((s3.new_protective_stop_px - 94.28).abs() < 1e-9);
    }

    #[test]
    fn test_stepped_chandelier_monotonic_invariance_across_path() {
        let limits = RiskLimitsConfig::default();
        // Long path across all tiers and pullback: stop must never move backward
        let mut stop_l = 95.60;
        let mut high_l = 100.0;
        let mut low_l = 100.0;
        for mark in [101.5, 102.64, 103.5, 104.40, 106.0, 107.04, 108.0, 106.5] {
            let ev = evaluate_1s_position_tpsl(
                OrderSide::Long,
                100.0,
                mark,
                5.0,
                stop_l,
                120.0,
                high_l,
                low_l,
                Some(2.0),
                0.01,
                &limits,
            );
            assert!(!ev.triggered);
            assert!(
                ev.new_protective_stop_px >= stop_l,
                "LONG stop regressed at mark={mark}: {} < {stop_l}",
                ev.new_protective_stop_px
            );
            stop_l = ev.new_protective_stop_px;
            high_l = ev.highest_price_seen;
            low_l = ev.lowest_price_seen;
        }
        assert!(stop_l >= 104.40);

        // Short path across all tiers and bounce: stop must never move upward
        let mut stop_s = 104.40;
        let mut high_s = 100.0;
        let mut low_s = 100.0;
        for mark in [98.5, 97.36, 96.5, 95.60, 94.0, 92.96, 92.0, 93.5] {
            let ev = evaluate_1s_position_tpsl(
                OrderSide::Short,
                100.0,
                mark,
                5.0,
                stop_s,
                80.0,
                high_s,
                low_s,
                Some(2.0),
                0.01,
                &limits,
            );
            assert!(!ev.triggered);
            assert!(
                ev.new_protective_stop_px <= stop_s,
                "SHORT stop regressed at mark={mark}: {} > {stop_s}",
                ev.new_protective_stop_px
            );
            stop_s = ev.new_protective_stop_px;
            high_s = ev.highest_price_seen;
            low_s = ev.lowest_price_seen;
        }
        assert!(stop_s <= 95.60);
    }

    #[test]
    fn test_breakeven_04r_lock_and_stagnation_time_stop_long_short() {
        let limits = RiskLimitsConfig::default();
        // entry=100, atr=2.0 -> stop_dist=4.4, 0.40R=1.76, +0.05R lock = +0.22 -> 100.22 / 99.78
        // 1. Exact 0.40R breakeven lock for LONG and SHORT
        let long_be = evaluate_1s_position_lifecycle(
            &StopEvaluationInput {
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 101.76,
                quantity: 5.0,
                current_stop_px: 95.60,
                current_tp_px: 105.60,
                highest_price_seen: 101.76,
                lowest_price_seen: 99.80,
                atr_opt: Some(2.0),
                tick_size: 0.01,
                check_tp: true,
                held_duration_ms: Some(1_000_000),
                entry_r0: None,
            },
            &limits,
        );
        assert!(!long_be.triggered);
        assert!(long_be.stop_ratcheted);
        assert!((long_be.new_protective_stop_px - 100.22).abs() < 1e-9);

        let short_be = evaluate_1s_position_lifecycle(
            &StopEvaluationInput {
                side: OrderSide::Short,
                entry_price: 100.0,
                mark_price: 98.24,
                quantity: 5.0,
                current_stop_px: 104.40,
                current_tp_px: 94.40,
                highest_price_seen: 100.20,
                lowest_price_seen: 98.24,
                atr_opt: Some(2.0),
                tick_size: 0.01,
                check_tp: true,
                held_duration_ms: Some(1_000_000),
                entry_r0: None,
            },
            &limits,
        );
        assert!(!short_be.triggered);
        assert!(short_be.stop_ratcheted);
        assert!((short_be.new_protective_stop_px - 99.78).abs() < 1e-9);

        // 2. Below default 12h (5h = 18_000_000 ms) must NOT trigger stagnation time-stop
        let pre_12h_exempt = evaluate_1s_position_lifecycle(
            &StopEvaluationInput {
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 100.20,
                quantity: 5.0,
                current_stop_px: 95.60,
                current_tp_px: 105.60,
                highest_price_seen: 101.00,
                lowest_price_seen: 99.20,
                atr_opt: Some(2.0),
                tick_size: 0.01,
                check_tp: true,
                held_duration_ms: Some(18_000_000),
                entry_r0: None,
            },
            &limits,
        );
        assert!(!pre_12h_exempt.triggered);

        // 3. Stagnation time-stop LONG: held 12h (43_200_000 ms), mfe_r < 0.35 (highest=101.0 -> 0.227R), cur_r <= 0.15
        let long_stag = evaluate_1s_position_lifecycle(
            &StopEvaluationInput {
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 100.20,
                quantity: 5.0,
                current_stop_px: 95.60,
                current_tp_px: 105.60,
                highest_price_seen: 101.00,
                lowest_price_seen: 99.20,
                atr_opt: Some(2.0),
                tick_size: 0.01,
                check_tp: true,
                held_duration_ms: Some(43_200_000),
                entry_r0: None,
            },
            &limits,
        );
        assert!(long_stag.triggered);
        assert_eq!(
            long_stag.exit_reason.as_deref(),
            Some("STAGNATION_TIME_STOP_LONG")
        );
        assert_eq!(long_stag.cooldown_duration_ms, 600_000);

        // 4. Stagnation time-stop SHORT: held 12h, mfe_r < 0.35 (lowest=99.0 -> 0.227R), cur_r <= 0.15
        let short_stag = evaluate_1s_position_lifecycle(
            &StopEvaluationInput {
                side: OrderSide::Short,
                entry_price: 100.0,
                mark_price: 99.80,
                quantity: 5.0,
                current_stop_px: 104.40,
                current_tp_px: 94.40,
                highest_price_seen: 100.80,
                lowest_price_seen: 99.00,
                atr_opt: Some(2.0),
                tick_size: 0.01,
                check_tp: true,
                held_duration_ms: Some(43_200_000),
                entry_r0: None,
            },
            &limits,
        );
        assert!(short_stag.triggered);
        assert_eq!(
            short_stag.exit_reason.as_deref(),
            Some("STAGNATION_TIME_STOP_SHORT")
        );
        assert_eq!(short_stag.cooldown_duration_ms, 600_000);

        // 5. Exempt if actively breaking out (cur_r > 0.15) OR held < 12h OR MFE >= 0.35R OR disabled (hours=0.0)
        let exempt_breakout = evaluate_1s_position_lifecycle(
            &StopEvaluationInput {
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 100.90, // cur_r = 0.204 > 0.15
                quantity: 5.0,
                current_stop_px: 95.60,
                current_tp_px: 105.60,
                highest_price_seen: 100.90,
                lowest_price_seen: 99.20,
                atr_opt: Some(2.0),
                tick_size: 0.01,
                check_tp: true,
                held_duration_ms: Some(43_200_000),
                entry_r0: None,
            },
            &limits,
        );
        assert!(!exempt_breakout.triggered);

        let exempt_mfe = evaluate_1s_position_lifecycle(
            &StopEvaluationInput {
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 100.10,
                quantity: 5.0,
                current_stop_px: 95.60,
                current_tp_px: 105.60,
                highest_price_seen: 101.60, // mfe_r = 0.363 >= 0.35
                lowest_price_seen: 99.20,
                atr_opt: Some(2.0),
                tick_size: 0.01,
                check_tp: true,
                held_duration_ms: Some(43_200_000),
                entry_r0: None,
            },
            &limits,
        );
        assert!(!exempt_mfe.triggered);

        let disabled_limits = RiskLimitsConfig {
            stagnation_time_stop_hours: 0.0,
            ..limits
        };
        let disabled_stag = evaluate_1s_position_lifecycle(
            &StopEvaluationInput {
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 100.20,
                quantity: 5.0,
                current_stop_px: 95.60,
                current_tp_px: 105.60,
                highest_price_seen: 101.00,
                lowest_price_seen: 99.20,
                atr_opt: Some(2.0),
                tick_size: 0.01,
                check_tp: true,
                held_duration_ms: Some(86_400_000),
                entry_r0: None,
            },
            &disabled_limits,
        );
        assert!(!disabled_stag.triggered);
    }

    #[test]
    fn test_frozen_entry_r0_protects_stagnation_and_chandelier_floors_under_2x_atr_expansion() {
        let limits = RiskLimitsConfig::default();
        // Entry at 100.0 with initial ATR = 2.0 => frozen entry_r0 = 4.40 (0.35R = 1.54, 0.40R = 1.76).
        // Later, live ATR doubles to 4.0 (live stop_dist = 8.80, so 0.35 * 8.80 = 3.08, 0.40 * 8.80 = 3.52).
        // 1. Stagnation exemption: excursion = 1.60 => 0.3636 R_0 (>= 0.35), even though 1.60 / 8.80 = 0.182 < 0.35!
        let frozen_stag_exempt = evaluate_1s_position_lifecycle(
            &StopEvaluationInput {
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 100.10,
                quantity: 5.0,
                current_stop_px: 95.60,
                current_tp_px: 112.00,
                highest_price_seen: 101.60,
                lowest_price_seen: 99.50,
                atr_opt: Some(4.0), // 2x ATR expansion
                tick_size: 0.01,
                check_tp: true,
                held_duration_ms: Some(43_200_000),
                entry_r0: Some(4.40),
            },
            &limits,
        );
        assert!(
            !frozen_stag_exempt.triggered,
            "Frozen R_0 (4.40) must exempt excursion=1.60 (0.364 R_0 >= 0.35) even under 2x ATR expansion"
        );

        // Without frozen entry_r0, same 2x ATR expansion falsely triggers stagnation!
        let unfrozen_stag = evaluate_1s_position_lifecycle(
            &StopEvaluationInput {
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 100.10,
                quantity: 5.0,
                current_stop_px: 95.60,
                current_tp_px: 112.00,
                highest_price_seen: 101.60,
                lowest_price_seen: 99.50,
                atr_opt: Some(4.0),
                tick_size: 0.01,
                check_tp: true,
                held_duration_ms: Some(43_200_000),
                entry_r0: None,
            },
            &limits,
        );
        assert!(unfrozen_stag.triggered);
        assert_eq!(
            unfrozen_stag.exit_reason.as_deref(),
            Some("STAGNATION_TIME_STOP_LONG")
        );

        // 2. Breakeven 0.40 R_0 & Chandelier profit-lock floor under 2x ATR expansion (LONG & SHORT):
        // excursion = 1.98 (= 0.45 R_0 >= 0.40 R_0, but only 0.225 * 8.80).
        // With entry_r0 = 4.40 and atr = 4.0 (min_buf = 2.20), at mark = 103.0:
        // max_safe_stop = 100.80 >= profit_lock = 100.0 + 0.05 * 4.40 = 100.22 -> stop ratchets to 100.22!
        let long_be_frozen = evaluate_1s_position_lifecycle(
            &StopEvaluationInput {
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 103.00,
                quantity: 5.0,
                current_stop_px: 95.60,
                current_tp_px: 112.00,
                highest_price_seen: 103.00, // 0.68 R_0 -> tier [0.60, 1.00) -> lock 0.10 R_0 = 100.44
                lowest_price_seen: 99.80,
                atr_opt: Some(4.0),
                tick_size: 0.01,
                check_tp: true,
                held_duration_ms: Some(3_600_000),
                entry_r0: Some(4.40),
            },
            &limits,
        );
        assert!(long_be_frozen.stop_ratcheted);
        assert!((long_be_frozen.new_protective_stop_px - 100.44).abs() < 1e-9);

        let short_be_frozen = evaluate_1s_position_lifecycle(
            &StopEvaluationInput {
                side: OrderSide::Short,
                entry_price: 100.0,
                mark_price: 97.00,
                quantity: 5.0,
                current_stop_px: 104.40,
                current_tp_px: 88.00,
                highest_price_seen: 100.20,
                lowest_price_seen: 97.00, // 0.68 R_0 -> lock 0.10 R_0 = 99.56
                atr_opt: Some(4.0),
                tick_size: 0.01,
                check_tp: true,
                held_duration_ms: Some(3_600_000),
                entry_r0: Some(4.40),
            },
            &limits,
        );
        assert!(short_be_frozen.stop_ratcheted);
        assert!((short_be_frozen.new_protective_stop_px - 99.56).abs() < 1e-9);
    }
}
