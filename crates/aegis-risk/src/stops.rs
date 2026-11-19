//! Initial Protective Stop & 1-Second High-Water Chandelier/ATR Trailing Stop Ratchet (`aegis-risk::stops`).

use crate::limits::{round_to_tick, RiskLimitsConfig};
use aegis_core::{round_dp, OhlcvBar, OrderSide};
use serde::{Deserialize, Serialize};

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
pub fn compute_atr_from_bars(
    bars: &[OhlcvBar],
    period: usize,
    reference_price: f64,
    default_pct: f64,
) -> f64 {
    let ref_px = if reference_price.is_finite() {
        reference_price.max(1e-8)
    } else {
        1.0
    };
    let fallback = (ref_px * (default_pct.max(0.005) / 2.2)).max(1e-8);
    if bars.len() < 2 {
        return fallback;
    }
    let lookback = period.max(2).min(bars.len() - 1);
    let start = bars.len() - lookback;
    let mut sum_tr = 0.0;
    for i in start..bars.len() {
        let cur = &bars[i];
        let prev_close = bars[i - 1].close;
        if !cur.high.is_finite() || !cur.low.is_finite() || !prev_close.is_finite() {
            return fallback;
        }
        let hl = (cur.high - cur.low).abs();
        let hc = (cur.high - prev_close).abs();
        let lc = (cur.low - prev_close).abs();
        sum_tr += hl.max(hc).max(lc);
    }
    let min_floor = (ref_px * 0.004).max(1e-8);
    (sum_tr / (lookback as f64)).max(min_floor)
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
    let min_tp_dist = (ref_px * 0.016).max(step * 4.0);
    let raw_dist = match atr_opt.filter(|v| v.is_finite() && *v > 0.0) {
        Some(atr) => atr * limits.take_profit_atr_mult,
        None => ref_px * limits.take_profit_default_pct,
    };
    let stop_floor = resolve_stop_distance(ref_px, atr_opt, tick_size, limits) * 1.5;
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

/// Evaluates the 1-second High-Water Chandelier/ATR trailing stop ratchet on an `OPEN` holding.
/// - Ratchets the stop monotonically in the direction of profit (`new_stop >= current_stop` for `LONG`,
///   `new_stop <= current_stop` for `SHORT`).
/// - Locks in break-even + profit buffer once favorable excursion exceeds 1.0x stop distance.
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
    evaluate_1s_position_tpsl(
        side,
        entry_price,
        mark_price,
        quantity,
        current_stop_px,
        0.0,
        highest_price_seen,
        lowest_price_seen,
        atr_opt,
        tick_size,
        limits,
    )
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
    let init_stop = compute_initial_protective_stop(side, entry_price, atr_opt, tick_size, limits);
    let init_tp = compute_initial_take_profit(side, entry_price, atr_opt, tick_size, limits);
    let step = tick_step(tick_size);
    let min_buf = (stop_dist * 0.25).max(step * 4.0);

    match side {
        OrderSide::Long => {
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
                lowest_price_seen.min(ref_mark)
            } else {
                entry_price.min(ref_mark)
            };

            if active_stop > 0.0 && ref_mark <= active_stop {
                let tp_dist_pct = round_dp(((active_tp - ref_mark).max(0.0) / ref_mark) * 100.0, 2);
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
                    exit_reason: Some("PROTECTIVE_STOP_LONG".to_string()),
                    cooldown_duration_ms: limits.stop_out_cooldown_sec * 1000,
                };
            }

            if active_tp > 0.0 && ref_mark >= active_tp {
                let stop_dist_pct =
                    round_dp(((ref_mark - active_stop).max(0.0) / ref_mark) * 100.0, 2);
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
                    exit_reason: Some("TAKE_PROFIT_LONG".to_string()),
                    cooldown_duration_ms: 0,
                };
            }

            let max_safe_stop = (ref_mark - min_buf).max(step);
            let mut ratcheted_stop = active_stop;
            if new_highest > entry_price + step && active_stop < max_safe_stop {
                let mut candidate = new_highest - stop_dist;
                let excursion = (new_highest - entry_price).max(0.0);
                if excursion >= stop_dist {
                    // Break-even profit lock once +1R excursion achieved
                    let profit_lock = entry_price + 0.25 * stop_dist;
                    candidate = candidate.max(profit_lock);
                }
                let capped_candidate = candidate.min(max_safe_stop);
                let rounded_candidate = round_to_tick(capped_candidate, tick_size, true);
                if rounded_candidate > active_stop + step * 0.5
                    && rounded_candidate <= max_safe_stop + 1e-12
                {
                    ratcheted_stop = rounded_candidate;
                }
            }
            let stop_ratcheted = ratcheted_stop > active_stop + step * 0.5;
            let dist_pct = round_dp(((ref_mark - ratcheted_stop).max(0.0) / ref_mark) * 100.0, 2);
            let tp_dist_pct = round_dp(((active_tp - ref_mark).max(0.0) / ref_mark) * 100.0, 2);

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
        OrderSide::Short => {
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

            if active_stop > 0.0 && ref_mark >= active_stop {
                let tp_dist_pct = round_dp(((ref_mark - active_tp).max(0.0) / ref_mark) * 100.0, 2);
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
                    exit_reason: Some("PROTECTIVE_STOP_SHORT".to_string()),
                    cooldown_duration_ms: limits.stop_out_cooldown_sec * 1000,
                };
            }

            if active_tp > 0.0 && ref_mark <= active_tp {
                let stop_dist_pct =
                    round_dp(((active_stop - ref_mark).max(0.0) / ref_mark) * 100.0, 2);
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
                    exit_reason: Some("TAKE_PROFIT_SHORT".to_string()),
                    cooldown_duration_ms: 0,
                };
            }

            let min_safe_stop = ref_mark + min_buf;
            let mut ratcheted_stop = active_stop;
            if new_lowest > 0.0 && new_lowest < entry_price - step && active_stop > min_safe_stop {
                let mut candidate = new_lowest + stop_dist;
                let excursion = (entry_price - new_lowest).max(0.0);
                if excursion >= stop_dist {
                    // Break-even profit lock once +1R excursion achieved
                    let profit_lock = (entry_price - 0.25 * stop_dist).max(step);
                    candidate = candidate.min(profit_lock);
                }
                let capped_candidate = candidate.max(min_safe_stop);
                let rounded_candidate = round_to_tick(capped_candidate, tick_size, false);
                if rounded_candidate + step * 0.5 < active_stop
                    && rounded_candidate >= min_safe_stop - 1e-12
                {
                    ratcheted_stop = rounded_candidate;
                }
            }
            let stop_ratcheted = ratcheted_stop + step * 0.5 < active_stop;
            let dist_pct = round_dp(((ratcheted_stop - ref_mark).max(0.0) / ref_mark) * 100.0, 2);
            let tp_dist_pct = round_dp(((ref_mark - active_tp).max(0.0) / ref_mark) * 100.0, 2);

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
        // LONG: entry 100, highest 105 (in profit), pulled back to mark 101, atr 2.0 (stop_dist = 4.4, min_buf = 1.1)
        // Candidate = 105 - 4.4 = 100.6, which is > mark - min_buf (101 - 1.1 = 99.9) -> must clamp to 99.9 (not 100.99 1-tick!)
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
}
