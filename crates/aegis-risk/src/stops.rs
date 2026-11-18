//! Initial Protective Stop & 1-Second High-Water Chandelier/ATR Trailing Stop Ratchet (`aegis-risk::stops`).

use crate::limits::{round_to_tick, RiskLimitsConfig};
use aegis_core::{round_dp, OhlcvBar, OrderSide};
use serde::{Deserialize, Serialize};

/// Result of a 1-second protective stop ratchet & breach check on an `OPEN` position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopRatchetEvaluation {
    pub triggered: bool,
    pub new_protective_stop_px: f64,
    pub stop_ratcheted: bool,
    pub highest_price_seen: f64,
    pub lowest_price_seen: f64,
    pub stop_distance_pct: f64,
    pub unrealized_pnl_usdc: f64,
    pub exit_reason: Option<String>,
    pub cooldown_duration_ms: i64,
}

/// Computes Average True Range (ATR) over `period` OHLCV bars with a safe price-fraction floor.
pub fn compute_atr_from_bars(
    bars: &[OhlcvBar],
    period: usize,
    reference_price: f64,
    default_pct: f64,
) -> f64 {
    let fallback = (reference_price.max(1e-8) * (default_pct.max(0.005) / 2.2)).max(1e-8);
    if bars.len() < 2 {
        return fallback;
    }
    let lookback = period.max(2).min(bars.len() - 1);
    let start = bars.len() - lookback;
    let mut sum_tr = 0.0;
    for i in start..bars.len() {
        let cur = &bars[i];
        let prev_close = bars[i - 1].close;
        let hl = (cur.high - cur.low).abs();
        let hc = (cur.high - prev_close).abs();
        let lc = (cur.low - prev_close).abs();
        sum_tr += hl.max(hc).max(lc);
    }
    let min_floor = (reference_price.max(1e-8) * 0.004).max(1e-8);
    (sum_tr / (lookback as f64)).max(min_floor)
}

/// Computes the effective stop distance in price units given `entry_price`, optional `atr_value`, and `RiskLimitsConfig`.
pub fn resolve_stop_distance(
    reference_price: f64,
    atr_opt: Option<f64>,
    tick_size: f64,
    limits: &RiskLimitsConfig,
) -> f64 {
    let ref_px = reference_price.max(1e-8);
    let min_stop_dist = (ref_px * 0.008).max(tick_size.max(1e-6) * 2.0);
    let raw_dist = match atr_opt.filter(|v| v.is_finite() && *v > 0.0) {
        Some(atr) => atr * limits.protective_stop_atr_mult,
        None => ref_px * limits.protective_stop_default_pct,
    };
    raw_dist.max(min_stop_dist)
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
    match side {
        OrderSide::Long => round_to_tick(
            (entry_price - dist).max(tick_size.max(1e-6)),
            tick_size,
            true,
        ),
        OrderSide::Short => round_to_tick(entry_price + dist, tick_size, false),
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
    let ref_mark = if mark_price > 0.0 {
        mark_price
    } else {
        entry_price.max(1e-8)
    };
    let qty = quantity.abs();
    let unrealized_pnl_usdc = round_dp((ref_mark - entry_price) * qty * side.sign(), 4);
    let stop_dist = resolve_stop_distance(ref_mark, atr_opt, tick_size, limits);
    let init_stop = compute_initial_protective_stop(side, entry_price, atr_opt, tick_size, limits);
    let step = tick_size.max(1e-6);

    match side {
        OrderSide::Long => {
            let active_stop = if current_stop_px > 0.0 {
                current_stop_px
            } else {
                init_stop
            };
            let new_highest = highest_price_seen.max(entry_price).max(ref_mark);
            let new_lowest = if lowest_price_seen > 0.0 {
                lowest_price_seen.min(ref_mark)
            } else {
                entry_price.min(ref_mark)
            };

            if ref_mark <= active_stop {
                return StopRatchetEvaluation {
                    triggered: true,
                    new_protective_stop_px: active_stop,
                    stop_ratcheted: false,
                    highest_price_seen: new_highest,
                    lowest_price_seen: new_lowest,
                    stop_distance_pct: 0.0,
                    unrealized_pnl_usdc,
                    exit_reason: Some("PROTECTIVE_STOP_LONG".to_string()),
                    cooldown_duration_ms: limits.stop_out_cooldown_sec * 1000,
                };
            }

            let mut candidate = new_highest - stop_dist;
            let excursion = (new_highest - entry_price).max(0.0);
            if excursion >= stop_dist {
                // Break-even profit lock once +1R excursion achieved
                let profit_lock = entry_price + 0.25 * stop_dist;
                candidate = candidate.max(profit_lock);
            }
            let capped_candidate = candidate.min((ref_mark - step).max(step));
            let ratcheted_stop = round_to_tick(
                active_stop.max(init_stop).max(capped_candidate),
                tick_size,
                true,
            );
            let stop_ratcheted = ratcheted_stop > active_stop + step * 0.5;
            let dist_pct = round_dp(((ref_mark - ratcheted_stop).max(0.0) / ref_mark) * 100.0, 2);

            StopRatchetEvaluation {
                triggered: false,
                new_protective_stop_px: ratcheted_stop,
                stop_ratcheted,
                highest_price_seen: new_highest,
                lowest_price_seen: new_lowest,
                stop_distance_pct: dist_pct,
                unrealized_pnl_usdc,
                exit_reason: None,
                cooldown_duration_ms: 0,
            }
        }
        OrderSide::Short => {
            let active_stop = if current_stop_px > 0.0 {
                current_stop_px
            } else {
                init_stop
            };
            let new_highest = highest_price_seen.max(entry_price).max(ref_mark);
            let new_lowest = if lowest_price_seen > 0.0 {
                lowest_price_seen.min(entry_price).min(ref_mark)
            } else {
                entry_price.min(ref_mark)
            };

            if ref_mark >= active_stop {
                return StopRatchetEvaluation {
                    triggered: true,
                    new_protective_stop_px: active_stop,
                    stop_ratcheted: false,
                    highest_price_seen: new_highest,
                    lowest_price_seen: new_lowest,
                    stop_distance_pct: 0.0,
                    unrealized_pnl_usdc,
                    exit_reason: Some("PROTECTIVE_STOP_SHORT".to_string()),
                    cooldown_duration_ms: limits.stop_out_cooldown_sec * 1000,
                };
            }

            let mut candidate = new_lowest + stop_dist;
            let excursion = (entry_price - new_lowest).max(0.0);
            if excursion >= stop_dist {
                // Break-even profit lock once +1R excursion achieved
                let profit_lock = entry_price - 0.25 * stop_dist;
                candidate = candidate.min(profit_lock);
            }
            let capped_candidate = candidate.max(ref_mark + step);
            let ratcheted_stop = round_to_tick(
                active_stop.min(init_stop).min(capped_candidate),
                tick_size,
                false,
            );
            let stop_ratcheted = ratcheted_stop + step * 0.5 < active_stop;
            let dist_pct = round_dp(((ratcheted_stop - ref_mark).max(0.0) / ref_mark) * 100.0, 2);

            StopRatchetEvaluation {
                triggered: false,
                new_protective_stop_px: ratcheted_stop,
                stop_ratcheted,
                highest_price_seen: new_highest,
                lowest_price_seen: new_lowest,
                stop_distance_pct: dist_pct,
                unrealized_pnl_usdc,
                exit_reason: None,
                cooldown_duration_ms: 0,
            }
        }
    }
}
