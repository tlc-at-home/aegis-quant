//! Sleeve 2: Breadth-Confirmed Multi-Speed Trend & Live 3s Breakout (`aegis-signals::trend_breakout`).
//!
//! Computes `s2_trend_alignment` (`-1`, `0`, `+1`), `s2_vol_surge_ratio`, `w_s2_trend_breakout`,
//! and detects `"LIVE_BREAKOUT_SURGE"` events.

use aegis_core::{round_dp, ExchangeAsset, OhlcvBar};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const TRIGGER_LIVE_BREAKOUT_SURGE: &str = "LIVE_BREAKOUT_SURGE";

/// Computed Sleeve 2 metrics for a single Hyperliquid perpetual asset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendBreakoutMetrics {
    pub asset_id: i64,
    pub symbol: String,
    pub s2_trend_alignment: i32,
    pub s2_vol_surge_ratio: f64,
    pub w_s2_trend_breakout: f64,
    pub event_trigger: Option<String>,
}

/// Computes a strictly causal Exponential Moving Average (`alpha = 2 / (span + 1)`).
pub fn causal_ema(series: &[f64], span: f64) -> Vec<f64> {
    if series.is_empty() {
        return Vec::new();
    }
    let alpha = (2.0 / (span.max(1.0) + 1.0)).clamp(0.001, 1.0);
    let mut out = Vec::with_capacity(series.len());
    out.push(series[0]);
    for &val in &series[1..] {
        let prev = *out.last().unwrap_or(&val);
        out.push(alpha * val + (1.0 - alpha) * prev);
    }
    out
}

/// Computes causal 14-period RSI (`[0.0, 100.0]`), lookback range position (`[0.0, 1.0]`),
/// and whether price is bouncing off the bottom quartile of its range.
pub fn compute_rsi_and_range_position(bars: &[OhlcvBar], live_px: f64) -> (f64, f64, bool) {
    if bars.is_empty() || !live_px.is_finite() || live_px <= 0.0 {
        return (50.0, 0.5, false);
    }

    let range_lookback = bars.len().min(72);
    let range_slice = &bars[bars.len() - range_lookback..];
    let min_low = range_slice
        .iter()
        .map(|b| b.low.min(b.close))
        .fold(f64::MAX, f64::min);
    let max_high = range_slice
        .iter()
        .map(|b| b.high.max(b.close))
        .fold(f64::MIN, f64::max);
    let span = max_high - min_low;
    let range_pos = if span > 1e-12 {
        ((live_px - min_low) / span).clamp(0.0, 1.0)
    } else {
        0.5
    };
    if min_low < f64::MAX && live_px < min_low * 0.995 {
        return (50.0, 0.30, false);
    }

    let rsi_14 = if bars.len() >= 4 {
        let mut closes: Vec<f64> = bars
            .iter()
            .rev()
            .take(15)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .map(|b| b.close.max(1e-12))
            .collect();
        closes.push(live_px);
        let mut gains = 0.0;
        let mut losses = 0.0;
        let diffs = closes.len().saturating_sub(1);
        for pair in closes.windows(2) {
            let diff = pair[1] - pair[0];
            if diff > 0.0 {
                gains += diff;
            } else if diff < 0.0 {
                losses += -diff;
            }
        }
        if diffs == 0 {
            50.0
        } else {
            let avg_gain = gains / (diffs as f64);
            let avg_loss = losses / (diffs as f64);
            if avg_loss <= 1e-12 && avg_gain <= 1e-12 {
                50.0
            } else if avg_loss <= 1e-12 {
                100.0
            } else {
                let rs = avg_gain / avg_loss;
                100.0 - (100.0 / (1.0 + rs))
            }
        }
    } else {
        50.0
    };

    let holding_above_swing_low =
        min_low > 0.0 && min_low < f64::MAX && live_px >= min_low * 1.003 && range_pos <= 0.20;
    let bouncing_from_low = (bars.last().is_some_and(|b| live_px >= b.open) && range_pos <= 0.25)
        || holding_above_swing_low;
    (rsi_14, range_pos, bouncing_from_low)
}

/// Computes multi-speed trend alignment (`-1`, `0`, `+1`), volume surge ratio, and live breakout detection
/// for a single asset.
pub fn compute_asset_trend_breakout(
    asset: &ExchangeAsset,
    bars: &[OhlcvBar],
) -> (i32, f64, f64, Option<String>) {
    if bars.len() >= 10 {
        let mut closes: Vec<f64> = bars.iter().map(|b| b.close.max(1e-12)).collect();
        if asset.mark_price > 0.0 {
            closes.push(asset.mark_price);
        }
        let n = closes.len();
        let ema_fast = causal_ema(&closes, 8.0);
        let ema_mid = causal_ema(&closes, 21.0);
        let ema_slow = causal_ema(&closes, 55.0);

        let f = ema_fast[n - 1];
        let m = ema_mid[n - 1];
        let s = ema_slow[n - 1].max(1e-12);

        let alignment = if f > m && m > s {
            1
        } else if f < m && m < s {
            -1
        } else {
            0
        };

        let lookback = bars.len().min(20);
        let recent_bars = &bars[bars.len() - lookback..];
        let avg_vol =
            recent_bars.iter().map(|b| b.volume.max(1.0)).sum::<f64>() / (lookback as f64);
        let last_vol = recent_bars
            .last()
            .map(|b| b.volume.max(1.0))
            .unwrap_or(avg_vol);

        let chan_len = bars.len().saturating_sub(1).clamp(4, 18);
        let prior_slice = &bars[bars.len() - 1 - chan_len..bars.len() - 1];
        let channel_high = prior_slice.iter().map(|b| b.high).fold(f64::MIN, f64::max);
        let channel_low = prior_slice.iter().map(|b| b.low).fold(f64::MAX, f64::min);

        let live_px = closes[n - 1];
        let broke_high = channel_high > 0.0 && live_px > channel_high * 1.0015;
        let broke_low = channel_low > 0.0 && live_px < channel_low * 0.9985;

        let mut surge_ratio = (last_vol / avg_vol.max(1e-8)).clamp(0.25, 6.0);
        if (alignment > 0 && broke_high) || (alignment < 0 && broke_low) {
            surge_ratio = surge_ratio.max(1.85);
        }
        let surge_ratio = round_dp(surge_ratio, 4);

        let (rsi_14, range_pos, bouncing_from_low) = compute_rsi_and_range_position(bars, live_px);
        if alignment < 0 && (rsi_14 < 36.0 || range_pos < 0.22 || bouncing_from_low) {
            return (0, surge_ratio, 0.0, None);
        }
        if alignment > 0
            && rsi_14 > 76.0
            && range_pos > 0.90
            && bars.last().is_some_and(|b| live_px < b.open)
        {
            return (0, surge_ratio, 0.0, None);
        }

        let is_breakout_surge = (alignment == 1
            && (broke_high
                || (surge_ratio >= 1.75
                    && range_pos >= 0.65
                    && bars.last().is_some_and(|b| live_px >= b.open))))
            || (alignment == -1
                && (broke_low
                    || (surge_ratio >= 1.75
                        && (0.22..=0.35).contains(&range_pos)
                        && bars.last().is_some_and(|b| live_px <= b.open))));
        let trigger = is_breakout_surge.then(|| TRIGGER_LIVE_BREAKOUT_SURGE.to_string());

        let trend_strength = ((f - s) / s * 12.0).tanh();
        let raw_score = if alignment != 0 {
            (alignment as f64) * (0.60 + 0.40 * trend_strength.abs()) * surge_ratio.min(2.5)
        } else {
            0.0
        };

        return (alignment, surge_ratio, raw_score, trigger);
    }

    if asset.prev_day_price > 0.0 && asset.mark_price > 0.0 {
        let pct_24h = (asset.mark_price - asset.prev_day_price) / asset.prev_day_price;
        let alignment = if pct_24h >= 0.015 {
            1
        } else if pct_24h <= -0.015 {
            -1
        } else {
            0
        };
        let surge_ratio = round_dp((1.0 + pct_24h.abs() * 12.0).clamp(0.5, 4.0), 4);
        let is_surge = alignment != 0 && (pct_24h.abs() >= 0.045 || surge_ratio >= 1.75);
        let trigger = is_surge.then(|| TRIGGER_LIVE_BREAKOUT_SURGE.to_string());
        let raw_score = (alignment as f64) * (pct_24h.abs() * 15.0).tanh() * surge_ratio.min(2.5);
        return (alignment, surge_ratio, raw_score, trigger);
    }

    (0, 1.0, 0.0, None)
}

/// Evaluates Sleeve 2 (Breadth-Confirmed Multi-Speed Trend & Live 3s Breakout) across the universe.
pub fn evaluate_sleeve_s2(
    assets: &[ExchangeAsset],
    bars_by_asset: &HashMap<i64, Vec<OhlcvBar>>,
    market_breadth_bull: f64,
    s2_budget: f64,
) -> HashMap<i64, TrendBreakoutMetrics> {
    let budget = s2_budget.clamp(0.0, 1.50);
    let breadth = market_breadth_bull.clamp(0.0, 1.0);

    let bull_breadth_mult = (0.65 + 0.70 * breadth).clamp(0.50, 1.35);
    let bear_breadth_mult = (0.65 + 0.70 * (1.0 - breadth)).clamp(0.50, 1.35);

    let mut raw_items = Vec::with_capacity(assets.len());
    let mut abs_sum = 0.0;

    for asset in assets {
        let bars = bars_by_asset
            .get(&asset.id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let (alignment, surge_ratio, raw_score, trigger) =
            compute_asset_trend_breakout(asset, bars);

        let eligible = asset.is_core_eligible || (asset.is_event_eligible && trigger.is_some());
        let confirmed_score = if eligible && alignment != 0 {
            let b_mult = if alignment > 0 {
                bull_breadth_mult
            } else {
                bear_breadth_mult
            };
            raw_score * b_mult
        } else {
            0.0
        };

        abs_sum += confirmed_score.abs();
        raw_items.push((
            asset.id,
            asset.symbol.clone(),
            alignment,
            surge_ratio,
            confirmed_score,
            trigger,
        ));
    }

    let _ = abs_sum;
    let mut abs_scores: Vec<f64> = raw_items
        .iter()
        .map(|x| x.4.abs())
        .filter(|&v| v > 1e-6)
        .collect();
    abs_scores.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    let top_abs_sum: f64 = abs_scores.iter().take(8).sum();
    let denom = if top_abs_sum > 1e-8 {
        top_abs_sum
    } else {
        1.0
    };
    let mut out = HashMap::with_capacity(raw_items.len());

    for (asset_id, symbol, s2_trend_alignment, s2_vol_surge_ratio, score, event_trigger) in
        raw_items
    {
        let w_s2 = if top_abs_sum > 1e-8 {
            round_dp((score / denom) * budget, 4)
        } else {
            0.0
        };
        out.insert(
            asset_id,
            TrendBreakoutMetrics {
                asset_id,
                symbol,
                s2_trend_alignment,
                s2_vol_surge_ratio,
                w_s2_trend_breakout: w_s2,
                event_trigger,
            },
        );
    }

    out
}
