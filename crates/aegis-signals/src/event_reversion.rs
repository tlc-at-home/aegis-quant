//! Sleeve 3: Live Capitulation Flush & Funding Dislocation Harvester (`aegis-signals::event_reversion`).
//!
//! Computes `s3_funding_zscore`, `s3_flush_atr_mult`, `w_s3_event_reversion`,
//! and detects `"CAPITULATION_FLUSH"` and `"FUNDING_SQUEEZE"` triggers.

use crate::trend_breakout::{causal_ema, compute_rsi_and_range_position};
use aegis_core::{round_dp, ExchangeAsset, OhlcvBar};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const TRIGGER_CAPITULATION_FLUSH: &str = "CAPITULATION_FLUSH";
pub const TRIGGER_FUNDING_SQUEEZE: &str = "FUNDING_SQUEEZE";

/// Computed Sleeve 3 metrics for a single Hyperliquid perpetual asset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventReversionMetrics {
    pub asset_id: i64,
    pub symbol: String,
    pub s3_funding_zscore: f64,
    pub s3_flush_atr_mult: f64,
    pub w_s3_event_reversion: f64,
    pub event_trigger: Option<String>,
}

/// Computes Average True Range (ATR) over `period` bars with a safe price-percentage floor.
pub fn compute_atr(bars: &[OhlcvBar], period: usize, fallback_price: f64) -> f64 {
    let min_floor = (fallback_price.max(1e-8) * 0.012).max(1e-8);
    if bars.len() < 2 {
        return min_floor;
    }
    let lookback = period.max(2).min(bars.len() - 1);
    let start = bars.len() - lookback;
    let mut tr_sum = 0.0;
    for i in start..bars.len() {
        let cur = &bars[i];
        let prev_close = bars[i - 1].close;
        let hl = (cur.high - cur.low).abs();
        let hc = (cur.high - prev_close).abs();
        let lc = (cur.low - prev_close).abs();
        tr_sum += hl.max(hc).max(lc);
    }
    (tr_sum / (lookback as f64)).max(min_floor)
}

/// Computes signed excursion from short-term equilibrium in ATR units (`(mark_price - ema_anchor) / atr`).
/// Negative values (`<= -2.0`) indicate an acute downward capitulation flush;
/// positive values (`>= +2.0`) indicate an acute upward blow-off extension.
pub fn compute_flush_atr_multiple(asset: &ExchangeAsset, bars: &[OhlcvBar]) -> f64 {
    let live_px = if asset.mark_price > 0.0 {
        asset.mark_price
    } else {
        bars.last().map(|b| b.close).unwrap_or(0.0)
    };
    if live_px <= 0.0 {
        return 0.0;
    }

    if bars.len() >= 6 {
        let closes: Vec<f64> = bars.iter().map(|b| b.close.max(1e-12)).collect();
        let ema12 = causal_ema(&closes, 12.0);
        let anchor = *ema12.last().unwrap_or(&live_px);
        let atr = compute_atr(bars, 14, live_px);
        let mult = ((live_px - anchor) / atr.max(1e-8)).clamp(-8.0, 8.0);
        return round_dp(mult, 4);
    }

    if asset.prev_day_price > 0.0 {
        let est_atr = (asset.prev_day_price * 0.025).max(1e-8);
        let mult = ((live_px - asset.prev_day_price) / est_atr).clamp(-8.0, 8.0);
        return round_dp(mult, 4);
    }

    0.0
}

/// Evaluates Sleeve 3 (Live Capitulation Flush & Funding Dislocation Harvester) across the universe.
pub fn evaluate_sleeve_s3(
    assets: &[ExchangeAsset],
    bars_by_asset: &HashMap<i64, Vec<OhlcvBar>>,
    s3_budget: f64,
) -> HashMap<i64, EventReversionMetrics> {
    let budget = s3_budget.clamp(0.0, 1.50);
    if assets.is_empty() {
        return HashMap::new();
    }

    // Cross-sectional funding rate distribution over eligible assets only
    let eligible_fundings: Vec<f64> = assets
        .iter()
        .filter(|a| {
            (a.is_core_eligible || a.is_event_eligible) && a.funding_rate_hourly.is_finite()
        })
        .map(|a| a.funding_rate_hourly)
        .collect();
    let (mean_funding, std_funding) = if eligible_fundings.is_empty() {
        (0.0, 0.00004)
    } else {
        let n = eligible_fundings.len() as f64;
        let mean = eligible_fundings.iter().sum::<f64>() / n;
        let var = eligible_fundings
            .iter()
            .map(|&f| {
                let d = f - mean;
                d * d
            })
            .sum::<f64>()
            / (n - 1.0).max(1.0);
        (mean, var.sqrt().max(0.00004))
    };

    let mut raw_items = Vec::with_capacity(assets.len());

    for asset in assets {
        let bars = bars_by_asset
            .get(&asset.id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let funding = if asset.funding_rate_hourly.is_finite() {
            asset.funding_rate_hourly
        } else {
            mean_funding
        };
        let f_zscore = round_dp(((funding - mean_funding) / std_funding).clamp(-6.0, 6.0), 4);
        let flush_atr = compute_flush_atr_multiple(asset, bars);
        let live_px = if asset.mark_price.is_finite() && asset.mark_price > 0.0 {
            asset.mark_price
        } else {
            bars.last().map(|b| b.close).unwrap_or(0.0)
        };
        let (rsi_14, range_pos, bouncing_from_low) = if bars.len() >= 8 && live_px > 0.0 {
            compute_rsi_and_range_position(bars, live_px)
        } else {
            (50.0, 0.5, false)
        };

        let eligible = asset.is_event_eligible || asset.is_core_eligible;
        let is_capitulation = flush_atr <= -2.0 || (flush_atr >= 2.0 && range_pos > 0.55);
        let is_funding_squeeze =
            f_zscore <= -1.8 || (f_zscore >= 1.8 && flush_atr > 0.25 && range_pos > 0.35);
        let is_bottom_reversion = bars.len() >= 8
            && range_pos <= 0.20
            && rsi_14 < 54.0
            && flush_atr <= 0.25
            && bouncing_from_low;

        let trigger = if !eligible {
            None
        } else if is_capitulation || is_bottom_reversion {
            Some(TRIGGER_CAPITULATION_FLUSH.to_string())
        } else if is_funding_squeeze {
            Some(TRIGGER_FUNDING_SQUEEZE.to_string())
        } else {
            None
        };

        // Contrarian mean-reversion & funding dislocation score:
        // - Deep downward flush (`flush_atr <= -2.0`) -> contrarian LONG (+score)
        // - Deeply negative funding (`f_zscore <= -1.8`) -> short squeeze LONG (+score)
        // - Confirmed oversold range-bottom green bounce -> contrarian LONG (monotonic with flush)
        // - Extreme blow-off + crowded positive funding -> contrarian SHORT (-score)
        let raw_score = if eligible {
            let cap = if is_capitulation {
                (-flush_atr * 0.45).clamp(-1.5, 1.5)
            } else {
                0.0
            };
            let fund = if is_funding_squeeze {
                (-f_zscore * 0.40).clamp(-1.5, 1.5)
            } else {
                0.0
            };
            let bounce = if is_bottom_reversion {
                let depth_boost = ((0.22 - range_pos).max(0.02) / 0.22).clamp(0.2, 1.0);
                0.45 + 0.45 * depth_boost
            } else {
                0.0
            };
            (cap + fund + bounce).clamp(-1.5, 1.5)
        } else {
            0.0
        };

        raw_items.push((
            asset.id,
            asset.symbol.clone(),
            f_zscore,
            flush_atr,
            raw_score,
            trigger,
        ));
    }

    let mut abs_scores: Vec<f64> = raw_items
        .iter()
        .map(|x| x.4.abs())
        .filter(|v| v.is_finite() && *v > 1e-6)
        .collect();
    abs_scores.sort_by(|a, b| b.total_cmp(a));
    let top_abs_sum: f64 = abs_scores.iter().take(8).sum();
    let denom = if top_abs_sum > 1e-8 { top_abs_sum } else { 1.0 };
    let mut out = HashMap::with_capacity(raw_items.len());

    for (asset_id, symbol, s3_funding_zscore, s3_flush_atr_mult, score, event_trigger) in raw_items
    {
        let w_s3 = if top_abs_sum > 1e-8 && score.abs() > 1e-6 {
            round_dp((score / denom) * budget, 4)
        } else {
            0.0
        };
        out.insert(
            asset_id,
            EventReversionMetrics {
                asset_id,
                symbol,
                s3_funding_zscore,
                s3_flush_atr_mult,
                w_s3_event_reversion: w_s3,
                event_trigger,
            },
        );
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_asset(id: i64, coin: &str, funding: f64, mark: f64, eligible: bool) -> ExchangeAsset {
        ExchangeAsset {
            id,
            symbol: format!("{coin}-USDC"),
            coin: coin.to_string(),
            asset_index: id as u32,
            sz_decimals: 2,
            tick_size: 0.01,
            lot_step_size: 0.01,
            max_leverage: 20.0,
            mark_price: mark,
            mid_price: mark,
            oracle_price: mark,
            funding_rate_hourly: funding,
            day_volume_usdc: 10_000_000.0,
            open_interest_usdc: 4_000_000.0,
            prev_day_price: mark,
            is_core_eligible: eligible,
            is_event_eligible: eligible,
            is_priority_watched: false,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_000_000,
        }
    }

    fn make_downtrend_bars(
        asset_id: i64,
        start: f64,
        step_pct: f64,
        count: usize,
    ) -> Vec<OhlcvBar> {
        let mut bars = Vec::with_capacity(count);
        let mut px = start;
        for i in 0..count {
            let open = px;
            px *= 1.0 + step_pct;
            let close = px;
            bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 14_400_000,
                open,
                high: open.max(close) * 1.004,
                low: open.min(close) * 0.996,
                close,
                volume: 100_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        bars
    }

    #[test]
    fn test_funding_zscore_excludes_ineligible_and_blocks_ineligible_triggers() {
        let a1 = test_asset(1, "BTC", 0.00001, 90_000.0, true);
        let a2 = test_asset(2, "ETH", 0.00002, 3_000.0, true);
        let a3 = test_asset(3, "SOL", -0.00020, 150.0, true);
        let dead = test_asset(4, "DEAD", 0.05000, 1.0, false);

        let bars = HashMap::new();
        let with_dead =
            evaluate_sleeve_s3(&[a1.clone(), a2.clone(), a3.clone(), dead], &bars, 0.25);
        let without_dead = evaluate_sleeve_s3(&[a1, a2, a3], &bars, 0.25);

        assert!(
            (with_dead[&3].s3_funding_zscore - without_dead[&3].s3_funding_zscore).abs() < 1e-6
        );
        assert_eq!(with_dead[&4].event_trigger, None);
        assert_eq!(with_dead[&4].w_s3_event_reversion, 0.0);
    }

    #[test]
    fn test_bottom_reversion_requires_confirmed_bounce_and_monotonic_flush() {
        // 1. Falling knife (red candle, no green bounce, mild flush > -2.0 ATR) must NOT trigger CAPITULATION_FLUSH
        let mut knife_bars = make_downtrend_bars(1, 100.0, -0.008, 25);
        let prev_c = knife_bars.last().unwrap().close;
        let red_close = prev_c * 0.994;
        knife_bars.push(OhlcvBar {
            id: 26,
            asset_id: 1,
            timeframe: "4h".to_string(),
            timestamp_ms: 1_700_000_000_000 + 25 * 14_400_000,
            open: prev_c,
            high: prev_c * 1.001,
            low: red_close * 0.990,
            close: red_close,
            volume: 100_000.0,
            funding_rate: 0.0,
            created_at_ms: 0,
            updated_at_ms: 0,
        });
        let knife_asset = test_asset(1, "KNIFE", 0.0, red_close, true);
        let mut map = HashMap::new();
        map.insert(1, knife_bars);
        let out_knife = evaluate_sleeve_s3(&[knife_asset], &map, 0.25);
        if out_knife[&1].s3_flush_atr_mult > -2.0 {
            assert_eq!(out_knife[&1].event_trigger, None);
            assert_eq!(out_knife[&1].w_s3_event_reversion, 0.0);
        }

        // 2. Confirmed green bounce at range bottom: deeper flush (-2.6 ATR) gets >= weight of shallower flush (-1.4 ATR)
        let mut shallow_bars = make_downtrend_bars(2, 100.0, -0.005, 25);
        let s_prev = shallow_bars.last().unwrap().close;
        let s_close = s_prev * 1.005;
        shallow_bars.push(OhlcvBar {
            id: 26,
            asset_id: 2,
            timeframe: "4h".to_string(),
            timestamp_ms: 1_700_000_000_000 + 25 * 14_400_000,
            open: s_prev,
            high: s_close * 1.002,
            low: s_prev * 0.998,
            close: s_close,
            volume: 100_000.0,
            funding_rate: 0.0,
            created_at_ms: 0,
            updated_at_ms: 0,
        });

        let mut deep_bars = make_downtrend_bars(3, 100.0, -0.018, 25);
        let d_prev = deep_bars.last().unwrap().close;
        let d_close = d_prev * 1.005;
        deep_bars.push(OhlcvBar {
            id: 26,
            asset_id: 3,
            timeframe: "4h".to_string(),
            timestamp_ms: 1_700_000_000_000 + 25 * 14_400_000,
            open: d_prev,
            high: d_close * 1.002,
            low: d_prev * 0.998,
            close: d_close,
            volume: 100_000.0,
            funding_rate: 0.0,
            created_at_ms: 0,
            updated_at_ms: 0,
        });

        let a_shallow = test_asset(2, "SHAL", 0.0, s_close, true);
        let a_deep = test_asset(3, "DEEP", 0.0, d_close, true);
        let mut both_bars = HashMap::new();
        both_bars.insert(2, shallow_bars);
        both_bars.insert(3, deep_bars);
        let out_both = evaluate_sleeve_s3(&[a_shallow, a_deep], &both_bars, 0.25);
        assert_eq!(
            out_both[&2].event_trigger.as_deref(),
            Some(TRIGGER_CAPITULATION_FLUSH)
        );
        assert_eq!(
            out_both[&3].event_trigger.as_deref(),
            Some(TRIGGER_CAPITULATION_FLUSH)
        );
        assert!(
            out_both[&3].w_s3_event_reversion >= out_both[&2].w_s3_event_reversion,
            "Deeper flush ({}) must not get halved below shallower bounce ({})",
            out_both[&3].w_s3_event_reversion,
            out_both[&2].w_s3_event_reversion
        );
    }
}
