//! Sleeve 2: Breadth-Confirmed Multi-Speed Trend & Live 3s Breakout (`aegis-signals::trend_breakout`).
//!
//! Computes `s2_trend_alignment` (`-1`, `0`, `+1`), `s2_vol_surge_ratio`, `w_s2_trend_breakout`,
//! and detects `"LIVE_BREAKOUT_SURGE"` events.

use crate::breadth::RegimeGateState;
use crate::features::{
    compute_range_bounds_and_rsi_tail, compute_rsi_and_range_from_parts, ema_step,
    CycleFeatureCache, PrecomputedAssetFeatures,
};
use crate::liquidity::{
    compute_unfloored_atr, default_passes_quality, evaluate_asset_liquidity_quality,
};
use aegis_core::indicators::{causal_ema as core_causal_ema, infer_bar_hours, top_k_normalized};
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
    #[serde(default = "default_passes_quality")]
    pub passes_quality: bool,
}

/// Computes a strictly causal Exponential Moving Average (`alpha = 2 / (span + 1)`).
/// Delegates to `aegis_core::indicators::causal_ema` (`R14-M28`).
pub fn causal_ema(series: &[f64], span: f64) -> Vec<f64> {
    core_causal_ema(series, span)
}

/// Computes causal 14-period RSI (`[0.0, 100.0]`), lookback range position (`[0.0, 1.0]`),
/// and whether price is bouncing off the bottom quartile of its range.
pub fn compute_rsi_and_range_position(bars: &[OhlcvBar], live_px: f64) -> (f64, f64, bool) {
    if bars.is_empty() || !live_px.is_finite() || live_px <= 0.0 {
        return (50.0, 0.5, false);
    }
    let (min_low, max_high, tail, tail_len) = compute_range_bounds_and_rsi_tail(bars);
    compute_rsi_and_range_from_parts(
        bars.len(),
        bars.last().map(|b| b.open),
        bars.last().map(|b| b.close),
        min_low,
        max_high,
        &tail[..tail_len],
        live_px,
    )
}

/// Computes multi-speed trend alignment (`-1`, `0`, `+1`), volume surge ratio, and live breakout detection
/// for a single asset.
pub fn compute_asset_trend_breakout(
    asset: &ExchangeAsset,
    bars: &[OhlcvBar],
) -> (i32, f64, f64, Option<String>) {
    compute_asset_trend_breakout_with_features(asset, bars, None)
}

pub(crate) fn compute_asset_trend_breakout_with_features(
    asset: &ExchangeAsset,
    bars: &[OhlcvBar],
    feat_opt: Option<&PrecomputedAssetFeatures>,
) -> (i32, f64, f64, Option<String>) {
    let bars_len = feat_opt.map(|f| f.bars_len).unwrap_or(bars.len());
    if bars_len >= 10 {
        let last_bar_close = feat_opt
            .and_then(|f| f.last_bar_close)
            .or_else(|| bars.last().map(|b| b.close));
        let last_bar_open = feat_opt
            .and_then(|f| f.last_bar_open)
            .or_else(|| bars.last().map(|b| b.open));

        let tick_tol = if asset.tick_size.is_finite() && asset.tick_size > 0.0 {
            asset.tick_size * 0.5
        } else {
            1e-12
        };
        let append_mark = asset.mark_price.is_finite()
            && asset.mark_price > 0.0
            && last_bar_close.is_none_or(|c| (asset.mark_price - c).abs() > tick_tol);

        let (mult, atr_4h_scale) = if let Some(feat) = feat_opt {
            (feat.norm_mult, feat.atr_4h_scale)
        } else {
            let bar_hours = infer_bar_hours(bars);
            (
                (4.0 / bar_hours).clamp(0.25, 16.0),
                (4.0 / bar_hours).sqrt().clamp(0.5, 4.0),
            )
        };

        let (mut f, mut m, mut s) = if let Some(feat) = feat_opt {
            (feat.ema8_bars, feat.ema21_bars, feat.ema55_bars)
        } else {
            let init = bars[0].close.max(1e-12);
            let mut e8 = if init.is_finite() { init } else { 0.0 };
            let mut e21 = e8;
            let mut e55 = e8;
            for b in &bars[1..] {
                let c = b.close.max(1e-12);
                e8 = ema_step(e8, c, 8.0 * mult);
                e21 = ema_step(e21, c, 21.0 * mult);
                e55 = ema_step(e55, c, 55.0 * mult);
            }
            (e8, e21, e55)
        };

        let live_px = if append_mark {
            f = ema_step(f, asset.mark_price, 8.0 * mult);
            m = ema_step(m, asset.mark_price, 21.0 * mult);
            s = ema_step(s, asset.mark_price, 55.0 * mult);
            asset.mark_price
        } else {
            last_bar_close.map(|c| c.max(1e-12)).unwrap_or(1e-12)
        };
        let s = s.max(1e-12);

        let alignment = if f > m && m > s {
            1
        } else if f < m && m < s {
            -1
        } else {
            0
        };

        let vol_floor = (100.0 / live_px.max(1e-8)).max(1.0);
        let (active_vol_sum, active_vol_count, last_vol) = if let Some(feat) = feat_opt {
            (
                feat.avg_vol_20_sum,
                feat.avg_vol_20_count,
                feat.last_bar_vol,
            )
        } else {
            let vol_cap = (20.0 * mult).round().max(1.0) as usize;
            let lookback = bars.len().min(vol_cap);
            let recent_bars = &bars[bars.len() - lookback..];
            let mut sum = 0.0;
            let mut count = 0usize;
            for b in recent_bars {
                if b.volume.is_finite() && b.volume > 0.0 {
                    sum += b.volume;
                    count += 1;
                }
            }
            let lv = recent_bars
                .last()
                .map(|b| {
                    if b.volume.is_finite() && b.volume > 0.0 {
                        b.volume
                    } else {
                        0.0
                    }
                })
                .unwrap_or(0.0);
            (sum, count, lv)
        };
        let avg_vol = if active_vol_count > 0 {
            (active_vol_sum / (active_vol_count as f64)).max(vol_floor)
        } else {
            vol_floor
        };

        let (channel_high, channel_low) = if let Some(feat) = feat_opt {
            (feat.channel_high_18, feat.channel_low_18)
        } else {
            let max_chan = (18.0 * mult).round().max(4.0) as usize;
            let chan_len = bars.len().saturating_sub(1).clamp(4, max_chan);
            let prior_slice = &bars[bars.len() - 1 - chan_len..bars.len() - 1];
            let ch = prior_slice.iter().map(|b| b.high).fold(f64::MIN, f64::max);
            let cl = prior_slice.iter().map(|b| b.low).fold(f64::MAX, f64::min);
            (ch, cl)
        };

        let raw_atr = feat_opt
            .map(|feat| feat.quality.raw_atr)
            .unwrap_or_else(|| compute_unfloored_atr(bars, 14));
        let raw_atr_4h = raw_atr * atr_4h_scale;
        let high_margin = (0.20 * raw_atr_4h).max(channel_high * 0.0015);
        let low_margin = (0.20 * raw_atr_4h).max(channel_low * 0.0015);
        let broke_high = channel_high > 0.0 && live_px > channel_high + high_margin;
        let broke_low = channel_low > 0.0 && live_px < channel_low - low_margin;

        let mut surge_ratio = (last_vol / avg_vol.max(1e-8)).clamp(0.25, 6.0);
        if (alignment > 0 && broke_high) || (alignment < 0 && broke_low) {
            surge_ratio = surge_ratio.max(1.85);
        }
        let surge_ratio = round_dp(surge_ratio, 4);

        let (rsi_14, range_pos, bouncing_from_low) = if let Some(feat) = feat_opt {
            if (live_px - feat.live_px).abs() <= 1e-12 {
                (feat.rsi_14, feat.range_pos, feat.bouncing_from_low)
            } else {
                compute_rsi_and_range_from_parts(
                    feat.bars_len,
                    feat.last_bar_open,
                    feat.last_bar_close,
                    feat.min_low_72,
                    feat.max_high_72,
                    &feat.rsi_tail_closes[..feat.rsi_tail_len],
                    live_px,
                )
            }
        } else {
            compute_rsi_and_range_position(bars, live_px)
        };

        if alignment < 0 && (rsi_14 < 36.0 || range_pos < 0.22 || bouncing_from_low) {
            return (0, surge_ratio, 0.0, None);
        }
        if alignment > 0
            && rsi_14 > 76.0
            && range_pos > 0.90
            && last_bar_open.is_some_and(|open| live_px < open)
        {
            return (0, surge_ratio, 0.0, None);
        }

        let is_breakout_surge = (alignment == 1
            && (broke_high
                || (surge_ratio >= 1.75
                    && range_pos >= 0.65
                    && last_bar_open.is_some_and(|open| live_px >= open))))
            || (alignment == -1
                && (broke_low
                    || (surge_ratio >= 1.75
                        && (0.22..=0.35).contains(&range_pos)
                        && last_bar_open.is_some_and(|open| live_px <= open))));
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

/// Evaluates Sleeve 2 using precomputed [`CycleFeatureCache`] and macro [`RegimeGateState`].
pub(crate) fn evaluate_sleeve_s2_with_features<B: AsRef<[OhlcvBar]>>(
    assets: &[ExchangeAsset],
    bars_by_asset: &HashMap<i64, B>,
    features: &CycleFeatureCache,
    regime_gate: &RegimeGateState,
    market_breadth_bull: f64,
    s2_budget: f64,
    top_k: usize,
) -> HashMap<i64, TrendBreakoutMetrics> {
    evaluate_sleeve_s2_inner(
        assets,
        bars_by_asset,
        Some(features),
        regime_gate,
        market_breadth_bull,
        s2_budget,
        top_k,
    )
}

/// Evaluates Sleeve 2 (Breadth-Confirmed Multi-Speed Trend & Live 3s Breakout) across the universe.
pub fn evaluate_sleeve_s2<B: AsRef<[OhlcvBar]>>(
    assets: &[ExchangeAsset],
    bars_by_asset: &HashMap<i64, B>,
    market_breadth_bull: f64,
    s2_budget: f64,
) -> HashMap<i64, TrendBreakoutMetrics> {
    evaluate_sleeve_s2_inner(
        assets,
        bars_by_asset,
        None,
        &RegimeGateState::default(),
        market_breadth_bull,
        s2_budget,
        8,
    )
}

fn evaluate_sleeve_s2_inner<B: AsRef<[OhlcvBar]>>(
    assets: &[ExchangeAsset],
    bars_by_asset: &HashMap<i64, B>,
    features_opt: Option<&CycleFeatureCache>,
    regime_gate: &RegimeGateState,
    market_breadth_bull: f64,
    s2_budget: f64,
    top_k: usize,
) -> HashMap<i64, TrendBreakoutMetrics> {
    struct RawS2Item {
        asset_id: i64,
        symbol: String,
        day_volume_usdc: f64,
        s2_trend_alignment: i32,
        s2_vol_surge_ratio: f64,
        confirmed_score: f64,
        event_trigger: Option<String>,
        passes_quality: bool,
    }

    let budget = s2_budget.clamp(0.0, 1.50);
    let breadth = market_breadth_bull.clamp(0.0, 1.0);

    let bull_breadth_mult = if regime_gate.block_new_longs {
        0.0
    } else {
        (0.65 + 0.70 * breadth).clamp(0.50, 1.35)
    };
    let bear_breadth_mult = if regime_gate.block_new_shorts {
        0.0
    } else {
        (0.65 + 0.70 * (1.0 - breadth)).clamp(0.50, 1.35)
    };

    let mut raw_items = Vec::with_capacity(assets.len());

    for asset in assets {
        let bars = bars_by_asset
            .get(&asset.id)
            .map(AsRef::as_ref)
            .unwrap_or(&[]);
        let feat_opt = features_opt.and_then(|f| f.per_asset.get(&asset.id));
        let quality = feat_opt
            .map(|f| f.quality)
            .unwrap_or_else(|| evaluate_asset_liquidity_quality(asset, bars));
        let (alignment, surge_ratio, raw_score, trigger) =
            compute_asset_trend_breakout_with_features(asset, bars, feat_opt);

        let eligible = quality.passes_quality
            && (asset.is_core_eligible || (asset.is_event_eligible && trigger.is_some()));
        let blocked_dir = (alignment > 0 && regime_gate.block_new_longs)
            || (alignment < 0 && regime_gate.block_new_shorts);
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
        let emitted_trigger = if eligible && !blocked_dir {
            trigger
        } else {
            None
        };

        raw_items.push(RawS2Item {
            asset_id: asset.id,
            symbol: asset.symbol.clone(),
            day_volume_usdc: asset.day_volume_usdc,
            s2_trend_alignment: alignment,
            s2_vol_surge_ratio: surge_ratio,
            confirmed_score,
            event_trigger: emitted_trigger,
            passes_quality: quality.passes_quality,
        });
    }

    let scored: Vec<(i64, f64, f64)> = raw_items
        .iter()
        .map(|item| (item.asset_id, item.confirmed_score, item.day_volume_usdc))
        .collect();
    let weights = top_k_normalized(&scored, top_k.max(1), budget);
    let mut out = HashMap::with_capacity(raw_items.len());

    for item in raw_items {
        let w_s2 = weights.get(&item.asset_id).copied().unwrap_or(0.0);
        out.insert(
            item.asset_id,
            TrendBreakoutMetrics {
                asset_id: item.asset_id,
                symbol: item.symbol,
                s2_trend_alignment: item.s2_trend_alignment,
                s2_vol_surge_ratio: item.s2_vol_surge_ratio,
                w_s2_trend_breakout: w_s2,
                event_trigger: item.event_trigger,
                passes_quality: item.passes_quality,
            },
        );
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_asset(id: i64, mark: f64) -> ExchangeAsset {
        ExchangeAsset {
            id,
            symbol: "TST-USDC".to_string(),
            coin: "TST".to_string(),
            asset_index: id as u32,
            sz_decimals: 2,
            tick_size: 0.01,
            lot_step_size: 0.01,
            max_leverage: 20.0,
            mark_price: mark,
            mid_price: mark,
            oracle_price: mark,
            funding_rate_hourly: 0.0,
            day_volume_usdc: 10_000_000.0,
            open_interest_usdc: 4_000_000.0,
            prev_day_price: mark,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn test_breakdown_below_prior_completed_bars_is_reachable() {
        // 20 completed bars trending gently down from 110 -> 100, then final forming bar breaks down to 97.5
        let mut bars = Vec::with_capacity(21);
        let mut px = 110.0;
        for i in 0..20 {
            let open = px;
            px -= 0.5;
            let close = px;
            bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 1,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 14_400_000,
                open,
                high: open + 0.3,
                low: close - 0.2,
                close,
                volume: 10_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        // Forming bar breaks well below prior min_low (100.3) even when live_px == bar.close == 97.5
        bars.push(OhlcvBar {
            id: 21,
            asset_id: 1,
            timeframe: "4h".to_string(),
            timestamp_ms: 1_700_000_000_000 + 20 * 14_400_000,
            open: 100.5,
            high: 100.6,
            low: 97.0,
            close: 97.5,
            volume: 25_000.0,
            funding_rate: 0.0,
            created_at_ms: 0,
            updated_at_ms: 0,
        });

        let (rsi, range_pos, bouncing) = compute_rsi_and_range_position(&bars, 97.5);
        assert_eq!(rsi, 50.0);
        assert!((range_pos - 0.30).abs() < 1e-9);
        assert!(!bouncing);

        let asset = test_asset(1, 97.5);
        let (align, _surge, score, trigger) = compute_asset_trend_breakout(&asset, &bars);
        assert_eq!(align, -1);
        assert!(score < 0.0);
        assert_eq!(trigger.as_deref(), Some(TRIGGER_LIVE_BREAKOUT_SURGE));
    }

    #[test]
    fn test_red_falling_knife_with_lower_wick_not_flagged_as_bounce() {
        let mut bars = Vec::with_capacity(20);
        let mut px = 120.0;
        for i in 0..19 {
            let open = px;
            px *= 0.99;
            bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 1,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 14_400_000,
                open,
                high: open * 1.003,
                low: px * 0.997,
                close: px,
                volume: 10_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        let prior_min_low = bars.iter().map(|b| b.low).fold(f64::MAX, f64::min);
        let prev_close = bars.last().unwrap().close;
        // Crashing red candle: opens at prev_close, closes down at prior_min_low * 1.004 (so >= min_low * 1.003)
        // with a 0.5% lower wick — MUST NOT set bouncing_from_low = true
        let red_close = prior_min_low * 1.004;
        let mut crash_bars = bars.clone();
        crash_bars.push(OhlcvBar {
            id: 20,
            asset_id: 1,
            timeframe: "4h".to_string(),
            timestamp_ms: 1_700_000_000_000 + 19 * 14_400_000,
            open: prev_close * 1.02,
            high: prev_close * 1.022,
            low: red_close * 0.995,
            close: red_close,
            volume: 10_000.0,
            funding_rate: 0.0,
            created_at_ms: 0,
            updated_at_ms: 0,
        });
        let (_, _, red_bouncing) = compute_rsi_and_range_position(&crash_bars, red_close);
        assert!(
            !red_bouncing,
            "Red falling knife must not trigger bouncing_from_low"
        );

        // Confirmed green bounce candle at range bottom (live_px > open * 1.001) DOES set bouncing = true
        let green_open = prior_min_low * 1.002;
        let green_close = green_open * 1.005;
        let mut bounce_bars = bars;
        bounce_bars.push(OhlcvBar {
            id: 20,
            asset_id: 1,
            timeframe: "4h".to_string(),
            timestamp_ms: 1_700_000_000_000 + 19 * 14_400_000,
            open: green_open,
            high: green_close * 1.002,
            low: prior_min_low,
            close: green_close,
            volume: 10_000.0,
            funding_rate: 0.0,
            created_at_ms: 0,
            updated_at_ms: 0,
        });
        let (_, _, green_bouncing) = compute_rsi_and_range_position(&bounce_bars, green_close);
        assert!(
            green_bouncing,
            "Confirmed green bounce should set bouncing_from_low"
        );
    }

    #[test]
    fn test_no_duplicate_mark_price_push_when_matching_last_close() {
        let mut bars = Vec::with_capacity(20);
        let mut px = 100.0;
        for i in 0..20 {
            let open = px;
            px *= 1.01;
            bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 1,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 14_400_000,
                open,
                high: px * 1.002,
                low: open * 0.998,
                close: px,
                volume: 10_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        let last_close = bars.last().unwrap().close;
        let asset_exact = test_asset(1, last_close);
        let asset_tiny_diff = test_asset(1, last_close + 0.002); // <= tick_size(0.01) * 0.5
        let out_exact = compute_asset_trend_breakout(&asset_exact, &bars);
        let out_tiny = compute_asset_trend_breakout(&asset_tiny_diff, &bars);
        assert_eq!(out_exact.0, out_tiny.0);
        assert!((out_exact.2 - out_tiny.2).abs() < 1e-12);
    }

    #[test]
    fn test_s2_blocks_fake_volume_surge_single_tick_bounce_and_dead_books() {
        // 1. 19 empty bars + 1 tiny print ($8 notional on $0.008 coin) must NOT max surge_ratio to 6.0
        let mut sparse_bars = Vec::with_capacity(20);
        let mut px = 0.008;
        for i in 0..20 {
            if i == 19 {
                px *= 1.0005;
            }
            sparse_bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 10,
                timeframe: "1h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 3_600_000,
                open: 0.008,
                high: px,
                low: 0.008,
                close: px,
                volume: if i == 19 { 1_000.0 } else { 0.0 },
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        let mut sparse_asset = test_asset(10, px);
        sparse_asset.tick_size = 0.000001;
        sparse_asset.day_volume_usdc = 5_000.0;
        let (_align, surge, _score, trig) =
            compute_asset_trend_breakout(&sparse_asset, &sparse_bars);
        assert!(
            surge <= 1.0,
            "19 empty bars + $8 print must not produce fake surge (got {surge})"
        );
        assert_eq!(trig, None);

        let mut map = HashMap::new();
        map.insert(10, sparse_bars);
        let s2_sparse = evaluate_sleeve_s2(&[sparse_asset], &map, 0.7, 0.40);
        assert!(!s2_sparse[&10].passes_quality);
        assert_eq!(s2_sparse[&10].w_s2_trend_breakout, 0.0);
        assert_eq!(s2_sparse[&10].event_trigger, None);

        // 2. Breakout requires clearing channel_high + (0.20 * raw_atr).max(channel_high * 0.0015):
        //    a tiny 0.2% poke above channel_high when raw_atr is ~2.0% must NOT fire LIVE_BREAKOUT_SURGE,
        //    whereas clearing channel_high + 0.25 * raw_atr DOES fire LIVE_BREAKOUT_SURGE.
        let mut trend_bars = Vec::with_capacity(20);
        let mut p = 100.0;
        for i in 0..20 {
            let open = p;
            p *= 1.006;
            trend_bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 11,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 14_400_000,
                open,
                high: p * 1.008,
                low: open * 0.992,
                close: p,
                volume: 10_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        let chan_high = trend_bars[1..19]
            .iter()
            .map(|b| b.high)
            .fold(f64::MIN, f64::max);
        let raw_atr = compute_unfloored_atr(&trend_bars, 14);
        assert!(raw_atr > chan_high * 0.015);

        // Tiny poke: > chan_high * 1.0015, but < chan_high + 0.20 * raw_atr (and normal 1.0x volume)
        let small_poke_px = chan_high * 1.002;
        assert!(small_poke_px < chan_high + 0.20 * raw_atr);
        let poke_asset = test_asset(11, small_poke_px);
        let (a_poke, s_poke, _sc_poke, trig_poke) =
            compute_asset_trend_breakout(&poke_asset, &trend_bars);
        assert_eq!(a_poke, 1);
        assert!(s_poke < 1.75);
        assert_eq!(
            trig_poke, None,
            "Sub-0.20*ATR poke must not fire LIVE_BREAKOUT_SURGE"
        );

        // Real follow-through clearing chan_high + 0.30 * raw_atr fires LIVE_BREAKOUT_SURGE
        let real_break_px = chan_high + 0.30 * raw_atr;
        let break_asset = test_asset(11, real_break_px);
        let (a_brk, s_brk, _sc_brk, trig_brk) =
            compute_asset_trend_breakout(&break_asset, &trend_bars);
        assert_eq!(a_brk, 1);
        assert!(s_brk >= 1.85);
        assert_eq!(trig_brk.as_deref(), Some(TRIGGER_LIVE_BREAKOUT_SURGE));
    }

    #[test]
    fn test_s2_1h_wall_clock_normalization_and_breakout_margin() {
        // 80 1h bars rising smoothly so EMA(32) > EMA(84) > EMA(220) and 4h-ref ATR = 2.0 * raw_1h_atr
        let mut bars_1h = Vec::with_capacity(80);
        let mut p = 100.0;
        for i in 0..80 {
            let open = p;
            p *= 1.002;
            bars_1h.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 21,
                timeframe: "1h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 3_600_000,
                open,
                high: p * 1.005,
                low: open * 0.995,
                close: p,
                volume: 10_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        let chan_high = bars_1h[7..79]
            .iter()
            .map(|b| b.high)
            .fold(f64::MIN, f64::max);
        let raw_1h_atr = compute_unfloored_atr(&bars_1h, 14);
        let atr_4h_ref = raw_1h_atr * 2.0;

        // 0.30 * raw_1h_atr is BELOW 0.20 * atr_4h_ref (= 0.40 * raw_1h_atr) -> no breakout
        let sub_break = test_asset(21, chan_high + 0.30 * raw_1h_atr);
        let (a_sub, _, _, t_sub) = compute_asset_trend_breakout(&sub_break, &bars_1h);
        assert_eq!(a_sub, 1);
        assert_eq!(t_sub, None);

        // 0.25 * atr_4h_ref clears 0.20 * atr_4h_ref -> fires LIVE_BREAKOUT_SURGE
        let full_break = test_asset(21, chan_high + 0.25 * atr_4h_ref);
        let (a_full, s_full, _, t_full) = compute_asset_trend_breakout(&full_break, &bars_1h);
        assert_eq!(a_full, 1);
        assert!(s_full >= 1.85);
        assert_eq!(t_full.as_deref(), Some(TRIGGER_LIVE_BREAKOUT_SURGE));
    }
}
