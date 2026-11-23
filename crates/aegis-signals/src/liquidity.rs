//! Asset Liquidity, Bar-Liveness & Microstructure Quality Filter (`aegis-signals::liquidity`).
//!
//! Evaluates per-asset tradability before sleeves S1/S2/S3 emit alpha or event triggers:
//! - Rejects coarse tick grids (`one_tick_bps > 15.0`, e.g., `MEME` at `33 bps`).
//! - Computes unfloored 14-period ATR (no `1.2%` floor) to reject dead flat ranges and ensure
//!   realizable volatility clears execution friction (`raw_atr_bps >= (one_tick_bps * 2.5).max(35.0)`).
//! - Measures trailing 12-bar activity (`volume > 0` and `high > low`) to reject ghost/sparse books
//!   (`kSHIB` `3/12`, `EIGEN` `0/12`), and produces a smooth `[0.0, 1.0]` `activity_scale` attenuation.

use aegis_core::indicators::{
    compute_one_tick_bps_opt, compute_unfloored_atr as core_compute_unfloored_atr,
};
use aegis_core::{ExchangeAsset, OhlcvBar};
use serde::{Deserialize, Serialize};

pub const MAX_ONE_TICK_BPS: f64 = 15.0;
pub const MIN_ACTIVE_BAR_RATIO: f64 = 0.50;
pub const MIN_RAW_ATR_BPS: f64 = 35.0;
pub const MIN_RAW_ATR_TICK_MULT: f64 = 2.5;
pub const MIN_FALLBACK_DAY_VOLUME_USDC: f64 = 2_500.0;
pub const LIVENESS_WINDOW_BARS: usize = 12;
pub const MIN_BARS_FOR_LIVENESS_GATE: usize = 8;
pub const ATR_LOOKBACK_PERIOD: usize = 14;

/// Serde default helper for sleeve metric `passes_quality` fields.
#[inline]
pub fn default_passes_quality() -> bool {
    true
}

/// Microstructure and bar-liveness quality assessment for a single perpetual asset.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct AssetLiquidityQuality {
    pub active_bars: usize,
    pub window_bars: usize,
    pub active_bar_ratio: f64,
    pub raw_atr: f64,
    pub raw_atr_bps: f64,
    pub one_tick_bps: f64,
    pub activity_scale: f64,
    pub passes_quality: bool,
}

/// Computes unfloored Average True Range (ATR) over up to `period` trailing bars.
///
/// Delegates to `aegis_core::indicators::compute_unfloored_atr` (`R14-M28`).
#[inline]
pub fn compute_unfloored_atr(bars: &[OhlcvBar], period: usize) -> f64 {
    core_compute_unfloored_atr(bars, period)
}

/// Fail-closed single-tick price increment in basis points at `live_px` (`R14-M8`).
/// Returns `None` when `tick_size` or `live_px` is non-finite or non-positive.
#[inline]
pub fn compute_one_tick_bps(asset: &ExchangeAsset, live_px: f64) -> Option<f64> {
    compute_one_tick_bps_opt(asset.tick_size, live_px)
}

/// Counts `(active_bars, window_bars)` over the trailing `LIVENESS_WINDOW_BARS` window.
#[inline]
pub(crate) fn count_trailing_active_bars(bars: &[OhlcvBar]) -> (usize, usize) {
    let window_bars = bars.len().min(LIVENESS_WINDOW_BARS);
    let trailing = &bars[bars.len() - window_bars..];
    let active_bars = trailing
        .iter()
        .filter(|b| {
            b.volume.is_finite()
                && b.volume > 0.0
                && b.high.is_finite()
                && b.low.is_finite()
                && b.high > b.low
        })
        .count();
    (active_bars, window_bars)
}

/// Evaluates bar-liveness, tick granularity, unfloored ATR vs friction, and smooth activity scaling
/// from precomputed bar indicators (`raw_atr`, `active_bars`, `window_bars`).
pub(crate) fn evaluate_asset_liquidity_quality_with_bar_stats(
    asset: &ExchangeAsset,
    bars_len: usize,
    last_bar_close: Option<f64>,
    raw_atr: f64,
    active_bars: usize,
    window_bars: usize,
) -> AssetLiquidityQuality {
    let live_px = if asset.mark_price.is_finite() && asset.mark_price > 0.0 {
        asset.mark_price
    } else {
        last_bar_close
            .filter(|c| c.is_finite() && *c > 0.0)
            .unwrap_or(0.0)
    };

    let one_tick_bps_opt = compute_one_tick_bps(asset, live_px);
    let one_tick_bps = one_tick_bps_opt.unwrap_or(0.0);
    let raw_atr_bps = if live_px > 0.0 && raw_atr.is_finite() {
        (raw_atr / live_px) * 10_000.0
    } else {
        0.0
    };

    let active_bar_ratio = if window_bars > 0 {
        (active_bars as f64) / (window_bars as f64)
    } else {
        0.0
    };

    let tick_ok = one_tick_bps_opt.is_some_and(|bps| bps <= MAX_ONE_TICK_BPS);
    let min_atr_bps = (one_tick_bps * MIN_RAW_ATR_TICK_MULT).max(MIN_RAW_ATR_BPS);
    let passes_quality = if live_px <= 0.0 || !tick_ok {
        false
    } else if bars_len >= MIN_BARS_FOR_LIVENESS_GATE {
        active_bar_ratio >= MIN_ACTIVE_BAR_RATIO
            && raw_atr_bps.is_finite()
            && raw_atr_bps >= min_atr_bps
    } else {
        asset.day_volume_usdc.is_finite() && asset.day_volume_usdc >= MIN_FALLBACK_DAY_VOLUME_USDC
    };

    let activity_scale = if live_px <= 0.0 || !tick_ok {
        0.0
    } else {
        let bar_factor = if bars_len >= MIN_BARS_FOR_LIVENESS_GATE {
            (active_bars as f64 / 10.0).clamp(0.0, 1.0)
        } else {
            0.75
        };
        let vol_factor = if asset.day_volume_usdc.is_finite() && asset.day_volume_usdc > 0.0 {
            (asset.day_volume_usdc / 25_000.0).clamp(0.45, 1.0)
        } else {
            0.45
        };
        (bar_factor * vol_factor).clamp(0.0, 1.0)
    };

    AssetLiquidityQuality {
        active_bars,
        window_bars,
        active_bar_ratio,
        raw_atr,
        raw_atr_bps,
        one_tick_bps,
        activity_scale,
        passes_quality,
    }
}

/// Evaluates bar-liveness, tick granularity, unfloored ATR vs friction, and smooth activity scaling
/// for a single `ExchangeAsset` and its historical OHLCV bars.
pub fn evaluate_asset_liquidity_quality(
    asset: &ExchangeAsset,
    bars: &[OhlcvBar],
) -> AssetLiquidityQuality {
    let raw_atr = compute_unfloored_atr(bars, ATR_LOOKBACK_PERIOD);
    let (active_bars, window_bars) = count_trailing_active_bars(bars);
    evaluate_asset_liquidity_quality_with_bar_stats(
        asset,
        bars.len(),
        bars.last().map(|b| b.close),
        raw_atr,
        active_bars,
        window_bars,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_asset(
        coin: &str,
        mark_price: f64,
        tick_size: f64,
        day_volume_usdc: f64,
    ) -> ExchangeAsset {
        ExchangeAsset {
            id: 1,
            symbol: format!("{coin}-USDC"),
            coin: coin.to_string(),
            asset_index: 1,
            sz_decimals: 2,
            tick_size,
            lot_step_size: 0.01,
            max_leverage: 20.0,
            mark_price,
            mid_price: mark_price,
            oracle_price: mark_price,
            funding_rate_hourly: 0.0,
            day_volume_usdc,
            open_interest_usdc: day_volume_usdc * 0.5,
            prev_day_price: mark_price,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_000_000,
        }
    }

    fn mk_bars(
        total: usize,
        active_tail_count: usize,
        base_px: f64,
        bar_range_bps: f64,
        vol: f64,
    ) -> Vec<OhlcvBar> {
        let half_frac = (bar_range_bps / 10_000.0) * 0.5;
        let window_start = total.saturating_sub(LIVENESS_WINDOW_BARS);
        let active_start = total.saturating_sub(active_tail_count);
        (0..total)
            .map(|i| {
                let is_active = if i >= window_start {
                    i >= active_start
                } else {
                    active_tail_count > 0
                };
                let (high, low, volume) = if is_active {
                    (
                        base_px * (1.0 + half_frac),
                        base_px * (1.0 - half_frac),
                        vol,
                    )
                } else {
                    (base_px, base_px, 0.0)
                };
                OhlcvBar {
                    id: (i + 1) as i64,
                    asset_id: 1,
                    timeframe: "1h".to_string(),
                    timestamp_ms: 1_700_000_000_000 + (i as i64) * 3_600_000,
                    open: base_px,
                    high,
                    low,
                    close: base_px,
                    volume,
                    funding_rate: 0.0,
                    created_at_ms: 0,
                    updated_at_ms: 0,
                }
            })
            .collect()
    }

    #[test]
    fn test_kshib_sparse_active_bars_fails_quality() {
        let kshib = mk_asset("kSHIB", 0.012, 0.000001, 12_000.0);
        let bars = mk_bars(20, 3, 0.012, 80.0, 50_000.0);
        let q = evaluate_asset_liquidity_quality(&kshib, &bars);
        assert_eq!(q.window_bars, 12);
        assert_eq!(q.active_bars, 3);
        assert!((q.active_bar_ratio - 0.25).abs() < 1e-9);
        assert!(!q.passes_quality, "kSHIB with 3/12 active bars must fail");
    }

    #[test]
    fn test_eigen_zero_active_bars_fails_quality() {
        let eigen = mk_asset("EIGEN", 1.85, 0.0001, 8_000.0);
        let bars = mk_bars(20, 0, 1.85, 80.0, 10_000.0);
        let q = evaluate_asset_liquidity_quality(&eigen, &bars);
        assert_eq!(q.window_bars, 12);
        assert_eq!(q.active_bars, 0);
        assert_eq!(q.active_bar_ratio, 0.0);
        assert_eq!(q.raw_atr, 0.0);
        assert_eq!(q.raw_atr_bps, 0.0);
        assert_eq!(q.activity_scale, 0.0);
        assert!(!q.passes_quality, "EIGEN with 0/12 active bars must fail");
    }

    #[test]
    fn test_meme_coarse_tick_33bps_fails_quality() {
        // 0.00001 tick on $0.00303 price => ~33.0 bps per tick (> 15 bps cap)
        let meme = mk_asset("MEME", 0.00303, 0.00001, 500_000.0);
        let bars = mk_bars(20, 12, 0.00303, 120.0, 1_000_000.0);
        let q = evaluate_asset_liquidity_quality(&meme, &bars);
        assert!(q.one_tick_bps > 32.0 && q.one_tick_bps < 34.0);
        assert!(
            !q.passes_quality,
            "MEME with 33 bps tick must fail even with active bars"
        );

        // Also fails in the < 8 bars fallback branch
        let q_short = evaluate_asset_liquidity_quality(&meme, &[]);
        assert!(!q_short.passes_quality);
    }

    #[test]
    fn test_dead_flat_range_10bps_atr_fails_quality() {
        let flat = mk_asset("FLAT", 10.0, 0.001, 100_000.0);
        let bars = mk_bars(20, 12, 10.0, 10.0, 5_000.0);
        let q = evaluate_asset_liquidity_quality(&flat, &bars);
        assert_eq!(q.active_bars, 12);
        assert!((q.raw_atr_bps - 10.0).abs() < 1e-6);
        assert!(
            !q.passes_quality,
            "Flat 10 bps ATR must fail (unfloored ATR < 35 bps)"
        );
    }

    #[test]
    fn test_healthy_btc_arb_inj_pass_quality() {
        let cases = [
            ("BTC", 92_000.0, 1.0, 65_000_000.0, 65.0),
            ("ARB", 0.72, 0.0001, 1_200_000.0, 85.0),
            ("INJ", 18.50, 0.001, 850_000.0, 95.0),
        ];
        for (coin, px, tick, vol, atr_bps) in cases {
            let asset = mk_asset(coin, px, tick, vol);
            let bars = mk_bars(20, 12, px, atr_bps, 25_000.0);
            let q = evaluate_asset_liquidity_quality(&asset, &bars);
            assert!(
                q.passes_quality,
                "{coin} should pass quality (one_tick_bps={}, raw_atr_bps={}, active={}/{})",
                q.one_tick_bps, q.raw_atr_bps, q.active_bars, q.window_bars
            );
            assert_eq!(q.active_bars, 12);
            assert!((q.raw_atr_bps - atr_bps).abs() < 1e-6);
            assert!((q.activity_scale - 1.0).abs() < 1e-9);
        }
    }

    #[test]
    fn test_short_bar_fallback_and_tick_multiple_and_activity_scale() {
        // Liquid fixture with empty bars passes fallback
        let liq = mk_asset("SOL", 150.0, 0.01, 10_000_000.0);
        let q_liq = evaluate_asset_liquidity_quality(&liq, &[]);
        assert!(q_liq.passes_quality);
        assert!((q_liq.activity_scale - 0.75).abs() < 1e-9);

        // Ghost $79 book with empty bars fails fallback
        let ghost = mk_asset("WIF", 0.85, 0.0001, 79.0);
        let q_ghost = evaluate_asset_liquidity_quality(&ghost, &[]);
        assert!(!q_ghost.passes_quality);

        // Coarse tick near cap (14.5 bps) requires ATR >= 14.5 * 2.5 = 36.25 bps
        let near_cap = mk_asset("NEAR", 1.0, 0.00145, 50_000.0);
        let bars_low_atr = mk_bars(20, 12, 1.0, 35.5, 5_000.0);
        let bars_ok_atr = mk_bars(20, 12, 1.0, 40.0, 5_000.0);
        assert!(!evaluate_asset_liquidity_quality(&near_cap, &bars_low_atr).passes_quality);
        assert!(evaluate_asset_liquidity_quality(&near_cap, &bars_ok_atr).passes_quality);

        // Smooth activity_scale on 6/12 active bars & $5k volume: (6/10) * 0.45 = 0.27
        let thin = mk_asset("THIN", 5.0, 0.001, 5_000.0);
        let thin_bars = mk_bars(20, 6, 5.0, 140.0, 1_000.0);
        let q_thin = evaluate_asset_liquidity_quality(&thin, &thin_bars);
        assert!(q_thin.passes_quality);
        assert!((q_thin.activity_scale - 0.27).abs() < 1e-9);
    }

    #[test]
    fn test_r14_m8_zero_nan_tick_size_fails_closed() {
        let healthy_bars = mk_bars(20, 12, 100.0, 80.0, 50_000.0);
        for bad_tick in [0.0, -0.01, f64::NAN, f64::INFINITY] {
            let bad_asset = mk_asset("BADTICK", 100.0, bad_tick, 1_000_000.0);
            assert_eq!(compute_one_tick_bps(&bad_asset, 100.0), None);
            let q = evaluate_asset_liquidity_quality(&bad_asset, &healthy_bars);
            assert!(!q.passes_quality, "tick={bad_tick} must fail quality");
            assert_eq!(q.activity_scale, 0.0);
            assert!(q.one_tick_bps.is_finite());
        }
        for bad_px in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let asset = mk_asset("BADPX", bad_px, 0.01, 1_000_000.0);
            assert_eq!(compute_one_tick_bps(&asset, bad_px), None);
            let q = evaluate_asset_liquidity_quality(&asset, &[]);
            assert!(!q.passes_quality, "mark={bad_px} must fail quality");
            assert_eq!(q.activity_scale, 0.0);
        }
    }
}
