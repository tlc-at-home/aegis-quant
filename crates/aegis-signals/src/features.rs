//! Per-Cycle Feature Cache & Closed-Bar Indicator Memoizer (`aegis-signals::features`, Sec. 8).
//!
//! Eliminates redundant per-tick recomputation across Sleeves S1/S2/S3 and breadth calculation:
//! - Builds `btc_close_by_ts` once per cycle instead of once per asset.
//! - Computes liquidity quality, ATR, stack-allocated RSI/range position, and preliminary breadth in one pass.
//! - Memoizes closed-bar indicators (`annualized_vol`, `market_beta`, `raw_atr`, `active_bars`,
//!   72-bar range bounds, 18-bar channel, 20-bar volume average, and completed-bar EMA states)
//!   across 3-second ticks via [`BarCloseCache`] so live mark updates run in O(1) with zero Vec allocations.

use crate::breadth::{
    classify_market_regime, compute_regime_gate_state, effective_breadth_anchor_price,
    MarketBreadthSnapshot, RegimeGateConfig, RegimeGateState,
};
use crate::liquidity::{
    count_trailing_active_bars, evaluate_asset_liquidity_quality_with_bar_stats,
    AssetLiquidityQuality, ATR_LOOKBACK_PERIOD,
};
use aegis_core::indicators::{
    compute_annualized_vol_from_bars, compute_unfloored_atr, infer_bar_hours, timeframe_to_ms,
};
use aegis_core::{round_dp, ExchangeAsset, OhlcvBar};
use std::collections::HashMap;

/// Per-asset consecutive bar confirmation state for Lever N2 signal-exit hysteresis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExitConfirmState {
    pub count: u32,
    pub last_bar_ts_ms: i64,
}

/// Stack-allocated causal 14-period RSI, 72-bar lookback range position, and bounce detector.
#[inline]
pub(crate) fn compute_rsi_and_range_from_parts(
    bars_len: usize,
    last_bar_open: Option<f64>,
    last_bar_close: Option<f64>,
    min_low: f64,
    max_high: f64,
    tail_closes: &[f64],
    live_px: f64,
) -> (f64, f64, bool) {
    if bars_len == 0 || !live_px.is_finite() || live_px <= 0.0 {
        return (50.0, 0.5, false);
    }

    let span = max_high - min_low;
    let range_pos = if min_low < f64::MAX && max_high > f64::MIN && span > 1e-12 {
        ((live_px - min_low) / span).clamp(0.0, 1.0)
    } else {
        0.5
    };
    if min_low < f64::MAX && live_px < min_low * 0.995 {
        return (50.0, 0.30, false);
    }

    let rsi_14 = if bars_len >= 4 {
        let mut closes = [0.0_f64; 64];
        let copy_len = tail_closes.len().min(63);
        closes[..copy_len].copy_from_slice(&tail_closes[tail_closes.len() - copy_len..]);
        let mut len = copy_len;

        let dup_tol = (live_px * 1e-6).max(1e-12);
        if last_bar_close.is_none_or(|c| (live_px - c).abs() > dup_tol) && len < 64 {
            closes[len] = live_px;
            len += 1;
        }

        let mut gains = 0.0;
        let mut losses = 0.0;
        let diffs = len.saturating_sub(1);
        for pair in closes[..len].windows(2) {
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

    let last_green = last_bar_open.is_some_and(|o| o.is_finite() && o > 0.0 && live_px > o * 1.001);
    let holding_above_swing_low = last_green
        && min_low > 0.0
        && min_low < f64::MAX
        && live_px >= min_low * 1.003
        && range_pos <= 0.20;
    let bouncing_from_low = (last_green && range_pos <= 0.25) || holding_above_swing_low;
    (rsi_14, range_pos, bouncing_from_low)
}

/// Computes completed-bar wall-clock-normalized range bounds and trailing chronological closes on the stack.
#[inline]
pub(crate) fn compute_range_bounds_and_rsi_tail_with_mult(
    bars: &[OhlcvBar],
    mult: f64,
) -> (f64, f64, [f64; 64], usize) {
    if bars.is_empty() {
        return (f64::MAX, f64::MIN, [0.0; 64], 0);
    }
    let eff_mult = if mult.is_finite() && mult > 0.0 {
        mult.clamp(0.25, 16.0)
    } else {
        1.0
    };
    let end = bars.len().saturating_sub(1).max(1);
    let max_range_bars = (72.0 * eff_mult).round().max(1.0) as usize;
    let range_lookback = end.min(max_range_bars);
    let range_slice = &bars[end.saturating_sub(range_lookback)..end];
    let min_low = range_slice
        .iter()
        .filter(|b| b.low.is_finite() && b.close.is_finite())
        .map(|b| b.low.min(b.close))
        .fold(f64::MAX, f64::min);
    let max_high = range_slice
        .iter()
        .filter(|b| b.high.is_finite() && b.close.is_finite())
        .map(|b| b.high.max(b.close))
        .fold(f64::MIN, f64::max);

    let rsi_period = (14.0 * eff_mult).round().clamp(4.0, 62.0) as usize;
    let tail_cap = (rsi_period + 1).min(63);
    let start_tail = bars.len().saturating_sub(tail_cap);
    let mut tail = [0.0_f64; 64];
    let mut tail_len = 0usize;
    for b in &bars[start_tail..] {
        if tail_len < tail_cap {
            tail[tail_len] = b.close.max(1e-12);
            tail_len += 1;
        }
    }
    (min_low, max_high, tail, tail_len)
}

/// Computes completed-bar range bounds and trailing closes using inferred bar duration.
#[inline]
pub(crate) fn compute_range_bounds_and_rsi_tail(bars: &[OhlcvBar]) -> (f64, f64, [f64; 64], usize) {
    let mult = (4.0 / infer_bar_hours(bars)).clamp(0.25, 16.0);
    compute_range_bounds_and_rsi_tail_with_mult(bars, mult)
}

#[inline]
fn ema_alpha(span: f64) -> f64 {
    (2.0 / (span.max(1.0) + 1.0)).clamp(0.001, 1.0)
}

#[inline]
pub(crate) fn ema_step(prev: f64, val: f64, span: f64) -> f64 {
    let alpha = ema_alpha(span);
    let v = if val.is_finite() { val } else { prev };
    alpha * v + (1.0 - alpha) * prev
}

/// Computes completed-bar EMA endpoints `(ema8, ema21, ema55, ema12)` scaled by `mult` in O(1) memory.
#[inline]
fn compute_bar_emas(bars: &[OhlcvBar], mult: f64) -> (f64, f64, f64, f64) {
    let Some(first) = bars.first() else {
        return (0.0, 0.0, 0.0, 0.0);
    };
    let eff_mult = if mult.is_finite() && mult > 0.0 {
        mult.clamp(0.25, 16.0)
    } else {
        1.0
    };
    let c0 = first.close.max(1e-12);
    let init = if c0.is_finite() { c0 } else { 0.0 };
    let (mut e8, mut e21, mut e55, mut e12) = (init, init, init, init);
    let (a8, a21, a55, a12) = (
        ema_alpha(8.0 * eff_mult),
        ema_alpha(21.0 * eff_mult),
        ema_alpha(55.0 * eff_mult),
        ema_alpha(12.0 * eff_mult),
    );
    for b in &bars[1..] {
        let c = b.close.max(1e-12);
        let finite = c.is_finite();
        let v8 = if finite { c } else { e8 };
        let v21 = if finite { c } else { e21 };
        let v55 = if finite { c } else { e55 };
        let v12 = if finite { c } else { e12 };
        e8 = a8 * v8 + (1.0 - a8) * e8;
        e21 = a21 * v21 + (1.0 - a21) * e21;
        e55 = a55 * v55 + (1.0 - a55) * e55;
        e12 = a12 * v12 + (1.0 - a12) * e12;
    }
    (e8, e21, e55, e12)
}

#[inline]
fn hash_bars(bars: &[OhlcvBar], wall_clock_normalize: bool) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    let mix = |acc: &mut u64, val: u64| {
        *acc ^= val;
        *acc = acc.wrapping_mul(0x100000001b3);
    };
    mix(&mut h, wall_clock_normalize as u64);
    mix(&mut h, bars.len() as u64);
    for b in bars {
        mix(&mut h, b.timestamp_ms as u64);
        mix(&mut h, b.open.to_bits());
        mix(&mut h, b.high.to_bits());
        mix(&mut h, b.low.to_bits());
        mix(&mut h, b.close.to_bits());
        mix(&mut h, b.volume.to_bits());
        mix(&mut h, timeframe_to_ms(&b.timeframe).unwrap_or(0) as u64);
    }
    h
}

/// Memoized bar-level indicators keyed by `(asset_id, bar_fingerprint)`.
#[derive(Debug, Clone)]
pub struct CachedBarIndicators {
    pub bar_fingerprint: u64,
    pub wall_clock_normalize: bool,
    pub bar_hours: f64,
    pub norm_mult: f64,
    pub atr_4h_scale: f64,
    pub last_bar_ts_ms: i64,
    pub bars_len: usize,
    pub last_bar_open: Option<f64>,
    pub last_bar_close: Option<f64>,
    pub fourth_from_last_close: Option<f64>,
    pub raw_atr: f64,
    pub unfloored_atr_finite: Option<f64>,
    pub active_bars: usize,
    pub window_bars: usize,
    pub annualized_vol_opt: Option<f64>,
    pub min_low_72: f64,
    pub max_high_72: f64,
    pub rsi_tail_closes: [f64; 64],
    pub rsi_tail_len: usize,
    pub ema8_bars: f64,
    pub ema21_bars: f64,
    pub ema55_bars: f64,
    pub ema12_bars: f64,
    pub avg_vol_20_sum: f64,
    pub avg_vol_20_count: usize,
    pub last_bar_vol: f64,
    pub channel_high_18: f64,
    pub channel_low_18: f64,
    pub btc_fingerprint: u64,
    pub market_beta_opt: Option<f64>,
}

/// Cross-tick memoizer for closed-bar technical indicators (`Sec. 8`) and N2 exit confirmation state.
#[derive(Debug, Clone, Default)]
pub struct BarCloseCache {
    entries: HashMap<i64, CachedBarIndicators>,
    btc_fingerprint: u64,
    btc_close_by_ts: HashMap<i64, f64>,
    pub(crate) exit_confirm_by_asset: HashMap<i64, ExitConfirmState>,
}

impl BarCloseCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.btc_fingerprint = 0;
        self.btc_close_by_ts.clear();
        self.exit_confirm_by_asset.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn exit_confirm_state(&self, asset_id: i64) -> Option<ExitConfirmState> {
        self.exit_confirm_by_asset.get(&asset_id).copied()
    }

    fn get_or_build_btc_map(&mut self, btc_bars: &[OhlcvBar]) -> (u64, &HashMap<i64, f64>) {
        let fp = hash_bars(btc_bars, false);
        if fp != self.btc_fingerprint || (btc_bars.is_empty() && !self.btc_close_by_ts.is_empty()) {
            self.btc_close_by_ts.clear();
            self.btc_close_by_ts.reserve(btc_bars.len());
            for b in btc_bars {
                if b.close.is_finite() && b.close > 0.0 {
                    self.btc_close_by_ts.insert(b.timestamp_ms, b.close);
                }
            }
            self.btc_fingerprint = fp;
        }
        (self.btc_fingerprint, &self.btc_close_by_ts)
    }

    fn get_or_compute_asset(
        &mut self,
        asset: &ExchangeAsset,
        bars: &[OhlcvBar],
        btc_fp: u64,
        btc_close_by_ts: &HashMap<i64, f64>,
        wall_clock_normalize: bool,
    ) -> CachedBarIndicators {
        let fp = hash_bars(bars, wall_clock_normalize);
        if let Some(existing) = self.entries.get(&asset.id) {
            if existing.bar_fingerprint == fp
                && existing.bars_len == bars.len()
                && existing.wall_clock_normalize == wall_clock_normalize
            {
                if existing.btc_fingerprint == btc_fp {
                    return existing.clone();
                }
                let mut updated = existing.clone();
                updated.btc_fingerprint = btc_fp;
                updated.market_beta_opt =
                    compute_market_beta_from_map(bars, btc_close_by_ts, asset);
                self.entries.insert(asset.id, updated.clone());
                return updated;
            }
        }

        let bar_hours = if wall_clock_normalize {
            infer_bar_hours(bars)
        } else {
            4.0
        };
        let norm_mult = if wall_clock_normalize {
            (4.0 / bar_hours).clamp(0.25, 16.0)
        } else {
            1.0
        };
        let atr_4h_scale = if wall_clock_normalize {
            (4.0 / bar_hours).sqrt().clamp(0.5, 4.0)
        } else {
            1.0
        };

        let raw_atr = compute_unfloored_atr(bars, ATR_LOOKBACK_PERIOD);
        let unfloored_atr_finite = compute_finite_atr_core(bars, ATR_LOOKBACK_PERIOD);
        let (active_bars, window_bars) = count_trailing_active_bars(bars);
        let annualized_vol_opt = compute_annualized_vol_from_bars(bars);
        let (min_low_72, max_high_72, rsi_tail_closes, rsi_tail_len) =
            compute_range_bounds_and_rsi_tail_with_mult(bars, norm_mult);
        let (ema8_bars, ema21_bars, ema55_bars, ema12_bars) = compute_bar_emas(bars, norm_mult);

        let vol_lookback_cap = (20.0 * norm_mult).round().max(1.0) as usize;
        let lookback_20 = bars.len().min(vol_lookback_cap);
        let recent_20 = &bars[bars.len() - lookback_20..];
        let mut avg_vol_20_sum = 0.0;
        let mut avg_vol_20_count = 0usize;
        for b in recent_20 {
            if b.volume.is_finite() && b.volume > 0.0 {
                avg_vol_20_sum += b.volume;
                avg_vol_20_count += 1;
            }
        }
        let last_bar_vol = recent_20
            .last()
            .map(|b| {
                if b.volume.is_finite() && b.volume > 0.0 {
                    b.volume
                } else {
                    0.0
                }
            })
            .unwrap_or(0.0);

        let (channel_high_18, channel_low_18) = if bars.len() >= 10 {
            let max_chan = (18.0 * norm_mult).round().max(4.0) as usize;
            let chan_len = bars.len().saturating_sub(1).clamp(4, max_chan);
            let prior_slice = &bars[bars.len() - 1 - chan_len..bars.len() - 1];
            let ch = prior_slice.iter().map(|b| b.high).fold(f64::MIN, f64::max);
            let cl = prior_slice.iter().map(|b| b.low).fold(f64::MAX, f64::min);
            (ch, cl)
        } else {
            (f64::MIN, f64::MAX)
        };

        let lookback_24h = (6.0 * norm_mult).round().max(1.0) as usize;
        let market_beta_opt = compute_market_beta_from_map(bars, btc_close_by_ts, asset);

        let entry = CachedBarIndicators {
            bar_fingerprint: fp,
            wall_clock_normalize,
            bar_hours,
            norm_mult,
            atr_4h_scale,
            last_bar_ts_ms: bars.last().map(|b| b.timestamp_ms).unwrap_or(0),
            bars_len: bars.len(),
            last_bar_open: bars.last().map(|b| b.open),
            last_bar_close: bars.last().map(|b| b.close),
            fourth_from_last_close: bars
                .get(bars.len().saturating_sub(lookback_24h))
                .map(|b| b.close),
            raw_atr,
            unfloored_atr_finite,
            active_bars,
            window_bars,
            annualized_vol_opt,
            min_low_72,
            max_high_72,
            rsi_tail_closes,
            rsi_tail_len,
            ema8_bars,
            ema21_bars,
            ema55_bars,
            ema12_bars,
            avg_vol_20_sum,
            avg_vol_20_count,
            last_bar_vol,
            channel_high_18,
            channel_low_18,
            btc_fingerprint: btc_fp,
            market_beta_opt,
        };
        self.entries.insert(asset.id, entry.clone());
        entry
    }
}

#[inline]
pub(crate) fn compute_finite_atr_core(bars: &[OhlcvBar], period: usize) -> Option<f64> {
    if bars.len() < 2 || period == 0 {
        return None;
    }
    let lookback = period.max(2).min(bars.len() - 1);
    let start = bars.len() - lookback;
    let mut sum_tr = 0.0;
    for i in start..bars.len() {
        let cur = &bars[i];
        let prev_close = bars[i - 1].close;
        if !cur.high.is_finite() || !cur.low.is_finite() || !prev_close.is_finite() {
            return None;
        }
        let hl = (cur.high - cur.low).abs();
        let hc = (cur.high - prev_close).abs();
        let lc = (cur.low - prev_close).abs();
        sum_tr += hl.max(hc).max(lc);
    }
    Some(sum_tr / (lookback as f64))
}

/// Computes market beta relative to `BTC-USDC` using a prebuilt `btc_close_by_ts` map.
pub(crate) fn compute_market_beta_from_map(
    asset_bars: &[OhlcvBar],
    btc_close_by_ts: &HashMap<i64, f64>,
    asset: &ExchangeAsset,
) -> Option<f64> {
    if asset.coin.eq_ignore_ascii_case("BTC") || asset.symbol.eq_ignore_ascii_case("BTC-USDC") {
        return Some(1.0);
    }

    let mut aligned: Vec<(i64, f64, f64)> = asset_bars
        .iter()
        .filter(|a| a.close.is_finite() && a.close > 0.0)
        .filter_map(|a| {
            btc_close_by_ts
                .get(&a.timestamp_ms)
                .map(|&bc| (a.timestamp_ms, a.close, bc))
        })
        .collect();
    aligned.sort_by_key(|x| x.0);
    aligned.dedup_by_key(|x| x.0);

    if aligned.len() >= 8 {
        let mut a_rets = Vec::with_capacity(aligned.len() - 1);
        let mut b_rets = Vec::with_capacity(aligned.len() - 1);
        for pair in aligned.windows(2) {
            let ar = (pair[1].1 / pair[0].1).ln();
            let br = (pair[1].2 / pair[0].2).ln();
            if ar.is_finite() && br.is_finite() {
                a_rets.push(ar);
                b_rets.push(br);
            }
        }

        if a_rets.len() >= 7 {
            let m = a_rets.len() as f64;
            let mean_a = a_rets.iter().sum::<f64>() / m;
            let mean_b = b_rets.iter().sum::<f64>() / m;

            let mut cov = 0.0;
            let mut var_b = 0.0;
            for i in 0..a_rets.len() {
                let da = a_rets[i] - mean_a;
                let db = b_rets[i] - mean_b;
                cov += da * db;
                var_b += db * db;
            }
            if var_b > 1e-10 {
                let beta = (cov / var_b).clamp(0.15, 2.80);
                return Some(round_dp(beta, 4));
            }
        }
    }
    None
}

#[inline]
pub(crate) fn fallback_beta(asset: &ExchangeAsset) -> f64 {
    let default_beta = match asset.coin.as_str() {
        "ETH" => 1.12,
        "SOL" | "HYPE" | "SUI" | "AVAX" | "INJ" | "TAO" => 1.35,
        _ => 1.25,
    };
    round_dp(default_beta, 4)
}

#[inline]
pub(crate) fn fallback_annualized_vol(asset: &ExchangeAsset) -> f64 {
    if asset.prev_day_price.is_finite()
        && asset.prev_day_price > 0.0
        && asset.mark_price.is_finite()
        && asset.mark_price > 0.0
    {
        let day_ret = ((asset.mark_price / asset.prev_day_price).ln()).abs();
        let est = (day_ret * (365.0_f64).sqrt()).clamp(0.25, 2.80);
        if est.is_finite() {
            return round_dp(est, 4);
        }
    }
    0.65
}

/// Precomputed per-asset features built once per cycle by [`CycleFeatureCache`].
#[derive(Debug, Clone)]
pub struct PrecomputedAssetFeatures {
    pub asset_id: i64,
    pub live_px: f64,
    pub bar_hours: f64,
    pub norm_mult: f64,
    pub atr_4h_scale: f64,
    pub last_bar_ts_ms: i64,
    pub bars_len: usize,
    pub last_bar_open: Option<f64>,
    pub last_bar_close: Option<f64>,
    pub quality: AssetLiquidityQuality,
    pub floored_atr: f64,
    pub annualized_vol: f64,
    pub market_beta: f64,
    pub rsi_14: f64,
    pub range_pos: f64,
    pub bouncing_from_low: bool,
    pub min_low_72: f64,
    pub max_high_72: f64,
    pub rsi_tail_closes: [f64; 64],
    pub rsi_tail_len: usize,
    pub ema8_bars: f64,
    pub ema21_bars: f64,
    pub ema55_bars: f64,
    pub ema12_bars: f64,
    pub avg_vol_20_sum: f64,
    pub avg_vol_20_count: usize,
    pub last_bar_vol: f64,
    pub channel_high_18: f64,
    pub channel_low_18: f64,
}

/// Single-pass per-cycle feature bundle shared across S1, S2, S3, and Breadth (`Sec. 8`).
#[derive(Debug, Clone)]
pub struct CycleFeatureCache {
    pub btc_close_by_ts: HashMap<i64, f64>,
    pub per_asset: HashMap<i64, PrecomputedAssetFeatures>,
    pub prelim_breadth_bull: f64,
    pub eligible_coin_count: i32,
    pub avg_eligible_funding: f64,
}

impl CycleFeatureCache {
    /// Builds all shared per-cycle features in a single universe pass with wall-clock normalization enabled.
    pub fn build<B: AsRef<[OhlcvBar]>>(
        assets: &[ExchangeAsset],
        bars_by_asset: &HashMap<i64, B>,
        bar_cache: &mut BarCloseCache,
    ) -> Self {
        Self::build_with_options(assets, bars_by_asset, bar_cache, true)
    }

    /// Builds all shared per-cycle features in a single universe pass, using `bar_cache` to
    /// skip closed-bar indicator recomputation across 3-second ticks when bars have not changed.
    pub fn build_with_options<B: AsRef<[OhlcvBar]>>(
        assets: &[ExchangeAsset],
        bars_by_asset: &HashMap<i64, B>,
        bar_cache: &mut BarCloseCache,
        wall_clock_normalize_indicators: bool,
    ) -> Self {
        let btc_bars: &[OhlcvBar] = assets
            .iter()
            .find(|a| {
                a.coin.eq_ignore_ascii_case("BTC") || a.symbol.eq_ignore_ascii_case("BTC-USDC")
            })
            .and_then(|btc| bars_by_asset.get(&btc.id))
            .map(AsRef::as_ref)
            .unwrap_or(&[]);

        let (btc_fp, btc_map_ref) = bar_cache.get_or_build_btc_map(btc_bars);
        let btc_close_by_ts = btc_map_ref.clone();

        let mut per_asset = HashMap::with_capacity(assets.len());
        let mut eligible_coin_count = 0i32;
        let mut bull_count = 0.0;
        let mut eligible_funding_sum = 0.0;
        let mut eligible_funding_count = 0usize;

        for asset in assets {
            let bars: &[OhlcvBar] = bars_by_asset
                .get(&asset.id)
                .map(AsRef::as_ref)
                .unwrap_or(&[]);

            let cached = bar_cache.get_or_compute_asset(
                asset,
                bars,
                btc_fp,
                &btc_close_by_ts,
                wall_clock_normalize_indicators,
            );

            let live_px = if asset.mark_price.is_finite() && asset.mark_price > 0.0 {
                asset.mark_price
            } else {
                cached
                    .last_bar_close
                    .filter(|c| c.is_finite() && *c > 0.0)
                    .unwrap_or(0.0)
            };

            let quality = evaluate_asset_liquidity_quality_with_bar_stats(
                asset,
                cached.bars_len,
                cached.last_bar_close,
                cached.raw_atr,
                cached.active_bars,
                cached.window_bars,
                cached.bar_hours,
            );

            let safe_ref = if live_px.is_finite() && live_px > 0.0 {
                live_px.max(1e-8)
            } else {
                1.0
            };
            let atr_floor = (safe_ref * 0.012).max(1e-8);
            let floored_atr = cached
                .unfloored_atr_finite
                .map(|raw| raw * cached.atr_4h_scale)
                .unwrap_or(atr_floor)
                .max(atr_floor);

            let annualized_vol = cached
                .annualized_vol_opt
                .unwrap_or_else(|| fallback_annualized_vol(asset));
            let market_beta = cached
                .market_beta_opt
                .unwrap_or_else(|| fallback_beta(asset));

            let (rsi_14, range_pos, bouncing_from_low) = compute_rsi_and_range_from_parts(
                cached.bars_len,
                cached.last_bar_open,
                cached.last_bar_close,
                cached.min_low_72,
                cached.max_high_72,
                &cached.rsi_tail_closes[..cached.rsi_tail_len],
                live_px,
            );

            if (asset.is_core_eligible || asset.is_event_eligible)
                && asset.day_volume_usdc.is_finite()
                && asset.day_volume_usdc > 0.0
            {
                eligible_coin_count += 1;
                if asset.funding_rate_hourly.is_finite() {
                    eligible_funding_sum += asset.funding_rate_hourly;
                    eligible_funding_count += 1;
                }
                if cached.bars_len >= 4 {
                    let first = cached.fourth_from_last_close.unwrap_or(0.0).max(1e-12);
                    let last =
                        effective_breadth_anchor_price(asset.mark_price, cached.last_bar_close);
                    if last > first * 1.002 {
                        bull_count += 1.0;
                    } else if (last - first).abs() / first <= 0.002 {
                        bull_count += 0.5;
                    }
                } else if asset.prev_day_price.is_finite()
                    && asset.prev_day_price > 0.0
                    && asset.mark_price.is_finite()
                    && asset.mark_price > 0.0
                {
                    if asset.mark_price > asset.prev_day_price * 1.002 {
                        bull_count += 1.0;
                    } else if (asset.mark_price - asset.prev_day_price).abs() / asset.prev_day_price
                        <= 0.002
                    {
                        bull_count += 0.5;
                    }
                } else {
                    bull_count += 0.5;
                }
            }

            per_asset.insert(
                asset.id,
                PrecomputedAssetFeatures {
                    asset_id: asset.id,
                    live_px,
                    bar_hours: cached.bar_hours,
                    norm_mult: cached.norm_mult,
                    atr_4h_scale: cached.atr_4h_scale,
                    last_bar_ts_ms: cached.last_bar_ts_ms,
                    bars_len: cached.bars_len,
                    last_bar_open: cached.last_bar_open,
                    last_bar_close: cached.last_bar_close,
                    quality,
                    floored_atr,
                    annualized_vol,
                    market_beta,
                    rsi_14,
                    range_pos,
                    bouncing_from_low,
                    min_low_72: cached.min_low_72,
                    max_high_72: cached.max_high_72,
                    rsi_tail_closes: cached.rsi_tail_closes,
                    rsi_tail_len: cached.rsi_tail_len,
                    ema8_bars: cached.ema8_bars,
                    ema21_bars: cached.ema21_bars,
                    ema55_bars: cached.ema55_bars,
                    ema12_bars: cached.ema12_bars,
                    avg_vol_20_sum: cached.avg_vol_20_sum,
                    avg_vol_20_count: cached.avg_vol_20_count,
                    last_bar_vol: cached.last_bar_vol,
                    channel_high_18: cached.channel_high_18,
                    channel_low_18: cached.channel_low_18,
                },
            );
        }

        let prelim_breadth_bull = if eligible_coin_count <= 0 {
            0.50
        } else {
            round_dp(
                (bull_count / (eligible_coin_count as f64)).clamp(0.0, 1.0),
                4,
            )
        };
        let avg_eligible_funding = if eligible_funding_count == 0 {
            0.0
        } else {
            eligible_funding_sum / (eligible_funding_count as f64)
        };

        Self {
            btc_close_by_ts,
            per_asset,
            prelim_breadth_bull,
            eligible_coin_count,
            avg_eligible_funding,
        }
    }

    /// Computes initial [`RegimeGateState`] prior to running sleeves.
    pub fn initial_regime_gate(&self, cfg: &RegimeGateConfig) -> RegimeGateState {
        compute_regime_gate_state(
            self.prelim_breadth_bull,
            self.eligible_coin_count,
            self.avg_eligible_funding,
            0,
            cfg,
        )
    }

    /// Finalizes [`MarketBreadthSnapshot`] after S3 capitulation count is known, avoiding a second breadth pass.
    pub fn finalize_breadth_snapshot(
        &self,
        beta_by_asset: &HashMap<i64, f64>,
        portfolio_weights: &HashMap<i64, f64>,
        capitulation_count: usize,
    ) -> MarketBreadthSnapshot {
        let mut beta_exposure = 0.0;
        for (asset_id, &w) in portfolio_weights {
            if w.is_finite() {
                let beta = beta_by_asset
                    .get(asset_id)
                    .copied()
                    .filter(|b| b.is_finite())
                    .unwrap_or(1.0);
                beta_exposure += w * beta;
            }
        }
        let market_regime_label = classify_market_regime(
            self.prelim_breadth_bull,
            self.avg_eligible_funding,
            capitulation_count,
        );
        MarketBreadthSnapshot {
            market_breadth_bull: self.prelim_breadth_bull,
            market_beta_exposure: round_dp(beta_exposure, 4),
            market_regime_label,
            eligible_coin_count: self.eligible_coin_count,
        }
    }
}
