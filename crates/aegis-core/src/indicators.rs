//! Shared technical indicators, causal EMA, cross-sectional normalization, and volatility estimators (`aegis-core::indicators`).

use crate::models::OhlcvBar;
use crate::time::round_dp;
use std::collections::{BTreeSet, HashMap};

const MS_PER_YEAR: f64 = 365.0 * 86_400_000.0;

/// Computes unfloored Average True Range (ATR) over up to `period` trailing bars.
///
/// Applies no price-percentage floor so dead/flat books report true near-zero range.
/// Returns `0.0` when fewer than 2 valid bars exist.
pub fn compute_unfloored_atr(bars: &[OhlcvBar], period: usize) -> f64 {
    if bars.len() < 2 || period == 0 {
        return 0.0;
    }
    let lookback = period.min(bars.len() - 1);
    let start = bars.len() - lookback;
    let mut tr_sum = 0.0;
    let mut count = 0usize;
    for i in start..bars.len() {
        let cur = &bars[i];
        let prev_close = bars[i - 1].close;
        if !cur.high.is_finite() || !cur.low.is_finite() || !prev_close.is_finite() {
            continue;
        }
        let hl = (cur.high - cur.low).abs();
        let hc = (cur.high - prev_close).abs();
        let lc = (cur.low - prev_close).abs();
        let tr = hl.max(hc).max(lc);
        if tr.is_finite() {
            tr_sum += tr;
            count += 1;
        }
    }
    if count == 0 {
        0.0
    } else {
        tr_sum / (count as f64)
    }
}

/// Computes Average True Range (ATR) over `period` bars with optional reference-price floor and fallback percentages.
pub fn compute_atr(
    bars: &[OhlcvBar],
    period: usize,
    ref_px: f64,
    floor_pct: Option<f64>,
    fallback_pct: Option<f64>,
) -> f64 {
    let safe_ref = if ref_px.is_finite() {
        ref_px.max(1e-8)
    } else {
        1.0
    };
    let floor = floor_pct
        .filter(|p| p.is_finite() && *p > 0.0)
        .map(|p| (safe_ref * p).max(1e-8))
        .unwrap_or(0.0);
    let fallback = fallback_pct
        .filter(|p| p.is_finite() && *p > 0.0)
        .map(|p| (safe_ref * p).max(1e-8))
        .unwrap_or(floor);

    if bars.len() < 2 || period == 0 {
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
    (sum_tr / (lookback as f64)).max(floor)
}

/// Computes a strictly causal Exponential Moving Average (`alpha = 2 / (span + 1)`).
pub fn causal_ema(series: &[f64], span: f64) -> Vec<f64> {
    if series.is_empty() {
        return Vec::new();
    }
    let alpha = (2.0 / (span.max(1.0) + 1.0)).clamp(0.001, 1.0);
    let mut out = Vec::with_capacity(series.len());
    let mut prev = if series[0].is_finite() {
        series[0]
    } else {
        0.0
    };
    out.push(prev);
    for &val in &series[1..] {
        let v = if val.is_finite() { val } else { prev };
        prev = alpha * v + (1.0 - alpha) * prev;
        out.push(prev);
    }
    out
}

/// Computes only the final value of a causal EMA in O(1) stack memory with zero heap allocations.
pub fn causal_ema_last(series: &[f64], span: f64) -> Option<f64> {
    let first = *series.first()?;
    if !first.is_finite() {
        return None;
    }
    let alpha = (2.0 / (span.max(1.0) + 1.0)).clamp(0.001, 1.0);
    let mut prev = first;
    for &val in &series[1..] {
        if !val.is_finite() {
            return None;
        }
        prev = alpha * val + (1.0 - alpha) * prev;
    }
    Some(prev)
}

/// Computes cross-sectional z-scores rounded to 4dp and clamped to `[-clamp_abs, +clamp_abs]`.
pub fn cross_sectional_zscore(
    values: &[(i64, f64)],
    min_std: f64,
    clamp_abs: f64,
) -> HashMap<i64, f64> {
    let valid: Vec<f64> = values
        .iter()
        .filter_map(|(_, v)| v.is_finite().then_some(*v))
        .collect();
    let mut out = HashMap::with_capacity(values.len());
    if valid.len() < 2 {
        for &(id, _) in values {
            out.insert(id, 0.0);
        }
        return out;
    }
    let n = valid.len() as f64;
    let mean = valid.iter().sum::<f64>() / n;
    let var = valid
        .iter()
        .map(|&v| {
            let d = v - mean;
            d * d
        })
        .sum::<f64>()
        / (n - 1.0).max(1.0);
    let eff_min_std = if min_std.is_finite() && min_std > 0.0 {
        min_std
    } else {
        1e-8
    };
    let std = var.sqrt().max(eff_min_std);
    let cap = if clamp_abs.is_finite() && clamp_abs > 0.0 {
        clamp_abs
    } else {
        6.0
    };

    for &(id, val) in values {
        let z = if val.is_finite() {
            round_dp(((val - mean) / std).clamp(-cap, cap), 4)
        } else {
            0.0
        };
        out.insert(id, z);
    }
    out
}

/// Hard Top-K sleeve weight normalizer (`R14-H7`).
///
/// Ranks `(asset_id, signed_score, tie_breaker_vol)` by `|signed_score|` descending
/// (deterministic tie-break by `tie_breaker_vol` descending, then `asset_id` ascending),
/// zeroes out ranks `> k`, and truncates weights toward zero at 4dp so `sum(|w_i|) <= budget` strictly.
pub fn top_k_normalized(scores: &[(i64, f64, f64)], k: usize, budget: f64) -> HashMap<i64, f64> {
    let eff_budget = if budget.is_finite() {
        budget.clamp(0.0, 1.50)
    } else {
        0.0
    };
    let mut out = HashMap::new();
    if k == 0 || eff_budget <= 0.0 {
        return out;
    }

    let mut ranked: Vec<(i64, f64, f64)> = scores
        .iter()
        .copied()
        .filter(|&(_, s, _)| s.is_finite() && s.abs() > 1e-6)
        .collect();
    if ranked.is_empty() {
        return out;
    }

    ranked.sort_by(|a, b| {
        b.1.abs()
            .total_cmp(&a.1.abs())
            .then_with(|| {
                let vb = if b.2.is_finite() { b.2 } else { 0.0 };
                let va = if a.2.is_finite() { a.2 } else { 0.0 };
                vb.total_cmp(&va)
            })
            .then_with(|| a.0.cmp(&b.0))
    });

    let kept = &ranked[..ranked.len().min(k)];
    let denom: f64 = kept.iter().map(|x| x.1.abs()).sum();
    if !denom.is_finite() || denom <= 1e-8 {
        return out;
    }

    for &(id, score, _) in kept {
        let raw = (score / denom) * eff_budget;
        let mag = ((raw.abs() * 10_000.0 + 1e-9).floor()) / 10_000.0;
        let w = if mag <= 0.0 { 0.0 } else { mag.copysign(raw) };
        out.insert(id, w);
    }
    out
}

/// Fail-closed single-tick increment in basis points (`None` when `tick_size` or `live_px` is non-finite/non-positive) (`R14-M8`).
#[inline]
pub fn compute_one_tick_bps_opt(tick_size: f64, live_px: f64) -> Option<f64> {
    if live_px.is_finite() && live_px > 0.0 && tick_size.is_finite() && tick_size > 0.0 {
        let bps = (tick_size / live_px) * 10_000.0;
        (bps.is_finite() && bps > 0.0).then_some(bps)
    } else {
        None
    }
}

/// Maps all 10 canonical bar timeframes to millisecond duration (`R14-M9`).
pub fn timeframe_to_ms(tf: &str) -> Option<i64> {
    match tf.trim().to_ascii_lowercase().as_str() {
        "1m" => Some(60_000),
        "5m" => Some(300_000),
        "15m" => Some(900_000),
        "30m" => Some(1_800_000),
        "1h" => Some(3_600_000),
        "2h" => Some(7_200_000),
        "4h" => Some(14_400_000),
        "8h" => Some(28_800_000),
        "12h" => Some(43_200_000),
        "1d" => Some(86_400_000),
        _ => None,
    }
}

/// Estimates time-normalized annualized volatility from OHLCV bars (`R14-M9`).
///
/// Uses observed positive timestamp deltas (`dt_ms >= 60_000`) across mixed-timeframe or
/// gapped bar series; falls back to uniform `timeframe` string only when timestamps are missing
/// and all bars share one recognized timeframe. Returns `None` on insufficient or ambiguous inputs.
pub fn compute_annualized_vol_from_bars(bars: &[OhlcvBar]) -> Option<f64> {
    if bars.len() < 6 {
        return None;
    }

    let mut distinct_labels: BTreeSet<String> = BTreeSet::new();
    let mut timed_pairs: Vec<(f64, i64)> = Vec::with_capacity(bars.len() - 1);
    let mut all_log_rets: Vec<f64> = Vec::with_capacity(bars.len() - 1);

    for b in bars {
        let tf = b.timeframe.trim().to_ascii_lowercase();
        if !tf.is_empty() {
            distinct_labels.insert(tf);
        }
    }

    for pair in bars.windows(2) {
        let p0 = pair[0].close;
        let p1 = pair[1].close;
        if !p0.is_finite() || p0 <= 0.0 || !p1.is_finite() || p1 <= 0.0 {
            continue;
        }
        let r = (p1 / p0).ln();
        if !r.is_finite() {
            continue;
        }
        all_log_rets.push(r);
        let dt_ms = pair[1].timestamp_ms.saturating_sub(pair[0].timestamp_ms);
        if dt_ms >= 60_000 {
            timed_pairs.push((r, dt_ms));
        }
    }

    if timed_pairs.len() >= 5 {
        let mut dts: Vec<i64> = timed_pairs.iter().map(|&(_, dt)| dt).collect();
        dts.sort_unstable();
        let median_dt = dts[dts.len() / 2];

        if distinct_labels.len() > 1 {
            tracing::warn!(
                timeframes = ?distinct_labels,
                median_dt_ms = median_dt,
                "Mixed timeframe labels detected in OHLCV series; using observed timestamp deltas for annualized volatility"
            );
        } else if let Some(label) = distinct_labels.iter().next() {
            if let Some(label_ms) = timeframe_to_ms(label) {
                let rel_diff = ((median_dt - label_ms).abs() as f64) / (label_ms as f64);
                if rel_diff > 0.20 {
                    tracing::warn!(
                        timeframe = %label,
                        label_ms,
                        median_dt_ms = median_dt,
                        "OHLCV timeframe label disagrees with observed timestamp deltas; using timestamp deltas"
                    );
                }
            }
        }

        let mut sum_r = 0.0;
        let mut sum_dt_yr = 0.0;
        let mut pairs_yr = Vec::with_capacity(timed_pairs.len());
        for (r, dt_ms) in timed_pairs {
            let dt_yr = (dt_ms as f64) / MS_PER_YEAR;
            sum_r += r;
            sum_dt_yr += dt_yr;
            pairs_yr.push((r, dt_yr));
        }
        if sum_dt_yr <= 0.0 {
            return None;
        }
        let mu_ann = sum_r / sum_dt_yr;
        let n = pairs_yr.len() as f64;
        let var_ann = pairs_yr
            .iter()
            .map(|&(r, dt_yr)| {
                let d = r - mu_ann * dt_yr;
                (d * d) / dt_yr
            })
            .sum::<f64>()
            / (n - 1.0).max(1.0);
        if !var_ann.is_finite() || var_ann < 0.0 {
            return None;
        }
        let ann = var_ann.sqrt().clamp(0.18, 3.20);
        return ann.is_finite().then(|| round_dp(ann, 4));
    }

    // Fallback when timestamps are absent/constant: require a single recognized timeframe
    if distinct_labels.len() != 1 || all_log_rets.len() < 5 {
        return None;
    }
    let label = distinct_labels.iter().next()?;
    let tf_ms = timeframe_to_ms(label)?;
    let periods_per_year = MS_PER_YEAR / (tf_ms as f64);
    let n = all_log_rets.len() as f64;
    let mean = all_log_rets.iter().sum::<f64>() / n;
    let var = all_log_rets
        .iter()
        .map(|&r| {
            let d = r - mean;
            d * d
        })
        .sum::<f64>()
        / (n - 1.0).max(1.0);
    let ann = (var.sqrt() * periods_per_year.sqrt()).clamp(0.18, 3.20);
    ann.is_finite().then(|| round_dp(ann, 4))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_bar(ts_ms: i64, tf: &str, open: f64, high: f64, low: f64, close: f64) -> OhlcvBar {
        OhlcvBar {
            id: 0,
            asset_id: 1,
            timeframe: tf.to_string(),
            timestamp_ms: ts_ms,
            open,
            high,
            low,
            close,
            volume: 1_000.0,
            funding_rate: 0.0,
            created_at_ms: ts_ms,
            updated_at_ms: ts_ms,
        }
    }

    #[test]
    fn test_r14_h7_top_k_normalized_40_equal_assets_respects_budget_and_tiebreak() {
        let scores: Vec<(i64, f64, f64)> = (1..=40)
            .map(|id| (id, 1.0, (id as f64) * 1_000.0))
            .collect();
        let weights = top_k_normalized(&scores, 8, 0.45);
        assert_eq!(weights.len(), 8);
        let sum_abs: f64 = weights.values().map(|w| w.abs()).sum();
        assert!(sum_abs <= 0.45 + 1e-12);
        assert!(sum_abs >= 0.45 - 0.001);
        for id in 33..=40 {
            assert!(weights.contains_key(&id), "expected top vol asset {id}");
        }
        assert!(!weights.contains_key(&32));

        // Check budget = 0.10 with 3 assets (never exceeds 0.10)
        let s3 = top_k_normalized(&[(1, 1.0, 10.0), (2, 1.0, 20.0), (3, 1.0, 30.0)], 8, 0.10);
        let s3_sum: f64 = s3.values().map(|w| w.abs()).sum();
        assert!(s3_sum <= 0.10 + 1e-12);
    }

    #[test]
    fn test_r14_m8_one_tick_bps_fail_closed() {
        assert_eq!(compute_one_tick_bps_opt(0.0, 100.0), None);
        assert_eq!(compute_one_tick_bps_opt(-0.01, 100.0), None);
        assert_eq!(compute_one_tick_bps_opt(f64::NAN, 100.0), None);
        assert_eq!(compute_one_tick_bps_opt(f64::INFINITY, 100.0), None);
        assert_eq!(compute_one_tick_bps_opt(0.01, 0.0), None);
        assert_eq!(compute_one_tick_bps_opt(0.01, -50.0), None);
        assert_eq!(compute_one_tick_bps_opt(0.01, f64::NAN), None);

        let bps = compute_one_tick_bps_opt(0.01, 100.0).unwrap();
        assert!((bps - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_r14_m9_timeframes_mixed_and_mislabeled_bars_vol() {
        for (tf, ms) in [
            ("1m", 60_000),
            ("5m", 300_000),
            ("15m", 900_000),
            ("30m", 1_800_000),
            ("1h", 3_600_000),
            ("2h", 7_200_000),
            ("4h", 14_400_000),
            ("8h", 28_800_000),
            ("12h", 43_200_000),
            ("1d", 86_400_000),
        ] {
            assert_eq!(timeframe_to_ms(tf), Some(ms));
        }
        assert_eq!(timeframe_to_ms("unknown"), None);

        // Uniform 4h bars: exact parity with sample_var * periods_per_year
        let step_4h = 14_400_000i64;
        let closes = [100.0, 101.0, 99.5, 102.0, 100.5, 103.0, 101.5, 104.0];
        let bars_4h: Vec<OhlcvBar> = closes
            .iter()
            .enumerate()
            .map(|(i, &c)| mk_bar((i as i64 + 1) * step_4h, "4h", c, c, c, c))
            .collect();
        let vol_timed = compute_annualized_vol_from_bars(&bars_4h).unwrap();

        // Mislabeled "1m" on true 4h timestamps uses timestamp deltas instead of 15.5x blow-up
        let bars_mislabeled: Vec<OhlcvBar> = closes
            .iter()
            .enumerate()
            .map(|(i, &c)| mk_bar((i as i64 + 1) * step_4h, "1m", c, c, c, c))
            .collect();
        assert_eq!(
            compute_annualized_vol_from_bars(&bars_mislabeled),
            Some(vol_timed)
        );

        // Zero timestamps with consistent known label falls back cleanly to label
        let bars_zero_ts: Vec<OhlcvBar> = closes
            .iter()
            .map(|&c| mk_bar(0, "4h", c, c, c, c))
            .collect();
        assert_eq!(
            compute_annualized_vol_from_bars(&bars_zero_ts),
            Some(vol_timed)
        );

        // Zero timestamps with mixed or unknown labels returns None
        let mut bars_bad = bars_zero_ts.clone();
        bars_bad[0].timeframe = "1h".to_string();
        assert_eq!(compute_annualized_vol_from_bars(&bars_bad), None);
        let bars_unknown: Vec<OhlcvBar> = closes
            .iter()
            .map(|&c| mk_bar(0, "weird", c, c, c, c))
            .collect();
        assert_eq!(compute_annualized_vol_from_bars(&bars_unknown), None);

        // Mixed 4h + 1h bars with constant annualized diffusive sigma
        // 1% per 4h <=> 0.5% per 1h (var scales linearly with dt)
        let mut mixed = Vec::new();
        let mut px = 100.0_f64;
        let mut ts = 1_000_000_000i64;
        mixed.push(mk_bar(ts, "4h", px, px, px, px));
        for i in 0..8 {
            let dt = if i < 4 { 14_400_000 } else { 3_600_000 };
            let r: f64 = if i < 4 {
                if i % 2 == 0 {
                    0.01
                } else {
                    -0.01
                }
            } else if i % 2 == 0 {
                0.005
            } else {
                -0.005
            };
            px *= r.exp();
            ts += dt;
            let tf = if i < 4 { "4h" } else { "1h" };
            mixed.push(mk_bar(ts, tf, px, px, px, px));
        }
        let mixed_vol = compute_annualized_vol_from_bars(&mixed).unwrap();
        let expected_vol = (0.01_f64 * (365.0 * 6.0_f64).sqrt()).clamp(0.18, 3.20);
        assert!(
            (mixed_vol - expected_vol).abs() < 0.05,
            "mixed_vol={mixed_vol}, expected={expected_vol}"
        );
    }

    #[test]
    fn test_atr_ema_and_zscore_helpers() {
        let series = [100.0, 102.0, 101.0, 104.0, 106.0];
        let full = causal_ema(&series, 3.0);
        let last = causal_ema_last(&series, 3.0).unwrap();
        assert!((full.last().unwrap() - last).abs() < 1e-12);

        let flat_bars = vec![
            mk_bar(1_000, "1h", 10.0, 10.0, 10.0, 10.0),
            mk_bar(2_000, "1h", 10.0, 10.0, 10.0, 10.0),
            mk_bar(3_000, "1h", 10.0, 10.0, 10.0, 10.0),
        ];
        assert_eq!(compute_unfloored_atr(&flat_bars, 14), 0.0);
        assert!((compute_atr(&flat_bars, 14, 10.0, Some(0.012), Some(0.012)) - 0.12).abs() < 1e-9);

        let zs = cross_sectional_zscore(&[(1, 1.0), (2, 2.0), (3, 3.0)], 0.0001, 6.0);
        assert!((zs[&2] - 0.0).abs() < 1e-9);
        assert!((zs[&1] + 1.0).abs() < 1e-9);
        assert!((zs[&3] - 1.0).abs() < 1e-9);
    }
}
