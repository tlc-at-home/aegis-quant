//! Sleeve 1: Cross-Sectional Carry-Momentum & Market-Beta Hedge (`aegis-signals::carry_momentum`).
//!
//! Computes per-asset `s1_mom_score`, `s1_carry_score`, `market_beta`, `annualized_vol`,
//! and risk-adjusted inverse-volatility weight `w_s1_carry_mom` across core-eligible Hyperliquid perps.

use crate::liquidity::{default_passes_quality, evaluate_asset_liquidity_quality};
use crate::trend_breakout::compute_rsi_and_range_position;
use aegis_core::{round_dp, ExchangeAsset, OhlcvBar};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Computed Sleeve 1 metrics for a single Hyperliquid perpetual asset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CarryMomentumMetrics {
    pub asset_id: i64,
    pub symbol: String,
    pub annualized_vol: f64,
    pub market_beta: f64,
    pub s1_mom_score: f64,
    pub s1_carry_score: f64,
    pub w_s1_carry_mom: f64,
    #[serde(default = "default_passes_quality")]
    pub passes_quality: bool,
}

/// Estimates annualized realized volatility from historical OHLCV bars (or 24h price change fallback).
pub fn compute_annualized_vol(bars: &[OhlcvBar], asset: &ExchangeAsset) -> f64 {
    if bars.len() >= 6 {
        let mut log_rets = Vec::with_capacity(bars.len() - 1);
        for pair in bars.windows(2) {
            let p0 = pair[0].close.max(1e-12);
            let p1 = pair[1].close.max(1e-12);
            log_rets.push((p1 / p0).ln());
        }
        let n = log_rets.len() as f64;
        let mean = log_rets.iter().sum::<f64>() / n;
        let var = log_rets
            .iter()
            .map(|r| {
                let d = *r - mean;
                d * d
            })
            .sum::<f64>()
            / (n - 1.0).max(1.0);
        let std_per_bar = var.sqrt();
        let periods_per_year: f64 = match bars.last().map(|b| b.timeframe.as_str()).unwrap_or("4h")
        {
            "1m" => 365.0 * 24.0 * 60.0,
            "5m" => 365.0 * 24.0 * 12.0,
            "15m" => 365.0 * 24.0 * 4.0,
            "1h" => 365.0 * 24.0,
            "1d" => 365.0,
            _ => 365.0 * 6.0, // 4h default
        };
        let ann = (std_per_bar * periods_per_year.sqrt()).clamp(0.18, 3.20);
        return round_dp(ann, 4);
    }

    if asset.prev_day_price > 0.0 && asset.mark_price > 0.0 {
        let day_ret = ((asset.mark_price / asset.prev_day_price).ln()).abs();
        let est = (day_ret * (365.0_f64).sqrt()).clamp(0.25, 2.80);
        return round_dp(est, 4);
    }

    0.65
}

/// Estimates rolling market beta relative to `BTC-USDC` benchmark returns.
pub fn compute_market_beta(
    asset_bars: &[OhlcvBar],
    btc_bars: &[OhlcvBar],
    asset: &ExchangeAsset,
) -> f64 {
    if asset.coin.eq_ignore_ascii_case("BTC") || asset.symbol.eq_ignore_ascii_case("BTC-USDC") {
        return 1.0;
    }

    let btc_by_ts: HashMap<i64, f64> = btc_bars
        .iter()
        .filter(|b| b.close.is_finite() && b.close > 0.0)
        .map(|b| (b.timestamp_ms, b.close))
        .collect();

    let mut aligned: Vec<(i64, f64, f64)> = asset_bars
        .iter()
        .filter(|a| a.close.is_finite() && a.close > 0.0)
        .filter_map(|a| {
            btc_by_ts
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
                return round_dp(beta, 4);
            }
        }
    }

    let default_beta = match asset.coin.as_str() {
        "ETH" => 1.12,
        "SOL" | "HYPE" | "SUI" | "AVAX" | "INJ" | "TAO" => 1.35,
        _ => 1.25,
    };
    round_dp(default_beta, 4)
}

/// Computes multi-horizon risk-adjusted momentum score in `[-1.0, +1.0]`.
pub fn compute_s1_momentum_score(
    bars: &[OhlcvBar],
    asset: &ExchangeAsset,
    annualized_vol: f64,
) -> f64 {
    let vol_norm = annualized_vol.max(0.20);
    if bars.len() >= 8 {
        let n = bars.len();
        let last_px = if asset.mark_price.is_finite() && asset.mark_price > 0.0 {
            asset.mark_price
        } else {
            bars[n - 1].close.max(1e-12)
        };
        let fast_idx = n.saturating_sub(6);
        let mid_idx = n.saturating_sub(18);
        let slow_idx = n.saturating_sub(42);

        let r_fast = (last_px / bars[fast_idx].close.max(1e-12)).ln();
        let r_mid = (last_px / bars[mid_idx].close.max(1e-12)).ln();
        let r_slow = (last_px / bars[slow_idx].close.max(1e-12)).ln();

        let composite_ret = 0.45 * r_fast + 0.35 * r_mid + 0.20 * r_slow;
        let score = (composite_ret / (vol_norm * 0.18)).tanh().clamp(-1.0, 1.0);
        let (rsi_14, range_pos, bouncing_from_low) = compute_rsi_and_range_position(bars, last_px);
        if score < 0.0 && (rsi_14 < 36.0 || range_pos < 0.22 || bouncing_from_low) {
            return 0.0;
        }
        if score > 0.0
            && rsi_14 > 76.0
            && range_pos > 0.90
            && bars.last().is_some_and(|b| last_px < b.open)
        {
            return round_dp(score * 0.25, 4);
        }
        return round_dp(score, 4);
    }

    if asset.prev_day_price > 0.0 && asset.mark_price > 0.0 {
        let r_24h = (asset.mark_price / asset.prev_day_price).ln();
        let score = (r_24h / (vol_norm * 0.08)).tanh();
        return round_dp(score.clamp(-1.0, 1.0), 4);
    }

    0.0
}

/// Computes funding carry score in `[-1.0, +1.0]`.
pub fn compute_s1_carry_score(funding_rate_hourly: f64, mom_score: f64) -> f64 {
    let baseline = 0.0000125;
    let excess_funding = funding_rate_hourly - baseline;
    let carry_raw = (-excess_funding / 0.00015).tanh();
    let blended =
        (0.70 * carry_raw + 0.30 * mom_score * (1.0 - carry_raw.abs() * 0.5)).clamp(-1.0, 1.0);
    round_dp(blended, 4)
}

/// Evaluates Sleeve 1 (Cross-Sectional Carry-Momentum & Market-Beta Hedge) across the universe.
pub fn evaluate_sleeve_s1(
    assets: &[ExchangeAsset],
    bars_by_asset: &HashMap<i64, Vec<OhlcvBar>>,
    btc_bars: &[OhlcvBar],
    s1_budget: f64,
) -> HashMap<i64, CarryMomentumMetrics> {
    struct RawS1Metric {
        asset_id: i64,
        symbol: String,
        annualized_vol: f64,
        market_beta: f64,
        s1_mom_score: f64,
        s1_carry_score: f64,
        inv_vol_score: f64,
        at_range_bottom: bool,
        passes_quality: bool,
    }

    let budget = s1_budget.clamp(0.0, 1.50);
    let mut raw_metrics = Vec::with_capacity(assets.len());

    for asset in assets {
        let bars = bars_by_asset
            .get(&asset.id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let quality = evaluate_asset_liquidity_quality(asset, bars);
        let annualized_vol = compute_annualized_vol(bars, asset);
        let market_beta = compute_market_beta(bars, btc_bars, asset);
        let s1_mom_score = compute_s1_momentum_score(bars, asset, annualized_vol);
        let s1_carry_score = compute_s1_carry_score(asset.funding_rate_hourly, s1_mom_score);

        let last_px = if asset.mark_price > 0.0 {
            asset.mark_price
        } else {
            bars.last().map(|b| b.close).unwrap_or(0.0)
        };
        let at_range_bottom = if bars.len() >= 8 && last_px > 0.0 {
            let (rsi_14, range_pos, bouncing_from_low) =
                compute_rsi_and_range_position(bars, last_px);
            rsi_14 < 36.0 || range_pos < 0.22 || bouncing_from_low
        } else {
            false
        };

        let eff_carry_score = if at_range_bottom {
            s1_carry_score.max(0.0)
        } else {
            s1_carry_score
        };
        let mut composite_alpha = 0.65 * s1_mom_score + 0.35 * eff_carry_score;
        if at_range_bottom && composite_alpha < 0.0 {
            composite_alpha = 0.0;
        }
        let eff_vol = annualized_vol.max(0.45);
        let inv_vol_score = if asset.is_core_eligible
            && quality.passes_quality
            && composite_alpha.is_finite()
            && composite_alpha.abs() >= 0.05
        {
            (composite_alpha / eff_vol) * quality.activity_scale
        } else {
            0.0
        };

        raw_metrics.push(RawS1Metric {
            asset_id: asset.id,
            symbol: asset.symbol.clone(),
            annualized_vol,
            market_beta,
            s1_mom_score,
            s1_carry_score,
            inv_vol_score,
            at_range_bottom,
            passes_quality: quality.passes_quality,
        });
    }

    let eligible_scores: Vec<f64> = raw_metrics
        .iter()
        .filter(|m| m.inv_vol_score.is_finite() && m.inv_vol_score.abs() > 1e-6)
        .map(|m| m.inv_vol_score)
        .collect();
    let mean_score = if eligible_scores.len() >= 3 {
        eligible_scores.iter().sum::<f64>() / (eligible_scores.len() as f64)
    } else {
        0.0
    };

    let mut adjusted_scores: Vec<(i64, f64)> = Vec::with_capacity(raw_metrics.len());
    for m in &raw_metrics {
        let mut adj = if m.inv_vol_score.is_finite() && m.inv_vol_score.abs() > 1e-6 {
            0.65 * m.inv_vol_score + 0.35 * (m.inv_vol_score - mean_score)
        } else {
            0.0
        };
        if m.at_range_bottom && adj < 0.0 {
            adj = 0.0;
        }
        adjusted_scores.push((m.asset_id, adj));
    }

    let mut abs_scores: Vec<f64> = adjusted_scores
        .iter()
        .map(|(_, adj)| adj.abs())
        .filter(|v| v.is_finite() && *v > 1e-6)
        .collect();
    abs_scores.sort_by(|a, b| b.total_cmp(a));
    let top_abs_sum: f64 = abs_scores.iter().take(8).sum();
    let norm_denom = if top_abs_sum > 1e-8 { top_abs_sum } else { 1.0 };
    let score_lookup: HashMap<i64, f64> = adjusted_scores.into_iter().collect();

    let mut out = HashMap::with_capacity(raw_metrics.len());
    for m in raw_metrics {
        let adj = score_lookup.get(&m.asset_id).copied().unwrap_or(0.0);
        let w_s1 = if top_abs_sum > 1e-8 {
            round_dp((adj / norm_denom) * budget, 4)
        } else {
            0.0
        };

        out.insert(
            m.asset_id,
            CarryMomentumMetrics {
                asset_id: m.asset_id,
                symbol: m.symbol,
                annualized_vol: m.annualized_vol,
                market_beta: m.market_beta,
                s1_mom_score: m.s1_mom_score,
                s1_carry_score: m.s1_carry_score,
                w_s1_carry_mom: w_s1,
                passes_quality: m.passes_quality,
            },
        );
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_asset(id: i64, coin: &str, mark: f64, funding: f64) -> ExchangeAsset {
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
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn test_range_bottom_blocks_short_without_positive_sign_inversion() {
        let mut bars = Vec::with_capacity(26);
        let mut px = 100.0;
        for i in 0..25 {
            let open = px;
            px *= 0.985;
            bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 1,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 14_400_000,
                open,
                high: open * 1.002,
                low: px * 0.998,
                close: px,
                volume: 10_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        let prev = bars.last().unwrap().close;
        let bounce_px = prev * 1.005;
        bars.push(OhlcvBar {
            id: 26,
            asset_id: 1,
            timeframe: "4h".to_string(),
            timestamp_ms: 1_700_000_000_000 + 25 * 14_400_000,
            open: prev,
            high: bounce_px * 1.001,
            low: prev * 0.999,
            close: bounce_px,
            volume: 10_000.0,
            funding_rate: 0.0,
            created_at_ms: 0,
            updated_at_ms: 0,
        });
        let asset = test_asset(1, "FET", bounce_px, 0.0);
        let mom = compute_s1_momentum_score(&bars, &asset, 0.65);
        assert_eq!(
            mom, 0.0,
            "Range-bottom negative momentum must return 0.0 without flipping to +0.90"
        );
    }

    #[test]
    fn test_uniform_vol_scaling_and_uniform_demeaning_at_range_bottom() {
        // High-vol vs moderate-vol asset at range bottom with identical positive carry -> high-vol must get smaller weight (no 0.65 cap)
        let mut bars_low_vol = Vec::with_capacity(26);
        let mut bars_high_vol = Vec::with_capacity(26);
        let mut p1 = 100.0;
        let mut p2 = 100.0;
        for i in 0..25 {
            p1 *= 0.992;
            let step = if i % 2 == 0 { 0.95 } else { 1.025 };
            p2 *= step;
            bars_low_vol.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 1,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 14_400_000,
                open: p1 * 1.005,
                high: p1 * 1.006,
                low: p1 * 0.997,
                close: p1,
                volume: 10_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
            bars_high_vol.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 2,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 14_400_000,
                open: p2 * 1.01,
                high: p2 * 1.02,
                low: p2 * 0.98,
                close: p2,
                volume: 10_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        // Green bounce on final bar for both
        let b1 = bars_low_vol.last().unwrap().close * 1.004;
        bars_low_vol.push(OhlcvBar {
            id: 26,
            asset_id: 1,
            timeframe: "4h".to_string(),
            timestamp_ms: 1_700_000_000_000 + 25 * 14_400_000,
            open: b1 / 1.004,
            high: b1 * 1.001,
            low: b1 / 1.004,
            close: b1,
            volume: 10_000.0,
            funding_rate: 0.0,
            created_at_ms: 0,
            updated_at_ms: 0,
        });
        let b2 = bars_high_vol.last().unwrap().close * 1.004;
        bars_high_vol.push(OhlcvBar {
            id: 26,
            asset_id: 2,
            timeframe: "4h".to_string(),
            timestamp_ms: 1_700_000_000_000 + 25 * 14_400_000,
            open: b2 / 1.004,
            high: b2 * 1.001,
            low: b2 / 1.004,
            close: b2,
            volume: 10_000.0,
            funding_rate: 0.0,
            created_at_ms: 0,
            updated_at_ms: 0,
        });

        let a_low = test_asset(1, "LOWV", b1, -0.0005);
        let a_high = test_asset(2, "HIGHV", b2, -0.0005);
        let mut map = HashMap::new();
        map.insert(1, bars_low_vol);
        map.insert(2, bars_high_vol);

        let res = evaluate_sleeve_s1(&[a_low, a_high], &map, &[], 0.45);
        assert!(res[&2].annualized_vol > 0.65);
        assert!(
            res[&1].w_s1_carry_mom > res[&2].w_s1_carry_mom,
            "Lower-vol asset ({}) must receive strictly higher inverse-vol weight than high-vol ({})",
            res[&1].w_s1_carry_mom,
            res[&2].w_s1_carry_mom
        );

        // Uniform demeaning: 3 equal-score assets (2 uptrend + 1 range-bottom with identical funding carry) must stay balanced
        let mut map_eq = HashMap::new();
        let up_bars = |id: i64| -> Vec<OhlcvBar> {
            (0..26)
                .map(|i| {
                    let c = 100.0 * 1.008_f64.powi(i);
                    OhlcvBar {
                        id: (i + 1) as i64,
                        asset_id: id,
                        timeframe: "4h".to_string(),
                        timestamp_ms: 1_700_000_000_000 + (i as i64) * 14_400_000,
                        open: c / 1.008,
                        high: c * 1.002,
                        low: (c / 1.008) * 0.998,
                        close: c,
                        volume: 10_000.0,
                        funding_rate: 0.0,
                        created_at_ms: 0,
                        updated_at_ms: 0,
                    }
                })
                .collect()
        };
        let u1 = up_bars(10);
        let u2 = up_bars(11);
        let u3 = up_bars(12);
        let a10 = test_asset(10, "A10", u1.last().unwrap().close, -0.0004);
        let a11 = test_asset(11, "A11", u2.last().unwrap().close, -0.0004);
        let a12 = test_asset(12, "A12", u3.last().unwrap().close, -0.0004);
        map_eq.insert(10, u1);
        map_eq.insert(11, u2);
        map_eq.insert(12, u3);
        let eq_out = evaluate_sleeve_s1(&[a10, a11, a12], &map_eq, &[], 0.45);
        assert!((eq_out[&10].w_s1_carry_mom - eq_out[&11].w_s1_carry_mom).abs() < 1e-6);
    }

    #[test]
    fn test_market_beta_aligns_by_timestamp_not_tail_index() {
        let mut btc_bars = Vec::new();
        let mut asset_bars = Vec::new();
        let base_ts = 1_700_000_000_000i64;
        let step = 14_400_000i64;

        // BTC timestamps t=0..25 with alternating returns
        let mut btc_px = 50_000.0;
        let mut ast_px = 100.0;
        for i in 0..25 {
            let r = if i % 2 == 0 { 0.02 } else { -0.015 };
            btc_px *= 1.0 + r;
            btc_bars.push(OhlcvBar {
                id: i + 1,
                asset_id: 1,
                timeframe: "4h".to_string(),
                timestamp_ms: base_ts + i * step,
                open: btc_px,
                high: btc_px,
                low: btc_px,
                close: btc_px,
                volume: 1_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
            // Asset starts 5 bars later (t=5..25) and has +1.5x BTC return at matching timestamps!
            // If misaligned by tail index (i=5..25 vs btc i=0..20), phase shifts by 1 bar -> negative correlation!
            if i >= 5 {
                ast_px *= 1.0 + 1.5 * r;
                asset_bars.push(OhlcvBar {
                    id: i + 1,
                    asset_id: 2,
                    timeframe: "4h".to_string(),
                    timestamp_ms: base_ts + i * step,
                    open: ast_px,
                    high: ast_px,
                    low: ast_px,
                    close: ast_px,
                    volume: 1_000.0,
                    funding_rate: 0.0,
                    created_at_ms: 0,
                    updated_at_ms: 0,
                });
            }
        }
        let sol = test_asset(2, "SOL", ast_px, 0.0);
        let beta = compute_market_beta(&asset_bars, &btc_bars, &sol);
        assert!(
            (beta - 1.50).abs() < 0.15,
            "Timestamp-aligned beta should be ~1.50, got {beta}"
        );

        // Disjoint timestamps fall back cleanly to default beta instead of garbage covariance
        let disjoint_asset: Vec<OhlcvBar> = asset_bars
            .iter()
            .map(|b| {
                let mut c = b.clone();
                c.timestamp_ms += 1_000_000_000;
                c
            })
            .collect();
        let fallback_beta = compute_market_beta(&disjoint_asset, &btc_bars, &sol);
        assert!((fallback_beta - 1.35).abs() < 1e-6);
    }

    #[test]
    fn test_s1_zeros_dead_books_and_prevents_low_vol_amplification() {
        // 1. Dead sparse book (kSHIB: 3/12 active bars) must get 0.0 weight even with strong funding carry
        let mut kshib_bars = Vec::with_capacity(20);
        let mut px = 0.012;
        for i in 0..20 {
            let active = i >= 17;
            if active {
                px *= 1.015;
            }
            kshib_bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 100,
                timeframe: "1h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 3_600_000,
                open: px,
                high: if active { px * 1.005 } else { px },
                low: if active { px * 0.995 } else { px },
                close: px,
                volume: if active { 50_000.0 } else { 0.0 },
                funding_rate: -0.0004,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        let mut kshib = test_asset(100, "kSHIB", px, -0.0004);
        kshib.tick_size = 0.000001;
        kshib.day_volume_usdc = 12_000.0;

        // 2. Stagnant thin low-vol asset (6/12 active, $6k vol, low realized vol ~0.18 -> floored at 0.45)
        //    vs active liquid mover (12/12 active, $500k vol, ~0.55 vol) with comparable upward drift
        let mut thin_bars = Vec::with_capacity(20);
        let mut active_bars = Vec::with_capacity(20);
        let mut p_thin = 10.0;
        let mut p_act = 10.0;
        for i in 0..20 {
            let thin_live = !(8..14).contains(&i); // 6/12 active in trailing 12 bars
            if thin_live {
                p_thin *= 1.006;
            }
            thin_bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 101,
                timeframe: "1h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 3_600_000,
                open: p_thin,
                high: if thin_live { p_thin * 1.006 } else { p_thin },
                low: if thin_live { p_thin * 0.994 } else { p_thin },
                close: p_thin,
                volume: if thin_live { 500.0 } else { 0.0 },
                funding_rate: -0.0002,
                created_at_ms: 0,
                updated_at_ms: 0,
            });

            let step = if i % 2 == 0 { 0.998 } else { 1.012 };
            let open_a = p_act;
            p_act *= step;
            active_bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 102,
                timeframe: "1h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 3_600_000,
                open: open_a,
                high: open_a.max(p_act) * 1.004,
                low: open_a.min(p_act) * 0.996,
                close: p_act,
                volume: 50_000.0,
                funding_rate: -0.0002,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        let mut thin = test_asset(101, "THIN", p_thin, -0.0002);
        thin.tick_size = 0.001;
        thin.day_volume_usdc = 6_000.0;

        let mut active = test_asset(102, "ACTV", p_act, -0.0002);
        active.tick_size = 0.001;
        active.day_volume_usdc = 500_000.0;

        let mut bars_map = HashMap::new();
        bars_map.insert(100, kshib_bars);
        bars_map.insert(101, thin_bars);
        bars_map.insert(102, active_bars);

        let res = evaluate_sleeve_s1(&[kshib, thin, active], &bars_map, &[], 0.45);
        assert!(!res[&100].passes_quality);
        assert_eq!(res[&100].w_s1_carry_mom, 0.0);
        assert!(res[&101].passes_quality);
        assert!(res[&102].passes_quality);
        assert!(res[&101].annualized_vol < 0.45);
        assert!(res[&102].annualized_vol > 0.55);
        assert!(
            res[&102].w_s1_carry_mom > res[&101].w_s1_carry_mom,
            "Active liquid mover ({}) must outweigh stagnant thin low-vol coin ({})",
            res[&102].w_s1_carry_mom,
            res[&101].w_s1_carry_mom
        );

        // 3. Verify eff_vol >= 0.45 floor: two liquid 12/12 coins at range bottom with identical
        //    positive carry (and zero momentum) at vol=0.18 vs vol=0.40 must receive equal weight
        //    (no 1.8x/4x low-vol amplification below 0.45).
        let mk_bottom_bars = |id: i64, swing: f64| -> Vec<OhlcvBar> {
            let mut out = Vec::with_capacity(26);
            let mut p = 100.0;
            for i in 0..25 {
                let mult = if i % 2 == 0 {
                    0.992 - swing
                } else {
                    0.992 + swing
                };
                let open = p;
                p *= mult;
                out.push(OhlcvBar {
                    id: (i + 1) as i64,
                    asset_id: id,
                    timeframe: "4h".to_string(),
                    timestamp_ms: 1_700_000_000_000 + (i as i64) * 14_400_000,
                    open,
                    high: open.max(p) * 1.005,
                    low: open.min(p) * 0.995,
                    close: p,
                    volume: 50_000.0,
                    funding_rate: -0.0005,
                    created_at_ms: 0,
                    updated_at_ms: 0,
                });
            }
            let prev = out.last().unwrap().close;
            let bounce = prev * 1.004;
            out.push(OhlcvBar {
                id: 26,
                asset_id: id,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_000_000_000 + 25 * 14_400_000,
                open: prev,
                high: bounce * 1.002,
                low: prev * 0.998,
                close: bounce,
                volume: 50_000.0,
                funding_rate: -0.0005,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
            out
        };
        let b_v18 = mk_bottom_bars(201, 0.0);
        let b_v40 = mk_bottom_bars(202, 0.0075);
        let a_v18 = test_asset(201, "V18", b_v18.last().unwrap().close, -0.0005);
        let a_v40 = test_asset(202, "V40", b_v40.last().unwrap().close, -0.0005);
        let mut floor_map = HashMap::new();
        floor_map.insert(201, b_v18);
        floor_map.insert(202, b_v40);
        let floor_res = evaluate_sleeve_s1(&[a_v18, a_v40], &floor_map, &[], 0.45);
        assert!(floor_res[&201].annualized_vol < 0.25);
        assert!(
            floor_res[&202].annualized_vol > 0.30 && floor_res[&202].annualized_vol <= 0.45,
            "Expected second coin vol in (0.30, 0.45], got {}",
            floor_res[&202].annualized_vol
        );
        assert!(
            (floor_res[&201].w_s1_carry_mom - floor_res[&202].w_s1_carry_mom).abs() < 1e-6,
            "Coins below 0.45 eff_vol floor must receive equal weight ({} vs {})",
            floor_res[&201].w_s1_carry_mom,
            floor_res[&202].w_s1_carry_mom
        );
    }
}
