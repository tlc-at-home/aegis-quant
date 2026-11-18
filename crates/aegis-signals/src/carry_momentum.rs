//! Sleeve 1: Cross-Sectional Carry-Momentum & Market-Beta Hedge (`aegis-signals::carry_momentum`).
//!
//! Computes per-asset `s1_mom_score`, `s1_carry_score`, `market_beta`, `annualized_vol`,
//! and risk-adjusted inverse-volatility weight `w_s1_carry_mom` across core-eligible Hyperliquid perps.

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

    let n = asset_bars.len().min(btc_bars.len());
    if n >= 8 {
        let a_slice = &asset_bars[asset_bars.len() - n..];
        let b_slice = &btc_bars[btc_bars.len() - n..];

        let mut a_rets = Vec::with_capacity(n - 1);
        let mut b_rets = Vec::with_capacity(n - 1);
        for i in 1..n {
            let ar = (a_slice[i].close.max(1e-12) / a_slice[i - 1].close.max(1e-12)).ln();
            let br = (b_slice[i].close.max(1e-12) / b_slice[i - 1].close.max(1e-12)).ln();
            a_rets.push(ar);
            b_rets.push(br);
        }

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
        let last_px = if asset.mark_price > 0.0 {
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
        let score = (composite_ret / (vol_norm * 0.18)).tanh();
        return round_dp(score.clamp(-1.0, 1.0), 4);
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
    let budget = s1_budget.clamp(0.0, 1.50);
    let mut raw_metrics = Vec::with_capacity(assets.len());

    for asset in assets {
        let bars = bars_by_asset
            .get(&asset.id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let annualized_vol = compute_annualized_vol(bars, asset);
        let market_beta = compute_market_beta(bars, btc_bars, asset);
        let s1_mom_score = compute_s1_momentum_score(bars, asset, annualized_vol);
        let s1_carry_score = compute_s1_carry_score(asset.funding_rate_hourly, s1_mom_score);

        let composite_alpha = 0.65 * s1_mom_score + 0.35 * s1_carry_score;
        let inv_vol_score = if asset.is_core_eligible && composite_alpha.abs() >= 0.05 {
            composite_alpha / annualized_vol.max(0.25)
        } else {
            0.0
        };

        raw_metrics.push((
            asset.id,
            asset.symbol.clone(),
            annualized_vol,
            market_beta,
            s1_mom_score,
            s1_carry_score,
            inv_vol_score,
        ));
    }

    let eligible_scores: Vec<f64> = raw_metrics
        .iter()
        .filter(|m| m.6.abs() > 1e-6)
        .map(|m| m.6)
        .collect();
    let mean_score = if eligible_scores.len() >= 3 {
        eligible_scores.iter().sum::<f64>() / (eligible_scores.len() as f64)
    } else {
        0.0
    };

    let mut adjusted_scores: Vec<(i64, f64)> = Vec::with_capacity(raw_metrics.len());
    let mut abs_sum = 0.0;
    for m in &raw_metrics {
        let adj = if m.6.abs() > 1e-6 {
            0.65 * m.6 + 0.35 * (m.6 - mean_score)
        } else {
            0.0
        };
        abs_sum += adj.abs();
        adjusted_scores.push((m.0, adj));
    }

    let norm_denom = if abs_sum > 1e-8 { abs_sum } else { 1.0 };
    let score_lookup: HashMap<i64, f64> = adjusted_scores.into_iter().collect();

    let mut out = HashMap::with_capacity(raw_metrics.len());
    for (asset_id, symbol, annualized_vol, market_beta, s1_mom_score, s1_carry_score, _) in
        raw_metrics
    {
        let adj = score_lookup.get(&asset_id).copied().unwrap_or(0.0);
        let w_s1 = if abs_sum > 1e-8 {
            round_dp((adj / norm_denom) * budget, 4)
        } else {
            0.0
        };

        out.insert(
            asset_id,
            CarryMomentumMetrics {
                asset_id,
                symbol,
                annualized_vol,
                market_beta,
                s1_mom_score,
                s1_carry_score,
                w_s1_carry_mom: w_s1,
            },
        );
    }

    out
}
