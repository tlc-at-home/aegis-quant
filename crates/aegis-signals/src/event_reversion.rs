//! Sleeve 3: Live Capitulation Flush & Funding Dislocation Harvester (`aegis-signals::event_reversion`).
//!
//! Computes `s3_funding_zscore`, `s3_flush_atr_mult`, `w_s3_event_reversion`,
//! and detects `"CAPITULATION_FLUSH"` and `"FUNDING_SQUEEZE"` triggers.

use crate::trend_breakout::causal_ema;
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

    // Cross-sectional funding rate distribution (with baseline floor on standard deviation)
    let n = assets.len() as f64;
    let mean_funding = assets.iter().map(|a| a.funding_rate_hourly).sum::<f64>() / n;
    let var_funding = assets
        .iter()
        .map(|a| {
            let d = a.funding_rate_hourly - mean_funding;
            d * d
        })
        .sum::<f64>()
        / (n - 1.0).max(1.0);
    let std_funding = var_funding.sqrt().max(0.00004);

    let mut raw_items = Vec::with_capacity(assets.len());
    let mut abs_sum = 0.0;

    for asset in assets {
        let bars = bars_by_asset
            .get(&asset.id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let f_zscore = round_dp(
            ((asset.funding_rate_hourly - mean_funding) / std_funding).clamp(-6.0, 6.0),
            4,
        );
        let flush_atr = compute_flush_atr_multiple(asset, bars);

        let is_capitulation = flush_atr.abs() >= 2.0;
        let is_funding_squeeze = f_zscore.abs() >= 1.8;

        let trigger = if is_capitulation {
            Some(TRIGGER_CAPITULATION_FLUSH.to_string())
        } else if is_funding_squeeze {
            Some(TRIGGER_FUNDING_SQUEEZE.to_string())
        } else {
            None
        };

        let eligible = asset.is_event_eligible || asset.is_core_eligible || trigger.is_some();

        // Contrarian mean-reversion & funding dislocation score:
        // - Deep downward flush (`flush_atr <= -2.0`) -> contrarian LONG (+score)
        // - Deeply negative funding (`f_zscore <= -1.8`) -> short squeeze LONG (+score)
        // - Extreme blow-off + crowded positive funding -> contrarian SHORT (-score)
        let mut raw_score = 0.0;
        if eligible {
            if is_capitulation {
                raw_score += (-flush_atr * 0.45).clamp(-1.5, 1.5);
            }
            if is_funding_squeeze {
                raw_score += (-f_zscore * 0.40).clamp(-1.5, 1.5);
            }
        }

        abs_sum += raw_score.abs();
        raw_items.push((
            asset.id,
            asset.symbol.clone(),
            f_zscore,
            flush_atr,
            raw_score,
            trigger,
        ));
    }

    let denom = if abs_sum > 1e-8 { abs_sum } else { 1.0 };
    let mut out = HashMap::with_capacity(raw_items.len());

    for (asset_id, symbol, s3_funding_zscore, s3_flush_atr_mult, score, event_trigger) in raw_items
    {
        let w_s3 = if abs_sum > 1e-8 && score.abs() > 1e-6 {
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
