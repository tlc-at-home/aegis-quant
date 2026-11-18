//! Cross-Sectional Market Breadth & Regime Classifier (`aegis-signals::breadth`).
//!
//! Computes `market_breadth_bull`, `market_beta_exposure`, and `market_regime_label`
//! across the active Hyperliquid perpetual universe.

use aegis_core::{round_dp, ExchangeAsset, OhlcvBar};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Cross-sectional market breadth and portfolio beta exposure snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketBreadthSnapshot {
    pub market_breadth_bull: f64,
    pub market_beta_exposure: f64,
    pub market_regime_label: String,
    pub eligible_coin_count: i32,
}

impl Default for MarketBreadthSnapshot {
    fn default() -> Self {
        Self {
            market_breadth_bull: 0.50,
            market_beta_exposure: 0.0,
            market_regime_label: "NEUTRAL_ROTATION".to_string(),
            eligible_coin_count: 0,
        }
    }
}

/// Computes preliminary cross-sectional bullish breadth fraction in `[0.0, 1.0]` before Sleeve 2 runs.
pub fn compute_preliminary_bull_breadth(
    assets: &[ExchangeAsset],
    bars_by_asset: &HashMap<i64, Vec<OhlcvBar>>,
) -> (f64, i32) {
    let mut eligible_count = 0i32;
    let mut bull_count = 0.0;

    for asset in assets {
        if !asset.is_core_eligible && !asset.is_event_eligible && asset.day_volume_usdc <= 0.0 {
            continue;
        }
        eligible_count += 1;

        if let Some(bars) = bars_by_asset.get(&asset.id) {
            if bars.len() >= 4 {
                let first = bars[bars.len().saturating_sub(6)].close.max(1e-12);
                let last = if asset.mark_price > 0.0 {
                    asset.mark_price
                } else {
                    bars[bars.len() - 1].close
                };
                if last > first * 1.002 {
                    bull_count += 1.0;
                } else if (last - first).abs() / first <= 0.002 {
                    bull_count += 0.5;
                }
                continue;
            }
        }

        if asset.prev_day_price > 0.0 && asset.mark_price > 0.0 {
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

    if eligible_count <= 0 {
        return (0.50, 0);
    }

    let breadth = round_dp((bull_count / (eligible_count as f64)).clamp(0.0, 1.0), 4);
    (breadth, eligible_count)
}

/// Classifies the macro market regime label from cross-sectional breadth, average funding, and event triggers.
pub fn classify_market_regime(
    market_breadth_bull: f64,
    avg_funding_hourly: f64,
    capitulation_count: usize,
) -> String {
    if capitulation_count >= 2 && market_breadth_bull < 0.30 {
        "CAPITULATION_SHOCK".to_string()
    } else if market_breadth_bull >= 0.68 {
        "BULL_EXPANSION".to_string()
    } else if market_breadth_bull >= 0.55 {
        if avg_funding_hourly > 0.00004 {
            "BULL_CARRY".to_string()
        } else {
            "BULL_TREND".to_string()
        }
    } else if market_breadth_bull <= 0.32 {
        "BEAR_CONTRACTION".to_string()
    } else if market_breadth_bull <= 0.45 {
        "DEFENSIVE_CHOP".to_string()
    } else {
        "NEUTRAL_ROTATION".to_string()
    }
}

/// Finalizes the `MarketBreadthSnapshot` including portfolio-weighted beta exposure (`sum(w_i * beta_i)`).
pub fn compute_market_breadth_snapshot(
    assets: &[ExchangeAsset],
    bars_by_asset: &HashMap<i64, Vec<OhlcvBar>>,
    beta_by_asset: &HashMap<i64, f64>,
    target_weights: &HashMap<i64, f64>,
    capitulation_count: usize,
) -> MarketBreadthSnapshot {
    let (market_breadth_bull, eligible_coin_count) =
        compute_preliminary_bull_breadth(assets, bars_by_asset);

    let mut beta_exposure = 0.0;
    for (asset_id, &w) in target_weights {
        let beta = beta_by_asset.get(asset_id).copied().unwrap_or(1.0);
        beta_exposure += w * beta;
    }

    let avg_funding = if assets.is_empty() {
        0.0
    } else {
        assets.iter().map(|a| a.funding_rate_hourly).sum::<f64>() / (assets.len() as f64)
    };

    let market_regime_label =
        classify_market_regime(market_breadth_bull, avg_funding, capitulation_count);

    MarketBreadthSnapshot {
        market_breadth_bull,
        market_beta_exposure: round_dp(beta_exposure, 4),
        market_regime_label,
        eligible_coin_count,
    }
}
