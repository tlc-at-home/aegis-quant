//! Cross-Sectional Market Breadth & Regime Classifier (`aegis-signals::breadth`).
//!
//! Computes `market_breadth_bull`, `market_beta_exposure`, and `market_regime_label`
//! across the active Hyperliquid perpetual universe.

use aegis_core::{round_dp, ExchangeAsset, OhlcvBar};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Minimum eligible coin count required before breadth regime gating activates,
/// preventing small unit/smoke fixtures from self-blocking.
pub const MIN_REGIME_GATE_COINS: i32 = 3;

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

/// Configuration thresholds for breadth-driven directional entry gating (`P1-Strat`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct RegimeGateConfig {
    pub enable_breadth_regime_gate: bool,
    pub bull_block_shorts_threshold: f64,
    pub bear_block_longs_threshold: f64,
}

impl Default for RegimeGateConfig {
    fn default() -> Self {
        Self {
            enable_breadth_regime_gate: true,
            bull_block_shorts_threshold: 0.68,
            bear_block_longs_threshold: 0.32,
        }
    }
}

/// Active macro regime directional entry gate computed prior to sleeve normalization (`P1-Strat`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RegimeGateState {
    pub breadth_bull: f64,
    pub eligible_coin_count: i32,
    pub regime_label: String,
    pub block_new_shorts: bool,
    pub block_new_longs: bool,
}

/// Computes `RegimeGateState` from cross-sectional breadth, eligible count, average funding, and capitulation count.
pub fn compute_regime_gate_state(
    breadth_bull: f64,
    eligible_coin_count: i32,
    avg_funding_hourly: f64,
    capitulation_count: usize,
    cfg: &RegimeGateConfig,
) -> RegimeGateState {
    let eff_breadth = if breadth_bull.is_finite() {
        breadth_bull.clamp(0.0, 1.0)
    } else {
        0.50
    };
    let eff_funding = if avg_funding_hourly.is_finite() {
        avg_funding_hourly
    } else {
        0.0
    };
    let regime_label = classify_market_regime(eff_breadth, eff_funding, capitulation_count);
    let gate_active =
        cfg.enable_breadth_regime_gate && eligible_coin_count >= MIN_REGIME_GATE_COINS;
    let bull_thresh = if cfg.bull_block_shorts_threshold.is_finite() {
        cfg.bull_block_shorts_threshold.clamp(0.55, 0.95)
    } else {
        0.68
    };
    let bear_thresh = if cfg.bear_block_longs_threshold.is_finite() {
        cfg.bear_block_longs_threshold.clamp(0.05, 0.45)
    } else {
        0.32
    };
    let block_new_shorts =
        gate_active && (eff_breadth >= bull_thresh || regime_label == "BULL_EXPANSION");
    let block_new_longs = gate_active
        && (eff_breadth <= bear_thresh
            || matches!(
                regime_label.as_str(),
                "BEAR_CONTRACTION" | "CAPITULATION_SHOCK"
            ));

    RegimeGateState {
        breadth_bull: eff_breadth,
        eligible_coin_count,
        regime_label,
        block_new_shorts,
        block_new_longs,
    }
}

/// Computes preliminary cross-sectional bullish breadth fraction in `[0.0, 1.0]` before Sleeve 2 runs.
pub fn compute_preliminary_bull_breadth<B: AsRef<[OhlcvBar]>>(
    assets: &[ExchangeAsset],
    bars_by_asset: &HashMap<i64, B>,
) -> (f64, i32) {
    let mut eligible_count = 0i32;
    let mut bull_count = 0.0;

    for asset in assets {
        if (!asset.is_core_eligible && !asset.is_event_eligible)
            || !asset.day_volume_usdc.is_finite()
            || asset.day_volume_usdc <= 0.0
        {
            continue;
        }
        eligible_count += 1;

        if let Some(bars_entry) = bars_by_asset.get(&asset.id) {
            let bars = bars_entry.as_ref();
            if bars.len() >= 4 {
                let first = bars[bars.len().saturating_sub(6)].close.max(1e-12);
                let last = if asset.mark_price.is_finite() && asset.mark_price > 0.0 {
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

        if asset.prev_day_price.is_finite()
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
pub fn compute_market_breadth_snapshot<B: AsRef<[OhlcvBar]>>(
    assets: &[ExchangeAsset],
    bars_by_asset: &HashMap<i64, B>,
    beta_by_asset: &HashMap<i64, f64>,
    portfolio_weights: &HashMap<i64, f64>,
    capitulation_count: usize,
) -> MarketBreadthSnapshot {
    let (market_breadth_bull, eligible_coin_count) =
        compute_preliminary_bull_breadth(assets, bars_by_asset);
    compute_market_breadth_snapshot_from_prelim(
        assets,
        market_breadth_bull,
        eligible_coin_count,
        beta_by_asset,
        portfolio_weights,
        capitulation_count,
    )
}

/// Finalizes `MarketBreadthSnapshot` using a precomputed `(market_breadth_bull, eligible_coin_count)`.
pub(crate) fn compute_market_breadth_snapshot_from_prelim(
    assets: &[ExchangeAsset],
    market_breadth_bull: f64,
    eligible_coin_count: i32,
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

    let eligible_fundings: Vec<f64> = assets
        .iter()
        .filter(|a| {
            (a.is_core_eligible || a.is_event_eligible)
                && a.day_volume_usdc.is_finite()
                && a.day_volume_usdc > 0.0
                && a.funding_rate_hourly.is_finite()
        })
        .map(|a| a.funding_rate_hourly)
        .collect();
    let avg_funding = if eligible_fundings.is_empty() {
        0.0
    } else {
        eligible_fundings.iter().sum::<f64>() / (eligible_fundings.len() as f64)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_asset(
        id: i64,
        coin: &str,
        mark_price: f64,
        prev_day_price: f64,
        funding_rate_hourly: f64,
        day_volume_usdc: f64,
        eligible: bool,
    ) -> ExchangeAsset {
        ExchangeAsset {
            id,
            symbol: format!("{coin}-USDC"),
            coin: coin.to_string(),
            asset_index: id as u32,
            sz_decimals: 2,
            tick_size: 0.01,
            lot_step_size: 0.01,
            max_leverage: 20.0,
            mark_price,
            mid_price: mark_price,
            oracle_price: mark_price,
            funding_rate_hourly,
            day_volume_usdc,
            open_interest_usdc: day_volume_usdc * 0.4,
            prev_day_price,
            is_core_eligible: eligible,
            is_event_eligible: eligible,
            is_priority_watched: false,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_000_000,
        }
    }

    fn mk_bars(asset_id: i64, closes: &[f64]) -> Vec<OhlcvBar> {
        closes
            .iter()
            .enumerate()
            .map(|(i, &c)| OhlcvBar {
                id: (i + 1) as i64,
                asset_id,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 14_400_000,
                open: c,
                high: c * 1.005,
                low: c * 0.995,
                close: c,
                volume: 10_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            })
            .collect()
    }

    #[test]
    fn test_r14_l23_breadth_all_regimes_and_boundaries() {
        // 1. All 7 label outputs across the 6 macro regime families & exact thresholds
        assert_eq!(classify_market_regime(0.68, 0.0, 0), "BULL_EXPANSION");
        assert_eq!(classify_market_regime(0.6799, 0.0, 0), "BULL_TREND");
        assert_eq!(classify_market_regime(0.55, 0.00004, 0), "BULL_TREND");
        assert_eq!(classify_market_regime(0.55, 0.000041, 0), "BULL_CARRY");
        assert_eq!(
            classify_market_regime(0.5499, 0.0002, 0),
            "NEUTRAL_ROTATION"
        );
        assert_eq!(classify_market_regime(0.4501, 0.0, 0), "NEUTRAL_ROTATION");
        assert_eq!(classify_market_regime(0.45, 0.0, 0), "DEFENSIVE_CHOP");
        assert_eq!(classify_market_regime(0.3201, 0.0, 0), "DEFENSIVE_CHOP");
        assert_eq!(classify_market_regime(0.32, 0.0, 0), "BEAR_CONTRACTION");
        assert_eq!(classify_market_regime(0.2999, 0.0, 1), "BEAR_CONTRACTION");
        assert_eq!(classify_market_regime(0.30, 0.0, 2), "BEAR_CONTRACTION");
        assert_eq!(classify_market_regime(0.2999, 0.0, 2), "CAPITULATION_SHOCK");

        // 2. Preliminary breadth: empty, ineligible, zero/NaN volume -> (0.50, 0)
        let empty_map: HashMap<i64, Vec<OhlcvBar>> = HashMap::new();
        assert_eq!(compute_preliminary_bull_breadth(&[], &empty_map), (0.50, 0));

        let inelig = mk_asset(1, "DEAD", 100.0, 90.0, 0.0, 1_000_000.0, false);
        let zero_vol = mk_asset(2, "ZV", 100.0, 90.0, 0.0, 0.0, true);
        let nan_vol = mk_asset(3, "NV", 100.0, 90.0, 0.0, f64::NAN, true);
        assert_eq!(
            compute_preliminary_bull_breadth(&[inelig.clone(), zero_vol, nan_vol], &empty_map),
            (0.50, 0)
        );

        // 3. Bar-based (> +0.2%, flat <= 0.2%, < -0.2%) + prev_day_price fallback + missing prev_day
        let a_bull = mk_asset(10, "BULL", 101.0, 100.0, 0.0001, 5_000_000.0, true);
        let a_flat = mk_asset(11, "FLAT", 100.1, 100.0, 0.0001, 5_000_000.0, true);
        let a_bear = mk_asset(12, "BEAR", 99.0, 100.0, 0.0001, 5_000_000.0, true);
        let a_prev_bull = mk_asset(13, "PB", 105.0, 100.0, 0.0003, 5_000_000.0, true);
        let a_no_ref = mk_asset(14, "NR", 100.0, 0.0, 0.0000, 5_000_000.0, true);

        let mut bar_map: HashMap<i64, Vec<OhlcvBar>> = HashMap::new();
        bar_map.insert(10, mk_bars(10, &[100.0, 100.2, 100.5, 100.8, 101.0])); // +1.0% -> +1.0
        bar_map.insert(11, mk_bars(11, &[100.0, 100.0, 100.1, 100.1, 100.1])); // +0.1% -> +0.5
        bar_map.insert(12, mk_bars(12, &[100.0, 99.8, 99.5, 99.2, 99.0])); // -1.0% -> 0.0
                                                                           // 13 uses prev_day_price (+5% -> +1.0); 14 has no bars/prev_day -> +0.5
                                                                           // Total = (1.0 + 0.5 + 0.0 + 1.0 + 0.5) / 5 = 3.0 / 5 = 0.60
        let universe = [a_bull, a_flat, a_bear, a_prev_bull, a_no_ref, inelig];
        let (breadth, count) = compute_preliminary_bull_breadth(&universe, &bar_map);
        assert_eq!(count, 5);
        assert!((breadth - 0.60).abs() < 1e-6);

        // 4. Market breadth snapshot: beta exposure (with NaN/missing fallback to 1.0) + eligible-only avg funding
        let mut beta_map = HashMap::new();
        beta_map.insert(10, 1.5);
        beta_map.insert(11, f64::NAN); // falls back to 1.0
                                       // 12 missing -> falls back to 1.0
        let mut weights = HashMap::new();
        weights.insert(10, 0.20); // 0.20 * 1.5 = 0.30
        weights.insert(11, -0.10); // -0.10 * 1.0 = -0.10
        weights.insert(12, 0.05); // 0.05 * 1.0 = 0.05 -> total = 0.25
        let mut universe_with_bad_elig_funding = universe.to_vec();
        universe_with_bad_elig_funding[5].funding_rate_hourly = 0.99; // ineligible -> must be ignored
        let snap = compute_market_breadth_snapshot(
            &universe_with_bad_elig_funding,
            &bar_map,
            &beta_map,
            &weights,
            0,
        );
        assert_eq!(snap.eligible_coin_count, 5);
        assert!((snap.market_breadth_bull - 0.60).abs() < 1e-6);
        assert!((snap.market_beta_exposure - 0.25).abs() < 1e-6);
        // avg funding across 5 eligible = (0.0001*3 + 0.0003 + 0.0) / 5 = 0.00012 > 0.00004 -> BULL_CARRY
        assert_eq!(snap.market_regime_label, "BULL_CARRY");

        // 5. RegimeGateState gating and MIN_REGIME_GATE_COINS threshold
        let cfg = RegimeGateConfig::default();
        let small = compute_regime_gate_state(0.80, 2, 0.0, 0, &cfg);
        assert!(!small.block_new_shorts);
        assert!(!small.block_new_longs);

        let bull_gate = compute_regime_gate_state(0.68, 3, 0.0, 0, &cfg);
        assert!(bull_gate.block_new_shorts);
        assert!(!bull_gate.block_new_longs);

        let bear_gate = compute_regime_gate_state(0.32, 3, 0.0, 0, &cfg);
        assert!(!bear_gate.block_new_shorts);
        assert!(bear_gate.block_new_longs);

        let disabled = compute_regime_gate_state(
            0.90,
            10,
            0.0,
            0,
            &RegimeGateConfig {
                enable_breadth_regime_gate: false,
                ..cfg
            },
        );
        assert!(!disabled.block_new_shorts);
        assert!(!disabled.block_new_longs);
    }
}
