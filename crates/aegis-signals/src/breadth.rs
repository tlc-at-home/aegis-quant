//! Cross-Sectional Market Breadth & Regime Classifier (`aegis-signals::breadth`).
//!
//! Computes `market_breadth_bull`, `market_beta_exposure`, and `market_regime_label`
//! across the active Hyperliquid perpetual universe.

use aegis_core::indicators::infer_bar_hours;
use aegis_core::{round_dp, ExchangeAsset, OhlcvBar};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Minimum eligible coin count required before breadth regime gating activates,
/// preventing small unit/smoke fixtures from self-blocking.
pub const MIN_REGIME_GATE_COINS: i32 = 3;

/// Canonical macro market regime variants emitted by cross-sectional breadth classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum MarketRegime {
    CapitulationShock,
    BullExpansion,
    BullCarry,
    BullTrend,
    #[default]
    NeutralRotation,
    DefensiveChop,
    BearContraction,
}

impl MarketRegime {
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CapitulationShock => "CAPITULATION_SHOCK",
            Self::BullExpansion => "BULL_EXPANSION",
            Self::BullCarry => "BULL_CARRY",
            Self::BullTrend => "BULL_TREND",
            Self::NeutralRotation => "NEUTRAL_ROTATION",
            Self::DefensiveChop => "DEFENSIVE_CHOP",
            Self::BearContraction => "BEAR_CONTRACTION",
        }
    }
}

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

/// Configuration thresholds for breadth-driven directional entry gating and N1 regime budget multipliers (`P1-Strat`, `N1`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct RegimeGateConfig {
    pub enable_breadth_regime_gate: bool,
    pub bull_block_shorts_threshold: f64,
    pub bear_block_longs_threshold: f64,
    #[serde(default)]
    pub enable_regime_budget_multipliers: bool,
    #[serde(default = "default_chop_long_weight_mult")]
    pub chop_long_weight_mult: f64,
    #[serde(default = "default_chop_short_weight_mult")]
    pub chop_short_weight_mult: f64,
    #[serde(default = "default_neutral_long_weight_mult")]
    pub neutral_long_weight_mult: f64,
}

fn default_chop_long_weight_mult() -> f64 {
    0.50
}

fn default_chop_short_weight_mult() -> f64 {
    0.65
}

fn default_neutral_long_weight_mult() -> f64 {
    0.65
}

fn default_one() -> f64 {
    1.0
}

impl Default for RegimeGateConfig {
    fn default() -> Self {
        Self {
            enable_breadth_regime_gate: true,
            bull_block_shorts_threshold: 0.68,
            bear_block_longs_threshold: 0.32,
            enable_regime_budget_multipliers: false,
            chop_long_weight_mult: 0.50,
            chop_short_weight_mult: 0.65,
            neutral_long_weight_mult: 0.65,
        }
    }
}

/// Active macro regime directional entry gate and directional budget multipliers computed prior to sleeve normalization (`P1-Strat`, `N1`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegimeGateState {
    pub breadth_bull: f64,
    pub eligible_coin_count: i32,
    pub regime_label: String,
    #[serde(default)]
    pub regime: MarketRegime,
    pub block_new_shorts: bool,
    pub block_new_longs: bool,
    #[serde(default = "default_one")]
    pub long_weight_mult: f64,
    #[serde(default = "default_one")]
    pub short_weight_mult: f64,
}

impl Default for RegimeGateState {
    fn default() -> Self {
        Self {
            breadth_bull: 0.0,
            eligible_coin_count: 0,
            regime_label: String::new(),
            regime: MarketRegime::NeutralRotation,
            block_new_shorts: false,
            block_new_longs: false,
            long_weight_mult: 1.0,
            short_weight_mult: 1.0,
        }
    }
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
    let regime = classify_market_regime_enum(eff_breadth, eff_funding, capitulation_count);
    let regime_label = regime.as_str().to_string();
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
        gate_active && (eff_breadth >= bull_thresh || regime == MarketRegime::BullExpansion);
    let block_new_longs = gate_active
        && (eff_breadth <= bear_thresh
            || matches!(
                regime,
                MarketRegime::BearContraction | MarketRegime::CapitulationShock
            ));

    let (mut long_weight_mult, mut short_weight_mult) =
        if cfg.enable_regime_budget_multipliers && gate_active && eligible_coin_count >= 3 {
            let neutral_long = if cfg.neutral_long_weight_mult.is_finite() {
                cfg.neutral_long_weight_mult.clamp(0.0, 1.0)
            } else {
                0.65
            };
            let chop_long = if cfg.chop_long_weight_mult.is_finite() {
                cfg.chop_long_weight_mult.clamp(0.0, 1.0)
            } else {
                0.50
            };
            let chop_short = if cfg.chop_short_weight_mult.is_finite() {
                cfg.chop_short_weight_mult.clamp(0.0, 1.0)
            } else {
                0.65
            };
            match regime {
                MarketRegime::BullExpansion => (1.0, 0.0),
                MarketRegime::BullCarry | MarketRegime::BullTrend => (1.0, 1.0),
                MarketRegime::NeutralRotation => (neutral_long, 1.0),
                MarketRegime::DefensiveChop => (chop_long, chop_short),
                MarketRegime::BearContraction | MarketRegime::CapitulationShock => (0.0, 1.0),
            }
        } else {
            (1.0, 1.0)
        };
    if cfg.enable_regime_budget_multipliers {
        if block_new_longs {
            long_weight_mult = 0.0;
        }
        if block_new_shorts {
            short_weight_mult = 0.0;
        }
    }

    RegimeGateState {
        breadth_bull: eff_breadth,
        eligible_coin_count,
        regime_label,
        regime,
        block_new_shorts,
        block_new_longs,
        long_weight_mult,
        short_weight_mult,
    }
}

/// Anchors 24h breadth on closed-bar price unless live `mark_price` diverged `>= 3.5%` intra-bar.
#[inline]
pub(crate) fn effective_breadth_anchor_price(mark_price: f64, last_bar_close: Option<f64>) -> f64 {
    if let Some(close) = last_bar_close.filter(|c| c.is_finite() && *c > 0.0) {
        if mark_price.is_finite() && mark_price > 0.0 && (mark_price - close).abs() / close >= 0.035
        {
            mark_price
        } else {
            close
        }
    } else if mark_price.is_finite() && mark_price > 0.0 {
        mark_price
    } else {
        0.0
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
                let bar_hours = infer_bar_hours(bars);
                let mult = (4.0 / bar_hours).clamp(0.25, 16.0);
                let lookback_24h = (6.0 * mult).round().max(1.0) as usize;
                let first = bars[bars.len().saturating_sub(lookback_24h)]
                    .close
                    .max(1e-12);
                let last = effective_breadth_anchor_price(
                    asset.mark_price,
                    Some(bars[bars.len() - 1].close),
                );
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

/// Classifies the macro market regime variant from cross-sectional breadth, average funding, and event triggers.
pub fn classify_market_regime_enum(
    market_breadth_bull: f64,
    avg_funding_hourly: f64,
    capitulation_count: usize,
) -> MarketRegime {
    if capitulation_count >= 2 && market_breadth_bull < 0.30 {
        MarketRegime::CapitulationShock
    } else if market_breadth_bull >= 0.68 {
        MarketRegime::BullExpansion
    } else if market_breadth_bull >= 0.55 {
        if avg_funding_hourly > 0.00004 {
            MarketRegime::BullCarry
        } else {
            MarketRegime::BullTrend
        }
    } else if market_breadth_bull <= 0.32 {
        MarketRegime::BearContraction
    } else if market_breadth_bull <= 0.45 {
        MarketRegime::DefensiveChop
    } else {
        MarketRegime::NeutralRotation
    }
}

/// Classifies the macro market regime label from cross-sectional breadth, average funding, and event triggers.
pub fn classify_market_regime(
    market_breadth_bull: f64,
    avg_funding_hourly: f64,
    capitulation_count: usize,
) -> String {
    classify_market_regime_enum(market_breadth_bull, avg_funding_hourly, capitulation_count)
        .as_str()
        .to_string()
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
        assert!((disabled.long_weight_mult - 1.0).abs() < 1e-9);
        assert!((disabled.short_weight_mult - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_n1_regime_budget_multipliers_and_1h_preliminary_breadth() {
        let mut n1_cfg = RegimeGateConfig {
            enable_regime_budget_multipliers: true,
            ..RegimeGateConfig::default()
        };

        // Small universe (< 3) keeps (1.0, 1.0)
        let small = compute_regime_gate_state(0.40, 2, 0.0, 0, &n1_cfg);
        assert_eq!(small.long_weight_mult, 1.0);
        assert_eq!(small.short_weight_mult, 1.0);

        // All 7 regimes when N1 enabled and gate active
        let bull_exp = compute_regime_gate_state(0.75, 5, 0.0, 0, &n1_cfg);
        assert_eq!(bull_exp.regime, MarketRegime::BullExpansion);
        assert_eq!(
            (bull_exp.long_weight_mult, bull_exp.short_weight_mult),
            (1.0, 0.0)
        );

        let bull_carry = compute_regime_gate_state(0.60, 5, 0.0001, 0, &n1_cfg);
        assert_eq!(bull_carry.regime, MarketRegime::BullCarry);
        assert_eq!(
            (bull_carry.long_weight_mult, bull_carry.short_weight_mult),
            (1.0, 1.0)
        );

        let bull_trend = compute_regime_gate_state(0.60, 5, 0.0, 0, &n1_cfg);
        assert_eq!(bull_trend.regime, MarketRegime::BullTrend);
        assert_eq!(
            (bull_trend.long_weight_mult, bull_trend.short_weight_mult),
            (1.0, 1.0)
        );

        let neutral = compute_regime_gate_state(0.50, 5, 0.0, 0, &n1_cfg);
        assert_eq!(neutral.regime, MarketRegime::NeutralRotation);
        assert_eq!(
            (neutral.long_weight_mult, neutral.short_weight_mult),
            (0.65, 1.0)
        );

        let chop = compute_regime_gate_state(0.40, 5, 0.0, 0, &n1_cfg);
        assert_eq!(chop.regime, MarketRegime::DefensiveChop);
        assert_eq!(
            (chop.long_weight_mult, chop.short_weight_mult),
            (0.50, 0.65)
        );

        let bear = compute_regime_gate_state(0.25, 5, 0.0, 0, &n1_cfg);
        assert_eq!(bear.regime, MarketRegime::BearContraction);
        assert_eq!((bear.long_weight_mult, bear.short_weight_mult), (0.0, 1.0));

        let shock = compute_regime_gate_state(0.20, 5, 0.0, 3, &n1_cfg);
        assert_eq!(shock.regime, MarketRegime::CapitulationShock);
        assert_eq!(
            (shock.long_weight_mult, shock.short_weight_mult),
            (0.0, 1.0)
        );

        // Gate disabled -> (1.0, 1.0)
        n1_cfg.enable_breadth_regime_gate = false;
        let off = compute_regime_gate_state(0.40, 5, 0.0, 0, &n1_cfg);
        assert_eq!((off.long_weight_mult, off.short_weight_mult), (1.0, 1.0));

        // 1h 24h return lookback scales from 6 bars to 24 bars
        let mut closes_1h = vec![100.0; 26];
        // Bars 0..20 flat at 100, bar 2 (index len-24 = 2) is 100.0, bars 21..25 dip then recover to 105.0
        for (i, c) in closes_1h.iter_mut().enumerate().skip(20) {
            *c = if i == 20 { 110.0 } else { 105.0 };
        }
        let bars_1h: Vec<OhlcvBar> = closes_1h
            .iter()
            .enumerate()
            .map(|(i, &c)| OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 100,
                timeframe: "1h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 3_600_000,
                open: c,
                high: c,
                low: c,
                close: c,
                volume: 10_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            })
            .collect();
        let a = mk_asset(100, "BULL24H", 105.0, 100.0, 0.0, 5_000_000.0, true);
        let mut map = HashMap::new();
        map.insert(100, bars_1h);
        // 24 bars back (index 2) is 100.0 -> 105.0 > 100.0 * 1.002 => bullish (1.0)!
        // (Old 6 bars back index 20 was 110.0, which would falsely mark it bearish 0.0!)
        let (b_1h, cnt) = compute_preliminary_bull_breadth(&[a], &map);
        assert_eq!(cnt, 1);
        assert!((b_1h - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_wave4_closed_bar_breadth_anchors_on_close_below_3_5pct_and_tracks_spike() {
        use crate::features::{BarCloseCache, CycleFeatureCache};

        // Closed bars end at 100.1 (+0.1% vs first=100.0 => flat 0.50 breadth).
        // Live mark at 103.0 (+2.897% vs close=100.1, < 3.5%) must stay anchored to 100.1 -> 0.50!
        let mut a_mild = mk_asset(201, "MILD", 103.0, 100.0, 0.0, 5_000_000.0, true);
        let mut map = HashMap::new();
        map.insert(201, mk_bars(201, &[100.0, 100.0, 100.0, 100.0, 100.1]));

        let (b_mild, cnt_mild) =
            compute_preliminary_bull_breadth(std::slice::from_ref(&a_mild), &map);
        assert_eq!(cnt_mild, 1);
        assert!(
            (b_mild - 0.50).abs() < 1e-9,
            "Intra-bar move < 3.5% must keep closed-bar breadth anchored at 0.50, got {b_mild}"
        );
        let mut bar_cache = BarCloseCache::new();
        let feat_mild = CycleFeatureCache::build_with_options(
            std::slice::from_ref(&a_mild),
            &map,
            &mut bar_cache,
            true,
        );
        assert!((feat_mild.prelim_breadth_bull - b_mild).abs() < 1e-9);

        // Live mark at 103.7 (+3.596% vs close=100.1, >= 3.5%) must switch to mark_price -> 1.0!
        a_mild.mark_price = 103.7;
        let (b_spike, _) = compute_preliminary_bull_breadth(std::slice::from_ref(&a_mild), &map);
        assert!(
            (b_spike - 1.0).abs() < 1e-9,
            "Intra-bar spike >= 3.5% must switch breadth anchor to live mark -> 1.0, got {b_spike}"
        );
        let feat_spike =
            CycleFeatureCache::build_with_options(&[a_mild], &map, &mut bar_cache, true);
        assert!((feat_spike.prelim_breadth_bull - b_spike).abs() < 1e-9);

        // Downward flash crash >= 3.5% (-3.6% below close=100.1 -> 96.4) must flip bearish -> 0.0!
        let a_crash = mk_asset(202, "CRASH", 96.4, 100.0, 0.0, 5_000_000.0, true);
        let mut map_crash = HashMap::new();
        map_crash.insert(202, mk_bars(202, &[100.0, 100.0, 100.0, 100.0, 100.1]));
        let (b_crash, _) =
            compute_preliminary_bull_breadth(std::slice::from_ref(&a_crash), &map_crash);
        assert!((b_crash - 0.0).abs() < 1e-9);
        let feat_crash =
            CycleFeatureCache::build_with_options(&[a_crash], &map_crash, &mut bar_cache, true);
        assert!((feat_crash.prelim_breadth_bull - b_crash).abs() < 1e-9);
    }
}
