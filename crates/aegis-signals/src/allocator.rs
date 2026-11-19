//! Multi-Sleeve Portfolio Weight Allocator & Cooldown Gate (`aegis-signals::allocator`).
//!
//! Combines `(w_s1 + w_s2 + w_s3) * risk_tier.default_leverage_multiplier()` (seeded at `Extreme 1.85x`),
//! blocks new entries for any symbol in active cooldown (`v_assets_in_cooldown`),
//! enforces single-asset and gross leverage caps, and classifies `rebalance_action`
//! (`OPEN_LONG`, `OPEN_SHORT`, `INCREASE`, `REDUCE`, `CLOSE`, `FLIP`, `HOLD`, `COOLDOWN_BLOCKED`).

use crate::carry_momentum::CarryMomentumMetrics;
use crate::event_reversion::EventReversionMetrics;
use crate::trend_breakout::TrendBreakoutMetrics;
use aegis_core::{round_dp, ConfigMap, CooldownAssetView, ExchangeAsset, RiskTier, SleeveSignal};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub const ACTION_OPEN_LONG: &str = "OPEN_LONG";
pub const ACTION_OPEN_SHORT: &str = "OPEN_SHORT";
pub const ACTION_INCREASE: &str = "INCREASE";
pub const ACTION_REDUCE: &str = "REDUCE";
pub const ACTION_CLOSE: &str = "CLOSE";
pub const ACTION_FLIP: &str = "FLIP";
pub const ACTION_HOLD: &str = "HOLD";
pub const ACTION_COOLDOWN_BLOCKED: &str = "COOLDOWN_BLOCKED";

/// Extension trait adding `default_leverage_multiplier()` directly on `RiskTier`.
pub trait RiskTierLeverageExt {
    fn default_leverage_multiplier(&self) -> f64;
}

impl RiskTierLeverageExt for RiskTier {
    #[inline]
    fn default_leverage_multiplier(&self) -> f64 {
        self.leverage_multiplier(None)
    }
}

/// Allocator configuration parameters loaded from `system_config` or defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocatorConfig {
    pub risk_tier: RiskTier,
    pub custom_leverage_override: Option<f64>,
    pub max_gross_leverage_x: f64,
    pub max_single_asset_weight: f64,
    pub min_rebalance_delta_weight: f64,
    pub s1_carry_mom_budget: f64,
    pub s2_trend_breakout_budget: f64,
    pub s3_event_reversion_budget: f64,
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self {
            risk_tier: RiskTier::Extreme,
            custom_leverage_override: None,
            max_gross_leverage_x: 2.50,
            max_single_asset_weight: 0.30,
            min_rebalance_delta_weight: 0.025,
            s1_carry_mom_budget: 0.45,
            s2_trend_breakout_budget: 0.40,
            s3_event_reversion_budget: 0.25,
        }
    }
}

impl AllocatorConfig {
    pub fn from_config_map(cfg: &ConfigMap) -> Self {
        let tier = cfg.risk_tier();
        let mult = cfg.target_leverage_multiplier();
        Self {
            risk_tier: tier,
            custom_leverage_override: (tier == RiskTier::Custom).then_some(mult),
            max_gross_leverage_x: cfg.get_f64("max_gross_leverage_x", 2.50).clamp(0.20, 5.00),
            max_single_asset_weight: cfg
                .get_f64("max_single_asset_weight", 0.30)
                .clamp(0.05, 1.00),
            min_rebalance_delta_weight: cfg
                .get_f64("min_rebalance_delta_weight", 0.025)
                .clamp(0.005, 0.25),
            s1_carry_mom_budget: cfg.get_f64("s1_carry_mom_budget", 0.45).clamp(0.0, 1.50),
            s2_trend_breakout_budget: cfg
                .get_f64("s2_trend_breakout_budget", 0.40)
                .clamp(0.0, 1.50),
            s3_event_reversion_budget: cfg
                .get_f64("s3_event_reversion_budget", 0.25)
                .clamp(0.0, 1.50),
        }
    }

    pub fn effective_leverage_multiplier(&self) -> f64 {
        match self.risk_tier {
            RiskTier::Custom => self
                .risk_tier
                .leverage_multiplier(self.custom_leverage_override),
            _ => self.risk_tier.default_leverage_multiplier(),
        }
    }
}

/// Complete multi-sleeve signal & allocation output for a single asset, ready for DB persistence
/// (`sleeve_signals`) and order execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SleeveSignalInput {
    pub asset_id: i64,
    pub symbol: String,
    pub coin: String,
    pub mark_price: f64,
    pub day_volume_usdc: f64,
    pub funding_rate_hourly: f64,
    pub annualized_vol: f64,
    pub market_beta: f64,
    pub s1_mom_score: f64,
    pub s1_carry_score: f64,
    pub s2_trend_alignment: i32,
    pub s2_vol_surge_ratio: f64,
    pub s3_funding_zscore: f64,
    pub s3_flush_atr_mult: f64,
    pub w_s1_carry_mom: f64,
    pub w_s2_trend_breakout: f64,
    pub w_s3_event_reversion: f64,
    pub raw_combined_weight: f64,
    pub target_weight: f64,
    pub current_weight: f64,
    pub delta_weight: f64,
    pub event_trigger: Option<String>,
    pub rebalance_action: String,
}

impl SleeveSignalInput {
    /// Converts this allocation result into an `aegis_core::SleeveSignal` domain record.
    pub fn to_sleeve_signal(&self, id: i64, ts_ms: i64) -> SleeveSignal {
        SleeveSignal {
            id,
            asset_id: self.asset_id,
            mark_price: self.mark_price,
            day_volume_usdc: self.day_volume_usdc,
            funding_rate_hourly: self.funding_rate_hourly,
            annualized_vol: self.annualized_vol,
            market_beta: self.market_beta,
            s1_mom_score: self.s1_mom_score,
            s1_carry_score: self.s1_carry_score,
            s2_trend_alignment: self.s2_trend_alignment,
            s2_vol_surge_ratio: self.s2_vol_surge_ratio,
            s3_funding_zscore: self.s3_funding_zscore,
            s3_flush_atr_mult: self.s3_flush_atr_mult,
            w_s1_carry_mom: self.w_s1_carry_mom,
            w_s2_trend_breakout: self.w_s2_trend_breakout,
            w_s3_event_reversion: self.w_s3_event_reversion,
            raw_combined_weight: self.raw_combined_weight,
            target_weight: self.target_weight,
            current_weight: self.current_weight,
            delta_weight: self.delta_weight,
            event_trigger: self.event_trigger.clone(),
            rebalance_action: self.rebalance_action.clone(),
            created_at_ms: ts_ms,
            updated_at_ms: ts_ms,
        }
    }
}

/// Classifies the rebalance action given `current_weight`, `target_weight`, `min_rebalance_delta_weight`,
/// and whether a new entry/increase was blocked by an active cooldown in `v_assets_in_cooldown`.
pub fn classify_rebalance_action(
    current_weight: f64,
    target_weight: f64,
    unblocked_desired_weight: f64,
    in_cooldown: bool,
    min_rebalance_delta: f64,
) -> String {
    let min_delta = min_rebalance_delta.max(1e-4);
    let is_flat = current_weight.abs() < 1e-4;

    if in_cooldown {
        if is_flat && unblocked_desired_weight.abs() >= min_delta {
            return ACTION_COOLDOWN_BLOCKED.to_string();
        }
        if !is_flat {
            // While in cooldown, never increase or flip an existing position; only allow closing/reducing/holding
            if target_weight.abs() < 1e-4 {
                return ACTION_CLOSE.to_string();
            }
            if (target_weight - current_weight).abs() >= min_delta
                && target_weight.abs() < current_weight.abs()
                && target_weight * current_weight >= 0.0
            {
                return ACTION_REDUCE.to_string();
            }
            if (unblocked_desired_weight - current_weight).abs() < min_delta
                && unblocked_desired_weight * current_weight >= 0.0
            {
                return ACTION_HOLD.to_string();
            }
            return ACTION_COOLDOWN_BLOCKED.to_string();
        }
    }

    let delta = target_weight - current_weight;
    if is_flat {
        if target_weight >= min_delta {
            ACTION_OPEN_LONG.to_string()
        } else if target_weight <= -min_delta {
            ACTION_OPEN_SHORT.to_string()
        } else {
            ACTION_HOLD.to_string()
        }
    } else if target_weight.abs() < 1e-4 {
        ACTION_CLOSE.to_string()
    } else if current_weight * target_weight < 0.0 {
        if target_weight.abs() >= min_delta * 1.5 && delta.abs() >= min_delta {
            ACTION_FLIP.to_string()
        } else {
            ACTION_CLOSE.to_string()
        }
    } else if delta.abs() < min_delta {
        ACTION_HOLD.to_string()
    } else if target_weight.abs() > current_weight.abs() {
        ACTION_INCREASE.to_string()
    } else {
        ACTION_REDUCE.to_string()
    }
}

/// Combines Sleeve 1, Sleeve 2, and Sleeve 3 weights, scales by `RiskTier` (`Extreme 1.85x` default),
/// blocks new entries on assets in `v_assets_in_cooldown`, and enforces single-asset and gross leverage caps.
pub fn allocate_multi_sleeve_weights(
    assets: &[ExchangeAsset],
    s1_map: &HashMap<i64, CarryMomentumMetrics>,
    s2_map: &HashMap<i64, TrendBreakoutMetrics>,
    s3_map: &HashMap<i64, EventReversionMetrics>,
    current_weights: &HashMap<i64, f64>,
    active_cooldowns: &[CooldownAssetView],
    cfg: &AllocatorConfig,
) -> Vec<SleeveSignalInput> {
    let lev_mult = cfg.effective_leverage_multiplier();
    let max_single = cfg.max_single_asset_weight.max(0.01);
    let max_gross = cfg.max_gross_leverage_x.max(0.10);

    let cooldown_ids: HashSet<i64> = active_cooldowns.iter().map(|c| c.asset_id).collect();
    let cooldown_symbols: HashSet<String> = active_cooldowns
        .iter()
        .map(|c| c.symbol.to_ascii_uppercase())
        .collect();

    struct DraftAlloc {
        asset: ExchangeAsset,
        annualized_vol: f64,
        market_beta: f64,
        s1_mom_score: f64,
        s1_carry_score: f64,
        s2_trend_alignment: i32,
        s2_vol_surge_ratio: f64,
        s3_funding_zscore: f64,
        s3_flush_atr_mult: f64,
        w_s1: f64,
        w_s2: f64,
        w_s3: f64,
        raw_combined_weight: f64,
        unblocked_scaled_weight: f64,
        capped_single_weight: f64,
        current_weight: f64,
        in_cooldown: bool,
        event_trigger: Option<String>,
    }

    let mut drafts = Vec::with_capacity(assets.len());

    for asset in assets {
        let s1 = s1_map.get(&asset.id);
        let s2 = s2_map.get(&asset.id);
        let s3 = s3_map.get(&asset.id);

        let annualized_vol = s1.map(|x| x.annualized_vol).unwrap_or(0.65);
        let market_beta = s1.map(|x| x.market_beta).unwrap_or(1.0);
        let s1_mom_score = s1.map(|x| x.s1_mom_score).unwrap_or(0.0);
        let s1_carry_score = s1.map(|x| x.s1_carry_score).unwrap_or(0.0);
        let w_s1 = s1.map(|x| x.w_s1_carry_mom).unwrap_or(0.0);

        let s2_trend_alignment = s2.map(|x| x.s2_trend_alignment).unwrap_or(0);
        let s2_vol_surge_ratio = s2.map(|x| x.s2_vol_surge_ratio).unwrap_or(1.0);
        let w_s2 = s2.map(|x| x.w_s2_trend_breakout).unwrap_or(0.0);

        let s3_funding_zscore = s3.map(|x| x.s3_funding_zscore).unwrap_or(0.0);
        let s3_flush_atr_mult = s3.map(|x| x.s3_flush_atr_mult).unwrap_or(0.0);
        let w_s3 = s3.map(|x| x.w_s3_event_reversion).unwrap_or(0.0);

        let is_downward_flush = (w_s3 > 0.0
            && s3_flush_atr_mult < 0.0
            && (s1_mom_score >= 0.0 || s2_trend_alignment >= 0))
            || (s3_flush_atr_mult <= -3.5 && s3_funding_zscore < 0.5);
        let w_s1 = if is_downward_flush {
            w_s1.max(0.0)
        } else {
            w_s1
        };
        let w_s2 = if is_downward_flush {
            w_s2.max(0.0)
        } else {
            w_s2
        };

        let event_trigger = s3
            .and_then(|x| x.event_trigger.clone())
            .or_else(|| s2.and_then(|x| x.event_trigger.clone()));

        let raw_combined_weight = round_dp(w_s1 + w_s2 + w_s3, 4);
        let unblocked_scaled_weight = round_dp(raw_combined_weight * lev_mult, 4);

        let cur_w = round_dp(current_weights.get(&asset.id).copied().unwrap_or(0.0), 4);
        let in_cooldown = cooldown_ids.contains(&asset.id)
            || cooldown_symbols.contains(&asset.symbol.to_ascii_uppercase());

        // Block new entries or position increases when the asset is in active cooldown
        let post_cooldown_weight = if in_cooldown {
            if cur_w.abs() < 1e-4 {
                0.0
            } else if unblocked_scaled_weight * cur_w > 0.0
                && unblocked_scaled_weight.abs() > cur_w.abs()
            {
                cur_w
            } else if unblocked_scaled_weight * cur_w < 0.0 {
                0.0
            } else {
                unblocked_scaled_weight
            }
        } else {
            unblocked_scaled_weight
        };

        let capped_single = post_cooldown_weight.clamp(-max_single, max_single);

        drafts.push(DraftAlloc {
            asset: asset.clone(),
            annualized_vol,
            market_beta,
            s1_mom_score,
            s1_carry_score,
            s2_trend_alignment,
            s2_vol_surge_ratio,
            s3_funding_zscore,
            s3_flush_atr_mult,
            w_s1,
            w_s2,
            w_s3,
            raw_combined_weight,
            unblocked_scaled_weight,
            capped_single_weight: capped_single,
            current_weight: cur_w,
            in_cooldown,
            event_trigger,
        });
    }

    if drafts.len() > 12 {
        let mut keep_ids: HashSet<i64> = HashSet::new();
        for d in &drafts {
            if d.current_weight.abs() >= 1e-4 {
                keep_ids.insert(d.asset.id);
            }
        }
        let mut ranked: Vec<(i64, f64, f64)> = drafts
            .iter()
            .map(|d| {
                (
                    d.asset.id,
                    d.capped_single_weight.abs(),
                    d.asset.day_volume_usdc,
                )
            })
            .collect();
        ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| b.2.total_cmp(&a.2)));
        for (id, weight_abs, _) in ranked {
            if keep_ids.len() >= 12 {
                break;
            }
            if weight_abs > 1e-6 {
                keep_ids.insert(id);
            }
        }
        for d in &mut drafts {
            if !keep_ids.contains(&d.asset.id) {
                d.capped_single_weight = 0.0;
                d.w_s1 = 0.0;
                d.w_s2 = 0.0;
                d.w_s3 = 0.0;
            }
        }
    }

    let total_gross_before_cap: f64 = drafts.iter().map(|d| d.capped_single_weight.abs()).sum();

    let gross_scale = if total_gross_before_cap > max_gross && total_gross_before_cap > 1e-8 {
        max_gross / total_gross_before_cap
    } else {
        1.0
    };

    let mut results = Vec::with_capacity(drafts.len());
    for d in drafts {
        let target_weight = round_dp(d.capped_single_weight * gross_scale, 4);
        let delta_weight = round_dp(target_weight - d.current_weight, 4);
        let eff_sleeve_scale =
            if target_weight.abs() <= 1e-8 || d.unblocked_scaled_weight.abs() <= 1e-8 {
                0.0
            } else if target_weight * d.unblocked_scaled_weight > 0.0 {
                (target_weight.abs() / d.unblocked_scaled_weight.abs()).clamp(0.0, 1.0)
            } else {
                0.0
            };
        let w_s1_scaled = round_dp(d.w_s1 * eff_sleeve_scale, 4);
        let w_s2_scaled = round_dp(d.w_s2 * eff_sleeve_scale, 4);
        let w_s3_scaled = round_dp(d.w_s3 * eff_sleeve_scale, 4);
        let rebalance_action = classify_rebalance_action(
            d.current_weight,
            target_weight,
            d.unblocked_scaled_weight,
            d.in_cooldown,
            cfg.min_rebalance_delta_weight,
        );

        results.push(SleeveSignalInput {
            asset_id: d.asset.id,
            symbol: d.asset.symbol,
            coin: d.asset.coin,
            mark_price: d.asset.mark_price,
            day_volume_usdc: d.asset.day_volume_usdc,
            funding_rate_hourly: d.asset.funding_rate_hourly,
            annualized_vol: d.annualized_vol,
            market_beta: d.market_beta,
            s1_mom_score: d.s1_mom_score,
            s1_carry_score: d.s1_carry_score,
            s2_trend_alignment: d.s2_trend_alignment,
            s2_vol_surge_ratio: d.s2_vol_surge_ratio,
            s3_funding_zscore: d.s3_funding_zscore,
            s3_flush_atr_mult: d.s3_flush_atr_mult,
            w_s1_carry_mom: w_s1_scaled,
            w_s2_trend_breakout: w_s2_scaled,
            w_s3_event_reversion: w_s3_scaled,
            raw_combined_weight: d.raw_combined_weight,
            target_weight,
            current_weight: d.current_weight,
            delta_weight,
            event_trigger: d.event_trigger,
            rebalance_action,
        });
    }

    results.sort_by(|a, b| {
        b.target_weight
            .abs()
            .total_cmp(&a.target_weight.abs())
            .then_with(|| b.day_volume_usdc.total_cmp(&a.day_volume_usdc))
    });

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_asset(id: i64, vol: f64) -> ExchangeAsset {
        ExchangeAsset {
            id,
            symbol: format!("A{id}-USDC"),
            coin: format!("A{id}"),
            asset_index: id as u32,
            sz_decimals: 2,
            tick_size: 0.01,
            lot_step_size: 0.01,
            max_leverage: 20.0,
            mark_price: 100.0,
            mid_price: 100.0,
            oracle_price: 100.0,
            funding_rate_hourly: 0.0,
            day_volume_usdc: vol,
            open_interest_usdc: vol * 0.4,
            prev_day_price: 100.0,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn test_flip_hysteresis_closes_small_opposite_targets() {
        let min_delta = 0.025;
        // Opposite sign below 1.5 * min_delta (0.0375) must CLOSE rather than FLIP
        assert_eq!(
            classify_rebalance_action(0.15, -0.03, -0.03, false, min_delta),
            ACTION_CLOSE
        );
        assert_eq!(
            classify_rebalance_action(-0.10, 0.02, 0.02, false, min_delta),
            ACTION_CLOSE
        );
        // Opposite sign >= 1.5 * min_delta flips cleanly
        assert_eq!(
            classify_rebalance_action(0.15, -0.05, -0.05, false, min_delta),
            ACTION_FLIP
        );
    }

    #[test]
    fn test_top_12_cap_removes_s1_s3_exemption_and_scales_sleeve_weights() {
        let cfg = AllocatorConfig {
            max_gross_leverage_x: 1.50,
            max_single_asset_weight: 0.30,
            ..AllocatorConfig::default()
        };
        let mut assets = Vec::new();
        let mut s1 = HashMap::new();
        let mut s2 = HashMap::new();
        let mut s3 = HashMap::new();

        // 20 flat assets ALL meeting old exemption (w_s1 > 0 && w_s3 > 0 && s3_flush_atr_mult < 0)
        // and exceeding gross leverage so gross_scale < 1.0 applies
        for i in 1..=20i64 {
            let a = test_asset(i, 10_000_000.0 - (i as f64) * 100_000.0);
            assets.push(a.clone());
            s1.insert(
                i,
                CarryMomentumMetrics {
                    asset_id: i,
                    symbol: a.symbol.clone(),
                    annualized_vol: 0.5,
                    market_beta: 1.0,
                    s1_mom_score: 0.4,
                    s1_carry_score: 0.1,
                    w_s1_carry_mom: 0.08,
                },
            );
            s2.insert(
                i,
                TrendBreakoutMetrics {
                    asset_id: i,
                    symbol: a.symbol.clone(),
                    s2_trend_alignment: 1,
                    s2_vol_surge_ratio: 1.2,
                    w_s2_trend_breakout: 0.04,
                    event_trigger: None,
                },
            );
            s3.insert(
                i,
                EventReversionMetrics {
                    asset_id: i,
                    symbol: a.symbol.clone(),
                    s3_funding_zscore: -0.5,
                    s3_flush_atr_mult: -1.5,
                    w_s3_event_reversion: 0.05,
                    event_trigger: None,
                },
            );
        }

        let signals =
            allocate_multi_sleeve_weights(&assets, &s1, &s2, &s3, &HashMap::new(), &[], &cfg);
        let non_zero: Vec<&SleeveSignalInput> = signals
            .iter()
            .filter(|s| s.target_weight.abs() > 1e-6)
            .collect();
        assert_eq!(non_zero.len(), 12, "Top-12 cap must reject S1+S3 exemption");

        let lev = cfg.effective_leverage_multiplier();
        for s in &signals {
            let sleeve_sum =
                (s.w_s1_carry_mom + s.w_s2_trend_breakout + s.w_s3_event_reversion) * lev;
            if s.target_weight.abs() <= 1e-6 {
                assert_eq!(s.w_s1_carry_mom, 0.0);
                assert_eq!(s.w_s2_trend_breakout, 0.0);
                assert_eq!(s.w_s3_event_reversion, 0.0);
            } else {
                assert!(
                    (sleeve_sum - s.target_weight).abs() < 2e-3,
                    "Scaled sleeve weights ({sleeve_sum}) must match target_weight ({})",
                    s.target_weight
                );
            }
        }
    }
}
