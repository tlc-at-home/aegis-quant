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
            // While in cooldown, never increase or flip an existing position; only allow closing/reducing
            if target_weight.abs() < 1e-4 {
                return ACTION_CLOSE.to_string();
            }
            if (target_weight - current_weight).abs() >= min_delta
                && target_weight.abs() < current_weight.abs()
                && target_weight * current_weight >= 0.0
            {
                return ACTION_REDUCE.to_string();
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
    } else if current_weight * target_weight < 0.0 && delta.abs() >= min_delta {
        ACTION_FLIP.to_string()
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
    let mut total_gross_before_cap = 0.0;

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
        total_gross_before_cap += capped_single.abs();

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

    let gross_scale = if total_gross_before_cap > max_gross && total_gross_before_cap > 1e-8 {
        max_gross / total_gross_before_cap
    } else {
        1.0
    };

    let mut results = Vec::with_capacity(drafts.len());
    for d in drafts {
        let target_weight = round_dp(d.capped_single_weight * gross_scale, 4);
        let delta_weight = round_dp(target_weight - d.current_weight, 4);
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
            w_s1_carry_mom: d.w_s1,
            w_s2_trend_breakout: d.w_s2,
            w_s3_event_reversion: d.w_s3,
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
            .partial_cmp(&a.target_weight.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                b.day_volume_usdc
                    .partial_cmp(&a.day_volume_usdc)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });

    results
}
