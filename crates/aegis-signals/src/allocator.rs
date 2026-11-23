//! Multi-Sleeve Portfolio Weight Allocator & Cooldown Gate (`aegis-signals::allocator`).
//!
//! Combines `(w_s1 + w_s2 + w_s3) * risk_tier.default_leverage_multiplier()` (seeded at `Extreme 1.85x`),
//! enforces breadth-based directional regime gating (`REGIME_BLOCKED`) and active cooldowns (`COOLDOWN_BLOCKED`),
//! applies asymmetric exit hysteresis for held winners vs underwater losers,
//! enforces single-asset (`0.15`) and gross leverage caps, and classifies `rebalance_action`.

use crate::breadth::{RegimeGateConfig, RegimeGateState};
use crate::carry_momentum::CarryMomentumMetrics;
use crate::event_reversion::EventReversionMetrics;
use crate::liquidity::{compute_one_tick_bps, MAX_ONE_TICK_BPS, MIN_FALLBACK_DAY_VOLUME_USDC};
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
pub const ACTION_REGIME_BLOCKED: &str = "REGIME_BLOCKED";

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

/// Asymmetric exit hysteresis configuration for held positions (`R14-L21`, `P1-Strat`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct HysteresisConfig {
    pub min_rebalance_delta_weight: f64,
    pub early_loser_invalidation_enabled: bool,
    pub s1_reversal_mom_threshold: f64,
    pub s1_support_composite_threshold: f64,
}

impl Default for HysteresisConfig {
    fn default() -> Self {
        Self {
            min_rebalance_delta_weight: 0.025,
            early_loser_invalidation_enabled: true,
            s1_reversal_mom_threshold: 0.08,
            s1_support_composite_threshold: 0.025,
        }
    }
}

/// Live position context passed into allocator evaluation for asymmetric loser/winner exit hysteresis.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
pub struct HeldPositionContext {
    pub current_weight: f64,
    /// Fractional unrealized PnL (`(mark - entry) / entry * side.sign()`, e.g. `-0.003` = `-0.3%`, `+0.005` = `+0.5%`).
    pub unrealized_pnl_pct: f64,
}

/// Parameter bundle for 2-arg [`apply_held_exit_hysteresis`] (`R14-L21`).
struct HeldHysteresisInput {
    cur_w: f64,
    unblocked_scaled_weight: f64,
    raw_combined_weight: f64,
    unrealized_pnl_pct: f64,
    is_eligible: bool,
    has_signal_eval: bool,
    is_downward_flush: bool,
    counter_regime_held: bool,
    s1_mom_score: f64,
    s1_carry_score: f64,
    s2_trend_alignment: i32,
    w_s3: f64,
    s3_funding_zscore: f64,
    s3_flush_atr_mult: f64,
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
    #[serde(default = "default_true")]
    pub enable_breadth_regime_gate: bool,
    #[serde(default = "default_bull_block_shorts")]
    pub breadth_bull_block_shorts_threshold: f64,
    #[serde(default = "default_bear_block_longs")]
    pub breadth_bear_block_longs_threshold: f64,
    #[serde(default)]
    pub hysteresis: HysteresisConfig,
}

fn default_true() -> bool {
    true
}

fn default_bull_block_shorts() -> f64 {
    0.68
}

fn default_bear_block_longs() -> f64 {
    0.32
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self {
            risk_tier: RiskTier::Extreme,
            custom_leverage_override: None,
            max_gross_leverage_x: 2.50,
            max_single_asset_weight: 0.15,
            min_rebalance_delta_weight: 0.025,
            s1_carry_mom_budget: 0.45,
            s2_trend_breakout_budget: 0.40,
            s3_event_reversion_budget: 0.10,
            enable_breadth_regime_gate: true,
            breadth_bull_block_shorts_threshold: 0.68,
            breadth_bear_block_longs_threshold: 0.32,
            hysteresis: HysteresisConfig::default(),
        }
    }
}

impl AllocatorConfig {
    pub fn from_config_map(cfg: &ConfigMap) -> Self {
        let tier = cfg.risk_tier();
        let mult = cfg.target_leverage_multiplier();
        let min_rebalance_delta_weight = cfg
            .get_f64("min_rebalance_delta_weight", 0.025)
            .clamp(0.005, 0.25);
        Self {
            risk_tier: tier,
            custom_leverage_override: (tier == RiskTier::Custom).then_some(mult),
            max_gross_leverage_x: cfg.get_f64("max_gross_leverage_x", 2.50).clamp(0.20, 5.00),
            max_single_asset_weight: cfg
                .get_f64("max_single_asset_weight", 0.15)
                .clamp(0.01, 1.00),
            min_rebalance_delta_weight,
            s1_carry_mom_budget: cfg.get_f64("s1_carry_mom_budget", 0.45).clamp(0.0, 1.50),
            s2_trend_breakout_budget: cfg
                .get_f64("s2_trend_breakout_budget", 0.40)
                .clamp(0.0, 1.50),
            s3_event_reversion_budget: cfg
                .get_f64("s3_event_reversion_budget", 0.10)
                .clamp(0.0, 1.50),
            enable_breadth_regime_gate: cfg.get_bool("enable_breadth_regime_gate", true),
            breadth_bull_block_shorts_threshold: cfg
                .get_f64("breadth_bull_block_shorts_threshold", 0.68)
                .clamp(0.55, 0.95),
            breadth_bear_block_longs_threshold: cfg
                .get_f64("breadth_bear_block_longs_threshold", 0.32)
                .clamp(0.05, 0.45),
            hysteresis: HysteresisConfig {
                min_rebalance_delta_weight,
                early_loser_invalidation_enabled: cfg
                    .get_bool("early_loser_invalidation_enabled", true),
                s1_reversal_mom_threshold: cfg
                    .get_f64("hysteresis_s1_reversal_mom_threshold", 0.08)
                    .clamp(0.01, 0.50),
                s1_support_composite_threshold: cfg
                    .get_f64("hysteresis_s1_support_composite_threshold", 0.025)
                    .clamp(0.005, 0.25),
            },
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

    pub fn regime_gate_config(&self) -> RegimeGateConfig {
        RegimeGateConfig {
            enable_breadth_regime_gate: self.enable_breadth_regime_gate,
            bull_block_shorts_threshold: self.breadth_bull_block_shorts_threshold,
            bear_block_longs_threshold: self.breadth_bear_block_longs_threshold,
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
    classify_rebalance_action_with_gates(
        current_weight,
        target_weight,
        unblocked_desired_weight,
        in_cooldown,
        false,
        min_rebalance_delta,
    )
}

/// Classifies rebalance action accounting for both cooldown (`R14-L24`) and breadth regime gates (`P1-Strat`).
pub fn classify_rebalance_action_with_gates(
    current_weight: f64,
    target_weight: f64,
    unblocked_desired_weight: f64,
    in_cooldown: bool,
    regime_blocked: bool,
    min_rebalance_delta: f64,
) -> String {
    let min_delta = min_rebalance_delta.max(1e-4);
    let is_flat = current_weight.abs() < 1e-4;

    if in_cooldown {
        // R14-L24: EVERY flat asset in active cooldown is classified as COOLDOWN_BLOCKED.
        if is_flat {
            return ACTION_COOLDOWN_BLOCKED.to_string();
        }
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

    if regime_blocked
        && ((is_flat && target_weight.abs() < 1e-4)
            || (!is_flat && (target_weight - current_weight).abs() < min_delta))
    {
        return ACTION_REGIME_BLOCKED.to_string();
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

/// Applies asymmetric exit hysteresis for currently held positions (`cur_w.abs() >= 1e-4`):
/// - Underwater losers (`unrealized_pnl_pct <= -0.003` when `early_loser_invalidation_enabled=true`)
///   bypass hold hysteresis immediately upon momentum/trend/regime invalidation.
/// - Winners (`unrealized_pnl_pct >= +0.005`) require stronger reversal (`s2` opposing, `s1 +-0.12`, or opposing `w_s3`)
///   to signal-close, letting stepped Chandelier/TP harvest right-tail runners.
///
/// Returns `(effective_desired_weight, hysteresis_held)`.
fn apply_held_exit_hysteresis(input: &HeldHysteresisInput, cfg: &HysteresisConfig) -> (f64, bool) {
    if input.cur_w.abs() < 1e-4 || !input.is_eligible || !input.has_signal_eval {
        return (input.unblocked_scaled_weight, false);
    }

    let min_delta = cfg.min_rebalance_delta_weight.max(1e-4);
    let min_hold_band = (min_delta * 0.5).max(1e-3);

    // Already meaningfully aligned on the held side -> no risk of spurious 0.0 close
    if input.unblocked_scaled_weight * input.cur_w > 0.0
        && input.unblocked_scaled_weight.abs() >= min_hold_band
    {
        return (input.unblocked_scaled_weight, false);
    }

    let s1_composite = 0.65 * input.s1_mom_score + 0.35 * input.s1_carry_score;
    let is_loser = cfg.early_loser_invalidation_enabled
        && input.unrealized_pnl_pct.is_finite()
        && input.unrealized_pnl_pct <= -0.003;
    let is_winner = cfg.early_loser_invalidation_enabled
        && input.unrealized_pnl_pct.is_finite()
        && input.unrealized_pnl_pct >= 0.005;

    let rev_thresh = if cfg.s1_reversal_mom_threshold.is_finite() {
        cfg.s1_reversal_mom_threshold.clamp(0.01, 0.50)
    } else {
        0.08
    };
    let sup_thresh = if cfg.s1_support_composite_threshold.is_finite() {
        cfg.s1_support_composite_threshold.clamp(0.005, 0.25)
    } else {
        0.025
    };
    let loser_mom_thresh = (rev_thresh * 0.5).clamp(0.01, 0.04);
    let winner_rev_thresh = (rev_thresh * 1.5).max(0.12);

    if input.cur_w > 0.0 {
        if is_loser {
            let invalidated = (input.s2_trend_alignment <= 0 && input.s1_mom_score < 0.0)
                || input.s2_trend_alignment < 0
                || input.s1_mom_score <= -loser_mom_thresh
                || input.counter_regime_held;
            if invalidated {
                let exit_w = if input.unblocked_scaled_weight > 0.0 {
                    0.0
                } else {
                    input.unblocked_scaled_weight
                };
                return (exit_w, false);
            }
        }

        if is_winner {
            let strong_reversal = input.unblocked_scaled_weight <= -min_hold_band
                || input.w_s3 < 0.0
                || input.s2_trend_alignment < 0
                || input.s1_mom_score <= -winner_rev_thresh;
            return if strong_reversal {
                (input.unblocked_scaled_weight, false)
            } else {
                (input.cur_w, true)
            };
        }

        // Neutral band: clear bearish reversal / exit -> never block CLOSE or FLIP
        if input.unblocked_scaled_weight <= -min_hold_band
            || input.w_s3 < 0.0
            || input.s2_trend_alignment < 0
            || input.s1_mom_score <= -rev_thresh
        {
            return (input.unblocked_scaled_weight, false);
        }

        let has_mild_long_support = input.raw_combined_weight > 0.0
            || (input.s1_mom_score > 0.01 && s1_composite > 0.0)
            || s1_composite > sup_thresh
            || input.s2_trend_alignment > 0
            || input.s3_flush_atr_mult <= -0.5
            || input.s3_funding_zscore <= -0.8;

        if has_mild_long_support {
            (input.cur_w, true)
        } else {
            (input.unblocked_scaled_weight, false)
        }
    } else {
        if is_loser {
            let invalidated = (input.s2_trend_alignment >= 0 && input.s1_mom_score > 0.0)
                || input.s2_trend_alignment > 0
                || input.s1_mom_score >= loser_mom_thresh
                || input.counter_regime_held;
            if invalidated {
                let exit_w = if input.unblocked_scaled_weight < 0.0 {
                    0.0
                } else {
                    input.unblocked_scaled_weight
                };
                return (exit_w, false);
            }
        }

        if is_winner {
            let strong_reversal = input.is_downward_flush
                || input.w_s3 > 0.0
                || input.unblocked_scaled_weight >= min_hold_band
                || input.s2_trend_alignment > 0
                || input.s1_mom_score >= winner_rev_thresh;
            return if strong_reversal {
                (input.unblocked_scaled_weight, false)
            } else {
                (input.cur_w, true)
            };
        }

        // Held SHORT neutral band: flush conflict or clear bullish reversal / exit -> allow CLOSE or FLIP
        if input.is_downward_flush
            || input.w_s3 > 0.0
            || input.unblocked_scaled_weight >= min_hold_band
            || input.s2_trend_alignment > 0
            || input.s1_mom_score >= rev_thresh
        {
            return (input.unblocked_scaled_weight, false);
        }

        let has_mild_short_support = input.raw_combined_weight < 0.0
            || (input.s1_mom_score < -0.01 && s1_composite < 0.0)
            || s1_composite < -sup_thresh
            || input.s2_trend_alignment < 0
            || input.s3_flush_atr_mult >= 0.5
            || input.s3_funding_zscore >= 0.8
            || (input.s3_flush_atr_mult > -2.0
                && input.s3_flush_atr_mult < -0.1
                && input.s1_mom_score <= 0.0
                && input.s2_trend_alignment <= 0);

        if has_mild_short_support {
            (input.cur_w, true)
        } else {
            (input.unblocked_scaled_weight, false)
        }
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
    let held_contexts: HashMap<i64, HeldPositionContext> = current_weights
        .iter()
        .map(|(&id, &w)| {
            (
                id,
                HeldPositionContext {
                    current_weight: w,
                    unrealized_pnl_pct: 0.0,
                },
            )
        })
        .collect();
    allocate_multi_sleeve_weights_with_context(
        assets,
        s1_map,
        s2_map,
        s3_map,
        &held_contexts,
        active_cooldowns,
        &RegimeGateState::default(),
        cfg,
    )
}

/// Full multi-sleeve allocator enforcing [`HeldPositionContext`] asymmetric hysteresis,
/// [`RegimeGateState`] defense-in-depth gating (`ACTION_REGIME_BLOCKED`), cooldowns, and caps.
#[allow(clippy::too_many_arguments)]
pub fn allocate_multi_sleeve_weights_with_context(
    assets: &[ExchangeAsset],
    s1_map: &HashMap<i64, CarryMomentumMetrics>,
    s2_map: &HashMap<i64, TrendBreakoutMetrics>,
    s3_map: &HashMap<i64, EventReversionMetrics>,
    held_contexts: &HashMap<i64, HeldPositionContext>,
    active_cooldowns: &[CooldownAssetView],
    regime_gate: &RegimeGateState,
    cfg: &AllocatorConfig,
) -> Vec<SleeveSignalInput> {
    let lev_mult = cfg.effective_leverage_multiplier();
    let max_single = cfg.max_single_asset_weight.max(0.01);
    let max_gross = cfg.max_gross_leverage_x.max(0.10);
    let min_delta = cfg.min_rebalance_delta_weight.max(1e-4);
    let mut hyst_cfg = cfg.hysteresis;
    hyst_cfg.min_rebalance_delta_weight = cfg.min_rebalance_delta_weight;

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
        effective_desired_weight: f64,
        hysteresis_held: bool,
        capped_single_weight: f64,
        current_weight: f64,
        in_cooldown: bool,
        regime_blocked: bool,
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
        let raw_w_s1 = s1.map(|x| x.w_s1_carry_mom).unwrap_or(0.0);

        let s2_trend_alignment = s2.map(|x| x.s2_trend_alignment).unwrap_or(0);
        let s2_vol_surge_ratio = s2.map(|x| x.s2_vol_surge_ratio).unwrap_or(1.0);
        let raw_w_s2 = s2.map(|x| x.w_s2_trend_breakout).unwrap_or(0.0);

        let s3_funding_zscore = s3.map(|x| x.s3_funding_zscore).unwrap_or(0.0);
        let s3_flush_atr_mult = s3.map(|x| x.s3_flush_atr_mult).unwrap_or(0.0);
        let raw_w_s3 = s3.map(|x| x.w_s3_event_reversion).unwrap_or(0.0);

        let asset_eligible = asset.is_core_eligible || asset.is_event_eligible;
        let sleeve_quality_ok = s1.is_none_or(|x| x.passes_quality)
            && s2.is_none_or(|x| x.passes_quality)
            && s3.is_none_or(|x| x.passes_quality);
        let tick_ok = asset.mark_price.is_finite()
            && asset.mark_price > 0.0
            && compute_one_tick_bps(asset, asset.mark_price)
                .is_some_and(|bps| bps <= MAX_ONE_TICK_BPS);
        let quality_ok = sleeve_quality_ok && tick_ok;

        let w_s1 = if quality_ok { raw_w_s1 } else { 0.0 };
        let w_s2 = if quality_ok { raw_w_s2 } else { 0.0 };
        let w_s3 = if quality_ok { raw_w_s3 } else { 0.0 };

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

        let event_trigger = if quality_ok {
            s3.and_then(|x| x.event_trigger.clone())
                .or_else(|| s2.and_then(|x| x.event_trigger.clone()))
        } else {
            None
        };

        let raw_combined_weight = round_dp(w_s1 + w_s2 + w_s3, 4);
        let unblocked_scaled_weight = round_dp(raw_combined_weight * lev_mult, 4);

        let ctx = held_contexts.get(&asset.id).copied().unwrap_or_default();
        let cur_w = round_dp(ctx.current_weight, 4);
        let in_cooldown = cooldown_ids.contains(&asset.id)
            || cooldown_symbols.contains(&asset.symbol.to_ascii_uppercase());

        // Never allow hysteresis to trap a held dead/illiquid coin forever when eligibility drops,
        // sleeve quality fails, tick size exceeds cap, or a ghost-volume book reports zero sleeve alpha.
        let all_sleeves_zero = w_s1.abs() <= 1e-6 && w_s2.abs() <= 1e-6 && w_s3.abs() <= 1e-6;
        let ghost_zero_alpha = all_sleeves_zero
            && (!asset.day_volume_usdc.is_finite()
                || asset.day_volume_usdc < MIN_FALLBACK_DAY_VOLUME_USDC);
        let hysteresis_eligible = asset_eligible && quality_ok && !ghost_zero_alpha;

        let counter_regime_held = (cur_w > 0.0 && regime_gate.block_new_longs)
            || (cur_w < 0.0 && regime_gate.block_new_shorts);

        let hyst_input = HeldHysteresisInput {
            cur_w,
            unblocked_scaled_weight,
            raw_combined_weight,
            unrealized_pnl_pct: ctx.unrealized_pnl_pct,
            is_eligible: hysteresis_eligible,
            has_signal_eval: s1.is_some() || s2.is_some() || s3.is_some(),
            is_downward_flush,
            counter_regime_held,
            s1_mom_score,
            s1_carry_score,
            s2_trend_alignment,
            w_s3,
            s3_funding_zscore,
            s3_flush_atr_mult,
        };
        let (effective_desired_weight, hysteresis_held) =
            apply_held_exit_hysteresis(&hyst_input, &hyst_cfg);

        // Defense-in-depth Breadth Regime Gate (`P1-Strat`): NEVER blocks CLOSE or REDUCE!
        let mut regime_blocked = false;
        let post_regime_weight = if regime_gate.block_new_shorts {
            if cur_w.abs() < 1e-4 && effective_desired_weight < 0.0 {
                if effective_desired_weight.abs() >= min_delta {
                    regime_blocked = true;
                }
                0.0
            } else if cur_w > 0.0 && effective_desired_weight < 0.0 {
                // Held LONG flipping into blocked SHORT -> CLOSE instead of FLIP
                0.0
            } else if cur_w < 0.0 && effective_desired_weight < cur_w {
                if (effective_desired_weight - cur_w).abs() >= min_delta {
                    regime_blocked = true;
                }
                cur_w
            } else {
                effective_desired_weight
            }
        } else if regime_gate.block_new_longs {
            if cur_w.abs() < 1e-4 && effective_desired_weight > 0.0 {
                if effective_desired_weight.abs() >= min_delta {
                    regime_blocked = true;
                }
                0.0
            } else if cur_w < 0.0 && effective_desired_weight > 0.0 {
                // Held SHORT flipping into blocked LONG -> CLOSE instead of FLIP
                0.0
            } else if cur_w > 0.0 && effective_desired_weight > cur_w {
                if (effective_desired_weight - cur_w).abs() >= min_delta {
                    regime_blocked = true;
                }
                cur_w
            } else {
                effective_desired_weight
            }
        } else {
            effective_desired_weight
        };

        // Block new entries or position increases when the asset is in active cooldown
        let post_cooldown_weight = if in_cooldown {
            if cur_w.abs() < 1e-4 {
                0.0
            } else if post_regime_weight * cur_w > 0.0 && post_regime_weight.abs() > cur_w.abs() {
                cur_w
            } else if post_regime_weight * cur_w < 0.0 {
                0.0
            } else {
                post_regime_weight
            }
        } else {
            post_regime_weight
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
            effective_desired_weight,
            hysteresis_held,
            capped_single_weight: capped_single,
            current_weight: cur_w,
            in_cooldown,
            regime_blocked,
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
        let mut target_weight = round_dp(d.capped_single_weight * gross_scale, 4);
        if d.hysteresis_held && target_weight.abs() < 1e-4 {
            target_weight = d.current_weight.signum() * 1e-4;
        }
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
        let rebalance_action = classify_rebalance_action_with_gates(
            d.current_weight,
            target_weight,
            d.effective_desired_weight,
            d.in_cooldown,
            d.regime_blocked,
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
                    passes_quality: true,
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
                    passes_quality: true,
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
                    passes_quality: true,
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

    #[allow(clippy::too_many_arguments)]
    fn run_single_alloc(
        asset: ExchangeAsset,
        cur_w: f64,
        cooldowns: &[CooldownAssetView],
        s1_mom: f64,
        s1_carry: f64,
        w_s1: f64,
        s2_align: i32,
        w_s2: f64,
        s3_fund_z: f64,
        s3_flush_atr: f64,
        w_s3: f64,
    ) -> SleeveSignalInput {
        let cfg = AllocatorConfig::default();
        let mut s1 = HashMap::new();
        s1.insert(
            asset.id,
            CarryMomentumMetrics {
                asset_id: asset.id,
                symbol: asset.symbol.clone(),
                annualized_vol: 0.50,
                market_beta: 1.0,
                s1_mom_score: s1_mom,
                s1_carry_score: s1_carry,
                w_s1_carry_mom: w_s1,
                passes_quality: true,
            },
        );
        let mut s2 = HashMap::new();
        s2.insert(
            asset.id,
            TrendBreakoutMetrics {
                asset_id: asset.id,
                symbol: asset.symbol.clone(),
                s2_trend_alignment: s2_align,
                s2_vol_surge_ratio: 1.1,
                w_s2_trend_breakout: w_s2,
                event_trigger: None,
                passes_quality: true,
            },
        );
        let mut s3 = HashMap::new();
        s3.insert(
            asset.id,
            EventReversionMetrics {
                asset_id: asset.id,
                symbol: asset.symbol.clone(),
                s3_funding_zscore: s3_fund_z,
                s3_flush_atr_mult: s3_flush_atr,
                w_s3_event_reversion: w_s3,
                event_trigger: None,
                passes_quality: true,
            },
        );
        let mut cur = HashMap::new();
        if cur_w.abs() > 0.0 {
            cur.insert(asset.id, cur_w);
        }
        allocate_multi_sleeve_weights(&[asset], &s1, &s2, &s3, &cur, cooldowns, &cfg)
            .into_iter()
            .next()
            .unwrap()
    }

    #[test]
    fn test_held_long_hysteresis_blocks_noise_close_but_preserves_exits_and_flips() {
        let a = test_asset(1, 10_000_000.0);

        // 1. Sub-threshold S1 momentum flicker (w_s1 snapped to 0.0) -> HOLD instead of CLOSE
        let hold_s1 =
            run_single_alloc(a.clone(), 0.12, &[], 0.03, 0.05, 0.0, 0, 0.0, 0.0, 0.0, 0.0);
        assert_eq!(hold_s1.rebalance_action, ACTION_HOLD);
        assert!((hold_s1.target_weight - 0.12).abs() < 1e-6);

        // 2. Event-only S2 breakout trigger lost (w_s2 = 0.0) while s2_trend_alignment == +1 -> HOLD
        let mut ev_only = a.clone();
        ev_only.is_core_eligible = false;
        let hold_s2 = run_single_alloc(ev_only, 0.10, &[], 0.0, 0.0, 0.0, 1, 0.0, 0.0, 0.0, 0.0);
        assert_eq!(hold_s2.rebalance_action, ACTION_HOLD);
        assert!((hold_s2.target_weight - 0.10).abs() < 1e-6);

        // 3. S3 flush recovery flicker (w_s3 snapped to 0.0, flush still -1.2 ATR) -> HOLD
        let hold_s3 = run_single_alloc(a.clone(), 0.12, &[], 0.0, 0.0, 0.0, 0, 0.0, 0.0, -1.2, 0.0);
        assert_eq!(hold_s3.rebalance_action, ACTION_HOLD);

        // 4. Completely neutral signal (including baseline 0.0-funding carry 0.0582) -> CLOSE
        let close_flat = run_single_alloc(
            a.clone(),
            0.12,
            &[],
            0.0,
            0.0582,
            0.0,
            0,
            0.0,
            0.0,
            0.0,
            0.0,
        );
        assert_eq!(close_flat.rebalance_action, ACTION_CLOSE);
        assert_eq!(close_flat.target_weight, 0.0);

        // 5. Clear bearish shift -> CLOSE (small negative) or FLIP (strong negative)
        let close_bear = run_single_alloc(
            a.clone(),
            0.12,
            &[],
            -0.12,
            0.0,
            -0.015,
            -1,
            0.0,
            0.0,
            0.0,
            0.0,
        );
        assert_eq!(close_bear.rebalance_action, ACTION_CLOSE);

        let flip_bear = run_single_alloc(
            a.clone(),
            0.12,
            &[],
            -0.40,
            -0.20,
            -0.06,
            -1,
            -0.04,
            0.0,
            0.0,
            0.0,
        );
        assert_eq!(flip_bear.rebalance_action, ACTION_FLIP);
        assert!(flip_bear.target_weight < 0.0);

        // 6. Ineligible asset must CLOSE even if metrics look positive
        let mut dead = a;
        dead.is_core_eligible = false;
        dead.is_event_eligible = false;
        let close_inelig = run_single_alloc(dead, 0.12, &[], 0.5, 0.2, 0.0, 1, 0.0, 0.0, 0.0, 0.0);
        assert_eq!(close_inelig.rebalance_action, ACTION_CLOSE);
    }

    #[test]
    fn test_held_short_hysteresis_and_downward_flush_conflict_and_cooldown() {
        let a = test_asset(2, 10_000_000.0);

        // 1. Short paused at range bottom (s1=0, s2=0, w_s1=0, w_s2=0, w_s3=0, -2.0 < flush < -0.1) -> HOLD
        let hold_rb = run_single_alloc(
            a.clone(),
            -0.12,
            &[],
            0.0,
            0.0,
            0.0,
            0,
            0.0,
            0.0,
            -1.25,
            0.0,
        );
        assert_eq!(hold_rb.rebalance_action, ACTION_HOLD);
        assert!((hold_rb.target_weight + 0.12).abs() < 1e-6);

        // 2. Downward flush conflict (w_s3 > 0 at negative flush) must CLOSE (or FLIP if strong)
        let close_flush = run_single_alloc(
            a.clone(),
            -0.12,
            &[],
            0.0,
            0.0,
            0.0,
            0,
            0.0,
            -1.2,
            -2.1,
            0.015,
        );
        assert_eq!(close_flush.rebalance_action, ACTION_CLOSE);

        let flip_flush = run_single_alloc(
            a.clone(),
            -0.12,
            &[],
            0.0,
            0.0,
            0.0,
            0,
            0.0,
            -2.0,
            -2.6,
            0.05,
        );
        assert_eq!(flip_flush.rebalance_action, ACTION_FLIP);
        assert!(flip_flush.target_weight > 0.0);

        // 3. Extreme crash flush conflict (s3_flush_atr_mult <= -3.5) must CLOSE held short
        let close_extreme = run_single_alloc(
            a.clone(),
            -0.12,
            &[],
            -0.3,
            0.0,
            -0.05,
            -1,
            -0.04,
            0.0,
            -3.6,
            0.0,
        );
        assert_eq!(close_extreme.rebalance_action, ACTION_CLOSE);
        assert_eq!(close_extreme.target_weight, 0.0);

        // 4. Cooldown invariance on held short:
        let cd = CooldownAssetView {
            cooldown_id: 1,
            asset_id: a.id,
            symbol: a.symbol.clone(),
            coin: a.coin.clone(),
            cooldown_type: "STOP_OUT".to_string(),
            reason: "test".to_string(),
            triggered_at_ms: 1_700_000_000_000,
            expires_at_ms: 1_700_001_800_000,
            remaining_seconds: 1800,
            updated_at_ms: 1_700_000_000_000,
        };
        // Mild noise holds cleanly under cooldown
        let cd_hold = run_single_alloc(
            a.clone(),
            -0.12,
            std::slice::from_ref(&cd),
            0.0,
            0.0,
            0.0,
            0,
            0.0,
            0.0,
            -1.25,
            0.0,
        );
        assert_eq!(cd_hold.rebalance_action, ACTION_HOLD);
        // Attempt to increase short under cooldown stays ACTION_COOLDOWN_BLOCKED
        let cd_inc = run_single_alloc(
            a.clone(),
            -0.12,
            std::slice::from_ref(&cd),
            -0.5,
            -0.2,
            -0.08,
            -1,
            -0.05,
            0.0,
            0.0,
            0.0,
        );
        assert_eq!(cd_inc.rebalance_action, ACTION_COOLDOWN_BLOCKED);
        // Downward flush under cooldown closes instead of flipping
        let cd_close = run_single_alloc(a, -0.12, &[cd], 0.0, 0.0, 0.0, 0, 0.0, -2.0, -2.6, 0.05);
        assert_eq!(cd_close.rebalance_action, ACTION_CLOSE);
        assert_eq!(cd_close.target_weight, 0.0);
    }

    #[test]
    fn test_hysteresis_does_not_trap_held_dead_or_illiquid_coins() {
        let cfg = AllocatorConfig::default();

        // 1. Held LONG still marked core-eligible in DB, but failed bar-liveness quality across sleeves
        //    even with stale supportive momentum/trend/flush scores -> must CLOSE (never trapped!)
        let dead_live_flag = test_asset(10, 8_000.0);
        let mut s1 = HashMap::new();
        s1.insert(
            10,
            CarryMomentumMetrics {
                asset_id: 10,
                symbol: dead_live_flag.symbol.clone(),
                annualized_vol: 0.25,
                market_beta: 1.0,
                s1_mom_score: 0.45,
                s1_carry_score: 0.20,
                w_s1_carry_mom: 0.0,
                passes_quality: false,
            },
        );
        let mut s2 = HashMap::new();
        s2.insert(
            10,
            TrendBreakoutMetrics {
                asset_id: 10,
                symbol: dead_live_flag.symbol.clone(),
                s2_trend_alignment: 1,
                s2_vol_surge_ratio: 1.0,
                w_s2_trend_breakout: 0.0,
                event_trigger: None,
                passes_quality: false,
            },
        );
        let mut s3 = HashMap::new();
        s3.insert(
            10,
            EventReversionMetrics {
                asset_id: 10,
                symbol: dead_live_flag.symbol.clone(),
                s3_funding_zscore: -1.5,
                s3_flush_atr_mult: -1.2,
                w_s3_event_reversion: 0.0,
                event_trigger: None,
                passes_quality: false,
            },
        );
        let mut cur_long = HashMap::new();
        cur_long.insert(10, 0.12);
        let out_long = allocate_multi_sleeve_weights(
            std::slice::from_ref(&dead_live_flag),
            &s1,
            &s2,
            &s3,
            &cur_long,
            &[],
            &cfg,
        );
        assert_eq!(out_long[0].rebalance_action, ACTION_CLOSE);
        assert_eq!(out_long[0].target_weight, 0.0);

        // Same for held SHORT on a dead quality-failing book
        let mut cur_short = HashMap::new();
        cur_short.insert(10, -0.12);
        let out_short =
            allocate_multi_sleeve_weights(&[dead_live_flag], &s1, &s2, &s3, &cur_short, &[], &cfg);
        assert_eq!(out_short[0].rebalance_action, ACTION_CLOSE);
        assert_eq!(out_short[0].target_weight, 0.0);

        // 2. Held coin with coarse tick (> 15 bps) -> must CLOSE
        let mut coarse = test_asset(11, 500_000.0);
        coarse.mark_price = 0.003;
        coarse.tick_size = 0.00001; // 33.3 bps
        let close_coarse =
            run_single_alloc(coarse, 0.10, &[], 0.30, 0.10, 0.0, 1, 0.0, 0.0, 0.0, 0.0);
        assert_eq!(close_coarse.rebalance_action, ACTION_CLOSE);
        assert_eq!(close_coarse.target_weight, 0.0);

        // 3. Ghost volume (< $2,500) with zero sleeve weights -> must CLOSE
        let ghost = test_asset(12, 79.0);
        let close_ghost =
            run_single_alloc(ghost, 0.10, &[], 0.03, 0.05, 0.0, 0, 0.0, 0.0, -1.2, 0.0);
        assert_eq!(close_ghost.rebalance_action, ACTION_CLOSE);
        assert_eq!(close_ghost.target_weight, 0.0);
    }

    #[test]
    fn test_r14_l24_flat_asset_in_cooldown_always_cooldown_blocked() {
        let min_delta = 0.025;
        // Flat asset in cooldown MUST always return ACTION_COOLDOWN_BLOCKED regardless of desired weight
        assert_eq!(
            classify_rebalance_action(0.0, 0.0, 0.15, true, min_delta),
            ACTION_COOLDOWN_BLOCKED
        );
        assert_eq!(
            classify_rebalance_action(0.0, 0.0, 0.0, true, min_delta),
            ACTION_COOLDOWN_BLOCKED
        );
        assert_eq!(
            classify_rebalance_action(0.0, 0.0, 0.005, true, min_delta),
            ACTION_COOLDOWN_BLOCKED
        );
        assert_eq!(
            classify_rebalance_action(0.0, 0.0, -0.01, true, min_delta),
            ACTION_COOLDOWN_BLOCKED
        );

        let a = test_asset(42, 10_000_000.0);
        let cd = CooldownAssetView {
            cooldown_id: 99,
            asset_id: a.id,
            symbol: a.symbol.clone(),
            coin: a.coin.clone(),
            cooldown_type: "STOP_OUT".to_string(),
            reason: "stop".to_string(),
            triggered_at_ms: 1_700_000_000_000,
            expires_at_ms: 1_700_001_800_000,
            remaining_seconds: 1800,
            updated_at_ms: 1_700_000_000_000,
        };
        let flat_zero_alpha = run_single_alloc(a, 0.0, &[cd], 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0, 0.0);
        assert_eq!(flat_zero_alpha.rebalance_action, ACTION_COOLDOWN_BLOCKED);
        assert_eq!(flat_zero_alpha.target_weight, 0.0);
    }

    #[test]
    fn test_asymmetric_loser_invalidation_vs_winner_protection() {
        let a = test_asset(7, 10_000_000.0);
        let cfg = AllocatorConfig::default();

        let mut s1 = HashMap::new();
        let mut s2 = HashMap::new();
        let mut s3 = HashMap::new();

        // Mild cooling: s1_mom = -0.05 (not past -0.08 or -0.12), s1_carry = 0.15 (positive composite > 0.025),
        // s2 = 0, raw w_s1 = 0.0 (below threshold), w_s2 = 0.0, w_s3 = 0.0
        s1.insert(
            a.id,
            CarryMomentumMetrics {
                asset_id: a.id,
                symbol: a.symbol.clone(),
                annualized_vol: 0.50,
                market_beta: 1.0,
                s1_mom_score: -0.05,
                s1_carry_score: 0.25,
                w_s1_carry_mom: 0.0,
                passes_quality: true,
            },
        );
        s2.insert(
            a.id,
            TrendBreakoutMetrics {
                asset_id: a.id,
                symbol: a.symbol.clone(),
                s2_trend_alignment: 0,
                s2_vol_surge_ratio: 1.0,
                w_s2_trend_breakout: 0.0,
                event_trigger: None,
                passes_quality: true,
            },
        );
        s3.insert(
            a.id,
            EventReversionMetrics {
                asset_id: a.id,
                symbol: a.symbol.clone(),
                s3_funding_zscore: 0.0,
                s3_flush_atr_mult: 0.0,
                w_s3_event_reversion: 0.0,
                event_trigger: None,
                passes_quality: true,
            },
        );

        // 1. Underwater loser (-0.5% <= -0.3%) with s2=0 & s1_mom=-0.05 -> bypasses hold hysteresis and CLOSES!
        let mut loser_ctx = HashMap::new();
        loser_ctx.insert(
            a.id,
            HeldPositionContext {
                current_weight: 0.12,
                unrealized_pnl_pct: -0.005,
            },
        );
        let loser_out = allocate_multi_sleeve_weights_with_context(
            std::slice::from_ref(&a),
            &s1,
            &s2,
            &s3,
            &loser_ctx,
            &[],
            &RegimeGateState::default(),
            &cfg,
        );
        assert_eq!(loser_out[0].rebalance_action, ACTION_CLOSE);
        assert_eq!(loser_out[0].target_weight, 0.0);

        // 2. Winner (+1.2% >= +0.5%) with mild cooling (s1_mom = -0.09, s2 = 0, w_s3 = 0) -> HOLDS for Chandelier/TP!
        s1.get_mut(&a.id).unwrap().s1_mom_score = -0.09;
        let mut winner_ctx = HashMap::new();
        winner_ctx.insert(
            a.id,
            HeldPositionContext {
                current_weight: 0.12,
                unrealized_pnl_pct: 0.012,
            },
        );
        let winner_hold = allocate_multi_sleeve_weights_with_context(
            std::slice::from_ref(&a),
            &s1,
            &s2,
            &s3,
            &winner_ctx,
            &[],
            &RegimeGateState::default(),
            &cfg,
        );
        assert_eq!(winner_hold[0].rebalance_action, ACTION_HOLD);
        assert!((winner_hold[0].target_weight - 0.12).abs() < 1e-6);

        // 3. Winner with strong reversal (s1_mom <= -0.12) -> CLOSES!
        s1.get_mut(&a.id).unwrap().s1_mom_score = -0.13;
        let winner_exit = allocate_multi_sleeve_weights_with_context(
            std::slice::from_ref(&a),
            &s1,
            &s2,
            &s3,
            &winner_ctx,
            &[],
            &RegimeGateState::default(),
            &cfg,
        );
        assert_eq!(winner_exit[0].rebalance_action, ACTION_CLOSE);
        assert_eq!(winner_exit[0].target_weight, 0.0);

        // 4. Held SHORT loser (-0.5%) with mild positive momentum (s1_mom = +0.05) -> CLOSES!
        let mut short_loser_ctx = HashMap::new();
        short_loser_ctx.insert(
            a.id,
            HeldPositionContext {
                current_weight: -0.12,
                unrealized_pnl_pct: -0.005,
            },
        );
        s1.get_mut(&a.id).unwrap().s1_mom_score = 0.05;
        s1.get_mut(&a.id).unwrap().s1_carry_score = -0.25;
        let short_loser_out = allocate_multi_sleeve_weights_with_context(
            &[a],
            &s1,
            &s2,
            &s3,
            &short_loser_ctx,
            &[],
            &RegimeGateState::default(),
            &cfg,
        );
        assert_eq!(short_loser_out[0].rebalance_action, ACTION_CLOSE);
        assert_eq!(short_loser_out[0].target_weight, 0.0);
    }

    #[test]
    fn test_r14_m8_allocator_zero_nan_tick_fails_closed() {
        for bad_tick in [0.0, -0.01, f64::NAN, f64::INFINITY] {
            let mut a = test_asset(55, 10_000_000.0);
            a.tick_size = bad_tick;
            // Held long with positive sleeve weights MUST close instead of holding when tick_size is corrupt
            let out = run_single_alloc(a, 0.12, &[], 0.50, 0.20, 0.08, 1, 0.05, 0.0, 0.0, 0.0);
            assert_eq!(out.w_s1_carry_mom, 0.0);
            assert_eq!(out.w_s2_trend_breakout, 0.0);
            assert_eq!(out.target_weight, 0.0);
            assert_eq!(out.rebalance_action, ACTION_CLOSE);
        }
    }

    #[test]
    fn test_regime_gate_defense_in_depth_blocks_counter_entries_never_closes() {
        let cfg = AllocatorConfig::default();
        let a = test_asset(88, 10_000_000.0);
        let bull_gate = RegimeGateState {
            breadth_bull: 0.80,
            eligible_coin_count: 10,
            regime_label: "BULL_EXPANSION".to_string(),
            block_new_shorts: true,
            block_new_longs: false,
        };

        let mut s1 = HashMap::new();
        s1.insert(
            a.id,
            CarryMomentumMetrics {
                asset_id: a.id,
                symbol: a.symbol.clone(),
                annualized_vol: 0.5,
                market_beta: 1.0,
                s1_mom_score: -0.5,
                s1_carry_score: -0.2,
                w_s1_carry_mom: -0.06,
                passes_quality: true,
            },
        );
        let s2 = HashMap::new();
        let s3 = HashMap::new();

        // 1. Flat asset wanting SHORT under block_new_shorts -> ACTION_REGIME_BLOCKED, weight 0.0
        let blocked_flat = allocate_multi_sleeve_weights_with_context(
            std::slice::from_ref(&a),
            &s1,
            &s2,
            &s3,
            &HashMap::new(),
            &[],
            &bull_gate,
            &cfg,
        );
        assert_eq!(blocked_flat[0].rebalance_action, ACTION_REGIME_BLOCKED);
        assert_eq!(blocked_flat[0].target_weight, 0.0);

        // 2. Held SHORT increasing under block_new_shorts -> ACTION_REGIME_BLOCKED (stays at -0.06)
        let mut held_short = HashMap::new();
        held_short.insert(
            a.id,
            HeldPositionContext {
                current_weight: -0.06,
                unrealized_pnl_pct: 0.0,
            },
        );
        let blocked_inc = allocate_multi_sleeve_weights_with_context(
            std::slice::from_ref(&a),
            &s1,
            &s2,
            &s3,
            &held_short,
            &[],
            &bull_gate,
            &cfg,
        );
        assert_eq!(blocked_inc[0].rebalance_action, ACTION_REGIME_BLOCKED);
        assert!((blocked_inc[0].target_weight + 0.06).abs() < 1e-6);

        // 3. Held SHORT wanting to CLOSE (signal flipped neutral) under block_new_shorts -> ACTION_CLOSE (never blocked!)
        s1.get_mut(&a.id).unwrap().s1_mom_score = 0.0;
        s1.get_mut(&a.id).unwrap().s1_carry_score = 0.0;
        s1.get_mut(&a.id).unwrap().w_s1_carry_mom = 0.0;
        let close_short = allocate_multi_sleeve_weights_with_context(
            std::slice::from_ref(&a),
            &s1,
            &s2,
            &s3,
            &held_short,
            &[],
            &bull_gate,
            &cfg,
        );
        assert_eq!(close_short[0].rebalance_action, ACTION_CLOSE);
        assert_eq!(close_short[0].target_weight, 0.0);

        // 4. Held LONG flipping toward SHORT under block_new_shorts -> ACTION_CLOSE (not ACTION_FLIP!)
        s1.get_mut(&a.id).unwrap().s1_mom_score = -0.5;
        s1.get_mut(&a.id).unwrap().w_s1_carry_mom = -0.06;
        let mut held_long = HashMap::new();
        held_long.insert(
            a.id,
            HeldPositionContext {
                current_weight: 0.10,
                unrealized_pnl_pct: 0.0,
            },
        );
        let flip_to_close = allocate_multi_sleeve_weights_with_context(
            &[a],
            &s1,
            &s2,
            &s3,
            &held_long,
            &[],
            &bull_gate,
            &cfg,
        );
        assert_eq!(flip_to_close[0].rebalance_action, ACTION_CLOSE);
        assert_eq!(flip_to_close[0].target_weight, 0.0);
    }
}
