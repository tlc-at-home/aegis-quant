//! Multi-Sleeve Portfolio Weight Allocator & Cooldown Gate (`aegis-signals::allocator`).
//!
//! Combines `(w_s1 + w_s2 + w_s3) * risk_tier.default_leverage_multiplier()` (seeded at `Extreme 1.85x`),
//! enforces breadth-based directional regime gating (`REGIME_BLOCKED`) and active cooldowns (`COOLDOWN_BLOCKED`),
//! applies asymmetric exit hysteresis for held winners vs underwater losers,
//! enforces single-asset (`0.15`) and gross leverage caps, and classifies `rebalance_action`.

use crate::breadth::{MarketRegime, RegimeGateConfig, RegimeGateState};
use crate::carry_momentum::CarryMomentumMetrics;
use crate::event_reversion::EventReversionMetrics;
use crate::features::ExitConfirmState;
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

/// Asymmetric exit hysteresis configuration for held positions (`R14-L21`, `P1-Strat`, `R14-W2`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct HysteresisConfig {
    pub min_rebalance_delta_weight: f64,
    pub early_loser_invalidation_enabled: bool,
    #[serde(default = "default_early_loser_r0_mult")]
    pub early_loser_r0_mult: f64,
    #[serde(default = "default_early_loser_max_loss_pct")]
    pub early_loser_max_loss_pct: f64,
    pub s1_reversal_mom_threshold: f64,
    pub s1_support_composite_threshold: f64,
    #[serde(default = "default_maker_fee_bps")]
    pub maker_fee_bps: f64,
    #[serde(default = "default_taker_fee_bps")]
    pub taker_fee_bps: f64,
    #[serde(default = "default_reduce_min_net_edge_bps")]
    pub reduce_min_net_edge_bps: f64,
}

fn default_early_loser_r0_mult() -> f64 {
    0.25
}

fn default_early_loser_max_loss_pct() -> f64 {
    0.009
}

fn default_maker_fee_bps() -> f64 {
    1.5
}

fn default_taker_fee_bps() -> f64 {
    4.5
}

fn default_reduce_min_net_edge_bps() -> f64 {
    8.0
}

impl Default for HysteresisConfig {
    fn default() -> Self {
        Self {
            min_rebalance_delta_weight: 0.025,
            early_loser_invalidation_enabled: true,
            early_loser_r0_mult: 0.25,
            early_loser_max_loss_pct: 0.009,
            s1_reversal_mom_threshold: 0.08,
            s1_support_composite_threshold: 0.025,
            maker_fee_bps: 1.5,
            taker_fee_bps: 4.5,
            reduce_min_net_edge_bps: 8.0,
        }
    }
}

/// Computes the fractional breakeven-plus-edge hurdle (`round_trip_cost + reduce_min_net_edge`,
/// e.g. `(1.5 + 4.5 + 8.0) / 10_000 = 0.0014` = `14.0 bps`) required before a non-emergency
/// `ACTION_REDUCE` trim is emitted (`R14-W2`).
pub fn min_reduce_breakeven_edge_pct(
    maker_fee_bps: f64,
    taker_fee_bps: f64,
    reduce_min_net_edge_bps: f64,
) -> f64 {
    let maker = if maker_fee_bps.is_finite() {
        maker_fee_bps.clamp(0.0, 100.0)
    } else {
        1.5
    };
    let taker = if taker_fee_bps.is_finite() {
        taker_fee_bps.clamp(0.0, 100.0)
    } else {
        4.5
    };
    let edge = if reduce_min_net_edge_bps.is_finite() {
        reduce_min_net_edge_bps.clamp(0.0, 500.0)
    } else {
        8.0
    };
    (maker + taker + edge) / 10_000.0
}

/// Computes the fractional unrealized PnL hurdle (negative, e.g. `-0.009`) below which
/// an underwater position qualifies for early-loser invalidation / hold-floor bypass.
pub fn effective_early_loser_threshold(entry_r0_pct: f64, r0_mult: f64, max_loss_pct: f64) -> f64 {
    let mult = if r0_mult.is_finite() {
        r0_mult.clamp(0.05, 1.0)
    } else {
        0.25
    };
    let max_cap = if max_loss_pct.is_finite() {
        max_loss_pct.clamp(0.003, 0.10)
    } else {
        0.009
    };
    if entry_r0_pct.is_finite() && entry_r0_pct > 1e-6 {
        -((entry_r0_pct * mult).clamp(0.003, max_cap))
    } else {
        -0.003
    }
}

/// Live position context passed into allocator evaluation for asymmetric loser/winner exit hysteresis.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
pub struct HeldPositionContext {
    pub current_weight: f64,
    /// Fractional unrealized PnL (`(mark - entry) / entry * side.sign()`, e.g. `-0.003` = `-0.3%`, `+0.005` = `+0.5%`).
    pub unrealized_pnl_pct: f64,
    /// Initial stop-loss distance as a fraction of entry price (`entry_r0 / entry_price`), 0.0 when unknown.
    #[serde(default)]
    pub entry_r0_pct: f64,
}

/// Parameter bundle for 2-arg [`apply_held_exit_hysteresis`] (`R14-L21`).
struct HeldHysteresisInput {
    cur_w: f64,
    unblocked_scaled_weight: f64,
    raw_combined_weight: f64,
    unrealized_pnl_pct: f64,
    entry_r0_pct: f64,
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
    #[serde(default = "default_max_open_positions")]
    pub max_open_positions: usize,
    pub s1_carry_mom_budget: f64,
    pub s2_trend_breakout_budget: f64,
    pub s3_event_reversion_budget: f64,
    #[serde(default = "default_true")]
    pub enable_breadth_regime_gate: bool,
    #[serde(default = "default_bull_block_shorts")]
    pub breadth_bull_block_shorts_threshold: f64,
    #[serde(default = "default_bear_block_longs")]
    pub breadth_bear_block_longs_threshold: f64,
    #[serde(default = "default_true")]
    pub wall_clock_normalize_indicators: bool,
    #[serde(default = "default_true")]
    pub s3_strict_admission_enabled: bool,
    #[serde(default)]
    pub enable_regime_budget_multipliers: bool,
    #[serde(default = "default_chop_long_weight_mult")]
    pub chop_long_weight_mult: f64,
    #[serde(default = "default_chop_short_weight_mult")]
    pub chop_short_weight_mult: f64,
    #[serde(default = "default_neutral_long_weight_mult")]
    pub neutral_long_weight_mult: f64,
    #[serde(default = "default_signal_exit_confirm_bars")]
    pub signal_exit_confirm_bars: u32,
    #[serde(default)]
    pub suppress_direct_flips: bool,
    #[serde(default = "default_one")]
    pub max_net_exposure_ratio_of_gross: f64,
    #[serde(default)]
    pub hysteresis: HysteresisConfig,
}

fn default_true() -> bool {
    true
}

fn default_max_open_positions() -> usize {
    6
}

fn default_bull_block_shorts() -> f64 {
    0.68
}

fn default_bear_block_longs() -> f64 {
    0.32
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

fn default_signal_exit_confirm_bars() -> u32 {
    1
}

fn default_one() -> f64 {
    1.0
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self {
            risk_tier: RiskTier::Extreme,
            custom_leverage_override: None,
            max_gross_leverage_x: 2.50,
            max_single_asset_weight: RiskTier::Extreme.default_max_single_asset_weight(None),
            min_rebalance_delta_weight: 0.025,
            max_open_positions: 6,
            s1_carry_mom_budget: 0.45,
            s2_trend_breakout_budget: 0.40,
            s3_event_reversion_budget: 0.10,
            enable_breadth_regime_gate: true,
            breadth_bull_block_shorts_threshold: 0.68,
            breadth_bear_block_longs_threshold: 0.32,
            wall_clock_normalize_indicators: true,
            s3_strict_admission_enabled: true,
            enable_regime_budget_multipliers: false,
            chop_long_weight_mult: 0.50,
            chop_short_weight_mult: 0.65,
            neutral_long_weight_mult: 0.65,
            signal_exit_confirm_bars: 1,
            suppress_direct_flips: false,
            max_net_exposure_ratio_of_gross: 1.0,
            hysteresis: HysteresisConfig::default(),
        }
    }
}

impl AllocatorConfig {
    pub fn from_config_map(cfg: &ConfigMap) -> Self {
        let tier = cfg.risk_tier();
        let mult = cfg.target_leverage_multiplier();
        let default_max_single = tier.default_max_single_asset_weight(Some(mult));
        let min_rebalance_delta_weight = cfg
            .get_f64("min_rebalance_delta_weight", 0.025)
            .clamp(0.005, 0.25);
        Self {
            risk_tier: tier,
            custom_leverage_override: (tier == RiskTier::Custom).then_some(mult),
            max_gross_leverage_x: cfg.get_f64("max_gross_leverage_x", 2.50).clamp(0.20, 5.00),
            max_single_asset_weight: cfg
                .get_f64("max_single_asset_weight", default_max_single)
                .clamp(0.01, 1.00),
            min_rebalance_delta_weight,
            max_open_positions: cfg.get_usize("max_open_positions", 6).clamp(1, 100),
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
            wall_clock_normalize_indicators: cfg.get_bool("wall_clock_normalize_indicators", true),
            s3_strict_admission_enabled: cfg.get_bool("s3_strict_admission_enabled", true),
            enable_regime_budget_multipliers: cfg
                .get_bool("enable_regime_budget_multipliers", false),
            chop_long_weight_mult: cfg.get_f64("chop_long_weight_mult", 0.50).clamp(0.0, 1.0),
            chop_short_weight_mult: cfg.get_f64("chop_short_weight_mult", 0.65).clamp(0.0, 1.0),
            neutral_long_weight_mult: cfg
                .get_f64("neutral_long_weight_mult", 0.65)
                .clamp(0.0, 1.0),
            signal_exit_confirm_bars: cfg.get_i64("signal_exit_confirm_bars", 1).clamp(1, 5) as u32,
            suppress_direct_flips: cfg.get_bool("suppress_direct_flips", false),
            max_net_exposure_ratio_of_gross: cfg
                .get_f64("max_net_exposure_ratio_of_gross", 1.0)
                .clamp(0.10, 1.0),
            hysteresis: HysteresisConfig {
                min_rebalance_delta_weight,
                early_loser_invalidation_enabled: cfg
                    .get_bool("early_loser_invalidation_enabled", true),
                early_loser_r0_mult: cfg.get_f64("early_loser_r0_mult", 0.25).clamp(0.05, 1.0),
                early_loser_max_loss_pct: cfg
                    .get_f64("early_loser_max_loss_pct", 0.009)
                    .clamp(0.003, 0.10),
                s1_reversal_mom_threshold: cfg
                    .get_f64("hysteresis_s1_reversal_mom_threshold", 0.08)
                    .clamp(0.01, 0.50),
                s1_support_composite_threshold: cfg
                    .get_f64("hysteresis_s1_support_composite_threshold", 0.025)
                    .clamp(0.005, 0.25),
                maker_fee_bps: cfg.get_f64("maker_fee_bps", 1.5).clamp(0.0, 100.0),
                taker_fee_bps: cfg.get_f64("taker_fee_bps", 4.5).clamp(0.0, 100.0),
                reduce_min_net_edge_bps: cfg
                    .get_f64("reduce_min_net_edge_bps", 8.0)
                    .clamp(0.0, 500.0),
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
            enable_regime_budget_multipliers: self.enable_regime_budget_multipliers,
            chop_long_weight_mult: self.chop_long_weight_mult,
            chop_short_weight_mult: self.chop_short_weight_mult,
            neutral_long_weight_mult: self.neutral_long_weight_mult,
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
/// Returns `(effective_desired_weight, hysteresis_held, emergency_exit_bypass)`.
fn apply_held_exit_hysteresis(
    input: &HeldHysteresisInput,
    cfg: &HysteresisConfig,
) -> (f64, bool, bool) {
    if input.cur_w.abs() < 1e-4 {
        return (input.unblocked_scaled_weight, false, false);
    }
    if !input.is_eligible {
        return (input.unblocked_scaled_weight, false, true);
    }
    if !input.has_signal_eval {
        return (input.unblocked_scaled_weight, false, false);
    }

    let short_flush_emergency =
        (input.cur_w < 0.0 && input.is_downward_flush) || input.s3_flush_atr_mult <= -3.5;
    let base_emergency = input.counter_regime_held || short_flush_emergency;

    let min_delta = cfg.min_rebalance_delta_weight.max(1e-4);
    let min_hold_band = (min_delta * 0.5).max(1e-3);

    // Already meaningfully aligned on the held side -> no risk of spurious 0.0 close
    if input.unblocked_scaled_weight * input.cur_w > 0.0
        && input.unblocked_scaled_weight.abs() >= min_hold_band
    {
        return (input.unblocked_scaled_weight, false, base_emergency);
    }

    let s1_composite = 0.65 * input.s1_mom_score + 0.35 * input.s1_carry_score;
    let loser_thresh = effective_early_loser_threshold(
        input.entry_r0_pct,
        cfg.early_loser_r0_mult,
        cfg.early_loser_max_loss_pct,
    );
    let is_loser = cfg.early_loser_invalidation_enabled
        && input.unrealized_pnl_pct.is_finite()
        && input.unrealized_pnl_pct <= loser_thresh;
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
                return (exit_w, false, true);
            }
        }

        if is_winner {
            let strong_reversal = input.unblocked_scaled_weight <= -min_hold_band
                || input.w_s3 < 0.0
                || input.s2_trend_alignment < 0
                || input.s1_mom_score <= -winner_rev_thresh;
            return if strong_reversal {
                (input.unblocked_scaled_weight, false, base_emergency)
            } else {
                (input.cur_w, true, base_emergency)
            };
        }

        // Neutral band: clear bearish reversal / exit -> never block CLOSE or FLIP
        if input.unblocked_scaled_weight <= -min_hold_band
            || input.w_s3 < 0.0
            || input.s2_trend_alignment < 0
            || input.s1_mom_score <= -rev_thresh
        {
            return (input.unblocked_scaled_weight, false, base_emergency);
        }

        let has_mild_long_support = input.raw_combined_weight > 0.0
            || (input.s1_mom_score > 0.01 && s1_composite > 0.0)
            || s1_composite > sup_thresh
            || input.s2_trend_alignment > 0
            || input.s3_flush_atr_mult <= -0.5
            || input.s3_funding_zscore <= -0.8;

        if has_mild_long_support {
            (input.cur_w, true, base_emergency)
        } else {
            (input.unblocked_scaled_weight, false, base_emergency)
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
                return (exit_w, false, true);
            }
        }

        if is_winner {
            let strong_reversal = input.is_downward_flush
                || input.w_s3 > 0.0
                || input.unblocked_scaled_weight >= min_hold_band
                || input.s2_trend_alignment > 0
                || input.s1_mom_score >= winner_rev_thresh;
            return if strong_reversal {
                (input.unblocked_scaled_weight, false, base_emergency)
            } else {
                (input.cur_w, true, base_emergency)
            };
        }

        // Held SHORT neutral band: flush conflict or clear bullish reversal / exit -> allow CLOSE or FLIP
        if input.is_downward_flush
            || input.w_s3 > 0.0
            || input.unblocked_scaled_weight >= min_hold_band
            || input.s2_trend_alignment > 0
            || input.s1_mom_score >= rev_thresh
        {
            return (input.unblocked_scaled_weight, false, base_emergency);
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
            (input.cur_w, true, base_emergency)
        } else {
            (input.unblocked_scaled_weight, false, base_emergency)
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
                    ..Default::default()
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
    let mut empty_confirm = HashMap::new();
    let empty_ts = HashMap::new();
    allocate_multi_sleeve_weights_with_exit_confirm(
        assets,
        s1_map,
        s2_map,
        s3_map,
        held_contexts,
        active_cooldowns,
        regime_gate,
        cfg,
        &empty_ts,
        &mut empty_confirm,
    )
}

/// Internal allocator entrypoint supporting persistent `BarCloseCache` N2 exit confirmation state.
#[allow(clippy::too_many_arguments)]
pub(crate) fn allocate_multi_sleeve_weights_with_exit_confirm(
    assets: &[ExchangeAsset],
    s1_map: &HashMap<i64, CarryMomentumMetrics>,
    s2_map: &HashMap<i64, TrendBreakoutMetrics>,
    s3_map: &HashMap<i64, EventReversionMetrics>,
    held_contexts: &HashMap<i64, HeldPositionContext>,
    active_cooldowns: &[CooldownAssetView],
    regime_gate: &RegimeGateState,
    cfg: &AllocatorConfig,
    bar_ts_by_asset: &HashMap<i64, i64>,
    exit_confirm_by_asset: &mut HashMap<i64, ExitConfirmState>,
) -> Vec<SleeveSignalInput> {
    let lev_mult = cfg.effective_leverage_multiplier();
    let max_single = cfg.max_single_asset_weight.max(0.01);
    let max_gross = cfg.max_gross_leverage_x.max(0.10);
    let eff_min_delta = (cfg.min_rebalance_delta_weight * lev_mult)
        .max(1e-4)
        .min(cfg.max_single_asset_weight.max(1e-4));
    let mut hyst_cfg = cfg.hysteresis;
    hyst_cfg.min_rebalance_delta_weight = eff_min_delta;

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
        post_cooldown_weight: f64,
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

        let ctx = held_contexts.get(&asset.id).copied().unwrap_or_default();
        let cur_w = round_dp(ctx.current_weight, 4);
        let loser_thresh = effective_early_loser_threshold(
            ctx.entry_r0_pct,
            cfg.hysteresis.early_loser_r0_mult,
            cfg.hysteresis.early_loser_max_loss_pct,
        );
        let is_loser = cfg.hysteresis.early_loser_invalidation_enabled
            && ctx.unrealized_pnl_pct.is_finite()
            && ctx.unrealized_pnl_pct <= loser_thresh;

        // Lever N1: apply unified directional regime multiplier ONCE across sleeves,
        // protecting already-held non-loser winners from spurious partial-reduce churn.
        let raw_sum = w_s1 + w_s2 + w_s3;
        let side_mult = if !cfg.enable_regime_budget_multipliers {
            1.0
        } else if raw_sum > 0.0 {
            regime_gate.long_weight_mult.clamp(0.0, 1.0)
        } else if raw_sum < 0.0 {
            regime_gate.short_weight_mult.clamp(0.0, 1.0)
        } else {
            1.0
        };
        let eff_side_mult =
            if cur_w * raw_sum > 0.0 && !is_loser && side_mult > 0.0 && side_mult < 1.0 {
                let min_hold_raw = cur_w.abs() / lev_mult.max(1e-6);
                let desired_raw_mag = (raw_sum * side_mult).abs();
                if desired_raw_mag < min_hold_raw {
                    (raw_sum.abs().min(min_hold_raw) / raw_sum.abs().max(1e-12))
                        .clamp(side_mult, 1.0)
                } else {
                    side_mult
                }
            } else {
                side_mult
            };

        let w_s1 = w_s1 * eff_side_mult;
        let w_s2 = w_s2 * eff_side_mult;
        let w_s3 = w_s3 * eff_side_mult;

        let raw_combined_weight = round_dp(w_s1 + w_s2 + w_s3, 4);
        let unblocked_scaled_weight = round_dp(raw_combined_weight * lev_mult, 4);

        let in_cooldown = cooldown_ids.contains(&asset.id)
            || cooldown_symbols.contains(&asset.symbol.to_ascii_uppercase());

        // Never allow hysteresis to trap a held dead/illiquid coin forever when eligibility drops,
        // sleeve quality fails, tick size exceeds cap, or a ghost-volume book reports zero sleeve alpha.
        let all_sleeves_zero = w_s1.abs() <= 1e-6 && w_s2.abs() <= 1e-6 && w_s3.abs() <= 1e-6;
        let ghost_zero_alpha = all_sleeves_zero
            && (!asset.day_volume_usdc.is_finite()
                || asset.day_volume_usdc < MIN_FALLBACK_DAY_VOLUME_USDC);
        let hysteresis_eligible = asset_eligible && quality_ok && !ghost_zero_alpha;

        let sparse_universe_confirm = cfg.signal_exit_confirm_bars > 1
            && regime_gate.eligible_coin_count > 0
            && regime_gate.eligible_coin_count < 10;
        let allow_counter_regime_emergency = !sparse_universe_confirm
            || regime_gate.regime == MarketRegime::CapitulationShock
            || regime_gate.breadth_bull >= 0.80
            || regime_gate.breadth_bull <= 0.20;
        let counter_regime_held = ((cur_w > 0.0 && regime_gate.block_new_longs)
            || (cur_w < 0.0 && regime_gate.block_new_shorts))
            && allow_counter_regime_emergency;

        let hyst_input = HeldHysteresisInput {
            cur_w,
            unblocked_scaled_weight,
            raw_combined_weight,
            unrealized_pnl_pct: ctx.unrealized_pnl_pct,
            entry_r0_pct: ctx.entry_r0_pct,
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
        let (mut effective_desired_weight, mut hysteresis_held, emergency_exit_bypass) =
            apply_held_exit_hysteresis(&hyst_input, &hyst_cfg);

        // Defense-in-depth Breadth Regime Gate (`P1-Strat`): NEVER blocks CLOSE or REDUCE!
        let mut regime_blocked = false;
        let post_regime_weight = if regime_gate.block_new_shorts {
            if cur_w.abs() < 1e-4 && effective_desired_weight < 0.0 {
                if effective_desired_weight.abs() >= eff_min_delta {
                    regime_blocked = true;
                }
                0.0
            } else if cur_w > 0.0 && effective_desired_weight < 0.0 {
                // Held LONG flipping into blocked SHORT -> CLOSE instead of FLIP
                0.0
            } else if cur_w < 0.0 && effective_desired_weight < cur_w {
                if (effective_desired_weight - cur_w).abs() >= eff_min_delta {
                    regime_blocked = true;
                }
                cur_w
            } else {
                effective_desired_weight
            }
        } else if regime_gate.block_new_longs {
            if cur_w.abs() < 1e-4 && effective_desired_weight > 0.0 {
                if effective_desired_weight.abs() >= eff_min_delta {
                    regime_blocked = true;
                }
                0.0
            } else if cur_w < 0.0 && effective_desired_weight > 0.0 {
                // Held SHORT flipping into blocked LONG -> CLOSE instead of FLIP
                0.0
            } else if cur_w > 0.0 && effective_desired_weight > cur_w {
                if (effective_desired_weight - cur_w).abs() >= eff_min_delta {
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

        // Lever N3: optional suppression of direct opposite-side flips -> exit-to-flat ACTION_CLOSE.
        let post_cooldown_weight = if cfg.suppress_direct_flips
            && cur_w.abs() >= 1e-4
            && post_cooldown_weight * cur_w < 0.0
        {
            0.0
        } else {
            post_cooldown_weight
        };

        let mut capped_single = post_cooldown_weight.clamp(-max_single, max_single);

        // R14-W2: Breakeven-plus hysteresis on non-emergency signal ACTION_REDUCE trims.
        // Suppress calm partial trims until unrealized PnL clears round-trip cost + reduce_min_net_edge_bps,
        // while allowing underwater early-loser invalidations and single-asset/gross cap trims unconditionally.
        if cur_w.abs() >= 1e-4
            && !is_loser
            && !emergency_exit_bypass
            && !in_cooldown
            && hysteresis_eligible
            && cur_w.abs() <= max_single + 1e-9
            && capped_single * cur_w > 0.0
            && capped_single.abs() < cur_w.abs()
            && (capped_single - cur_w).abs() >= eff_min_delta
        {
            let min_reduce_edge = min_reduce_breakeven_edge_pct(
                hyst_cfg.maker_fee_bps,
                hyst_cfg.taker_fee_bps,
                hyst_cfg.reduce_min_net_edge_bps,
            );
            if !ctx.unrealized_pnl_pct.is_finite() || ctx.unrealized_pnl_pct < min_reduce_edge {
                effective_desired_weight = cur_w;
                capped_single = cur_w;
                hysteresis_held = true;
            }
        }

        // Lever N2: multi-bar confirmation for non-emergency ACTION_CLOSE (with mandatory R13-S1 bypass).
        let confirm_bars = cfg.signal_exit_confirm_bars.clamp(1, 5);
        if cur_w.abs() < 1e-4 || confirm_bars <= 1 {
            exit_confirm_by_asset.remove(&asset.id);
        } else {
            let tentative_action = classify_rebalance_action_with_gates(
                cur_w,
                round_dp(capped_single, 4),
                effective_desired_weight,
                in_cooldown,
                regime_blocked,
                eff_min_delta,
            );
            if tentative_action == ACTION_CLOSE && !emergency_exit_bypass {
                let bar_ts = bar_ts_by_asset.get(&asset.id).copied().unwrap_or(0);
                let next_state = match exit_confirm_by_asset.get(&asset.id).copied() {
                    None => ExitConfirmState {
                        count: 1,
                        last_bar_ts_ms: bar_ts,
                    },
                    Some(prev) => {
                        if bar_ts > prev.last_bar_ts_ms {
                            ExitConfirmState {
                                count: prev.count.saturating_add(1),
                                last_bar_ts_ms: bar_ts,
                            }
                        } else {
                            prev
                        }
                    }
                };
                exit_confirm_by_asset.insert(asset.id, next_state);
                if next_state.count < confirm_bars {
                    effective_desired_weight = cur_w;
                    capped_single = cur_w;
                    hysteresis_held = true;
                }
            } else {
                exit_confirm_by_asset.remove(&asset.id);
            }
        }

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
            post_cooldown_weight,
            hysteresis_held,
            capped_single_weight: capped_single,
            current_weight: cur_w,
            in_cooldown,
            regime_blocked,
            event_trigger,
        });
    }

    let prune_cap = cfg.max_open_positions.saturating_add(6).max(12);
    if drafts.len() > prune_cap {
        let mut keep_ids: HashSet<i64> = HashSet::new();
        for d in &drafts {
            if d.current_weight.abs() >= 1e-4 && d.capped_single_weight.abs() >= 1e-4 {
                keep_ids.insert(d.asset.id);
            }
        }
        let mut ranked: Vec<(i64, f64, f64, f64)> = drafts
            .iter()
            .filter(|d| !keep_ids.contains(&d.asset.id) && d.capped_single_weight.abs() > 1e-6)
            .map(|d| {
                (
                    d.asset.id,
                    d.capped_single_weight.abs(),
                    d.post_cooldown_weight.abs(),
                    d.asset.day_volume_usdc,
                )
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.1.total_cmp(&a.1)
                .then_with(|| b.2.total_cmp(&a.2))
                .then_with(|| b.3.total_cmp(&a.3))
                .then_with(|| a.0.cmp(&b.0))
        });
        for (id, _, _, _) in ranked {
            if keep_ids.len() >= prune_cap {
                break;
            }
            keep_ids.insert(id);
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

    let draft_asset_ids: HashSet<i64> = drafts.iter().map(|d| d.asset.id).collect();
    let unlisted_active_gross: f64 = held_contexts
        .iter()
        .filter(|(id, _)| !draft_asset_ids.contains(id))
        .map(|(_, c)| {
            if c.current_weight.is_finite() {
                c.current_weight.abs()
            } else {
                0.0
            }
        })
        .sum();
    let eff_max_gross = (max_gross - unlisted_active_gross).max(0.0);

    let total_gross_before_cap: f64 = drafts.iter().map(|d| d.capped_single_weight.abs()).sum();

    let gross_scale = if total_gross_before_cap > eff_max_gross && total_gross_before_cap > 1e-8 {
        eff_max_gross / total_gross_before_cap
    } else {
        1.0
    };

    // Lever N5: portfolio net-direction exposure cap relative to pre-net-cap gross G_0.
    let net_ratio_cap = cfg.max_net_exposure_ratio_of_gross.clamp(0.10, 1.0);
    let (long_net_scale, short_net_scale) = if net_ratio_cap < 0.9999 {
        let mut sum_long = 0.0_f64;
        let mut sum_short = 0.0_f64;
        for d in &drafts {
            let w = d.capped_single_weight * gross_scale;
            if w > 0.0 {
                sum_long += w;
            } else if w < 0.0 {
                sum_short += w.abs();
            }
        }
        let g0 = sum_long + sum_short;
        if g0 > 1e-8 {
            let cap = net_ratio_cap * g0;
            let net = sum_long - sum_short;
            if net > cap && sum_long > 1e-8 {
                (((cap + sum_short) / sum_long).clamp(0.0, 1.0), 1.0)
            } else if -net > cap && sum_short > 1e-8 {
                (1.0, ((cap + sum_long) / sum_short).clamp(0.0, 1.0))
            } else {
                (1.0, 1.0)
            }
        } else {
            (1.0, 1.0)
        }
    } else {
        (1.0, 1.0)
    };

    let mut results = Vec::with_capacity(drafts.len());
    for d in drafts {
        let raw_scaled = d.capped_single_weight * gross_scale;
        let net_scale = if raw_scaled > 0.0 {
            long_net_scale
        } else if raw_scaled < 0.0 {
            short_net_scale
        } else {
            1.0
        };
        let mut target_weight = round_dp(raw_scaled * net_scale, 4);
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
            eff_min_delta,
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
            ..RegimeGateState::default()
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
                ..Default::default()
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
                ..Default::default()
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

    #[test]
    fn test_leak_l4_eff_min_delta_and_tier_max_single_weight_defaults() {
        use aegis_core::ConfigMap;

        // 1. Default and tier-scaled max_single_asset_weight fallbacks
        assert!((AllocatorConfig::default().max_single_asset_weight - 0.25).abs() < 1e-9);
        for (tier_str, expected_max) in [
            ("conservative", 0.15),
            ("standard", 0.15),
            ("aggressive", 0.20),
            ("extreme", 0.25),
        ] {
            let mut cm = ConfigMap::new();
            cm.insert("risk_tier", tier_str);
            let cfg = AllocatorConfig::from_config_map(&cm);
            assert!(
                (cfg.max_single_asset_weight - expected_max).abs() < 1e-9,
                "tier={tier_str}: expected {expected_max}, got {}",
                cfg.max_single_asset_weight
            );
        }
        let mut explicit_cm = ConfigMap::new();
        explicit_cm.insert("risk_tier", "extreme");
        explicit_cm.insert("max_single_asset_weight", "0.01");
        assert!(
            (AllocatorConfig::from_config_map(&explicit_cm).max_single_asset_weight - 0.01).abs()
                < 1e-9
        );

        // 2. Extreme (1.85x): eff_min_delta = 0.025 * 1.85 = 0.04625 -> 0.015 unlevered (0.0278 levered) is HOLD, not OPEN
        let a = test_asset(1, 10_000_000.0);
        let mut s1 = HashMap::new();
        s1.insert(
            a.id,
            CarryMomentumMetrics {
                asset_id: a.id,
                symbol: a.symbol.clone(),
                annualized_vol: 0.5,
                market_beta: 1.0,
                s1_mom_score: 0.3,
                s1_carry_score: 0.1,
                w_s1_carry_mom: 0.015,
                passes_quality: true,
            },
        );
        let ext_cfg = AllocatorConfig::default();
        let ext_small = allocate_multi_sleeve_weights(
            std::slice::from_ref(&a),
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &[],
            &ext_cfg,
        );
        assert_eq!(ext_small[0].rebalance_action, ACTION_HOLD);

        // 0.030 unlevered (0.0555 >= 0.04625) opens cleanly
        s1.get_mut(&a.id).unwrap().w_s1_carry_mom = 0.030;
        let ext_open = allocate_multi_sleeve_weights(
            std::slice::from_ref(&a),
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &[],
            &ext_cfg,
        );
        assert_eq!(ext_open[0].rebalance_action, ACTION_OPEN_LONG);

        // 3. Conservative (0.65x): eff_min_delta = 0.025 * 0.65 = 0.01625 -> 0.035 unlevered (0.0228 levered) opens cleanly!
        let cons_cfg = AllocatorConfig {
            risk_tier: RiskTier::Conservative,
            ..AllocatorConfig::default()
        };
        s1.get_mut(&a.id).unwrap().w_s1_carry_mom = 0.035;
        let cons_open = allocate_multi_sleeve_weights(
            &[a],
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &[],
            &cons_cfg,
        );
        assert_eq!(cons_open[0].rebalance_action, ACTION_OPEN_LONG);
        assert!((cons_open[0].target_weight - 0.0228).abs() < 1e-4);
    }

    #[test]
    fn test_p1_1_prune_cap_scales_with_mp_excludes_closing_and_tiebreaks_pre_clamp() {
        let cfg = AllocatorConfig {
            max_open_positions: 10, // prune_cap = 10 + 6 = 16
            max_single_asset_weight: 0.25,
            max_gross_leverage_x: 4.50,
            ..AllocatorConfig::default()
        };
        let mut assets = Vec::new();
        let mut s1 = HashMap::new();
        let mut held = HashMap::new();

        // 6 held active (ids 1..=6), 4 held closing (ids 7..=10, zero sleeve signal),
        // and 11 flat challengers (ids 11..=21). Total 21 assets; 17 non-zero candidates > 16 cap!
        for i in 1..=21i64 {
            let vol = if i == 11 {
                1_000_000.0 // lower volume challenger, high pre-clamp weight
            } else if i == 12 {
                50_000_000.0 // higher volume challenger, lower pre-clamp weight
            } else {
                (i as f64) * 1_000_000.0
            };
            let a = test_asset(i, vol);
            assets.push(a.clone());

            let w_s1 = if (1..=6).contains(&i) {
                0.12
            } else if (7..=10).contains(&i) {
                0.0 // closing!
            } else if i == 11 {
                0.40 // clamped 0.25, pre_clamp 0.74
            } else if i == 12 {
                0.16 // clamped 0.25, pre_clamp 0.296
            } else {
                0.30 // clamped 0.25, pre_clamp 0.555
            };
            s1.insert(
                i,
                CarryMomentumMetrics {
                    asset_id: i,
                    symbol: a.symbol.clone(),
                    annualized_vol: 0.5,
                    market_beta: 1.0,
                    s1_mom_score: if w_s1 > 0.0 { 0.5 } else { 0.0 },
                    s1_carry_score: 0.0,
                    w_s1_carry_mom: w_s1,
                    passes_quality: true,
                },
            );
            if i <= 10 {
                held.insert(
                    i,
                    HeldPositionContext {
                        current_weight: 0.12,
                        unrealized_pnl_pct: 0.0,
                        ..Default::default()
                    },
                );
            }
        }

        let signals = allocate_multi_sleeve_weights_with_context(
            &assets,
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &held,
            &[],
            &RegimeGateState::default(),
            &cfg,
        );
        let non_zero: Vec<i64> = signals
            .iter()
            .filter(|s| s.target_weight.abs() > 1e-6)
            .map(|s| s.asset_id)
            .collect();
        assert_eq!(non_zero.len(), 16, "mp10 must keep 16 active positions");

        // All 4 closing positions emit ACTION_CLOSE without consuming any of the 16 candidate slots
        for id in 7..=10i64 {
            let sig = signals.iter().find(|s| s.asset_id == id).unwrap();
            assert_eq!(sig.rebalance_action, ACTION_CLOSE);
            assert_eq!(sig.target_weight, 0.0);
        }

        // Asset 11 (pre-clamp 0.74, lower vol) must beat Asset 12 (pre-clamp 0.296, higher vol) for the last slot
        assert!(
            non_zero.contains(&11),
            "Higher pre-clamp weight asset 11 must win slot"
        );
        assert!(
            !non_zero.contains(&12),
            "Lower pre-clamp weight asset 12 must be pruned"
        );
    }

    #[test]
    fn test_n1_regime_throttle_and_non_loser_winner_protection() {
        use crate::breadth::{compute_regime_gate_state, RegimeGateConfig};

        let cfg = AllocatorConfig {
            enable_regime_budget_multipliers: true,
            chop_long_weight_mult: 0.50,
            chop_short_weight_mult: 0.65,
            neutral_long_weight_mult: 0.65,
            ..AllocatorConfig::default()
        };
        let chop_gate = compute_regime_gate_state(0.40, 10, 0.0, 0, &cfg.regime_gate_config());
        assert_eq!(chop_gate.long_weight_mult, 0.50);
        assert_eq!(chop_gate.short_weight_mult, 0.65);

        let a_flat_long = test_asset(1, 10_000_000.0);
        let a_flat_short = test_asset(2, 10_000_000.0);
        let a_winner_long = test_asset(3, 10_000_000.0);
        let a_loser_long = test_asset(4, 10_000_000.0);
        let assets = vec![
            a_flat_long.clone(),
            a_flat_short.clone(),
            a_winner_long.clone(),
            a_loser_long.clone(),
        ];

        let mut s1 = HashMap::new();
        for (id, w) in [(1, 0.10), (2, -0.10), (3, 0.14), (4, 0.14)] {
            s1.insert(
                id,
                CarryMomentumMetrics {
                    asset_id: id,
                    symbol: format!("A{id}-USDC"),
                    annualized_vol: 0.5,
                    market_beta: 1.0,
                    s1_mom_score: if w > 0.0 { 0.5 } else { -0.5 },
                    s1_carry_score: 0.0,
                    w_s1_carry_mom: w,
                    passes_quality: true,
                },
            );
        }
        let mut held = HashMap::new();
        held.insert(
            3,
            HeldPositionContext {
                current_weight: 0.20,
                unrealized_pnl_pct: 0.015, // winner: unthrottled = 0.259, throttled = 0.1295 < 0.20 -> HOLD at ~0.20!
                ..Default::default()
            },
        );
        held.insert(
            4,
            HeldPositionContext {
                current_weight: 0.20,
                unrealized_pnl_pct: -0.010, // loser: throttled down to 0.1295 -> ACTION_REDUCE!
                ..Default::default()
            },
        );

        let out = allocate_multi_sleeve_weights_with_context(
            &assets,
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &held,
            &[],
            &chop_gate,
            &cfg,
        );
        let s_long = out.iter().find(|s| s.asset_id == 1).unwrap();
        assert!((s_long.target_weight - 0.0925).abs() < 1e-4);
        assert!((s_long.w_s1_carry_mom * 1.85 - s_long.target_weight).abs() < 1e-3);

        let s_short = out.iter().find(|s| s.asset_id == 2).unwrap();
        assert!((s_short.target_weight + 0.1202).abs() < 2e-4);

        let s_win = out.iter().find(|s| s.asset_id == 3).unwrap();
        assert_eq!(s_win.rebalance_action, ACTION_HOLD);
        assert!((s_win.target_weight - 0.20).abs() < 5e-4);

        let s_lose = out.iter().find(|s| s.asset_id == 4).unwrap();
        assert_eq!(s_lose.rebalance_action, ACTION_REDUCE);
        assert!((s_lose.target_weight - 0.1295).abs() < 1e-4);
        let _ = RegimeGateConfig::default();
    }

    #[test]
    fn test_n3_suppress_direct_flips_converts_flip_to_close() {
        let a = test_asset(1, 10_000_000.0);
        let mut s1 = HashMap::new();
        s1.insert(
            1,
            CarryMomentumMetrics {
                asset_id: 1,
                symbol: a.symbol.clone(),
                annualized_vol: 0.5,
                market_beta: 1.0,
                s1_mom_score: -0.6,
                s1_carry_score: -0.2,
                w_s1_carry_mom: -0.10,
                passes_quality: true,
            },
        );
        let mut held = HashMap::new();
        held.insert(
            1,
            HeldPositionContext {
                current_weight: 0.15,
                unrealized_pnl_pct: 0.0,
                ..Default::default()
            },
        );

        let off_cfg = AllocatorConfig {
            suppress_direct_flips: false,
            ..AllocatorConfig::default()
        };
        let flip_out = allocate_multi_sleeve_weights_with_context(
            std::slice::from_ref(&a),
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &held,
            &[],
            &RegimeGateState::default(),
            &off_cfg,
        );
        assert_eq!(flip_out[0].rebalance_action, ACTION_FLIP);

        let on_cfg = AllocatorConfig {
            suppress_direct_flips: true,
            ..AllocatorConfig::default()
        };
        let close_out = allocate_multi_sleeve_weights_with_context(
            &[a],
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &held,
            &[],
            &RegimeGateState::default(),
            &on_cfg,
        );
        assert_eq!(close_out[0].rebalance_action, ACTION_CLOSE);
        assert_eq!(close_out[0].target_weight, 0.0);
        assert_eq!(close_out[0].w_s1_carry_mom, 0.0);
    }

    #[test]
    fn test_n5_net_exposure_cap_two_sided_and_one_sided_bull_expansion() {
        let mut assets = Vec::new();
        let mut s1 = HashMap::new();
        // 8 long assets at w_s1 = 0.10 (levered = 0.185 each => L = 1.48) + 2 short assets (levered = -0.185 each => S = 0.37)
        for i in 1..=10i64 {
            let a = test_asset(i, 10_000_000.0);
            assets.push(a.clone());
            let is_long = i <= 8;
            s1.insert(
                i,
                CarryMomentumMetrics {
                    asset_id: i,
                    symbol: a.symbol.clone(),
                    annualized_vol: 0.5,
                    market_beta: 1.0,
                    s1_mom_score: if is_long { 0.5 } else { -0.5 },
                    s1_carry_score: 0.0,
                    w_s1_carry_mom: if is_long { 0.10 } else { -0.10 },
                    passes_quality: true,
                },
            );
        }

        let cfg = AllocatorConfig {
            max_net_exposure_ratio_of_gross: 0.50,
            ..AllocatorConfig::default()
        };
        let out = allocate_multi_sleeve_weights_with_context(
            &assets,
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &[],
            &RegimeGateState::default(),
            &cfg,
        );
        let l_sum: f64 = out
            .iter()
            .filter(|s| s.target_weight > 0.0)
            .map(|s| s.target_weight)
            .sum();
        let s_sum: f64 = out
            .iter()
            .filter(|s| s.target_weight < 0.0)
            .map(|s| s.target_weight.abs())
            .sum();
        // G_0 = 1.48 + 0.37 = 1.85, cap = 0.50 * 1.85 = 0.925, S = 0.37 -> L' = 0.925 + 0.37 = 1.295
        assert!((s_sum - 0.37).abs() < 1e-3);
        assert!((l_sum - 1.295).abs() < 2e-3);
        assert!((l_sum - s_sum) <= 0.50 * 1.85 + 2e-3);

        // One-sided BULL_EXPANSION (S = 0.0): must NOT collapse portfolio to zero!
        let bull_assets = assets[..8].to_vec();
        let bull_gate = RegimeGateState {
            breadth_bull: 0.85,
            eligible_coin_count: 8,
            regime_label: "BULL_EXPANSION".to_string(),
            block_new_shorts: true,
            block_new_longs: false,
            ..RegimeGateState::default()
        };
        let cfg_bull = AllocatorConfig {
            max_net_exposure_ratio_of_gross: 0.60,
            ..AllocatorConfig::default()
        };
        let bull_out = allocate_multi_sleeve_weights_with_context(
            &bull_assets,
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &[],
            &bull_gate,
            &cfg_bull,
        );
        let bull_gross: f64 = bull_out.iter().map(|s| s.target_weight.abs()).sum();
        // L_0 = 1.48, S = 0 -> L' = 0.60 * 1.48 = 0.888 (not 0.0!)
        assert!(
            (bull_gross - 0.888).abs() < 2e-3,
            "One-sided BULL_EXPANSION must retain 0.60 * G_0 = 0.888 without singularity collapse, got {bull_gross}"
        );
    }

    #[test]
    fn test_wave4_effective_early_loser_threshold_and_hysteresis_scaling() {
        // 1. Unknown/zero entry_r0_pct falls back to -0.003
        assert!((effective_early_loser_threshold(0.0, 0.25, 0.009) - (-0.003)).abs() < 1e-12);
        assert!((effective_early_loser_threshold(f64::NAN, 0.25, 0.009) - (-0.003)).abs() < 1e-12);

        // 2. Tight stop (1.0% * 0.25 = 0.25%) floors at -0.003
        assert!((effective_early_loser_threshold(0.01, 0.25, 0.009) - (-0.003)).abs() < 1e-12);

        // 3. Moderate stop (2.4% * 0.25 = 0.6%) scales linearly
        assert!((effective_early_loser_threshold(0.024, 0.25, 0.009) - (-0.006)).abs() < 1e-12);

        // 4. Wide 4h stop (7.0% * 0.25 = 1.75%) caps at -0.009 (or -0.018 when max_loss_pct = 0.018)
        assert!((effective_early_loser_threshold(0.07, 0.25, 0.009) - (-0.009)).abs() < 1e-12);
        assert!((effective_early_loser_threshold(0.07, 0.25, 0.018) - (-0.0175)).abs() < 1e-12);

        // 5. Allocator respects R-scaled threshold: -0.55% noise on a 7% entry_r0 position
        //    with mild support holds (not treated as invalidated early loser), whereas -1.0% closes.
        let cfg = AllocatorConfig::default();
        let a = test_asset(77, 10_000_000.0);
        let mut s1 = HashMap::new();
        s1.insert(
            a.id,
            CarryMomentumMetrics {
                asset_id: a.id,
                symbol: a.symbol.clone(),
                annualized_vol: 0.65,
                market_beta: 1.0,
                s1_mom_score: -0.03,
                s1_carry_score: 0.15, // composite = 0.65*(-0.03)+0.35*0.15 = +0.033 > 0.025 mild support!
                w_s1_carry_mom: 0.0,
                passes_quality: true,
            },
        );
        let mut noise_held = HashMap::new();
        noise_held.insert(
            a.id,
            HeldPositionContext {
                current_weight: 0.15,
                unrealized_pnl_pct: -0.0055,
                entry_r0_pct: 0.07,
            },
        );
        let out_noise = allocate_multi_sleeve_weights_with_context(
            std::slice::from_ref(&a),
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &noise_held,
            &[],
            &RegimeGateState::default(),
            &cfg,
        );
        assert_eq!(out_noise[0].rebalance_action, ACTION_HOLD);

        let mut deep_loss_held = HashMap::new();
        deep_loss_held.insert(
            a.id,
            HeldPositionContext {
                current_weight: 0.15,
                unrealized_pnl_pct: -0.010, // <= -0.009 cap -> invalidated early loser!
                entry_r0_pct: 0.07,
            },
        );
        let out_loss = allocate_multi_sleeve_weights_with_context(
            &[a],
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &deep_loss_held,
            &[],
            &RegimeGateState::default(),
            &cfg,
        );
        assert_eq!(out_loss[0].rebalance_action, ACTION_CLOSE);
    }

    #[test]
    fn test_wave4_sparse_universe_counter_regime_confirmation() {
        let cfg = AllocatorConfig {
            signal_exit_confirm_bars: 2,
            ..AllocatorConfig::default()
        };
        let a_held_short = test_asset(1, 10_000_000.0);
        let a_flat_short = test_asset(2, 10_000_000.0);
        let a_held_long = test_asset(3, 10_000_000.0);
        let assets = vec![
            a_held_short.clone(),
            a_flat_short.clone(),
            a_held_long.clone(),
        ];

        let mut s1 = HashMap::new();
        // Neutral signals on held assets (1 & 3) -> candidate ACTION_CLOSE; flat asset (2) wants SHORT -> BLOCKED
        s1.insert(
            1,
            CarryMomentumMetrics {
                asset_id: 1,
                symbol: a_held_short.symbol.clone(),
                annualized_vol: 0.5,
                market_beta: 1.0,
                s1_mom_score: 0.0,
                s1_carry_score: 0.0,
                w_s1_carry_mom: 0.0,
                passes_quality: true,
            },
        );
        s1.insert(
            2,
            CarryMomentumMetrics {
                asset_id: 2,
                symbol: a_flat_short.symbol.clone(),
                annualized_vol: 0.5,
                market_beta: 1.0,
                s1_mom_score: -0.5,
                s1_carry_score: -0.2,
                w_s1_carry_mom: -0.08,
                passes_quality: true,
            },
        );
        s1.insert(
            3,
            CarryMomentumMetrics {
                asset_id: 3,
                symbol: a_held_long.symbol.clone(),
                annualized_vol: 0.5,
                market_beta: 1.0,
                s1_mom_score: 0.0,
                s1_carry_score: 0.0,
                w_s1_carry_mom: 0.0,
                passes_quality: true,
            },
        );

        let mut held = HashMap::new();
        held.insert(
            1,
            HeldPositionContext {
                current_weight: -0.12,
                unrealized_pnl_pct: 0.0,
                ..Default::default()
            },
        );
        held.insert(
            3,
            HeldPositionContext {
                current_weight: 0.12,
                unrealized_pnl_pct: 0.0,
                ..Default::default()
            },
        );

        // 1. Marginal 5/7 = 0.7143 (< 10 coins, < 0.80) bull breadth:
        //    Blocks new counter-regime SHORT (asset 2 -> ACTION_REGIME_BLOCKED),
        //    while held counter-regime SHORT (asset 1) waits for confirm_bars = 2!
        let gate_5_of_7 = RegimeGateState {
            breadth_bull: 5.0 / 7.0,
            eligible_coin_count: 7,
            regime_label: "BULL_EXPANSION".to_string(),
            regime: MarketRegime::BullExpansion,
            block_new_shorts: true,
            block_new_longs: false,
            ..RegimeGateState::default()
        };
        let mut confirm_map = HashMap::new();
        let mut bar_ts = HashMap::from([(1, 1000), (2, 1000), (3, 1000)]);
        let bar1 = allocate_multi_sleeve_weights_with_exit_confirm(
            &assets,
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &held,
            &[],
            &gate_5_of_7,
            &cfg,
            &bar_ts,
            &mut confirm_map,
        );
        assert_eq!(
            bar1.iter()
                .find(|s| s.asset_id == 2)
                .unwrap()
                .rebalance_action,
            ACTION_REGIME_BLOCKED
        );
        assert_eq!(
            bar1.iter()
                .find(|s| s.asset_id == 1)
                .unwrap()
                .rebalance_action,
            ACTION_HOLD,
            "Marginal 5/7=0.7143 counter-regime close must wait for bar 2 confirmation"
        );

        // Bar 2 at newer timestamp confirms exit -> ACTION_CLOSE!
        bar_ts.insert(1, 2000);
        bar_ts.insert(3, 2000);
        let bar2 = allocate_multi_sleeve_weights_with_exit_confirm(
            &assets,
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &held,
            &[],
            &gate_5_of_7,
            &cfg,
            &bar_ts,
            &mut confirm_map,
        );
        assert_eq!(
            bar2.iter()
                .find(|s| s.asset_id == 1)
                .unwrap()
                .rebalance_action,
            ACTION_CLOSE
        );

        // 2. Extreme breadth <= 0.20 (e.g. 1/7 = 0.1429) on < 10 coins STILL bypasses bar 1 immediately!
        let gate_bear_extreme = RegimeGateState {
            breadth_bull: 1.0 / 7.0,
            eligible_coin_count: 7,
            regime_label: "BEAR_CONTRACTION".to_string(),
            regime: MarketRegime::BearContraction,
            block_new_shorts: false,
            block_new_longs: true,
            ..RegimeGateState::default()
        };
        let mut confirm_extreme = HashMap::new();
        let bar1_extreme = allocate_multi_sleeve_weights_with_exit_confirm(
            &assets,
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &held,
            &[],
            &gate_bear_extreme,
            &cfg,
            &bar_ts,
            &mut confirm_extreme,
        );
        assert_eq!(
            bar1_extreme
                .iter()
                .find(|s| s.asset_id == 3)
                .unwrap()
                .rebalance_action,
            ACTION_CLOSE,
            "Extreme <= 0.20 counter-regime held LONG must still bypass confirm_bars on bar 1"
        );
    }

    #[test]
    fn test_w2_breakeven_plus_reduce_hysteresis_and_unlisted_working_exposure_cap() {
        let doge = test_asset(1, 10_000_000.0);
        let cfg = AllocatorConfig::default(); // 1.5 + 4.5 + 8.0 = 14.0 bps = 0.0014 required edge
        assert!(
            (min_reduce_breakeven_edge_pct(1.5, 4.5, 8.0) - 0.0014).abs() < 1e-9,
            "Default reduce breakeven+edge must equal 14 bps"
        );

        let mut s1 = HashMap::new();
        // Desired scaled weight = 0.05 * 1.85 = 0.0925 (< current_weight 0.20 by > min_delta)
        s1.insert(
            1,
            CarryMomentumMetrics {
                asset_id: 1,
                symbol: doge.symbol.clone(),
                annualized_vol: 0.5,
                market_beta: 1.0,
                s1_mom_score: 0.25,
                s1_carry_score: 0.05,
                w_s1_carry_mom: 0.05,
                passes_quality: true,
            },
        );

        // 1. Sub-breakeven trim (-2.65 bps, DOGE 0.056656 -> 0.056641) is suppressed -> ACTION_HOLD
        let mut held_sub = HashMap::new();
        held_sub.insert(
            1,
            HeldPositionContext {
                current_weight: 0.20,
                unrealized_pnl_pct: -0.000265,
                entry_r0_pct: 0.04,
            },
        );
        let out_sub = allocate_multi_sleeve_weights_with_context(
            std::slice::from_ref(&doge),
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &held_sub,
            &[],
            &RegimeGateState::default(),
            &cfg,
        );
        assert_eq!(out_sub[0].rebalance_action, ACTION_HOLD);
        assert!((out_sub[0].target_weight - 0.20).abs() < 1e-4);

        // 2. Profitable trim (+20 bps >= 14 bps) executes ACTION_REDUCE cleanly
        let mut held_prof = HashMap::new();
        held_prof.insert(
            1,
            HeldPositionContext {
                current_weight: 0.20,
                unrealized_pnl_pct: 0.0020,
                entry_r0_pct: 0.04,
            },
        );
        let out_prof = allocate_multi_sleeve_weights_with_context(
            std::slice::from_ref(&doge),
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &held_prof,
            &[],
            &RegimeGateState::default(),
            &cfg,
        );
        assert_eq!(out_prof[0].rebalance_action, ACTION_REDUCE);
        assert!((out_prof[0].target_weight - 0.0925).abs() < 1e-4);

        // 3. Unlisted resting WORKING / OPEN exposure in held_contexts reduces remaining max_gross
        let cfg_tight_gross = AllocatorConfig {
            max_gross_leverage_x: 0.30,
            ..AllocatorConfig::default()
        };
        let mut held_working = HashMap::new();
        held_working.insert(
            999, // resting WORKING order on unlisted asset taking 0.22 weight
            HeldPositionContext {
                current_weight: 0.22,
                unrealized_pnl_pct: 0.0,
                entry_r0_pct: 0.04,
            },
        );
        let out_capped = allocate_multi_sleeve_weights_with_context(
            std::slice::from_ref(&doge),
            &s1,
            &HashMap::new(),
            &HashMap::new(),
            &held_working,
            &[],
            &RegimeGateState::default(),
            &cfg_tight_gross,
        );
        assert!(
            out_capped[0].target_weight <= 0.0801,
            "Remaining gross headroom must deduct unlisted WORKING exposure (0.30 - 0.22 = 0.08), got {}",
            out_capped[0].target_weight
        );
    }
}
