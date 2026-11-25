//! Unified Multi-Sleeve Signal Engine (`aegis-signals::engine`).
//!
//! Orchestrates Sleeve 1 (`carry_momentum`), Sleeve 2 (`trend_breakout`), Sleeve 3 (`event_reversion`),
//! Cross-Sectional Market Breadth (`breadth`), and Risk-Tier Portfolio Allocation (`allocator`).

use crate::allocator::{
    allocate_multi_sleeve_weights_with_exit_confirm, AllocatorConfig, HeldPositionContext,
    RiskTierLeverageExt, SleeveSignalInput,
};
use crate::breadth::{compute_regime_gate_state, MarketBreadthSnapshot};
use crate::carry_momentum::evaluate_sleeve_s1_with_features;
use crate::event_reversion::{evaluate_sleeve_s3_with_features, TRIGGER_CAPITULATION_FLUSH};
use crate::features::{BarCloseCache, CycleFeatureCache};
use crate::trend_breakout::evaluate_sleeve_s2_with_features;
use aegis_core::{
    round_dp, ConfigMap, CooldownAssetView, ExchangeAsset, OhlcvBar, OrderSide, PositionRecord,
    PositionStatus, RiskTier,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

/// Full evaluation output of a single 3s/1s multi-sleeve signal cycle across the universe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalCycleOutput {
    pub signals: Vec<SleeveSignalInput>,
    pub breadth: MarketBreadthSnapshot,
    pub risk_tier: RiskTier,
    pub leverage_multiplier: f64,
    pub s1_gross_weight: f64,
    pub s2_gross_weight: f64,
    pub s3_gross_weight: f64,
    pub total_gross_weight: f64,
    pub total_net_weight: f64,
}

/// Primary Aegis Quant v2.0 Multi-Sleeve Signal Engine.
#[derive(Debug, Default)]
pub struct SignalEngine {
    pub config: AllocatorConfig,
    bar_cache: Mutex<BarCloseCache>,
}

impl Clone for SignalEngine {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            bar_cache: Mutex::new(BarCloseCache::default()),
        }
    }
}

impl SignalEngine {
    pub fn new(config: AllocatorConfig) -> Self {
        Self {
            config,
            bar_cache: Mutex::new(BarCloseCache::default()),
        }
    }

    pub fn from_config_map(cfg: &ConfigMap) -> Self {
        Self::new(AllocatorConfig::from_config_map(cfg))
    }

    /// Evaluates all three sleeves (S1 Carry-Momentum, S2 Trend-Breakout, S3 Event-Reversion),
    /// cross-sectional breadth, cooldown blocking, and risk-tier allocation across `assets`.
    pub fn evaluate_universe<B: AsRef<[OhlcvBar]>>(
        &self,
        assets: &[ExchangeAsset],
        bars_by_asset: &HashMap<i64, B>,
        current_weights: &HashMap<i64, f64>,
        active_cooldowns: &[CooldownAssetView],
    ) -> SignalCycleOutput {
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
        self.evaluate_universe_with_context(assets, bars_by_asset, &held_contexts, active_cooldowns)
    }

    /// Evaluates the universe using explicit per-asset [`HeldPositionContext`] (supporting
    /// asymmetric loser invalidation / winner protection hysteresis).
    pub fn evaluate_universe_with_context<B: AsRef<[OhlcvBar]>>(
        &self,
        assets: &[ExchangeAsset],
        bars_by_asset: &HashMap<i64, B>,
        held_contexts: &HashMap<i64, HeldPositionContext>,
        active_cooldowns: &[CooldownAssetView],
    ) -> SignalCycleOutput {
        let mut guard = self
            .bar_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.evaluate_universe_with_cache(
            assets,
            bars_by_asset,
            held_contexts,
            active_cooldowns,
            &mut guard,
        )
    }

    /// Evaluates the universe using an externally owned [`BarCloseCache`] (lock-free path for backtests).
    pub fn evaluate_universe_with_cache<B: AsRef<[OhlcvBar]>>(
        &self,
        assets: &[ExchangeAsset],
        bars_by_asset: &HashMap<i64, B>,
        held_contexts: &HashMap<i64, HeldPositionContext>,
        active_cooldowns: &[CooldownAssetView],
        bar_cache: &mut BarCloseCache,
    ) -> SignalCycleOutput {
        let features = CycleFeatureCache::build_with_options(
            assets,
            bars_by_asset,
            bar_cache,
            self.config.wall_clock_normalize_indicators,
        );
        let gate_cfg = self.config.regime_gate_config();
        let mut regime_gate = features.initial_regime_gate(&gate_cfg);
        let sleeve_top_k = self.config.max_open_positions.max(8);

        let s1_map = evaluate_sleeve_s1_with_features(
            assets,
            bars_by_asset,
            &features,
            &regime_gate,
            self.config.s1_carry_mom_budget,
            sleeve_top_k,
        );
        let s2_map = evaluate_sleeve_s2_with_features(
            assets,
            bars_by_asset,
            &features,
            &regime_gate,
            features.prelim_breadth_bull,
            self.config.s2_trend_breakout_budget,
            sleeve_top_k,
        );
        let s3_map = evaluate_sleeve_s3_with_features(
            assets,
            bars_by_asset,
            &features,
            &regime_gate,
            self.config.s3_event_reversion_budget,
            self.config.s3_strict_admission_enabled,
        );

        let capitulation_count = s3_map
            .values()
            .filter(|m| m.event_trigger.as_deref() == Some(TRIGGER_CAPITULATION_FLUSH))
            .count();
        regime_gate = compute_regime_gate_state(
            features.prelim_breadth_bull,
            features.eligible_coin_count,
            features.avg_eligible_funding,
            capitulation_count,
            &gate_cfg,
        );

        let bar_ts_by_asset: HashMap<i64, i64> = features
            .per_asset
            .iter()
            .map(|(&aid, feat)| (aid, feat.last_bar_ts_ms))
            .collect();

        let signals = allocate_multi_sleeve_weights_with_exit_confirm(
            assets,
            &s1_map,
            &s2_map,
            &s3_map,
            held_contexts,
            active_cooldowns,
            &regime_gate,
            &self.config,
            &bar_ts_by_asset,
            &mut bar_cache.exit_confirm_by_asset,
        );

        let beta_by_asset: HashMap<i64, f64> = s1_map
            .iter()
            .map(|(&aid, m)| (aid, m.market_beta))
            .collect();
        let current_weights: HashMap<i64, f64> = held_contexts
            .iter()
            .map(|(&aid, ctx)| (aid, ctx.current_weight))
            .collect();

        let breadth = features.finalize_breadth_snapshot(
            &beta_by_asset,
            &current_weights,
            capitulation_count,
        );

        let lev_mult = self.config.effective_leverage_multiplier();
        let mut s1_gross = 0.0;
        let mut s2_gross = 0.0;
        let mut s3_gross = 0.0;
        let mut total_gross = 0.0;
        let mut total_net = 0.0;

        for sig in &signals {
            s1_gross += (sig.w_s1_carry_mom * lev_mult).abs();
            s2_gross += (sig.w_s2_trend_breakout * lev_mult).abs();
            s3_gross += (sig.w_s3_event_reversion * lev_mult).abs();
            total_gross += sig.target_weight.abs();
            total_net += sig.target_weight;
        }

        SignalCycleOutput {
            signals,
            breadth,
            risk_tier: self.config.risk_tier,
            leverage_multiplier: lev_mult,
            s1_gross_weight: round_dp(s1_gross, 4),
            s2_gross_weight: round_dp(s2_gross, 4),
            s3_gross_weight: round_dp(s3_gross, 4),
            total_gross_weight: round_dp(total_gross, 4),
            total_net_weight: round_dp(total_net, 4),
        }
    }

    /// Convenience entrypoint that derives `current_weights` and `unrealized_pnl_pct` from active
    /// (`WORKING` and `OPEN`) positions and true portfolio equity before running `evaluate_universe_with_context`.
    pub fn evaluate_universe_with_positions<B: AsRef<[OhlcvBar]>>(
        &self,
        assets: &[ExchangeAsset],
        bars_by_asset: &HashMap<i64, B>,
        active_positions: &[PositionRecord],
        active_cooldowns: &[CooldownAssetView],
        total_equity_usdc: f64,
    ) -> SignalCycleOutput {
        let eq_valid = total_equity_usdc.is_finite() && total_equity_usdc > 1e-6;
        let mut weight_by_asset: HashMap<i64, f64> = HashMap::new();
        let mut open_pnl_acc: HashMap<i64, (f64, f64)> = HashMap::new();

        for pos in active_positions {
            let status = PositionStatus::parse(&pos.status);
            if !matches!(status, PositionStatus::Working | PositionStatus::Open) {
                continue;
            }
            let Ok(side) = OrderSide::parse(&pos.side) else {
                tracing::error!(
                    position_id = pos.id,
                    side = %pos.side,
                    "Skipping active position with invalid OrderSide in evaluate_universe_with_positions"
                );
                continue;
            };
            let notional = if status == PositionStatus::Open {
                if pos.current_price.is_finite()
                    && pos.current_price > 0.0
                    && pos.filled_quantity.is_finite()
                    && pos.filled_quantity > 0.0
                {
                    pos.current_price * pos.filled_quantity
                } else {
                    pos.committed_notional_usdc
                }
            } else {
                pos.entry_price * pos.requested_quantity
            };
            let signed_w = if eq_valid && notional.is_finite() && notional > 0.0 {
                (notional / total_equity_usdc) * side.sign()
            } else {
                pos.portfolio_weight.abs() * side.sign()
            };
            *weight_by_asset.entry(pos.asset_id).or_insert(0.0) += signed_w;

            if status == PositionStatus::Open {
                let entry_notional = if pos.entry_price.is_finite()
                    && pos.entry_price > 0.0
                    && pos.filled_quantity.is_finite()
                    && pos.filled_quantity > 0.0
                {
                    pos.entry_price * pos.filled_quantity
                } else if pos.committed_notional_usdc.is_finite()
                    && pos.committed_notional_usdc > 0.0
                {
                    pos.committed_notional_usdc
                } else {
                    0.0
                };
                if entry_notional > 1e-6 {
                    let upnl = if pos.current_price.is_finite()
                        && pos.current_price > 0.0
                        && pos.entry_price.is_finite()
                        && pos.entry_price > 0.0
                        && pos.filled_quantity.is_finite()
                        && pos.filled_quantity > 0.0
                        && (pos.current_price - pos.entry_price).abs() > 1e-12
                    {
                        (pos.current_price - pos.entry_price) * pos.filled_quantity * side.sign()
                    } else if pos.unrealized_pnl_usdc.is_finite() {
                        pos.unrealized_pnl_usdc
                    } else {
                        0.0
                    };
                    let entry = open_pnl_acc.entry(pos.asset_id).or_insert((0.0, 0.0));
                    entry.0 += upnl;
                    entry.1 += entry_notional;
                }
            }
        }

        let mut held_contexts = HashMap::with_capacity(weight_by_asset.len());
        for (asset_id, current_weight) in weight_by_asset {
            let unrealized_pnl_pct = open_pnl_acc
                .get(&asset_id)
                .filter(|(_, denom)| *denom > 1e-6)
                .map(|(upnl, denom)| upnl / denom)
                .unwrap_or(0.0);
            held_contexts.insert(
                asset_id,
                HeldPositionContext {
                    current_weight,
                    unrealized_pnl_pct,
                },
            );
        }

        self.evaluate_universe_with_context(assets, bars_by_asset, &held_contexts, active_cooldowns)
    }
}

/// Helper to confirm `RiskTier::Extreme.default_leverage_multiplier() == 1.85`.
pub fn default_extreme_leverage_multiplier() -> f64 {
    RiskTier::Extreme.default_leverage_multiplier()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::{
        classify_rebalance_action, ACTION_CLOSE, ACTION_COOLDOWN_BLOCKED, ACTION_FLIP, ACTION_HOLD,
        ACTION_INCREASE, ACTION_OPEN_LONG, ACTION_OPEN_SHORT, ACTION_REDUCE,
    };
    use crate::event_reversion::{TRIGGER_CAPITULATION_FLUSH, TRIGGER_FUNDING_SQUEEZE};
    use crate::trend_breakout::TRIGGER_LIVE_BREAKOUT_SURGE;

    fn sample_asset(
        id: i64,
        coin: &str,
        mark_price: f64,
        prev_day_price: f64,
        funding_rate_hourly: f64,
        day_volume_usdc: f64,
    ) -> ExchangeAsset {
        let tick_size = if mark_price < 1.0 {
            0.0001
        } else if mark_price < 10.0 {
            0.001
        } else {
            0.01
        };
        ExchangeAsset {
            id,
            symbol: format!("{coin}-USDC"),
            coin: coin.to_string(),
            asset_index: id as u32,
            sz_decimals: 2,
            tick_size,
            lot_step_size: 0.01,
            max_leverage: 20.0,
            mark_price,
            mid_price: mark_price,
            oracle_price: mark_price,
            funding_rate_hourly,
            day_volume_usdc,
            open_interest_usdc: day_volume_usdc * 0.4,
            prev_day_price,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_000_000,
        }
    }

    fn build_trend_bars(
        asset_id: i64,
        start_px: f64,
        step_pct: f64,
        count: usize,
        last_vol_mult: f64,
    ) -> Vec<OhlcvBar> {
        let mut bars = Vec::with_capacity(count);
        let mut px = start_px;
        for i in 0..count {
            let open = px;
            px *= 1.0 + step_pct;
            let close = px;
            let high = open.max(close) * 1.005;
            let low = open.min(close) * 0.995;
            let volume = if i + 1 == count {
                10_000.0 * last_vol_mult
            } else {
                10_000.0
            };
            bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_000_000_000 + (i as i64) * 14_400_000,
                open,
                high,
                low,
                close,
                volume,
                funding_rate: 0.00001,
                created_at_ms: 1_700_000_000_000,
                updated_at_ms: 1_700_000_000_000,
            });
        }
        bars
    }

    #[test]
    fn test_extreme_risk_tier_leverage_and_multi_sleeve_allocation() {
        assert!((default_extreme_leverage_multiplier() - 1.85).abs() < 1e-9);

        let btc_bars = build_trend_bars(1, 90_000.0, 0.006, 25, 1.2);
        let sol_bars = build_trend_bars(2, 160.0, 0.015, 25, 2.4);
        let eth_bars = build_trend_bars(3, 3_200.0, -0.012, 25, 2.1);

        let btc = sample_asset(
            1,
            "BTC",
            btc_bars.last().unwrap().close,
            90_000.0,
            0.00001,
            50_000_000.0,
        );
        let sol = sample_asset(
            2,
            "SOL",
            sol_bars.last().unwrap().close * 1.02,
            160.0,
            -0.00015,
            25_000_000.0,
        );
        let eth = sample_asset(
            3,
            "ETH",
            eth_bars.last().unwrap().close * 0.98,
            3_200.0,
            0.00035,
            30_000_000.0,
        );

        let assets = vec![btc, sol, eth];
        let mut bars_by_asset = HashMap::new();
        bars_by_asset.insert(1, btc_bars);
        bars_by_asset.insert(2, sol_bars);
        bars_by_asset.insert(3, eth_bars);

        let engine = SignalEngine::default();
        let output = engine.evaluate_universe(&assets, &bars_by_asset, &HashMap::new(), &[]);

        assert_eq!(output.risk_tier, RiskTier::Extreme);
        assert!((output.leverage_multiplier - 1.85).abs() < 1e-9);
        assert_eq!(output.signals.len(), 3);
        assert!(output.total_gross_weight <= engine.config.max_gross_leverage_x + 1e-6);

        let sol_sig = output
            .signals
            .iter()
            .find(|s| s.symbol == "SOL-USDC")
            .unwrap();
        assert!(sol_sig.s1_mom_score > 0.0);
        assert_eq!(sol_sig.s2_trend_alignment, 1);
        assert!(sol_sig.target_weight > 0.0);
        assert_eq!(sol_sig.rebalance_action, ACTION_OPEN_LONG);
        assert!(
            sol_sig.event_trigger.as_deref() == Some(TRIGGER_LIVE_BREAKOUT_SURGE)
                || sol_sig.event_trigger.as_deref() == Some(TRIGGER_FUNDING_SQUEEZE)
                || sol_sig.event_trigger.as_deref() == Some(TRIGGER_CAPITULATION_FLUSH)
        );

        let eth_sig = output
            .signals
            .iter()
            .find(|s| s.symbol == "ETH-USDC")
            .unwrap();
        assert!(eth_sig.s1_mom_score < 0.0);
        assert_eq!(eth_sig.s2_trend_alignment, -1);
        assert!(eth_sig.target_weight < 0.0);
        assert_eq!(eth_sig.rebalance_action, ACTION_OPEN_SHORT);
    }

    #[test]
    fn test_cooldown_blocks_new_entries() {
        let sol_bars = build_trend_bars(2, 150.0, 0.015, 20, 2.2);
        let sol = sample_asset(
            2,
            "SOL",
            sol_bars.last().unwrap().close * 1.01,
            150.0,
            -0.0001,
            20_000_000.0,
        );
        let mut bars_by_asset = HashMap::new();
        bars_by_asset.insert(2, sol_bars);

        let cooldown = CooldownAssetView {
            cooldown_id: 10,
            asset_id: 2,
            symbol: "SOL-USDC".to_string(),
            coin: "SOL".to_string(),
            cooldown_type: "STOP_OUT".to_string(),
            reason: "Protective stop triggered".to_string(),
            triggered_at_ms: 1_700_000_000_000,
            expires_at_ms: 1_700_001_800_000,
            remaining_seconds: 1800,
            updated_at_ms: 1_700_000_000_000,
        };

        let engine = SignalEngine::default();
        let output = engine.evaluate_universe(&[sol], &bars_by_asset, &HashMap::new(), &[cooldown]);

        let sol_sig = &output.signals[0];
        assert_eq!(sol_sig.target_weight, 0.0);
        assert_eq!(sol_sig.rebalance_action, ACTION_COOLDOWN_BLOCKED);
    }

    #[test]
    fn test_all_rebalance_action_classifications() {
        let min_delta = 0.025;
        assert_eq!(
            classify_rebalance_action(0.0, 0.15, 0.15, false, min_delta),
            ACTION_OPEN_LONG
        );
        assert_eq!(
            classify_rebalance_action(0.0, -0.15, -0.15, false, min_delta),
            ACTION_OPEN_SHORT
        );
        assert_eq!(
            classify_rebalance_action(0.0, 0.01, 0.01, false, min_delta),
            ACTION_HOLD
        );
        assert_eq!(
            classify_rebalance_action(0.10, 0.22, 0.22, false, min_delta),
            ACTION_INCREASE
        );
        assert_eq!(
            classify_rebalance_action(0.22, 0.10, 0.10, false, min_delta),
            ACTION_REDUCE
        );
        assert_eq!(
            classify_rebalance_action(0.18, 0.0, 0.0, false, min_delta),
            ACTION_CLOSE
        );
        assert_eq!(
            classify_rebalance_action(0.18, -0.15, -0.15, false, min_delta),
            ACTION_FLIP
        );
        assert_eq!(
            classify_rebalance_action(0.0, 0.0, 0.15, true, min_delta),
            ACTION_COOLDOWN_BLOCKED
        );
    }

    #[test]
    fn test_bottom_range_asset_is_not_shorted() {
        // Simulate an asset (like FET-USDC) that fell over the lookback window and is now
        // bouncing slightly off the bottom of its range without breaking down below min_low.
        let mut fet_bars = build_trend_bars(10, 0.25, -0.014, 30, 1.0);
        let last_close = fet_bars.last().unwrap().close;
        // Make the final bar stabilize slightly higher off the low so it is not actively crashing
        let prev_close = last_close;
        let bounce_close = prev_close * 1.006;
        let mut last_bar = fet_bars.last().unwrap().clone();
        last_bar.id = 31;
        last_bar.timestamp_ms += 14_400_000;
        last_bar.open = prev_close;
        last_bar.low = prev_close * 0.996;
        last_bar.close = bounce_close;
        last_bar.high = bounce_close * 1.002;
        fet_bars.push(last_bar);

        let fet = sample_asset(10, "FET", bounce_close, 0.25, 0.0001, 5_000_000.0);
        let mut bars_by_asset = HashMap::new();
        bars_by_asset.insert(10, fet_bars);

        let engine = SignalEngine::default();
        let output = engine.evaluate_universe(&[fet], &bars_by_asset, &HashMap::new(), &[]);
        let fet_sig = &output.signals[0];

        assert!(
            fet_sig.s1_mom_score >= 0.0,
            "S1 must block negative short momentum at range bottom without fake sign inversion, got {}",
            fet_sig.s1_mom_score
        );
        assert!(
            fet_sig.w_s1_carry_mom >= 0.0,
            "S1 carry-mom weight must not be negative at bouncing range bottom, got {}",
            fet_sig.w_s1_carry_mom
        );
        assert!(
            fet_sig.w_s3_event_reversion > 0.0,
            "S3 bottom reversion weight should be positive, got {}",
            fet_sig.w_s3_event_reversion
        );
        assert_ne!(
            fet_sig.s2_trend_alignment, -1,
            "S2 should block bearish alignment at range bottom"
        );
        assert!(
            fet_sig.target_weight > 0.0,
            "Bottom-range bouncing asset should receive positive LONG target weight, got {}",
            fet_sig.target_weight
        );
        assert_eq!(
            fet_sig.rebalance_action, ACTION_OPEN_LONG,
            "Range-bottom bouncing asset should receive OPEN_LONG action"
        );
    }

    #[test]
    fn test_breadth_filters_ineligible_and_beta_uses_held_weights() {
        let btc_bars = build_trend_bars(1, 90_000.0, 0.01, 20, 1.0);
        let btc = sample_asset(
            1,
            "BTC",
            btc_bars.last().unwrap().close,
            90_000.0,
            0.0,
            50_000_000.0,
        );

        let mut dead = sample_asset(2, "DEAD", 1.0, 1.0, 0.0, 100.0);
        dead.is_core_eligible = false;
        dead.is_event_eligible = false;
        let dead_bars = build_trend_bars(2, 1.0, -0.02, 20, 1.0);

        let mut bars_by_asset = HashMap::new();
        bars_by_asset.insert(1, btc_bars);
        bars_by_asset.insert(2, dead_bars);

        let engine = SignalEngine::default();
        // Flat current_weights -> market_beta_exposure must be 0.0 (not target_weights beta), and DEAD excluded from breadth
        let flat_out = engine.evaluate_universe(
            &[btc.clone(), dead.clone()],
            &bars_by_asset,
            &HashMap::new(),
            &[],
        );
        assert_eq!(flat_out.breadth.eligible_coin_count, 1);
        assert!((flat_out.breadth.market_breadth_bull - 1.0).abs() < 1e-6);
        assert_eq!(flat_out.breadth.market_beta_exposure, 0.0);

        // With BTC held at +0.25 -> market_beta_exposure = 0.25 * 1.0 = 0.25
        let mut held_weights = HashMap::new();
        held_weights.insert(1, 0.25);
        let held_out = engine.evaluate_universe(&[btc, dead], &bars_by_asset, &held_weights, &[]);
        assert!((held_out.breadth.market_beta_exposure - 0.25).abs() < 1e-4);
    }

    #[test]
    fn test_held_dead_coin_failing_bar_liveness_emits_close_end_to_end() {
        let btc_bars = build_trend_bars(1, 90_000.0, 0.008, 25, 1.5);
        let btc = sample_asset(
            1,
            "BTC",
            btc_bars.last().unwrap().close,
            90_000.0,
            0.00001,
            50_000_000.0,
        );

        // EIGEN rose earlier then completely froze for the trailing 12 bars (0/12 active),
        // while still marked eligible with $8k 24h volume and negative funding carry.
        let mut eigen_bars = build_trend_bars(2, 1.80, 0.012, 12, 1.0);
        let frozen_px = eigen_bars.last().unwrap().close;
        let last_ts = eigen_bars.last().unwrap().timestamp_ms;
        for i in 0..12 {
            eigen_bars.push(OhlcvBar {
                id: (13 + i) as i64,
                asset_id: 2,
                timeframe: "1h".to_string(),
                timestamp_ms: last_ts + ((i + 1) as i64) * 3_600_000,
                open: frozen_px,
                high: frozen_px,
                low: frozen_px,
                close: frozen_px,
                volume: 0.0,
                funding_rate: -0.0006,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        let eigen = sample_asset(2, "EIGEN", frozen_px, 1.80, -0.0006, 8_000.0);

        let mut bars_by_asset = HashMap::new();
        bars_by_asset.insert(1, btc_bars);
        bars_by_asset.insert(2, eigen_bars);

        let mut held = HashMap::new();
        held.insert(2, 0.12);

        let engine = SignalEngine::default();
        let out = engine.evaluate_universe(&[btc, eigen], &bars_by_asset, &held, &[]);
        let eigen_sig = out
            .signals
            .iter()
            .find(|s| s.symbol == "EIGEN-USDC")
            .unwrap();
        assert_eq!(eigen_sig.w_s1_carry_mom, 0.0);
        assert_eq!(eigen_sig.w_s2_trend_breakout, 0.0);
        assert_eq!(eigen_sig.w_s3_event_reversion, 0.0);
        assert_eq!(eigen_sig.event_trigger, None);
        assert_eq!(eigen_sig.target_weight, 0.0);
        assert_eq!(
            eigen_sig.rebalance_action, ACTION_CLOSE,
            "Held dead coin failing bar-liveness must emit ACTION_CLOSE, never trapped in HOLD"
        );
    }

    #[test]
    fn test_r14_h7_40_equal_score_assets_respect_budget_and_preserve_s3() {
        use crate::allocator::ACTION_REGIME_BLOCKED;
        use crate::carry_momentum::evaluate_sleeve_s1;
        use crate::event_reversion::evaluate_sleeve_s3;
        use crate::trend_breakout::evaluate_sleeve_s2;
        let _ = ACTION_REGIME_BLOCKED;

        // 40 equal-score assets with distinct day_volume_usdc: 1..=40
        let mut assets = Vec::with_capacity(41);
        let mut bars_by_asset = HashMap::new();
        for id in 1..=40i64 {
            let mut bars = build_trend_bars(id, 100.0, 0.012, 26, 2.0);
            let close = bars.last().unwrap().close;
            let mut a = sample_asset(
                id,
                &format!("C{id}"),
                close * 1.01,
                100.0,
                -0.00015,
                (id as f64) * 1_000_000.0,
            );
            a.tick_size = 0.01;
            bars.last_mut().unwrap().volume *= 2.0;
            bars_by_asset.insert(id, bars);
            assets.push(a);
        }

        // Per-sleeve budget invariance: S1 (0.45), S2 (0.40) -> strictly 8 non-zero top-volume IDs (33..=40)
        let s1 = evaluate_sleeve_s1(&assets, &bars_by_asset, &[], 0.45);
        let s1_nz: Vec<i64> = s1
            .values()
            .filter(|m| m.w_s1_carry_mom.abs() > 1e-6)
            .map(|m| m.asset_id)
            .collect();
        let s1_sum: f64 = s1.values().map(|m| m.w_s1_carry_mom.abs()).sum();
        assert_eq!(s1_nz.len(), 8);
        assert!((0.45 - 0.001..=0.45 + 1e-6).contains(&s1_sum));
        for id in 33..=40 {
            assert!(s1_nz.contains(&id), "S1 must pick top-8 volume ID {id}");
        }

        let s2 = evaluate_sleeve_s2(&assets, &bars_by_asset, 0.55, 0.40);
        let s2_nz: Vec<i64> = s2
            .values()
            .filter(|m| m.w_s2_trend_breakout.abs() > 1e-6)
            .map(|m| m.asset_id)
            .collect();
        let s2_sum: f64 = s2.values().map(|m| m.w_s2_trend_breakout.abs()).sum();
        assert_eq!(s2_nz.len(), 8);
        assert!((0.40 - 0.001..=0.40 + 1e-6).contains(&s2_sum));
        for id in 33..=40 {
            assert!(s2_nz.contains(&id), "S2 must pick top-8 volume ID {id}");
        }

        // S3 with 40 identical capitulation assets (budget 0.10) -> strictly 8 non-zero IDs (33..=40)
        let mut s3_assets = Vec::with_capacity(40);
        let mut s3_bars_map = HashMap::new();
        for id in 1..=40i64 {
            let mut bars = build_trend_bars(id, 100.0, -0.015, 26, 1.0);
            let prev = bars.last().unwrap().close;
            let flush_px = prev * 0.88;
            let mut b = bars.last().unwrap().clone();
            b.id = 27;
            b.timestamp_ms += 14_400_000;
            b.open = prev;
            b.low = flush_px;
            b.close = flush_px;
            b.high = prev;
            bars.push(b);
            let mut a = sample_asset(
                id,
                &format!("F{id}"),
                flush_px,
                100.0,
                -0.0002,
                (id as f64) * 1_000_000.0,
            );
            a.tick_size = 0.01;
            s3_bars_map.insert(id, bars);
            s3_assets.push(a);
        }
        let s3 = evaluate_sleeve_s3(&s3_assets, &s3_bars_map, 0.10);
        let s3_nz: Vec<i64> = s3
            .values()
            .filter(|m| m.w_s3_event_reversion.abs() > 1e-6)
            .map(|m| m.asset_id)
            .collect();
        let s3_sum: f64 = s3.values().map(|m| m.w_s3_event_reversion.abs()).sum();
        assert_eq!(s3_nz.len(), 8);
        assert!((0.10 - 0.001..=0.10 + 1e-6).contains(&s3_sum));

        // End-to-end SignalEngine with 40 equal S1/S2 assets + 1 S3 capitulation asset:
        // S3 asset MUST retain positive w_s3_event_reversion and NOT be crowded out!
        let mut cap_bars = build_trend_bars(99, 100.0, -0.015, 26, 1.0);
        let c_prev = cap_bars.last().unwrap().close;
        let c_flush = c_prev * 0.88;
        let mut c_last = cap_bars.last().unwrap().clone();
        c_last.id = 27;
        c_last.timestamp_ms += 14_400_000;
        c_last.open = c_prev;
        c_last.low = c_flush;
        c_last.close = c_flush;
        c_last.high = c_prev;
        cap_bars.push(c_last);
        let cap_asset = sample_asset(99, "CAP", c_flush, 100.0, -0.0003, 500_000.0);
        bars_by_asset.insert(99, cap_bars);
        assets.push(cap_asset);

        let engine = SignalEngine::default();
        let out = engine.evaluate_universe(&assets, &bars_by_asset, &HashMap::new(), &[]);
        let cap_sig = out.signals.iter().find(|s| s.asset_id == 99).unwrap();
        assert!(
            cap_sig.w_s3_event_reversion > 0.0,
            "S3 capitulation asset must not be crowded out by 40 S1/S2 assets, got w_s3={}",
            cap_sig.w_s3_event_reversion
        );
        assert!(out.s1_gross_weight <= 0.45 * 1.85 + 1e-3);
        assert!(out.s2_gross_weight <= 0.40 * 1.85 + 1e-3);
    }

    #[test]
    fn test_breadth_regime_gates_shorts_in_bull_and_longs_in_bear_and_fixes_s1_demeaning() {
        // 1. BULL_EXPANSION (4 strong leaders + 1 mild positive laggard + 1 bearish coin -> breadth >= 0.68):
        //    - Lagging positive coin MUST NOT invert sign via S1 cross-sectional demeaning!
        //    - Bearish coin MUST be gated to 0.0 (no counter-regime SHORT in BULL_EXPANSION).
        //    - Held SHORT on bearish coin MUST still emit ACTION_CLOSE when signal exits (never blocked).
        let mut bull_assets = Vec::new();
        let mut bull_bars = HashMap::new();
        for id in 1..=4i64 {
            let b = build_trend_bars(id, 100.0, 0.015, 26, 2.0);
            let close = b.last().unwrap().close;
            bull_assets.push(sample_asset(
                id,
                &format!("L{id}"),
                close * 1.01,
                100.0,
                -0.00025,
                20_000_000.0,
            ));
            bull_bars.insert(id, b);
        }
        // Mildly positive laggard (id=5) with small positive drift + baseline carry
        let lag_bars = build_trend_bars(5, 100.0, 0.001, 26, 1.0);
        let lag_close = lag_bars.last().unwrap().close;
        bull_assets.push(sample_asset(
            5,
            "LAG",
            lag_close * 1.003,
            100.0,
            -0.00001,
            10_000_000.0,
        ));
        bull_bars.insert(5, lag_bars);

        // Bearish coin (id=6): breadth = 5 / 6 = 0.8333 >= 0.68 -> BULL_EXPANSION, block_new_shorts = true
        // First 12 bars up, then 14 bars down so EMA8 < EMA21 < EMA55 while sitting mid-range (range_pos ~ 0.4)
        // without triggering contrarian downward-flush LONG.
        let mut bear_bars = build_trend_bars(6, 100.0, 0.004, 10, 1.0);
        let mut b_px = bear_bars.last().unwrap().close;
        let mut b_ts = bear_bars.last().unwrap().timestamp_ms;
        for i in 10..26 {
            b_ts += 14_400_000;
            let open = b_px;
            b_px *= 0.985;
            bear_bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 6,
                timeframe: "4h".to_string(),
                timestamp_ms: b_ts,
                open,
                high: open * 1.02,
                low: b_px * 0.98,
                close: b_px,
                volume: if i == 25 { 20_000.0 } else { 10_000.0 },
                funding_rate: 0.00025,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        // Keep a prior swing low far below so range_pos stays above 0.25
        bear_bars[0].low = 60.0;
        let bear_close = bear_bars.last().unwrap().close;
        bull_assets.push(sample_asset(
            6,
            "BEAR",
            bear_close,
            100.0,
            0.00025,
            15_000_000.0,
        ));
        bull_bars.insert(6, bear_bars);

        // Overextended blow-off coin (id=8) with crowded positive funding -> S3 would emit contrarian SHORT,
        // which BULL_EXPANSION must suppress to 0.0 (breadth = 6/7 >= 0.68)
        let mut blow_bars = build_trend_bars(8, 100.0, 0.005, 25, 1.0);
        let bl_prev = blow_bars.last().unwrap().close;
        let bl_spike = bl_prev * 1.12;
        let mut bl_last = blow_bars.last().unwrap().clone();
        bl_last.id = 26;
        bl_last.timestamp_ms += 14_400_000;
        bl_last.open = bl_prev;
        bl_last.high = bl_spike;
        bl_last.close = bl_spike;
        bl_last.low = bl_prev;
        blow_bars.push(bl_last);
        bull_assets.push(sample_asset(
            8,
            "BLOW",
            bl_spike,
            100.0,
            0.0020,
            15_000_000.0,
        ));
        bull_bars.insert(8, blow_bars);

        let engine = SignalEngine::default();
        let bull_out = engine.evaluate_universe(&bull_assets, &bull_bars, &HashMap::new(), &[]);
        assert_eq!(bull_out.breadth.market_regime_label, "BULL_EXPANSION");

        let lag_sig = bull_out.signals.iter().find(|s| s.asset_id == 5).unwrap();
        assert!(
            lag_sig.w_s1_carry_mom >= 0.0 && lag_sig.target_weight >= 0.0,
            "S1 demeaning must preserve positive sign on lagging bull coin, got w_s1={}",
            lag_sig.w_s1_carry_mom
        );

        let bear_sig = bull_out.signals.iter().find(|s| s.asset_id == 6).unwrap();
        assert_eq!(bear_sig.w_s1_carry_mom, 0.0);
        assert_eq!(bear_sig.w_s2_trend_breakout, 0.0);
        assert_eq!(bear_sig.w_s3_event_reversion, 0.0);
        assert_eq!(bear_sig.target_weight, 0.0);

        let blow_sig = bull_out.signals.iter().find(|s| s.asset_id == 8).unwrap();
        assert!(blow_sig.w_s3_event_reversion >= 0.0);
        assert!(blow_sig.target_weight >= 0.0);

        // Held SHORT on neutral/exiting coin in BULL_EXPANSION MUST still emit ACTION_CLOSE
        let mut flat_coin = sample_asset(7, "EXITSHORT", 100.0, 100.0, 0.0, 15_000_000.0);
        flat_coin.is_event_eligible = false;
        let flat_bars = build_trend_bars(7, 100.0, 0.0, 26, 1.0);
        bull_assets.push(flat_coin);
        bull_bars.insert(7, flat_bars);
        let mut held = HashMap::new();
        held.insert(7, -0.10);
        let close_out = engine.evaluate_universe(&bull_assets, &bull_bars, &held, &[]);
        let close_sig = close_out.signals.iter().find(|s| s.asset_id == 7).unwrap();
        assert_eq!(close_sig.rebalance_action, ACTION_CLOSE);
        assert_eq!(close_sig.target_weight, 0.0);

        // 2. BEAR_CONTRACTION (4 bearish coins + 1 bullish coin -> breadth = 1/5 = 0.20 <= 0.32):
        //    - Bullish coin MUST be gated to 0.0 (no counter-regime LONG).
        let mut bear_assets = Vec::new();
        let mut bear_bars_map = HashMap::new();
        for id in 10..=13i64 {
            let b = build_trend_bars(id, 100.0, -0.012, 26, 2.0);
            let close = b.last().unwrap().close;
            bear_assets.push(sample_asset(
                id,
                &format!("B{id}"),
                close * 0.99,
                100.0,
                0.0002,
                20_000_000.0,
            ));
            bear_bars_map.insert(id, b);
        }
        let loner_bars = build_trend_bars(14, 100.0, 0.008, 26, 2.0);
        let loner_close = loner_bars.last().unwrap().close;
        bear_assets.push(sample_asset(
            14,
            "LONER",
            loner_close * 1.01,
            100.0,
            -0.0003,
            15_000_000.0,
        ));
        bear_bars_map.insert(14, loner_bars);

        // Downward flush dip-buy coin (id=15) in BEAR_CONTRACTION (breadth = 1/6 = 0.1667 <= 0.32):
        // S3 would emit a contrarian LONG, which BEAR_CONTRACTION must block to 0.0!
        let mut knife_bars = build_trend_bars(15, 100.0, -0.015, 25, 1.0);
        let k_prev = knife_bars.last().unwrap().close;
        let k_flush = k_prev * 0.88;
        let mut k_last = knife_bars.last().unwrap().clone();
        k_last.id = 26;
        k_last.timestamp_ms += 14_400_000;
        k_last.open = k_prev;
        k_last.low = k_flush;
        k_last.close = k_flush;
        k_last.high = k_prev;
        knife_bars.push(k_last);
        bear_assets.push(sample_asset(
            15,
            "KNIFE",
            k_flush,
            100.0,
            -0.0004,
            15_000_000.0,
        ));
        bear_bars_map.insert(15, knife_bars);

        let bear_out = engine.evaluate_universe(&bear_assets, &bear_bars_map, &HashMap::new(), &[]);
        assert_eq!(bear_out.breadth.market_regime_label, "BEAR_CONTRACTION");
        let loner_sig = bear_out.signals.iter().find(|s| s.asset_id == 14).unwrap();
        assert_eq!(loner_sig.w_s1_carry_mom, 0.0);
        assert_eq!(loner_sig.w_s2_trend_breakout, 0.0);
        assert!(loner_sig.target_weight <= 0.0);
        let knife_sig = bear_out.signals.iter().find(|s| s.asset_id == 15).unwrap();
        assert_eq!(knife_sig.w_s1_carry_mom, 0.0);
        assert_eq!(knife_sig.w_s2_trend_breakout, 0.0);
        assert_eq!(knife_sig.w_s3_event_reversion, 0.0);
        assert_eq!(knife_sig.target_weight, 0.0);
    }

    #[test]
    fn test_cycle_feature_cache_and_bar_close_cache_bit_exact_and_slice_parity() {
        let btc_bars = build_trend_bars(1, 90_000.0, 0.006, 25, 1.2);
        let sol_bars = build_trend_bars(2, 160.0, 0.015, 25, 2.4);
        let eth_bars = build_trend_bars(3, 3_200.0, -0.012, 25, 2.1);

        let btc = sample_asset(
            1,
            "BTC",
            btc_bars.last().unwrap().close,
            90_000.0,
            0.00001,
            50_000_000.0,
        );
        let sol = sample_asset(
            2,
            "SOL",
            sol_bars.last().unwrap().close * 1.02,
            160.0,
            -0.00015,
            25_000_000.0,
        );
        let eth = sample_asset(
            3,
            "ETH",
            eth_bars.last().unwrap().close * 0.98,
            3_200.0,
            0.00035,
            30_000_000.0,
        );
        let assets = vec![btc, sol, eth];

        let mut vec_map: HashMap<i64, Vec<OhlcvBar>> = HashMap::new();
        vec_map.insert(1, btc_bars);
        vec_map.insert(2, sol_bars);
        vec_map.insert(3, eth_bars);

        let slice_map: HashMap<i64, &[OhlcvBar]> =
            vec_map.iter().map(|(&id, v)| (id, v.as_slice())).collect();

        let engine = SignalEngine::default();
        let first = engine.evaluate_universe(&assets, &vec_map, &HashMap::new(), &[]);
        // Second call hits BarCloseCache on unchanged bars -> bit-exact parity
        let second = engine.evaluate_universe(&assets, &slice_map, &HashMap::new(), &[]);

        assert_eq!(first.signals.len(), second.signals.len());
        for (a, b) in first.signals.iter().zip(second.signals.iter()) {
            assert_eq!(a.asset_id, b.asset_id);
            assert_eq!(a.w_s1_carry_mom, b.w_s1_carry_mom);
            assert_eq!(a.w_s2_trend_breakout, b.w_s2_trend_breakout);
            assert_eq!(a.w_s3_event_reversion, b.w_s3_event_reversion);
            assert_eq!(a.target_weight, b.target_weight);
            assert_eq!(a.rebalance_action, b.rebalance_action);
        }
        assert_eq!(
            first.breadth.market_regime_label,
            second.breadth.market_regime_label
        );
    }

    #[test]
    fn test_p1_1_s1_s2_top_k_scales_with_max_open_positions_and_s3_strict_default() {
        use aegis_core::ConfigMap;

        let mut assets = Vec::with_capacity(15);
        let mut bars_by_asset = HashMap::new();
        for id in 1..=15i64 {
            let mut bars = build_trend_bars(id, 100.0, 0.012, 26, 2.0);
            let close = bars.last().unwrap().close;
            let a = sample_asset(
                id,
                &format!("M{id}"),
                close * 1.01,
                100.0,
                -0.00015,
                (id as f64) * 1_000_000.0,
            );
            bars.last_mut().unwrap().volume *= 2.0;
            bars_by_asset.insert(id, bars);
            assets.push(a);
        }

        let mut cm = ConfigMap::new();
        cm.insert("max_open_positions", "10");
        let engine_10 = SignalEngine::from_config_map(&cm);
        assert!(engine_10.config.s3_strict_admission_enabled);
        assert!(engine_10.config.wall_clock_normalize_indicators);

        let out_10 = engine_10.evaluate_universe(&assets, &bars_by_asset, &HashMap::new(), &[]);
        let s1_nz = out_10
            .signals
            .iter()
            .filter(|s| s.w_s1_carry_mom.abs() > 1e-6)
            .count();
        let s2_nz = out_10
            .signals
            .iter()
            .filter(|s| s.w_s2_trend_breakout.abs() > 1e-6)
            .count();
        assert_eq!(s1_nz, 10, "S1 top_k must scale to max_open_positions=10");
        assert_eq!(s2_nz, 10, "S2 top_k must scale to max_open_positions=10");

        let engine_6 = SignalEngine::default();
        let out_6 = engine_6.evaluate_universe(&assets, &bars_by_asset, &HashMap::new(), &[]);
        let s1_6 = out_6
            .signals
            .iter()
            .filter(|s| s.w_s1_carry_mom.abs() > 1e-6)
            .count();
        let s2_6 = out_6
            .signals
            .iter()
            .filter(|s| s.w_s2_trend_breakout.abs() > 1e-6)
            .count();
        assert_eq!(s1_6, 8);
        assert_eq!(s2_6, 8);
    }

    #[test]
    fn test_p1_2_end_to_end_1h_vs_4h_invariance_and_bar_cache_toggle() {
        let base_ts = 1_700_000_000_000i64;
        let mut bars_4h = Vec::with_capacity(60);
        let mut p4 = 100.0;
        for i in 0..60 {
            let open = p4;
            p4 *= 1.006;
            bars_4h.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 1,
                timeframe: "4h".to_string(),
                timestamp_ms: base_ts + (i as i64) * 14_400_000,
                open,
                high: p4 * 1.003,
                low: open * 0.997,
                close: p4,
                volume: 50_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        let mut bars_1h = Vec::with_capacity(240);
        let mut p1 = 100.0;
        let r_1h = 1.006_f64.powf(0.25);
        for i in 0..240 {
            let open = p1;
            p1 *= r_1h;
            bars_1h.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 1,
                timeframe: "1h".to_string(),
                timestamp_ms: base_ts + (i as i64) * 3_600_000,
                open,
                high: p1 * 1.0015,
                low: open * 0.9985,
                close: p1,
                volume: 12_500.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }

        let a4 = sample_asset(
            1,
            "SOL",
            bars_4h.last().unwrap().close,
            100.0,
            0.0,
            20_000_000.0,
        );
        let a1 = sample_asset(
            1,
            "SOL",
            bars_1h.last().unwrap().close,
            100.0,
            0.0,
            20_000_000.0,
        );
        let mut m4 = HashMap::new();
        m4.insert(1, bars_4h);
        let mut m1 = HashMap::new();
        m1.insert(1, bars_1h);

        let eng_norm = SignalEngine::default();
        let out_4h = eng_norm.evaluate_universe(&[a4], &m4, &HashMap::new(), &[]);
        let out_1h =
            eng_norm.evaluate_universe(std::slice::from_ref(&a1), &m1, &HashMap::new(), &[]);
        assert_eq!(out_4h.signals[0].s2_trend_alignment, 1);
        assert_eq!(out_1h.signals[0].s2_trend_alignment, 1);
        assert!(
            (out_4h.signals[0].s1_mom_score - out_1h.signals[0].s1_mom_score).abs() < 0.03,
            "1h ({}) vs 4h ({}) S1 momentum parity",
            out_1h.signals[0].s1_mom_score,
            out_4h.signals[0].s1_mom_score
        );

        // Toggle wall_clock_normalize_indicators on a shared BarCloseCache must invalidate and recompute
        let mut shared_cache = BarCloseCache::new();
        let norm_on = eng_norm.evaluate_universe_with_cache(
            std::slice::from_ref(&a1),
            &m1,
            &HashMap::new(),
            &[],
            &mut shared_cache,
        );
        let mut eng_off = SignalEngine::default();
        eng_off.config.wall_clock_normalize_indicators = false;
        let norm_off = eng_off.evaluate_universe_with_cache(
            &[a1],
            &m1,
            &HashMap::new(),
            &[],
            &mut shared_cache,
        );
        assert!(
            (norm_on.signals[0].s1_mom_score - norm_off.signals[0].s1_mom_score).abs() > 0.05,
            "BarCloseCache must respect wall_clock_normalize_indicators toggle in cache key"
        );
    }

    #[test]
    fn test_n2_signal_exit_confirm_bars_and_mandatory_r13_s1_bypasses() {
        let base_ts = 1_700_000_000_000i64;
        let step = 3_600_000i64;
        let mut bar_cache = BarCloseCache::new();
        let mut cfg = AllocatorConfig {
            signal_exit_confirm_bars: 2,
            ..AllocatorConfig::default()
        };
        let engine = SignalEngine::new(cfg.clone());

        // Neutral flat bars (zero sleeves -> non-emergency ACTION_CLOSE candidate)
        let mut neutral_bars: Vec<OhlcvBar> = (0..20)
            .map(|i| OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 1,
                timeframe: "1h".to_string(),
                timestamp_ms: base_ts + (i as i64) * step,
                open: 100.0,
                high: 100.4,
                low: 99.6,
                close: 100.0,
                volume: 50_000.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            })
            .collect();
        let asset = sample_asset(1, "SOL", 100.0, 100.0, 0.0000125, 20_000_000.0);
        let mut held = HashMap::new();
        held.insert(
            1,
            HeldPositionContext {
                current_weight: 0.12,
                unrealized_pnl_pct: 0.0,
            },
        );
        let mut map = HashMap::new();
        map.insert(1, neutral_bars.clone());

        // 1. Bar 1 (t1): first exit signal -> held at cur_w (count = 1 < 2)
        let out1 = engine.evaluate_universe_with_cache(
            std::slice::from_ref(&asset),
            &map,
            &held,
            &[],
            &mut bar_cache,
        );
        assert_eq!(out1.signals[0].rebalance_action, ACTION_HOLD);
        assert!((out1.signals[0].target_weight - 0.12).abs() < 1e-6);
        assert_eq!(bar_cache.exit_confirm_state(1).unwrap().count, 1);

        // 2. Repeated 3s tick on SAME bar (same timestamp_ms, forming bar volume updated) -> STILL count=1 -> HOLD
        neutral_bars.last_mut().unwrap().volume = 50_100.0;
        map.insert(1, neutral_bars.clone());
        let out_same_bar = engine.evaluate_universe_with_cache(
            std::slice::from_ref(&asset),
            &map,
            &held,
            &[],
            &mut bar_cache,
        );
        assert_eq!(out_same_bar.signals[0].rebalance_action, ACTION_HOLD);
        assert_eq!(bar_cache.exit_confirm_state(1).unwrap().count, 1);

        // 3. New bar (t2 > t1) still neutral -> count increments to 2 -> ACTION_CLOSE!
        neutral_bars.push(OhlcvBar {
            id: 21,
            asset_id: 1,
            timeframe: "1h".to_string(),
            timestamp_ms: base_ts + 20 * step,
            open: 100.0,
            high: 100.4,
            low: 99.6,
            close: 100.0,
            volume: 50_000.0,
            funding_rate: 0.0,
            created_at_ms: 0,
            updated_at_ms: 0,
        });
        map.insert(1, neutral_bars.clone());
        let out2 = engine.evaluate_universe_with_cache(
            std::slice::from_ref(&asset),
            &map,
            &held,
            &[],
            &mut bar_cache,
        );
        assert_eq!(out2.signals[0].rebalance_action, ACTION_CLOSE);
        assert_eq!(out2.signals[0].target_weight, 0.0);

        // 4. Signal re-alignment clears state, then new exit restarts at count=1
        let mut bull_bars = build_trend_bars(1, 100.0, 0.015, 25, 2.0);
        let bull_asset = sample_asset(
            1,
            "SOL",
            bull_bars.last().unwrap().close * 1.01,
            100.0,
            -0.0002,
            20_000_000.0,
        );
        let mut bull_map = HashMap::new();
        bull_map.insert(1, bull_bars.clone());
        let _realigned = engine.evaluate_universe_with_cache(
            &[bull_asset],
            &bull_map,
            &held,
            &[],
            &mut bar_cache,
        );
        assert_eq!(bar_cache.exit_confirm_state(1), None);
        let mut fresh_neutral = neutral_bars.clone();
        fresh_neutral.last_mut().unwrap().timestamp_ms = base_ts + 40 * step;
        map.insert(1, fresh_neutral);
        let restarted = engine.evaluate_universe_with_cache(
            std::slice::from_ref(&asset),
            &map,
            &held,
            &[],
            &mut bar_cache,
        );
        assert_eq!(restarted.signals[0].rebalance_action, ACTION_HOLD);
        assert_eq!(bar_cache.exit_confirm_state(1).unwrap().count, 1);

        // 5. Mandatory R13-S1 immediate bypass regression tests (signal_exit_confirm_bars = 3):
        //    Each must fire ACTION_CLOSE immediately on bar 1!
        cfg.signal_exit_confirm_bars = 3;
        let strict_engine = SignalEngine::new(cfg.clone());

        // 5a. !hysteresis_eligible (0/12 active dead book)
        let mut cache_5a = BarCloseCache::new();
        let dead_bars: Vec<OhlcvBar> = (0..16)
            .map(|i| OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 2,
                timeframe: "1h".to_string(),
                timestamp_ms: base_ts + (i as i64) * step,
                open: 10.0,
                high: 10.0,
                low: 10.0,
                close: 10.0,
                volume: 0.0,
                funding_rate: 0.0,
                created_at_ms: 0,
                updated_at_ms: 0,
            })
            .collect();
        let dead_asset = sample_asset(2, "DEAD", 10.0, 10.0, 0.0, 10_000.0);
        let mut dead_map = HashMap::new();
        dead_map.insert(2, dead_bars);
        let mut dead_held = HashMap::new();
        dead_held.insert(
            2,
            HeldPositionContext {
                current_weight: 0.12,
                unrealized_pnl_pct: 0.01,
            },
        );
        let r5a = strict_engine.evaluate_universe_with_cache(
            &[dead_asset],
            &dead_map,
            &dead_held,
            &[],
            &mut cache_5a,
        );
        assert_eq!(
            r5a.signals[0].rebalance_action, ACTION_CLOSE,
            "R13-S1 !hysteresis_eligible must bypass exit_confirm_bars immediately on bar 1"
        );

        // 5b. counter_regime_held (BEAR_CONTRACTION blocking longs while holding LONG)
        let mut cache_5b = BarCloseCache::new();
        let mut bear_assets = Vec::new();
        let mut bear_map = HashMap::new();
        for id in 10..=13i64 {
            let b = build_trend_bars(id, 100.0, -0.012, 26, 2.0);
            let close = b.last().unwrap().close;
            bear_assets.push(sample_asset(
                id,
                &format!("B{id}"),
                close * 0.99,
                100.0,
                0.0002,
                20_000_000.0,
            ));
            bear_map.insert(id, b);
        }
        // Asset 14 held LONG in BEAR_CONTRACTION with neutral bars & neutral funding z-score
        // (would otherwise be held by N2 confirm_bars=3 without R13-S1 counter_regime_held bypass)
        let b14 = build_trend_bars(14, 100.0, 0.0, 26, 1.0);
        let c14 = b14.last().unwrap().close;
        bear_assets.push(sample_asset(
            14,
            "HELDLONG",
            c14,
            100.0,
            0.0002,
            20_000_000.0,
        ));
        bear_map.insert(14, b14);
        let mut held_5b = HashMap::new();
        held_5b.insert(
            14,
            HeldPositionContext {
                current_weight: 0.12,
                unrealized_pnl_pct: 0.0,
            },
        );
        let r5b = strict_engine.evaluate_universe_with_cache(
            &bear_assets,
            &bear_map,
            &held_5b,
            &[],
            &mut cache_5b,
        );
        assert_eq!(r5b.breadth.market_regime_label, "BEAR_CONTRACTION");
        let sig14 = r5b.signals.iter().find(|s| s.asset_id == 14).unwrap();
        assert_eq!(
            sig14.rebalance_action, ACTION_CLOSE,
            "R13-S1 counter_regime_held must bypass exit_confirm_bars immediately on bar 1"
        );

        // 5c. is_downward_flush (s3_flush_atr_mult <= -3.5) on held SHORT
        let mut cache_5c = BarCloseCache::new();
        let mut crash_bars = build_trend_bars(20, 100.0, -0.004, 25, 1.0);
        let c_prev = crash_bars.last().unwrap().close;
        let c_flush = c_prev * 0.85;
        let mut c_last = crash_bars.last().unwrap().clone();
        c_last.id = 26;
        c_last.timestamp_ms += 14_400_000;
        c_last.open = c_prev;
        c_last.low = c_flush;
        c_last.close = c_flush;
        c_last.high = c_prev;
        crash_bars.push(c_last);
        let crash_asset = sample_asset(20, "CRASH", c_flush, 100.0, 0.0, 20_000_000.0);
        let mut crash_map = HashMap::new();
        crash_map.insert(20, crash_bars);
        let mut held_5c = HashMap::new();
        held_5c.insert(
            20,
            HeldPositionContext {
                current_weight: -0.12,
                unrealized_pnl_pct: 0.02,
            },
        );
        let r5c = strict_engine.evaluate_universe_with_cache(
            &[crash_asset],
            &crash_map,
            &held_5c,
            &[],
            &mut cache_5c,
        );
        assert!(r5c.signals[0].s3_flush_atr_mult <= -3.5);
        assert!(
            matches!(
                r5c.signals[0].rebalance_action.as_str(),
                ACTION_CLOSE | ACTION_FLIP
            ),
            "R13-S1 downward flush <= -3.5 ATR on held SHORT must never hold on bar 1, got {}",
            r5c.signals[0].rebalance_action
        );

        // 5d. is_loser && invalidated: underwater loser with mild bearish momentum (RSI > 36, s2 == 0, w_s1 == 0)
        //     must immediately emit ACTION_CLOSE on bar 1 (never held by exit_confirm_bars = 3)
        let mut cache_5d = BarCloseCache::new();
        let mut loser_bars = Vec::with_capacity(26);
        let closes_5d: [f64; 26] = [
            100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0,
            100.0, 100.0, 100.0, 100.0, 102.0, 102.0, 102.0, 102.0, 102.0, 102.0, 100.2, 100.0,
            99.8, 99.6,
        ];
        for (i, &c) in closes_5d.iter().enumerate() {
            let open = if i == 0 { 100.0 } else { closes_5d[i - 1] };
            let high = if i == 0 { 108.0 } else { open.max(c) * 1.004 };
            let low = if i == 0 { 85.0 } else { open.min(c) * 0.996 };
            loser_bars.push(OhlcvBar {
                id: (i + 1) as i64,
                asset_id: 21,
                timeframe: "4h".to_string(),
                timestamp_ms: base_ts + (i as i64) * 14_400_000,
                open,
                high,
                low,
                close: c,
                volume: 50_000.0,
                funding_rate: -0.00010,
                created_at_ms: 0,
                updated_at_ms: 0,
            });
        }
        let loser_close = *closes_5d.last().unwrap();
        let loser_asset = sample_asset(21, "LOSER", loser_close, 100.0, -0.00010, 20_000_000.0);
        let mut loser_map = HashMap::new();
        loser_map.insert(21, loser_bars);
        let mut held_5d = HashMap::new();
        held_5d.insert(
            21,
            HeldPositionContext {
                current_weight: 0.12,
                unrealized_pnl_pct: -0.006, // <= -0.003 loser!
            },
        );
        let r5d = strict_engine.evaluate_universe_with_cache(
            &[loser_asset],
            &loser_map,
            &held_5d,
            &[],
            &mut cache_5d,
        );
        assert!(
            r5d.signals[0].s1_mom_score < 0.0,
            "s1_mom={}",
            r5d.signals[0].s1_mom_score
        );
        assert_eq!(
            r5d.signals[0].rebalance_action, ACTION_CLOSE,
            "R13-S1 is_loser && invalidated must bypass exit_confirm_bars immediately on bar 1, got {}",
            r5d.signals[0].rebalance_action
        );
        let _ = &mut bull_bars;
    }
}
