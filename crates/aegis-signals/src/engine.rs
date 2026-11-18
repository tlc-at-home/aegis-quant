//! Unified Multi-Sleeve Signal Engine (`aegis-signals::engine`).
//!
//! Orchestrates Sleeve 1 (`carry_momentum`), Sleeve 2 (`trend_breakout`), Sleeve 3 (`event_reversion`),
//! Cross-Sectional Market Breadth (`breadth`), and Risk-Tier Portfolio Allocation (`allocator`).

use crate::allocator::{
    allocate_multi_sleeve_weights, AllocatorConfig, RiskTierLeverageExt, SleeveSignalInput,
};
use crate::breadth::{
    compute_market_breadth_snapshot, compute_preliminary_bull_breadth, MarketBreadthSnapshot,
};
use crate::carry_momentum::evaluate_sleeve_s1;
use crate::event_reversion::{evaluate_sleeve_s3, TRIGGER_CAPITULATION_FLUSH};
use crate::trend_breakout::evaluate_sleeve_s2;
use aegis_core::{
    round_dp, ConfigMap, CooldownAssetView, ExchangeAsset, OhlcvBar, OrderSide, PositionRecord,
    PositionStatus, RiskTier,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
#[derive(Debug, Clone, Default)]
pub struct SignalEngine {
    pub config: AllocatorConfig,
}

impl SignalEngine {
    pub fn new(config: AllocatorConfig) -> Self {
        Self { config }
    }

    pub fn from_config_map(cfg: &ConfigMap) -> Self {
        Self {
            config: AllocatorConfig::from_config_map(cfg),
        }
    }

    /// Evaluates all three sleeves (S1 Carry-Momentum, S2 Trend-Breakout, S3 Event-Reversion),
    /// cross-sectional breadth, cooldown blocking, and risk-tier allocation across `assets`.
    pub fn evaluate_universe(
        &self,
        assets: &[ExchangeAsset],
        bars_by_asset: &HashMap<i64, Vec<OhlcvBar>>,
        current_weights: &HashMap<i64, f64>,
        active_cooldowns: &[CooldownAssetView],
    ) -> SignalCycleOutput {
        let btc_bars: &[OhlcvBar] = assets
            .iter()
            .find(|a| {
                a.coin.eq_ignore_ascii_case("BTC") || a.symbol.eq_ignore_ascii_case("BTC-USDC")
            })
            .and_then(|btc| bars_by_asset.get(&btc.id))
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        let (prelim_breadth_bull, _) = compute_preliminary_bull_breadth(assets, bars_by_asset);

        let s1_map = evaluate_sleeve_s1(
            assets,
            bars_by_asset,
            btc_bars,
            self.config.s1_carry_mom_budget,
        );
        let s2_map = evaluate_sleeve_s2(
            assets,
            bars_by_asset,
            prelim_breadth_bull,
            self.config.s2_trend_breakout_budget,
        );
        let s3_map =
            evaluate_sleeve_s3(assets, bars_by_asset, self.config.s3_event_reversion_budget);

        let signals = allocate_multi_sleeve_weights(
            assets,
            &s1_map,
            &s2_map,
            &s3_map,
            current_weights,
            active_cooldowns,
            &self.config,
        );

        let beta_by_asset: HashMap<i64, f64> = s1_map
            .iter()
            .map(|(&aid, m)| (aid, m.market_beta))
            .collect();
        let target_weights: HashMap<i64, f64> = signals
            .iter()
            .map(|s| (s.asset_id, s.target_weight))
            .collect();
        let capitulation_count = s3_map
            .values()
            .filter(|m| m.event_trigger.as_deref() == Some(TRIGGER_CAPITULATION_FLUSH))
            .count();

        let breadth = compute_market_breadth_snapshot(
            assets,
            bars_by_asset,
            &beta_by_asset,
            &target_weights,
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

    /// Convenience entrypoint that derives `current_weights` from active (`WORKING` and `OPEN`) positions
    /// and total portfolio equity before running `evaluate_universe`.
    pub fn evaluate_universe_with_positions(
        &self,
        assets: &[ExchangeAsset],
        bars_by_asset: &HashMap<i64, Vec<OhlcvBar>>,
        active_positions: &[PositionRecord],
        active_cooldowns: &[CooldownAssetView],
        total_equity_usdc: f64,
    ) -> SignalCycleOutput {
        let eq = total_equity_usdc.max(1.0);
        let mut current_weights = HashMap::new();
        for pos in active_positions {
            let status = PositionStatus::parse(&pos.status);
            if !matches!(status, PositionStatus::Working | PositionStatus::Open) {
                continue;
            }
            let side = OrderSide::parse(&pos.side);
            let signed_w = if pos.portfolio_weight.abs() > 1e-6 {
                pos.portfolio_weight.abs() * side.sign()
            } else {
                let notional = if status == PositionStatus::Open {
                    pos.committed_notional_usdc
                } else {
                    pos.entry_price * pos.requested_quantity
                };
                (notional / eq) * side.sign()
            };
            *current_weights.entry(pos.asset_id).or_insert(0.0) += signed_w;
        }
        self.evaluate_universe(assets, bars_by_asset, &current_weights, active_cooldowns)
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
            fet_sig.s1_mom_score > 0.0,
            "S1 should produce positive recovery momentum at bouncing range bottom, got {}",
            fet_sig.s1_mom_score
        );
        assert!(
            fet_sig.w_s1_carry_mom > 0.0,
            "S1 carry-mom weight should be positive at bouncing range bottom, got {}",
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
}
