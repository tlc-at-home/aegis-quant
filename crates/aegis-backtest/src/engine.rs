//! Multi-Sleeve S1/S2/S3 Walk-Forward Simulation Engine (`aegis-backtest::engine`).
//!
//! Simulates the complete Aegis Quant v2.0 architecture across historical or synthetic OHLCV series:
//! - Sleeve 1 (`carry_momentum`), Sleeve 2 (`trend_breakout`), and Sleeve 3 (`event_reversion`)
//! - Risk-Tier scaling (`Extreme 1.85x` default) via `aegis-signals::SignalEngine`
//! - Order-to-Open Capital Lifecycle (`WORKING` reserves margin -> `OPEN` commits capital) via `aegis-risk::RiskEngine`
//! - High-Water Chandelier/ATR trailing stop ratchet + post-stop asset cooldown enforcement (`v_assets_in_cooldown`).

use aegis_core::{
    round_dp, ConfigMap, CooldownAssetView, ExchangeAsset, OhlcvBar, OrderSide, PortfolioSnapshot,
    PositionRecord, PositionStatus, Result, RiskTier,
};
use aegis_risk::{compute_atr_from_bars, RiskEngine, RiskLimitsConfig};
use aegis_signals::{
    AllocatorConfig, SignalEngine, ACTION_CLOSE, ACTION_COOLDOWN_BLOCKED, ACTION_FLIP,
    ACTION_INCREASE, ACTION_OPEN_LONG, ACTION_OPEN_SHORT, ACTION_REDUCE,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Configuration for the multi-sleeve walk-forward simulation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfig {
    pub initial_equity_usdc: f64,
    pub risk_tier: RiskTier,
    pub days: usize,
    pub warmup_bars: usize,
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
    pub slippage_bps: f64,
    pub stop_out_cooldown_bars: usize,
    pub allocator: AllocatorConfig,
    pub risk_limits: RiskLimitsConfig,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            initial_equity_usdc: 10_000.0,
            risk_tier: RiskTier::Extreme,
            days: 30,
            warmup_bars: 24,
            maker_fee_bps: 1.5,
            taker_fee_bps: 4.5,
            slippage_bps: 2.0,
            stop_out_cooldown_bars: 4,
            allocator: AllocatorConfig::default(),
            risk_limits: RiskLimitsConfig::default(),
        }
    }
}

impl BacktestConfig {
    pub fn from_config_map(cfg: &ConfigMap, days: usize) -> Self {
        let allocator = AllocatorConfig::from_config_map(cfg);
        let risk_limits = RiskLimitsConfig::from_config_map(cfg);
        Self {
            initial_equity_usdc: cfg
                .get_f64("initial_paper_equity_usdc", 10_000.0)
                .max(100.0),
            risk_tier: allocator.risk_tier,
            days: days.max(5),
            warmup_bars: 24,
            maker_fee_bps: risk_limits.maker_fee_bps,
            taker_fee_bps: risk_limits.taker_fee_bps,
            slippage_bps: 2.0,
            stop_out_cooldown_bars: 4,
            allocator,
            risk_limits,
        }
    }
}

/// Comprehensive multi-sleeve walk-forward backtest tear sheet & performance report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestReport {
    pub platform_version: String,
    pub risk_tier: String,
    pub leverage_multiplier: f64,
    pub initial_equity_usdc: f64,
    pub final_equity_usdc: f64,
    pub total_return_pct: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub max_drawdown_pct: f64,
    pub win_rate_pct: f64,
    pub profit_factor: f64,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub stop_out_count: usize,
    pub cooldown_blocks: usize,
    pub s1_attribution_usdc: f64,
    pub s2_attribution_usdc: f64,
    pub s3_attribution_usdc: f64,
    pub total_fees_usdc: f64,
    pub total_funding_usdc: f64,
    pub bars_evaluated: usize,
    pub assets_evaluated: usize,
    pub equity_curve: Vec<PortfolioSnapshot>,
    pub closed_positions: Vec<PositionRecord>,
}

pub type BacktestTearSheet = BacktestReport;

/// Primary Aegis Quant v2.0 Multi-Sleeve Walk-Forward Backtest Engine.
#[derive(Debug, Clone, Default)]
pub struct BacktestEngine {
    pub config: BacktestConfig,
}

impl BacktestEngine {
    pub fn new(config: BacktestConfig) -> Self {
        Self { config }
    }

    pub fn from_config_map(cfg: &ConfigMap, days: usize) -> Self {
        Self {
            config: BacktestConfig::from_config_map(cfg, days),
        }
    }

    /// Generates a deterministic multi-asset Hyperliquid perpetual dataset (`BTC-USDC`, `ETH-USDC`,
    /// `SOL-USDC`, `HYPE-USDC`, `SUI-USDC`, `AVAX-USDC`, `LINK-USDC`, `TAO-USDC`) for walk-forward verification.
    pub fn generate_canonical_dataset(
        days: usize,
        warmup_bars: usize,
    ) -> (Vec<ExchangeAsset>, HashMap<i64, Vec<OhlcvBar>>) {
        let total_bars = (days.max(5) * 6) + warmup_bars.max(20);
        let specs = [
            (1i64, "BTC", 92_000.0, 0.0022, 0.000010, 65_000_000.0),
            (2i64, "ETH", 3_150.0, 0.0018, 0.000012, 42_000_000.0),
            (3i64, "SOL", 185.0, 0.0035, -0.000025, 34_000_000.0),
            (4i64, "HYPE", 26.5, 0.0042, -0.000040, 22_000_000.0),
            (5i64, "SUI", 3.20, 0.0030, 0.000015, 16_000_000.0),
            (6i64, "AVAX", 34.0, -0.0015, 0.000045, 14_000_000.0),
            (7i64, "LINK", 18.5, 0.0020, 0.000008, 12_500_000.0),
            (8i64, "TAO", 510.0, 0.0038, -0.000030, 11_000_000.0),
        ];

        let start_ts = 1_700_000_000_000i64;
        let step_ms = 4 * 3_600_000i64;
        let mut assets = Vec::with_capacity(specs.len());
        let mut bars_by_asset = HashMap::with_capacity(specs.len());

        for (id, coin, base_px, drift, base_funding, day_vol) in specs {
            let mut hasher = Sha256::new();
            hasher.update(format!("AEGIS_V2_BT:{coin}:{id}").as_bytes());
            let digest = hasher.finalize();
            let mut rng = u64::from_le_bytes(digest[0..8].try_into().unwrap_or([1; 8]));

            let mut px = base_px;
            let mut bars = Vec::with_capacity(total_bars);
            for t in 0..total_bars {
                rng = rng
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let u = ((rng >> 11) as f64) / ((1u64 << 53) as f64) - 0.5;
                let cycle_wave = ((t as f64) * 0.11 + (id as f64)).sin() * 0.006;
                let ret = drift + cycle_wave + u * 0.014;
                let open = px;
                px = (px * (1.0 + ret)).max(base_px * 0.25);
                let close = px;
                let high = open.max(close) * (1.0 + 0.006 + u.abs() * 0.005);
                let low = open.min(close) * (1.0 - 0.006 - u.abs() * 0.005);
                let vol_surge = if t % 17 == (id as usize % 17) {
                    2.3
                } else {
                    1.0 + u.abs() * 0.4
                };
                let ts = start_ts + (t as i64) * step_ms;

                bars.push(OhlcvBar {
                    id: (t + 1) as i64,
                    asset_id: id,
                    timeframe: "4h".to_string(),
                    timestamp_ms: ts,
                    open: round_dp(open, 4),
                    high: round_dp(high, 4),
                    low: round_dp(low, 4),
                    close: round_dp(close, 4),
                    volume: round_dp((day_vol / 6.0) * vol_surge, 2),
                    funding_rate: base_funding,
                    created_at_ms: ts,
                    updated_at_ms: ts,
                });
            }

            let last_close = bars.last().map(|b| b.close).unwrap_or(base_px);
            let prev_close = bars
                .get(bars.len().saturating_sub(7))
                .map(|b| b.close)
                .unwrap_or(base_px);

            assets.push(ExchangeAsset {
                id,
                symbol: format!("{coin}-USDC"),
                coin: coin.to_string(),
                asset_index: id as u32,
                sz_decimals: 2,
                tick_size: if base_px < 10.0 { 0.001 } else { 0.01 },
                lot_step_size: 0.01,
                max_leverage: 20.0,
                mark_price: last_close,
                mid_price: last_close,
                oracle_price: last_close,
                funding_rate_hourly: base_funding,
                day_volume_usdc: day_vol,
                open_interest_usdc: day_vol * 0.45,
                prev_day_price: prev_close,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
                created_at_ms: start_ts,
                updated_at_ms: start_ts,
            });
            bars_by_asset.insert(id, bars);
        }

        (assets, bars_by_asset)
    }

    /// Runs a walk-forward simulation on the canonical Hyperliquid dataset.
    pub fn run_default_simulation(&self) -> Result<BacktestReport> {
        let (assets, bars_by_asset) =
            Self::generate_canonical_dataset(self.config.days, self.config.warmup_bars);
        self.run_walk_forward(&assets, &bars_by_asset)
    }

    /// Executes a strictly causal walk-forward multi-sleeve (S1/S2/S3) simulation across `assets` and `bars_by_asset`.
    pub fn run_walk_forward(
        &self,
        assets: &[ExchangeAsset],
        bars_by_asset: &HashMap<i64, Vec<OhlcvBar>>,
    ) -> Result<BacktestReport> {
        let signal_engine = SignalEngine::new(self.config.allocator.clone());
        let risk_engine = RiskEngine::new(self.config.risk_limits.clone());

        let min_len = assets
            .iter()
            .filter_map(|a| bars_by_asset.get(&a.id).map(Vec::len))
            .min()
            .unwrap_or(0);

        let warmup = self
            .config
            .warmup_bars
            .min(min_len.saturating_sub(2))
            .max(1);
        let mut realized_cash_equity = self.config.initial_equity_usdc;
        let mut active_positions: HashMap<i64, PositionRecord> = HashMap::new();
        let mut closed_positions: Vec<PositionRecord> = Vec::new();
        let mut cooldown_until_ms: HashMap<i64, (i64, String)> = HashMap::new();
        let mut equity_curve: Vec<PortfolioSnapshot> = Vec::new();
        let mut bar_returns: Vec<f64> = Vec::new();

        let mut prev_total_equity = self.config.initial_equity_usdc;
        let mut peak_equity = self.config.initial_equity_usdc;
        let mut max_drawdown_pct: f64 = 0.0;
        let mut next_pos_id = 1i64;
        let mut stop_out_count = 0usize;
        let mut cooldown_blocks = 0usize;
        let mut total_fees_usdc = 0.0;
        let mut total_funding_usdc = 0.0;
        let mut s1_attribution_usdc = 0.0;
        let mut s2_attribution_usdc = 0.0;
        let mut s3_attribution_usdc = 0.0;

        for t in warmup..min_len {
            let mut point_assets = Vec::with_capacity(assets.len());
            let mut point_bars: HashMap<i64, Vec<OhlcvBar>> = HashMap::with_capacity(assets.len());
            let mut ts_ms = 1_700_000_000_000i64 + (t as i64) * 14_400_000;

            for asset in assets {
                if let Some(series) = bars_by_asset.get(&asset.id) {
                    let slice = series[..=t].to_vec();
                    let cur_bar = &slice[slice.len() - 1];
                    ts_ms = cur_bar.timestamp_ms;
                    let prev_day_px = slice
                        .get(slice.len().saturating_sub(7))
                        .map(|b| b.close)
                        .unwrap_or(cur_bar.open);

                    let mut a = asset.clone();
                    a.mark_price = cur_bar.close;
                    a.mid_price = cur_bar.close;
                    a.oracle_price = cur_bar.close;
                    a.prev_day_price = prev_day_px;
                    a.funding_rate_hourly = cur_bar.funding_rate;
                    a.updated_at_ms = ts_ms;

                    point_assets.push(a);
                    point_bars.insert(asset.id, slice);
                }
            }

            // 1. Promote WORKING orders from prior bar to OPEN (Order-to-Open Capital Commitment)
            let active_list: Vec<PositionRecord> = active_positions.values().cloned().collect();
            let pre_ledger = risk_engine.evaluate_ledger(realized_cash_equity, &active_list);

            for asset in &point_assets {
                let Some(pos) = active_positions.get_mut(&asset.id) else {
                    continue;
                };
                if PositionStatus::parse(&pos.status) == PositionStatus::Working {
                    let side = OrderSide::parse(&pos.side);
                    let bars = point_bars.get(&asset.id).map(Vec::as_slice).unwrap_or(&[]);
                    let atr = compute_atr_from_bars(
                        bars,
                        14,
                        asset.mark_price,
                        risk_engine.limits.protective_stop_default_pct,
                    );
                    let commit = risk_engine.confirm_order_fill_to_open(
                        side,
                        pos.entry_price,
                        pos.requested_quantity,
                        pos.reserved_margin_usdc,
                        true,
                        asset.tick_size,
                        Some(atr),
                        &pre_ledger,
                    );
                    pos.status = "OPEN".to_string();
                    pos.fsm_state = "FILLED_OPEN".to_string();
                    pos.filled_quantity = commit.filled_quantity;
                    pos.reserved_margin_usdc = 0.0;
                    pos.committed_notional_usdc = commit.committed_notional_usdc;
                    pos.protective_stop_px = commit.protective_stop_px;
                    pos.fees_paid_usdc += commit.fees_paid_usdc;
                    pos.opened_at_ms = Some(ts_ms);
                    pos.updated_at_ms = ts_ms;
                    realized_cash_equity -= commit.fees_paid_usdc;
                    total_fees_usdc += commit.fees_paid_usdc;
                }
            }

            // 2. Evaluate 1s/bar high-water Chandelier trailing stops & funding on OPEN positions
            let mut stopped_assets = Vec::new();
            for asset in &point_assets {
                let Some(pos) = active_positions.get_mut(&asset.id) else {
                    continue;
                };
                if PositionStatus::parse(&pos.status) != PositionStatus::Open {
                    continue;
                }
                let side = OrderSide::parse(&pos.side);
                let bars = point_bars.get(&asset.id).map(Vec::as_slice).unwrap_or(&[]);
                let atr = compute_atr_from_bars(
                    bars,
                    14,
                    asset.mark_price,
                    risk_engine.limits.protective_stop_default_pct,
                );

                // Funding accrual over 4h bar (-side.sign() * funding_rate_hourly * 4 * notional)
                let funding_cash =
                    -side.sign() * asset.funding_rate_hourly * 4.0 * pos.committed_notional_usdc;
                pos.funding_accrued_usdc = round_dp(pos.funding_accrued_usdc + funding_cash, 4);
                realized_cash_equity += funding_cash;
                total_funding_usdc += funding_cash;

                let stop_eval = risk_engine.evaluate_1s_stop_ratchet(
                    side,
                    pos.entry_price,
                    asset.mark_price,
                    pos.filled_quantity,
                    pos.protective_stop_px,
                    pos.highest_price_seen,
                    pos.lowest_price_seen,
                    Some(atr),
                    asset.tick_size,
                );

                pos.current_price = asset.mark_price;
                pos.highest_price_seen = stop_eval.highest_price_seen;
                pos.lowest_price_seen = stop_eval.lowest_price_seen;
                pos.protective_stop_px = stop_eval.new_protective_stop_px;
                pos.unrealized_pnl_usdc = stop_eval.unrealized_pnl_usdc;
                pos.updated_at_ms = ts_ms;

                if stop_eval.triggered {
                    stopped_assets.push((
                        asset.id,
                        asset.symbol.clone(),
                        asset.mark_price,
                        stop_eval
                            .exit_reason
                            .unwrap_or_else(|| "PROTECTIVE_STOP".to_string()),
                    ));
                }
            }

            for (aid, sym, exit_px, reason) in stopped_assets {
                if let Some(mut pos) = active_positions.remove(&aid) {
                    let side = OrderSide::parse(&pos.side);
                    let gross_pnl = (exit_px - pos.entry_price) * pos.filled_quantity * side.sign();
                    let exit_fee = round_dp(
                        exit_px * pos.filled_quantity * (self.config.taker_fee_bps / 10_000.0),
                        4,
                    );
                    let net_pnl = round_dp(gross_pnl - exit_fee, 4);
                    realized_cash_equity += net_pnl;
                    total_fees_usdc += exit_fee;

                    let w_sum = (pos.s1_weight_contrib.abs()
                        + pos.s2_weight_contrib.abs()
                        + pos.s3_weight_contrib.abs())
                    .max(1e-8);
                    s1_attribution_usdc += net_pnl * (pos.s1_weight_contrib.abs() / w_sum);
                    s2_attribution_usdc += net_pnl * (pos.s2_weight_contrib.abs() / w_sum);
                    s3_attribution_usdc += net_pnl * (pos.s3_weight_contrib.abs() / w_sum);

                    pos.status = "CLOSED".to_string();
                    pos.fsm_state = "CLOSED".to_string();
                    pos.exit_price = Some(exit_px);
                    pos.reserved_margin_usdc = 0.0;
                    pos.committed_notional_usdc = 0.0;
                    pos.realized_pnl_usdc =
                        round_dp(net_pnl - pos.fees_paid_usdc + pos.funding_accrued_usdc, 4);
                    pos.unrealized_pnl_usdc = 0.0;
                    pos.fees_paid_usdc = round_dp(pos.fees_paid_usdc + exit_fee, 4);
                    pos.closed_at_ms = Some(ts_ms);
                    pos.updated_at_ms = ts_ms;
                    pos.exit_reason = Some(reason.clone());
                    closed_positions.push(pos);
                    stop_out_count += 1;

                    let exp_ms = ts_ms + (self.config.stop_out_cooldown_bars as i64) * 14_400_000;
                    cooldown_until_ms.insert(aid, (exp_ms, sym));
                }
            }

            // 3. Build active cooldown views (`v_assets_in_cooldown`)
            cooldown_until_ms.retain(|_, (exp, _)| *exp > ts_ms);
            let active_cooldowns: Vec<CooldownAssetView> = cooldown_until_ms
                .iter()
                .map(|(&aid, (exp_ms, sym))| CooldownAssetView {
                    cooldown_id: aid,
                    asset_id: aid,
                    symbol: sym.clone(),
                    coin: sym.split('-').next().unwrap_or(sym).to_string(),
                    cooldown_type: "STOP_OUT".to_string(),
                    reason: "Post-stop cooldown".to_string(),
                    triggered_at_ms: ts_ms,
                    expires_at_ms: *exp_ms,
                    remaining_seconds: ((*exp_ms - ts_ms) / 1000).max(0),
                    updated_at_ms: ts_ms,
                })
                .collect();

            // 4. Evaluate Multi-Sleeve Signals (S1 + S2 + S3 + Breadth + Risk-Tier Allocator)
            let current_active_vec: Vec<PositionRecord> =
                active_positions.values().cloned().collect();
            let mid_ledger = risk_engine.evaluate_ledger(realized_cash_equity, &current_active_vec);

            let cycle_out = signal_engine.evaluate_universe_with_positions(
                &point_assets,
                &point_bars,
                &current_active_vec,
                &active_cooldowns,
                mid_ledger.total_equity_usdc,
            );

            // 5. Execute rebalance actions (`CLOSE`, `FLIP`, `OPEN_LONG`, `OPEN_SHORT`, `INCREASE`, `REDUCE`)
            for sig in &cycle_out.signals {
                if sig.rebalance_action == ACTION_COOLDOWN_BLOCKED {
                    cooldown_blocks += 1;
                    continue;
                }

                let Some(asset) = point_assets.iter().find(|a| a.id == sig.asset_id) else {
                    continue;
                };

                if matches!(
                    sig.rebalance_action.as_str(),
                    ACTION_CLOSE | ACTION_FLIP | ACTION_REDUCE
                ) {
                    if let Some(mut pos) = active_positions.remove(&sig.asset_id) {
                        if PositionStatus::parse(&pos.status) == PositionStatus::Open {
                            let side = OrderSide::parse(&pos.side);
                            let gross_pnl = (asset.mark_price - pos.entry_price)
                                * pos.filled_quantity
                                * side.sign();
                            let exit_fee = round_dp(
                                asset.mark_price
                                    * pos.filled_quantity
                                    * (self.config.maker_fee_bps / 10_000.0),
                                4,
                            );
                            let net_pnl = round_dp(gross_pnl - exit_fee, 4);
                            realized_cash_equity += net_pnl;
                            total_fees_usdc += exit_fee;

                            let w_sum = (pos.s1_weight_contrib.abs()
                                + pos.s2_weight_contrib.abs()
                                + pos.s3_weight_contrib.abs())
                            .max(1e-8);
                            s1_attribution_usdc += net_pnl * (pos.s1_weight_contrib.abs() / w_sum);
                            s2_attribution_usdc += net_pnl * (pos.s2_weight_contrib.abs() / w_sum);
                            s3_attribution_usdc += net_pnl * (pos.s3_weight_contrib.abs() / w_sum);

                            pos.status = "CLOSED".to_string();
                            pos.fsm_state = "CLOSED".to_string();
                            pos.exit_price = Some(asset.mark_price);
                            pos.reserved_margin_usdc = 0.0;
                            pos.committed_notional_usdc = 0.0;
                            pos.realized_pnl_usdc = round_dp(
                                net_pnl - pos.fees_paid_usdc + pos.funding_accrued_usdc,
                                4,
                            );
                            pos.unrealized_pnl_usdc = 0.0;
                            pos.fees_paid_usdc = round_dp(pos.fees_paid_usdc + exit_fee, 4);
                            pos.closed_at_ms = Some(ts_ms);
                            pos.updated_at_ms = ts_ms;
                            pos.exit_reason = Some(sig.rebalance_action.clone());
                            closed_positions.push(pos);
                        }
                    }
                }

                if matches!(
                    sig.rebalance_action.as_str(),
                    ACTION_OPEN_LONG | ACTION_OPEN_SHORT | ACTION_FLIP | ACTION_INCREASE
                ) && !active_positions.contains_key(&sig.asset_id)
                {
                    let cur_vec: Vec<PositionRecord> = active_positions.values().cloned().collect();
                    let cur_ledger = risk_engine.evaluate_ledger(realized_cash_equity, &cur_vec);
                    let bars = point_bars.get(&asset.id).map(Vec::as_slice).unwrap_or(&[]);
                    let atr = compute_atr_from_bars(
                        bars,
                        14,
                        asset.mark_price,
                        risk_engine.limits.protective_stop_default_pct,
                    );

                    let plan = risk_engine.size_working_order_from_weights(
                        asset,
                        sig.target_weight,
                        0.0,
                        sig.w_s1_carry_mom,
                        sig.w_s2_trend_breakout,
                        sig.w_s3_event_reversion,
                        &cur_ledger,
                        Some(atr),
                    );

                    if plan.approved {
                        let pid = next_pos_id;
                        next_pos_id += 1;
                        active_positions.insert(
                            asset.id,
                            PositionRecord {
                                id: pid,
                                asset_id: asset.id,
                                client_order_id: format!("bt-coid-{pid}"),
                                side: plan.side.as_str().to_string(),
                                status: "WORKING".to_string(),
                                fsm_state: "WORKING_ALO".to_string(),
                                entry_price: plan.limit_price,
                                current_price: plan.limit_price,
                                exit_price: None,
                                requested_quantity: plan.requested_quantity,
                                filled_quantity: 0.0,
                                reserved_margin_usdc: plan.reserved_margin_usdc,
                                committed_notional_usdc: 0.0,
                                portfolio_weight: plan.portfolio_weight,
                                s1_weight_contrib: plan.s1_weight_contrib,
                                s2_weight_contrib: plan.s2_weight_contrib,
                                s3_weight_contrib: plan.s3_weight_contrib,
                                protective_stop_px: plan.protective_stop_px,
                                protective_stop_oid: None,
                                highest_price_seen: plan.limit_price,
                                lowest_price_seen: plan.limit_price,
                                chase_count: 0,
                                realized_pnl_usdc: 0.0,
                                unrealized_pnl_usdc: 0.0,
                                fees_paid_usdc: 0.0,
                                funding_accrued_usdc: 0.0,
                                slippage_paid_usdc: 0.0,
                                exchange_order_id: Some(format!("hl-bt-{pid}")),
                                created_at_ms: ts_ms,
                                opened_at_ms: None,
                                closed_at_ms: None,
                                updated_at_ms: ts_ms,
                                exit_reason: None,
                            },
                        );
                    }
                }
            }

            // 6. Record end-of-bar PortfolioSnapshot
            let end_positions: Vec<PositionRecord> = active_positions.values().cloned().collect();
            let end_ledger = risk_engine.evaluate_ledger(realized_cash_equity, &end_positions);

            let bar_ret = if prev_total_equity > 0.0 {
                (end_ledger.total_equity_usdc - prev_total_equity) / prev_total_equity
            } else {
                0.0
            };
            bar_returns.push(bar_ret);
            prev_total_equity = end_ledger.total_equity_usdc;
            peak_equity = peak_equity.max(end_ledger.total_equity_usdc);
            if peak_equity > 0.0 {
                let dd = ((peak_equity - end_ledger.total_equity_usdc) / peak_equity) * 100.0;
                max_drawdown_pct = max_drawdown_pct.max(dd);
            }

            equity_curve.push(PortfolioSnapshot {
                id: (equity_curve.len() + 1) as i64,
                total_equity_usdc: end_ledger.total_equity_usdc,
                available_cash_usdc: end_ledger.available_cash_usdc,
                reserved_order_cash_usdc: end_ledger.reserved_order_cash_usdc,
                committed_notional_usdc: end_ledger.committed_notional_usdc,
                net_notional_usdc: end_ledger.net_notional_usdc,
                gross_leverage_x: end_ledger.gross_leverage_x,
                net_leverage_x: end_ledger.net_leverage_x,
                market_beta_exposure: cycle_out.breadth.market_beta_exposure,
                market_breadth_bull: cycle_out.breadth.market_breadth_bull,
                market_regime_label: cycle_out.breadth.market_regime_label,
                risk_tier: cycle_out.risk_tier.as_str().to_string(),
                leverage_multiplier: cycle_out.leverage_multiplier,
                s1_gross_weight: cycle_out.s1_gross_weight,
                s2_gross_weight: cycle_out.s2_gross_weight,
                s3_gross_weight: cycle_out.s3_gross_weight,
                working_order_count: end_ledger.working_order_count,
                open_long_count: end_ledger.open_long_count,
                open_short_count: end_ledger.open_short_count,
                eligible_coin_count: cycle_out.breadth.eligible_coin_count,
                created_at_ms: ts_ms,
                updated_at_ms: ts_ms,
            });
        }

        let final_equity_usdc = equity_curve
            .last()
            .map(|s| s.total_equity_usdc)
            .unwrap_or(self.config.initial_equity_usdc);
        let total_return_pct = round_dp(
            ((final_equity_usdc - self.config.initial_equity_usdc)
                / self.config.initial_equity_usdc.max(1.0))
                * 100.0,
            2,
        );

        let n_rets = bar_returns.len().max(1) as f64;
        let mean_ret = bar_returns.iter().sum::<f64>() / n_rets;
        let var_ret = bar_returns
            .iter()
            .map(|r| {
                let d = *r - mean_ret;
                d * d
            })
            .sum::<f64>()
            / (n_rets - 1.0).max(1.0);
        let std_ret = var_ret.sqrt();
        let down_var = bar_returns
            .iter()
            .map(|r| {
                let d = r.min(0.0);
                d * d
            })
            .sum::<f64>()
            / (n_rets - 1.0).max(1.0);
        let down_std = down_var.sqrt();
        let ann_factor = (365.0_f64 * 6.0).sqrt();

        let sharpe_ratio = if std_ret > 1e-9 {
            round_dp((mean_ret / std_ret) * ann_factor, 2)
        } else {
            0.0
        };
        let sortino_ratio = if down_std > 1e-9 {
            round_dp((mean_ret / down_std) * ann_factor, 2)
        } else {
            sharpe_ratio
        };

        let total_trades = closed_positions.len();
        let winning_trades = closed_positions
            .iter()
            .filter(|p| p.realized_pnl_usdc > 0.0)
            .count();
        let losing_trades = closed_positions
            .iter()
            .filter(|p| p.realized_pnl_usdc <= 0.0)
            .count();
        let win_rate_pct = if total_trades > 0 {
            round_dp((winning_trades as f64 / total_trades as f64) * 100.0, 2)
        } else {
            0.0
        };

        let gross_profit: f64 = closed_positions
            .iter()
            .filter(|p| p.realized_pnl_usdc > 0.0)
            .map(|p| p.realized_pnl_usdc)
            .sum();
        let gross_loss: f64 = closed_positions
            .iter()
            .filter(|p| p.realized_pnl_usdc < 0.0)
            .map(|p| p.realized_pnl_usdc.abs())
            .sum();
        let profit_factor = if gross_loss > 1e-6 {
            round_dp(gross_profit / gross_loss, 2)
        } else if gross_profit > 0.0 {
            9.99
        } else {
            1.0
        };

        Ok(BacktestReport {
            platform_version: "2.0.0".to_string(),
            risk_tier: self.config.risk_tier.as_str().to_string(),
            leverage_multiplier: self.config.allocator.effective_leverage_multiplier(),
            initial_equity_usdc: self.config.initial_equity_usdc,
            final_equity_usdc: round_dp(final_equity_usdc, 2),
            total_return_pct,
            sharpe_ratio,
            sortino_ratio,
            max_drawdown_pct: round_dp(max_drawdown_pct, 2),
            win_rate_pct,
            profit_factor,
            total_trades,
            winning_trades,
            losing_trades,
            stop_out_count,
            cooldown_blocks,
            s1_attribution_usdc: round_dp(s1_attribution_usdc, 2),
            s2_attribution_usdc: round_dp(s2_attribution_usdc, 2),
            s3_attribution_usdc: round_dp(s3_attribution_usdc, 2),
            total_fees_usdc: round_dp(total_fees_usdc, 2),
            total_funding_usdc: round_dp(total_funding_usdc, 2),
            bars_evaluated: equity_curve.len(),
            assets_evaluated: assets.len(),
            equity_curve,
            closed_positions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_sleeve_walk_forward_backtest() {
        let engine = BacktestEngine::new(BacktestConfig {
            days: 10,
            warmup_bars: 15,
            ..BacktestConfig::default()
        });
        let report = engine.run_default_simulation().unwrap();

        assert_eq!(report.platform_version, "2.0.0");
        assert_eq!(report.risk_tier, "extreme");
        assert!((report.leverage_multiplier - 1.85).abs() < 1e-9);
        assert!(report.bars_evaluated >= 50);
        assert_eq!(report.assets_evaluated, 8);
        assert!(report.final_equity_usdc > 0.0);
        assert!(!report.equity_curve.is_empty());
    }
}
