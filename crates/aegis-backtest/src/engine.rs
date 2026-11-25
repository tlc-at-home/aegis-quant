//! Aegis Quant v2.0 Multi-Sleeve Walk-Forward Backtest Orchestrator (`aegis-backtest`).

use crate::config::{BacktestConfig, BacktestReport};
use crate::dataset::{
    count_timeline_bars_before, generate_canonical_dataset as build_canonical_dataset,
    HistoricalDataset,
};
use crate::metrics::compute_backtest_report;
use crate::settlement::{
    build_active_cooldown_views, execute_rebalance_actions, fill_working_orders,
    finalize_open_attribution, process_intrabar_stops_and_funding, record_bar_snapshot,
    SimulationState,
};
use aegis_core::indicators::timeframe_to_ms;
use aegis_core::{ConfigMap, ExchangeAsset, OhlcvBar, Result};
use aegis_risk::RiskEngine;
use aegis_signals::SignalEngine;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};

/// High-fidelity, zero-lookahead walk-forward simulation engine unifying S1/S2/S3, RiskEngine, and live-gate parity.
#[derive(Debug, Clone)]
pub struct BacktestEngine {
    pub config: BacktestConfig,
}

impl BacktestEngine {
    pub fn new(config: BacktestConfig) -> Self {
        Self { config }
    }

    pub fn from_config_map(cfg: &ConfigMap, days: usize) -> Self {
        Self::new(BacktestConfig::from_config_map(cfg, days))
    }

    /// Generates a deterministic multi-asset Hyperliquid perpetual dataset for synthetic walk-forward runs.
    pub fn generate_canonical_dataset(
        days: usize,
        warmup_bars: usize,
    ) -> (Vec<ExchangeAsset>, HashMap<i64, Vec<OhlcvBar>>) {
        build_canonical_dataset(days, warmup_bars)
    }

    /// Runs a walk-forward simulation on the canonical Hyperliquid dataset.
    pub fn run_default_simulation(&self) -> Result<BacktestReport> {
        let (assets, bars_by_asset) =
            Self::generate_canonical_dataset(self.config.days, self.config.warmup_bars);
        self.run_walk_forward(&assets, &bars_by_asset)
    }

    /// Runs a walk-forward simulation against a pre-assembled [`HistoricalDataset`] (`--source db`, `mainnet`, or `synthetic`).
    pub fn run_dataset(&self, dataset: &HistoricalDataset) -> Result<BacktestReport> {
        if let Some(cutoff_ms) = dataset.eval_start_ms {
            let eval_warmup = count_timeline_bars_before(&dataset.bars_by_asset, cutoff_ms);
            if eval_warmup > 0 {
                let mut cloned = self.clone();
                cloned.config.warmup_bars = eval_warmup;
                return cloned.run_walk_forward(&dataset.assets, &dataset.bars_by_asset);
            }
        }
        self.run_walk_forward(&dataset.assets, &dataset.bars_by_asset)
    }

    /// Executes a strictly causal walk-forward multi-sleeve (S1/S2/S3) simulation across `assets` and `bars_by_asset`.
    ///
    /// Uses monotonic per-asset cursors and zero-copy rolling `&[OhlcvBar]` slices (`R14-L22`),
    /// delegating fill/stop/funding/rebalance settlement to `settlement` and reporting to `metrics`.
    pub fn run_walk_forward(
        &self,
        assets: &[ExchangeAsset],
        bars_by_asset: &HashMap<i64, Vec<OhlcvBar>>,
    ) -> Result<BacktestReport> {
        let signal_engine = SignalEngine::new(self.config.allocator.clone());
        let mut risk_limits = self.config.risk_limits.clone();
        risk_limits.maker_fee_bps = self.config.maker_fee_bps;
        risk_limits.taker_fee_bps = self.config.taker_fee_bps;
        let risk_engine = RiskEngine::new(risk_limits);

        let mut sorted_bars: HashMap<i64, Cow<'_, [OhlcvBar]>> =
            HashMap::with_capacity(assets.len());
        let mut cursor_by_asset: HashMap<i64, usize> = HashMap::with_capacity(assets.len());
        let mut ts_set = BTreeSet::new();
        let mut inferred_tf = String::new();

        for asset in assets {
            if let Some(series) = bars_by_asset.get(&asset.id) {
                let ordered: Cow<'_, [OhlcvBar]> = if series
                    .windows(2)
                    .all(|w| w[0].timestamp_ms < w[1].timestamp_ms)
                {
                    Cow::Borrowed(series.as_slice())
                } else {
                    let mut owned = series.clone();
                    owned.sort_by_key(|b| b.timestamp_ms);
                    owned.dedup_by_key(|b| b.timestamp_ms);
                    Cow::Owned(owned)
                };
                if inferred_tf.is_empty() {
                    if let Some(tf) = ordered
                        .first()
                        .map(|b| b.timeframe.trim())
                        .filter(|s| !s.is_empty())
                    {
                        inferred_tf = tf.to_string();
                    }
                }
                for b in ordered.iter() {
                    ts_set.insert(b.timestamp_ms);
                }
                sorted_bars.insert(asset.id, ordered);
                cursor_by_asset.insert(asset.id, 0);
            }
        }
        if inferred_tf.is_empty() {
            inferred_tf = "4h".to_string();
        }
        let dominant_tf_ms = timeframe_to_ms(&inferred_tf);
        let timeline: Vec<i64> = ts_set.into_iter().collect();
        let warmup = self
            .config
            .warmup_bars
            .min(timeline.len().saturating_sub(2))
            .max(1);
        let mut state = SimulationState::new(self.config.initial_equity_usdc);

        for t_idx in warmup..timeline.len() {
            let ts_ms = timeline[t_idx];
            let step_ms = (ts_ms - timeline[t_idx - 1]).max(60_000);
            let bar_duration_ms = dominant_tf_ms.unwrap_or(step_ms).max(60_000);
            let bar_close_ts_ms = ts_ms + bar_duration_ms;
            let bar_hours = (step_ms as f64) / 3_600_000.0;
            state.bar_hours_sum += bar_hours;
            state.bar_hours_samples += 1;
            state.first_evaluated_ts_ms.get_or_insert(ts_ms);

            let mut point_assets = Vec::with_capacity(assets.len());
            let mut point_bars: HashMap<i64, &[OhlcvBar]> = HashMap::with_capacity(assets.len());
            let mut cur_bars_by_asset: HashMap<i64, &OhlcvBar> =
                HashMap::with_capacity(assets.len());

            for asset in assets {
                let Some(series) = sorted_bars.get(&asset.id) else {
                    continue;
                };
                let cursor = cursor_by_asset.entry(asset.id).or_insert(0);
                while *cursor < series.len() && series[*cursor].timestamp_ms <= ts_ms {
                    *cursor += 1;
                }
                let count_up_to = *cursor;
                if count_up_to == 0 {
                    continue;
                }
                let rolling = &series[count_up_to.saturating_sub(480)..count_up_to];
                let prefix = &series[..count_up_to];
                let cur_bar = &rolling[rolling.len() - 1];
                if cur_bar.timestamp_ms == ts_ms {
                    cur_bars_by_asset.insert(asset.id, cur_bar);
                }

                let prev_idx = prefix
                    .partition_point(|b| b.timestamp_ms <= ts_ms - 86_400_000)
                    .saturating_sub(1);
                let prev_day_px = prefix
                    .get(prev_idx)
                    .map(|b| b.close)
                    .unwrap_or(cur_bar.open);
                let rolling_24h_vol: f64 = prefix
                    .iter()
                    .rev()
                    .take_while(|b| b.timestamp_ms > ts_ms - 86_400_000)
                    .map(|b| {
                        let v = b.volume.max(0.0);
                        let px = b.close.max(0.0);
                        if v.is_finite() && px.is_finite() {
                            v * px
                        } else {
                            0.0
                        }
                    })
                    .sum();

                let mut point_asset = asset.clone();
                point_asset.mark_price = cur_bar.close;
                point_asset.mid_price = cur_bar.close;
                point_asset.oracle_price = cur_bar.close;
                point_asset.prev_day_price = prev_day_px;
                point_asset.day_volume_usdc = if rolling_24h_vol > 0.0 {
                    rolling_24h_vol
                } else {
                    asset.day_volume_usdc
                };
                point_asset.funding_rate_hourly = cur_bar.funding_rate;
                point_asset.updated_at_ms = ts_ms;

                point_assets.push(point_asset);
                point_bars.insert(asset.id, rolling);
            }

            let just_opened = fill_working_orders(
                &point_assets,
                &point_bars,
                &cur_bars_by_asset,
                ts_ms,
                self.config.maker_fee_bps,
                &risk_engine,
                &mut state,
            )?;
            process_intrabar_stops_and_funding(
                &point_assets,
                &point_bars,
                &cur_bars_by_asset,
                &just_opened,
                bar_close_ts_ms,
                bar_duration_ms,
                bar_hours,
                &self.config,
                &risk_engine,
                &mut state,
            )?;

            let active_cooldowns = build_active_cooldown_views(bar_close_ts_ms, &mut state);
            let active_list = state.active_positions_snapshot();
            let mid_ledger = risk_engine.evaluate_ledger(state.realized_cash_equity, &active_list);
            let cycle_out = signal_engine.evaluate_universe_with_positions(
                &point_assets,
                &point_bars,
                &active_list,
                &active_cooldowns,
                mid_ledger.total_equity_usdc,
            );

            execute_rebalance_actions(
                &point_assets,
                &point_bars,
                &cycle_out,
                &mid_ledger,
                bar_close_ts_ms,
                &self.config,
                &risk_engine,
                &mut state,
            )?;
            record_bar_snapshot(bar_close_ts_ms, &cycle_out, &risk_engine, &mut state);
        }

        finalize_open_attribution(&mut state)?;
        Ok(compute_backtest_report(
            &self.config,
            inferred_tf,
            assets.len(),
            state,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::assemble_db_dataset;
    use aegis_core::AegisError;

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

        let sleeve_sum =
            report.s1_attribution_usdc + report.s2_attribution_usdc + report.s3_attribution_usdc;
        let total_pnl = report.final_equity_usdc - report.initial_equity_usdc;
        assert!(
            (sleeve_sum - total_pnl).abs() < 0.25,
            "Sleeve attribution sum ({sleeve_sum:.2}) must reconcile with total PnL ({total_pnl:.2}) including open positions"
        );
    }

    #[test]
    fn test_walk_forward_timestamp_alignment_limit_range_and_intrabar_stop() {
        // Asset 1 (BTC) runs t=0..30; Asset 2 (SOL) starts later at t=10..30, rallies, then wicks down intra-bar to hit SL while closing above SL
        let (mut assets, mut bars_by_asset) = BacktestEngine::generate_canonical_dataset(10, 15);
        assets.truncate(2);
        let btc_id = assets[0].id;
        let sol_id = assets[1].id;

        if let Some(sol_bars) = bars_by_asset.get_mut(&sol_id) {
            sol_bars.drain(0..10);
            let n = sol_bars.len();
            let mut px = sol_bars[n - 8].close;
            for bar in &mut sol_bars[(n - 7)..(n - 1)] {
                let open = px;
                px *= 1.03;
                bar.open = open;
                bar.close = px;
                bar.high = px * 1.005;
                bar.low = open * 0.995;
                bar.volume = 10_000_000.0;
            }
            sol_bars[n - 1].open = px;
            sol_bars[n - 1].high = px * 1.002;
            sol_bars[n - 1].low = px * 0.85;
            sol_bars[n - 1].close = px;
        }

        let mut filtered_bars = HashMap::new();
        filtered_bars.insert(btc_id, bars_by_asset.remove(&btc_id).unwrap());
        filtered_bars.insert(sol_id, bars_by_asset.remove(&sol_id).unwrap());

        let engine = BacktestEngine::new(BacktestConfig {
            days: 10,
            warmup_bars: 15,
            slippage_bps: 10.0,
            ..BacktestConfig::default()
        });
        let report = engine.run_walk_forward(&assets, &filtered_bars).unwrap();
        assert_eq!(report.assets_evaluated, 2);
        assert!(report.bars_evaluated >= 45);
        assert!(
            report.stop_out_count >= 1 || report.total_trades >= 1,
            "Expected intra-bar wick stop or rebalance trade during walk-forward"
        );
    }

    #[test]
    fn test_r14_m14_m15_l22_backtest_live_gate_parity_mark_funding_bar_close_ts_sortino() {
        // 1. R14-M15 Causal DB dataset validation & no silent fallback
        let err_empty =
            assemble_db_dataset(vec![], HashMap::new(), HashMap::new(), 24).unwrap_err();
        assert!(matches!(err_empty, AegisError::Validation(_)));

        let (assets, mut bars_by_asset) = BacktestEngine::generate_canonical_dataset(10, 15);
        let err_short =
            assemble_db_dataset(assets.clone(), bars_by_asset.clone(), HashMap::new(), 500)
                .unwrap_err();
        assert!(matches!(err_short, AegisError::Validation(_)));

        // Causal funding merge: bar 5 zeroed out, bar 6 retains original rate, bar 5 gets only events in [open, close)
        let btc_id = assets[0].id;
        let b5_open = bars_by_asset.get(&btc_id).unwrap()[5].timestamp_ms;
        if let Some(btc_bars) = bars_by_asset.get_mut(&btc_id) {
            btc_bars[5].funding_rate = 0.0;
            btc_bars[6].funding_rate = 0.000123;
        }
        let mut funding_events = HashMap::new();
        funding_events.insert(
            btc_id,
            vec![
                (b5_open - 1000, 0.0099),       // before bar 5 -> excluded
                (b5_open + 3_600_000, 0.0002),  // inside bar 5 -> included
                (b5_open + 7_200_000, 0.0004),  // inside bar 5 -> average 0.0003
                (b5_open + 14_400_000, 0.0088), // at bar 6 open (bar_close) -> excluded from bar 5 & bar 6 non-zero
            ],
        );
        let ds =
            assemble_db_dataset(assets.clone(), bars_by_asset.clone(), funding_events, 20).unwrap();
        assert_eq!(ds.source, "db");
        assert_eq!(ds.timeframe, "4h");
        let b5 = &ds.bars_by_asset.get(&btc_id).unwrap()[5];
        let b6 = &ds.bars_by_asset.get(&btc_id).unwrap()[6];
        assert!((b5.funding_rate - 0.0003).abs() < 1e-12);
        assert!((b6.funding_rate - 0.000123).abs() < 1e-12);

        // 2. R14-M14 max_open_positions cap, R14-L22 bar_close_ts snapshots, closed_at_ms causality, & days_evaluated
        let limited_engine = BacktestEngine::new(BacktestConfig {
            days: 10,
            warmup_bars: 15,
            max_open_positions: 2,
            ..BacktestConfig::default()
        });
        let report = limited_engine.run_dataset(&ds).unwrap();
        assert_eq!(report.timeframe, "4h");
        assert!(report.days_evaluated >= 9 && report.days_evaluated <= 12);
        for snap in &report.equity_curve {
            let total_pos = snap.working_order_count + snap.open_long_count + snap.open_short_count;
            assert!(
                total_pos <= 2,
                "max_open_positions=2 violated: working={} long={} short={}",
                snap.working_order_count,
                snap.open_long_count,
                snap.open_short_count
            );
            // Snapshots stamped at bar_close_ts_ms (b_open + 4h = multiples of 14_400_000 from 1_700_000_000_000)
            assert_eq!((snap.created_at_ms - 1_700_000_000_000) % 14_400_000, 0);
        }
        assert!(
            report.equity_curve[0].created_at_ms
                > ds.bars_by_asset.get(&btc_id).unwrap()[15].timestamp_ms
        );
        for closed in &report.closed_positions {
            if let (Some(opened), Some(closed_ts)) = (closed.opened_at_ms, closed.closed_at_ms) {
                assert!(
                    opened < closed_ts,
                    "opened {opened} must strictly precede bar_close {closed_ts}"
                );
            }
        }

        // 3. R14-L22 Sortino denominator check (N=3, [0.10, -0.10, -0.10]): down_var = 0.02 / 2 = 0.01, std = 0.10
        let mut sortino_state = SimulationState::new(1000.0);
        sortino_state.bar_returns = vec![0.10, -0.10, -0.10];
        sortino_state.bar_hours_sum = 24.0 * 3.0;
        sortino_state.bar_hours_samples = 3;
        sortino_state.first_evaluated_ts_ms = Some(1_700_000_000_000);
        sortino_state.last_bar_close_ts_ms = Some(1_700_000_000_000 + 3 * 86_400_000);
        let sortino_report = compute_backtest_report(
            &BacktestConfig::default(),
            "1d".to_string(),
            1,
            sortino_state,
        );
        // mean = -0.10/3, down_std (n-1 denom) = sqrt(0.02 / 2) = 0.10, ann = sqrt(365)
        let expected_sortino = ((-0.10 / 3.0) / 0.10) * 365.0_f64.sqrt();
        assert!(
            (sortino_report.sortino_ratio - (expected_sortino * 100.0).round() / 100.0).abs()
                < 0.02,
            "Sortino {} did not use (n-1) sample denominator (expected {:.2})",
            sortino_report.sortino_ratio,
            expected_sortino
        );
        assert_eq!(sortino_report.days_evaluated, 3);

        // 4. R14-L22 mark-notional funding parity: verify funding accrues on mark_price * qty rather than entry_price * qty
        // Construct 2-asset trending market where asset 1 goes up 2x before close while carrying positive funding_rate
        let mut fund_assets = assets[..2].to_vec();
        let mut fund_bars: HashMap<i64, Vec<OhlcvBar>> = HashMap::new();
        for (idx, a) in fund_assets.iter_mut().enumerate() {
            a.funding_rate_hourly = 0.01;
            let mut series = Vec::new();
            let mut px = 100.0;
            for t in 0..35 {
                let step_ret = if idx == 0 {
                    if t >= 20 {
                        0.04
                    } else {
                        0.01
                    }
                } else {
                    -0.005
                };
                let open = px;
                px *= 1.0 + step_ret;
                series.push(OhlcvBar {
                    id: t + 1,
                    asset_id: a.id,
                    timeframe: "4h".to_string(),
                    timestamp_ms: 1_700_000_000_000 + t * 14_400_000,
                    open,
                    high: px.max(open) * 1.002,
                    low: px.min(open) * 0.998,
                    close: px,
                    volume: 20_000_000.0,
                    funding_rate: if t >= 22 { 0.005 } else { 0.0 },
                    created_at_ms: 1_700_000_000_000 + t * 14_400_000,
                    updated_at_ms: 1_700_000_000_000 + t * 14_400_000,
                });
            }
            fund_bars.insert(a.id, series);
        }
        let fund_report = BacktestEngine::new(BacktestConfig {
            days: 5,
            warmup_bars: 15,
            ..BacktestConfig::default()
        })
        .run_walk_forward(&fund_assets, &fund_bars)
        .unwrap();
        assert!(
            fund_report.total_funding_usdc.abs() > 0.0,
            "Expected non-zero mark-notional funding accrual"
        );

        // 5. R14-M14 drawdown halt + ruin halt on severe gap crash
        let mut crash_assets = assets[..4].to_vec();
        let mut crash_bars: HashMap<i64, Vec<OhlcvBar>> = HashMap::new();
        for (idx, a) in crash_assets.iter_mut().enumerate() {
            let mut series = Vec::new();
            let mut px = 100.0;
            for t in 0..32 {
                let open = px;
                if t < 24 {
                    let r = match idx {
                        0 => 0.025,
                        1 => 0.015,
                        2 => 0.004,
                        _ => -0.008,
                    };
                    px *= 1.0 + r;
                    series.push(OhlcvBar {
                        id: t + 1,
                        asset_id: a.id,
                        timeframe: "4h".to_string(),
                        timestamp_ms: 1_700_000_000_000 + t * 14_400_000,
                        open,
                        high: px.max(open) * 1.002,
                        low: px.min(open) * 0.998,
                        close: px,
                        volume: 20_000_000.0,
                        funding_rate: 0.0,
                        created_at_ms: 1_700_000_000_000 + t * 14_400_000,
                        updated_at_ms: 1_700_000_000_000 + t * 14_400_000,
                    });
                } else {
                    // Gap shock against all open leaders/laggards
                    let mult = if idx <= 1 { 0.45 } else { 1.45 };
                    let gap_open = open * mult;
                    px = gap_open * 0.99;
                    series.push(OhlcvBar {
                        id: t + 1,
                        asset_id: a.id,
                        timeframe: "4h".to_string(),
                        timestamp_ms: 1_700_000_000_000 + t * 14_400_000,
                        open: gap_open,
                        high: gap_open.max(px) * 1.002,
                        low: gap_open.min(px) * 0.998,
                        close: px,
                        volume: 20_000_000.0,
                        funding_rate: 0.0,
                        created_at_ms: 1_700_000_000_000 + t * 14_400_000,
                        updated_at_ms: 1_700_000_000_000 + t * 14_400_000,
                    });
                }
            }
            crash_bars.insert(a.id, series);
        }
        let mut crash_cfg = BacktestConfig {
            days: 5,
            warmup_bars: 15,
            initial_equity_usdc: 10_000.0,
            max_daily_drawdown_pct: 0.03,
            ..BacktestConfig::default()
        };
        crash_cfg.risk_limits.min_equity_halt_usdc = 9_700.0;
        let crash_report = BacktestEngine::new(crash_cfg)
            .run_walk_forward(&crash_assets, &crash_bars)
            .unwrap();
        assert!(
            crash_report.drawdown_halt_bars > 0 && crash_report.ruin_halted,
            "Expected 24h drawdown halt ({}) AND ruin halt ({}) during gap crash",
            crash_report.drawdown_halt_bars,
            crash_report.ruin_halted
        );

        // 6. R14-M14 min_entry_interval_sec pacing block on rapid 1m bars (60s spacing < 150s interval)
        let mut fast_assets = assets[..3].to_vec();
        let mut fast_bars: HashMap<i64, Vec<OhlcvBar>> = HashMap::new();
        for (i, a) in fast_assets.iter_mut().enumerate() {
            let mut series = Vec::new();
            let mut px = 100.0;
            for t in 0..30 {
                let open = px;
                let r = match i {
                    0 => 0.02,
                    1 => 0.015,
                    _ => -0.015,
                };
                px *= 1.0 + r;
                let ts = 1_700_000_000_000 + t * 60_000;
                series.push(OhlcvBar {
                    id: t + 1,
                    asset_id: a.id,
                    timeframe: "1m".to_string(),
                    timestamp_ms: ts,
                    open,
                    high: px.max(open) * 1.002,
                    low: px.min(open) * 0.998,
                    close: px,
                    volume: 20_000_000.0,
                    funding_rate: 0.0,
                    created_at_ms: ts,
                    updated_at_ms: ts,
                });
            }
            fast_bars.insert(a.id, series);
        }
        let fast_report = BacktestEngine::new(BacktestConfig {
            days: 5,
            warmup_bars: 15,
            min_entry_interval_sec: 150,
            ..BacktestConfig::default()
        })
        .run_walk_forward(&fast_assets, &fast_bars)
        .unwrap();
        assert_eq!(fast_report.timeframe, "1m");
        assert!(
            fast_report.entry_interval_blocks > 0 || fast_report.total_trades > 0,
            "Expected min_entry_interval_sec pacing on 1m bars"
        );
    }

    #[test]
    fn test_r14_m14_l22_exact_mark_funding_profit_gate_chase_ioc_and_wall_clock_cooldown() {
        use aegis_core::{PositionRecord, RiskTier};
        use aegis_signals::{MarketBreadthSnapshot, SignalCycleOutput, SleeveSignalInput};
        use std::collections::HashSet;

        let (mut assets, _bars_by_asset) = BacktestEngine::generate_canonical_dataset(10, 15);
        let mut btc = assets.remove(0);
        btc.mark_price = 200.0;
        btc.mid_price = 200.0;
        btc.oracle_price = 200.0;
        btc.funding_rate_hourly = 0.01;
        btc.tick_size = 0.01;
        btc.lot_step_size = 0.01;

        let mut cfg = BacktestConfig {
            profit_gate_signal_exits: true,
            signal_exit_min_net_profit_pct: 0.0025,
            max_alo_chase_ticks: 1,
            max_alo_chase_drift_bps: 60.0,
            ..BacktestConfig::default()
        };
        cfg.risk_limits.stop_out_cooldown_sec = 1800;
        let risk_engine = RiskEngine::new(cfg.risk_limits.clone());

        let bar_ts = 1_700_000_000_000i64;
        let cur_bar = OhlcvBar {
            id: 1,
            asset_id: btc.id,
            timeframe: "4h".to_string(),
            timestamp_ms: bar_ts,
            open: 200.0,
            high: 205.0,
            low: 195.0,
            close: 200.0,
            volume: 10_000_000.0,
            funding_rate: 0.01,
            created_at_ms: bar_ts,
            updated_at_ms: bar_ts,
        };
        let point_bars_vec = vec![cur_bar.clone(); 20];
        let mut point_bars = HashMap::new();
        point_bars.insert(btc.id, point_bars_vec.as_slice());
        let mut cur_bars = HashMap::new();
        cur_bars.insert(btc.id, &cur_bar);

        let mk_pos = |id: i64,
                      status: &str,
                      entry: f64,
                      cur: f64,
                      qty: f64,
                      stop: f64,
                      tp: f64,
                      chase_count: u32| {
            let is_open = status == "OPEN";
            PositionRecord {
                id,
                asset_id: btc.id,
                client_order_id: format!("pos-{id}"),
                side: "LONG".to_string(),
                status: status.to_string(),
                fsm_state: if is_open {
                    "FILLED_OPEN"
                } else {
                    "CHASING_ALO"
                }
                .to_string(),
                entry_price: entry,
                current_price: cur,
                exit_price: None,
                requested_quantity: qty,
                filled_quantity: if is_open { qty } else { 0.0 },
                reserved_margin_usdc: if is_open { 0.0 } else { entry * qty },
                committed_notional_usdc: if is_open { entry * qty } else { 0.0 },
                portfolio_weight: 0.05,
                s1_weight_contrib: 1.0,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: stop,
                protective_stop_oid: None,
                take_profit_px: tp,
                take_profit_oid: None,
                highest_price_seen: cur.max(entry),
                lowest_price_seen: cur.min(entry),
                chase_count,
                realized_pnl_usdc: 0.0,
                unrealized_pnl_usdc: if is_open { (cur - entry) * qty } else { 0.0 },
                fees_paid_usdc: 0.1,
                funding_accrued_usdc: 0.0,
                slippage_paid_usdc: 0.0,
                exchange_order_id: Some(format!("hl-{id}")),
                created_at_ms: bar_ts - 14_400_000,
                opened_at_ms: if is_open {
                    Some(bar_ts - 14_400_000)
                } else {
                    None
                },
                closed_at_ms: None,
                updated_at_ms: bar_ts - 14_400_000,
                exit_reason: None,
            }
        };

        // 1. Mark-notional funding: entry=100, qty=2, mark=200 => mark_notional=400 => -0.01 * 4 * 400 = -16.0 (NOT -8.0!)
        let mut state = SimulationState::new(10_000.0);
        state
            .active_positions
            .insert(btc.id, mk_pos(1, "OPEN", 100.0, 200.0, 2.0, 80.0, 250.0, 0));
        let point_assets = vec![btc.clone()];
        process_intrabar_stops_and_funding(
            &point_assets,
            &point_bars,
            &cur_bars,
            &HashSet::new(),
            bar_ts + 14_400_000,
            14_400_000,
            4.0,
            &cfg,
            &risk_engine,
            &mut state,
        )
        .unwrap();
        assert!(
            (state.total_funding_usdc - (-16.0)).abs() < 1e-6,
            "Expected mark-notional funding -16.0, got {}",
            state.total_funding_usdc
        );

        // 2. Wall-clock 1800s stop-out cooldown expires before next 4h bar close (bar_ts + 14_400_000)
        state
            .active_positions
            .get_mut(&btc.id)
            .unwrap()
            .protective_stop_px = 197.0;
        process_intrabar_stops_and_funding(
            &point_assets,
            &point_bars,
            &cur_bars,
            &HashSet::new(),
            bar_ts + 14_400_000,
            14_400_000,
            4.0,
            &cfg,
            &risk_engine,
            &mut state,
        )
        .unwrap();
        assert_eq!(state.stop_out_count, 1);
        let exp = state.cooldown_until_ms.get(&btc.id).unwrap().0;
        assert_eq!(exp, bar_ts + 14_400_000 + 1_800_000);
        let cds_after_4h = build_active_cooldown_views(bar_ts + 28_800_000, &mut state);
        assert!(
            cds_after_4h.is_empty(),
            "1800s stop cooldown must expire within 4h"
        );

        // 3. Profit gate blocks underwater CLOSE when profit_gate_signal_exits=true, but FLIP is exempt
        let mk_sig = |action: &str, target_w: f64| SignalCycleOutput {
            signals: vec![SleeveSignalInput {
                asset_id: btc.id,
                symbol: btc.symbol.clone(),
                coin: btc.coin.clone(),
                mark_price: 99.8,
                day_volume_usdc: 20_000_000.0,
                funding_rate_hourly: 0.0,
                annualized_vol: 0.5,
                market_beta: 1.0,
                s1_mom_score: 0.0,
                s1_carry_score: 0.0,
                s2_trend_alignment: 0,
                s2_vol_surge_ratio: 1.0,
                s3_funding_zscore: 0.0,
                s3_flush_atr_mult: 0.0,
                w_s1_carry_mom: target_w,
                w_s2_trend_breakout: 0.0,
                w_s3_event_reversion: 0.0,
                raw_combined_weight: target_w,
                target_weight: target_w,
                current_weight: 0.1,
                delta_weight: target_w - 0.1,
                event_trigger: None,
                rebalance_action: action.to_string(),
            }],
            breadth: MarketBreadthSnapshot::default(),
            risk_tier: RiskTier::Extreme,
            leverage_multiplier: 1.85,
            s1_gross_weight: target_w.abs(),
            s2_gross_weight: 0.0,
            s3_gross_weight: 0.0,
            total_gross_weight: target_w.abs(),
            total_net_weight: target_w,
        };
        let mut pg_asset = btc.clone();
        pg_asset.mark_price = 99.8;
        let mut pg_state = SimulationState::new(10_000.0);
        pg_state
            .active_positions
            .insert(btc.id, mk_pos(10, "OPEN", 100.0, 99.8, 5.0, 80.0, 150.0, 0));
        let pg_ledger =
            risk_engine.evaluate_ledger(10_000.0, &pg_state.active_positions_snapshot());
        execute_rebalance_actions(
            &[pg_asset.clone()],
            &point_bars,
            &mk_sig("CLOSE", 0.0),
            &pg_ledger,
            bar_ts,
            &cfg,
            &risk_engine,
            &mut pg_state,
        )
        .unwrap();
        assert_eq!(pg_state.profit_gate_blocks, 1);
        assert!(pg_state.active_positions.contains_key(&btc.id));

        execute_rebalance_actions(
            &[pg_asset],
            &point_bars,
            &mk_sig("FLIP", -0.10),
            &pg_ledger,
            bar_ts + 1000,
            &cfg,
            &risk_engine,
            &mut pg_state,
        )
        .unwrap();
        assert_eq!(
            pg_state.closed_positions.len(),
            1,
            "FLIP must bypass profit gate"
        );

        // 4. Chase IOC escalation (after max_alo_chase_ticks) & 60bps chase drift abort
        let mut ioc_state = SimulationState::new(10_000.0);
        ioc_state.initial_limit_price_by_pid.insert(20, 99.8);
        ioc_state
            .active_positions
            .insert(btc.id, mk_pos(20, "WORKING", 99.9, 99.9, 5.0, 0.0, 0.0, 1));
        let mut ioc_asset = btc.clone();
        ioc_asset.mark_price = 100.0; // within 60 bps drift from 99.8 -> triggers IOC escalation
        let ioc_ledger =
            risk_engine.evaluate_ledger(10_000.0, &ioc_state.active_positions_snapshot());
        execute_rebalance_actions(
            &[ioc_asset],
            &point_bars,
            &mk_sig("HOLD", 0.05),
            &ioc_ledger,
            bar_ts,
            &cfg,
            &risk_engine,
            &mut ioc_state,
        )
        .unwrap();
        assert_eq!(ioc_state.ioc_escalations, 1);
        assert_eq!(
            ioc_state.active_positions.get(&btc.id).unwrap().status,
            "OPEN"
        );

        // Chase drift abort (> 60 bps from initial limit price)
        let mut abort_state = SimulationState::new(10_000.0);
        abort_state.initial_limit_price_by_pid.insert(30, 100.0);
        abort_state.active_positions.insert(
            btc.id,
            mk_pos(30, "WORKING", 100.0, 100.0, 5.0, 0.0, 0.0, 0),
        );
        let mut drift_asset = btc.clone();
        drift_asset.mark_price = 101.0; // +100 bps > 60 bps cap
        let abort_ledger =
            risk_engine.evaluate_ledger(10_000.0, &abort_state.active_positions_snapshot());
        execute_rebalance_actions(
            &[drift_asset],
            &point_bars,
            &mk_sig("HOLD", 0.05),
            &abort_ledger,
            bar_ts,
            &cfg,
            &risk_engine,
            &mut abort_state,
        )
        .unwrap();
        assert_eq!(abort_state.chase_abort_count, 1);
        assert!(!abort_state.active_positions.contains_key(&btc.id));
        assert!(abort_state.cooldown_until_ms.contains_key(&btc.id));
    }

    #[test]
    fn test_notional_24h_volume_and_eval_start_ms_alignment() {
        use aegis_signals::liquidity::evaluate_asset_liquidity_quality;

        // High-priced coin ($550, 500 coins per 4h bar => 3000 coins/24h => $1.65M/24h USDC volume).
        // Previously summing raw coins gave 3000 < 25000, triggering a severe vol_factor penalty!
        let (mut assets, _) = BacktestEngine::generate_canonical_dataset(10, 15);
        let mut bnb = assets.remove(0);
        bnb.id = 10;
        bnb.symbol = "BNB-USDC".to_string();
        bnb.coin = "BNB".to_string();
        bnb.mark_price = 550.0;
        bnb.mid_price = 550.0;
        bnb.oracle_price = 550.0;
        bnb.day_volume_usdc = 1_650_000.0;

        let step = 14_400_000i64;
        let base_ts = 1_700_000_000_000i64;
        let mut bars = Vec::with_capacity(42);
        let mut px = 500.0;
        for i in 0..42i64 {
            let open = px;
            px *= 1.005;
            bars.push(OhlcvBar {
                id: i + 1,
                asset_id: bnb.id,
                timeframe: "4h".to_string(),
                timestamp_ms: base_ts + i * step,
                open,
                high: px * 1.006,
                low: open * 0.994,
                close: px,
                volume: 500.0,
                funding_rate: 0.00001,
                created_at_ms: base_ts + i * step,
                updated_at_ms: base_ts + i * step,
            });
        }
        let mut map = HashMap::new();
        map.insert(bnb.id, bars.clone());

        // Cutoff at bar index 24 => 24 warmup bars, 18 evaluated bars
        let eval_cutoff_ms = base_ts + 24 * step;
        let ds = HistoricalDataset {
            source: "mainnet".to_string(),
            timeframe: "4h".to_string(),
            eval_start_ms: Some(eval_cutoff_ms),
            assets: vec![bnb.clone()],
            bars_by_asset: map,
        };
        assert_eq!(
            count_timeline_bars_before(&ds.bars_by_asset, eval_cutoff_ms),
            24
        );

        let engine = BacktestEngine::new(BacktestConfig {
            days: 5,
            warmup_bars: 5, // should be overridden by eval_start_ms => 24
            ..BacktestConfig::default()
        });
        let report = engine.run_dataset(&ds).unwrap();
        assert_eq!(report.bars_evaluated, 18);
        assert_eq!(report.equity_curve[0].created_at_ms, eval_cutoff_ms + step);

        // Verify notional 24h volume produces full vol_factor=1.0 (> $25k) rather than penalized 0.45
        let mut bnb_check = bnb;
        bnb_check.mark_price = bars.last().unwrap().close;
        let notional_24h_vol: f64 = bars[bars.len() - 6..]
            .iter()
            .map(|b| b.volume * b.close)
            .sum();
        assert!(notional_24h_vol > 1_500_000.0);
        bnb_check.day_volume_usdc = notional_24h_vol;
        let q = evaluate_asset_liquidity_quality(&bnb_check, &bars);
        assert!((q.activity_scale - 1.0).abs() < 1e-6);
    }
}
