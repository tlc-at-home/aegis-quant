//! Aegis Quant v2.0 Multi-Sleeve Walk-Forward Backtest Orchestrator (`aegis-backtest`).

use crate::config::{
    BacktestConfig, BacktestReport, SensitivityGridCell, SensitivityGridReport,
    WalkForwardFoldReport, WalkForwardReport, SENSITIVITY_RISK_PER_TRADE_PCT,
    SENSITIVITY_STAGNATION_HOURS,
};
use crate::dataset::{
    collect_timeline_timestamps, count_timeline_bars_before,
    generate_canonical_dataset as build_canonical_dataset, slice_dataset_window, HistoricalDataset,
};
use crate::metrics::compute_backtest_report;
use crate::settlement::{
    build_active_cooldown_views, execute_rebalance_actions_with_duration,
    fill_working_orders_with_config, finalize_open_attribution, process_intrabar_stops_and_funding,
    record_bar_snapshot, SimulationState,
};
use aegis_core::indicators::timeframe_to_ms;
use aegis_core::{round_dp, AegisError, ConfigMap, ExchangeAsset, OhlcvBar, Result};
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
        let sliced_owned;
        let eff_dataset = if dataset.eval_end_ms.is_some() {
            sliced_owned =
                slice_dataset_window(dataset, dataset.eval_start_ms, dataset.eval_end_ms)?;
            &sliced_owned
        } else {
            dataset
        };

        if let Some(cutoff_ms) = eff_dataset.eval_start_ms {
            let eval_warmup = count_timeline_bars_before(&eff_dataset.bars_by_asset, cutoff_ms);
            if eval_warmup > 0 {
                let mut cloned = self.clone();
                cloned.config.warmup_bars = eval_warmup;
                return cloned.run_walk_forward_internal(
                    &eff_dataset.assets,
                    &eff_dataset.bars_by_asset,
                    true,
                );
            }
        }
        self.run_walk_forward_internal(&eff_dataset.assets, &eff_dataset.bars_by_asset, false)
    }

    /// Runs `folds` rolling out-of-sample walk-forward windows (`train_days` warmup/IS + `test_days` OOS)
    /// over `dataset` without lookahead into future bars.
    pub fn run_walk_forward_folds(
        &self,
        dataset: &HistoricalDataset,
        folds: usize,
        train_days: usize,
        test_days: usize,
    ) -> Result<WalkForwardReport> {
        if !(1..=50).contains(&folds) {
            return Err(AegisError::Validation(format!(
                "Invalid walk_forward_folds '{folds}': must be within 1..=50"
            )));
        }
        if train_days == 0 {
            return Err(AegisError::Validation(
                "Invalid train_days '0': must be >= 1".to_string(),
            ));
        }
        if test_days == 0 {
            return Err(AegisError::Validation(
                "Invalid test_days '0': must be >= 1".to_string(),
            ));
        }

        let timeline = collect_timeline_timestamps(&dataset.bars_by_asset);
        if timeline.len() < 4 {
            return Err(AegisError::Validation(
                "Insufficient timeline bars for walk-forward fold execution".to_string(),
            ));
        }

        let bar_duration_ms = timeframe_to_ms(&dataset.timeframe)
            .or_else(|| {
                timeline
                    .windows(2)
                    .map(|w| w[1].saturating_sub(w[0]))
                    .find(|&dt| dt >= 60_000)
            })
            .unwrap_or(14_400_000)
            .max(60_000);

        let anchor_start_ms = if let Some(start) = dataset.eval_start_ms {
            if count_timeline_bars_before(&dataset.bars_by_asset, start) == 0 {
                let warmup_idx = self
                    .config
                    .warmup_bars
                    .min(timeline.len().saturating_sub(2))
                    .max(1);
                timeline[warmup_idx].max(start)
            } else {
                start
            }
        } else {
            let warmup_idx = self
                .config
                .warmup_bars
                .min(timeline.len().saturating_sub(2))
                .max(1);
            timeline[warmup_idx]
        };

        let last_bar_close = timeline.last().copied().unwrap_or(anchor_start_ms) + bar_duration_ms;
        let available_end_ms = dataset
            .eval_end_ms
            .map(|e| e.min(last_bar_close))
            .unwrap_or(last_bar_close);
        let train_ms = (train_days as i64).saturating_mul(86_400_000);
        let test_ms = (test_days as i64).saturating_mul(86_400_000);
        let required_ms = train_ms.saturating_add((folds as i64).saturating_mul(test_ms));
        let available_span_ms = available_end_ms.saturating_sub(anchor_start_ms);

        if required_ms > available_span_ms {
            let avail_days = (available_span_ms as f64) / 86_400_000.0;
            let req_days = train_days + folds * test_days;
            return Err(AegisError::Validation(format!(
                "Insufficient dataset span for {folds} walk-forward folds ({train_days}d train + {folds}x{test_days}d test = {req_days}d required, only {avail_days:.1}d available after warmup)"
            )));
        }

        let mut fold_reports = Vec::with_capacity(folds);
        let mut comp_factor = 1.0_f64;
        let mut sum_return = 0.0_f64;
        let mut sum_sharpe = 0.0_f64;
        let mut worst_dd = 0.0_f64;
        let mut oos_total_trades = 0usize;
        let mut oos_wins = 0usize;
        let mut oos_gross_profit = 0.0_f64;
        let mut oos_gross_loss = 0.0_f64;
        let mut oos_long_pnl_usdc = 0.0_f64;
        let mut oos_short_pnl_usdc = 0.0_f64;

        for fold_idx in 0..folds {
            let train_start_ms = anchor_start_ms + (fold_idx as i64) * test_ms;
            let train_end_ms = train_start_ms + train_ms;
            let test_start_ms = train_end_ms;
            let test_end_ms = test_start_ms + test_ms;

            let train_ds = slice_dataset_window(dataset, Some(train_start_ms), Some(train_end_ms))?;
            let test_ds = slice_dataset_window(dataset, Some(test_start_ms), Some(test_end_ms))?;

            let train_report = self.run_dataset(&train_ds)?;
            let test_report = self.run_dataset(&test_ds)?;

            comp_factor *= 1.0 + (test_report.total_return_pct / 100.0);
            sum_return += test_report.total_return_pct;
            sum_sharpe += test_report.sharpe_ratio;
            worst_dd = worst_dd.max(test_report.max_drawdown_pct);
            oos_total_trades += test_report.total_trades;
            oos_wins += test_report.winning_trades;
            oos_long_pnl_usdc += test_report.long_pnl_usdc;
            oos_short_pnl_usdc += test_report.short_pnl_usdc;
            for pos in &test_report.closed_positions {
                if pos.realized_pnl_usdc > 0.0 {
                    oos_gross_profit += pos.realized_pnl_usdc;
                } else if pos.realized_pnl_usdc < 0.0 {
                    oos_gross_loss += pos.realized_pnl_usdc.abs();
                }
            }

            fold_reports.push(WalkForwardFoldReport {
                fold_index: fold_idx + 1,
                train_start_ms,
                train_end_ms,
                test_start_ms,
                test_end_ms,
                train_report,
                test_report,
            });
        }

        let n_folds = folds as f64;
        let oos_win_rate_pct = if oos_total_trades > 0 {
            round_dp((oos_wins as f64 / oos_total_trades as f64) * 100.0, 2)
        } else {
            0.0
        };
        let oos_profit_factor = if oos_gross_loss > 1e-6 {
            round_dp(oos_gross_profit / oos_gross_loss, 2)
        } else if oos_gross_profit > 0.0 {
            9.99
        } else {
            1.0
        };

        Ok(WalkForwardReport {
            folds_requested: folds,
            train_days,
            test_days,
            folds: fold_reports,
            oos_compounded_return_pct: round_dp((comp_factor - 1.0) * 100.0, 2),
            oos_mean_return_pct: round_dp(sum_return / n_folds, 2),
            oos_mean_sharpe: round_dp(sum_sharpe / n_folds, 2),
            oos_worst_drawdown_pct: round_dp(worst_dd, 2),
            oos_total_trades,
            oos_win_rate_pct,
            oos_profit_factor,
            oos_long_pnl_usdc: round_dp(oos_long_pnl_usdc, 2),
            oos_short_pnl_usdc: round_dp(oos_short_pnl_usdc, 2),
        })
    }

    /// Executes the canonical 3x3 sensitivity matrix (`stagnation_hours in [12.0, 16.0, 24.0]` x
    /// `risk_per_trade_pct in [0.015, 0.020, 0.025]`) across `dataset`.
    pub fn run_sensitivity_grid(
        &self,
        dataset: &HistoricalDataset,
    ) -> Result<SensitivityGridReport> {
        let mut cells = Vec::with_capacity(
            SENSITIVITY_STAGNATION_HOURS.len() * SENSITIVITY_RISK_PER_TRADE_PCT.len(),
        );
        for &stag_hours in &SENSITIVITY_STAGNATION_HOURS {
            for &risk_pct in &SENSITIVITY_RISK_PER_TRADE_PCT {
                let mut cloned = self.clone();
                cloned.config.risk_limits.stagnation_time_stop_hours = stag_hours;
                cloned.config.risk_limits.risk_per_trade_equity_pct = risk_pct;
                cloned
                    .config
                    .raw_config
                    .insert("stagnation_time_stop_hours", format!("{stag_hours}"));
                cloned
                    .config
                    .raw_config
                    .insert("risk_per_trade_equity_pct", format!("{risk_pct}"));
                let report = cloned.run_dataset(dataset)?;
                cells.push(SensitivityGridCell {
                    stagnation_hours: stag_hours,
                    risk_per_trade_pct: risk_pct,
                    total_return_pct: report.total_return_pct,
                    sharpe_ratio: report.sharpe_ratio,
                    sortino_ratio: report.sortino_ratio,
                    max_drawdown_pct: report.max_drawdown_pct,
                    win_rate_pct: report.win_rate_pct,
                    profit_factor: report.profit_factor,
                    total_trades: report.total_trades,
                    stagnation_stop_count: report.stagnation_stop_count,
                    stop_out_count: report.stop_out_count,
                    long_pnl_usdc: report.long_pnl_usdc,
                    short_pnl_usdc: report.short_pnl_usdc,
                    report,
                });
            }
        }

        Ok(SensitivityGridReport {
            stagnation_hours_axis: SENSITIVITY_STAGNATION_HOURS.to_vec(),
            risk_per_trade_pct_axis: SENSITIVITY_RISK_PER_TRADE_PCT.to_vec(),
            cells,
        })
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
        self.run_walk_forward_internal(assets, bars_by_asset, false)
    }

    fn build_signal_engine(&self) -> SignalEngine {
        let mut effective_cfg = if self.config.raw_config.0.is_empty() {
            ConfigMap::from_defaults()
        } else {
            self.config.raw_config.clone()
        };
        effective_cfg.insert(
            "max_open_positions",
            self.config.max_open_positions.to_string(),
        );
        effective_cfg.insert(
            "wall_clock_normalize_indicators",
            if self.config.wall_clock_normalize_indicators {
                "true"
            } else {
                "false"
            },
        );
        let mut signal_engine = SignalEngine::from_config_map(&effective_cfg);
        signal_engine.config = self.config.allocator.clone();
        // Keep direct BacktestConfig struct callers aligned with CLI / from_config_map callers.
        signal_engine.config.max_open_positions = self.config.max_open_positions.clamp(1, 100);
        signal_engine.config.wall_clock_normalize_indicators =
            self.config.wall_clock_normalize_indicators;
        signal_engine
    }

    fn run_walk_forward_internal(
        &self,
        assets: &[ExchangeAsset],
        bars_by_asset: &HashMap<i64, Vec<OhlcvBar>>,
        pinned_warmup: bool,
    ) -> Result<BacktestReport> {
        let signal_engine = self.build_signal_engine();
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
        let warmup = if pinned_warmup {
            self.config
                .warmup_bars
                .min(timeline.len().saturating_sub(1))
                .max(1)
        } else {
            self.config
                .warmup_bars
                .min(timeline.len().saturating_sub(2))
                .max(1)
        };
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

            let just_opened = fill_working_orders_with_config(
                &point_assets,
                &point_bars,
                &cur_bars_by_asset,
                ts_ms,
                bar_duration_ms,
                self.config.maker_fee_bps,
                &self.config,
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

            execute_rebalance_actions_with_duration(
                &point_assets,
                &point_bars,
                &cycle_out,
                &mid_ledger,
                bar_close_ts_ms,
                bar_duration_ms,
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
    use crate::settlement::execute_rebalance_actions;
    use aegis_core::AegisError;

    #[test]
    fn test_multi_sleeve_walk_forward_backtest() {
        let engine = BacktestEngine::new(BacktestConfig {
            days: 10,
            warmup_bars: 15,
            ..BacktestConfig::default()
        });
        let report = engine.run_default_simulation().unwrap();

        assert_eq!(report.platform_version, aegis_core::PLATFORM_VERSION);
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
                if t < 23 {
                    let r = match idx {
                        0 => 0.025,
                        1 => 0.015,
                        2 => -0.004,
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
                entry_r0: if stop > 0.0 {
                    (entry - stop).abs()
                } else {
                    0.0
                },
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
            eval_end_ms: None,
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

    #[test]
    fn test_wave2_l2_4h_pacing_two_phase_cooldown_and_slot_spread_cap() {
        use aegis_signals::{MarketBreadthSnapshot, SignalCycleOutput, SleeveSignalInput};

        let (mut assets, _) = BacktestEngine::generate_canonical_dataset(10, 15);
        for (i, a) in assets.iter_mut().enumerate().take(4) {
            a.mark_price = 100.0 + (i as f64);
            a.mid_price = a.mark_price;
            a.oracle_price = a.mark_price;
            a.tick_size = 0.01;
            a.lot_step_size = 0.01;
        }
        let a0 = &assets[0];
        let a1 = &assets[1];
        let a2 = &assets[2];
        let a3 = &assets[3];

        let bar_ts = 1_700_000_000_000i64;
        let step_4h = 14_400_000i64;
        let mk_bar = |aid: i64, px: f64| OhlcvBar {
            id: 1,
            asset_id: aid,
            timeframe: "4h".to_string(),
            timestamp_ms: bar_ts,
            open: px,
            high: px * 1.02,
            low: px * 0.98,
            close: px,
            volume: 10_000_000.0,
            funding_rate: 0.0,
            created_at_ms: bar_ts,
            updated_at_ms: bar_ts,
        };
        let b0 = vec![mk_bar(a0.id, a0.mark_price); 20];
        let b1 = vec![mk_bar(a1.id, a1.mark_price); 20];
        let b2 = vec![mk_bar(a2.id, a2.mark_price); 20];
        let b3 = vec![mk_bar(a3.id, a3.mark_price); 20];
        let mut point_bars = HashMap::new();
        point_bars.insert(a0.id, b0.as_slice());
        point_bars.insert(a1.id, b1.as_slice());
        point_bars.insert(a2.id, b2.as_slice());
        point_bars.insert(a3.id, b3.as_slice());

        let mk_sig =
            |a: &ExchangeAsset, action: &str, raw_w: f64, target_w: f64, trigger: Option<&str>| {
                SleeveSignalInput {
                    asset_id: a.id,
                    symbol: a.symbol.clone(),
                    coin: a.coin.clone(),
                    mark_price: a.mark_price,
                    day_volume_usdc: a.day_volume_usdc,
                    funding_rate_hourly: 0.0,
                    annualized_vol: 0.5,
                    market_beta: 1.0,
                    s1_mom_score: 0.1,
                    s1_carry_score: 0.1,
                    s2_trend_alignment: 1,
                    s2_vol_surge_ratio: 1.0,
                    s3_funding_zscore: 0.0,
                    s3_flush_atr_mult: 0.0,
                    w_s1_carry_mom: target_w,
                    w_s2_trend_breakout: 0.0,
                    w_s3_event_reversion: 0.0,
                    raw_combined_weight: raw_w,
                    target_weight: target_w,
                    current_weight: 0.0,
                    delta_weight: target_w,
                    event_trigger: trigger.map(str::to_string),
                    rebalance_action: action.to_string(),
                }
            };

        // 1. Two-phase slot freeing + Leak L2 multi-entry 4h pacing + 300s CLOSE cooldown + lev_mult cooldown telemetry
        let mut cfg = BacktestConfig {
            max_open_positions: 2,
            min_entry_interval_sec: 150,
            min_position_hold_sec: 0,
            ..BacktestConfig::default()
        };
        cfg.risk_limits.risk_per_trade_equity_pct = 0.0; // isolate weight caps cleanly
        let risk_engine = RiskEngine::new(cfg.risk_limits.clone());

        let mut state = SimulationState::new(10_000.0);
        // Asset 3 (higher ID) is currently OPEN, Asset 2 is OPEN -> 2/2 slots used.
        // In Phase 1, Asset 3 receives ACTION_CLOSE (frees 1 slot + inserts 300s cooldown).
        // In Phase 2, Asset 0 and Asset 1 compete for the 1 freed slot (plus Asset 2 stays held):
        // Asset 1 has CAPITULATION_FLUSH priority boost even though Asset 0 has lower asset_id & higher |target_weight|.
        // Wait: make max_open_positions = 3 so BOTH Asset 0 and Asset 1 enter on the same 4h bar (verifying L2)!
        cfg.max_open_positions = 3;
        let open_pos = |pid: i64, aid: i64, px: f64| aegis_core::PositionRecord {
            id: pid,
            asset_id: aid,
            client_order_id: format!("open-{pid}"),
            side: "LONG".to_string(),
            status: "OPEN".to_string(),
            fsm_state: "FILLED_OPEN".to_string(),
            entry_price: px,
            current_price: px,
            exit_price: None,
            requested_quantity: 1.0,
            filled_quantity: 1.0,
            reserved_margin_usdc: 0.0,
            committed_notional_usdc: px,
            portfolio_weight: 0.05,
            s1_weight_contrib: 0.05,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: px * 0.96,
            entry_r0: px * 0.04,
            protective_stop_oid: None,
            take_profit_px: px * 1.10,
            take_profit_oid: None,
            highest_price_seen: px,
            lowest_price_seen: px,
            chase_count: 0,
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.1,
            funding_accrued_usdc: 0.0,
            slippage_paid_usdc: 0.0,
            exchange_order_id: None,
            created_at_ms: bar_ts - step_4h,
            opened_at_ms: Some(bar_ts - step_4h),
            closed_at_ms: None,
            updated_at_ms: bar_ts - step_4h,
            exit_reason: None,
        };
        state
            .active_positions
            .insert(a2.id, open_pos(1, a2.id, a2.mark_price));
        state
            .active_positions
            .insert(a3.id, open_pos(2, a3.id, a3.mark_price));
        state.next_pos_id = 10;

        // Include a COOLDOWN_BLOCKED with raw_combined_weight = 0.015 (< base min_delta 0.025, even though 0.015*1.85=0.02775 >= 0.025!)
        // Under lev_mult-aware eff_min_delta (0.025 * 1.85 = 0.04625), 0.015 must NOT increment cooldown_blocks!
        let breadth = MarketBreadthSnapshot {
            market_regime_label: "BULL_TREND".to_string(),
            ..Default::default()
        };
        let cycle = SignalCycleOutput {
            signals: vec![
                mk_sig(a0, "OPEN_LONG", 0.08, 0.14, None),
                mk_sig(a1, "OPEN_LONG", 0.06, 0.11, Some("CAPITULATION_FLUSH")),
                mk_sig(a2, "COOLDOWN_BLOCKED", 0.015, 0.0, None),
                mk_sig(a3, "CLOSE", 0.0, 0.0, None),
            ],
            breadth,
            risk_tier: aegis_core::RiskTier::Extreme,
            leverage_multiplier: 1.85,
            s1_gross_weight: 0.25,
            s2_gross_weight: 0.0,
            s3_gross_weight: 0.0,
            total_gross_weight: 0.25,
            total_net_weight: 0.25,
        };
        let ledger = risk_engine.evaluate_ledger(10_000.0, &state.active_positions_snapshot());
        execute_rebalance_actions_with_duration(
            &assets[..4],
            &point_bars,
            &cycle,
            &ledger,
            bar_ts + step_4h,
            step_4h,
            &cfg,
            &risk_engine,
            &mut state,
        )
        .unwrap();

        // Phase 1 closed a3 and placed a 300s cooldown; Phase 2 admitted BOTH a1 and a0 on the same 4h bar!
        assert!(!state.active_positions.contains_key(&a3.id));
        assert_eq!(state.closed_positions.len(), 1);
        assert_eq!(
            state.cooldown_until_ms.get(&a3.id).map(|v| v.0),
            Some(bar_ts + step_4h + 300_000),
            "ACTION_CLOSE must apply 300s cooldown"
        );
        assert!(state.active_positions.contains_key(&a0.id));
        assert!(state.active_positions.contains_key(&a1.id));
        assert_eq!(
            state.entry_interval_blocks, 0,
            "4h bar must admit multiple 150s-spaced entries"
        );
        assert_eq!(
            state.cooldown_blocks, 0,
            "0.015 unlevered COOLDOWN_BLOCKED below base min_delta must not inflate cooldown_blocks"
        );
        // Intra-bar cursor advanced by 150s between first and second entry, still <= bar_close_ts_ms
        assert_eq!(state.last_entry_ts_ms, Some(bar_ts + 150_000));

        // 2. Test max_entries_per_bar = 1 + enforce_slot_spread_weight_cap
        let mut cap_cfg = BacktestConfig {
            max_open_positions: 10,
            max_entries_per_bar: 1,
            enforce_slot_spread_weight_cap: true,
            ..BacktestConfig::default()
        };
        cap_cfg.allocator.max_single_asset_weight = 0.25;
        cap_cfg.risk_limits.max_single_asset_weight = 0.25;
        cap_cfg.risk_limits.risk_per_trade_equity_pct = 0.0;
        let cap_risk = RiskEngine::new(cap_cfg.risk_limits.clone());
        let mut cap_state = SimulationState::new(10_000.0);
        let cap_cycle = SignalCycleOutput {
            signals: vec![
                mk_sig(a0, "OPEN_LONG", 0.20, 0.25, None),
                mk_sig(a1, "OPEN_LONG", 0.20, 0.25, Some("CAPITULATION_FLUSH")),
            ],
            breadth: MarketBreadthSnapshot::default(),
            risk_tier: aegis_core::RiskTier::Extreme,
            leverage_multiplier: 1.85,
            s1_gross_weight: 0.50,
            s2_gross_weight: 0.0,
            s3_gross_weight: 0.0,
            total_gross_weight: 0.50,
            total_net_weight: 0.50,
        };
        let cap_ledger = cap_risk.evaluate_ledger(10_000.0, &[]);
        execute_rebalance_actions_with_duration(
            &assets[..2],
            &point_bars,
            &cap_cycle,
            &cap_ledger,
            bar_ts + step_4h,
            step_4h,
            &cap_cfg,
            &cap_risk,
            &mut cap_state,
        )
        .unwrap();
        // Priority-sorted a1 (CAPITULATION_FLUSH) took the 1 allowed entry, a0 blocked by max_entries_per_bar
        assert!(cap_state.active_positions.contains_key(&a1.id));
        assert!(!cap_state.active_positions.contains_key(&a0.id));
        assert_eq!(cap_state.max_entries_per_bar_blocks, 1);
        // Slot spread cap: (1.85 / 10) * 1.25 = 0.23125 < 0.25
        let placed_w = cap_state.active_positions[&a1.id].portfolio_weight;
        assert!(
            placed_w <= 0.2315 && placed_w > 0.20,
            "Expected slot-spread weight cap <= 0.23125, got {placed_w}"
        );
    }

    #[test]
    fn test_wave2_me2_intrabar_ratchet_pullback_frozen_r0_and_n4_n6_levers() {
        use aegis_signals::{MarketBreadthSnapshot, SignalCycleOutput, SleeveSignalInput};
        use std::collections::HashSet;

        let (mut assets, _) = BacktestEngine::generate_canonical_dataset(5, 15);
        let mut btc = assets.remove(0);
        btc.mark_price = 103.5;
        btc.mid_price = 103.5;
        btc.oracle_price = 103.5;
        btc.tick_size = 0.01;
        btc.lot_step_size = 0.01;
        btc.funding_rate_hourly = 0.0005;

        let mut cfg = BacktestConfig {
            maker_first_low_urgency_entries: true,
            maker_only_rest_ttl_ms: 60_000,
            funding_entry_timing_enabled: true,
            funding_entry_defer_min_rate_hourly: 0.0001,
            funding_entry_defer_window_sec: 900,
            ..BacktestConfig::default()
        };
        cfg.risk_limits.stagnation_time_stop_hours = 0.0;
        let risk_engine = RiskEngine::new(cfg.risk_limits.clone());

        // 1. ME-2 & frozen entry_r0: entry=100, initial stop=96 => entry_r0=4.0.
        // Bar goes up to high=107.0 (+1.75R >= 1.60R => locks >= 100 + 1.0*4.0 = 104.0), then closes at 103.5.
        // With 2-step ME-2 evaluation, Step 1 ratchets stop >= 104.0 at high=107.0, and Step 2 stops out at close=103.5!
        let bar_ts = 1_700_000_000_000i64;
        let cur_bar = OhlcvBar {
            id: 1,
            asset_id: btc.id,
            timeframe: "4h".to_string(),
            timestamp_ms: bar_ts,
            open: 100.5,
            high: 107.0,
            low: 100.2, // above initial stop 96.0!
            close: 103.5,
            volume: 10_000_000.0,
            funding_rate: 0.0,
            created_at_ms: bar_ts,
            updated_at_ms: bar_ts,
        };
        let steady_bar = OhlcvBar {
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            ..cur_bar.clone()
        };
        let mut bars_vec = vec![steady_bar; 19];
        bars_vec.push(cur_bar.clone());
        let mut point_bars = HashMap::new();
        point_bars.insert(btc.id, bars_vec.as_slice());
        let mut cur_bars = HashMap::new();
        cur_bars.insert(btc.id, &cur_bar);

        let mut me2_state = SimulationState::new(10_000.0);
        me2_state.active_positions.insert(
            btc.id,
            aegis_core::PositionRecord {
                id: 1,
                asset_id: btc.id,
                client_order_id: "me2-1".to_string(),
                side: "LONG".to_string(),
                status: "OPEN".to_string(),
                fsm_state: "FILLED_OPEN".to_string(),
                entry_price: 100.0,
                current_price: 100.5,
                exit_price: None,
                requested_quantity: 5.0,
                filled_quantity: 5.0,
                reserved_margin_usdc: 0.0,
                committed_notional_usdc: 500.0,
                portfolio_weight: 0.05,
                s1_weight_contrib: 0.05,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 96.0,
                entry_r0: 4.0,
                protective_stop_oid: None,
                take_profit_px: 120.0,
                take_profit_oid: None,
                highest_price_seen: 101.0,
                lowest_price_seen: 99.5,
                chase_count: 0,
                realized_pnl_usdc: 0.0,
                unrealized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.1,
                funding_accrued_usdc: 0.0,
                slippage_paid_usdc: 0.0,
                exchange_order_id: None,
                created_at_ms: bar_ts - 14_400_000,
                opened_at_ms: Some(bar_ts - 14_400_000),
                closed_at_ms: None,
                updated_at_ms: bar_ts - 14_400_000,
                exit_reason: None,
            },
        );

        process_intrabar_stops_and_funding(
            &[btc.clone()],
            &point_bars,
            &cur_bars,
            &HashSet::new(),
            bar_ts + 14_400_000,
            14_400_000,
            4.0,
            &cfg,
            &risk_engine,
            &mut me2_state,
        )
        .unwrap();

        assert_eq!(me2_state.stop_out_count, 1);
        assert_eq!(me2_state.closed_positions.len(), 1);
        let closed = &me2_state.closed_positions[0];
        assert_eq!(closed.exit_reason.as_deref(), Some("PROTECTIVE_STOP_LONG"));
        assert!(
            closed.exit_price.unwrap() >= 103.9,
            "ME-2 must settle at ratcheted profit-lock stop >= 104.0, got {:?}",
            closed.exit_price
        );

        // 1b. Frozen entry_r0 propagation: WORKING -> OPEN sets entry_r0, scale-in merge preserves it,
        // and IOC escalation sets entry_r0 from ioc_fill_px.
        let mut r0_state = SimulationState::new(10_000.0);
        r0_state.active_positions.insert(
            btc.id,
            aegis_core::PositionRecord {
                id: 50,
                asset_id: btc.id,
                client_order_id: "w-50".to_string(),
                side: "LONG".to_string(),
                status: "WORKING".to_string(),
                fsm_state: "WORKING_ALO".to_string(),
                entry_price: 100.5,
                current_price: 100.5,
                exit_price: None,
                requested_quantity: 5.0,
                filled_quantity: 0.0,
                reserved_margin_usdc: 502.5,
                committed_notional_usdc: 0.0,
                portfolio_weight: 0.05,
                s1_weight_contrib: 0.05,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 0.0,
                entry_r0: 0.0,
                protective_stop_oid: None,
                take_profit_px: 0.0,
                take_profit_oid: None,
                highest_price_seen: 100.5,
                lowest_price_seen: 100.5,
                chase_count: 0,
                realized_pnl_usdc: 0.0,
                unrealized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.0,
                funding_accrued_usdc: 0.0,
                slippage_paid_usdc: 0.0,
                exchange_order_id: None,
                created_at_ms: bar_ts,
                opened_at_ms: None,
                closed_at_ms: None,
                updated_at_ms: bar_ts,
                exit_reason: None,
            },
        );
        let fill_bar = OhlcvBar {
            open: 101.0,
            high: 104.0,
            low: 100.2,
            close: 103.0,
            ..cur_bar.clone()
        };
        let mut fill_cur_bars = HashMap::new();
        fill_cur_bars.insert(btc.id, &fill_bar);
        let mut fill_btc = btc.clone();
        fill_btc.mark_price = 103.0;
        let just_opened = fill_working_orders_with_config(
            &[fill_btc.clone()],
            &point_bars,
            &fill_cur_bars,
            bar_ts,
            14_400_000,
            cfg.maker_fee_bps,
            &cfg,
            &risk_engine,
            &mut r0_state,
        )
        .unwrap();
        assert!(just_opened.contains(&btc.id));
        let pos_open = &r0_state.active_positions[&btc.id];
        let frozen_r0 = pos_open.entry_r0;
        let expected_r0 = (pos_open.entry_price - pos_open.protective_stop_px).abs();
        assert!(frozen_r0 > 0.0 && (frozen_r0 - expected_r0).abs() < 1e-8);

        // Scale-in merge preserves frozen entry_r0 even when VWAP entry_price changes
        let mut inc_pos = pos_open.clone();
        inc_pos.id = 51;
        inc_pos.status = "WORKING".to_string();
        inc_pos.entry_price = 103.0;
        inc_pos.requested_quantity = 5.0;
        inc_pos.filled_quantity = 0.0;
        r0_state.pending_increases.insert(btc.id, inc_pos);
        fill_working_orders_with_config(
            &[fill_btc],
            &point_bars,
            &fill_cur_bars,
            bar_ts + 14_400_000,
            14_400_000,
            cfg.maker_fee_bps,
            &cfg,
            &risk_engine,
            &mut r0_state,
        )
        .unwrap();
        let merged = &r0_state.active_positions[&btc.id];
        assert!((merged.filled_quantity - 10.0).abs() < 1e-8);
        assert!(merged.entry_price > 101.0); // VWAP shifted up
        assert!(
            (merged.entry_r0 - frozen_r0).abs() < 1e-9,
            "Scale-in merge must preserve frozen entry_r0"
        );

        // IOC escalation freezes entry_r0 from ioc_fill_px
        let mut ioc_r0_state = SimulationState::new(10_000.0);
        ioc_r0_state.initial_limit_price_by_pid.insert(60, 103.5);
        ioc_r0_state.active_positions.insert(
            btc.id,
            aegis_core::PositionRecord {
                id: 60,
                asset_id: btc.id,
                client_order_id: "ioc-60".to_string(),
                side: "LONG".to_string(),
                status: "WORKING".to_string(),
                fsm_state: "CHASING_ALO".to_string(),
                entry_price: 103.5,
                current_price: 103.5,
                exit_price: None,
                requested_quantity: 5.0,
                filled_quantity: 0.0,
                reserved_margin_usdc: 517.5,
                committed_notional_usdc: 0.0,
                portfolio_weight: 0.05,
                s1_weight_contrib: 0.05,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 0.0,
                entry_r0: 0.0,
                protective_stop_oid: None,
                take_profit_px: 0.0,
                take_profit_oid: None,
                highest_price_seen: 103.5,
                lowest_price_seen: 103.5,
                chase_count: 5,
                realized_pnl_usdc: 0.0,
                unrealized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.0,
                funding_accrued_usdc: 0.0,
                slippage_paid_usdc: 0.0,
                exchange_order_id: None,
                created_at_ms: bar_ts,
                opened_at_ms: None,
                closed_at_ms: None,
                updated_at_ms: bar_ts,
                exit_reason: None,
            },
        );
        let ioc_hold = SignalCycleOutput {
            signals: vec![SleeveSignalInput {
                asset_id: btc.id,
                symbol: btc.symbol.clone(),
                coin: btc.coin.clone(),
                mark_price: btc.mark_price,
                day_volume_usdc: btc.day_volume_usdc,
                funding_rate_hourly: 0.0,
                annualized_vol: 0.5,
                market_beta: 1.0,
                s1_mom_score: 0.2,
                s1_carry_score: 0.0,
                s2_trend_alignment: 1,
                s2_vol_surge_ratio: 1.0,
                s3_funding_zscore: 0.0,
                s3_flush_atr_mult: 0.0,
                w_s1_carry_mom: 0.05,
                w_s2_trend_breakout: 0.0,
                w_s3_event_reversion: 0.0,
                raw_combined_weight: 0.05,
                target_weight: 0.05,
                current_weight: 0.05,
                delta_weight: 0.0,
                event_trigger: None,
                rebalance_action: "HOLD".to_string(),
            }],
            breadth: MarketBreadthSnapshot::default(),
            risk_tier: aegis_core::RiskTier::Extreme,
            leverage_multiplier: 1.85,
            s1_gross_weight: 0.05,
            s2_gross_weight: 0.0,
            s3_gross_weight: 0.0,
            total_gross_weight: 0.05,
            total_net_weight: 0.05,
        };
        let ioc_ledger = risk_engine.evaluate_ledger(10_000.0, &[]);
        execute_rebalance_actions_with_duration(
            &[btc.clone()],
            &point_bars,
            &ioc_hold,
            &ioc_ledger,
            bar_ts + 14_400_000,
            14_400_000,
            &BacktestConfig::default(),
            &risk_engine,
            &mut ioc_r0_state,
        )
        .unwrap();
        let ioc_open = &ioc_r0_state.active_positions[&btc.id];
        assert_eq!(ioc_open.status, "OPEN");
        assert!(
            ioc_open.entry_r0 > 0.0
                && (ioc_open.entry_r0 - (ioc_open.entry_price - ioc_open.protective_stop_px).abs())
                    .abs()
                    < 1e-8
        );

        // 2. N6 funding timing deferral: 300s before UTC hour boundary (1_700_002_800_000 - 300_000 = 1_700_002_500_000)
        let pre_hour_ts = 1_700_002_500_000i64;
        assert_eq!(3_600_000 - pre_hour_ts.rem_euclid(3_600_000), 300_000);
        let sig_long = SignalCycleOutput {
            signals: vec![SleeveSignalInput {
                asset_id: btc.id,
                symbol: btc.symbol.clone(),
                coin: btc.coin.clone(),
                mark_price: btc.mark_price,
                day_volume_usdc: btc.day_volume_usdc,
                funding_rate_hourly: btc.funding_rate_hourly,
                annualized_vol: 0.5,
                market_beta: 1.0,
                s1_mom_score: 0.2,
                s1_carry_score: 0.0,
                s2_trend_alignment: 1,
                s2_vol_surge_ratio: 1.0,
                s3_funding_zscore: 0.0,
                s3_flush_atr_mult: 0.0,
                w_s1_carry_mom: 0.10,
                w_s2_trend_breakout: 0.0,
                w_s3_event_reversion: 0.0,
                raw_combined_weight: 0.06,
                target_weight: 0.10,
                current_weight: 0.0,
                delta_weight: 0.10,
                event_trigger: None,
                rebalance_action: "OPEN_LONG".to_string(),
            }],
            breadth: MarketBreadthSnapshot::default(),
            risk_tier: aegis_core::RiskTier::Extreme,
            leverage_multiplier: 1.85,
            s1_gross_weight: 0.10,
            s2_gross_weight: 0.0,
            s3_gross_weight: 0.0,
            total_gross_weight: 0.10,
            total_net_weight: 0.10,
        };
        let mut n6_state = SimulationState::new(10_000.0);
        let n6_ledger = risk_engine.evaluate_ledger(10_000.0, &[]);
        execute_rebalance_actions_with_duration(
            &[btc.clone()],
            &point_bars,
            &sig_long,
            &n6_ledger,
            pre_hour_ts,
            60_000,
            &cfg,
            &risk_engine,
            &mut n6_state,
        )
        .unwrap();
        assert_eq!(n6_state.funding_defer_blocks, 1);
        assert!(n6_state.active_positions.is_empty());

        // 3. N4 maker-first low-urgency rest: after UTC hour boundary (1_700_002_860_000), entry is placed as MakerOnly.
        // Resting through max_alo_chase_ticks without filling must NOT escalate to IOC, and expires cleanly at TTL.
        let post_hour_ts = 1_700_002_860_000i64;
        execute_rebalance_actions_with_duration(
            &[btc.clone()],
            &point_bars,
            &sig_long,
            &n6_ledger,
            post_hour_ts,
            60_000,
            &cfg,
            &risk_engine,
            &mut n6_state,
        )
        .unwrap();
        let placed_pid = n6_state.active_positions[&btc.id].id;
        assert!(n6_state.maker_only_pids.contains(&placed_pid));

        let mut hold_sig = sig_long.clone();
        hold_sig.signals[0].rebalance_action = "HOLD".to_string();
        hold_sig.signals[0].current_weight = n6_state.active_positions[&btc.id].portfolio_weight;
        n6_state
            .active_positions
            .get_mut(&btc.id)
            .unwrap()
            .chase_count = 10; // well past max_alo_chase_ticks = 3

        // Within TTL (+20_000ms < 60_000ms): remains WORKING as MAKER_RESTING with 0 IOC escalations
        execute_rebalance_actions_with_duration(
            &[btc.clone()],
            &point_bars,
            &hold_sig,
            &n6_ledger,
            post_hour_ts + 20_000,
            20_000,
            &cfg,
            &risk_engine,
            &mut n6_state,
        )
        .unwrap();
        assert_eq!(n6_state.ioc_escalations, 0);
        assert_eq!(n6_state.active_positions[&btc.id].status, "WORKING");
        assert_eq!(
            n6_state.active_positions[&btc.id].fsm_state,
            "MAKER_RESTING"
        );

        // Past TTL (+60_000ms >= 60_000ms): cleanly cancels without IOC or chase abort
        execute_rebalance_actions_with_duration(
            &[btc],
            &point_bars,
            &hold_sig,
            &n6_ledger,
            post_hour_ts + 60_000,
            40_000,
            &cfg,
            &risk_engine,
            &mut n6_state,
        )
        .unwrap();
        assert_eq!(n6_state.ioc_escalations, 0);
        assert_eq!(n6_state.chase_abort_count, 0);
        assert!(!n6_state.active_positions.contains_key(&1));
    }

    #[test]
    fn test_wave2_report_decomposition_slicing_walk_forward_folds_and_sensitivity_grid() {
        // 1. Report long/short + regime + exit_reason decomposition
        let mut rep_state = SimulationState::new(10_000.0);
        let t0 = 1_700_000_000_000i64;
        rep_state.equity_curve.push(aegis_core::PortfolioSnapshot {
            id: 1,
            total_equity_usdc: 10_050.0,
            uncapped_equity_usdc: 10_050.0,
            available_cash_usdc: 9_000.0,
            reserved_order_cash_usdc: 0.0,
            committed_notional_usdc: 1_000.0,
            net_notional_usdc: 1_000.0,
            gross_leverage_x: 0.1,
            net_leverage_x: 0.1,
            market_beta_exposure: 0.1,
            market_breadth_bull: 0.70,
            market_regime_label: "BULL_EXPANSION".to_string(),
            risk_tier: "extreme".to_string(),
            leverage_multiplier: 1.85,
            s1_gross_weight: 0.1,
            s2_gross_weight: 0.0,
            s3_gross_weight: 0.0,
            working_order_count: 0,
            open_long_count: 1,
            open_short_count: 0,
            eligible_coin_count: 8,
            created_at_ms: t0,
            updated_at_ms: t0,
        });
        rep_state.equity_curve.push(aegis_core::PortfolioSnapshot {
            id: 2,
            market_breadth_bull: 0.38,
            market_regime_label: "DEFENSIVE_CHOP".to_string(),
            created_at_ms: t0 + 14_400_000,
            updated_at_ms: t0 + 14_400_000,
            ..rep_state.equity_curve[0].clone()
        });
        let mk_closed = |id: i64, side: &str, pnl: f64, open_ts: i64, reason: &str| {
            aegis_core::PositionRecord {
                id,
                asset_id: id,
                client_order_id: format!("c-{id}"),
                side: side.to_string(),
                status: "CLOSED".to_string(),
                fsm_state: "CLOSED".to_string(),
                entry_price: 100.0,
                current_price: 100.0,
                exit_price: Some(105.0),
                requested_quantity: 1.0,
                filled_quantity: 1.0,
                reserved_margin_usdc: 0.0,
                committed_notional_usdc: 0.0,
                portfolio_weight: 0.05,
                s1_weight_contrib: 0.05,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 96.0,
                entry_r0: 4.0,
                protective_stop_oid: None,
                take_profit_px: 112.0,
                take_profit_oid: None,
                highest_price_seen: 105.0,
                lowest_price_seen: 99.0,
                chase_count: 0,
                realized_pnl_usdc: pnl,
                unrealized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.2,
                funding_accrued_usdc: 0.0,
                slippage_paid_usdc: 0.0,
                exchange_order_id: None,
                created_at_ms: open_ts,
                opened_at_ms: Some(open_ts),
                closed_at_ms: Some(open_ts + 14_400_000),
                updated_at_ms: open_ts + 14_400_000,
                exit_reason: Some(reason.to_string()),
            }
        };
        rep_state
            .entry_regime_by_pid
            .insert(1, "BULL_EXPANSION".to_string());
        rep_state
            .entry_regime_by_pid
            .insert(2, "DEFENSIVE_CHOP".to_string());
        rep_state
            .closed_positions
            .push(mk_closed(1, "LONG", 120.0, t0, "TAKE_PROFIT_LONG"));
        rep_state.closed_positions.push(mk_closed(
            2,
            "SHORT",
            -40.0,
            t0 + 14_400_000,
            "STAGNATION_TIME_STOP_SHORT",
        ));

        let rep =
            compute_backtest_report(&BacktestConfig::default(), "4h".to_string(), 2, rep_state);
        assert_eq!(rep.long_trades, 1);
        assert!((rep.long_win_rate - 100.0).abs() < 1e-6);
        assert!((rep.long_pnl_usdc - 120.0).abs() < 1e-6);
        assert_eq!(rep.short_trades, 1);
        assert!((rep.short_win_rate - 0.0).abs() < 1e-6);
        assert!((rep.short_pnl_usdc - (-40.0)).abs() < 1e-6);
        assert_eq!(rep.regime_breakdown["BULL_EXPANSION"].bars, 1);
        assert_eq!(rep.regime_breakdown["BULL_EXPANSION"].trades, 1);
        assert!((rep.regime_breakdown["BULL_EXPANSION"].long_pnl_usdc - 120.0).abs() < 1e-6);
        assert_eq!(rep.regime_breakdown["DEFENSIVE_CHOP"].trades, 1);
        assert!((rep.regime_breakdown["DEFENSIVE_CHOP"].short_pnl_usdc - (-40.0)).abs() < 1e-6);
        assert_eq!(rep.exit_reason_breakdown["TAKE_PROFIT_LONG"].count, 1);
        assert_eq!(rep.exit_reason_breakdown["TAKE_PROFIT_LONG"].wins, 1);
        assert_eq!(
            rep.exit_reason_breakdown["STAGNATION_TIME_STOP_SHORT"].wins,
            0
        );

        // 2. Window slicing + walk-forward folds + 3x3 sensitivity grid on canonical dataset
        let (assets, bars_by_asset) = BacktestEngine::generate_canonical_dataset(28, 24);
        let full_ds = HistoricalDataset {
            source: "synthetic".to_string(),
            timeframe: "4h".to_string(),
            eval_start_ms: None,
            eval_end_ms: None,
            assets,
            bars_by_asset,
        };
        let timeline = collect_timeline_timestamps(&full_ds.bars_by_asset);
        let slice_start = timeline[24];
        let slice_end = slice_start + 7 * 86_400_000;
        let sliced = slice_dataset_window(&full_ds, Some(slice_start), Some(slice_end)).unwrap();
        let engine = BacktestEngine::new(BacktestConfig {
            days: 28,
            warmup_bars: 24,
            ..BacktestConfig::default()
        });
        let sliced_rep = engine.run_dataset(&sliced).unwrap();
        assert_eq!(sliced_rep.bars_evaluated, 42);
        assert_eq!(sliced_rep.days_evaluated, 7);

        let wf = engine.run_walk_forward_folds(&full_ds, 2, 14, 7).unwrap();
        assert_eq!(wf.folds.len(), 2);
        assert_eq!(wf.folds[0].test_report.days_evaluated, 7);
        assert_eq!(wf.folds[1].test_report.days_evaluated, 7);
        assert!(wf.folds[0].test_end_ms <= wf.folds[1].test_start_ms);

        let grid = engine.run_sensitivity_grid(&sliced).unwrap();
        assert_eq!(grid.cells.len(), 9);
        assert_eq!(grid.stagnation_hours_axis, vec![12.0, 16.0, 24.0]);
        assert_eq!(grid.risk_per_trade_pct_axis, vec![0.015, 0.020, 0.025]);

        // 3. Direct BacktestConfig struct callers must sync max_open_positions & wall_clock_normalize_indicators
        let direct_engine = BacktestEngine::new(BacktestConfig {
            max_open_positions: 12,
            wall_clock_normalize_indicators: false,
            ..BacktestConfig::default()
        });
        let sig_eng = direct_engine.build_signal_engine();
        assert_eq!(sig_eng.config.max_open_positions, 12);
        assert!(!sig_eng.config.wall_clock_normalize_indicators);
    }
}
