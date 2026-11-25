//! Walk-Forward Backtest Performance Metrics & Tear Sheet Computation (`aegis-backtest::metrics`).

use crate::config::{BacktestConfig, BacktestReport, ExitReasonSummary, RegimeAttribution};
use crate::settlement::SimulationState;
use aegis_core::{round_dp, PortfolioSnapshot, PositionRecord};
use std::collections::BTreeMap;

fn resolve_trade_regime_label(
    pos: &PositionRecord,
    state: &SimulationState,
    equity_curve: &[PortfolioSnapshot],
) -> String {
    if let Some(label) = state.entry_regime_by_pid.get(&pos.id) {
        let trimmed = label.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    let lookup_ts = pos.opened_at_ms.or(pos.closed_at_ms);
    if let Some(ts) = lookup_ts {
        if !equity_curve.is_empty() {
            let idx = equity_curve.partition_point(|s| s.created_at_ms <= ts);
            let snap = if idx > 0 {
                &equity_curve[idx - 1]
            } else {
                &equity_curve[0]
            };
            let trimmed = snap.market_regime_label.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }
    "UNKNOWN".to_string()
}

/// Computes the final [`BacktestReport`] tear sheet from accumulated walk-forward simulation state.
///
/// Enforces `R14-L22`:
/// - Sortino downside variance uses the unbiased `(n_rets - 1.0).max(1.0)` sample denominator matching `var_ret`.
/// - `days_evaluated` derives cleanly from actual simulated wall-clock span `[first_evaluated_ts_ms, last_bar_close_ts_ms]`.
pub fn compute_backtest_report(
    config: &BacktestConfig,
    timeframe: String,
    assets_evaluated: usize,
    state: SimulationState,
) -> BacktestReport {
    let final_equity_usdc = state
        .equity_curve
        .last()
        .map(|s| s.total_equity_usdc)
        .unwrap_or(config.initial_equity_usdc);
    let init_denom = if config.initial_equity_usdc.abs() > 1e-6 {
        config.initial_equity_usdc.abs()
    } else {
        1.0
    };
    let total_return_pct = round_dp(
        ((final_equity_usdc - config.initial_equity_usdc) / init_denom) * 100.0,
        2,
    );

    let n_rets = state.bar_returns.len().max(1) as f64;
    let sample_denom = (n_rets - 1.0).max(1.0);
    let mean_ret = state.bar_returns.iter().sum::<f64>() / n_rets;
    let var_ret = state
        .bar_returns
        .iter()
        .map(|r| {
            let d = *r - mean_ret;
            d * d
        })
        .sum::<f64>()
        / sample_denom;
    let std_ret = var_ret.sqrt();
    let down_var = state
        .bar_returns
        .iter()
        .map(|r| {
            let d = r.min(0.0);
            d * d
        })
        .sum::<f64>()
        / sample_denom;
    let down_std = down_var.sqrt();

    let avg_bar_hours = if state.bar_hours_samples > 0 {
        (state.bar_hours_sum / (state.bar_hours_samples as f64)).max(1.0 / 60.0)
    } else {
        4.0
    };
    let ann_factor = (365.0 * (24.0 / avg_bar_hours)).sqrt();

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

    let total_trades = state.closed_positions.len();
    let winning_trades = state
        .closed_positions
        .iter()
        .filter(|p| p.realized_pnl_usdc > 0.0)
        .count();
    let losing_trades = state
        .closed_positions
        .iter()
        .filter(|p| p.realized_pnl_usdc <= 0.0)
        .count();
    let win_rate_pct = if total_trades > 0 {
        round_dp((winning_trades as f64 / total_trades as f64) * 100.0, 2)
    } else {
        0.0
    };

    let gross_profit: f64 = state
        .closed_positions
        .iter()
        .filter(|p| p.realized_pnl_usdc > 0.0)
        .map(|p| p.realized_pnl_usdc)
        .sum();
    let gross_loss: f64 = state
        .closed_positions
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

    let days_evaluated = match (state.first_evaluated_ts_ms, state.last_bar_close_ts_ms) {
        (Some(start), Some(end))
            if (!state.equity_curve.is_empty() || !state.bar_returns.is_empty()) && end > start =>
        {
            (((end - start) as f64) / 86_400_000.0).round().max(1.0) as usize
        }
        _ if !state.equity_curve.is_empty() => 1,
        _ => 0,
    };

    let mut long_trades = 0usize;
    let mut long_wins = 0usize;
    let mut long_pnl_usdc = 0.0_f64;
    let mut short_trades = 0usize;
    let mut short_wins = 0usize;
    let mut short_pnl_usdc = 0.0_f64;

    let mut regime_breakdown: BTreeMap<String, RegimeAttribution> = BTreeMap::new();
    for snap in &state.equity_curve {
        let label = {
            let trimmed = snap.market_regime_label.trim();
            if trimmed.is_empty() {
                "UNKNOWN".to_string()
            } else {
                trimmed.to_string()
            }
        };
        regime_breakdown.entry(label).or_default().bars += 1;
    }

    let mut exit_reason_breakdown: BTreeMap<String, ExitReasonSummary> = BTreeMap::new();
    for pos in &state.closed_positions {
        let is_win = pos.realized_pnl_usdc > 0.0;
        let is_long = pos.side.trim().eq_ignore_ascii_case("LONG");
        if is_long {
            long_trades += 1;
            if is_win {
                long_wins += 1;
            }
            long_pnl_usdc += pos.realized_pnl_usdc;
        } else {
            short_trades += 1;
            if is_win {
                short_wins += 1;
            }
            short_pnl_usdc += pos.realized_pnl_usdc;
        }

        let regime = resolve_trade_regime_label(pos, &state, &state.equity_curve);
        let r_entry = regime_breakdown.entry(regime).or_default();
        r_entry.trades += 1;
        if is_win {
            r_entry.winning_trades += 1;
        }
        if is_long {
            r_entry.long_trades += 1;
            r_entry.long_pnl_usdc += pos.realized_pnl_usdc;
        } else {
            r_entry.short_trades += 1;
            r_entry.short_pnl_usdc += pos.realized_pnl_usdc;
        }
        r_entry.total_pnl_usdc += pos.realized_pnl_usdc;

        let exit_reason = pos
            .exit_reason
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("UNKNOWN")
            .to_string();
        let e_entry = exit_reason_breakdown.entry(exit_reason).or_default();
        e_entry.count += 1;
        if is_win {
            e_entry.wins += 1;
        }
        e_entry.net_pnl_usdc += pos.realized_pnl_usdc;
    }

    let long_win_rate = if long_trades > 0 {
        round_dp((long_wins as f64 / long_trades as f64) * 100.0, 2)
    } else {
        0.0
    };
    let short_win_rate = if short_trades > 0 {
        round_dp((short_wins as f64 / short_trades as f64) * 100.0, 2)
    } else {
        0.0
    };
    for attr in regime_breakdown.values_mut() {
        attr.win_rate_pct = if attr.trades > 0 {
            round_dp((attr.winning_trades as f64 / attr.trades as f64) * 100.0, 2)
        } else {
            0.0
        };
        attr.long_pnl_usdc = round_dp(attr.long_pnl_usdc, 2);
        attr.short_pnl_usdc = round_dp(attr.short_pnl_usdc, 2);
        attr.total_pnl_usdc = round_dp(attr.total_pnl_usdc, 2);
    }
    for summary in exit_reason_breakdown.values_mut() {
        summary.win_rate_pct = if summary.count > 0 {
            round_dp((summary.wins as f64 / summary.count as f64) * 100.0, 2)
        } else {
            0.0
        };
        summary.net_pnl_usdc = round_dp(summary.net_pnl_usdc, 2);
    }

    BacktestReport {
        platform_version: "2.0.0".to_string(),
        risk_tier: config.risk_tier.as_str().to_string(),
        leverage_multiplier: config.allocator.effective_leverage_multiplier(),
        initial_equity_usdc: config.initial_equity_usdc,
        final_equity_usdc: round_dp(final_equity_usdc, 2),
        total_return_pct,
        sharpe_ratio,
        sortino_ratio,
        max_drawdown_pct: round_dp(state.max_drawdown_pct, 2),
        win_rate_pct,
        profit_factor,
        total_trades,
        winning_trades,
        losing_trades,
        long_trades,
        long_win_rate,
        long_pnl_usdc: round_dp(long_pnl_usdc, 2),
        short_trades,
        short_win_rate,
        short_pnl_usdc: round_dp(short_pnl_usdc, 2),
        stop_out_count: state.stop_out_count,
        cooldown_blocks: state.cooldown_blocks,
        s1_attribution_usdc: round_dp(state.s1_attribution_usdc, 2),
        s2_attribution_usdc: round_dp(state.s2_attribution_usdc, 2),
        s3_attribution_usdc: round_dp(state.s3_attribution_usdc, 2),
        total_fees_usdc: round_dp(state.total_fees_usdc, 2),
        total_funding_usdc: round_dp(state.total_funding_usdc, 2),
        bars_evaluated: state.equity_curve.len(),
        assets_evaluated,
        days_evaluated,
        timeframe,
        drawdown_halt_bars: state.drawdown_halt_bars,
        ruin_halted: state.ruin_halted,
        profit_gate_blocks: state.profit_gate_blocks,
        stagnation_stop_count: state.stagnation_stop_count,
        chase_abort_count: state.chase_abort_count,
        ioc_escalations: state.ioc_escalations,
        max_positions_blocks: state.max_positions_blocks,
        entry_interval_blocks: state.entry_interval_blocks,
        regime_breakdown,
        exit_reason_breakdown,
        equity_curve: state.equity_curve,
        closed_positions: state.closed_positions,
    }
}
