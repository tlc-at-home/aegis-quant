//! Walk-Forward Backtest Performance Metrics & Tear Sheet Computation (`aegis-backtest::metrics`).

use crate::config::{BacktestConfig, BacktestReport};
use crate::settlement::SimulationState;
use aegis_core::round_dp;

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
        equity_curve: state.equity_curve,
        closed_positions: state.closed_positions,
    }
}
