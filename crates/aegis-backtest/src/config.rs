//! Walk-Forward Backtest Configuration & Performance Report DTOs (`aegis-backtest::config`).

use aegis_core::{ConfigMap, PortfolioSnapshot, PositionRecord, RiskTier};
use aegis_risk::RiskLimitsConfig;
use aegis_signals::AllocatorConfig;
use serde::{Deserialize, Serialize};

/// Configuration for the multi-sleeve walk-forward simulation (`R14-M14`).
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
    #[serde(default)]
    pub stop_out_cooldown_override_bars: Option<usize>,
    pub allocator: AllocatorConfig,
    pub risk_limits: RiskLimitsConfig,
    #[serde(default = "default_max_open_positions")]
    pub max_open_positions: usize,
    #[serde(default = "default_min_position_hold_sec")]
    pub min_position_hold_sec: i64,
    #[serde(default = "default_min_entry_interval_sec")]
    pub min_entry_interval_sec: i64,
    #[serde(default)]
    pub profit_gate_signal_exits: bool,
    #[serde(default = "default_signal_exit_min_net_profit_pct")]
    pub signal_exit_min_net_profit_pct: f64,
    #[serde(default = "default_max_daily_drawdown_pct")]
    pub max_daily_drawdown_pct: f64,
    #[serde(default = "default_max_alo_chase_ticks")]
    pub max_alo_chase_ticks: u32,
    #[serde(default = "default_max_alo_chase_drift_bps")]
    pub max_alo_chase_drift_bps: f64,
    #[serde(default = "default_chase_reject_cooldown_sec")]
    pub chase_reject_cooldown_sec: i64,
    #[serde(default = "default_entry_ioc_max_slippage_bps")]
    pub entry_ioc_max_slippage_bps: f64,
}

fn default_max_open_positions() -> usize {
    6
}

fn default_min_position_hold_sec() -> i64 {
    900
}

fn default_min_entry_interval_sec() -> i64 {
    150
}

fn default_signal_exit_min_net_profit_pct() -> f64 {
    0.0025
}

fn default_max_daily_drawdown_pct() -> f64 {
    0.15
}

fn default_max_alo_chase_ticks() -> u32 {
    3
}

fn default_max_alo_chase_drift_bps() -> f64 {
    60.0
}

fn default_chase_reject_cooldown_sec() -> i64 {
    300
}

fn default_entry_ioc_max_slippage_bps() -> f64 {
    25.0
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
            stop_out_cooldown_override_bars: None,
            allocator: AllocatorConfig::default(),
            risk_limits: RiskLimitsConfig::default(),
            max_open_positions: 6,
            min_position_hold_sec: 900,
            min_entry_interval_sec: 150,
            profit_gate_signal_exits: false,
            signal_exit_min_net_profit_pct: 0.0025,
            max_daily_drawdown_pct: 0.15,
            max_alo_chase_ticks: 3,
            max_alo_chase_drift_bps: 60.0,
            chase_reject_cooldown_sec: 300,
            entry_ioc_max_slippage_bps: 25.0,
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
            stop_out_cooldown_override_bars: None,
            allocator,
            risk_limits,
            max_open_positions: cfg.get_usize("max_open_positions", 6).clamp(1, 100),
            min_position_hold_sec: cfg.get_i64("min_position_hold_sec", 900).clamp(0, 86_400),
            min_entry_interval_sec: cfg.get_i64("min_entry_interval_sec", 150).clamp(0, 86_400),
            profit_gate_signal_exits: cfg.get_bool("profit_gate_signal_exits", false),
            signal_exit_min_net_profit_pct: cfg
                .get_f64("signal_exit_min_net_profit_pct", 0.0025)
                .clamp(0.0, 0.50),
            max_daily_drawdown_pct: cfg
                .get_f64("max_daily_drawdown_pct", 0.15)
                .clamp(0.01, 0.95),
            max_alo_chase_ticks: cfg.get_usize("max_alo_chase_ticks", 3).clamp(1, 50) as u32,
            max_alo_chase_drift_bps: cfg
                .get_f64("max_alo_chase_drift_bps", 60.0)
                .clamp(5.0, 300.0),
            chase_reject_cooldown_sec: cfg
                .get_i64("chase_reject_cooldown_sec", 300)
                .clamp(30, 3_600),
            entry_ioc_max_slippage_bps: cfg
                .get_f64("entry_ioc_max_slippage_bps", 25.0)
                .clamp(5.0, 200.0),
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
    #[serde(default)]
    pub days_evaluated: usize,
    #[serde(default)]
    pub timeframe: String,
    #[serde(default)]
    pub drawdown_halt_bars: usize,
    #[serde(default)]
    pub ruin_halted: bool,
    #[serde(default)]
    pub profit_gate_blocks: usize,
    #[serde(default)]
    pub stagnation_stop_count: usize,
    #[serde(default)]
    pub chase_abort_count: usize,
    #[serde(default)]
    pub ioc_escalations: usize,
    #[serde(default)]
    pub max_positions_blocks: usize,
    #[serde(default)]
    pub entry_interval_blocks: usize,
    pub equity_curve: Vec<PortfolioSnapshot>,
    pub closed_positions: Vec<PositionRecord>,
}

pub type BacktestTearSheet = BacktestReport;
