//! Walk-Forward Backtest Configuration & Performance Report DTOs (`aegis-backtest::config`).

use aegis_core::{ConfigMap, PortfolioSnapshot, PositionRecord, RiskTier};
use aegis_risk::RiskLimitsConfig;
use aegis_signals::AllocatorConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Configuration for the multi-sleeve walk-forward simulation (`R14-M14`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfig {
    pub initial_equity_usdc: f64,
    pub risk_tier: RiskTier,
    pub days: usize,
    pub warmup_bars: usize,
    #[serde(default = "default_timeframe")]
    pub timeframe: String,
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
    #[serde(default = "default_true")]
    pub wall_clock_normalize_indicators: bool,
    #[serde(default = "default_true")]
    pub enable_scale_in_out_rebalance: bool,
    #[serde(default)]
    pub enforce_slot_spread_weight_cap: bool,
    #[serde(default)]
    pub max_entries_per_bar: usize,
    #[serde(default)]
    pub maker_first_low_urgency_entries: bool,
    #[serde(default = "default_maker_only_rest_ttl_ms")]
    pub maker_only_rest_ttl_ms: i64,
    #[serde(default)]
    pub funding_entry_timing_enabled: bool,
    #[serde(default = "default_funding_entry_defer_min_rate_hourly")]
    pub funding_entry_defer_min_rate_hourly: f64,
    #[serde(default = "default_funding_entry_defer_window_sec")]
    pub funding_entry_defer_window_sec: i64,
    #[serde(default = "ConfigMap::from_defaults")]
    pub raw_config: ConfigMap,
}

fn default_timeframe() -> String {
    "4h".to_string()
}

fn default_true() -> bool {
    true
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

fn default_maker_only_rest_ttl_ms() -> i64 {
    60_000
}

fn default_funding_entry_defer_min_rate_hourly() -> f64 {
    0.0001
}

fn default_funding_entry_defer_window_sec() -> i64 {
    900
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            initial_equity_usdc: 10_000.0,
            risk_tier: RiskTier::Extreme,
            days: 30,
            warmup_bars: 24,
            timeframe: default_timeframe(),
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
            wall_clock_normalize_indicators: true,
            enable_scale_in_out_rebalance: true,
            enforce_slot_spread_weight_cap: false,
            max_entries_per_bar: 0,
            maker_first_low_urgency_entries: false,
            maker_only_rest_ttl_ms: 60_000,
            funding_entry_timing_enabled: false,
            funding_entry_defer_min_rate_hourly: 0.0001,
            funding_entry_defer_window_sec: 900,
            raw_config: ConfigMap::from_defaults(),
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
            warmup_bars: cfg.get_i64("backtest_warmup_bars", 24).clamp(1, 500) as usize,
            timeframe: cfg.get_str("signal_timeframe", "4h"),
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
            wall_clock_normalize_indicators: cfg.get_bool("wall_clock_normalize_indicators", true),
            enable_scale_in_out_rebalance: cfg.get_bool("enable_scale_in_out_rebalance", true),
            enforce_slot_spread_weight_cap: cfg.get_bool("enforce_slot_spread_weight_cap", false),
            max_entries_per_bar: cfg.get_i64("max_entries_per_bar", 0).clamp(0, 100) as usize,
            maker_first_low_urgency_entries: cfg.get_bool("maker_first_low_urgency_entries", false),
            maker_only_rest_ttl_ms: cfg
                .get_i64("maker_only_rest_ttl_ms", 60_000)
                .clamp(5_000, 600_000),
            funding_entry_timing_enabled: cfg.get_bool("funding_entry_timing_enabled", false),
            funding_entry_defer_min_rate_hourly: cfg
                .get_f64("funding_entry_defer_min_rate_hourly", 0.0001)
                .clamp(0.0, 0.05),
            funding_entry_defer_window_sec: cfg
                .get_i64("funding_entry_defer_window_sec", 900)
                .clamp(0, 3_600),
            raw_config: cfg.clone(),
        }
    }
}

/// Per-regime simulation performance attribution breakdown (`market_regime_label`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RegimeAttribution {
    pub bars: usize,
    pub trades: usize,
    pub winning_trades: usize,
    pub win_rate_pct: f64,
    pub long_trades: usize,
    pub long_pnl_usdc: f64,
    pub short_trades: usize,
    pub short_pnl_usdc: f64,
    pub total_pnl_usdc: f64,
}

/// Per-exit-reason closed trade attribution summary (`exit_reason`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ExitReasonSummary {
    pub count: usize,
    pub wins: usize,
    pub win_rate_pct: f64,
    pub net_pnl_usdc: f64,
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
    #[serde(default)]
    pub long_trades: usize,
    #[serde(default)]
    pub long_win_rate: f64,
    #[serde(default)]
    pub long_pnl_usdc: f64,
    #[serde(default)]
    pub short_trades: usize,
    #[serde(default)]
    pub short_win_rate: f64,
    #[serde(default)]
    pub short_pnl_usdc: f64,
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
    #[serde(default)]
    pub regime_breakdown: BTreeMap<String, RegimeAttribution>,
    #[serde(default)]
    pub exit_reason_breakdown: BTreeMap<String, ExitReasonSummary>,
    pub equity_curve: Vec<PortfolioSnapshot>,
    pub closed_positions: Vec<PositionRecord>,
}

pub type BacktestTearSheet = BacktestReport;

/// Single rolling walk-forward fold containing in-sample train and out-of-sample test reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardFoldReport {
    pub fold_index: usize,
    pub train_start_ms: i64,
    pub train_end_ms: i64,
    pub test_start_ms: i64,
    pub test_end_ms: i64,
    pub train_report: BacktestReport,
    pub test_report: BacktestReport,
}

/// Aggregated rolling walk-forward validation report across `N` out-of-sample folds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardReport {
    pub folds_requested: usize,
    pub train_days: usize,
    pub test_days: usize,
    pub folds: Vec<WalkForwardFoldReport>,
    pub oos_compounded_return_pct: f64,
    pub oos_mean_return_pct: f64,
    pub oos_mean_sharpe: f64,
    pub oos_worst_drawdown_pct: f64,
    pub oos_total_trades: usize,
    pub oos_win_rate_pct: f64,
    pub oos_profit_factor: f64,
    pub oos_long_pnl_usdc: f64,
    pub oos_short_pnl_usdc: f64,
}

/// Canonical 3x3 sensitivity grid axes (`stagnation_time_stop_hours` x `risk_per_trade_equity_pct`).
pub const SENSITIVITY_STAGNATION_HOURS: [f64; 3] = [12.0, 16.0, 24.0];
pub const SENSITIVITY_RISK_PER_TRADE_PCT: [f64; 3] = [0.015, 0.020, 0.025];

/// Single cell of the 3x3 stagnation-hours x risk-per-trade sensitivity matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensitivityGridCell {
    pub stagnation_hours: f64,
    pub risk_per_trade_pct: f64,
    pub total_return_pct: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub max_drawdown_pct: f64,
    pub win_rate_pct: f64,
    pub profit_factor: f64,
    pub total_trades: usize,
    pub stagnation_stop_count: usize,
    pub stop_out_count: usize,
    pub long_pnl_usdc: f64,
    pub short_pnl_usdc: f64,
    pub report: BacktestReport,
}

/// Complete 3x3 parameter sensitivity matrix output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensitivityGridReport {
    pub stagnation_hours_axis: Vec<f64>,
    pub risk_per_trade_pct_axis: Vec<f64>,
    pub cells: Vec<SensitivityGridCell>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backtest_config_warmup_bars_from_config_map_clamping() {
        let default_cfg = BacktestConfig::from_config_map(&ConfigMap::new(), 30);
        assert_eq!(default_cfg.warmup_bars, 24);
        assert!(default_cfg.enable_scale_in_out_rebalance);
        assert!(!default_cfg.enforce_slot_spread_weight_cap);
        assert_eq!(default_cfg.max_entries_per_bar, 0);
        assert!(!default_cfg.maker_first_low_urgency_entries);
        assert_eq!(default_cfg.maker_only_rest_ttl_ms, 60_000);
        assert!(!default_cfg.funding_entry_timing_enabled);
        assert!((default_cfg.funding_entry_defer_min_rate_hourly - 0.0001).abs() < 1e-12);
        assert_eq!(default_cfg.funding_entry_defer_window_sec, 900);

        let mut cfg = ConfigMap::new();
        cfg.insert("backtest_warmup_bars", "480");
        cfg.insert("signal_timeframe", "1h");
        cfg.insert("enable_scale_in_out_rebalance", "false");
        cfg.insert("enforce_slot_spread_weight_cap", "true");
        cfg.insert("max_entries_per_bar", "3");
        cfg.insert("maker_first_low_urgency_entries", "true");
        cfg.insert("maker_only_rest_ttl_ms", "120000");
        cfg.insert("funding_entry_timing_enabled", "true");
        cfg.insert("funding_entry_defer_min_rate_hourly", "0.00025");
        cfg.insert("funding_entry_defer_window_sec", "600");
        let parsed = BacktestConfig::from_config_map(&cfg, 30);
        assert_eq!(parsed.warmup_bars, 480);
        assert_eq!(parsed.timeframe, "1h");
        assert!(!parsed.enable_scale_in_out_rebalance);
        assert!(parsed.enforce_slot_spread_weight_cap);
        assert_eq!(parsed.max_entries_per_bar, 3);
        assert!(parsed.maker_first_low_urgency_entries);
        assert_eq!(parsed.maker_only_rest_ttl_ms, 120_000);
        assert!(parsed.funding_entry_timing_enabled);
        assert!((parsed.funding_entry_defer_min_rate_hourly - 0.00025).abs() < 1e-12);
        assert_eq!(parsed.funding_entry_defer_window_sec, 600);

        cfg.insert("backtest_warmup_bars", "0");
        assert_eq!(BacktestConfig::from_config_map(&cfg, 30).warmup_bars, 1);

        cfg.insert("backtest_warmup_bars", "900");
        assert_eq!(BacktestConfig::from_config_map(&cfg, 30).warmup_bars, 500);
    }
}
