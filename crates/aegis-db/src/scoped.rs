//! Compile-time and runtime firewall enforcement (`BookScopedDb`, `PitConfig`, `PondConfig`).
//!
//! Enforces Isolation Invariants F1 (Signal Isolation), F2 (Position Isolation),
//! and F4 (Configuration Isolation) between Book A (`PIT`) and Book B (`POND`).

use crate::{ConfigMap, DatabaseManager};
use aegis_core::{
    AltPairMetadata, BookType, FirewallError, OhlcvBar, PondCell, PondChampion, PondDirectionPolicy,
    PondScanCycle, PondUniverseRow, PortfolioSnapshot, Result, SignalEvaluation, TradeRecord,
    UniverseAsset,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Namespaced Book A (The Pit) configuration loaded strictly from non-`pond_*` keys (Invariant F4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PitConfig {
    pub execution_mode: String,
    pub hyperliquid_testnet: bool,
    pub initial_equity_usdc: f64,
    pub book_allocation_pct: f64,
    pub book_baseline_equity_usdc: f64,
    pub baseline_risk_per_trade_usdc: f64,
    pub min_risk_per_trade_usdc: f64,
    pub max_risk_per_trade_usdc: f64,
    pub max_portfolio_heat_usdc: f64,
    pub max_portfolio_heat_pct: f64,
    pub max_single_asset_notional_usdc: f64,
    pub max_gross_leverage: f64,
    pub max_concurrent_long_positions: usize,
    pub max_concurrent_short_positions: usize,
    pub polling_interval_seconds: u64,
    pub ingest_lookback_bars: usize,
    pub platform_pp50_enabled: bool,
    pub platform_settle_gate_enabled: bool,
    pub platform_pp50_halt: bool,
}

impl PitConfig {
    pub fn from_config_map(cfg: &ConfigMap) -> Self {
        let initial_eq = cfg.get_f64("initial_equity_usdc", 10000.0);
        let pp50_enabled = cfg.get_bool("platform_pp50_enabled", false);
        let split = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
        Self {
            execution_mode: cfg.get_str("execution_mode", "PAPER"),
            hyperliquid_testnet: cfg.get_bool("hyperliquid_testnet", true),
            initial_equity_usdc: initial_eq,
            book_allocation_pct: split,
            book_baseline_equity_usdc: initial_eq * split,
            baseline_risk_per_trade_usdc: cfg.get_f64("baseline_risk_per_trade_usdc", 100.0),
            min_risk_per_trade_usdc: cfg.get_f64("min_risk_per_trade_usdc", 50.0),
            max_risk_per_trade_usdc: cfg.get_f64("max_risk_per_trade_usdc", 150.0),
            max_portfolio_heat_usdc: cfg.get_f64("max_portfolio_heat_usdc", 400.0),
            max_portfolio_heat_pct: cfg.get_f64("max_portfolio_heat_pct", 0.04),
            max_single_asset_notional_usdc: cfg.get_f64("max_single_asset_notional_usdc", 2500.0),
            max_gross_leverage: cfg.get_f64("max_gross_leverage", 1.5),
            max_concurrent_long_positions: cfg.get_usize("max_concurrent_long_positions", 2),
            max_concurrent_short_positions: cfg.get_usize("max_concurrent_short_positions", 2),
            polling_interval_seconds: cfg.get_i64("polling_interval_seconds", 15).max(1) as u64,
            ingest_lookback_bars: cfg.get_usize("ingest_lookback_bars", 120),
            platform_pp50_enabled: pp50_enabled,
            platform_settle_gate_enabled: cfg.get_bool("platform_settle_gate_enabled", false),
            platform_pp50_halt: cfg.get_bool("platform_pp50_halt", false),
        }
    }
}

/// Namespaced Book B (The Pond) configuration loaded strictly from `pond_*` and `platform_*` keys (Invariant F4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondConfig {
    pub pond_enabled: bool,
    pub pond_execution_mode: String,
    pub pond_direction_policy: PondDirectionPolicy,
    pub pond_universe_target_size: usize,
    pub pond_universe_refresh_weekday: u32,
    pub pond_universe_delist_rank: usize,
    pub pond_universe_warmup_hours: i64,
    pub pond_min_daily_volume_usdc: f64,
    pub pond_scan_offset_seconds: u64,
    pub pond_funding_z_window_hours: usize,
    pub pond_funding_z_entry: f64,
    pub pond_funding_z_exit: f64,
    pub pond_funding_floor_hourly: f64,
    pub pond_trend_veto_ema_fast: usize,
    pub pond_trend_veto_ema_slow: usize,
    pub pond_trend_veto_overextension_pct: f64,
    pub pond_liquidity_score_ref_usdc: f64,
    pub pond_max_concurrent_cells: usize,
    pub pond_max_opens_per_cycle: usize,
    pub pond_max_hold_hours: i64,
    pub pond_cooldown_hours: i64,
    pub pond_stop_cooldown_hours: i64,
    pub pond_baseline_equity_usdc: f64,
    pub pond_baseline_risk_usdc: f64,
    pub pond_min_risk_usdc: f64,
    pub pond_max_risk_usdc: f64,
    pub pond_conviction_scale_min: f64,
    pub pond_conviction_scale_max: f64,
    pub pond_max_heat_usdc: f64,
    pub pond_max_heat_pct: f64,
    pub pond_max_cell_notional_usdc: f64,
    pub pond_max_cell_pct: f64,
    pub pond_max_gross_leverage: f64,
    pub pond_kill_switch: bool,
    pub pond_ingest_stagger_seconds: f64,
    pub pond_ohlcv_retention_days: i64,
    pub pond_champion_active: String,
    pub pond_champion_recrown_margin_pct: f64,
    pub pond_universe_refresh_interval_hours: i64,
    pub pond_atr_period: usize,
    pub pond_gate5_parabolic_3h_atr_mult: f64,
    pub pond_gate6_atr_4h_floor_mult: f64,
    pub pond_gate6_structural_atr_mult: f64,
    pub pond_min_stop_pct_floor: f64,
    pub pond_conviction_z_span: f64,
    pub pond_maker_offset_bps: f64,
    pub pond_wick_buffer_atr_mult: f64,
    pub pond_chandelier_atr_mult: f64,
    pub pond_initial_stop_atr_mult: f64,
    pub pond_blowoff_atr_mult: f64,
    pub pond_blowoff_3h_adverse_atr_mult: f64,
    pub pond_trend_veto_4h_atr_mult: f64,
    pub platform_pp50_enabled: bool,
    pub platform_settle_gate_enabled: bool,
    pub platform_settle_gate_enabled_raw: bool,
    pub platform_pp50_halt: bool,
}

impl PondConfig {
    pub fn from_config_map(cfg: &ConfigMap) -> Self {
        let policy_raw = cfg.get_str("pond_direction_policy", "both");
        let policy = PondDirectionPolicy::parse(&policy_raw).unwrap_or(PondDirectionPolicy::Both);
        let initial_eq = cfg.get_f64("initial_equity_usdc", 10000.0);
        let settle_gate = cfg.get_bool("platform_settle_gate_enabled", false);
        Self {
            pond_enabled: cfg.get_bool("pond_enabled", false),
            pond_execution_mode: cfg.get_str("pond_execution_mode", "PAPER"),
            pond_direction_policy: policy,
            pond_universe_target_size: cfg.get_usize("pond_universe_target_size", 80),
            pond_universe_refresh_weekday: cfg.get_i64("pond_universe_refresh_weekday", 0).max(0) as u32,
            pond_universe_delist_rank: cfg.get_usize("pond_universe_delist_rank", 95),
            pond_universe_warmup_hours: cfg.get_i64("pond_universe_warmup_hours", 168),
            pond_min_daily_volume_usdc: cfg.get_f64("pond_min_daily_volume_usdc", 50_000.0),
            pond_scan_offset_seconds: cfg.get_i64("pond_scan_offset_seconds", 10).max(0) as u64,
            pond_funding_z_window_hours: cfg.get_usize("pond_funding_z_window_hours", 168),
            pond_funding_z_entry: cfg.get_f64("pond_funding_z_entry", 2.0),
            pond_funding_z_exit: cfg.get_f64("pond_funding_z_exit", 0.5),
            pond_funding_floor_hourly: cfg.get_f64("pond_funding_floor_hourly", 0.000015),
            pond_trend_veto_ema_fast: cfg.get_usize("pond_trend_veto_ema_fast", 16),
            pond_trend_veto_ema_slow: cfg.get_usize("pond_trend_veto_ema_slow", 64),
            pond_trend_veto_overextension_pct: cfg.get_f64("pond_trend_veto_overextension_pct", 0.08),
            pond_liquidity_score_ref_usdc: cfg.get_f64("pond_liquidity_score_ref_usdc", 250_000.0),
            pond_max_concurrent_cells: cfg.get_usize("pond_max_concurrent_cells", 8),
            pond_max_opens_per_cycle: cfg.get_usize("pond_max_opens_per_cycle", 3),
            pond_max_hold_hours: cfg.get_i64("pond_max_hold_hours", 72),
            pond_cooldown_hours: cfg.get_i64("pond_cooldown_hours", 24),
            pond_stop_cooldown_hours: cfg.get_i64("pond_stop_cooldown_hours", 56),
            pond_baseline_equity_usdc: initial_eq * (1.0 - cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90)),
            pond_baseline_risk_usdc: cfg.get_f64("pond_baseline_risk_usdc", 50.0),
            pond_min_risk_usdc: cfg.get_f64("pond_min_risk_usdc", 25.0),
            pond_max_risk_usdc: cfg.get_f64("pond_max_risk_usdc", 75.0),
            pond_conviction_scale_min: cfg.get_f64("pond_conviction_scale_min", 0.5),
            pond_conviction_scale_max: cfg.get_f64("pond_conviction_scale_max", 1.5),
            pond_max_heat_usdc: cfg.get_f64("pond_max_heat_usdc", 200.0),
            pond_max_heat_pct: cfg.get_f64("pond_max_heat_pct", 0.04),
            pond_max_cell_notional_usdc: cfg.get_f64("pond_max_cell_notional_usdc", 1250.0),
            pond_max_cell_pct: cfg.get_f64("pond_max_cell_pct", 0.25),
            pond_max_gross_leverage: cfg.get_f64("pond_max_gross_leverage", 1.5),
            pond_kill_switch: cfg.get_bool("pond_kill_switch", false),
            pond_ingest_stagger_seconds: cfg.get_f64("pond_ingest_stagger_seconds", 7.5),
            pond_ohlcv_retention_days: cfg.get_i64("pond_ohlcv_retention_days", 30),
            pond_champion_active: cfg.get_str("pond_champion_active", "NPS-01"),
            pond_champion_recrown_margin_pct: cfg.get_f64("pond_champion_recrown_margin_pct", 10.0),
            pond_universe_refresh_interval_hours: cfg.get_i64("pond_universe_refresh_interval_hours", 24),
            pond_atr_period: cfg.get_usize("pond_atr_period", 14),
            pond_gate5_parabolic_3h_atr_mult: cfg.get_f64("pond_gate5_parabolic_3h_atr_mult", 2.2),
            pond_gate6_atr_4h_floor_mult: cfg.get_f64("pond_gate6_atr_4h_floor_mult", 1.8),
            pond_gate6_structural_atr_mult: cfg.get_f64("pond_gate6_structural_atr_mult", 2.5),
            pond_min_stop_pct_floor: cfg.get_f64("pond_min_stop_pct_floor", 0.005),
            pond_conviction_z_span: cfg.get_f64("pond_conviction_z_span", 2.0),
            pond_maker_offset_bps: cfg.get_f64("pond_maker_offset_bps", 1.5),
            pond_wick_buffer_atr_mult: cfg.get_f64("pond_wick_buffer_atr_mult", 0.85),
            pond_chandelier_atr_mult: cfg.get_f64("pond_chandelier_atr_mult", 3.8),
            pond_initial_stop_atr_mult: cfg.get_f64("pond_initial_stop_atr_mult", 2.5),
            pond_blowoff_atr_mult: cfg.get_f64("pond_blowoff_atr_mult", 3.0),
            pond_blowoff_3h_adverse_atr_mult: cfg.get_f64("pond_blowoff_3h_adverse_atr_mult", 2.2),
            pond_trend_veto_4h_atr_mult: cfg.get_f64("pond_trend_veto_4h_atr_mult", 2.5),
            platform_pp50_enabled: cfg.get_bool("platform_pp50_enabled", false),
            platform_settle_gate_enabled: settle_gate,
            platform_settle_gate_enabled_raw: settle_gate,
            platform_pp50_halt: cfg.get_bool("platform_pp50_halt", false),
        }
    }
}

/// Firewalled database wrapper enforcing table and configuration whitelisting per book (Invariants F1, F2, F4).
#[derive(Clone)]
pub struct BookScopedDb {
    pool: DatabaseManager,
    book: BookType,
}

impl BookScopedDb {
    pub fn new(pool: DatabaseManager, book: BookType) -> Self {
        Self { pool, book }
    }

    pub fn book(&self) -> BookType {
        self.book
    }

    pub fn inner(&self) -> &DatabaseManager {
        &self.pool
    }

    pub fn now_ms(&self) -> i64 {
        self.pool.now_ms()
    }

    /// Enforces table & view whitelisting for `BookType::Pit` and `BookType::Pond`.
    pub fn check_table_access(&self, table_name: &str) -> std::result::Result<(), FirewallError> {
        let normalized = table_name.trim().to_ascii_lowercase();
        match self.book {
            BookType::Pit => {
                if normalized.starts_with("pond_")
                    || normalized.starts_with("v_pond_")
                    || normalized == "book_transfers"
                    || normalized == "v_book_transfers_recent"
                {
                    return Err(FirewallError::UnauthorizedTableAccess {
                        book: "PIT",
                        table: table_name.to_string(),
                    });
                }
            }
            BookType::Pond => {
                if normalized == "trades"
                    || normalized == "v_trades_pit"
                    || normalized == "signal_evaluations"
                    || normalized == "universe_assets"
                    || normalized.starts_with("v_universe_")
                    || normalized == "book_transfers"
                    || normalized == "v_book_transfers_recent"
                {
                    return Err(FirewallError::UnauthorizedTableAccess {
                        book: "POND",
                        table: table_name.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Enforces configuration key namespace isolation (Invariant F4).
    pub fn check_config_key_access(&self, config_key: &str) -> std::result::Result<(), FirewallError> {
        let normalized = config_key.trim().to_ascii_lowercase();
        match self.book {
            BookType::Pit => {
                if normalized.starts_with("pond_") || normalized.starts_with("settling_") {
                    return Err(FirewallError::UnauthorizedConfigAccess {
                        book: "PIT",
                        key: config_key.to_string(),
                    });
                }
            }
            BookType::Pond => {
                if normalized.starts_with("hmm_")
                    || normalized.starts_with("mace_")
                    || normalized.starts_with("snr_")
                    || normalized.starts_with("settling_")
                {
                    return Err(FirewallError::UnauthorizedConfigAccess {
                        book: "POND",
                        key: config_key.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn load_pit_config(&self) -> Result<PitConfig> {
        self.check_table_access("trades")?;
        self.check_config_key_access("hmm_window_bars")?;
        let cfg = self.pool.load_config()?;
        let mut pit_cfg = PitConfig::from_config_map(&cfg);
        pit_cfg.platform_settle_gate_enabled = self.pool.is_settle_gate_active()?;
        Ok(pit_cfg)
    }

    pub fn load_pond_config(&self) -> Result<PondConfig> {
        self.check_table_access("pond_cells")?;
        self.check_config_key_access("pond_direction_policy")?;
        let cfg = self.pool.load_config()?;
        let mut pond_cfg = PondConfig::from_config_map(&cfg);
        let active_gate = self.pool.is_settle_gate_active()?;
        pond_cfg.platform_settle_gate_enabled = active_gate;
        pond_cfg.platform_settle_gate_enabled_raw = active_gate;
        Ok(pond_cfg)
    }

    /// Returns true if the daily settling gate is currently active and has not exceeded its TTL (`NH-10`).
    pub fn is_settle_gate_active(&self) -> Result<bool> {
        self.pool.is_settle_gate_active()
    }

    // --- Shared Read-Only / Telemetry Operations ---

    pub fn get_ohlcv_bars(
        &self,
        symbol: &str,
        timeframe: &str,
        limit: usize,
        max_timestamp_ms: Option<i64>,
    ) -> Result<Vec<OhlcvBar>> {
        self.check_table_access("market_ohlcv")?;
        self.pool
            .get_ohlcv_bars(symbol, timeframe, limit, max_timestamp_ms)
    }

    pub fn upsert_ohlcv_batch(&self, bars: &[OhlcvBar]) -> Result<usize> {
        self.check_table_access("market_ohlcv")?;
        self.pool.upsert_ohlcv_batch(bars)
    }

    pub fn log_telemetry(
        &self,
        level: &str,
        component: &str,
        message: &str,
        details: Option<Value>,
    ) -> Result<()> {
        self.check_table_access("telemetry_logs")?;
        self.pool.log_telemetry(level, component, message, details)
    }

    pub fn get_wallet_open_symbols(&self) -> Result<std::collections::HashSet<String>> {
        self.check_table_access("books")?;
        self.pool.get_all_open_wallet_symbols()
    }

    pub fn get_wallet_perps_margin_state(&self) -> Result<(f64, f64, f64, f64)> {
        self.check_table_access("books")?;
        self.pool.get_wallet_perps_margin_state()
    }

    pub fn get_net_book_transfer_adjustment(&self) -> Result<f64> {
        self.check_table_access("books")?;
        self.pool.get_net_book_transfer_adjustment(self.book.as_str())
    }

    // --- Book A (Pit) Scoped Methods ---

    pub fn get_pit_usdc_perp_universe(&self) -> Result<Vec<UniverseAsset>> {
        self.check_table_access("v_universe_usdc_perp")?;
        self.pool.get_usdc_perp_universe()
    }

    pub fn get_pit_alt_pairs_universe(&self) -> Result<Vec<AltPairMetadata>> {
        self.check_table_access("v_universe_alt_pairs")?;
        self.pool.get_alt_pairs_universe()
    }

    pub fn get_pit_open_trades(&self) -> Result<Vec<TradeRecord>> {
        self.check_table_access("trades")?;
        self.pool.get_open_trades()
    }

    pub fn get_pit_latest_signals(&self, limit: usize) -> Result<Vec<SignalEvaluation>> {
        self.check_table_access("signal_evaluations")?;
        self.pool.get_latest_signals(limit)
    }

    pub fn get_pit_latest_portfolio_snapshot(&self) -> Result<PortfolioSnapshot> {
        self.check_table_access("portfolio_snapshots")?;
        self.pool.get_latest_portfolio_snapshot()
    }

    // --- Book B (Pond) Scoped Methods ---

    pub fn upsert_pond_universe_batch(&self, rows: &[PondUniverseRow]) -> Result<usize> {
        self.check_table_access("pond_universe")?;
        self.pool.upsert_pond_universe_batch(rows)
    }

    pub fn get_pond_universe(&self, active_only: bool) -> Result<Vec<PondUniverseRow>> {
        if active_only {
            self.check_table_access("v_pond_universe_active")?;
        } else {
            self.check_table_access("pond_universe")?;
        }
        self.pool.get_pond_universe(active_only)
    }

    pub fn get_active_pond_champion(&self) -> Result<PondChampion> {
        self.check_table_access("pond_champions")?;
        self.pool.get_active_pond_champion()
    }

    pub fn insert_pond_scan_cycle(&self, cycle: &PondScanCycle) -> Result<i64> {
        self.check_table_access("pond_scan_cycles")?;
        self.pool.insert_pond_scan_cycle(cycle)
    }

    pub fn update_pond_scan_cycle(&self, cycle: &PondScanCycle) -> Result<()> {
        self.check_table_access("pond_scan_cycles")?;
        self.pool.update_pond_scan_cycle(cycle)
    }

    pub fn get_recent_pond_scan_cycles(&self, limit: usize) -> Result<Vec<PondScanCycle>> {
        self.check_table_access("pond_scan_cycles")?;
        self.pool.get_recent_pond_scan_cycles(limit)
    }

    pub fn upsert_pond_cell(&self, cell: &PondCell) -> Result<i64> {
        self.check_table_access("pond_cells")?;
        self.pool.upsert_pond_cell(cell)
    }

    pub fn get_open_pond_cells(&self) -> Result<Vec<PondCell>> {
        self.check_table_access("v_pond_cells_open")?;
        self.pool.get_open_pond_cells()
    }

    pub fn get_armed_pond_cells(&self) -> Result<Vec<PondCell>> {
        self.check_table_access("pond_cells")?;
        self.pool.get_armed_pond_cells()
    }

    pub fn get_pond_cells(&self, limit: usize) -> Result<Vec<PondCell>> {
        self.check_table_access("pond_cells")?;
        self.pool.get_pond_cells(limit)
    }

    pub fn get_pond_realized_pnl(&self) -> Result<f64> {
        self.check_table_access("pond_cells")?;
        self.pool.get_pond_realized_pnl()
    }
}
