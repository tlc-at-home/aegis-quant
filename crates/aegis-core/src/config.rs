//! Canonical v2.0 configuration domain types, risk tiers, symbol normalization, and seed defaults (`aegis-core::config`).

use crate::errors::{AegisError, Result};
use crate::security::{
    is_immutable_security_config_key, validate_evm_address, validate_private_key,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub const PLATFORM_NAME: &str = "Aegis Quant";
pub const PLATFORM_VERSION: &str = "2.0.0";

/// Selectable portfolio leverage / risk appetite tier (`Conservative`, `Standard`, `Aggressive`, `Extreme`, `Custom`).
/// Seeded by default at `Extreme (1.85x)` per Rev 3.1 architecture spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RiskTier {
    Conservative,
    Standard,
    Aggressive,
    Extreme,
    Custom,
}

impl RiskTier {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "conservative" | "0.65x" => Self::Conservative,
            "standard" | "1.00x" | "1.0x" => Self::Standard,
            "aggressive" | "1.35x" => Self::Aggressive,
            "extreme" | "1.85x" => Self::Extreme,
            "custom" => Self::Custom,
            _ => Self::Standard,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Conservative => "conservative",
            Self::Standard => "standard",
            Self::Aggressive => "aggressive",
            Self::Extreme => "extreme",
            Self::Custom => "custom",
        }
    }

    pub fn display_badge(&self, custom_mult: Option<f64>) -> String {
        let m = self.leverage_multiplier(custom_mult);
        match self {
            Self::Conservative => format!("🛡️ CONSERVATIVE ({m:.2}x)"),
            Self::Standard => format!("⚖️ STANDARD ({m:.2}x)"),
            Self::Aggressive => format!("⚡ AGGRESSIVE ({m:.2}x)"),
            Self::Extreme => format!("🔥 EXTREME ({m:.2}x)"),
            Self::Custom => format!("🎛️ CUSTOM ({m:.2}x)"),
        }
    }

    pub fn leverage_multiplier(&self, custom_override: Option<f64>) -> f64 {
        match self {
            Self::Conservative => 0.65,
            Self::Standard => 1.00,
            Self::Aggressive => 1.35,
            Self::Extreme => 1.85,
            Self::Custom => custom_override
                .filter(|v| v.is_finite() && *v > 0.0)
                .unwrap_or(1.85)
                .clamp(0.10, 3.00),
        }
    }

    pub fn default_risk_per_trade_equity_pct(&self, custom_mult: Option<f64>) -> f64 {
        match self {
            Self::Conservative => 0.005,
            Self::Standard => 0.010,
            Self::Aggressive => 0.015,
            Self::Extreme => 0.020,
            Self::Custom => (0.010
                * custom_mult
                    .filter(|v| v.is_finite() && *v > 0.0)
                    .unwrap_or(1.0))
            .clamp(0.0025, 0.05),
        }
    }

    pub fn default_max_single_asset_weight(&self, custom_mult: Option<f64>) -> f64 {
        match self {
            Self::Conservative => 0.15,
            Self::Standard => 0.15,
            Self::Aggressive => 0.20,
            Self::Extreme => 0.25,
            Self::Custom => (0.15
                * custom_mult
                    .filter(|v| v.is_finite() && *v > 0.0)
                    .unwrap_or(1.0)
                    .max(1.0))
            .clamp(0.15, 0.35),
        }
    }
}

/// Execution routing mode (`PAPER`, `TESTNET`, `LIVE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ExecutionMode {
    Paper,
    Testnet,
    Live,
}

impl ExecutionMode {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_uppercase().as_str() {
            "LIVE" | "MAINNET" => Self::Live,
            "TESTNET" => Self::Testnet,
            _ => Self::Paper,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Paper => "PAPER",
            Self::Testnet => "TESTNET",
            Self::Live => "LIVE",
        }
    }

    pub fn is_live_or_testnet(&self) -> bool {
        matches!(self, Self::Testnet | Self::Live)
    }
}

/// Primary alert provider (`telegram` default, config-switchable to `discord` or `both`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertProvider {
    Telegram,
    Discord,
    Both,
}

impl AlertProvider {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "discord" => Self::Discord,
            "both" => Self::Both,
            _ => Self::Telegram,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Telegram => "telegram",
            Self::Discord => "discord",
            Self::Both => "both",
        }
    }
}

const HYPERLIQUID_K_PREFIX_COINS: &[&str] = &[
    "kPEPE", "kBONK", "kSHIB", "kFLOKI", "kLUNC", "kNEIRO", "kDOGS",
];

/// Normalizes any Hyperliquid coin or legacy symbol (`"BTC"`, `"BTC/USDC:USDC"`, `"BTC-USDC"`, `"kPEPE"`)
/// into canonical v2.0 `(symbol, coin)` tuple: `("BTC-USDC", "BTC")`.
pub fn normalize_hyperliquid_symbol(raw: &str) -> Result<(String, String)> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AegisError::Validation("Symbol cannot be empty".to_string()));
    }
    let coin_part = trimmed
        .split(['/', '-', ':'])
        .next()
        .unwrap_or(trimmed)
        .trim();
    if coin_part.is_empty()
        || !coin_part
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(AegisError::Validation(format!(
            "Invalid Hyperliquid coin symbol '{raw}'"
        )));
    }
    let coin = if let Some(canonical_k) = HYPERLIQUID_K_PREFIX_COINS
        .iter()
        .find(|&&c| c.eq_ignore_ascii_case(coin_part))
    {
        (*canonical_k).to_string()
    } else if coin_part.starts_with('k')
        && coin_part.len() > 1
        && coin_part
            .chars()
            .nth(1)
            .is_some_and(|c| c.is_ascii_uppercase())
    {
        format!("k{}", coin_part[1..].to_ascii_uppercase())
    } else {
        coin_part.to_ascii_uppercase()
    };
    let symbol = format!("{coin}-USDC");
    Ok((symbol, coin))
}

/// Typed key-value configuration snapshot loaded from MariaDB `system_config`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfigMap(pub HashMap<String, String>);

impl ConfigMap {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn from_map(map: HashMap<String, String>) -> Self {
        Self(
            map.into_iter()
                .map(|(k, v)| (k.trim().to_ascii_lowercase(), v))
                .collect(),
        )
    }

    /// Populates a `ConfigMap` from `default_v2_system_config_seeds()`.
    pub fn from_defaults() -> Self {
        let mut cfg = Self::new();
        for (k, v, _, _) in default_v2_system_config_seeds() {
            cfg.insert(k, v);
        }
        cfg
    }

    pub fn insert(&mut self, key: impl Into<String>, val: impl Into<String>) {
        self.0
            .insert(key.into().trim().to_ascii_lowercase(), val.into());
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.0
            .get(key)
            .or_else(|| self.0.get(&key.trim().to_ascii_lowercase()))
    }

    pub fn get_str(&self, key: &str, default: &str) -> String {
        self.get(key)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| default.to_string())
    }

    pub fn get_f64(&self, key: &str, default: f64) -> f64 {
        self.get(key)
            .and_then(|s| s.trim().parse::<f64>().ok())
            .filter(|v| v.is_finite())
            .unwrap_or(default)
    }

    pub fn get_i64(&self, key: &str, default: i64) -> i64 {
        self.get(key)
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(default)
    }

    pub fn get_usize(&self, key: &str, default: usize) -> usize {
        self.get(key)
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(default)
    }

    pub fn get_bool(&self, key: &str, default: bool) -> bool {
        self.get(key)
            .map(|s| match s.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" => false,
                _ => default,
            })
            .unwrap_or(default)
    }

    pub fn risk_tier(&self) -> RiskTier {
        RiskTier::parse(&self.get_str("risk_tier", "extreme"))
    }

    pub fn target_leverage_multiplier(&self) -> f64 {
        let tier = self.risk_tier();
        let custom = self
            .get("target_leverage_multiplier")
            .and_then(|v| v.trim().parse::<f64>().ok());
        if tier == RiskTier::Custom {
            tier.leverage_multiplier(custom)
        } else {
            tier.leverage_multiplier(None)
        }
    }

    pub fn execution_mode(&self) -> ExecutionMode {
        ExecutionMode::parse(&self.get_str("execution_mode", "TESTNET"))
    }

    pub fn alert_provider(&self) -> AlertProvider {
        AlertProvider::parse(&self.get_str("alert_provider", "telegram"))
    }
}

/// Resolves the workspace root directory.
pub fn resolve_project_root() -> PathBuf {
    if let Ok(env_root) = std::env::var("AEGIS_ROOT") {
        let p = PathBuf::from(env_root.trim());
        if p.is_dir() {
            return p;
        }
    }
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let p = Path::new(&manifest_dir);
        if let Some(root) = p.parent().and_then(|parent| parent.parent()) {
            if root.join("Cargo.toml").is_file() {
                return root.to_path_buf();
            }
        }
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

/// Returns `true` when Hyperliquid `userAbstraction` indicates unified/portfolio collateral
/// where `spotClearinghouseState` USDC `total` already encompasses perp margin allocation.
pub fn is_unified_account_mode(mode: &str) -> bool {
    matches!(
        mode.trim().to_ascii_lowercase().as_str(),
        "unifiedaccount" | "portfoliomargin"
    )
}

/// Canonical default v2.0 `system_config` seed entries (`(key, value, value_type, description)`).
pub fn default_v2_system_config_seeds(
) -> Vec<(&'static str, &'static str, &'static str, &'static str)> {
    vec![
        ("platform_version", "2.0.0", "string", "Aegis Quant v2.0 Modular Multi-Sleeve Engine"),
        ("execution_mode", "TESTNET", "string", "Execution routing mode: PAPER, TESTNET, or LIVE"),
        ("hyperliquid_testnet", "true", "bool", "Route orders and info queries to Hyperliquid Testnet when true"),
        ("hyperliquid_mainnet_url", "https://api.hyperliquid.xyz", "string", "Hyperliquid Mainnet API base URL"),
        ("hyperliquid_testnet_url", "https://api.hyperliquid-testnet.xyz", "string", "Hyperliquid Testnet API base URL"),
        ("hyperliquid_allowed_hosts", "api.hyperliquid.xyz,api.hyperliquid-testnet.xyz", "string", "Allowlisted Hyperliquid API hosts"),
        ("risk_tier", "extreme", "string", "Active portfolio risk tier: conservative (0.65x), standard (1.00x), aggressive (1.35x), extreme (1.85x), custom"),
        ("target_leverage_multiplier", "1.85", "float", "Active target leverage multiplier L_tier seeded at Extreme (1.85x)"),
        ("max_gross_leverage_x", "2.50", "float", "Hard portfolio gross leverage ceiling (2.50x)"),
        ("max_open_positions", "6", "int", "Hard cap on total concurrent active positions (OPEN + WORKING combined)"),
        ("min_entry_interval_sec", "150", "int", "Minimum cooldown interval in seconds between opening new positions (150s default)"),
        ("max_single_asset_weight", "0.25", "float", "Maximum single-asset portfolio weight cap (25% of equity at extreme tier)"),
        ("min_rebalance_delta_weight", "0.025", "float", "Minimum weight delta (2.5% of equity) required to submit a rebalance order"),
        ("min_order_notional_usdc", "12.0", "float", "Hyperliquid minimum order notional floor in USDC ($12.00)"),
        ("initial_paper_equity_usdc", "10000.0", "float", "Default starting equity in USDC for PAPER mode or empty account fallback"),
        ("live_exchange_equity_usdc", "0.0", "float", "Live Hyperliquid perp accountValue in USDC continuously synced by reconciler"),
        ("live_exchange_equity_synced_at_ms", "0", "int", "Timestamp (ms) of latest successful Hyperliquid clearinghouseState equity sync"),
        ("live_exchange_margin_used_usdc", "0.0", "float", "Latest synced Hyperliquid perp marginSummary.totalMarginUsed in USDC"),
        ("live_exchange_withdrawable_usdc", "0.0", "float", "Latest synced Hyperliquid perp withdrawable USDC balance"),
        ("live_exchange_open_orders_count", "0", "int", "Total resting Hyperliquid open orders from last reconcile (entry ALO + SL/TP triggers + passive closes)"),
        ("live_exchange_spot_usdc", "0.0", "float", "Latest synced Hyperliquid spot USDC balance (informational; segregated from perp margin)"),
        ("live_exchange_spot_synced_at_ms", "0", "int", "Timestamp (ms) of latest Hyperliquid spotClearinghouseState sync"),
        ("live_exchange_account_mode", "default", "string", "Latest synced Hyperliquid userAbstraction mode (default, unifiedAccount, portfolioMargin)"),
        ("live_exchange_perp_margin_equity_usdc", "0.0", "float", "Latest synced Hyperliquid perp clearinghouseState marginSummary.accountValue in USDC"),
        ("live_exchange_portfolio_pnl_json", "", "json", "Cached Hyperliquid portfolio perpDay/perpWeek/perpMonth/perpAllTime PnL snapshot JSON synced by reconciler (read-only)"),
        ("live_exchange_maker_fee_bps", "0.0", "float", "Synced account Hyperliquid maker fee tier in bps (0 falls back to static maker_fee_bps; read-only)"),
        ("live_exchange_taker_fee_bps", "0.0", "float", "Synced account Hyperliquid taker fee tier in bps (0 falls back to static taker_fee_bps; read-only)"),
        ("live_vault_info_json", "", "json", "Cached Hyperliquid vault portfolio snapshot JSON synced by reconciler (read-only; segregated from perp margin)"),
        ("priority_monitor_health_json", "", "json", "Cached 1s PriorityMonitor surveillance health snapshot JSON (read-only)"),
        ("ws_stream_status_json", "", "json", "Cached Hyperliquid WebSocket Market+User stream health snapshot JSON (read-only)"),
        ("hyperliquid_vault_address", "", "string", "Configured Hyperliquid Vault EVM address (0x...) tracked alongside active wallet vault deposits"),
        ("vault_role", "follower", "string", "Vault operating role: follower (Barbell/HLP), operator (Aegis-led User Vault), or none"),
        ("vault_target_allocation_pct", "0.0", "float", "Target percentage of combined wallet equity allocated to Hyperliquid vaults (0-100%, informational barbell benchmark)"),
        ("vault_profit_sweep_threshold_usdc", "0.0", "float", "Realized perp profit threshold in USDC (0 disables) for operator vault profit-sweep reminders"),
        ("vault_notes", "", "string", "Operator vault thesis, mandate, and maintenance notes (plain text, max 500 chars)"),
        ("risk_capital_cap_usdc", "1000.0", "float", "Optional user portfolio capital sizing cap in USDC (0 disables cap)"),
        ("risk_capital_cap_updated_at_ms", "0", "int", "Cutoff timestamp (ms; 0 disables legacy isolation) below which pre-cap holdings are excluded from active cap deleverage"),
        ("min_position_hold_sec", "900", "int", "Minimum hold duration in seconds before signal closes or auto-deleverage may target a newly filled position"),
        ("profit_gate_signal_exits", "false", "bool", "Block signal-driven CLOSE exits while estimated net PnL (after fees/funding) is below signal_exit_min_net_profit_pct; hard SL/TP/trailing stops and FLIP remain unrestricted"),
        ("signal_exit_min_net_profit_pct", "0.0025", "float", "Minimum estimated net profit fraction of entry notional (0.25% default) required before signal CLOSE may exit an open position when profit_gate_signal_exits=true"),
        ("max_entry_spread_bps", "10.0", "float", "Maximum allowed L2 bid-ask spread in basis points for new WORKING orders (10.0 bps default)"),
        ("max_entry_oracle_deviation_pct", "0.015", "float", "Maximum fractional divergence allowed between entry mid/mark price and oracle price (1.5% default)"),
        ("min_entry_l2_depth_mult", "1.5", "float", "Minimum 15bps L2 book depth multiplier relative to target order notional on both sides"),
        ("min_entry_l2_depth_usdc", "150.0", "float", "Minimum absolute 15bps L2 book depth floor in USDC on both bid and ask sides"),
        ("max_daily_drawdown_pct", "0.15", "float", "Maximum 24h peak-to-trough uncapped equity drawdown fraction before halting new entries"),
        ("min_equity_halt_usdc", "50.0", "float", "Minimum live equity floor in USDC below which ruin-halt blocks all new entries (R14-M12)"),
        ("risk_per_trade_equity_pct", "0.02", "float", "Risk-parity per-trade equity risk budget fraction (2.0% extreme default; 0.0 disables risk-parity cap)"),
        ("max_portfolio_heat_pct", "0.12", "float", "Maximum aggregate open stop-loss risk as a fraction of equity (12% default)"),
        ("auto_deleverage_confirm_ticks", "2", "int", "Consecutive breach ticks required before AUTO_DELEVERAGE closes OPEN holdings (1-5)"),
        ("scanner_interval_ms", "3000", "int", "Full-exchange 150+ perp scanner cadence in milliseconds (3s)"),
        ("priority_monitor_interval_ms", "1000", "int", "High-frequency priority watchlist & active order monitor cadence in milliseconds (1s)"),
        ("core_min_day_volume_usdc", "2000000.0", "float", "Minimum 24h USDC volume on Mainnet ($2M) for Core S1/S2 eligibility"),
        ("event_min_day_volume_usdc", "500000.0", "float", "Minimum 24h USDC volume on Mainnet ($500k) for Event S3 eligibility"),
        ("core_min_open_interest_usdc", "250000.0", "float", "Minimum open interest on Mainnet ($250k) for Core S1/S2 eligibility"),
        ("event_min_open_interest_usdc", "100000.0", "float", "Minimum open interest on Mainnet ($100k) for Event S3 eligibility"),
        ("testnet_core_min_day_volume_usdc", "50000.0", "float", "Minimum 24h USDC volume on Testnet ($50k) for Core S1/S2 eligibility"),
        ("testnet_event_min_day_volume_usdc", "50000.0", "float", "Minimum 24h USDC volume on Testnet ($50k) for Event S3 eligibility"),
        ("testnet_core_min_open_interest_usdc", "50000.0", "float", "Minimum open interest on Testnet ($50k) for Core S1/S2 eligibility"),
        ("testnet_event_min_open_interest_usdc", "50000.0", "float", "Minimum open interest on Testnet ($50k) for Event S3 eligibility"),
        ("s1_carry_mom_budget", "0.45", "float", "Sleeve 1 (Cross-Sectional Carry-Momentum & Beta Hedge) base portfolio weight budget"),
        ("s2_trend_breakout_budget", "0.40", "float", "Sleeve 2 (Breadth-Confirmed Multi-Speed Trend & Live 3s Breakout) base portfolio weight budget"),
        ("s3_event_reversion_budget", "0.10", "float", "Sleeve 3 (Live Capitulation Flush & Funding Dislocation) base portfolio weight budget"),
        ("enable_breadth_regime_gate", "true", "bool", "Gate counter-regime short/long entries when market breadth signals strong expansion/contraction"),
        ("breadth_bull_block_shorts_threshold", "0.68", "float", "Bullish breadth threshold above which counter-regime short entries are blocked"),
        ("breadth_bear_block_longs_threshold", "0.32", "float", "Bearish breadth threshold below which counter-regime long entries are blocked"),
        ("early_loser_invalidation_enabled", "true", "bool", "Allow immediate exit of underwater losers on trend/momentum invalidation without holding full 900s floor"),
        ("early_loser_r0_mult", "0.25", "float", "Fraction of initial entry_r0 stop distance required before early_loser_invalidation bypasses min_position_hold_sec (0.05-1.0)"),
        ("early_loser_max_loss_pct", "0.009", "float", "Maximum fractional loss hurdle cap for early_loser_invalidation bypass (0.003-0.10)"),
        ("hysteresis_s1_reversal_mom_threshold", "0.08", "float", "Held-position S1 momentum reversal magnitude threshold"),
        ("hysteresis_s1_support_composite_threshold", "0.025", "float", "Held-position S1 composite support threshold"),
        ("protective_stop_atr_mult", "2.2", "float", "Protective trailing stop distance multiplier in ATR units"),
        ("protective_stop_default_pct", "0.04", "float", "Default protective stop distance fraction of entry price (4.0%)"),
        ("breakeven_trigger_r", "0.40", "float", "MFE excursion multiple in R-units activating the breakeven stop lock"),
        ("breakeven_lock_r", "0.05", "float", "Profit lock multiple in R-units applied when breakeven tier activates"),
        ("stagnation_time_stop_hours", "12.0", "float", "Hours open below stagnation_min_mfe_r before closing stagnant position (0 disables)"),
        ("stagnation_min_mfe_r", "0.35", "float", "Minimum MFE in R-units required to keep a trade exempt from stagnation time-stop"),
        ("take_profit_atr_mult", "2.8", "float", "Take-profit target distance multiplier in ATR units (~1.27x reward-to-risk vs 2.2x ATR stop)"),
        ("take_profit_default_pct", "0.055", "float", "Default take-profit target distance fraction of entry price (5.5%)"),
        ("stop_out_cooldown_sec", "1800", "int", "Post-stop-out asset cooldown duration in seconds (30 minutes)"),
        ("max_alo_chase_ticks", "3", "int", "Maximum 1-second ALO maker limit chase attempts before IOC crossing or cancel"),
        ("alo_chase_threshold_bps", "8.0", "float", "Mid-price drift threshold in basis points triggering a 1s ALO limit price chase"),
        ("max_alo_chase_drift_bps", "60.0", "float", "Maximum cumulative ALO chase drift in bps from initial working limit price before abort"),
        ("max_alo_oracle_drift_bps", "150.0", "float", "Maximum ALO chase drift in bps from oracle price before abort"),
        ("chase_reject_cooldown_sec", "300", "int", "CHURN_GUARD cooldown in seconds after chase abort, repeated rejects, or ALO expiries"),
        ("entry_ioc_max_slippage_bps", "25.0", "float", "Maximum adverse slippage in bps allowed on entry chase-to-IOC escalation"),
        ("sl_trigger_limit_band_bps", "100.0", "float", "Stop-Limit protective trigger limit slippage band in bps (1.0% default)"),
        ("passive_close_rest_sec", "8", "int", "Seconds to rest maker-first reduce-only close order before IOC ladder escalation"),
        ("priority_unmonitored_alert_ticks", "10", "int", "Consecutive degraded 1s priority monitor ticks before firing CRITICAL unmonitored alert"),
        ("hl_reject_backoff_threshold", "3", "int", "Exchange rejects within 15m window triggering chase_reject_cooldown_sec"),
        ("alo_expiry_backoff_threshold", "2", "int", "ALO expiries within 30m window triggering full chase_reject_cooldown_sec"),
        ("trigger_oracle_fallback_basis_bps", "50.0", "float", "Mark-vs-oracle basis threshold in bps switching 1s surveillance to oracle price"),
        ("emergency_ioc_breach_pct", "0.015", "float", "Stop-breach gap fraction bypassing passive close wait for immediate IOC exit"),
        ("hl_rest_weight_limit_per_min", "900", "int", "Self-imposed Hyperliquid REST weight budget per minute (300-1150)"),
        ("candle_refresh_max_assets", "30", "int", "Maximum non-active top-volume assets refreshed per candle cycle"),
        ("backtest_warmup_bars", "24", "int", "Number of warmup bars preceding backtest evaluation window (1-500)"),
        ("signal_timeframe", "4h", "string", "Canonical bar timeframe used for multi-sleeve signal generation and ATR stops (1m, 5m, 15m, 1h, 4h, 1d)"),
        ("wall_clock_normalize_indicators", "true", "bool", "Normalize indicator lookbacks by bar duration wall-clock time across timeframes"),
        ("s3_strict_admission_enabled", "true", "bool", "Enforce strict Sleeve 3 capitulation flush and funding dislocation admission filters"),
        ("enable_scale_in_out_rebalance", "true", "bool", "Allow incremental scale-in and scale-out rebalance orders on existing open positions"),
        ("enforce_slot_spread_weight_cap", "false", "bool", "Cap per-position weight by slot-spread leverage quota when enabled"),
        ("max_entries_per_bar", "0", "int", "Maximum new entries permitted per signal bar (0 disables bar-level cap)"),
        ("enable_regime_budget_multipliers", "false", "bool", "Apply directional regime multipliers in chop and neutral breadth regimes"),
        ("chop_long_weight_mult", "0.50", "float", "Long target weight multiplier in CHOP regime when regime budget multipliers are enabled"),
        ("chop_short_weight_mult", "0.65", "float", "Short target weight multiplier in CHOP regime when regime budget multipliers are enabled"),
        ("neutral_long_weight_mult", "0.65", "float", "Long target weight multiplier in NEUTRAL regime when regime budget multipliers are enabled"),
        ("signal_exit_confirm_bars", "1", "int", "Consecutive signal bars required to confirm a signal-driven close exit (1-5)"),
        ("suppress_direct_flips", "false", "bool", "Close existing position instead of direct single-step side flip when true"),
        ("max_net_exposure_ratio_of_gross", "1.0", "float", "Maximum allowed net exposure as a fraction of total gross exposure (0.10-1.0)"),
        ("maker_first_low_urgency_entries", "false", "bool", "Prefer resting maker-only post-only entries for low-urgency rebalances"),
        ("maker_only_rest_ttl_ms", "60000", "int", "Maximum resting TTL in milliseconds for maker-only low-urgency entries (5000-600000)"),
        ("funding_entry_timing_enabled", "false", "bool", "Defer entries shortly before adverse hourly funding settlement when enabled"),
        ("funding_entry_defer_min_rate_hourly", "0.0001", "float", "Minimum adverse hourly funding rate threshold triggering entry deferral (0.0-0.05)"),
        ("funding_entry_defer_window_sec", "900", "int", "Seconds before hourly funding settlement within which adverse entries are deferred (0-3600)"),
        ("hl_websocket_enabled", "true", "bool", "Enable 2-connection Hyperliquid WebSocket Market+User cache with REST fallback"),
        ("retention_snapshots_days", "30", "int", "Days to retain intraday portfolio_snapshots (daily YTD anchors preserved 400d)"),
        ("retention_telemetry_days", "14", "int", "Days to retain telemetry_logs rows"),
        ("retention_order_audit_days", "30", "int", "Days to retain order_audit rows for closed/cancelled positions"),
        ("retention_cooldowns_days", "14", "int", "Days to retain inactive asset_cooldowns rows"),
        ("retention_alert_queue_days", "30", "int", "Days to retain terminal alert_queue rows"),
        ("retention_ohlcv_bars_days", "180", "int", "Days to retain ohlcv_bars rows beyond active open positions"),
        ("retention_funding_events_days", "365", "int", "Days to retain settled funding_events rows"),
        ("retention_prune_interval_sec", "3600", "int", "Automated DB retention prune cadence in seconds"),
        ("retention_prune_batch_size", "1000", "int", "Batch delete size per retention prune pass"),
        ("maker_fee_bps", "1.5", "float", "Hyperliquid perpetual maker fee in basis points (1.5 bps)"),
        ("taker_fee_bps", "4.5", "float", "Hyperliquid perpetual taker fee in basis points (4.5 bps)"),
        ("alert_provider", "telegram", "string", "Primary real-time alert provider: telegram (HTML cards), discord, or both"),
        ("telegram_api_base_url", "https://api.telegram.org", "string", "Telegram Bot API base endpoint URL"),
        ("telegram_bot_token", "", "secret", "Fernet-encrypted Telegram Bot API token"),
        ("telegram_chat_id", "", "string", "Telegram target chat ID for real-time operational notifications"),
        ("discord_webhook_url", "", "secret", "Fernet-encrypted Discord webhook URL"),
        ("wallet_address", "", "string", "Hyperliquid L1 master wallet EVM address (0x...)"),
        ("private_key", "", "secret", "Fernet-encrypted Trade-Only API Agent Wallet Private Key"),
        ("web_api_token", "", "secret", "Fernet-encrypted Web API Bearer token"),
        ("web_bind_host", "127.0.0.1", "string", "WebUI listening interface"),
        ("web_server_port", "8000", "int", "WebUI REST + SSE server listening port"),
        ("web_allow_remote_bind", "false", "bool", "Allow WebUI binding to non-loopback address without --allow-remote-bind"),
        ("web_trusted_proxies", "", "string", "Comma-separated CIDR/IP allowlist of trusted reverse proxies for X-Forwarded-* headers"),
        ("enforce_master_key", "true", "bool", "Require external or keyfile Fernet master key"),
    ]
}

fn parse_f64_bounded(key: &str, raw: &str, min: f64, max: f64) -> Result<f64> {
    let val = raw.trim().parse::<f64>().map_err(|_| {
        AegisError::Validation(format!("'{key}' must be a valid number, got '{raw}'"))
    })?;
    if !val.is_finite() || val < min || val > max {
        return Err(AegisError::Validation(format!(
            "'{key}' must be between {min} and {max}, got {val}"
        )));
    }
    Ok(val)
}

fn parse_i64_bounded(key: &str, raw: &str, min: i64, max: i64) -> Result<i64> {
    let val = raw.trim().parse::<i64>().map_err(|_| {
        AegisError::Validation(format!("'{key}' must be a valid integer, got '{raw}'"))
    })?;
    if val < min || val > max {
        return Err(AegisError::Validation(format!(
            "'{key}' must be between {min} and {max}, got {val}"
        )));
    }
    Ok(val)
}

fn parse_bool_strict(key: &str, raw: &str) -> Result<()> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "false" => Ok(()),
        _ => Err(AegisError::Validation(format!(
            "'{key}' must be 'true' or 'false', got '{raw}'"
        ))),
    }
}

/// Canonical single-source validator for `system_config` key/value updates (`#18` Sec B).
///
/// Enforces key syntax, immutable security/internal-state rejection (`SecurityViolation`),
/// raw ciphertext envelope rejection, enum membership, and bounded numeric ranges.
pub fn validate_core_config_key_value(key: &str, value_str: &str) -> Result<()> {
    let clean_key = key.trim();
    let clean_val = value_str.trim();

    if clean_key.is_empty() || clean_key.len() > 128 {
        return Err(AegisError::Validation(
            "config_key must be between 1 and 128 characters".to_string(),
        ));
    }
    if !clean_key
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic())
        || !clean_key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(AegisError::Validation(format!(
            "Invalid config_key '{clean_key}': must start with an ASCII letter and contain only [A-Za-z0-9_]"
        )));
    }

    if is_immutable_security_config_key(clean_key) {
        return Err(AegisError::SecurityViolation(format!(
            "Configuration key '{clean_key}' is an immutable security or internal state key and cannot be modified via Web API"
        )));
    }

    if clean_val.starts_with("gAAAAAB") || clean_val.starts_with("ENC:") {
        return Err(AegisError::Validation(format!(
            "Configuration value for '{clean_key}' cannot be a raw encrypted ciphertext envelope"
        )));
    }

    let lower_key = clean_key.to_ascii_lowercase();
    match lower_key.as_str() {
        "execution_mode" => {
            if !matches!(
                clean_val.to_ascii_uppercase().as_str(),
                "PAPER" | "TESTNET" | "LIVE"
            ) {
                return Err(AegisError::Validation(format!(
                    "Invalid execution_mode '{clean_val}': expected PAPER, TESTNET, or LIVE"
                )));
            }
        }
        "risk_tier" => {
            if !matches!(
                clean_val.to_ascii_lowercase().as_str(),
                "conservative" | "standard" | "aggressive" | "extreme" | "custom"
            ) {
                return Err(AegisError::Validation(format!(
                    "Invalid risk_tier '{clean_val}': expected conservative, standard, aggressive, extreme, or custom"
                )));
            }
        }
        "alert_provider" => {
            if !matches!(
                clean_val.to_ascii_lowercase().as_str(),
                "telegram" | "discord" | "both"
            ) {
                return Err(AegisError::Validation(format!(
                    "Invalid alert_provider '{clean_val}': expected telegram, discord, or both"
                )));
            }
        }
        "auto_rebalance_enabled"
        | "profit_gate_signal_exits"
        | "hl_websocket_enabled"
        | "enable_breadth_regime_gate"
        | "early_loser_invalidation_enabled"
        | "wall_clock_normalize_indicators"
        | "s3_strict_admission_enabled"
        | "enable_scale_in_out_rebalance"
        | "enforce_slot_spread_weight_cap"
        | "enable_regime_budget_multipliers"
        | "suppress_direct_flips"
        | "maker_first_low_urgency_entries"
        | "funding_entry_timing_enabled" => {
            parse_bool_strict(clean_key, clean_val)?;
        }
        "signal_timeframe" => {
            if !matches!(clean_val, "1m" | "5m" | "15m" | "1h" | "4h" | "1d") {
                return Err(AegisError::Validation(format!(
                    "Invalid signal_timeframe '{clean_val}': expected one of 1m, 5m, 15m, 1h, 4h, 1d"
                )));
            }
        }
        "signal_exit_min_net_profit_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 0.50)?;
        }
        "wallet_address" | "hyperliquid_agent_address" | "hyperliquid_vault_address" => {
            if !clean_val.is_empty() {
                let _ = validate_evm_address(clean_val)?;
            }
        }
        "vault_role" => {
            if !matches!(
                clean_val.to_ascii_lowercase().as_str(),
                "follower" | "operator" | "none"
            ) {
                return Err(AegisError::Validation(format!(
                    "Invalid vault_role '{clean_val}': expected follower, operator, or none"
                )));
            }
        }
        "vault_target_allocation_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 100.0)?;
        }
        "vault_profit_sweep_threshold_usdc" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 10_000_000.0)?;
        }
        "vault_notes" => {
            if clean_val.chars().count() > 500 {
                return Err(AegisError::Validation(
                    "vault_notes must be at most 500 characters".to_string(),
                ));
            }
            if value_str
                .chars()
                .any(|c| c.is_ascii_control() && !matches!(c, '\n' | '\r' | '\t'))
            {
                return Err(AegisError::Validation(
                    "vault_notes must not contain ASCII control characters".to_string(),
                ));
            }
        }
        "private_key" => {
            if clean_val.is_empty() {
                return Err(AegisError::Validation(
                    "private_key cannot be cleared to empty via Web API".to_string(),
                ));
            }
            let _ = validate_private_key(clean_val)?;
        }
        "telegram_bot_token" => {
            if clean_val.is_empty()
                || !clean_val.contains(':')
                || clean_val.chars().any(char::is_whitespace)
            {
                return Err(AegisError::Validation(
                    "telegram_bot_token must be a valid non-empty bot token containing ':'"
                        .to_string(),
                ));
            }
        }
        "telegram_chat_id" => {
            if !clean_val.is_empty()
                && (clean_val.len() > 64
                    || clean_val.chars().any(char::is_whitespace)
                    || clean_val.contains('<')
                    || clean_val.contains('>'))
            {
                return Err(AegisError::Validation(
                    "Invalid telegram_chat_id format".to_string(),
                ));
            }
        }
        "discord_webhook_url" => {
            if !clean_val.is_empty()
                && (!clean_val.starts_with("https://")
                    || clean_val.chars().any(char::is_whitespace))
            {
                return Err(AegisError::Validation(
                    "discord_webhook_url must start with https:// and contain no whitespace"
                        .to_string(),
                ));
            }
        }
        "gemini_api_key" => {
            if clean_val.is_empty() || clean_val.chars().any(char::is_whitespace) {
                return Err(AegisError::Validation(
                    "gemini_api_key must be non-empty without whitespace".to_string(),
                ));
            }
        }
        "target_leverage_multiplier" => {
            parse_f64_bounded(clean_key, clean_val, 0.10, 3.00)?;
        }
        "max_gross_leverage_x" => {
            parse_f64_bounded(clean_key, clean_val, 0.20, 5.00)?;
        }
        "max_single_asset_weight" => {
            parse_f64_bounded(clean_key, clean_val, 0.01, 1.00)?;
        }
        "min_rebalance_delta_weight" => {
            parse_f64_bounded(clean_key, clean_val, 0.001, 0.50)?;
        }
        "min_order_notional_usdc" => {
            parse_f64_bounded(clean_key, clean_val, 10.0, 100_000.0)?;
        }
        "initial_paper_equity_usdc" => {
            parse_f64_bounded(clean_key, clean_val, 100.0, 100_000_000.0)?;
        }
        "live_exchange_equity_usdc" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 100_000_000.0)?;
        }
        "risk_capital_cap_usdc" => {
            let v = parse_f64_bounded(clean_key, clean_val, 0.0, 100_000_000.0)?;
            if v > 0.0 && v < 10.0 {
                return Err(AegisError::Validation(
                    "risk_capital_cap_usdc must be 0 (disabled) or at least $10.00".to_string(),
                ));
            }
        }
        "core_min_day_volume_usdc" | "event_min_day_volume_usdc" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 10_000_000_000.0)?;
        }
        "testnet_core_min_day_volume_usdc"
        | "testnet_event_min_day_volume_usdc"
        | "testnet_core_min_open_interest_usdc"
        | "testnet_event_min_open_interest_usdc"
        | "core_min_open_interest_usdc"
        | "event_min_open_interest_usdc" => {
            parse_f64_bounded(clean_key, clean_val, 1_000.0, 100_000_000.0)?;
        }
        "s1_carry_mom_budget" | "s2_trend_breakout_budget" | "s3_event_reversion_budget" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 1.50)?;
        }
        "protective_stop_atr_mult" => {
            parse_f64_bounded(clean_key, clean_val, 0.5, 15.0)?;
        }
        "protective_stop_default_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.002, 0.50)?;
        }
        "take_profit_atr_mult" => {
            parse_f64_bounded(clean_key, clean_val, 0.5, 30.0)?;
        }
        "take_profit_default_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.002, 2.00)?;
        }
        "alo_chase_threshold_bps" => {
            parse_f64_bounded(clean_key, clean_val, 0.5, 500.0)?;
        }
        "max_alo_chase_drift_bps" => {
            parse_f64_bounded(clean_key, clean_val, 5.0, 300.0)?;
        }
        "max_alo_oracle_drift_bps" => {
            parse_f64_bounded(clean_key, clean_val, 20.0, 500.0)?;
        }
        "chase_reject_cooldown_sec" => {
            parse_i64_bounded(clean_key, clean_val, 30, 3_600)?;
        }
        "entry_ioc_max_slippage_bps" => {
            parse_f64_bounded(clean_key, clean_val, 5.0, 200.0)?;
        }
        "sl_trigger_limit_band_bps" => {
            parse_f64_bounded(clean_key, clean_val, 20.0, 350.0)?;
        }
        "passive_close_rest_sec" => {
            parse_i64_bounded(clean_key, clean_val, 0, 60)?;
        }
        "priority_unmonitored_alert_ticks" => {
            parse_i64_bounded(clean_key, clean_val, 3, 300)?;
        }
        "hl_reject_backoff_threshold" => {
            parse_i64_bounded(clean_key, clean_val, 1, 20)?;
        }
        "alo_expiry_backoff_threshold" => {
            parse_i64_bounded(clean_key, clean_val, 1, 10)?;
        }
        "trigger_oracle_fallback_basis_bps" => {
            parse_f64_bounded(clean_key, clean_val, 10.0, 500.0)?;
        }
        "emergency_ioc_breach_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.005, 0.05)?;
        }
        "max_entry_oracle_deviation_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.002, 0.05)?;
        }
        "min_entry_l2_depth_mult" => {
            parse_f64_bounded(clean_key, clean_val, 0.25, 10.0)?;
        }
        "min_entry_l2_depth_usdc" => {
            parse_f64_bounded(clean_key, clean_val, 25.0, 50_000.0)?;
        }
        "auto_deleverage_confirm_ticks" => {
            parse_i64_bounded(clean_key, clean_val, 1, 5)?;
        }
        "hl_rest_weight_limit_per_min" => {
            parse_i64_bounded(clean_key, clean_val, 300, 1_150)?;
        }
        "candle_refresh_max_assets" => {
            parse_i64_bounded(clean_key, clean_val, 5, 150)?;
        }
        "backtest_warmup_bars" => {
            parse_i64_bounded(clean_key, clean_val, 1, 500)?;
        }
        "breadth_bull_block_shorts_threshold" => {
            parse_f64_bounded(clean_key, clean_val, 0.55, 0.95)?;
        }
        "breadth_bear_block_longs_threshold" => {
            parse_f64_bounded(clean_key, clean_val, 0.05, 0.45)?;
        }
        "breakeven_trigger_r" => {
            parse_f64_bounded(clean_key, clean_val, 0.15, 1.50)?;
        }
        "breakeven_lock_r" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 0.50)?;
        }
        "stagnation_time_stop_hours" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 168.0)?;
        }
        "stagnation_min_mfe_r" => {
            parse_f64_bounded(clean_key, clean_val, 0.05, 2.0)?;
        }
        "hysteresis_s1_reversal_mom_threshold" => {
            parse_f64_bounded(clean_key, clean_val, 0.01, 0.50)?;
        }
        "hysteresis_s1_support_composite_threshold" => {
            parse_f64_bounded(clean_key, clean_val, 0.005, 0.25)?;
        }
        "early_loser_r0_mult" => {
            parse_f64_bounded(clean_key, clean_val, 0.05, 1.0)?;
        }
        "early_loser_max_loss_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.003, 0.10)?;
        }
        "risk_per_trade_equity_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 0.05)?;
        }
        "max_portfolio_heat_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.01, 1.0)?;
        }
        "max_entries_per_bar" => {
            parse_i64_bounded(clean_key, clean_val, 0, 100)?;
        }
        "chop_long_weight_mult" | "chop_short_weight_mult" | "neutral_long_weight_mult" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 1.0)?;
        }
        "signal_exit_confirm_bars" => {
            parse_i64_bounded(clean_key, clean_val, 1, 5)?;
        }
        "max_net_exposure_ratio_of_gross" => {
            parse_f64_bounded(clean_key, clean_val, 0.10, 1.0)?;
        }
        "maker_only_rest_ttl_ms" => {
            parse_i64_bounded(clean_key, clean_val, 5_000, 600_000)?;
        }
        "funding_entry_defer_min_rate_hourly" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 0.05)?;
        }
        "funding_entry_defer_window_sec" => {
            parse_i64_bounded(clean_key, clean_val, 0, 3_600)?;
        }
        "min_equity_halt_usdc" => {
            parse_f64_bounded(clean_key, clean_val, 1.0, 100_000.0)?;
        }
        "retention_snapshots_days"
        | "retention_telemetry_days"
        | "retention_order_audit_days"
        | "retention_cooldowns_days"
        | "retention_alert_queue_days" => {
            parse_i64_bounded(clean_key, clean_val, 1, 365)?;
        }
        "retention_ohlcv_bars_days" | "retention_funding_events_days" => {
            parse_i64_bounded(clean_key, clean_val, 30, 1_095)?;
        }
        "retention_prune_interval_sec" => {
            parse_i64_bounded(clean_key, clean_val, 300, 86_400)?;
        }
        "retention_prune_batch_size" => {
            parse_i64_bounded(clean_key, clean_val, 100, 5_000)?;
        }
        "maker_fee_bps" | "taker_fee_bps" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 100.0)?;
        }
        "max_entry_spread_bps" => {
            parse_f64_bounded(clean_key, clean_val, 1.0, 500.0)?;
        }
        "max_daily_drawdown_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.01, 0.95)?;
        }
        "max_open_positions" => {
            parse_i64_bounded(clean_key, clean_val, 1, 50)?;
        }
        "min_entry_interval_sec" | "min_position_hold_sec" => {
            parse_i64_bounded(clean_key, clean_val, 0, 86_400)?;
        }
        "scanner_interval_ms" => {
            parse_i64_bounded(clean_key, clean_val, 500, 60_000)?;
        }
        "priority_monitor_interval_ms" => {
            parse_i64_bounded(clean_key, clean_val, 250, 10_000)?;
        }
        "stop_out_cooldown_sec" => {
            parse_i64_bounded(clean_key, clean_val, 60, 604_800)?;
        }
        "max_alo_chase_ticks" => {
            parse_i64_bounded(clean_key, clean_val, 1, 50)?;
        }
        "alert_dedup_window_ms" => {
            parse_i64_bounded(clean_key, clean_val, 0, 86_400_000)?;
        }
        "alert_http_timeout_sec" => {
            parse_i64_bounded(clean_key, clean_val, 2, 60)?;
        }
        "telemetry_queue_capacity" => {
            parse_i64_bounded(clean_key, clean_val, 64, 65_536)?;
        }
        "web_server_port" => {
            parse_i64_bounded(clean_key, clean_val, 1024, 65535)?;
        }
        _ => {
            return Err(AegisError::Validation(format!(
                "Unknown or unsupported configuration key '{clean_key}'"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_symbol_normalization() {
        assert_eq!(
            normalize_hyperliquid_symbol("BTC").unwrap(),
            ("BTC-USDC".to_string(), "BTC".to_string())
        );
        assert_eq!(
            normalize_hyperliquid_symbol("SOL/USDC:USDC").unwrap(),
            ("SOL-USDC".to_string(), "SOL".to_string())
        );
        assert_eq!(
            normalize_hyperliquid_symbol("eth-usdc").unwrap(),
            ("ETH-USDC".to_string(), "ETH".to_string())
        );
        assert_eq!(
            normalize_hyperliquid_symbol("kPEPE/USDC:USDC").unwrap(),
            ("kPEPE-USDC".to_string(), "kPEPE".to_string())
        );
        assert_eq!(
            normalize_hyperliquid_symbol("kpepe").unwrap(),
            ("kPEPE-USDC".to_string(), "kPEPE".to_string())
        );
        assert_eq!(
            normalize_hyperliquid_symbol("KPEPE-USDC").unwrap(),
            ("kPEPE-USDC".to_string(), "kPEPE".to_string())
        );
        assert_eq!(
            normalize_hyperliquid_symbol("kbonk").unwrap(),
            ("kBONK-USDC".to_string(), "kBONK".to_string())
        );
        assert_eq!(
            normalize_hyperliquid_symbol("KSHIB").unwrap(),
            ("kSHIB-USDC".to_string(), "kSHIB".to_string())
        );
        assert_eq!(
            normalize_hyperliquid_symbol("kfloki").unwrap(),
            ("kFLOKI-USDC".to_string(), "kFLOKI".to_string())
        );
        assert_eq!(
            normalize_hyperliquid_symbol("klunc").unwrap(),
            ("kLUNC-USDC".to_string(), "kLUNC".to_string())
        );
        assert_eq!(
            normalize_hyperliquid_symbol("kneiro").unwrap(),
            ("kNEIRO-USDC".to_string(), "kNEIRO".to_string())
        );
        assert_eq!(
            normalize_hyperliquid_symbol("kdogs").unwrap(),
            ("kDOGS-USDC".to_string(), "kDOGS".to_string())
        );
        assert_eq!(
            normalize_hyperliquid_symbol("kaito").unwrap(),
            ("KAITO-USDC".to_string(), "KAITO".to_string())
        );
    }

    #[test]
    fn test_risk_tier_extreme_default_and_safe_parse_fallback() {
        let cfg = ConfigMap::new();
        assert_eq!(cfg.risk_tier(), RiskTier::Extreme);
        assert!((cfg.target_leverage_multiplier() - 1.85).abs() < 1e-9);

        assert_eq!(RiskTier::parse("unknown_tier"), RiskTier::Standard);
        assert_eq!(RiskTier::parse(""), RiskTier::Standard);

        let mut custom_cfg = ConfigMap::new();
        custom_cfg.insert("risk_tier", "custom");
        custom_cfg.insert("target_leverage_multiplier", " 1.25 ");
        assert!((custom_cfg.target_leverage_multiplier() - 1.25).abs() < 1e-9);
    }

    #[test]
    fn test_default_seeds_contain_cap_and_safe_bind() {
        let seeds = default_v2_system_config_seeds();
        let find = |k: &str| seeds.iter().find(|(key, ..)| *key == k).map(|s| s.1);

        // Tier-1 & R14-H1 updated seed defaults
        assert_eq!(find("risk_capital_cap_usdc"), Some("1000.0"));
        assert_eq!(find("risk_capital_cap_updated_at_ms"), Some("0"));
        assert_eq!(find("max_single_asset_weight"), Some("0.25"));
        assert_eq!(find("min_position_hold_sec"), Some("900"));
        assert_eq!(find("profit_gate_signal_exits"), Some("false"));
        assert_eq!(find("signal_exit_min_net_profit_pct"), Some("0.0025"));
        assert_eq!(find("max_entry_spread_bps"), Some("10.0"));
        assert_eq!(find("s3_event_reversion_budget"), Some("0.10"));
        assert_eq!(find("max_alo_chase_ticks"), Some("3"));
        assert_eq!(find("alo_chase_threshold_bps"), Some("8.0"));

        // New P0/P1/R14 canonical seeds
        assert_eq!(find("max_alo_chase_drift_bps"), Some("60.0"));
        assert_eq!(find("max_alo_oracle_drift_bps"), Some("150.0"));
        assert_eq!(find("chase_reject_cooldown_sec"), Some("300"));
        assert_eq!(find("entry_ioc_max_slippage_bps"), Some("25.0"));
        assert_eq!(find("sl_trigger_limit_band_bps"), Some("100.0"));
        assert_eq!(find("passive_close_rest_sec"), Some("8"));
        assert_eq!(find("priority_unmonitored_alert_ticks"), Some("10"));
        assert_eq!(find("hl_reject_backoff_threshold"), Some("3"));
        assert_eq!(find("alo_expiry_backoff_threshold"), Some("2"));
        assert_eq!(find("trigger_oracle_fallback_basis_bps"), Some("50.0"));
        assert_eq!(find("emergency_ioc_breach_pct"), Some("0.015"));
        assert_eq!(find("max_entry_oracle_deviation_pct"), Some("0.015"));
        assert_eq!(find("min_entry_l2_depth_mult"), Some("1.5"));
        assert_eq!(find("min_entry_l2_depth_usdc"), Some("150.0"));
        assert_eq!(find("testnet_core_min_day_volume_usdc"), Some("50000.0"));
        assert_eq!(find("testnet_event_min_day_volume_usdc"), Some("50000.0"));
        assert_eq!(find("testnet_core_min_open_interest_usdc"), Some("50000.0"));
        assert_eq!(
            find("testnet_event_min_open_interest_usdc"),
            Some("50000.0")
        );
        assert_eq!(find("core_min_open_interest_usdc"), Some("250000.0"));
        assert_eq!(find("event_min_open_interest_usdc"), Some("100000.0"));
        assert_eq!(find("auto_deleverage_confirm_ticks"), Some("2"));
        assert_eq!(find("hl_rest_weight_limit_per_min"), Some("900"));
        assert_eq!(find("candle_refresh_max_assets"), Some("30"));
        assert_eq!(find("backtest_warmup_bars"), Some("24"));
        assert_eq!(find("hl_websocket_enabled"), Some("true"));
        assert_eq!(find("enable_breadth_regime_gate"), Some("true"));
        assert_eq!(find("breadth_bull_block_shorts_threshold"), Some("0.68"));
        assert_eq!(find("breadth_bear_block_longs_threshold"), Some("0.32"));
        assert_eq!(find("breakeven_trigger_r"), Some("0.40"));
        assert_eq!(find("breakeven_lock_r"), Some("0.05"));
        assert_eq!(find("stagnation_time_stop_hours"), Some("12.0"));
        assert_eq!(find("stagnation_min_mfe_r"), Some("0.35"));
        assert_eq!(find("early_loser_invalidation_enabled"), Some("true"));
        assert_eq!(find("early_loser_r0_mult"), Some("0.25"));
        assert_eq!(find("early_loser_max_loss_pct"), Some("0.009"));
        assert_eq!(find("hysteresis_s1_reversal_mom_threshold"), Some("0.08"));
        assert_eq!(
            find("hysteresis_s1_support_composite_threshold"),
            Some("0.025")
        );
        assert_eq!(find("risk_per_trade_equity_pct"), Some("0.02"));
        assert_eq!(find("max_portfolio_heat_pct"), Some("0.12"));
        assert_eq!(find("signal_timeframe"), Some("4h"));
        assert_eq!(find("wall_clock_normalize_indicators"), Some("true"));
        assert_eq!(find("s3_strict_admission_enabled"), Some("true"));
        assert_eq!(find("enable_scale_in_out_rebalance"), Some("true"));
        assert_eq!(find("enforce_slot_spread_weight_cap"), Some("false"));
        assert_eq!(find("max_entries_per_bar"), Some("0"));
        assert_eq!(find("enable_regime_budget_multipliers"), Some("false"));
        assert_eq!(find("chop_long_weight_mult"), Some("0.50"));
        assert_eq!(find("chop_short_weight_mult"), Some("0.65"));
        assert_eq!(find("neutral_long_weight_mult"), Some("0.65"));
        assert_eq!(find("signal_exit_confirm_bars"), Some("1"));
        assert_eq!(find("suppress_direct_flips"), Some("false"));
        assert_eq!(find("max_net_exposure_ratio_of_gross"), Some("1.0"));
        assert_eq!(find("maker_first_low_urgency_entries"), Some("false"));
        assert_eq!(find("maker_only_rest_ttl_ms"), Some("60000"));
        assert_eq!(find("funding_entry_timing_enabled"), Some("false"));
        assert_eq!(find("funding_entry_defer_min_rate_hourly"), Some("0.0001"));
        assert_eq!(find("funding_entry_defer_window_sec"), Some("900"));
        assert_eq!(find("min_equity_halt_usdc"), Some("50.0"));
        assert_eq!(find("retention_snapshots_days"), Some("30"));
        assert_eq!(find("retention_telemetry_days"), Some("14"));
        assert_eq!(find("retention_order_audit_days"), Some("30"));
        assert_eq!(find("retention_cooldowns_days"), Some("14"));
        assert_eq!(find("retention_alert_queue_days"), Some("30"));
        assert_eq!(find("retention_ohlcv_bars_days"), Some("180"));
        assert_eq!(find("retention_funding_events_days"), Some("365"));
        assert_eq!(find("retention_prune_interval_sec"), Some("3600"));
        assert_eq!(find("retention_prune_batch_size"), Some("1000"));
        assert_eq!(find("web_trusted_proxies"), Some(""));
        assert_eq!(find("priority_monitor_health_json"), Some(""));
        assert_eq!(find("ws_stream_status_json"), Some(""));
        assert_eq!(find("live_exchange_maker_fee_bps"), Some("0.0"));
        assert_eq!(find("live_exchange_taker_fee_bps"), Some("0.0"));

        // Preserved seeds
        assert_eq!(find("core_min_day_volume_usdc"), Some("2000000.0"));
        assert_eq!(find("event_min_day_volume_usdc"), Some("500000.0"));
        assert_eq!(find("max_daily_drawdown_pct"), Some("0.15"));
        assert_eq!(find("web_allow_remote_bind"), Some("false"));
        assert_eq!(find("stop_out_cooldown_sec"), Some("1800"));
        assert_eq!(find("take_profit_atr_mult"), Some("2.8"));
        assert_eq!(find("take_profit_default_pct"), Some("0.055"));
        assert_eq!(find("live_vault_info_json"), Some(""));
        assert_eq!(find("hyperliquid_vault_address"), Some(""));
        assert_eq!(find("vault_role"), Some("follower"));
        assert_eq!(find("vault_target_allocation_pct"), Some("0.0"));
        assert_eq!(find("vault_profit_sweep_threshold_usdc"), Some("0.0"));
        assert_eq!(find("vault_notes"), Some(""));
        assert_eq!(find("live_exchange_equity_synced_at_ms"), Some("0"));
        assert_eq!(find("live_exchange_margin_used_usdc"), Some("0.0"));
        assert_eq!(find("live_exchange_withdrawable_usdc"), Some("0.0"));
        assert_eq!(find("live_exchange_open_orders_count"), Some("0"));
        assert_eq!(find("live_exchange_spot_usdc"), Some("0.0"));
        assert_eq!(find("live_exchange_spot_synced_at_ms"), Some("0"));
        assert_eq!(find("live_exchange_account_mode"), Some("default"));
        assert_eq!(find("live_exchange_perp_margin_equity_usdc"), Some("0.0"));
        assert_eq!(find("live_exchange_portfolio_pnl_json"), Some(""));

        // Ensure zero duplicate keys and every non-immutable, non-empty-secret seed validates
        let mut seen = std::collections::HashSet::new();
        for (k, v, _, _) in &seeds {
            assert!(seen.insert(*k), "duplicate seed key {k}");
            if is_immutable_security_config_key(k) {
                assert!(matches!(
                    validate_core_config_key_value(k, v),
                    Err(AegisError::SecurityViolation(_))
                ));
            } else if !matches!(*k, "private_key" | "telegram_bot_token") {
                assert!(
                    validate_core_config_key_value(k, v).is_ok(),
                    "seed {k}={v} failed validation"
                );
            }
        }
    }

    #[test]
    fn test_validate_core_config_key_value_bounds_and_security() {
        assert!(validate_core_config_key_value("max_alo_chase_drift_bps", "60.0").is_ok());
        assert!(validate_core_config_key_value("max_alo_chase_drift_bps", "301.0").is_err());
        assert!(validate_core_config_key_value("risk_per_trade_equity_pct", "0.0").is_ok());
        assert!(validate_core_config_key_value("risk_per_trade_equity_pct", "0.06").is_err());
        assert!(validate_core_config_key_value("retention_ohlcv_bars_days", "29").is_err());
        assert!(validate_core_config_key_value("retention_ohlcv_bars_days", "180").is_ok());
        assert!(validate_core_config_key_value("backtest_warmup_bars", "0").is_err());
        assert!(validate_core_config_key_value("backtest_warmup_bars", "1").is_ok());
        assert!(validate_core_config_key_value("backtest_warmup_bars", "500").is_ok());
        assert!(validate_core_config_key_value("backtest_warmup_bars", "501").is_err());
        assert!(validate_core_config_key_value("stagnation_time_stop_hours", "168.0").is_ok());
        assert!(validate_core_config_key_value("stagnation_time_stop_hours", "168.1").is_err());
        assert!(validate_core_config_key_value("hl_websocket_enabled", "maybe").is_err());
        assert!(matches!(
            validate_core_config_key_value("schema_migration_version", "2.2.0"),
            Err(AegisError::SecurityViolation(_))
        ));

        for tf in ["1m", "5m", "15m", "1h", "4h", "1d"] {
            assert!(validate_core_config_key_value("signal_timeframe", tf).is_ok());
        }
        assert!(validate_core_config_key_value("signal_timeframe", "30m").is_err());
        assert!(validate_core_config_key_value("max_portfolio_heat_pct", "0.01").is_ok());
        assert!(validate_core_config_key_value("max_portfolio_heat_pct", "1.0").is_ok());
        assert!(validate_core_config_key_value("max_portfolio_heat_pct", "0.009").is_err());
        assert!(validate_core_config_key_value("max_portfolio_heat_pct", "1.01").is_err());
        assert!(validate_core_config_key_value("max_entries_per_bar", "0").is_ok());
        assert!(validate_core_config_key_value("max_entries_per_bar", "100").is_ok());
        assert!(validate_core_config_key_value("max_entries_per_bar", "101").is_err());
        assert!(validate_core_config_key_value("chop_long_weight_mult", "0.0").is_ok());
        assert!(validate_core_config_key_value("chop_long_weight_mult", "1.01").is_err());
        assert!(validate_core_config_key_value("signal_exit_confirm_bars", "1").is_ok());
        assert!(validate_core_config_key_value("signal_exit_confirm_bars", "5").is_ok());
        assert!(validate_core_config_key_value("signal_exit_confirm_bars", "0").is_err());
        assert!(validate_core_config_key_value("signal_exit_confirm_bars", "6").is_err());
        assert!(validate_core_config_key_value("max_net_exposure_ratio_of_gross", "0.10").is_ok());
        assert!(validate_core_config_key_value("max_net_exposure_ratio_of_gross", "0.09").is_err());
        assert!(validate_core_config_key_value("maker_only_rest_ttl_ms", "5000").is_ok());
        assert!(validate_core_config_key_value("maker_only_rest_ttl_ms", "600000").is_ok());
        assert!(validate_core_config_key_value("maker_only_rest_ttl_ms", "4999").is_err());
        assert!(validate_core_config_key_value("maker_only_rest_ttl_ms", "600001").is_err());
        assert!(
            validate_core_config_key_value("funding_entry_defer_min_rate_hourly", "0.05").is_ok()
        );
        assert!(
            validate_core_config_key_value("funding_entry_defer_min_rate_hourly", "0.051").is_err()
        );
        assert!(validate_core_config_key_value("funding_entry_defer_window_sec", "3600").is_ok());
        assert!(validate_core_config_key_value("funding_entry_defer_window_sec", "3601").is_err());
        assert!(validate_core_config_key_value("early_loser_r0_mult", "0.05").is_ok());
        assert!(validate_core_config_key_value("early_loser_r0_mult", "1.0").is_ok());
        assert!(validate_core_config_key_value("early_loser_r0_mult", "0.049").is_err());
        assert!(validate_core_config_key_value("early_loser_r0_mult", "1.01").is_err());
        assert!(validate_core_config_key_value("early_loser_max_loss_pct", "0.003").is_ok());
        assert!(validate_core_config_key_value("early_loser_max_loss_pct", "0.10").is_ok());
        assert!(validate_core_config_key_value("early_loser_max_loss_pct", "0.0029").is_err());
        assert!(validate_core_config_key_value("early_loser_max_loss_pct", "0.101").is_err());
    }

    #[test]
    fn test_risk_tier_scaled_risk_budget_and_weight_defaults() {
        assert!(
            (RiskTier::Conservative.default_risk_per_trade_equity_pct(None) - 0.005).abs() < 1e-9
        );
        assert!((RiskTier::Standard.default_risk_per_trade_equity_pct(None) - 0.010).abs() < 1e-9);
        assert!(
            (RiskTier::Aggressive.default_risk_per_trade_equity_pct(None) - 0.015).abs() < 1e-9
        );
        assert!((RiskTier::Extreme.default_risk_per_trade_equity_pct(None) - 0.020).abs() < 1e-9);
        assert!(
            (RiskTier::Custom.default_risk_per_trade_equity_pct(Some(1.5)) - 0.015).abs() < 1e-9
        );
        assert!(
            (RiskTier::Custom.default_risk_per_trade_equity_pct(Some(0.1)) - 0.0025).abs() < 1e-9
        );
        assert!(
            (RiskTier::Custom.default_risk_per_trade_equity_pct(Some(10.0)) - 0.05).abs() < 1e-9
        );
        assert!(
            (RiskTier::Custom.default_risk_per_trade_equity_pct(Some(f64::NAN)) - 0.010).abs()
                < 1e-9
        );

        assert!((RiskTier::Conservative.default_max_single_asset_weight(None) - 0.15).abs() < 1e-9);
        assert!((RiskTier::Standard.default_max_single_asset_weight(None) - 0.15).abs() < 1e-9);
        assert!((RiskTier::Aggressive.default_max_single_asset_weight(None) - 0.20).abs() < 1e-9);
        assert!((RiskTier::Extreme.default_max_single_asset_weight(None) - 0.25).abs() < 1e-9);
        assert!((RiskTier::Custom.default_max_single_asset_weight(Some(0.5)) - 0.15).abs() < 1e-9);
        assert!((RiskTier::Custom.default_max_single_asset_weight(Some(1.5)) - 0.225).abs() < 1e-9);
        assert!((RiskTier::Custom.default_max_single_asset_weight(Some(3.0)) - 0.35).abs() < 1e-9);
        assert!(
            (RiskTier::Custom.default_max_single_asset_weight(Some(f64::NAN)) - 0.15).abs() < 1e-9
        );
    }

    #[test]
    fn test_is_unified_account_mode() {
        assert!(is_unified_account_mode("unifiedAccount"));
        assert!(is_unified_account_mode(" unifiedaccount "));
        assert!(is_unified_account_mode("portfolioMargin"));
        assert!(is_unified_account_mode("PORTFOLIOMARGIN"));
        assert!(!is_unified_account_mode("default"));
        assert!(!is_unified_account_mode("standard"));
        assert!(!is_unified_account_mode(""));
    }

    #[test]
    fn test_config_map_case_insensitive_normalization() {
        let mut raw = HashMap::new();
        raw.insert(" RISK_TIER ".to_string(), "custom".to_string());
        raw.insert("TARGET_LEVERAGE_MULTIPLIER".to_string(), "1.45".to_string());
        raw.insert("MAX_OPEN_POSITIONS".to_string(), "4".to_string());
        raw.insert("WEB_AUTH_ENABLED".to_string(), "true".to_string());
        let mut cfg = ConfigMap::from_map(raw);
        cfg.insert(" Risk_Capital_Cap_Usdc ", "2500.5");
        cfg.insert("MIN_ENTRY_INTERVAL_SEC", "90");

        assert_eq!(cfg.get("risk_tier").map(String::as_str), Some("custom"));
        assert_eq!(cfg.get("RISK_TIER").map(String::as_str), Some("custom"));
        assert_eq!(cfg.get_str("RISK_TIER", ""), "custom");
        assert!((cfg.target_leverage_multiplier() - 1.45).abs() < 1e-9);
        assert!((cfg.get_f64("RISK_CAPITAL_CAP_USDC", 0.0) - 2500.5).abs() < 1e-9);
        assert_eq!(cfg.get_i64("min_entry_interval_sec", 0), 90);
        assert_eq!(cfg.get_usize("max_open_positions", 0), 4);
        assert!(cfg.get_bool("web_auth_enabled", false));
        assert!(cfg.get_bool("WEB_AUTH_ENABLED", false));
    }

    #[test]
    fn test_config_map_from_defaults() {
        let cfg = ConfigMap::from_defaults();
        assert_eq!(cfg.risk_tier(), RiskTier::Extreme);
        assert!((cfg.get_f64("core_min_day_volume_usdc", 0.0) - 2_000_000.0).abs() < 1e-6);
    }
}
