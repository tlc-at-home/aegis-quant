//! Canonical v2.0 configuration domain types, risk tiers, symbol normalization, and seed defaults (`aegis-core::config`).

use crate::errors::{AegisError, Result};
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
        Self(map)
    }

    pub fn insert(&mut self, key: impl Into<String>, val: impl Into<String>) {
        self.0.insert(key.into(), val.into());
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.get(key)
    }

    pub fn get_str(&self, key: &str, default: &str) -> String {
        self.0
            .get(key)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| default.to_string())
    }

    pub fn get_f64(&self, key: &str, default: f64) -> f64 {
        self.0
            .get(key)
            .and_then(|s| s.trim().parse::<f64>().ok())
            .filter(|v| v.is_finite())
            .unwrap_or(default)
    }

    pub fn get_i64(&self, key: &str, default: i64) -> i64 {
        self.0
            .get(key)
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(default)
    }

    pub fn get_usize(&self, key: &str, default: usize) -> usize {
        self.0
            .get(key)
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(default)
    }

    pub fn get_bool(&self, key: &str, default: bool) -> bool {
        self.0
            .get(key)
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
            .0
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
        ("max_single_asset_weight", "0.30", "float", "Maximum single-asset portfolio weight cap (30% of equity)"),
        ("min_rebalance_delta_weight", "0.025", "float", "Minimum weight delta (2.5% of equity) required to submit a rebalance order"),
        ("min_order_notional_usdc", "12.0", "float", "Hyperliquid minimum order notional floor in USDC ($12.00)"),
        ("initial_paper_equity_usdc", "10000.0", "float", "Default starting equity in USDC for PAPER mode or empty account fallback"),
        ("live_exchange_equity_usdc", "0.0", "float", "Live Hyperliquid perp accountValue in USDC continuously synced by reconciler"),
        ("risk_capital_cap_usdc", "1000.0", "float", "Optional user portfolio capital sizing cap in USDC (0 disables cap)"),
        ("risk_capital_cap_updated_at_ms", "1795035600000", "int", "Cutoff timestamp (ms, 2026-11-18T21:00:00Z) below which pre-cap legacy positions/orders are ignored by active cap enforcement"),
        ("min_position_hold_sec", "300", "int", "Minimum hold duration in seconds before auto-deleverage may target a newly filled position"),
        ("max_entry_spread_bps", "20.0", "float", "Maximum allowed L2 bid-ask spread in basis points for new WORKING orders (20.0 bps default)"),
        ("max_daily_drawdown_pct", "0.15", "float", "Maximum 24h peak-to-trough equity drawdown fraction before halting new entries"),
        ("scanner_interval_ms", "3000", "int", "Full-exchange 150+ perp scanner cadence in milliseconds (3s)"),
        ("priority_monitor_interval_ms", "1000", "int", "High-frequency priority watchlist & active order monitor cadence in milliseconds (1s)"),
        ("core_min_day_volume_usdc", "2000000.0", "float", "Minimum 24h USDC volume on Mainnet ($2M; $5k on Testnet) for Core S1/S2 eligibility"),
        ("event_min_day_volume_usdc", "500000.0", "float", "Minimum 24h USDC volume on Mainnet ($500k; $2.5k on Testnet) for Event S3 eligibility"),
        ("s1_carry_mom_budget", "0.45", "float", "Sleeve 1 (Cross-Sectional Carry-Momentum & Beta Hedge) base portfolio weight budget"),
        ("s2_trend_breakout_budget", "0.40", "float", "Sleeve 2 (Breadth-Confirmed Multi-Speed Trend & Live 3s Breakout) base portfolio weight budget"),
        ("s3_event_reversion_budget", "0.25", "float", "Sleeve 3 (Live Capitulation Flush & Funding Dislocation) base portfolio weight budget"),
        ("protective_stop_atr_mult", "2.2", "float", "Protective trailing stop distance multiplier in ATR units"),
        ("protective_stop_default_pct", "0.04", "float", "Default protective stop distance fraction of entry price (4.0%)"),
        ("take_profit_atr_mult", "2.8", "float", "Take-profit target distance multiplier in ATR units (~1.27x reward-to-risk vs 2.2x ATR stop)"),
        ("take_profit_default_pct", "0.055", "float", "Default take-profit target distance fraction of entry price (5.5%)"),
        ("stop_out_cooldown_sec", "1800", "int", "Post-stop-out asset cooldown duration in seconds (30 minutes)"),
        ("max_alo_chase_ticks", "5", "int", "Maximum 1-second ALO maker limit chase attempts before IOC crossing or cancel"),
        ("alo_chase_threshold_bps", "4.0", "float", "Mid-price drift threshold in basis points triggering a 1s ALO limit price chase"),
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
        ("enforce_master_key", "true", "bool", "Require external or keyfile Fernet master key"),
    ]
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
        assert_eq!(find("risk_capital_cap_usdc"), Some("1000.0"));
        assert_eq!(
            find("risk_capital_cap_updated_at_ms"),
            Some("1795035600000")
        );
        assert_eq!(find("min_position_hold_sec"), Some("300"));
        assert_eq!(find("max_entry_spread_bps"), Some("20.0"));
        assert_eq!(find("core_min_day_volume_usdc"), Some("2000000.0"));
        assert_eq!(find("event_min_day_volume_usdc"), Some("500000.0"));
        assert_eq!(find("max_daily_drawdown_pct"), Some("0.15"));
        assert_eq!(find("web_allow_remote_bind"), Some("false"));
        assert_eq!(find("stop_out_cooldown_sec"), Some("1800"));
        assert_eq!(find("take_profit_atr_mult"), Some("2.8"));
        assert_eq!(find("take_profit_default_pct"), Some("0.055"));
    }
}
