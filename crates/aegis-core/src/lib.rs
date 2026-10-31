//! Core domain models, state machine enums, EVM/secp256k1 validation, and SHA-256 dataset provenance.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const PLATFORM_NAME: &str = "Aegis Quant";
pub const PLATFORM_VERSION: &str = "0.2.0-rc2";

pub const MAINNET_HLP_VAULT_ADDRESS: &str = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303";
pub const TESTNET_HLP_VAULT_ADDRESS: &str = "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0";

pub const SENSITIVE_CONFIG_KEYS: &[&str] = &[
    "private_key",
    "gemini_api_key",
    "telegram_bot_token",
    "discord_webhook_url",
];

pub const VALID_FSM_STATES: &[&str] = &[
    "PENDING",
    "OPEN_ON_BOOK",
    "PARTIALLY_FILLED",
    "FILLED",
    "CANCELLED",
    "REJECTED",
    "EXPIRED",
];

/// Baseline reference prices (USDC) for deterministic sandbox / synthetic fallback series.
pub fn base_prices_usdc() -> HashMap<&'static str, f64> {
    HashMap::from([
        ("BTC/USDC:USDC", 68500.0),
        ("ETH/USDC:USDC", 2650.0),
        ("SOL/USDC:USDC", 175.0),
        ("HYPE/USDC:USDC", 24.5),
        ("XRP/USDC:USDC", 0.56),
        ("DOGE/USDC:USDC", 0.145),
        ("ADA/USDC:USDC", 0.38),
        ("AVAX/USDC:USDC", 28.5),
        ("LINK/USDC:USDC", 12.4),
        ("SUI/USDC:USDC", 2.15),
        ("DOT/USDC:USDC", 4.35),
        ("NEAR/USDC:USDC", 4.85),
        ("APT/USDC:USDC", 9.80),
        ("ARB/USDC:USDC", 0.58),
        ("OP/USDC:USDC", 1.65),
        ("INJ/USDC:USDC", 21.2),
    ])
}

/// Timeframe duration in seconds.
pub fn timeframe_seconds(tf: &str) -> i64 {
    match tf.trim().to_ascii_lowercase().as_str() {
        "15m" => 900,
        "1h" => 3600,
        "2h" => 7200,
        "4h" => 14400,
        "6h" => 21600,
        "8h" => 28800,
        "12h" => 43200,
        "1d" => 86400,
        _ => 3600,
    }
}

#[derive(Debug, Error)]
pub enum AegisError {
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Security violation: {0}")]
    SecurityViolation(String),
    #[error("Cryptography error: {0}")]
    Crypto(String),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Data provenance error: {0}")]
    Provenance(String),
    #[error("Execution error: {0}")]
    Execution(String),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, AegisError>;

/// 3-State Gaussian HMM Regime (`BULL`, `BEAR`, `CHOP`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum RegimeType {
    Bull,
    Bear,
    Chop,
}

impl RegimeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bull => "BULL",
            Self::Bear => "BEAR",
            Self::Chop => "CHOP",
        }
    }

    pub fn from_str_lossy(s: &str) -> Self {
        match s.trim().to_ascii_uppercase().as_str() {
            "BULL" => Self::Bull,
            "BEAR" => Self::Bear,
            _ => Self::Chop,
        }
    }
}

impl fmt::Display for RegimeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Execution routing mode (`PAPER`, `LIVE`, `TESTNET`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ExecutionMode {
    Paper,
    Live,
    Testnet,
}

impl ExecutionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Paper => "PAPER",
            Self::Live => "LIVE",
            Self::Testnet => "TESTNET",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "PAPER" => Ok(Self::Paper),
            "LIVE" => Ok(Self::Live),
            "TESTNET" => Ok(Self::Testnet),
            other => Err(AegisError::Validation(format!(
                "Invalid execution_mode '{other}': must be PAPER, LIVE, or TESTNET."
            ))),
        }
    }

    pub fn is_live_or_testnet(&self) -> bool {
        matches!(self, Self::Live | Self::Testnet)
    }
}

impl fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 7-state Order Finite State Machine (`MAKER_LIMIT_CHASER`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderFsmState {
    Pending,
    OpenOnBook,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Expired,
}

impl OrderFsmState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::OpenOnBook => "OPEN_ON_BOOK",
            Self::PartiallyFilled => "PARTIALLY_FILLED",
            Self::Filled => "FILLED",
            Self::Cancelled => "CANCELLED",
            Self::Rejected => "REJECTED",
            Self::Expired => "EXPIRED",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "PENDING" => Ok(Self::Pending),
            "OPEN_ON_BOOK" => Ok(Self::OpenOnBook),
            "PARTIALLY_FILLED" => Ok(Self::PartiallyFilled),
            "FILLED" => Ok(Self::Filled),
            "CANCELLED" => Ok(Self::Cancelled),
            "REJECTED" => Ok(Self::Rejected),
            "EXPIRED" => Ok(Self::Expired),
            other => Err(AegisError::Validation(format!(
                "Invalid fsm_state '{other}': must be one of {VALID_FSM_STATES:?}"
            ))),
        }
    }
}

impl fmt::Display for OrderFsmState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Decoded 3-State Gaussian HMM posterior regime state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeState {
    pub regime: String,
    pub prob_bull: f64,
    pub prob_bear: f64,
    pub prob_chop: f64,
    pub window_bars: usize,
}

impl Default for RegimeState {
    fn default() -> Self {
        Self {
            regime: "CHOP".to_string(),
            prob_bull: 0.3333,
            prob_bear: 0.3333,
            prob_chop: 0.3334,
            window_bars: 0,
        }
    }
}

/// Historical or live OHLCV bar with hourly funding rate and synthetic flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OhlcvBar {
    pub symbol: String,
    pub timeframe: String,
    pub timestamp_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub funding_rate_hourly: f64,
    pub is_synthetic: i64,
}

/// Universe asset row (`universe_assets`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniverseAsset {
    pub asset_id: i64,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub instrument_type: String,
    pub tier: i64,
    pub tier_name: String,
    pub exec_timeframe: String,
    pub rs_timeframe: String,
    pub tick_size: f64,
    pub lot_step_size: f64,
    pub min_notional_usdc: f64,
    pub max_leverage: f64,
    pub is_active: i64,
    pub updated_at: String,
}

/// Paired execution (`USDC_PERP`) & relative-strength (`BTC_SYNTH`) row (`v_universe_alt_pairs`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AltPairMetadata {
    pub base_asset: String,
    pub tier: i64,
    pub tier_name: String,
    pub exec_symbol: String,
    pub synth_symbol: String,
    pub exec_timeframe: String,
    pub rs_timeframe: String,
    pub tick_size: f64,
    pub lot_step_size: f64,
    pub min_notional_usdc: f64,
    pub max_leverage: f64,
}

/// Cross-sectional signal evaluation record (`signal_evaluations`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalEvaluation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eval_id: Option<i64>,
    pub timestamp_ms: i64,
    pub base_asset: String,
    pub exec_symbol: String,
    pub synth_symbol: String,
    pub hmm_regime: String,
    pub prob_bull: f64,
    pub prob_bear: f64,
    pub prob_chop: f64,
    pub snr_score: f64,
    #[serde(default)]
    pub snr_synth: f64,
    #[serde(default)]
    pub rs_96: f64,
    #[serde(default)]
    pub r_usdc_8: f64,
    pub mace_score: f64,
    pub blended_conviction: f64,
    pub rs_rank: i64,
    pub signal_action: String,
    pub funding_rate_hourly: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

/// Validated position sizing and initial Chandelier stop allocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSizeResult {
    pub approved: bool,
    pub reason: String,
    pub symbol: String,
    pub side: String,
    pub entry_price: f64,
    pub quantity: f64,
    pub notional_usdc: f64,
    pub risk_budget_usdc: f64,
    pub conviction_multiplier: f64,
    pub initial_stop_price: f64,
    pub atr_value: f64,
}

/// Result of Two-Tier Chandelier Stop evaluation (catastrophic wick vs `bar_close` ratchet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChandelierStopEvaluation {
    pub triggered: bool,
    pub is_wick_stop: bool,
    pub exit_reason: Option<String>,
    pub exit_fill_price: f64,
    pub exit_slippage_usdc: f64,
    pub chandelier_stop_price: f64,
    pub highest_price_seen: f64,
    pub lowest_price_seen: f64,
    pub effective_atr: f64,
}

/// Result of executing an order through the `MAKER_LIMIT_CHASER` FSM or Paper Fill Model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderExecutionResult {
    pub client_order_id: String,
    pub exchange_order_id: String,
    pub symbol: String,
    pub side: String,
    pub fsm_state: String,
    pub requested_units: f64,
    pub filled_units: f64,
    pub avg_fill_price: f64,
    pub slippage_paid_usd: f64,
    pub effective_slippage_bps: f64,
    pub fees_paid_usdc: f64,
    pub is_maker: bool,
    pub fill_type: String,
    pub state_history: Vec<String>,
}

/// Normalized trade record (`trades` table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    #[serde(default)]
    pub trade_id: i64,
    pub client_order_id: String,
    pub symbol: String,
    pub base_asset: String,
    pub side: String,
    pub status: String,
    pub fsm_state: String,
    pub entry_price: f64,
    pub current_price: f64,
    pub exit_price: Option<f64>,
    pub quantity: f64,
    pub filled_units: f64,
    pub avg_fill_price: Option<f64>,
    pub notional_usdc: f64,
    pub risk_budget_usdc: f64,
    pub conviction_multiplier: f64,
    pub initial_stop_price: f64,
    pub chandelier_stop_price: f64,
    pub current_stop_price: f64,
    pub highest_price_seen: f64,
    pub lowest_price_seen: f64,
    pub realized_pnl_usdc: f64,
    pub unrealized_pnl_usdc: f64,
    pub fees_paid_usdc: f64,
    pub funding_accrued_usdc: f64,
    pub slippage_paid_usd: f64,
    pub exchange_order_id: Option<String>,
    pub opened_at: String,
    pub closed_at: Option<String>,
    pub exit_reason: Option<String>,
}

/// Point-in-time portfolio equity and risk telemetry snapshot (`portfolio_snapshots`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    pub timestamp_ms: i64,
    pub total_equity_usdc: f64,
    pub available_cash_usdc: f64,
    pub deployed_notional_usdc: f64,
    pub gross_leverage: f64,
    pub portfolio_heat_usdc: f64,
    pub portfolio_heat_pct: f64,
    pub basis_harvest_notional_usdc: f64,
    pub accrued_yield_usdc: f64,
    pub hmm_regime: String,
    pub prob_bull: f64,
    pub prob_bear: f64,
    pub prob_chop: f64,
    pub open_long_count: i64,
    pub open_short_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

/// Comprehensive point-in-time backtest tear sheet and acceptance gate verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestTearSheet {
    pub start_date: String,
    pub end_date: String,
    pub duration_days: f64,
    pub bars_replayed: usize,
    pub initial_equity_usdc: f64,
    pub final_equity_usdc: f64,
    pub total_return_pct: f64,
    pub annualized_return_pct: f64,
    pub annualized_sharpe: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
    pub max_drawdown_pct: f64,
    pub profit_factor: f64,
    pub trade_profit_factor: f64,
    pub win_rate_pct: f64,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub gross_profit_usdc: f64,
    pub gross_loss_usdc: f64,
    pub total_fees_usdc: f64,
    pub total_slippage_usdc: f64,
    pub wick_stop_outs: usize,
    pub fee_drag_pct: f64,
    pub funding_net_usdc: f64,
    pub basis_yield_usdc: f64,
    pub bull_regime_pnl_usdc: f64,
    pub bear_regime_pnl_usdc: f64,
    pub chop_regime_pnl_usdc: f64,
    pub bull_bars_count: usize,
    pub bear_bars_count: usize,
    pub chop_bars_count: usize,
    pub gate_sharpe_pass: bool,
    pub gate_mdd_pass: bool,
    pub gate_profit_factor_pass: bool,
    pub all_gates_passed: bool,
}

impl BacktestTearSheet {
    pub fn sharpe(&self) -> f64 {
        self.annualized_sharpe
    }

    pub fn max_drawdown(&self) -> f64 {
        ((self.max_drawdown_pct / 100.0) * 10000.0).round() / 10000.0
    }
}

/// Single Mainnet Go-Live readiness check evaluation outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub gate: String,
    pub check_id: String,
    pub name: String,
    pub status: String,
    pub observed: String,
    pub requirement: String,
    pub remediation: String,
}

/// Locate the project root containing `sql/schema_and_seed.sql`.
pub fn resolve_project_root() -> PathBuf {
    if let Ok(env_root) = std::env::var("AEGIS_PROJECT_ROOT") {
        let p = PathBuf::from(env_root);
        if p.exists() {
            return p;
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        for ancestor in cwd.ancestors() {
            if ancestor.join("sql").join("schema_and_seed.sql").exists() {
                return ancestor.to_path_buf();
            }
        }
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Validate an EVM/Arbitrum `0x` address (or allow empty string to clear).
pub fn validate_evm_address(value: &str, field_name: &str) -> Result<String> {
    let cleaned = value.trim();
    if cleaned.is_empty() {
        return Ok(String::new());
    }
    let has_prefix = cleaned.starts_with("0x") || cleaned.starts_with("0X");
    if !has_prefix || cleaned.len() != 42 || !cleaned[2..].chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(AegisError::Validation(format!(
            "Invalid {field_name}: must be a 42-character 0x-prefixed hex EVM address."
        )));
    }
    Ok(format!("0x{}", &cleaned[2..]))
}

/// Validate and normalize a 32-byte hex Trade-Only Agent Wallet secp256k1 private key.
pub fn validate_private_key(value: &str) -> Result<String> {
    let cleaned = value.trim().trim_matches(|c| c == '\'' || c == '"');
    if cleaned.is_empty() {
        return Ok(String::new());
    }
    let has_prefix = cleaned.starts_with("0x") || cleaned.starts_with("0X");
    if has_prefix && cleaned.len() == 42 && cleaned[2..].chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(AegisError::Validation(
            "Invalid private_key: must be a 64-hex-character (32-byte) private key (you entered a 42-character 0x wallet address — please paste the 64-hex Agent Private Key, not the Wallet Address).".to_string(),
        ));
    }
    let raw_hex = if has_prefix { &cleaned[2..] } else { cleaned };
    if raw_hex.len() != 64 || !raw_hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(AegisError::Validation(format!(
            "Invalid private_key: must be a 64-hex-character (32-byte) private key (received {} characters after 0x prefix).",
            raw_hex.len()
        )));
    }
    let bytes = hex::decode(raw_hex).map_err(|e| {
        AegisError::Validation(format!("Invalid private_key hex encoding: {e}"))
    })?;
    // Verify scalar is within secp256k1 range [1, n-1]
    if k256::SecretKey::from_slice(&bytes).is_err() {
        return Err(AegisError::Validation(
            "Invalid private_key: scalar must be within secp256k1 range [1, n-1].".to_string(),
        ));
    }
    Ok(format!("0x{}", raw_hex.to_ascii_lowercase()))
}

/// Mask sensitive private keys and API keys for safe telemetry output.
pub fn mask_secret(raw_val: &str) -> String {
    let val = raw_val.trim();
    if val.is_empty() {
        return String::new();
    }
    if val.len() > 10 {
        format!("{}...{}", &val[..6], &val[val.len() - 4..])
    } else {
        "********".to_string()
    }
}

/// Verify SHA-256 checksum of the historical backfill dataset (`data/real_4h_backfill.json`).
pub fn verify_data_provenance(backfill_path: impl AsRef<Path>) -> Result<bool> {
    let file_path = backfill_path.as_ref();
    let hash_path = PathBuf::from(format!("{}.sha256", file_path.display()));

    if !file_path.exists() {
        return Ok(false);
    }
    if !hash_path.exists() {
        return Ok(true);
    }

    let hash_text = fs::read_to_string(&hash_path)?;
    let expected_hash = hash_text
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let bytes = fs::read(file_path)?;
    let actual_hash = hex::encode(Sha256::digest(&bytes));

    if actual_hash != expected_hash {
        return Err(AegisError::Provenance(format!(
            "BACKFILL DATA CORRUPTION: SHA-256 mismatch for {}.\nExpected: {expected_hash}\nActual:   {actual_hash}",
            file_path.display()
        )));
    }
    Ok(true)
}

/// Round a floating-point number to `decimals` decimal places.
#[inline]
pub fn round_dp(val: f64, decimals: i32) -> f64 {
    if !val.is_finite() {
        return 0.0;
    }
    let factor = 10_f64.powi(decimals);
    (val * factor).round() / factor
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evm_and_private_key_validation() {
        let addr = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303";
        assert_eq!(validate_evm_address(addr, "wallet_address").unwrap(), addr);
        assert!(validate_evm_address("0x1234", "wallet_address").is_err());

        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        assert_eq!(validate_private_key(pk).unwrap(), pk);
        // Wallet address rejected as private key
        assert!(validate_private_key(addr).is_err());
        // Zero scalar rejected
        assert!(validate_private_key(&format!("0x{}", "0".repeat(64))).is_err());
    }

    #[test]
    fn test_mask_secret() {
        assert_eq!(
            mask_secret("0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"),
            "0x4c08...2318"
        );
        assert_eq!(mask_secret("short"), "********");
        assert_eq!(mask_secret(""), "");
    }
}
