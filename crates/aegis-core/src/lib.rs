//! Core domain models, state machine enums, EVM/secp256k1 validation, and SHA-256 dataset provenance.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const PLATFORM_NAME: &str = "Aegis Quant";
pub const PLATFORM_VERSION: &str = "0.3.0-rc1";

pub const MAINNET_HLP_VAULT_ADDRESS: &str = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303";
pub const TESTNET_HLP_VAULT_ADDRESS: &str = "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0";

pub const SENSITIVE_CONFIG_KEYS: &[&str] = &[
    "private_key",
    "gemini_api_key",
    "telegram_bot_token",
    "discord_webhook_url",
    "web_api_token",
];

/// Security-critical config keys that cannot be weakened or mutated via the Web API.
pub const IMMUTABLE_SECURITY_CONFIG_KEYS: &[&str] = &["enforce_master_key"];

/// Returns true if `key` is an immutable security policy key that cannot be modified via the Web API.
pub fn is_immutable_security_config_key(key: &str) -> bool {
    let k = key.trim().to_ascii_lowercase();
    IMMUTABLE_SECURITY_CONFIG_KEYS.contains(&k.as_str())
}

/// Returns true if `key` is a sensitive secret or credential key that must be encrypted at rest
/// and redacted in logs and API responses.
pub fn is_sensitive_config_key(key: &str) -> bool {
    let k = key.trim().to_ascii_lowercase();
    SENSITIVE_CONFIG_KEYS.contains(&k.as_str())
        || k == "web_api_token"
        || k.ends_with("_private_key")
        || k.ends_with("_api_key")
        || k.ends_with("_bot_token")
        || k.ends_with("_webhook_url")
        || k.ends_with("_secret")
}

/// Computes the absolute drift in basis points (`bps`) between Book A's equity and its target split of total platform equity.
pub fn compute_drift_bps(book_a_equity: f64, total_equity: f64, target_split_pct: f64) -> f64 {
    if total_equity <= 0.0 {
        return 0.0;
    }
    let target_a = total_equity * target_split_pct;
    round_dp(((book_a_equity - target_a).abs() / total_equity) * 10_000.0, 2)
}

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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FirewallError {
    #[error("Firewall violation (F1/F4): Book '{book}' attempted unauthorized access to table '{table}'")]
    UnauthorizedTableAccess { book: &'static str, table: String },
    #[error("Firewall violation (F4): Book '{book}' attempted unauthorized access to config key '{key}'")]
    UnauthorizedConfigAccess { book: &'static str, key: String },
    #[error("Firewall violation: {0}")]
    CrossBookViolation(String),
}

#[derive(Debug, Error)]
pub enum AegisError {
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Security violation: {0}")]
    SecurityViolation(String),
    #[error("Firewall violation: {0}")]
    Firewall(#[from] FirewallError),
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

/// PP-50 Book Identity (`PIT` = Book A, `POND` = Book B).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum BookType {
    Pit,
    Pond,
}

impl BookType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pit => "PIT",
            Self::Pond => "POND",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "PIT" | "BOOK_A" | "A" => Ok(Self::Pit),
            "POND" | "BOOK_B" | "B" => Ok(Self::Pond),
            other => Err(AegisError::Validation(format!(
                "Invalid book code '{other}': must be PIT or POND."
            ))),
        }
    }
}

impl fmt::Display for BookType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Dynamic Pond Universe membership status (`WARMUP`, `ACTIVE`, `DELISTING`, `RETIRED`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PondUniverseStatus {
    Warmup,
    Active,
    Delisting,
    Retired,
}

impl PondUniverseStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Warmup => "WARMUP",
            Self::Active => "ACTIVE",
            Self::Delisting => "DELISTING",
            Self::Retired => "RETIRED",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "WARMUP" => Ok(Self::Warmup),
            "ACTIVE" => Ok(Self::Active),
            "DELISTING" => Ok(Self::Delisting),
            "RETIRED" => Ok(Self::Retired),
            other => Err(AegisError::Validation(format!(
                "Invalid pond_universe status '{other}'"
            ))),
        }
    }
}

impl fmt::Display for PondUniverseStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Pond direction policy (`both`, `short_only`, `long_only`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PondDirectionPolicy {
    Both,
    ShortOnly,
    LongOnly,
}

impl PondDirectionPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Both => "both",
            Self::ShortOnly => "short_only",
            Self::LongOnly => "long_only",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "both" => Ok(Self::Both),
            "short_only" => Ok(Self::ShortOnly),
            "long_only" => Ok(Self::LongOnly),
            other => Err(AegisError::Validation(format!(
                "Invalid pond_direction_policy '{other}': must be 'both', 'short_only', or 'long_only'."
            ))),
        }
    }

    pub fn allows_side(&self, side: PondSide) -> bool {
        matches!(
            (self, side),
            (Self::Both, _) | (Self::ShortOnly, PondSide::Short) | (Self::LongOnly, PondSide::Long)
        )
    }

    pub fn allows_short_call(&self) -> bool {
        matches!(self, Self::Both | Self::ShortOnly)
    }

    pub fn allows_short_put(&self) -> bool {
        matches!(self, Self::Both | Self::LongOnly)
    }
}

impl fmt::Display for PondDirectionPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Perpetual side in Book B (`LONG` or `SHORT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PondSide {
    Long,
    Short,
}

impl PondSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Long => "LONG",
            Self::Short => "SHORT",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "LONG" => Ok(Self::Long),
            "SHORT" => Ok(Self::Short),
            other => Err(AegisError::Validation(format!(
                "Invalid Pond side '{other}': must be LONG or SHORT."
            ))),
        }
    }

    pub fn option_analogue(&self) -> OptionAnalogue {
        match self {
            Self::Long => OptionAnalogue::ShortPut,
            Self::Short => OptionAnalogue::ShortCall,
        }
    }
}

impl fmt::Display for PondSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Naked-option analogue in Book B (`SHORT_PUT` = Long Perp on negative funding, `SHORT_CALL` = Short Perp on positive funding).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OptionAnalogue {
    ShortPut,
    ShortCall,
}

impl OptionAnalogue {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ShortPut => "SHORT_PUT",
            Self::ShortCall => "SHORT_CALL",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "SHORT_PUT" => Ok(Self::ShortPut),
            "SHORT_CALL" => Ok(Self::ShortCall),
            other => Err(AegisError::Validation(format!(
                "Invalid option_analogue '{other}': must be SHORT_PUT or SHORT_CALL."
            ))),
        }
    }

    pub fn side(&self) -> PondSide {
        match self {
            Self::ShortPut => PondSide::Long,
            Self::ShortCall => PondSide::Short,
        }
    }
}

impl fmt::Display for OptionAnalogue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub const VALID_POND_CELL_STATES: &[&str] = &[
    "DORMANT",
    "SCREENED",
    "CANDIDATE",
    "ARMED",
    "OPEN",
    "TRAILED",
    "CLOSED",
    "COOLDOWN",
    "SKIPPED",
    "REJECTED",
];

/// 10-state Pond Cell Lifecycle FSM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PondCellState {
    Dormant,
    Screened,
    Candidate,
    Armed,
    Open,
    Trailed,
    Closed,
    Cooldown,
    Skipped,
    Rejected,
}

impl PondCellState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Dormant => "DORMANT",
            Self::Screened => "SCREENED",
            Self::Candidate => "CANDIDATE",
            Self::Armed => "ARMED",
            Self::Open => "OPEN",
            Self::Trailed => "TRAILED",
            Self::Closed => "CLOSED",
            Self::Cooldown => "COOLDOWN",
            Self::Skipped => "SKIPPED",
            Self::Rejected => "REJECTED",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "DORMANT" => Ok(Self::Dormant),
            "SCREENED" => Ok(Self::Screened),
            "CANDIDATE" => Ok(Self::Candidate),
            "ARMED" => Ok(Self::Armed),
            "OPEN" => Ok(Self::Open),
            "TRAILED" => Ok(Self::Trailed),
            "CLOSED" => Ok(Self::Closed),
            "COOLDOWN" => Ok(Self::Cooldown),
            "SKIPPED" => Ok(Self::Skipped),
            "REJECTED" => Ok(Self::Rejected),
            other => Err(AegisError::Validation(format!(
                "Invalid PondCellState '{other}': must be one of {VALID_POND_CELL_STATES:?}"
            ))),
        }
    }

    pub fn is_open_or_trailed(&self) -> bool {
        matches!(self, Self::Open | Self::Trailed)
    }

    pub fn can_transition_to(&self, next: PondCellState) -> bool {
        use PondCellState::*;
        matches!(
            (*self, next),
            (Dormant, Screened)
                | (Dormant, Cooldown)
                | (Dormant, Skipped)
                | (Screened, Candidate)
                | (Screened, Skipped)
                | (Screened, Dormant)
                | (Candidate, Armed)
                | (Candidate, Skipped)
                | (Candidate, Dormant)
                | (Armed, Open)
                | (Armed, Rejected)
                | (Armed, Skipped)
                | (Armed, Dormant)
                | (Open, Trailed)
                | (Open, Closed)
                | (Trailed, Trailed)
                | (Trailed, Closed)
                | (Closed, Cooldown)
                | (Closed, Dormant)
                | (Cooldown, Dormant)
                | (Skipped, Dormant)
                | (Rejected, Dormant)
        )
    }
}

impl fmt::Display for PondCellState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Book metadata row (`books` table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookRow {
    pub book_id: i64,
    pub book_code: String,
    pub description: String,
    pub equity_allocation_pct: f64,
    pub is_active: i64,
    pub created_at: String,
}

/// Dynamic 80-coin Pond universe row (`pond_universe` table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondUniverseRow {
    pub symbol: String,
    pub rank: i64,
    pub median_daily_volume_usdc: f64,
    pub tick_size: f64,
    pub lot_step_size: f64,
    pub min_notional_usdc: f64,
    pub max_leverage: f64,
    pub status: String,
    pub warmup_until: Option<String>,
    pub delist_streak: i64,
    pub added_at: String,
    pub delisted_at: Option<String>,
}

/// Pond Champion version record (`pond_champions` table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondChampion {
    pub champion_id: i64,
    pub version: String,
    pub params_json: String,
    pub walk_forward_score: f64,
    pub is_active: i64,
    pub crowned_at: Option<String>,
    pub retired_at: Option<String>,
}

/// Hourly Pond S-Cycle record (`pond_scan_cycles` table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondScanCycle {
    pub cycle_id: i64,
    pub started_at: String,
    pub champion_id: i64,
    pub coins_scanned: i64,
    pub candidates: i64,
    pub armed: i64,
    pub opened: i64,
    pub exits_evaluated: i64,
}

/// Pond Cell record (`pond_cells` table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondCell {
    pub cell_id: i64,
    pub symbol: String,
    pub champion_id: i64,
    pub state: String,
    pub cycle_opened_id: Option<i64>,
    pub side: String,
    pub option_analogue: String,
    pub entry_price: Option<f64>,
    pub quantity: Option<f64>,
    pub notional_usdc: Option<f64>,
    pub risk_budget_usdc: Option<f64>,
    pub conviction_multiplier: Option<f64>,
    pub funding_z_entry: Option<f64>,
    pub funding_rate_entry: Option<f64>,
    pub initial_stop: Option<f64>,
    pub chandelier_stop: Option<f64>,
    pub highest_price_seen: Option<f64>,
    pub lowest_price_seen: Option<f64>,
    pub funding_accrued_usdc: f64,
    pub realized_pnl_usdc: f64,
    pub fees_paid_usdc: f64,
    pub exit_price: Option<f64>,
    pub exit_reason: Option<String>,
    pub opened_at: Option<String>,
    pub closed_at: Option<String>,
    pub cooldown_until: Option<String>,
    pub updated_at: String,
}

/// Daily 00:05 UTC Settling transfer ledger row (`book_transfers` table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookTransfer {
    pub transfer_id: i64,
    pub settle_date: String,
    pub from_book: String,
    pub to_book: String,
    pub amount_usdc: f64,
    pub pre_from_usdc: Option<f64>,
    pub pre_to_usdc: Option<f64>,
    pub post_from_usdc: Option<f64>,
    pub post_to_usdc: Option<f64>,
    pub drift_bps_before: Option<f64>,
    pub drift_bps_after: Option<f64>,
    pub blocked_remainder_usdc: f64,
    pub status: String,
    pub attempts: i64,
    pub tx_ref: Option<String>,
    pub created_at: String,
}

/// Composite PP-50 two-book platform snapshot (`pp50_platform_snapshots` table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pp50PlatformSnapshot {
    pub snapshot_id: Option<i64>,
    pub timestamp_ms: i64,
    pub platform_equity_usdc: f64,
    pub pit_equity_usdc: f64,
    pub pond_equity_usdc: f64,
    pub split_pct: f64,
    pub drift_bps: f64,
    pub pit_open_count: i64,
    pub pond_open_cells: i64,
    pub settling_status: String,
    pub created_at: Option<String>,
}

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

/// Round `value` down (or nearest if `round_down == false`) to exchange lot or tick step size.
#[inline]
pub fn round_step(value: f64, step: f64, round_down: bool) -> f64 {
    if step <= 0.0 {
        return value;
    }
    let ratio = value / step;
    let stepped = if round_down {
        (ratio + 1e-12).floor() * step
    } else {
        ratio.round() * step
    };
    round_dp(stepped, 10)
}

/// Pure stateless Exponential Moving Average (EMA) series over `values` with span `span`.
pub fn compute_ema(values: &[f64], span: usize) -> Vec<f64> {
    if values.is_empty() {
        return Vec::new();
    }
    let eff_span = span.max(1) as f64;
    let alpha = 2.0 / (eff_span + 1.0);
    let mut out = Vec::with_capacity(values.len());
    let mut ema = values[0];
    out.push(ema);
    for &v in &values[1..] {
        ema = alpha * v + (1.0 - alpha) * ema;
        out.push(ema);
    }
    out
}

/// Pure stateless Average True Range (ATR) over `period` bars.
pub fn compute_atr_from_bars(bars: &[OhlcvBar], period: usize) -> f64 {
    if bars.len() < 2 {
        if let Some(last) = bars.last() {
            return (last.high - last.low).max(last.close * 0.015).max(1e-8);
        }
        return 1.0;
    }
    let mut tr_list = Vec::with_capacity(bars.len() - 1);
    for i in 1..bars.len() {
        let high = bars[i].high;
        let low = bars[i].low;
        let prev_close = bars[i - 1].close;
        let tr = (high - low)
            .max((high - prev_close).abs())
            .max((low - prev_close).abs());
        tr_list.push(tr);
    }
    let window = if tr_list.len() >= period && period > 0 {
        &tr_list[tr_list.len() - period..]
    } else {
        &tr_list[..]
    };
    let atr = if window.is_empty() {
        0.0
    } else {
        window.iter().sum::<f64>() / (window.len() as f64)
    };
    let current_close = bars.last().map(|b| b.close).unwrap_or(1.0);
    atr.max(current_close * 0.005).max(1e-8)
}

/// Pure stateless trailing z-score `(z_t, mean, std)` over the trailing `window` elements.
pub fn compute_trailing_z_score(values: &[f64], window: usize) -> (f64, f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let slice = if values.len() > window && window > 0 {
        &values[values.len() - window..]
    } else {
        values
    };
    let n = slice.len() as f64;
    let mean = slice.iter().sum::<f64>() / n;
    let var = slice
        .iter()
        .map(|&v| {
            let d = v - mean;
            d * d
        })
        .sum::<f64>()
        / n;
    let std = var.sqrt();
    let current = *slice.last().unwrap_or(&0.0);
    let z = if std > 1e-12 {
        (current - mean) / std
    } else {
        0.0
    };
    (z, mean, std)
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

    #[test]
    fn test_pond_cell_fsm_and_math_helpers() {
        use PondCellState::*;
        assert!(Dormant.can_transition_to(Screened));
        assert!(Screened.can_transition_to(Candidate));
        assert!(Candidate.can_transition_to(Armed));
        assert!(Armed.can_transition_to(Open));
        assert!(Open.can_transition_to(Trailed));
        assert!(Trailed.can_transition_to(Closed));
        assert!(Closed.can_transition_to(Cooldown));
        assert!(Cooldown.can_transition_to(Dormant));
        assert!(!Dormant.can_transition_to(Open));

        let mut series = vec![0.0001; 167];
        series.push(0.0005);
        let (z, mean, std) = compute_trailing_z_score(&series, 168);
        assert!(z > 2.0);
        assert!(mean > 0.0);
        assert!(std > 0.0);
    }
}

