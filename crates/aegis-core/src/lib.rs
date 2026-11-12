//! Core domain models, state machine enums, EVM/secp256k1 validation, and SHA-256 dataset provenance.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const PLATFORM_NAME: &str = "Aegis Quant";
pub const PLATFORM_VERSION: &str = "0.3.0-rc1";

pub const SENSITIVE_CONFIG_KEYS: &[&str] = &[
    "private_key",
    "gemini_api_key",
    "telegram_bot_token",
    "discord_webhook_url",
    "web_api_token",
];

/// Returns `true` if `key` is a sensitive secret or identifier that must be redacted in logs and REST echoes (`R3-B5`).
pub fn is_sensitive_config_key(key: &str) -> bool {
    let k = key.trim().to_ascii_lowercase();
    SENSITIVE_CONFIG_KEYS.contains(&k.as_str())
        || k == "telegram_chat_id"
        || k == "dashboard_api_token"
        || k.contains("secret")
        || k.contains("token")
        || k.contains("password")
        || k.contains("private_key")
        || k.contains("api_key")
        || k.contains("webhook")
}

/// Canonical Hyperliquid Mainnet HLP Vault EVM address.
pub const DEFAULT_MAINNET_HLP_VAULT_ADDRESS: &str = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303";

/// Canonical Hyperliquid Testnet HLP Vault EVM address.
pub const DEFAULT_TESTNET_HLP_VAULT_ADDRESS: &str = "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0";

/// Default Hyperliquid Testnet minimum daily volume cap (USDC) for Book B (`POND`) universe discovery and scanning (`R10-N4`, `Fix 6b`).
pub const TESTNET_POND_MIN_VOL_CAP_USDC: f64 = 1_000.0;

/// Security-critical config keys that cannot be weakened or mutated via the Web API.
pub const IMMUTABLE_SECURITY_CONFIG_KEYS: &[&str] = &[
    "enforce_master_key",
    "web_auth_enabled",
    "web_allow_remote_bind",
    "web_bind_host",
    "web_csp_policy",
    "web_cors_allowed_origins",
    "web_allowed_hosts",
    "web_api_token",
    "telegram_api_base_url",
    "hyperliquid_mainnet_url",
    "hyperliquid_testnet_url",
    "hyperliquid_info_url",
    "hyperliquid_testnet_info_url",
    "hyperliquid_exchange_url",
    "hyperliquid_testnet_exchange_url",
    "hyperliquid_mainnet_api_url",
    "hyperliquid_testnet_api_url",
    "hyperliquid_info_url_override",
    "hyperliquid_exchange_url_override",
    "pit_subaccount_address",
    "pond_subaccount_address",
    "hyperliquid_vault_address",
    "hyperliquid_default_vault_mainnet",
    "hyperliquid_default_vault_testnet",
    "hyperliquid_mainnet_vault_address",
    "hyperliquid_testnet_vault_address",
];

/// Returns true if `key` is an immutable security policy key that cannot be modified via the Web API.
pub fn is_immutable_security_config_key(key: &str) -> bool {
    let k = key.trim().to_ascii_lowercase();
    IMMUTABLE_SECURITY_CONFIG_KEYS.contains(&k.as_str())
}

/// Canonical UTC timestamp formatted as ISO-8601 (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn now_iso8601_utc() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Format a millisecond epoch timestamp as canonical ISO-8601 UTC (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn format_ms_iso8601_utc(ts_ms: i64) -> String {
    if ts_ms < 0 {
        return "1970-01-01T00:00:00Z".to_string();
    }
    chrono::DateTime::from_timestamp_millis(ts_ms)
        .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH)
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

/// Normalize a legacy SQLite (`YYYY-MM-DD HH:MM:SS`) or RFC3339 timestamp string into canonical ISO-8601 UTC (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn normalize_timestamp_iso8601_utc(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return dt
            .with_timezone(&chrono::Utc)
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        return ndt.and_utc().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S") {
        return ndt.and_utc().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    }
    trimmed.to_string()
}

/// Parse an RFC3339 or legacy SQLite UTC timestamp string into a `NaiveDateTime`.
pub fn parse_utc_timestamp(raw: &str) -> Option<chrono::NaiveDateTime> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.naive_utc());
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%SZ") {
        return Some(ndt);
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        return Some(ndt);
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S") {
        return Some(ndt);
    }
    None
}

/// Typed USDC amount newtype (`NM-15`) preventing unit confusion in risk sizing and equity accounting.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Usdc(pub f64);

impl Usdc {
    pub const ZERO: Self = Self(0.0);

    #[inline]
    pub fn new(val: f64) -> Self {
        debug_assert!(val.is_finite(), "Usdc::new called with non-finite value: {val}");
        if val.is_finite() {
            Self(round_dp(val, 4))
        } else {
            Self(0.0)
        }
    }

    #[inline]
    pub fn new_clamped(val: f64) -> Self {
        debug_assert!(val.is_finite(), "Usdc::new_clamped called with non-finite value: {val}");
        if val.is_finite() && val > 0.0 {
            Self(round_dp(val, 4))
        } else {
            Self(0.0)
        }
    }

    #[inline]
    pub const fn value(self) -> f64 {
        self.0
    }

    #[inline]
    pub const fn as_f64(self) -> f64 {
        self.0
    }

    #[inline]
    pub fn abs(self) -> Self {
        Self(self.0.abs())
    }

    #[inline]
    pub fn saturating_sub(self, rhs: Self) -> Self {
        Self((self.0 - rhs.0).max(0.0))
    }

    #[inline]
    pub fn max(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }

    #[inline]
    pub fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
    }

    #[inline]
    pub fn clamp(self, min: Self, max: Self) -> Self {
        let (lo, hi) = if min.0 <= max.0 {
            (min.0, max.0)
        } else {
            (max.0, min.0)
        };
        Self(self.0.clamp(lo, hi))
    }

    #[inline]
    pub fn round_dp(self, decimals: i32) -> Self {
        Self(round_dp(self.0, decimals))
    }

    #[inline]
    pub fn round_cents(self) -> Self {
        Self(round_dp(self.0, 2))
    }
}

impl std::ops::Add for Usdc {
    type Output = Self;
    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        Self::new(self.0 + rhs.0)
    }
}

impl std::ops::Sub for Usdc {
    type Output = Self;
    #[inline]
    fn sub(self, rhs: Self) -> Self::Output {
        Self::new(self.0 - rhs.0)
    }
}

impl std::ops::AddAssign for Usdc {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        *self = Self::new(self.0 + rhs.0);
    }
}

impl std::ops::SubAssign for Usdc {
    #[inline]
    fn sub_assign(&mut self, rhs: Self) {
        *self = Self::new(self.0 - rhs.0);
    }
}

impl std::ops::Mul<f64> for Usdc {
    type Output = Self;
    #[inline]
    fn mul(self, rhs: f64) -> Self::Output {
        Self::new(self.0 * rhs)
    }
}

impl std::ops::Div<f64> for Usdc {
    type Output = Self;
    #[inline]
    fn div(self, rhs: f64) -> Self::Output {
        if rhs.is_finite() && rhs.abs() > 0.0 {
            Self::new(self.0 / rhs)
        } else {
            Self::ZERO
        }
    }
}

impl std::iter::Sum for Usdc {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        Self::new(iter.map(|u| u.0).sum())
    }
}

impl From<f64> for Usdc {
    fn from(v: f64) -> Self {
        Self::new(v)
    }
}

impl fmt::Display for Usdc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.2}", self.0)
    }
}

/// Typed instrument price newtype (`NM-15`).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Price(pub f64);

impl Price {
    pub const ZERO: Self = Self(0.0);

    #[inline]
    pub fn new(val: f64) -> Self {
        debug_assert!(val.is_finite(), "Price::new called with non-finite value: {val}");
        if val.is_finite() {
            Self(val)
        } else {
            Self(0.0)
        }
    }

    #[inline]
    pub fn new_clamped(val: f64) -> Self {
        debug_assert!(val.is_finite(), "Price::new_clamped called with non-finite value: {val}");
        if val.is_finite() && val > 0.0 {
            Self(val)
        } else {
            Self(0.0)
        }
    }

    #[inline]
    pub const fn value(self) -> f64 {
        self.0
    }

    #[inline]
    pub const fn as_f64(self) -> f64 {
        self.0
    }

    #[inline]
    pub fn abs_diff(self, other: Self) -> f64 {
        (self.0 - other.0).abs()
    }

    #[inline]
    pub fn max(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }

    #[inline]
    pub fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
    }

    #[inline]
    pub fn round_step(self, step: f64, round_down: bool) -> Self {
        Self(round_step(self.0, step, round_down))
    }
}

impl From<f64> for Price {
    fn from(v: f64) -> Self {
        Self::new(v)
    }
}

impl std::ops::Add for Price {
    type Output = Self;
    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl std::ops::Sub for Price {
    type Output = Self;
    #[inline]
    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Typed contract quantity newtype (`NM-15`).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Quantity(pub f64);

impl Quantity {
    pub const ZERO: Self = Self(0.0);

    #[inline]
    pub fn new(val: f64) -> Self {
        debug_assert!(val.is_finite(), "Quantity::new called with non-finite value: {val}");
        if val.is_finite() {
            Self(val)
        } else {
            Self(0.0)
        }
    }

    #[inline]
    pub fn new_clamped(val: f64) -> Self {
        debug_assert!(val.is_finite(), "Quantity::new_clamped called with non-finite value: {val}");
        if val.is_finite() && val > 0.0 {
            Self(val)
        } else {
            Self(0.0)
        }
    }

    #[inline]
    pub const fn value(self) -> f64 {
        self.0
    }

    #[inline]
    pub const fn as_f64(self) -> f64 {
        self.0
    }

    #[inline]
    pub fn abs(self) -> Self {
        Self(self.0.abs())
    }

    #[inline]
    pub fn from_notional_rounded_down(notional: Usdc, price: Price, step: f64) -> Self {
        if price.0 <= 0.0 || !price.0.is_finite() {
            return Self(0.0);
        }
        let raw_qty = notional.0 / price.0;
        Self(round_step(raw_qty, step, true).max(0.0))
    }

    #[inline]
    pub fn notional_at(self, price: Price) -> Usdc {
        Usdc::new_clamped(self.0.abs() * price.0)
    }

    #[inline]
    pub fn max(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }

    #[inline]
    pub fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
    }

    #[inline]
    pub fn round_step(self, step: f64, round_down: bool) -> Self {
        Self(round_step(self.0, step, round_down))
    }
}

impl From<f64> for Quantity {
    fn from(v: f64) -> Self {
        Self::new(v)
    }
}

impl fmt::Display for Quantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Typed leverage multiplier newtype (`NM-15`).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Leverage(pub f64);

impl Leverage {
    pub const ZERO: Self = Self(0.0);

    #[inline]
    pub fn new(val: f64) -> Self {
        debug_assert!(val.is_finite(), "Leverage::new called with non-finite value: {val}");
        if val.is_finite() {
            Self(val)
        } else {
            Self(0.0)
        }
    }

    #[inline]
    pub fn new_clamped(val: f64) -> Self {
        debug_assert!(val.is_finite(), "Leverage::new_clamped called with non-finite value: {val}");
        if val.is_finite() && val >= 0.0 {
            Self(val)
        } else {
            Self(0.0)
        }
    }

    #[inline]
    pub const fn as_f64(self) -> f64 {
        self.0
    }
}

/// Typed basis-points (`1 bps = 0.01%`) newtype (`NM-15`).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Bps(pub f64);

impl Bps {
    pub const ZERO: Self = Self(0.0);

    #[inline]
    pub fn new(val: f64) -> Self {
        debug_assert!(val.is_finite(), "Bps::new called with non-finite value: {val}");
        if val.is_finite() {
            Self(val)
        } else {
            Self(0.0)
        }
    }

    #[inline]
    pub const fn as_f64(self) -> f64 {
        self.0
    }

    #[inline]
    pub fn from_fraction(frac: f64) -> Self {
        debug_assert!(frac.is_finite(), "Bps::from_fraction called with non-finite value: {frac}");
        if frac.is_finite() {
            Self(frac * 10_000.0)
        } else {
            Self(0.0)
        }
    }

    #[inline]
    pub fn to_fraction(self) -> f64 {
        self.0 / 10_000.0
    }
}

/// Computes the absolute drift in basis points (`bps`) between Book A's equity and its target split of total platform equity.
pub fn compute_drift_bps(book_a_equity: f64, total_equity: f64, target_split_pct: f64) -> f64 {
    if total_equity <= 0.0 {
        return 0.0;
    }
    let target_a = total_equity * target_split_pct;
    round_dp(((book_a_equity - target_a).abs() / total_equity) * 10_000.0, 2)
}

/// Strictly parse a timeframe string into its duration in seconds (`R3-N13`).
pub fn parse_timeframe_seconds(tf: &str) -> Result<i64> {
    match tf.trim().to_ascii_lowercase().as_str() {
        "1m" => Ok(60),
        "5m" => Ok(300),
        "15m" => Ok(900),
        "30m" => Ok(1800),
        "1h" => Ok(3600),
        "2h" => Ok(7200),
        "4h" => Ok(14400),
        "6h" => Ok(21600),
        "8h" => Ok(28800),
        "12h" => Ok(43200),
        "1d" | "24h" => Ok(86400),
        other => Err(AegisError::Validation(format!(
            "Unsupported timeframe '{other}': must be one of 1m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 24h."
        ))),
    }
}

/// Validate a comma-separated list of timeframes (`calibration_timeframes_csv`) (`R3-N13`).
pub fn validate_timeframe_csv(csv: &str) -> Result<String> {
    let mut normalized = Vec::new();
    for part in csv.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let _ = parse_timeframe_seconds(trimmed)?;
        normalized.push(trimmed.to_ascii_lowercase());
    }
    if normalized.is_empty() {
        return Err(AegisError::Validation(
            "Timeframe CSV cannot be empty; provide at least one valid timeframe (e.g. '1h,2h,4h')."
                .to_string(),
        ));
    }
    Ok(normalized.join(","))
}

/// Timeframe duration in seconds (warns and falls back to `3600` on unknown timeframe strings, `R3-N13`).
pub fn timeframe_seconds(tf: &str) -> i64 {
    match parse_timeframe_seconds(tf) {
        Ok(secs) => secs,
        Err(_) => {
            tracing::warn!(
                timeframe = %tf,
                fallback_seconds = 3600,
                "Unknown timeframe passed to timeframe_seconds(); falling back to 3600s (1h)"
            );
            3600
        }
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
    #[error("Configuration error: {0}")]
    Config(String),
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
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
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
    pub const ALL: &'static [Self] = &[
        Self::Dormant,
        Self::Screened,
        Self::Candidate,
        Self::Armed,
        Self::Open,
        Self::Trailed,
        Self::Closed,
        Self::Cooldown,
        Self::Skipped,
        Self::Rejected,
    ];

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
            other => {
                let valid: Vec<&'static str> = Self::ALL.iter().map(Self::as_str).collect();
                Err(AegisError::Validation(format!(
                    "Invalid PondCellState '{other}': must be one of {valid:?}"
                )))
            }
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

fn default_reference_price() -> f64 {
    0.0
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
    #[serde(default = "default_reference_price")]
    pub reference_price: f64,
    #[serde(default)]
    pub is_synthetic_volume: i64,
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
    #[serde(default)]
    pub resting_order_id: Option<String>,
}

/// Typed status enum for PP-50 daily Settling ledger rows (`book_transfers.status`, `R4-QA`, `R4-H1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BookTransferStatus {
    #[serde(rename = "PENDING")]
    Pending,
    #[serde(rename = "COMPLETED")]
    Completed,
    #[serde(rename = "CONFIRMED")]
    Confirmed,
    #[serde(rename = "PARTIAL_IDLE_CAPPED")]
    PartialIdleCapped,
    #[serde(rename = "PARTIAL")]
    Partial,
    #[serde(rename = "AMBIGUOUS")]
    Ambiguous,
    #[serde(rename = "SKIPPED_DEADBAND", alias = "SKIPPED_WITHIN_BAND", alias = "NO_ACTION_WITHIN_DEADBAND")]
    SkippedWithinBand,
    #[serde(rename = "BLOCKED_ZERO_IDLE", alias = "SKIPPED_NO_IDLE")]
    SkippedNoIdle,
    #[serde(rename = "HALTED_PP50")]
    HaltedPp50,
    #[serde(rename = "DRY_RUN")]
    DryRun,
    #[serde(rename = "FAILED")]
    Failed,
}

impl BookTransferStatus {
    pub const ALL: &'static [Self] = &[
        Self::Pending,
        Self::Completed,
        Self::Confirmed,
        Self::PartialIdleCapped,
        Self::Partial,
        Self::Ambiguous,
        Self::SkippedWithinBand,
        Self::SkippedNoIdle,
        Self::HaltedPp50,
        Self::DryRun,
        Self::Failed,
    ];

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Completed => "COMPLETED",
            Self::Confirmed => "CONFIRMED",
            Self::PartialIdleCapped => "PARTIAL_IDLE_CAPPED",
            Self::Partial => "PARTIAL",
            Self::Ambiguous => "AMBIGUOUS",
            Self::SkippedWithinBand => "SKIPPED_DEADBAND",
            Self::SkippedNoIdle => "BLOCKED_ZERO_IDLE",
            Self::HaltedPp50 => "HALTED_PP50",
            Self::DryRun => "DRY_RUN",
            Self::Failed => "FAILED",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "PENDING" => Ok(Self::Pending),
            "COMPLETED" => Ok(Self::Completed),
            "CONFIRMED" => Ok(Self::Confirmed),
            "PARTIAL_IDLE_CAPPED" => Ok(Self::PartialIdleCapped),
            "PARTIAL" => Ok(Self::Partial),
            "AMBIGUOUS" => Ok(Self::Ambiguous),
            "SKIPPED_DEADBAND" | "SKIPPED_WITHIN_BAND" | "NO_ACTION_WITHIN_DEADBAND" => {
                Ok(Self::SkippedWithinBand)
            }
            "BLOCKED_ZERO_IDLE" | "SKIPPED_NO_IDLE" => Ok(Self::SkippedNoIdle),
            "HALTED_PP50" => Ok(Self::HaltedPp50),
            "DRY_RUN" => Ok(Self::DryRun),
            "FAILED" => Ok(Self::Failed),
            other => {
                let valid: Vec<&'static str> = Self::ALL.iter().map(Self::as_str).collect();
                Err(AegisError::Validation(format!(
                    "Invalid BookTransferStatus '{other}': must be one of {valid:?}"
                )))
            }
        }
    }

    /// Returns `true` if this transfer status counts toward virtual two-book net equity adjustments.
    pub const fn is_ledger_counted(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Confirmed | Self::PartialIdleCapped
        )
    }

    /// Returns `true` if this transfer status indicates an unresolved two-leg live transfer
    /// requiring manual reconciliation (`PARTIAL` or `AMBIGUOUS`).
    pub const fn needs_manual_reconciliation(&self) -> bool {
        matches!(self, Self::Partial | Self::Ambiguous)
    }

    /// Returns `true` if a state transition from `self` to `next` is valid.
    pub const fn can_transition_to(&self, next: Self) -> bool {
        use BookTransferStatus::*;
        matches!(
            (*self, next),
            (Pending, Pending)
                | (Pending, Completed)
                | (Pending, Confirmed)
                | (Pending, PartialIdleCapped)
                | (Pending, Partial)
                | (Pending, Ambiguous)
                | (Pending, Failed)
                | (Pending, DryRun)
                | (Partial, Partial)
                | (Partial, Confirmed)
                | (Partial, Completed)
                | (Partial, Failed)
                | (Ambiguous, Ambiguous)
                | (Ambiguous, Confirmed)
                | (Ambiguous, Completed)
                | (Ambiguous, Partial)
                | (Ambiguous, Failed)
                | (Completed, Completed)
                | (Confirmed, Confirmed)
                | (PartialIdleCapped, PartialIdleCapped)
                | (SkippedWithinBand, SkippedWithinBand)
                | (SkippedNoIdle, SkippedNoIdle)
                | (HaltedPp50, HaltedPp50)
                | (DryRun, DryRun)
                | (Failed, Failed)
        )
    }
}

impl std::str::FromStr for BookTransferStatus {
    type Err = AegisError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl fmt::Display for BookTransferStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for BookTransferStatus {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
            || BookTransferStatus::parse(other)
                .map(|parsed| parsed == *self)
                .unwrap_or(false)
    }
}

impl PartialEq<String> for BookTransferStatus {
    fn eq(&self, other: &String) -> bool {
        self == &other.as_str()
    }
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
    pub const ALL: &'static [Self] = &[
        Self::Pending,
        Self::OpenOnBook,
        Self::PartiallyFilled,
        Self::Filled,
        Self::Cancelled,
        Self::Rejected,
        Self::Expired,
    ];

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
            other => {
                let valid: Vec<&'static str> = Self::ALL.iter().map(Self::as_str).collect();
                Err(AegisError::Validation(format!(
                    "Invalid fsm_state '{other}': must be one of {valid:?}"
                )))
            }
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Cancelled | Self::Rejected | Self::Expired
        )
    }

    pub fn can_transition_to(&self, next: OrderFsmState) -> bool {
        use OrderFsmState::*;
        matches!(
            (*self, next),
            (Pending, Pending)
                | (Pending, OpenOnBook)
                | (Pending, PartiallyFilled)
                | (Pending, Filled)
                | (Pending, Cancelled)
                | (Pending, Rejected)
                | (Pending, Expired)
                | (OpenOnBook, OpenOnBook)
                | (OpenOnBook, PartiallyFilled)
                | (OpenOnBook, Filled)
                | (OpenOnBook, Cancelled)
                | (OpenOnBook, Rejected)
                | (OpenOnBook, Expired)
                | (PartiallyFilled, PartiallyFilled)
                | (PartiallyFilled, Filled)
                | (PartiallyFilled, Cancelled)
                | (PartiallyFilled, Expired)
        )
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
    #[serde(default = "default_reference_price")]
    pub reference_price: f64,
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

impl OrderExecutionResult {
    /// Construct a rejected `OrderExecutionResult` (`Q1` / `NQ-1`).
    pub fn rejected(
        client_order_id: impl Into<String>,
        symbol: impl Into<String>,
        side: impl Into<String>,
        requested_units: f64,
    ) -> Self {
        Self::rejected_with_type(client_order_id, symbol, side, requested_units, "REJECTED")
    }

    /// Construct a rejected `OrderExecutionResult` with custom fill type (`Q1`).
    pub fn rejected_with_type(
        client_order_id: impl Into<String>,
        symbol: impl Into<String>,
        side: impl Into<String>,
        requested_units: f64,
        fill_type: impl Into<String>,
    ) -> Self {
        Self {
            client_order_id: client_order_id.into(),
            exchange_order_id: String::new(),
            symbol: symbol.into(),
            side: side.into(),
            fsm_state: "REJECTED".to_string(),
            requested_units: round_dp(requested_units, 6),
            filled_units: 0.0,
            avg_fill_price: 0.0,
            slippage_paid_usd: 0.0,
            effective_slippage_bps: 0.0,
            fees_paid_usdc: 0.0,
            is_maker: false,
            fill_type: fill_type.into(),
            state_history: vec!["CREATED".to_string(), "REJECTED".to_string()],
        }
    }

    /// Construct a cancelled `OrderExecutionResult`.
    pub fn cancelled(
        client_order_id: impl Into<String>,
        symbol: impl Into<String>,
        side: impl Into<String>,
        requested_units: f64,
    ) -> Self {
        Self {
            client_order_id: client_order_id.into(),
            exchange_order_id: String::new(),
            symbol: symbol.into(),
            side: side.into(),
            fsm_state: "CANCELLED".to_string(),
            requested_units: round_dp(requested_units, 6),
            filled_units: 0.0,
            avg_fill_price: 0.0,
            slippage_paid_usd: 0.0,
            effective_slippage_bps: 0.0,
            fees_paid_usdc: 0.0,
            is_maker: false,
            fill_type: "CANCELLED".to_string(),
            state_history: vec![
                "CREATED".to_string(),
                "SUBMITTED".to_string(),
                "OPEN_ON_BOOK".to_string(),
                "CANCELLED".to_string(),
            ],
        }
    }

    /// Construct an expired `OrderExecutionResult`.
    pub fn expired(
        client_order_id: impl Into<String>,
        symbol: impl Into<String>,
        side: impl Into<String>,
        requested_units: f64,
    ) -> Self {
        Self {
            client_order_id: client_order_id.into(),
            exchange_order_id: String::new(),
            symbol: symbol.into(),
            side: side.into(),
            fsm_state: "EXPIRED".to_string(),
            requested_units: round_dp(requested_units, 6),
            filled_units: 0.0,
            avg_fill_price: 0.0,
            slippage_paid_usd: 0.0,
            effective_slippage_bps: 0.0,
            fees_paid_usdc: 0.0,
            is_maker: false,
            fill_type: "EXPIRED".to_string(),
            state_history: vec![
                "CREATED".to_string(),
                "SUBMITTED".to_string(),
                "OPEN_ON_BOOK".to_string(),
                "EXPIRED".to_string(),
            ],
        }
    }

    /// Construct an `OPEN_ON_BOOK` resting order result (`NH-4` / `NH-7`).
    pub fn open_on_book(
        client_order_id: impl Into<String>,
        exchange_order_id: impl Into<String>,
        symbol: impl Into<String>,
        side: impl Into<String>,
        requested_units: f64,
    ) -> Self {
        Self {
            client_order_id: client_order_id.into(),
            exchange_order_id: exchange_order_id.into(),
            symbol: symbol.into(),
            side: side.into(),
            fsm_state: "OPEN_ON_BOOK".to_string(),
            requested_units: round_dp(requested_units, 6),
            filled_units: 0.0,
            avg_fill_price: 0.0,
            slippage_paid_usd: 0.0,
            effective_slippage_bps: 0.0,
            fees_paid_usdc: 0.0,
            is_maker: true,
            fill_type: "HL_RESTING_ALO".to_string(),
            state_history: vec![
                "CREATED".to_string(),
                "SUBMITTED".to_string(),
                "OPEN_ON_BOOK".to_string(),
            ],
        }
    }

    /// Construct a `FILLED` order execution result.
    #[allow(clippy::too_many_arguments)]
    pub fn filled(
        client_order_id: impl Into<String>,
        exchange_order_id: impl Into<String>,
        symbol: impl Into<String>,
        side: impl Into<String>,
        requested_units: f64,
        filled_units: f64,
        avg_fill_price: f64,
        slippage_paid_usd: f64,
        effective_slippage_bps: f64,
        fees_paid_usdc: f64,
        is_maker: bool,
        fill_type: impl Into<String>,
        state_history: Vec<String>,
    ) -> Self {
        Self {
            client_order_id: client_order_id.into(),
            exchange_order_id: exchange_order_id.into(),
            symbol: symbol.into(),
            side: side.into(),
            fsm_state: "FILLED".to_string(),
            requested_units: round_dp(requested_units, 6),
            filled_units: round_dp(filled_units, 6),
            avg_fill_price: round_dp(avg_fill_price, 6),
            slippage_paid_usd: round_dp(slippage_paid_usd, 4),
            effective_slippage_bps: round_dp(effective_slippage_bps, 4),
            fees_paid_usdc: round_dp(fees_paid_usdc, 4),
            is_maker,
            fill_type: fill_type.into(),
            state_history,
        }
    }
}

/// Normalized trade record (`trades` table).
///
/// Invariant (`R7-N4`): `quantity` and `filled_units` are strictly non-negative magnitudes
/// (`>= 0.0`); directional sign is represented exclusively by `side` (`"LONG"` vs `"SHORT"`).
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
    /// Positive position size magnitude (`>= 0.0`; `R7-N4`).
    pub quantity: f64,
    /// Positive filled unit magnitude (`>= 0.0`; `R7-N4`).
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

/// Locate the project root containing `sql/schema_and_seed.sql` (`R3-H19`).
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
    if let Ok(exe) = std::env::current_exe() {
        for ancestor in exe.ancestors() {
            if ancestor.join("sql").join("schema_and_seed.sql").exists() {
                return ancestor.to_path_buf();
            }
        }
    }
    if let Some(manifest_root) = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(Path::to_path_buf)
    {
        if manifest_root.exists() {
            return manifest_root;
        }
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
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
            "Invalid private_key: must be a 64-hex-character (64 hex characters / 32-byte) private key (received {} characters after 0x prefix).",
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

/// Derive the 42-character lowercase `0x` Ethereum address from a secp256k1 private key (`NH-12`).
pub fn derive_evm_address_from_private_key(private_key: &str) -> Result<String> {
    let norm_pk = validate_private_key(private_key)?;
    if norm_pk.is_empty() || norm_pk.len() < 66 {
        return Err(AegisError::Validation(
            "Cannot derive EVM address from empty private_key.".to_string(),
        ));
    }
    let pk_bytes = hex::decode(&norm_pk[2..]).map_err(|e| {
        AegisError::Validation(format!("Invalid private_key hex encoding: {e}"))
    })?;
    let signing_key = k256::ecdsa::SigningKey::from_slice(&pk_bytes).map_err(|e| {
        AegisError::Validation(format!("Invalid secp256k1 signing key: {e}"))
    })?;
    let verifying_key = signing_key.verifying_key();
    let encoded_pt = verifying_key.to_encoded_point(false);
    let pub_bytes = encoded_pt.as_bytes();
    if pub_bytes.len() != 65 || pub_bytes[0] != 0x04 {
        return Err(AegisError::Crypto(
            "Unexpected uncompressed secp256k1 public key length".to_string(),
        ));
    }
    use sha3::Keccak256;
    let hash = Keccak256::digest(&pub_bytes[1..]);
    Ok(format!("0x{}", hex::encode(&hash[12..])))
}

/// Verify that the Ethereum address derived from `private_key` matches `wallet_address`
/// or an explicitly configured `agent_address` (`NH-12`).
pub fn verify_signer_matches_wallet(
    private_key: &str,
    wallet_address: &str,
    agent_address: Option<&str>,
) -> Result<String> {
    let derived = derive_evm_address_from_private_key(private_key)?;
    let norm_wallet = validate_evm_address(wallet_address, "wallet_address")?;
    if norm_wallet.is_empty() {
        return Err(AegisError::Validation(
            "wallet_address cannot be empty when verifying signer binding.".to_string(),
        ));
    }
    if derived.eq_ignore_ascii_case(&norm_wallet) {
        return Ok(derived);
    }
    if let Some(agent_raw) = agent_address.map(str::trim).filter(|s| !s.is_empty()) {
        let norm_agent = validate_evm_address(agent_raw, "hyperliquid_agent_address")?;
        if derived.eq_ignore_ascii_case(&norm_agent) {
            return Ok(derived);
        }
        return Err(AegisError::Validation(format!(
            "Private key signer address '{derived}' matches neither wallet_address '{norm_wallet}' nor hyperliquid_agent_address '{norm_agent}'."
        )));
    }
    Err(AegisError::Validation(format!(
        "Private key signer address '{derived}' does not match wallet_address '{norm_wallet}' (and no hyperliquid_agent_address is configured)."
    )))
}

/// Mask sensitive private keys and API keys for safe telemetry output (`R3-H14`).
pub fn mask_secret(raw_val: &str) -> String {
    let val = raw_val.trim();
    if val.is_empty() {
        return String::new();
    }
    let chars: Vec<char> = val.chars().collect();
    if chars.len() > 10 {
        let prefix: String = chars[..6].iter().collect();
        let suffix: String = chars[chars.len() - 4..].iter().collect();
        format!("{prefix}...{suffix}")
    } else {
        "********".to_string()
    }
}

/// Mask semi-sensitive identifiers such as `telegram_chat_id` for API responses (`NL-6`, `R3-H14`).
pub fn mask_identifier(raw_val: &str) -> String {
    let val = raw_val.trim();
    if val.is_empty() {
        return String::new();
    }
    let chars: Vec<char> = val.chars().collect();
    if chars.len() <= 4 {
        "****".to_string()
    } else if chars.len() <= 6 {
        let prefix: String = chars[..2].iter().collect();
        let suffix: String = chars[chars.len() - 2..].iter().collect();
        format!("{prefix}***{suffix}")
    } else {
        let prefix: String = chars[..3].iter().collect();
        let suffix: String = chars[chars.len() - 3..].iter().collect();
        format!("{prefix}...{suffix}")
    }
}

fn is_url_token_delimiter(c: char) -> bool {
    c.is_whitespace() || matches!(c, '/' | '?' | '&' | '"' | '\'' | ',' | ';' | ')' | '}' | ']')
}

fn is_webhook_path_delimiter(c: char) -> bool {
    c.is_whitespace() || matches!(c, '?' | '&' | '"' | '\'' | ',' | ';' | ')' | '}' | ']')
}

/// Redact embedded Telegram bot URLs (`/bot<token>`) and Discord webhook URLs (`/api/webhooks/<id>/<token>`) (`R3-B5`, `R4-QC`).
pub fn redact_embedded_url_secrets(input: &str) -> String {
    let mut out = input.to_string();

    // 1. Redact Telegram bot URLs: `api.telegram.org/bot<token>` or `/bot<id>:<secret>`
    let mut search_from = 0;
    while search_from < out.len() {
        let lower = out[search_from..].to_ascii_lowercase();
        let Some(rel_idx) = lower.find("/bot") else {
            break;
        };
        let token_start = search_from + rel_idx + 4;
        if token_start >= out.len() {
            break;
        }
        let rest = &out[token_start..];
        let token_len = rest
            .find(is_url_token_delimiter)
            .unwrap_or(rest.len());
        let token_candidate = &rest[..token_len];
        let looks_like_bot_token = token_candidate != "[REDACTED]"
            && !token_candidate.is_empty()
            && (token_candidate.contains(':')
                || out[..search_from + rel_idx]
                    .to_ascii_lowercase()
                    .ends_with("api.telegram.org"));
        if looks_like_bot_token {
            out.replace_range(token_start..token_start + token_len, "[REDACTED]");
            search_from = token_start + "[REDACTED]".len();
        } else {
            search_from = token_start;
        }
    }

    // 2. Redact Discord webhook URLs: `/api/webhooks/<id>/<token>`
    let mut wh_from = 0;
    while wh_from < out.len() {
        let lower = out[wh_from..].to_ascii_lowercase();
        let Some(rel_idx) = lower.find("/api/webhooks/") else {
            break;
        };
        let path_start = wh_from + rel_idx + "/api/webhooks/".len();
        if path_start >= out.len() {
            break;
        }
        let rest = &out[path_start..];
        let path_len = rest
            .find(is_webhook_path_delimiter)
            .unwrap_or(rest.len());
        let candidate = &rest[..path_len];
        if candidate != "[REDACTED]" && !candidate.is_empty() {
            out.replace_range(path_start..path_start + path_len, "[REDACTED]");
            wh_from = path_start + "[REDACTED]".len();
        } else {
            wh_from = path_start;
        }
    }

    out
}

/// Scrub inline secret patterns (`private_key=...`, `private_key: ...`, `telegram_bot_token = ...`,
/// embedded Telegram/Discord webhook URLs, etc.) from log strings (`R3-B2`, `R3-B5`, `R4-N5`, `R4-QC`).
pub fn sanitize_telemetry_message(message: &str) -> String {
    let mut sanitized = redact_embedded_url_secrets(message);
    let lower = sanitized.to_ascii_lowercase();

    const SENSITIVE_KEYS: &[&str] = &[
        "hyperliquid_private_key",
        "private_key",
        "gemini_api_key",
        "telegram_bot_token",
        "bot_token",
        "discord_webhook_url",
        "webhook_url",
        "web_api_token",
        "web_auth_token",
        "auth_token",
        "aegis_master_key",
        "master_key",
    ];

    let has_sensitive = SENSITIVE_KEYS.iter().any(|k| lower.contains(k));
    if !has_sensitive {
        return sanitized;
    }

    // R4-N5: Only redact `config_value`/`value`/`val` when the message explicitly pairs
    // `config_key` (or `key`) with a sensitive key name (e.g. `config_key=private_key config_value=...`),
    // NOT merely because a sensitive key name is mentioned in prose alongside an unrelated `value=...`.
    let has_paired_config_key = SENSITIVE_KEYS.iter().any(|sk| {
        lower.contains(&format!("config_key={sk}"))
            || lower.contains(&format!("config_key: {sk}"))
            || lower.contains(&format!("config_key=\"{sk}\""))
            || lower.contains(&format!("key={sk}"))
            || lower.contains(&format!("key: {sk}"))
            || lower.contains(&format!("key=\"{sk}\""))
    });

    let mut keys_to_redact: Vec<&str> = SENSITIVE_KEYS.to_vec();
    if has_paired_config_key {
        keys_to_redact.push("config_value");
        keys_to_redact.push("value");
        keys_to_redact.push("val");
    }

    for key in keys_to_redact {
        let mut search_from = 0;
        while search_from < sanitized.len() {
            let lower_slice = sanitized[search_from..].to_ascii_lowercase();
            let Some(rel_idx) = lower_slice.find(key) else {
                break;
            };
            let abs_idx = search_from + rel_idx;
            if (key == "val" || key == "value" || key == "config_value") && abs_idx > 0 {
                let prev = sanitized[..abs_idx].chars().next_back().unwrap_or(' ');
                if prev.is_ascii_alphanumeric() || prev == '_' {
                    search_from = abs_idx + key.len();
                    continue;
                }
            }
            let after_key = abs_idx + key.len();
            if after_key >= sanitized.len() {
                break;
            }
            let tail = &sanitized[after_key..];
            let mut chars = tail.char_indices().peekable();
            while let Some(&(_, c)) = chars.peek() {
                if c == ' ' || c == '\t' || c == '"' || c == '\'' {
                    chars.next();
                } else {
                    break;
                }
            }
            let Some((_, sep)) = chars.next() else {
                search_from = after_key;
                continue;
            };
            if sep != '=' && sep != ':' {
                search_from = after_key;
                continue;
            }
            while let Some(&(_, c)) = chars.peek() {
                if c == ' ' || c == '\t' {
                    chars.next();
                } else {
                    break;
                }
            }
            let val_rel_start = chars.peek().map(|&(i, _)| i).unwrap_or(tail.len());
            let val_start = after_key + val_rel_start;
            if val_start >= sanitized.len() {
                break;
            }
            let rest = &sanitized[val_start..];
            let (replace_start, replace_end) =
                if let Some(quote) = rest.chars().next().filter(|&c| c == '"' || c == '\'') {
                    let inner = &rest[quote.len_utf8()..];
                    let end_rel = inner.find(quote).unwrap_or(inner.len());
                    (
                        val_start + quote.len_utf8(),
                        val_start + quote.len_utf8() + end_rel,
                    )
                } else {
                    let end_rel = rest
                        .find(|c: char| {
                            c.is_whitespace()
                                || matches!(c, ',' | ';' | ')' | '}' | ']' | '"' | '\'')
                        })
                        .unwrap_or(rest.len());
                    (val_start, val_start + end_rel)
                };
            let current_val = &sanitized[replace_start..replace_end];
            if current_val != "[REDACTED]" && !current_val.is_empty() {
                sanitized.replace_range(replace_start..replace_end, "[REDACTED]");
                search_from = replace_start + "[REDACTED]".len();
            } else {
                search_from = replace_end.max(after_key);
            }
        }
    }
    sanitized
}

/// Scrub any sensitive keys or inline credential literals from a JSON value before logging (`R3-B2`, `R3-B5`, `R4-N5`, `R4-QC`).
pub fn sanitize_telemetry_value(val: &mut Value) {
    match val {
        Value::Object(map) => {
            let paired_sensitive = map
                .get("config_key")
                .or_else(|| map.get("key"))
                .and_then(Value::as_str)
                .map(is_sensitive_config_key)
                .unwrap_or(false);
            for (k, v) in map.iter_mut() {
                if is_sensitive_config_key(k)
                    || (paired_sensitive
                        && matches!(
                            k.as_str(),
                            "config_value" | "value" | "val" | "new_value" | "old_value"
                        ))
                {
                    *v = Value::String("[REDACTED]".to_string());
                } else {
                    sanitize_telemetry_value(v);
                }
            }
        }
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                sanitize_telemetry_value(item);
            }
        }
        Value::String(s) => {
            *s = sanitize_telemetry_message(s);
        }
        _ => {}
    }
}

/// Return a sanitized copy of optional telemetry JSON details (`R4-QC`).
pub fn sanitize_telemetry_details(details: Option<Value>) -> Option<Value> {
    details.map(|mut v| {
        sanitize_telemetry_value(&mut v);
        v
    })
}

/// Verify SHA-256 checksum of the historical backfill dataset (`data/real_4h_backfill.json`) (`R3-H20`).
pub fn verify_data_provenance(backfill_path: impl AsRef<Path>) -> Result<bool> {
    let file_path = backfill_path.as_ref();
    let hash_path = PathBuf::from(format!("{}.sha256", file_path.display()));

    if !file_path.exists() || !hash_path.exists() {
        return Ok(false);
    }

    let hash_text = fs::read_to_string(&hash_path)?;
    let expected_hash = hash_text
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if expected_hash.is_empty() {
        return Ok(false);
    }
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

/// Round `value` down (or nearest if `round_down == false`) to exchange lot or tick step size (`R3-H11`).
#[inline]
pub fn round_step(value: f64, step: f64, round_down: bool) -> f64 {
    if step <= 0.0 || !value.is_finite() || !step.is_finite() {
        return value;
    }
    let ratio = value / step;
    let stepped = if round_down {
        let k_floor = ratio.floor();
        let k_next = k_floor + 1.0;
        let ulp_tol = 4.0 * f64::EPSILON * k_next.abs().max(1.0);
        let k = if (k_next - ratio) <= ulp_tol && round_dp(k_next * step, 12) <= value {
            k_next
        } else {
            k_floor
        };
        k * step
    } else {
        ratio.round() * step
    };
    round_dp(stepped, 10)
}

/// Typed configuration dictionary loaded from `system_config`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfigMap {
    pub values: HashMap<String, Value>,
}

impl ConfigMap {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    pub fn insert(&mut self, k: impl Into<String>, v: Value) {
        self.values.insert(k.into(), v);
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.values.get(key)
    }

    pub fn get_str(&self, key: &str, default: &str) -> String {
        match self.values.get(key) {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Bool(b)) => b.to_string(),
            Some(Value::Number(n)) => n.to_string(),
            _ => default.to_string(),
        }
    }

    pub fn get_f64(&self, key: &str, default: f64) -> f64 {
        match self.values.get(key) {
            Some(Value::Number(n)) => n.as_f64().unwrap_or(default),
            Some(Value::String(s)) => s.trim().parse::<f64>().unwrap_or(default),
            _ => default,
        }
    }

    pub fn get_i64(&self, key: &str, default: i64) -> i64 {
        match self.values.get(key) {
            Some(Value::Number(n)) => n
                .as_i64()
                .or_else(|| n.as_f64().map(|f| f as i64))
                .unwrap_or(default),
            Some(Value::String(s)) => s
                .trim()
                .parse::<f64>()
                .map(|f| f as i64)
                .unwrap_or(default),
            _ => default,
        }
    }

    pub fn get_usize(&self, key: &str, default: usize) -> usize {
        self.get_i64(key, default as i64).max(0) as usize
    }

    pub fn get_bool(&self, key: &str, default: bool) -> bool {
        match self.values.get(key) {
            Some(Value::Bool(b)) => *b,
            Some(Value::String(s)) => matches!(
                s.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "on" | "testnet"
            ),
            Some(Value::Number(n)) => n.as_i64().unwrap_or(0) != 0,
            _ => default,
        }
    }
}

/// Shared basis-harvest allocation and yield accrual helper (`R3-H6`).
/// Returns `(basis_notional_usdc, yield_delta_usdc)`.
pub fn compute_basis_harvest_accrual(
    available_cash_usdc: f64,
    alloc_pct: f64,
    basis_floor: f64,
    basis_rates_hourly: &[f64],
    accrue_yield: bool,
) -> (f64, f64) {
    if available_cash_usdc <= 0.0 || basis_rates_hourly.is_empty() {
        return (0.0, 0.0);
    }
    let eff_alloc = alloc_pct.clamp(0.0, 1.0);
    let basis_notional = round_dp(available_cash_usdc * eff_alloc, 2);
    if !accrue_yield {
        return (basis_notional, 0.0);
    }
    let avg_funding =
        (basis_rates_hourly.iter().sum::<f64>() / (basis_rates_hourly.len() as f64)).max(basis_floor);
    let yield_delta = round_dp(basis_notional * avg_funding * 4.0, 4);
    (basis_notional, yield_delta)
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

/// Pure stateless Average True Range (ATR) over `period` bars (`R3-N13`).
pub fn compute_atr_from_bars(bars: &[OhlcvBar], period: usize) -> f64 {
    if bars.len() < 2 {
        if let Some(last) = bars.last() {
            return (last.high - last.low).max(last.close * 0.005).max(1e-8);
        }
        return 0.0;
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
    let current_close = bars.last().map(|b| b.close).unwrap_or(0.0);
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

/// Normalizes an optional operator-supplied manual trade/cell exit reason (`R7-N6`, `R8-N6`).
///
/// Trims leading/trailing whitespace, caps length at 64 Unicode scalar values,
/// and falls back to `"MANUAL_TAKE_PROFIT"` when `None` or blank.
pub fn normalize_manual_exit_reason(reason: Option<&str>) -> String {
    reason
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(64).collect())
        .unwrap_or_else(|| "MANUAL_TAKE_PROFIT".to_string())
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
    fn test_mask_secret_and_identifier() {
        assert_eq!(
            mask_secret("0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"),
            "0x4c08...2318"
        );
        assert_eq!(mask_secret("short"), "********");
        assert_eq!(mask_secret(""), "");
        // Multi-byte UTF-8 character safety (R3-H14)
        assert_eq!(mask_secret("🔑🔑🔑🔑🔑🔑abcdef🔒🔒🔒🔒"), "🔑🔑🔑🔑🔑🔑...🔒🔒🔒🔒");
        assert_eq!(mask_identifier("🔑🔑🔑🔑🔑🔑🔑"), "🔑🔑🔑...🔑🔑🔑");

        assert_eq!(mask_identifier("-100123456789"), "-10...789");
        assert_eq!(mask_identifier("123456"), "12***56");
        assert_eq!(mask_identifier("1234"), "****");
        assert_eq!(mask_identifier(""), "");
    }

    #[test]
    fn test_round_step_ulp_and_floor_precision() {
        // Exact decimal representation 0.3 / 0.1 = 2.9999999999999996 should stay 0.3
        assert!((round_step(0.3, 0.1, true) - 0.3).abs() < 1e-12);
        // Values strictly below a step boundary must never round up on round_down = true (R3-H11)
        assert!((round_step(49999.99999999999, 1.0, true) - 49999.0).abs() < 1e-12);
        assert!((round_step(0.29999999999995, 0.1, true) - 0.2).abs() < 1e-12);
        assert!((round_step(0.29999999999995, 0.1, false) - 0.3).abs() < 1e-12);
    }

    #[test]
    fn test_newtype_arithmetic_and_inverted_clamp_safety() {
        let a = Usdc(150.0);
        let b = Usdc(50.0);
        assert_eq!(a + b, Usdc(200.0));
        assert_eq!(a - b, Usdc(100.0));
        // Inverted clamp bounds should not panic (R3-H13 / R3-QA)
        let clamped = Usdc(120.0).clamp(Usdc(200.0), Usdc(50.0));
        assert_eq!(clamped, Usdc(120.0));
        assert_eq!(default_reference_price(), 0.0);
    }

    #[test]
    fn test_pond_cell_fsm_and_order_fsm_transitions() {
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
        assert_eq!(PondCellState::ALL.len(), 10);

        use OrderFsmState::*;
        assert!(Pending.can_transition_to(OpenOnBook));
        assert!(Pending.can_transition_to(Filled));
        assert!(OpenOnBook.can_transition_to(PartiallyFilled));
        assert!(OpenOnBook.can_transition_to(Filled));
        assert!(OpenOnBook.can_transition_to(Cancelled));
        assert!(!Filled.can_transition_to(OpenOnBook));
        assert!(!Cancelled.can_transition_to(Filled));
        assert!(!OrderFsmState::Rejected.can_transition_to(Filled));
        assert!(Filled.is_terminal());
        assert_eq!(OrderFsmState::ALL.len(), 7);

        let mut series = vec![0.0001; 167];
        series.push(0.0005);
        let (z, mean, std) = compute_trailing_z_score(&series, 168);
        assert!(z > 2.0);
        assert!(mean > 0.0);
        assert!(std > 0.0);
    }

    #[test]
    fn test_iso8601_utc_and_immutable_keys() {
        assert_eq!(
            normalize_timestamp_iso8601_utc("2026-11-06 14:30:00"),
            "2026-11-06T14:30:00Z"
        );
        assert_eq!(
            normalize_timestamp_iso8601_utc("2026-11-06T14:30:00Z"),
            "2026-11-06T14:30:00Z"
        );
        assert!(is_immutable_security_config_key("web_auth_enabled"));
        assert!(is_immutable_security_config_key("telegram_api_base_url"));
        assert!(is_immutable_security_config_key("hyperliquid_info_url_override"));
        assert!(is_immutable_security_config_key("pit_subaccount_address"));

        assert_eq!(normalize_manual_exit_reason(None), "MANUAL_TAKE_PROFIT");
        assert_eq!(
            normalize_manual_exit_reason(Some("   ")),
            "MANUAL_TAKE_PROFIT"
        );
        assert_eq!(
            normalize_manual_exit_reason(Some("  OP_CLOSE  ")),
            "OP_CLOSE"
        );
        let long_reason = "X".repeat(100);
        assert_eq!(
            normalize_manual_exit_reason(Some(&long_reason)).chars().count(),
            64
        );
    }

    #[test]
    fn test_r4_n5_qc_qa_n13_telemetry_sanitization_book_transfer_status_and_timeframe() {
        // R4-N5: Unrelated `value=enabled` must NOT be redacted when a sensitive key name appears in prose
        let prose = sanitize_telemetry_message("telegram_bot_token rotated; value=enabled");
        assert_eq!(prose, "telegram_bot_token rotated; value=enabled");

        // Paired config_key=private_key config_value=... MUST redact config_value
        let paired = sanitize_telemetry_message(
            "Updated config_key=private_key config_value=0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
        );
        assert_eq!(
            paired,
            "Updated config_key=private_key config_value=[REDACTED]"
        );

        // JSON details sanitization
        let details = sanitize_telemetry_details(Some(serde_json::json!({
            "config_key": "telegram_bot_token",
            "config_value": "123456:SECRET",
            "note": "telegram_bot_token rotated; value=enabled"
        })))
        .unwrap();
        assert_eq!(details["config_value"], "[REDACTED]");
        assert_eq!(details["note"], "telegram_bot_token rotated; value=enabled");

        // R4-QA: BookTransferStatus parsing, ledger counting, and reconciliation flags
        assert_eq!(
            BookTransferStatus::parse("PARTIAL").unwrap(),
            BookTransferStatus::Partial
        );
        assert_eq!(
            BookTransferStatus::parse("AMBIGUOUS").unwrap(),
            BookTransferStatus::Ambiguous
        );
        assert!(BookTransferStatus::Partial.needs_manual_reconciliation());
        assert!(BookTransferStatus::Ambiguous.needs_manual_reconciliation());
        assert!(!BookTransferStatus::Partial.is_ledger_counted());
        assert!(!BookTransferStatus::Ambiguous.is_ledger_counted());
        assert!(BookTransferStatus::Confirmed.is_ledger_counted());
        assert!(BookTransferStatus::Completed.is_ledger_counted());
        assert!(BookTransferStatus::PartialIdleCapped.is_ledger_counted());

        // R3-N13: Strict timeframe parsing and CSV validation
        assert_eq!(parse_timeframe_seconds("4h").unwrap(), 14400);
        assert!(parse_timeframe_seconds("7h").is_err());
        assert_eq!(validate_timeframe_csv("1h, 2h, 4H").unwrap(), "1h,2h,4h");
        assert!(validate_timeframe_csv("1h,invalid").is_err());
    }

    #[test]
    fn test_r5_qa_usdc_price_quantity_finiteness_and_rounding() {
        // R6-N2: Finiteness guards on Usdc, Price, Quantity, Leverage, Bps
        #[cfg(debug_assertions)]
        {
            assert!(std::panic::catch_unwind(|| Usdc::new(f64::NAN)).is_err());
            assert!(std::panic::catch_unwind(|| Usdc::new(f64::INFINITY)).is_err());
            assert!(std::panic::catch_unwind(|| Usdc::new(f64::NEG_INFINITY)).is_err());
            assert!(std::panic::catch_unwind(|| Usdc::new_clamped(f64::NAN)).is_err());
            assert!(std::panic::catch_unwind(|| Price::new(f64::NAN)).is_err());
            assert!(std::panic::catch_unwind(|| Price::new(f64::INFINITY)).is_err());
            assert!(std::panic::catch_unwind(|| Price::new_clamped(f64::NAN)).is_err());
            assert!(std::panic::catch_unwind(|| Quantity::new(f64::NAN)).is_err());
            assert!(std::panic::catch_unwind(|| Quantity::new(f64::NEG_INFINITY)).is_err());
            assert!(std::panic::catch_unwind(|| Quantity::new_clamped(f64::NAN)).is_err());
            assert!(std::panic::catch_unwind(|| Leverage::new(f64::NAN)).is_err());
            assert!(std::panic::catch_unwind(|| Leverage::new_clamped(f64::NAN)).is_err());
            assert!(std::panic::catch_unwind(|| Bps::new(f64::NAN)).is_err());
            assert!(std::panic::catch_unwind(|| Bps::from_fraction(f64::NAN)).is_err());
        }

        #[cfg(not(debug_assertions))]
        {
            assert_eq!(Usdc::new(f64::NAN), Usdc::ZERO);
            assert_eq!(Usdc::new(f64::INFINITY), Usdc::ZERO);
            assert_eq!(Usdc::new(f64::NEG_INFINITY), Usdc::ZERO);
            assert_eq!(Usdc::new_clamped(f64::NAN), Usdc::ZERO);
            assert_eq!(Price::new(f64::NAN), Price::ZERO);
            assert_eq!(Price::new(f64::INFINITY), Price::ZERO);
            assert_eq!(Price::new_clamped(f64::NAN), Price::ZERO);
            assert_eq!(Quantity::new(f64::NAN), Quantity::ZERO);
            assert_eq!(Quantity::new(f64::NEG_INFINITY), Quantity::ZERO);
            assert_eq!(Quantity::new_clamped(f64::NAN), Quantity::ZERO);
            assert_eq!(Leverage::new(f64::NAN), Leverage::ZERO);
            assert_eq!(Leverage::new_clamped(f64::NAN), Leverage::ZERO);
            assert_eq!(Bps::new(f64::NAN), Bps::ZERO);
            assert_eq!(Bps::from_fraction(f64::NAN), Bps::ZERO);
        }

        // 4-decimal rounding in Usdc::new, Add, Sub, AddAssign, SubAssign, Mul, Div
        let a = Usdc::new(0.1);
        let b = Usdc::new(0.2);
        let sum = a + b;
        assert_eq!(sum.as_f64(), 0.3);
        let diff = sum - Usdc::new(0.1);
        assert_eq!(diff.as_f64(), 0.2);

        let mut acc = Usdc::new(100.123456);
        assert_eq!(acc.as_f64(), 100.1235);
        acc += Usdc::new(0.000049);
        assert_eq!(acc.as_f64(), 100.1235);
        acc -= Usdc::new(0.1235);
        assert_eq!(acc.as_f64(), 100.0);
        assert_eq!((acc / 0.0).as_f64(), 0.0);
        assert_eq!((acc / f64::NAN).as_f64(), 0.0);
    }
}


