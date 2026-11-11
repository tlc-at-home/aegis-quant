//! Aegis v0.3.0-rc1 Execution Engine (`crates/aegis-execution`).
//!
//! Modularized into:
//! - [`signing`]: Hyperliquid L1 EIP-712 / MessagePack / secp256k1 action signing & wire formatting
//! - [`http_client`]: HTTPS endpoint validation (`NH-2`) and shared blocking JSON HTTP helper (`NQ-2`)
//! - [`ingestion`]: Hyperliquid `candleSnapshot`, `metaAndAssetCtxs`, `fundingHistory` ingestion & synthetic seeding
//! - [`live_dispatch`]: Live Hyperliquid order submission, resting order status by `oid` (`NH-6`), and subaccount transfer (`NB-1`, `NM-6`)
//! - [`manual_close`]: Manual take-profit/close helpers, exchange absence confirmation, and cached `UniverseAsset` resolution (`R7-S3`, `R7-S4`, `R8-N2`, `R8-N3`, `R9-QB`)
//! - [`resting_reconcile`]: Hourly funding sync (`NM-4`) and resting PIT order reconciliation (`NH-4`, `R9-N1`, `R9-N3`, `R9-QB`)
//! - [`reconcile`]: Single-wallet exchange reconciliation (`NM-5`, `NM-10`, `R9-S1`)
//! - [`cycle`]: Book A (`PIT`) execution & portfolio management cycle (`NH-4`, `NH-8`, `NH-10`, `R10-QB`)

pub mod cycle;
pub mod http_client;
pub mod ingestion;
pub mod live_dispatch;
pub mod manual_close;
pub mod reconcile;
pub mod resting_reconcile;
pub mod signing;

use std::collections::HashMap;
use std::sync::Arc;

use aegis_alerts::AlertEngine;
pub use aegis_core::OrderExecutionResult;
use aegis_core::{round_dp, UniverseAsset};
use aegis_db::{ConfigMap, DatabaseManager};
use aegis_risk::RiskEngine;
use chrono::Utc;
use parking_lot::Mutex;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use http_client::{
    shared_blocking_client, HYPERLIQUID_EXCHANGE_URL, HYPERLIQUID_INFO_URL,
    HYPERLIQUID_TESTNET_EXCHANGE_URL, HYPERLIQUID_TESTNET_INFO_URL,
};
pub use ingestion::MarketIngestionEngine;
pub use live_dispatch::{
    parse_hyperliquid_order_ack, HL_NO_MATCH_RETRY_SLIP_PCT, HL_ORACLE_BAND_PCT,
    HL_ORACLE_RETRY_SLIP_PCT,
};
pub use signing::*;

/// Status of an order resting on the book (`OPEN_ON_BOOK`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RestingOrderStatus {
    pub state: String,
    pub filled_units: f64,
    pub entry_price: f64,
    #[serde(default)]
    pub fee_usdc: f64,
}

/// Typed status for `SubaccountTransferResult` (`R3-QB`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SubaccountTransferStatus {
    Executed,
    Confirmed,
    DryRunSimulated,
}

impl SubaccountTransferStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Executed => "EXECUTED",
            Self::Confirmed => "CONFIRMED",
            Self::DryRunSimulated => "DRY_RUN_SIMULATED",
        }
    }
}

impl std::fmt::Display for SubaccountTransferStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for SubaccountTransferStatus {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other.trim())
    }
}

impl PartialEq<String> for SubaccountTransferStatus {
    fn eq(&self, other: &String) -> bool {
        self.as_str().eq_ignore_ascii_case(other.trim())
    }
}

/// Typed result of a Hyperliquid L1 or virtual-ledger inter-book transfer (`R3-B3`, `R3-QB`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubaccountTransferResult {
    pub status: SubaccountTransferStatus,
    pub transfer_mode: String,
    pub from_book: String,
    pub to_book: String,
    pub amount_usdc: f64,
    pub tx_hash: String,
    pub leg_tx_hashes: Vec<String>,
    pub legs_executed: usize,
    pub dry_run: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange_response: Option<serde_json::Value>,
}

/// Typed status for `VaultTransferResult` (`R3-QB`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum VaultTransferStatus {
    Simulated,
    Submitted,
    Skipped,
    Error,
}

impl VaultTransferStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Simulated => "SIMULATED",
            Self::Submitted => "SUBMITTED",
            Self::Skipped => "SKIPPED",
            Self::Error => "ERROR",
        }
    }
}

impl std::fmt::Display for VaultTransferStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for VaultTransferStatus {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other.trim())
    }
}

impl PartialEq<String> for VaultTransferStatus {
    fn eq(&self, other: &String) -> bool {
        self.as_str().eq_ignore_ascii_case(other.trim())
    }
}

/// Typed result of an HLP Vault deposit or withdrawal (`R3-QB`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VaultTransferResult {
    pub status: VaultTransferStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    pub vault_address: String,
    pub is_deposit: bool,
    pub amount_usdc: f64,
    pub usd_micro: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_streak: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange_response: Option<serde_json::Value>,
}

/// Typed status for `WalletEquitySyncResult` (`R3-QB`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WalletEquitySyncStatus {
    PaperDefault,
    SyncedOnChain,
    UnreachableFallback,
}

impl WalletEquitySyncStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PaperDefault => "PAPER_DEFAULT",
            Self::SyncedOnChain => "SYNCED_ON_CHAIN",
            Self::UnreachableFallback => "UNREACHABLE_FALLBACK",
        }
    }
}

impl std::fmt::Display for WalletEquitySyncStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for WalletEquitySyncStatus {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other.trim())
    }
}

impl PartialEq<String> for WalletEquitySyncStatus {
    fn eq(&self, other: &String) -> bool {
        self.as_str().eq_ignore_ascii_case(other.trim())
    }
}

/// Typed result of on-chain wallet equity synchronization (`R3-QB`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WalletEquitySyncResult {
    pub status: WalletEquitySyncStatus,
    pub wallet_address: String,
    pub network: String,
    pub account_type: String,
    pub wallet_balance_usdc: f64,
    pub total_equity_usdc: f64,
    pub perp_account_value_usdc: f64,
    pub withdrawable_usdc: f64,
    pub vault_equity_usdc: f64,
}

/// Typed status for `ReconcileReport` (`R3-QB`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReconcileStatus {
    Reconciled,
    VerifiedPaper,
    SkippedOffline,
    ErrorDbUnavailable,
}

impl ReconcileStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Reconciled => "RECONCILED",
            Self::VerifiedPaper => "VERIFIED_PAPER",
            Self::SkippedOffline => "SKIPPED_OFFLINE",
            Self::ErrorDbUnavailable => "ERROR_DB_UNAVAILABLE",
        }
    }
}

impl std::fmt::Display for ReconcileStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for ReconcileStatus {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other.trim())
    }
}

impl PartialEq<String> for ReconcileStatus {
    fn eq(&self, other: &String) -> bool {
        self.as_str().eq_ignore_ascii_case(other.trim())
    }
}

/// Typed report of exchange clearinghouse state reconciliation (`R3-QB`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReconcileReport {
    pub status: ReconcileStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_open_trades: Option<usize>,
    pub discrepancies_found: usize,
    pub discrepancies: Vec<serde_json::Value>,
}

/// Typed status for `FundingSyncReport` (`R3-QB`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FundingSyncStatus {
    Synced,
    AlreadySyncedThisHour,
    NoOpenPositions,
    Error,
}

impl FundingSyncStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Synced => "SYNCED",
            Self::AlreadySyncedThisHour => "ALREADY_SYNCED_THIS_HOUR",
            Self::NoOpenPositions => "NO_OPEN_POSITIONS",
            Self::Error => "ERROR",
        }
    }
}

impl std::fmt::Display for FundingSyncStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for FundingSyncStatus {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other.trim())
    }
}

impl PartialEq<String> for FundingSyncStatus {
    fn eq(&self, other: &String) -> bool {
        self.as_str().eq_ignore_ascii_case(other.trim())
    }
}

/// Typed report of hourly funding payment synchronization (`R3-QB`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FundingSyncReport {
    pub status: FundingSyncStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub hour_bucket_ms: i64,
    pub positions_updated: usize,
    pub net_funding_delta_usdc: f64,
}

pub type AssetIndexCache = Arc<Mutex<HashMap<(bool, String), (u32, i64)>>>;
pub type PendingFix4TrueupMap = Arc<Mutex<HashMap<(bool, i64), (String, f64)>>>;

/// Infers a price-scaled fallback tick size for symbols not present in `universe_assets` or `pond_universe` (`R7-N5`).
pub fn infer_synthetic_tick_size(ref_price: f64) -> f64 {
    if ref_price >= 1000.0 {
        0.1
    } else if ref_price >= 10.0 {
        0.01
    } else if ref_price >= 1.0 {
        0.0001
    } else {
        0.00001
    }
}

/// Builds a synthetic [`UniverseAsset`] for a Pond or reconciled orphan symbol (`R7-N3`, `R7-N5`).
/// Preserves positive `min_notional_usdc` from `pond_universe` without overriding values `< 10.0`.
pub fn build_orphan_or_pond_universe_asset(
    symbol: &str,
    base_asset: &str,
    ref_price: f64,
    pond_row: Option<&aegis_core::PondUniverseRow>,
    tier_name: &str,
) -> UniverseAsset {
    let base = if base_asset.trim().is_empty() {
        symbol.split('/').next().unwrap_or(symbol).trim().to_string()
    } else {
        base_asset.trim().to_string()
    };
    let inferred_tick = infer_synthetic_tick_size(ref_price);
    let tick_size = pond_row
        .map(|p| p.tick_size)
        .filter(|&t| t > 0.0)
        .unwrap_or(inferred_tick)
        .max(1e-8);
    let lot_step_size = pond_row
        .map(|p| p.lot_step_size)
        .filter(|&l| l > 0.0)
        .unwrap_or(0.001)
        .max(1e-8);
    let min_notional_usdc = pond_row
        .map(|p| p.min_notional_usdc)
        .filter(|&n| n > 0.0)
        .unwrap_or(10.0);
    let max_leverage = pond_row
        .map(|p| p.max_leverage)
        .filter(|&m| m > 0.0)
        .unwrap_or(10.0)
        .max(1.0);
    let reference_price = pond_row
        .map(|p| p.reference_price)
        .filter(|&p| p > 0.0)
        .unwrap_or_else(|| ref_price.max(0.01));

    UniverseAsset {
        asset_id: 0,
        symbol: symbol.to_string(),
        base_asset: base,
        quote_asset: "USDC".to_string(),
        instrument_type: "USDC_PERP".to_string(),
        tier: 3,
        tier_name: tier_name.to_string(),
        exec_timeframe: "4h".to_string(),
        rs_timeframe: "4h".to_string(),
        tick_size,
        lot_step_size,
        min_notional_usdc,
        max_leverage,
        is_active: 1,
        updated_at: String::new(),
        reference_price,
    }
}

#[derive(Clone)]
pub struct ExecutionEngine {
    pub db: DatabaseManager,
    pub risk: RiskEngine,
    pub alerts: AlertEngine,
    pub timeout_sec: u64,
    pub(crate) http_client: Client,
    pub(crate) client_timeout_sec: u64,
    pub(crate) last_funding_sync_hour_ts: Arc<Mutex<Option<i64>>>,
    pub(crate) stop_cooldown_until_ms: Arc<Mutex<HashMap<String, i64>>>,
    pub(crate) hlp_vault_error_streak: Arc<Mutex<usize>>,
    /// Dynamic cache of `(is_testnet, COIN_UPPER) -> (asset_index, cached_at_ms)` (`NM-12`).
    pub asset_index_cache: AssetIndexCache,
    /// In-memory cache of `symbol -> (UniverseAsset, cached_at_ms)` for Pit, Pond, and reconciled orphan symbols (`R7-N3`, `R8-N2`).
    pub resolved_asset_cache: Arc<Mutex<HashMap<String, (UniverseAsset, i64)>>>,
    /// Pending `userFills` true-up queue `(is_pond_cell, row_id) -> (order_id, estimated_fee_usdc)`
    /// for rows race-promoted via `clearinghouseState` during indexer lag (`R13-N1`, `R13-N4(b)`).
    pub(crate) pending_fix4_fill_trueups: PendingFix4TrueupMap,
}

impl ExecutionEngine {
    pub fn new(db: DatabaseManager) -> Self {
        Self::with_timeout(db, 6)
    }

    pub fn with_timeout(db: DatabaseManager, timeout_sec: u64) -> Self {
        let risk = RiskEngine::new(db.clone());
        let alerts = AlertEngine::new(db.clone());
        let t_sec = timeout_sec.max(1);
        let http_client = shared_blocking_client(t_sec);
        Self {
            db,
            risk,
            alerts,
            timeout_sec: t_sec,
            http_client,
            client_timeout_sec: t_sec,
            last_funding_sync_hour_ts: Arc::new(Mutex::new(None)),
            stop_cooldown_until_ms: Arc::new(Mutex::new(HashMap::new())),
            hlp_vault_error_streak: Arc::new(Mutex::new(0)),
            asset_index_cache: Arc::new(Mutex::new(HashMap::new())),
            resolved_asset_cache: Arc::new(Mutex::new(HashMap::new())),
            pending_fix4_fill_trueups: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn load_config_logged(&self, caller: &str) -> ConfigMap {
        match self.db.load_config() {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::error!(caller = caller, error = %e, "Failed to load system_config in ExecutionEngine");
                ConfigMap::default()
            }
        }
    }

    /// Resolves the HLP Vault EVM address from `hyperliquid_vault_address` in `system_config` (`HP`).
    pub fn resolve_hlp_vault_address(cfg: &ConfigMap, is_testnet: bool) -> String {
        let explicit = cfg.get_str("hyperliquid_vault_address", "");
        let testnet_default = aegis_core::DEFAULT_TESTNET_HLP_VAULT_ADDRESS.to_string();
        let mainnet_default = aegis_core::DEFAULT_MAINNET_HLP_VAULT_ADDRESS.to_string();

        if !explicit.trim().is_empty() {
            if is_testnet && explicit.trim().eq_ignore_ascii_case(&mainnet_default) {
                tracing::warn!(
                    explicit_vault = explicit.trim(),
                    testnet_vault = %testnet_default,
                    "hyperliquid_vault_address is set to Mainnet default vault while running on Testnet; remapping to Testnet default vault"
                );
                return testnet_default;
            }
            if !is_testnet && explicit.trim().eq_ignore_ascii_case(&testnet_default) {
                tracing::warn!(
                    explicit_vault = explicit.trim(),
                    mainnet_vault = %mainnet_default,
                    "hyperliquid_vault_address is set to Testnet default vault while running on Mainnet; remapping to Mainnet default vault"
                );
                return mainnet_default;
            }
            return explicit.trim().to_string();
        }
        if is_testnet {
            testnet_default
        } else {
            mainnet_default
        }
    }

    /// Computes dynamic volume-and-volatility-adjusted slippage in basis points (`Q1`).
    #[allow(clippy::too_many_arguments)]
    pub fn calculate_dynamic_slippage_bps(
        &self,
        notional_usd: f64,
        bar_close: f64,
        bar_high: f64,
        bar_low: f64,
        bar_volume: f64,
        is_maker: bool,
        base_slippage_bps: Option<f64>,
    ) -> f64 {
        let cfg = self.load_config_logged("calculate_dynamic_slippage_bps");
        let base_bps = base_slippage_bps.unwrap_or_else(|| {
            cfg.get_f64(
                "paper_slippage_bps",
                cfg.get_f64("execution_default_base_slippage_bps", 2.0),
            )
        });
        let maker_rebate = cfg.get_f64("execution_maker_slippage_rebate_bps", -0.5);
        let impact_coeff = cfg.get_f64("execution_impact_coeff_bps", 40.0);
        let vol_coeff = cfg.get_f64("execution_vol_slippage_coeff_bps", 120.0);
        let min_slip = cfg.get_f64("execution_min_taker_slippage_bps", 0.5);
        let max_slip = cfg.get_f64("execution_max_taker_slippage_bps", 50.0);

        if is_maker {
            return maker_rebate;
        }
        let close = bar_close.max(1e-8);
        let bar_dollar_vol = (bar_volume.max(0.0) * close).max(1000.0);
        let participation = (notional_usd.max(0.0) / bar_dollar_vol).clamp(0.0, 1.0);
        let impact_bps = impact_coeff * participation.sqrt();
        let range_pct = ((bar_high - bar_low).max(0.0) / close).clamp(0.0, 0.25);
        let vol_bps = range_pct * vol_coeff;
        round_dp((base_bps + impact_bps + vol_bps).clamp(min_slip, max_slip), 2)
    }

    /// Simulates a realistic paper-trading fill with tick/lot rounding, dynamic slippage, and fees (`Q1`).
    #[allow(clippy::too_many_arguments)]
    pub fn simulate_paper_fill(
        &self,
        symbol: &str,
        side: &str,
        requested_units: f64,
        reference_price: f64,
        tick_size: f64,
        lot_step_size: f64,
        bar_high: f64,
        bar_low: f64,
        bar_volume: f64,
        is_stop_exit: bool,
        prefer_maker: bool,
        force_partial_ratio: Option<f64>,
        client_order_id: Option<&str>,
    ) -> OrderExecutionResult {
        let cfg = self.load_config_logged("simulate_paper_fill");
        let coid = client_order_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("paper-{}", Utc::now().timestamp_micros()));

        let norm_side = if side.eq_ignore_ascii_case("BUY") || side.eq_ignore_ascii_case("LONG") {
            "LONG"
        } else {
            "SHORT"
        };

        if requested_units <= 0.0 || reference_price <= 0.0 {
            return OrderExecutionResult::rejected(&coid, symbol, norm_side, requested_units);
        }

        let is_maker = prefer_maker && !is_stop_exit;
        let ratio = force_partial_ratio.unwrap_or(1.0).clamp(0.0, 1.0);
        let raw_units = requested_units * ratio;
        let filled_units = RiskEngine::round_step(raw_units, lot_step_size, true);
        if filled_units <= 0.0 {
            return OrderExecutionResult::rejected(&coid, symbol, norm_side, requested_units);
        }

        let notional = filled_units * reference_price;
        let slip_bps = self.calculate_dynamic_slippage_bps(
            notional,
            reference_price,
            bar_high,
            bar_low,
            bar_volume,
            is_maker,
            None,
        );
        let slip_mult = slip_bps / 10_000.0;
        let raw_fill_px = if norm_side == "LONG" {
            reference_price * (1.0 + slip_mult)
        } else {
            reference_price * (1.0 - slip_mult)
        };
        let avg_fill_price = RiskEngine::round_step(raw_fill_px, tick_size.max(1e-8), false);
        let slip_usd = round_dp(
            (avg_fill_price - reference_price).abs() * filled_units,
            4,
        );

        let fee_bps = if is_maker {
            cfg.get_f64("maker_fee_bps", 1.5)
        } else {
            cfg.get_f64("taker_fee_bps", 4.5)
        };
        let fees_usdc = round_dp(
            (filled_units * avg_fill_price) * (fee_bps / 10_000.0),
            4,
        );

        let mut history = vec!["CREATED".to_string(), "SUBMITTED".to_string()];
        if ratio < 0.9999 {
            history.push("PARTIALLY_FILLED".to_string());
        }
        history.push("FILLED".to_string());

        if client_order_id.is_some() {
            let _ = self.db.log_order_state_transition(
                &coid,
                "PENDING",
                "OPEN_ON_BOOK",
                Some("PAPER_SUBMITTED"),
            );
            if ratio < 0.9999 {
                let _ = self.db.log_order_state_transition(
                    &coid,
                    "OPEN_ON_BOOK",
                    "PARTIALLY_FILLED",
                    Some("PAPER_PARTIAL_FILL"),
                );
                let _ = self.db.log_order_state_transition(
                    &coid,
                    "PARTIALLY_FILLED",
                    "FILLED",
                    Some("PAPER_FILLED"),
                );
            } else {
                let _ = self.db.log_order_state_transition(
                    &coid,
                    "OPEN_ON_BOOK",
                    "FILLED",
                    Some("PAPER_FILLED"),
                );
            }
        }

        let ex_hash = hex::encode(Sha256::digest(
            format!("{coid}:{symbol}:{norm_side}:{filled_units}").as_bytes(),
        ));
        let ex_oid = format!("paper-{}", &ex_hash[..12]);

        OrderExecutionResult::filled(
            &coid,
            &ex_oid,
            symbol,
            norm_side,
            round_dp(requested_units, 6),
            filled_units,
            avg_fill_price,
            slip_usd,
            slip_bps,
            fees_usdc,
            is_maker,
            if is_stop_exit {
                "STOP_MARKET"
            } else if is_maker {
                "MAKER_LIMIT_ALO"
            } else {
                "TAKER_IOC"
            },
            history,
        )
    }

    /// Executes an order via the Maker Limit Chaser (`ALO` post-only with configurable chase ticks
    /// and IOC fallback) (`Q1`).
    #[allow(clippy::too_many_arguments)]
    pub fn execute_maker_limit_chaser(
        &self,
        asset: &UniverseAsset,
        side: &str,
        quantity: f64,
        reference_price: f64,
        bar_high: f64,
        bar_low: f64,
        bar_volume: f64,
        force_partial_ratio: Option<f64>,
        simulate_book_scenario: Option<&str>,
        client_order_id: Option<&str>,
        is_reduce_only: bool,
    ) -> OrderExecutionResult {
        let cfg = self.load_config_logged("execute_maker_limit_chaser");
        let coid = client_order_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("chaser-{}", Utc::now().timestamp_micros()));

        let norm_side = if side.eq_ignore_ascii_case("BUY") || side.eq_ignore_ascii_case("LONG") {
            "LONG"
        } else {
            "SHORT"
        };

        if let Some(scenario) = simulate_book_scenario {
            match scenario.to_uppercase().as_str() {
                "CANCELLED" => {
                    return OrderExecutionResult::cancelled(&coid, &asset.symbol, norm_side, quantity);
                }
                "EXPIRED" => {
                    return OrderExecutionResult::expired(&coid, &asset.symbol, norm_side, quantity);
                }
                "REJECTED" => {
                    return OrderExecutionResult::rejected(&coid, &asset.symbol, norm_side, quantity);
                }
                _ => {}
            }
        }

        let mode = if simulate_book_scenario == Some("PAPER") {
            "PAPER".to_string()
        } else {
            cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase()
        };

        if mode == "LIVE" || mode == "TESTNET" {
            let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
            let chase_bps = cfg.get_f64("maker_chaser_offset_bps", 1.5);
            let limit_px = if is_reduce_only {
                if norm_side == "LONG" {
                    reference_price * 1.005
                } else {
                    reference_price * 0.995
                }
            } else if norm_side == "LONG" {
                reference_price * (1.0 - chase_bps / 10_000.0)
            } else {
                reference_price * (1.0 + chase_bps / 10_000.0)
            };
            return self.submit_hyperliquid_order_live(
                asset,
                norm_side,
                quantity,
                limit_px,
                reference_price,
                !is_reduce_only,
                is_reduce_only,
                &coid,
                &cfg,
                is_testnet,
            );
        }

        self.simulate_paper_fill(
            &asset.symbol,
            norm_side,
            quantity,
            reference_price,
            asset.tick_size,
            asset.lot_step_size,
            bar_high,
            bar_low,
            bar_volume,
            false,
            true,
            force_partial_ratio,
            Some(&coid),
        )
    }

    /// Executes a Pond (Book B) order respecting `pond_execution_mode` (`PAPER` / `TESTNET` / `LIVE`).
    #[allow(clippy::too_many_arguments)]
    pub fn execute_pond_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: f64,
        reference_price: f64,
        tick_size: f64,
        lot_step_size: f64,
        bar_high: f64,
        bar_low: f64,
        bar_volume: f64,
        is_stop_exit: bool,
        prefer_maker: bool,
        is_reduce_only: bool,
        client_order_id: Option<&str>,
    ) -> OrderExecutionResult {
        let cfg = self.load_config_logged("execute_pond_order");
        let pit_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &pit_mode)
            .to_ascii_uppercase();
        let coid = client_order_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("pond-{}", Utc::now().timestamp_micros()));

        let norm_side = if side.eq_ignore_ascii_case("BUY") || side.eq_ignore_ascii_case("LONG") {
            "LONG"
        } else {
            "SHORT"
        };

        if pond_mode == "LIVE" || pond_mode == "TESTNET" {
            let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || pond_mode == "TESTNET";
            let base = symbol
                .split('/')
                .next()
                .unwrap_or(symbol)
                .trim()
                .to_string();
            let temp_asset = UniverseAsset {
                asset_id: 0,
                symbol: symbol.to_string(),
                base_asset: base,
                quote_asset: "USDC".to_string(),
                instrument_type: "USDC_PERP".to_string(),
                tier: 3,
                tier_name: "POND".to_string(),
                exec_timeframe: "4h".to_string(),
                rs_timeframe: "4h".to_string(),
                tick_size: tick_size.max(1e-8),
                lot_step_size: lot_step_size.max(1e-8),
                min_notional_usdc: 10.0,
                max_leverage: 20.0,
                is_active: 1,
                updated_at: String::new(),
                reference_price,
            };
            let limit_px = if prefer_maker && !is_stop_exit {
                let chase_bps = cfg.get_f64("maker_chaser_offset_bps", 1.5);
                if norm_side == "LONG" {
                    reference_price * (1.0 - chase_bps / 10_000.0)
                } else {
                    reference_price * (1.0 + chase_bps / 10_000.0)
                }
            } else if norm_side == "LONG" {
                reference_price * 1.005
            } else {
                reference_price * 0.995
            };
            return self.submit_hyperliquid_order_live(
                &temp_asset,
                norm_side,
                quantity,
                limit_px,
                reference_price,
                prefer_maker && !is_stop_exit,
                is_reduce_only,
                &coid,
                &cfg,
                is_testnet,
            );
        }

        self.simulate_paper_fill(
            symbol,
            norm_side,
            quantity,
            reference_price,
            tick_size,
            lot_step_size,
            bar_high,
            bar_low,
            bar_volume,
            is_stop_exit,
            prefer_maker,
            None,
            Some(&coid),
        )
    }
}


#[cfg(test)]
mod tests;
