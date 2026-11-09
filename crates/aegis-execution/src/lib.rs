//! Aegis v0.3.0-rc1 Execution Engine (`crates/aegis-execution`).
//!
//! Modularized into:
//! - [`signing`]: Hyperliquid L1 EIP-712 / MessagePack / secp256k1 action signing & wire formatting
//! - [`http_client`]: HTTPS endpoint validation (`NH-2`) and shared blocking JSON HTTP helper (`NQ-2`)
//! - [`ingestion`]: Hyperliquid `candleSnapshot`, `metaAndAssetCtxs`, `fundingHistory` ingestion & synthetic seeding
//! - [`live_dispatch`]: Live Hyperliquid order submission, resting order status by `oid` (`NH-6`), and subaccount transfer (`NB-1`, `NM-6`)
//! - [`reconcile`]: Hourly funding sync (`NM-4`), single-wallet reconciliation (`NM-5`, `NM-10`), and execution cycle (`NH-4`, `NH-8`, `NH-10`)

pub mod http_client;
pub mod ingestion;
pub mod live_dispatch;
pub mod reconcile;
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
pub use live_dispatch::parse_hyperliquid_order_ack;
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
    /// In-memory cache of `symbol -> UniverseAsset` for Pit, Pond, and reconciled orphan symbols (`R7-N3`).
    pub resolved_asset_cache: Arc<Mutex<HashMap<String, UniverseAsset>>>,
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
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_order_fsm_and_eip712_signing() {
        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        let exec = ExecutionEngine::new(db.clone());
        let assets = db.get_usdc_perp_universe().unwrap();
        let sol = assets.iter().find(|a| a.symbol == "SOL/USDC:USDC").unwrap();

        let partial = exec.execute_maker_limit_chaser(
            sol,
            "LONG",
            5.0,
            180.0,
            182.0,
            179.0,
            12_000.0,
            Some(0.5),
            None,
            Some("test-partial-1"),
            false,
        );
        assert_eq!(partial.fsm_state, "FILLED");
        assert!(partial
            .state_history
            .contains(&"PARTIALLY_FILLED".to_string()));

        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let sig = ExecutionEngine::sign_hyperliquid_action(
            pk,
            &json!({"type": "order"}),
            1700000000000,
            None,
            true,
        )
        .unwrap();
        assert!(sig["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig["signer"].as_str().unwrap().starts_with("0x"));
    }

    #[test]
    fn test_hyperliquid_python_sdk_action_hash_vector() {
        let order_action = OrderAction {
            action_type: "order".to_string(),
            orders: vec![OrderWire {
                a: 4,
                b: true,
                p: "1670.1".to_string(),
                s: "0.0147".to_string(),
                r: false,
                t: OrderTypeWire {
                    limit: Some(LimitOrderType {
                        tif: "Ioc".to_string(),
                    }),
                    trigger: None,
                },
                c: None,
            }],
            grouping: "na".to_string(),
        };

        let hash = compute_l1_action_hash(&order_action, None, 1677777606040, None).unwrap();
        assert_eq!(
            format!("0x{}", hex::encode(hash)),
            "0x0fcbeda5ae3c4950a548021552a4fea2226858c4453571bf3f24ba017eac2908"
        );
    }

    #[test]
    fn test_hyperliquid_python_sdk_l1_signing_order_vector() {
        let pk = "0x0123456789012345678901234567890123456789012345678901234567890123";
        let order_action = OrderAction {
            action_type: "order".to_string(),
            orders: vec![OrderWire {
                a: 1,
                b: true,
                p: "100".to_string(),
                s: "100".to_string(),
                r: false,
                t: OrderTypeWire {
                    limit: Some(LimitOrderType {
                        tif: "Gtc".to_string(),
                    }),
                    trigger: None,
                },
                c: None,
            }],
            grouping: "na".to_string(),
        };

        let sig_mainnet = sign_l1_action(pk, &order_action, None, 0, None, true).unwrap();
        assert_eq!(
            sig_mainnet["r"].as_str().unwrap(),
            "0xd65369825a9df5d80099e513cce430311d7d26ddf477f5b3a33d2806b100d78e"
        );
        assert_eq!(
            sig_mainnet["s"].as_str().unwrap(),
            "0x2b54116ff64054968aa237c20ca9ff68000f977c93289157748a3162b6ea940e"
        );
        assert_eq!(sig_mainnet["v"].as_u64().unwrap(), 28);

        let sig_testnet = sign_l1_action(pk, &order_action, None, 0, None, false).unwrap();
        assert_eq!(
            sig_testnet["r"].as_str().unwrap(),
            "0x82b2ba28e76b3d761093aaded1b1cdad4960b3af30212b343fb2e6cdfa4e3d54"
        );
        assert_eq!(
            sig_testnet["s"].as_str().unwrap(),
            "0x6b53878fc99d26047f4d7e8c90eb98955a109f44209163f52d8dc4278cbbd9f5"
        );
        assert_eq!(sig_testnet["v"].as_u64().unwrap(), 27);
    }

    #[test]
    fn test_vault_deposit_and_withdrawal_signing() {
        let pk = "0x0123456789012345678901234567890123456789012345678901234567890123";
        let vault_addr = "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0";

        let deposit_action = VaultTransferAction {
            action_type: "vaultTransfer".to_string(),
            vault_address: vault_addr.to_string(),
            is_deposit: true,
            usd: 100_000_000,
        };

        let msgpack_bytes = rmp_serde::encode::to_vec_named(&deposit_action).unwrap();
        assert!(!msgpack_bytes.is_empty());

        let deposit_hash =
            compute_l1_action_hash(&deposit_action, None, 1700000000000, None).unwrap();
        assert_eq!(deposit_hash.len(), 32);

        let sig_deposit_mainnet =
            sign_l1_action(pk, &deposit_action, None, 1700000000000, None, true).unwrap();
        assert!(sig_deposit_mainnet["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig_deposit_mainnet["s"].as_str().unwrap().starts_with("0x"));
        let v_dep_main = sig_deposit_mainnet["v"].as_u64().unwrap();
        assert!(v_dep_main == 27 || v_dep_main == 28);

        let sig_deposit_testnet =
            sign_l1_action(pk, &deposit_action, None, 1700000000000, None, false).unwrap();
        assert!(sig_deposit_testnet["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig_deposit_testnet["s"].as_str().unwrap().starts_with("0x"));
        let v_dep_test = sig_deposit_testnet["v"].as_u64().unwrap();
        assert!(v_dep_test == 27 || v_dep_test == 28);

        let withdraw_action = VaultTransferAction {
            action_type: "vaultTransfer".to_string(),
            vault_address: vault_addr.to_string(),
            is_deposit: false,
            usd: 50_000_000,
        };

        let withdraw_hash =
            compute_l1_action_hash(&withdraw_action, None, 1700000000000, None).unwrap();
        assert_eq!(withdraw_hash.len(), 32);
        assert_ne!(deposit_hash, withdraw_hash);

        let sig_withdraw_testnet =
            sign_l1_action(pk, &withdraw_action, None, 1700000000000, None, false).unwrap();
        assert!(sig_withdraw_testnet["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig_withdraw_testnet["s"].as_str().unwrap().starts_with("0x"));

        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        let exec = ExecutionEngine::new(db);
        let sim_deposit = exec.execute_hlp_vault_transfer(100.0, true, Some(vault_addr));
        assert_eq!(sim_deposit.status, "SIMULATED");
        assert_eq!(sim_deposit.usd_micro, 100_000_000);
        assert!(sim_deposit.is_deposit);

        let sim_withdraw = exec.execute_hlp_vault_transfer(50.0, false, Some(vault_addr));
        assert_eq!(sim_withdraw.status, "SIMULATED");
        assert_eq!(sim_withdraw.usd_micro, 50_000_000);
        assert!(!sim_withdraw.is_deposit);
    }

    #[test]
    fn test_asset_index_cache_and_fallback() {
        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        assert!(db
            .set_config_value("hyperliquid_mainnet_url", &json!("http://127.0.0.1:1"))
            .unwrap());
        assert!(db
            .set_config_value("hyperliquid_testnet_url", &json!("http://127.0.0.1:1"))
            .unwrap());
        assert!(db
            .set_config_value("hyperliquid_info_url", &json!("http://127.0.0.1:1/info"))
            .unwrap());
        assert!(db
            .set_config_value(
                "hyperliquid_testnet_info_url",
                &json!("http://127.0.0.1:1/info"),
            )
            .unwrap());
        let mut exec = ExecutionEngine::new(db.clone());
        exec.timeout_sec = 1;

        assert_eq!(
            exec.fetch_hyperliquid_universe_asset_index("ADA", true),
            Some(45)
        );
        assert_eq!(
            exec.fetch_hyperliquid_universe_asset_index("ada", true),
            Some(45)
        );
        assert_eq!(
            exec.fetch_hyperliquid_universe_asset_index("ADA", false),
            Some(65)
        );
        assert_eq!(
            exec.fetch_hyperliquid_universe_asset_index("SOL", true),
            Some(0)
        );
        assert_eq!(
            exec.fetch_hyperliquid_universe_asset_index("BTC", false),
            Some(0)
        );

        assert!(exec.asset_index_cache.lock().is_empty());
        assert!(db
            .set_config_value("execution_mode", &json!("LIVE"))
            .unwrap());
        assert_eq!(
            exec.fetch_hyperliquid_universe_asset_index("ADA", false),
            None
        );
        assert_eq!(
            exec.fetch_live_hyperliquid_universe_asset_index("BTC", false),
            None
        );
    }

    #[test]
    fn test_single_wallet_two_book_scaling_perps_cap_and_pond_reconciliation() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        let scaled = db.scale_risk_caps_for_equity(956.80).unwrap();
        assert!((scaled["initial_equity_usdc"] - 956.80).abs() < 1e-6);
        assert!((scaled["baseline_risk_per_trade_usdc"] - 4.78).abs() < 1e-6);
        assert!((scaled["min_risk_per_trade_usdc"] - 2.39).abs() < 1e-6);
        assert!((scaled["max_risk_per_trade_usdc"] - 7.17).abs() < 1e-6);
        assert!((scaled["max_portfolio_heat_usdc"] - 19.14).abs() < 1e-6);
        assert!((scaled["max_single_asset_notional_usdc"] - 119.60).abs() < 1e-6);
        assert!((scaled["max_gross_leverage_notional_usdc"] - 717.60).abs() < 1e-6);
        assert!((scaled["pond_baseline_risk_usdc"] - 4.78).abs() < 1e-6);
        assert!((scaled["pond_min_risk_usdc"] - 2.39).abs() < 1e-6);
        assert!((scaled["pond_max_risk_usdc"] - 7.17).abs() < 1e-6);
        assert!((scaled["pond_max_heat_usdc"] - 19.14).abs() < 1e-6);
        assert!((scaled["pond_max_cell_notional_usdc"] - 119.60).abs() < 1e-6);

        db.set_config_value("execution_mode", &json!("TESTNET"))
            .unwrap();
        db.set_config_value("pond_execution_mode", &json!("TESTNET"))
            .unwrap();
        let mut snap = db.get_latest_portfolio_snapshot().unwrap();
        snap.total_equity_usdc = 956.80;
        snap.basis_harvest_notional_usdc = 429.93;
        snap.available_cash_usdc = 526.87;
        db.save_portfolio_snapshot(&snap).unwrap();

        let (tot_eq, perps_eq, vault_eq, deployed) = db.get_wallet_perps_margin_state().unwrap();
        assert!((tot_eq - 956.80).abs() < 1e-6);
        assert!((perps_eq - 526.87).abs() < 1e-6);
        assert!((vault_eq - 429.93).abs() < 1e-6);
        assert!((deployed - 0.0).abs() < 1e-6);

        let phantom_cell = aegis_core::PondCell {
            cell_id: 0,
            symbol: "JUP/USDC:USDC".to_string(),
            champion_id: 1,
            state: "OPEN".to_string(),
            cycle_opened_id: None,
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(1.0),
            quantity: Some(100.0),
            notional_usdc: Some(100.0),
            risk_budget_usdc: Some(4.78),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.5),
            funding_rate_entry: Some(0.0005),
            initial_stop: Some(1.1),
            chandelier_stop: Some(1.1),
            highest_price_seen: Some(1.0),
            lowest_price_seen: Some(1.0),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            exit_price: None,
            exit_reason: None,
            resting_order_id: None,
            opened_at: Some("2026-11-05T00:00:00Z".to_string()),
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-11-05T00:00:00Z".to_string(),
        };
        db.upsert_pond_cell(&phantom_cell).unwrap();
        assert!(db
            .get_all_open_wallet_symbols()
            .unwrap()
            .contains("JUP/USDC:USDC"));

        let exec = ExecutionEngine::new(db.clone());
        let empty_on_chain = HashMap::new();

        let resting_orders = vec![json!({"coin": "JUP", "oid": 99999})];
        let recon_resting =
            exec.reconcile_exchange_state(Some(&empty_on_chain), Some(&resting_orders));
        assert_eq!(recon_resting.status, "RECONCILED");
        assert_eq!(recon_resting.discrepancies_found, 0);
        assert_eq!(db.get_open_pond_cells().unwrap().len(), 1);

        let recon = exec.reconcile_exchange_state(Some(&empty_on_chain), None);
        assert_eq!(recon.status, "RECONCILED");
        assert_eq!(recon.discrepancies_found, 1);
        assert_eq!(db.get_open_pond_cells().unwrap().len(), 0);
        let closed_cells = db.get_pond_cells(10).unwrap();
        assert_eq!(closed_cells[0].state, "CLOSED");
        assert_eq!(
            closed_cells[0].exit_reason.as_deref(),
            Some("RECONCILED_CLOSED_ON_EXCHANGE")
        );

        let risk = RiskEngine::new(db.clone());
        let (budget, mult) = risk.compute_conviction_risk_budget(0.5, 478.40, None);
        assert!((budget - 4.78).abs() < 1e-6);
        assert!((mult - 1.0).abs() < 1e-6);

        let fill = exec.execute_pond_order(
            "JUP/USDC:USDC",
            "SELL",
            100.0,
            1.0,
            0.0001,
            0.1,
            1.01,
            0.99,
            50_000.0,
            false,
            true,
            false,
            None,
        );
        assert_eq!(fill.fsm_state, "REJECTED");
    }

    #[test]
    fn test_hyperliquid_order_ack_parsing_and_wire_formatting_nh7_nh9_nm9() {
        // 1. Missing avgPx/totalSz in filled response must NOT fabricate FILLED (NH-7)
        let missing_fill_resp = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        { "filled": { "oid": 777 } }
                    ]
                }
            }
        });
        let res_missing = parse_hyperliquid_order_ack(
            &missing_fill_resp,
            "SOL/USDC:USDC",
            "LONG",
            5.0,
            180.0,
            true,
            1.5,
            "coid-1",
        );
        assert_eq!(res_missing.fsm_state, "OPEN_ON_BOOK");
        assert_eq!(res_missing.filled_units, 0.0);
        assert_eq!(
            res_missing.fill_type,
            "HL_ACK_MISSING_FILL_DETAILS_PENDING_RECONCILE"
        );

        // 2. Unrecognized / empty statuses on status=="ok" must NOT return REJECTED (NH-7)
        let empty_statuses_resp = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": []
                }
            }
        });
        let res_empty = parse_hyperliquid_order_ack(
            &empty_statuses_resp,
            "SOL/USDC:USDC",
            "LONG",
            5.0,
            180.0,
            true,
            1.5,
            "coid-2",
        );
        assert_eq!(res_empty.fsm_state, "OPEN_ON_BOOK");
        assert_eq!(
            res_empty.fill_type,
            "HL_OK_UNRECOGNIZED_STATUS_PENDING_RECONCILE"
        );

        // 3. Valid filled response computes fee_bps when fee field is absent (NM-9)
        let valid_fill_resp = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        { "filled": { "oid": 888, "avgPx": "180.0", "totalSz": "2.0" } }
                    ]
                }
            }
        });
        let res_valid = parse_hyperliquid_order_ack(
            &valid_fill_resp,
            "SOL/USDC:USDC",
            "LONG",
            2.0,
            180.0,
            false,
            4.5,
            "coid-3",
        );
        assert_eq!(res_valid.fsm_state, "FILLED");
        assert_eq!(res_valid.exchange_order_id, "hl-888");
        assert!((res_valid.fees_paid_usdc - 0.162).abs() < 1e-6);

        // 4. Wire formatting enforces 5 sig figs and (6 - szDecimals) decimals (NH-9)
        assert_eq!(format_hl_wire_price(95123.456, 0.1, 4), "95123");
        assert_eq!(format_hl_wire_price(185.1234, 0.01, 2), "185.12");
        assert_eq!(format_hl_wire_size(12.3456, 0.01, 2), "12.35");
    }

    #[test]
    fn test_live_subaccount_transfer_never_falls_back_to_virtual_ledger_nb1_nm6() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        db.set_config_value("execution_mode", &json!("LIVE")).unwrap();
        let pk = "0x0123456789012345678901234567890123456789012345678901234567890123";
        let derived_wallet = aegis_core::derive_evm_address_from_private_key(pk).unwrap();
        db.set_config_value("wallet_address", &json!(derived_wallet))
            .unwrap();
        db.set_config_value(
            "pond_subaccount_address",
            &json!("0x2222222222222222222222222222222222222222"),
        )
        .unwrap();

        let exec = ExecutionEngine::new(db.clone());
        // Without signing key in LIVE mode with subaccount configured, must return Err, never VIRTUAL_SINGLE_WALLET_LEDGER
        let err = exec
            .execute_subaccount_transfer("PIT", "POND", 100.0, false)
            .unwrap_err();
        assert!(err.to_string().contains("Missing signing key"));

        // With signing key but unreachable exchange endpoint, must also return Err, never VIRTUAL_SINGLE_WALLET_LEDGER
        db.set_config_value("hyperliquid_mainnet_url", &json!("https://127.0.0.1:1"))
            .unwrap();
        db.set_config_value("private_key", &json!(pk)).unwrap();
        let err_http = exec
            .execute_subaccount_transfer("PIT", "POND", 100.0, false)
            .unwrap_err();
        assert!(err_http.to_string().contains("subAccountTransfer"));
    }

    #[test]
    fn test_resting_order_status_does_not_misattribute_existing_position_nh6() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        db.set_config_value("execution_mode", &json!("TESTNET"))
            .unwrap();
        db.set_config_value(
            "wallet_address",
            &json!("0x1111111111111111111111111111111111111111"),
        )
        .unwrap();
        db.set_config_value("hyperliquid_testnet_url", &json!("https://127.0.0.1:1"))
            .unwrap();
        let exec = ExecutionEngine::with_timeout(db.clone(), 1);

        // R3-H4: When /info endpoint is unreachable, must report UNKNOWN (never GONE)
        let unreachable_status =
            exec.check_resting_order_status("SOL/USDC:USDC", Some("hl-123456"));
        assert_eq!(unreachable_status.state, "UNKNOWN");

        // Spin up a local mock /info server that returns empty openOrders, empty userFills, and empty orderStatus
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let stop_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_clone = stop_flag.clone();
        let handle = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed)
                && std::time::Instant::now() < deadline
            {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let _ =
                            stream.set_read_timeout(Some(std::time::Duration::from_millis(200)));
                        let mut buf = [0u8; 4096];
                        let mut total = Vec::new();
                        while let Ok(n) = stream.read(&mut buf) {
                            if n == 0 {
                                break;
                            }
                            total.extend_from_slice(&buf[..n]);
                            if let Some(hdr_end) =
                                total.windows(4).position(|w| w == b"\r\n\r\n")
                            {
                                let body_start = hdr_end + 4;
                                if total.len() > body_start {
                                    break;
                                }
                            }
                        }
                        let body = "[]";
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(resp.as_bytes());
                        let _ = stream.flush();
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let _ = stream.read(&mut buf);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        db.set_config_value(
            "hyperliquid_testnet_url",
            &json!(format!("http://{addr}")),
        )
        .unwrap();

        // NH-6 & R3-H4: When /info is reachable and order is absent from both openOrders and userFills, it must report GONE
        let status = exec.check_resting_order_status("SOL/USDC:USDC", Some("hl-123456"));
        stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
        handle.join().unwrap();
        assert_eq!(status.state, "GONE");
        assert_eq!(status.filled_units, 0.0);
    }

    #[test]
    fn test_short_private_key_rejected_nm11() {
        let err = sign_l1_action(
            "0x1234",
            &json!({"type": "order"}),
            None,
            1700000000000,
            None,
            true,
        )
        .unwrap_err();
        assert!(err.to_string().contains("64 hex characters"));
    }

    #[test]
    fn test_nh7_nh8_nm9_exchange_ack_parsing_partial_fills_and_live_fees() {
        test_hyperliquid_order_ack_parsing_and_wire_formatting_nh7_nh9_nm9();
    }

    #[test]
    fn test_nh9_wire_rounding_5_sig_figs_and_sz_decimals() {
        assert_eq!(format_hl_wire_price(95123.456, 0.1, 4), "95123");
        assert_eq!(format_hl_wire_price(185.1234, 0.01, 2), "185.12");
        assert_eq!(format_hl_wire_size(12.3456, 0.01, 2), "12.35");
    }

    #[test]
    fn test_r3_b4_reconcile_exchange_state_preserves_armed_pond_orders() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        let armed_cell = aegis_core::PondCell {
            cell_id: 0,
            symbol: "SOL/USDC:USDC".to_string(),
            champion_id: 1,
            state: "ARMED".to_string(),
            cycle_opened_id: None,
            side: "LONG".to_string(),
            option_analogue: "SHORT_PUT".to_string(),
            entry_price: Some(180.0),
            quantity: Some(2.0),
            notional_usdc: Some(360.0),
            risk_budget_usdc: Some(5.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(-2.0),
            funding_rate_entry: Some(-0.0002),
            initial_stop: Some(171.0),
            chandelier_stop: Some(171.0),
            highest_price_seen: Some(180.0),
            lowest_price_seen: Some(180.0),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            exit_price: None,
            exit_reason: None,
            resting_order_id: Some("hl-777888".to_string()),
            opened_at: Some("2026-11-08T00:00:00Z".to_string()),
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-11-08T00:00:00Z".to_string(),
        };
        db.upsert_pond_cell(&armed_cell).unwrap();

        let exec = ExecutionEngine::new(db.clone());
        let empty_on_chain = HashMap::new();
        // Resting order is present on Hyperliquid book matching ARMED Pond cell oid 777888
        let resting_orders = vec![json!({"coin": "SOL", "oid": 777888})];
        let recon = exec.reconcile_exchange_state(Some(&empty_on_chain), Some(&resting_orders));
        assert_eq!(recon.status, "RECONCILED");
        // Must NOT cancel or flag the ARMED Pond resting order as an orphaned order!
        assert_eq!(recon.discrepancies_found, 0);
        assert!(recon.discrepancies.is_empty());
    }

    #[test]
    fn test_r4_n6_n9_h2_utc_day_volume_median_and_subaccount_resume_guard() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let exec = ExecutionEngine::new(db.clone());

        // R4-H2: Dry-run virtual-ledger transfer prefixes tx_hash with "dryrun-0x"
        let dry_res = exec
            .execute_subaccount_transfer("PIT", "POND", 250.0, true)
            .unwrap();
        assert_eq!(dry_res.status, "DRY_RUN_SIMULATED");
        assert!(dry_res.tx_hash.starts_with("dryrun-0x"));
        assert!(dry_res.dry_run);

        let real_virt_res = exec
            .execute_subaccount_transfer("PIT", "POND", 250.0, false)
            .unwrap();
        assert_eq!(real_virt_res.status, "EXECUTED");
        assert!(real_virt_res.tx_hash.starts_with("0x"));
        assert!(!real_virt_res.dry_run);

        // R4-N9: Calling execute_subaccount_transfer_for_record when already_completed >= legs.len()
        // returns CONFIRMED immediately without making any HTTP call.
        db.set_config_value("execution_mode", &json!("LIVE")).unwrap();
        let pk = "0x0123456789012345678901234567890123456789012345678901234567890123";
        let derived_wallet = aegis_core::derive_evm_address_from_private_key(pk).unwrap();
        db.set_config_value("wallet_address", &json!(derived_wallet))
            .unwrap();
        db.set_config_value("private_key", &json!(pk)).unwrap();
        db.set_config_value(
            "pit_subaccount_address",
            &json!("0x1111111111111111111111111111111111111111"),
        )
        .unwrap();
        db.set_config_value(
            "pond_subaccount_address",
            &json!("0x2222222222222222222222222222222222222222"),
        )
        .unwrap();
        db.set_config_value("hyperliquid_mainnet_url", &json!("https://127.0.0.1:1"))
            .unwrap();

        let tid = db
            .insert_book_transfer_with_mode(
                &aegis_core::BookTransfer {
                    transfer_id: 0,
                    settle_date: "2026-11-08T00:05:00Z".to_string(),
                    from_book: "PIT".to_string(),
                    to_book: "POND".to_string(),
                    amount_usdc: 200.0,
                    pre_from_usdc: Some(5200.0),
                    pre_to_usdc: Some(4800.0),
                    post_from_usdc: Some(5200.0),
                    post_to_usdc: Some(4800.0),
                    drift_bps_before: Some(200.0),
                    drift_bps_after: Some(200.0),
                    blocked_remainder_usdc: 0.0,
                    status: "PENDING".to_string(),
                    attempts: 1,
                    tx_ref: None,
                    created_at: "2026-11-08T00:05:00Z".to_string(),
                },
                "LIVE",
            )
            .unwrap();
        db.update_book_transfer_leg_progress(tid, 2, Some("0xleg1"), Some("0xleg2"))
            .unwrap();

        let confirmed = exec
            .execute_subaccount_transfer_for_record(Some(tid), "PIT", "POND", 200.0, false)
            .unwrap();
        assert_eq!(confirmed.status, "CONFIRMED");
        assert_eq!(confirmed.tx_hash, "0xleg2");
        assert_eq!(confirmed.legs_executed, 2);

        // R4-N6: UTC calendar-day bucketing for median daily volume handles missing hourly bar gaps cleanly
        // Day 1 (UTC day 20000): 22 bars (2 hours missing due to API outage), each with 10_000 USDC notional => 220_000 USDC
        // Day 2 (UTC day 20001): 24 bars, each with 20_000 USDC notional => 480_000 USDC
        // Day 3 (UTC day 20002): 24 bars, each with 30_000 USDC notional => 720_000 USDC
        // Median of [220_000, 480_000, 720_000] = 480_000 USDC (no cross-day sliding window contamination)
        let mut bars = Vec::new();
        let day1_base_ms = 20_000 * 86_400_000i64;
        for h in 0..24 {
            if h == 5 || h == 6 {
                continue; // 2-hour outage gap on Day 1
            }
            bars.push(aegis_core::OhlcvBar {
                symbol: "SOL/USDC:USDC".to_string(),
                timeframe: "1h".to_string(),
                timestamp_ms: day1_base_ms + h * 3_600_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 100.0, // 100 * 100 = 10_000 USDC/hr
                funding_rate_hourly: 0.0001,
                is_synthetic: 0,
            });
        }
        let day2_base_ms = 20_001 * 86_400_000i64;
        for h in 0..24 {
            bars.push(aegis_core::OhlcvBar {
                symbol: "SOL/USDC:USDC".to_string(),
                timeframe: "1h".to_string(),
                timestamp_ms: day2_base_ms + h * 3_600_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 200.0, // 200 * 100 = 20_000 USDC/hr
                funding_rate_hourly: 0.0001,
                is_synthetic: 0,
            });
        }
        let day3_base_ms = 20_002 * 86_400_000i64;
        for h in 0..24 {
            bars.push(aegis_core::OhlcvBar {
                symbol: "SOL/USDC:USDC".to_string(),
                timeframe: "1h".to_string(),
                timestamp_ms: day3_base_ms + h * 3_600_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 300.0, // 300 * 100 = 30_000 USDC/hr
                funding_rate_hourly: 0.0001,
                is_synthetic: 0,
            });
        }
        db.upsert_ohlcv_batch(&bars).unwrap();
        let ingestion = MarketIngestionEngine::new(db.clone());
        let median_vol =
            ingestion.compute_median_daily_volume_from_bars("SOL/USDC:USDC", 50_000.0, 7);
        assert!((median_vol - 480_000.0).abs() < 1e-6);
    }

    #[test]
    fn test_orphan_position_stop_reconciliation_and_manual_take_profit() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let exec = ExecutionEngine::new(db.clone());

        // 1. Reconcile an orphan SHORT position on TAO/USDC:USDC (not in Pit universe_assets)
        db.reconcile_on_chain_orphan_position(
            "TAO/USDC:USDC",
            "TAO",
            "SHORT",
            0.39,
            317.85,
            305.00,
            5.01,
        )
        .unwrap();

        let open_trades = db.get_open_trades().unwrap();
        let tao = open_trades
            .iter()
            .find(|t| t.symbol == "TAO/USDC:USDC")
            .expect("orphan TAO trade created");
        // Verify initial stop and chandelier stop are tight around mark_price (305.00), NOT +50% (476.78)
        assert!(
            tao.chandelier_stop_price > 305.00 && tao.chandelier_stop_price < 310.00,
            "Orphan chandelier stop {} should be tight (~0.8% above mark 305.00)",
            tao.chandelier_stop_price
        );

        // 2. Insert 1h bars for TAO/USDC:USDC with latest close = 299.10 (no 4h bars needed due to 1h fallback)
        let mut tao_bars = Vec::new();
        let base_ts = 1_762_000_000_000i64;
        for i in 0..20 {
            let close = if i == 19 { 299.10 } else { 305.00 };
            tao_bars.push(aegis_core::OhlcvBar {
                symbol: "TAO/USDC:USDC".to_string(),
                timeframe: "1h".to_string(),
                timestamp_ms: base_ts + i * 3_600_000,
                open: close,
                high: close + 1.5,
                low: close - 1.5,
                close,
                volume: 1000.0,
                funding_rate_hourly: 0.0,
                is_synthetic: 0,
            });
        }
        db.upsert_ohlcv_batch(&tao_bars).unwrap();

        // Run reconcile_cycle_with_options(&[], false, true) (stops-only mode) and verify TAO/USDC:USDC is evaluated
        let _snapshot = exec.reconcile_cycle_with_options(&[], false, true).unwrap();

        let open_after = db.get_open_trades().unwrap();
        let tao_after = open_after
            .iter()
            .find(|t| t.symbol == "TAO/USDC:USDC")
            .expect("TAO still open");
        assert!(
            (tao_after.current_price - 299.10).abs() < 1e-6,
            "Orphan trade current_price must update to 299.10 (got {})",
            tao_after.current_price
        );
        let expected_upnl = (317.85 - 299.10) * 0.39;
        assert!(
            (tao_after.unrealized_pnl_usdc - expected_upnl).abs() < 1e-3,
            "Orphan trade uPnL must update to {:.4} (got {:.4})",
            expected_upnl,
            tao_after.unrealized_pnl_usdc
        );
        assert!(
            tao_after.chandelier_stop_price > 299.10 && tao_after.chandelier_stop_price < 302.00,
            "Orphan trade chandelier_stop_price must ratchet tightly above 299.10 (got {})",
            tao_after.chandelier_stop_price
        );

        // 3. Manual Take Profit closes TAO/USDC:USDC
        let close_res = exec
            .close_open_trade_manual(tao_after.trade_id, None)
            .expect("manual take profit succeeds");
        assert_eq!(close_res["status"], "CLOSED");
        assert_eq!(close_res["exit_reason"], "MANUAL_TAKE_PROFIT");
        assert!((close_res["exit_price"].as_f64().unwrap() - 299.10).abs() < 1e-6);

        // Edge case: attempting to close the same trade again must return an error
        assert!(
            exec.close_open_trade_manual(tao_after.trade_id, None)
                .is_err(),
            "Closing an already closed trade must return an error"
        );
        // Edge case: non-existent trade_id must return an error
        assert!(
            exec.close_open_trade_manual(999_999, None).is_err(),
            "Closing a non-existent trade must return an error"
        );

        // 4. Verify sync_funding_payments recomputes unrealized_pnl_usdc from mark_px instead of keeping stale uPnL
        let tao_long_id = db
            .reconcile_on_chain_orphan_position(
                "TAO/USDC:USDC",
                "TAO",
                "LONG",
                0.548,
                218.12,
                224.24,
                3.3538, // initially positive at 224.24
            )
            .unwrap();
        let next_hour_ts = base_ts + 20 * 3_600_000;
        db.upsert_ohlcv_batch(&[aegis_core::OhlcvBar {
            symbol: "TAO/USDC:USDC".to_string(),
            timeframe: "1h".to_string(),
            timestamp_ms: next_hour_ts,
            open: 204.20,
            high: 205.00,
            low: 203.50,
            close: 204.20,
            volume: 1200.0,
            funding_rate_hourly: 0.0001,
            is_synthetic: 0,
        }])
        .unwrap();
        let funding_report = exec.sync_funding_payments(Some(next_hour_ts), true);
        assert_eq!(funding_report.positions_updated, 1);
        let open_after_funding = db.get_open_trades().unwrap();
        let tao_long = open_after_funding
            .iter()
            .find(|t| t.trade_id == tao_long_id)
            .expect("TAO LONG still open");
        let expected_long_upnl = aegis_core::round_dp((204.20 - 218.12) * 0.548, 4);
        assert!(
            (tao_long.unrealized_pnl_usdc - expected_long_upnl).abs() < 1e-4,
            "sync_funding_payments must update unrealized_pnl_usdc to {} (got {})",
            expected_long_upnl,
            tao_long.unrealized_pnl_usdc
        );

        // 5. Verify close_open_pond_cell_manual cancels an ARMED Pond cell cleanly
        let armed_cell_id = db
            .upsert_pond_cell(&aegis_core::PondCell {
                cell_id: 0,
                symbol: "NEAR/USDC:USDC".to_string(),
                champion_id: 1,
                state: "ARMED".to_string(),
                cycle_opened_id: None,
                side: "SHORT".to_string(),
                option_analogue: "SHORT_CALL".to_string(),
                entry_price: Some(1.45),
                quantity: Some(500.0),
                notional_usdc: Some(725.0),
                risk_budget_usdc: Some(25.0),
                conviction_multiplier: Some(1.0),
                funding_z_entry: Some(0.0),
                funding_rate_entry: Some(0.0),
                initial_stop: Some(1.48),
                chandelier_stop: Some(1.48),
                highest_price_seen: Some(1.45),
                lowest_price_seen: Some(1.45),
                funding_accrued_usdc: 0.0,
                realized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.0,
                exit_price: None,
                exit_reason: None,
                opened_at: None,
                closed_at: None,
                cooldown_until: None,
                updated_at: "2026-11-09T09:00:00Z".to_string(),
                resting_order_id: Some("hl-paper-armed-1".to_string()),
            })
            .unwrap();
        let armed_close = exec
            .close_open_pond_cell_manual(armed_cell_id, None)
            .expect("closing ARMED Pond cell succeeds");
        assert_eq!(armed_close["status"], "CANCELLED_ARMED");
        assert!(db.get_armed_pond_cells().unwrap().is_empty());

        // 6. R7-N3 & R7-N5: build_orphan_or_pond_universe_asset preserves positive min_notional_usdc < 10.0
        let pepe_row = aegis_core::PondUniverseRow {
            symbol: "PEPE/USDC:USDC".to_string(),
            rank: 42,
            median_daily_volume_usdc: 5_000_000.0,
            tick_size: 1e-8,
            lot_step_size: 1.0,
            min_notional_usdc: 5.0,
            max_leverage: 5.0,
            status: "ACTIVE".to_string(),
            warmup_until: None,
            delist_streak: 0,
            added_at: "2026-11-09T00:00:00Z".to_string(),
            delisted_at: None,
            reference_price: 0.00001234,
            is_synthetic_volume: 0,
        };
        let pepe_asset = build_orphan_or_pond_universe_asset(
            "PEPE/USDC:USDC",
            "PEPE",
            0.00001234,
            Some(&pepe_row),
            "POND",
        );
        assert!(
            (pepe_asset.min_notional_usdc - 5.0).abs() < 1e-9,
            "build_orphan_or_pond_universe_asset must preserve min_notional_usdc = 5.0 (< 10.0) from pond_universe (R7-N3, R7-N5)"
        );
        assert_eq!(pepe_asset.lot_step_size, 1.0);
        assert!((pepe_asset.tick_size - 1e-8).abs() < 1e-12);

        // 7. R7-S4: In TESTNET/LIVE mode without valid live exchange fills or confirmed exchange absence,
        // manual close of a non-paper trade or Pond cell MUST return an error and keep the position OPEN locally.
        db.set_config_value("execution_mode", &serde_json::json!("TESTNET"))
            .unwrap();
        db.set_config_value("pond_execution_mode", &serde_json::json!("TESTNET"))
            .unwrap();
        let live_err = exec.close_open_trade_manual(tao_long_id, Some("MANUAL_TAKE_PROFIT"));
        assert!(
            live_err.is_err(),
            "Manual close in TESTNET mode must fail when exchange close/absence cannot be confirmed (R7-S4)"
        );
        assert!(
            db.get_open_trades()
                .unwrap()
                .iter()
                .any(|t| t.trade_id == tao_long_id),
            "Trade must remain OPEN in local DB when TESTNET exchange close fails (R7-S4)"
        );

        let live_cell_id = db
            .upsert_pond_cell(&aegis_core::PondCell {
                cell_id: 0,
                symbol: "NEAR/USDC:USDC".to_string(),
                champion_id: 1,
                state: "OPEN".to_string(),
                cycle_opened_id: None,
                side: "SHORT".to_string(),
                option_analogue: "SHORT_CALL".to_string(),
                entry_price: Some(1.45),
                quantity: Some(500.0),
                notional_usdc: Some(725.0),
                risk_budget_usdc: Some(25.0),
                conviction_multiplier: Some(1.0),
                funding_z_entry: Some(0.0),
                funding_rate_entry: Some(0.0),
                initial_stop: Some(1.48),
                chandelier_stop: Some(1.48),
                highest_price_seen: Some(1.45),
                lowest_price_seen: Some(1.45),
                funding_accrued_usdc: 0.0,
                realized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.0,
                exit_price: None,
                exit_reason: None,
                opened_at: Some("2026-11-09T09:00:00Z".to_string()),
                closed_at: None,
                cooldown_until: None,
                updated_at: "2026-11-09T09:00:00Z".to_string(),
                resting_order_id: Some("123456789".to_string()), // non-paper order ID
            })
            .unwrap();
        let live_cell_err = exec.close_open_pond_cell_manual(live_cell_id, None);
        assert!(
            live_cell_err.is_err(),
            "Manual Pond cell close in TESTNET mode must fail when exchange close/absence cannot be confirmed (R7-S4)"
        );
        assert!(
            db.get_open_pond_cells()
                .unwrap()
                .iter()
                .any(|c| c.cell_id == live_cell_id),
            "Pond cell must remain OPEN in local DB when TESTNET exchange close fails (R7-S4)"
        );
        assert!(
            exec.resolved_asset_cache
                .lock()
                .contains_key("TAO/USDC:USDC"),
            "ExecutionEngine::resolved_asset_cache must cache resolved orphan/universe assets (R7-N3)"
        );
    }

    #[test]
    fn test_dangling_order_cancelled_on_partial_fill_and_pit_sizing_ignores_unallocated_spot_usdc() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        db.set_config_value("execution_mode", &json!("TESTNET"))
            .unwrap();
        db.set_config_value("hyperliquid_testnet", &json!(true))
            .unwrap();
        db.scale_risk_caps_for_equity(1000.0).unwrap();

        let exec = ExecutionEngine::new(db.clone());

        // 1. Suppose an untracked resting order (oid=64920976942) on NEAR partially filled (-23.1 NEAR short on-chain).
        // Even though `ex_positions` contains "NEAR" (and even after "NEAR" is imported into `local_trades`),
        // `reconcile_exchange_state` MUST flag and cancel the untracked resting order (`!is_known_resting_oid`).
        let mut on_chain = HashMap::new();
        on_chain.insert(
            "NEAR".to_string(),
            json!({"szi": -23.1, "entryPx": 3.612, "markPx": 3.612, "unrealizedPnl": 0.0}),
        );
        let open_orders = vec![json!({"coin": "NEAR", "oid": 64920976942_u64})];

        let rep1 = exec.reconcile_exchange_state(Some(&on_chain), Some(&open_orders));
        assert_eq!(rep1.status, ReconcileStatus::Reconciled);
        assert!(
            rep1.discrepancies
                .iter()
                .any(|d| d.get("type").and_then(|v| v.as_str()) == Some("DANGLING_OPEN_ORDER_CANCELLED")
                    && d.get("base_asset").and_then(|v| v.as_str()) == Some("NEAR")),
            "Untracked resting order on coin with partial-fill position must be cancelled on tick 1"
        );

        // Tick 2: NEAR is now in `local_trades` (`seen_coins` contains "NEAR") and position grew to -115.2 NEAR.
        // Because oid 64920976942 is NOT a known resting order (`known_resting_oids`), it must still be cancelled!
        on_chain.insert(
            "NEAR".to_string(),
            json!({"szi": -115.2, "entryPx": 3.612, "markPx": 3.612, "unrealizedPnl": 0.0}),
        );
        let rep2 = exec.reconcile_exchange_state(Some(&on_chain), Some(&open_orders));
        assert!(
            rep2.discrepancies
                .iter()
                .any(|d| d.get("type").and_then(|v| v.as_str()) == Some("DANGLING_OPEN_ORDER_CANCELLED")
                    && d.get("base_asset").and_then(|v| v.as_str()) == Some("NEAR")),
            "Untracked resting order on coin with existing local trade + exchange position must still be cancelled"
        );

        // Close the reconciled NEAR trade so we can test tracked resting order edge cases and new entry sizing cleanly
        let open_trades = db.get_open_trades().unwrap();
        for t in open_trades {
            db.close_trade(t.trade_id, 3.612, 0.0, 0.0, "TEST_CLEANUP")
                .unwrap();
        }

        // 1b. Suppose a tracked resting PIT order (status = PENDING, oid = hl-11111) exists on SOL:
        // - Before any fill (ex_positions is empty), if open_orders has both tracked oid 11111 AND untracked oid 22222 on SOL,
        //   oid 11111 must be spared while untracked oid 22222 MUST be cancelled as DANGLING_OPEN_ORDER_CANCELLED.
        // - When oid 11111 partially fills on-chain while still resting in open_orders, reconcile_exchange_state must NOT
        //   import a duplicate ORPHAN_ON_CHAIN_IMPORTED trade and must NOT cancel oid 11111.
        let tracked_resting = aegis_core::TradeRecord {
            trade_id: 0,
            client_order_id: "hl-tracked-sol".to_string(),
            symbol: "SOL/USDC:USDC".to_string(),
            base_asset: "SOL".to_string(),
            side: "SHORT".to_string(),
            status: "PENDING".to_string(),
            fsm_state: "OPEN_ON_BOOK".to_string(),
            entry_price: 87.358,
            current_price: 87.358,
            exit_price: None,
            quantity: 1.37,
            filled_units: 0.0,
            avg_fill_price: None,
            notional_usdc: 119.68,
            risk_budget_usdc: 5.0,
            conviction_multiplier: 1.0,
            initial_stop_price: 91.0,
            chandelier_stop_price: 91.0,
            current_stop_price: 91.0,
            highest_price_seen: 87.358,
            lowest_price_seen: 87.358,
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            funding_accrued_usdc: 0.0,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some("hl-11111".to_string()),
            opened_at: "2026-11-09T16:08:51Z".to_string(),
            closed_at: None,
            exit_reason: None,
        };
        let tracked_tid = db.insert_trade(&tracked_resting).unwrap();

        let empty_ex = HashMap::new();
        let orders_mixed = vec![
            json!({"coin": "SOL", "oid": 11111_u64}),
            json!({"coin": "SOL", "oid": 22222_u64}),
        ];
        let rep_mixed = exec.reconcile_exchange_state(Some(&empty_ex), Some(&orders_mixed));
        assert_eq!(rep_mixed.discrepancies.len(), 1);
        assert_eq!(
            rep_mixed.discrepancies[0].get("type").and_then(|v| v.as_str()),
            Some("DANGLING_OPEN_ORDER_CANCELLED")
        );
        assert_eq!(
            rep_mixed.discrepancies[0].get("oid").and_then(|v| v.as_u64()),
            Some(22222_u64)
        );

        let mut partial_sol_ex = HashMap::new();
        partial_sol_ex.insert(
            "SOL".to_string(),
            json!({"szi": -0.50, "entryPx": 87.358, "markPx": 87.358, "unrealizedPnl": 0.0}),
        );
        let orders_tracked_only = vec![json!({"coin": "SOL", "oid": 11111_u64})];
        let rep_partial =
            exec.reconcile_exchange_state(Some(&partial_sol_ex), Some(&orders_tracked_only));
        assert_eq!(
            rep_partial.discrepancies.len(),
            0,
            "Tracked resting order with partial fill on-chain must not trigger ORPHAN_ON_CHAIN_IMPORTED or DANGLING_OPEN_ORDER_CANCELLED"
        );
        assert!(db.get_open_trades().unwrap().is_empty());

        db.cancel_resting_trade(tracked_tid, "TEST_CLEANUP")
            .unwrap();

        // 2. Pollute `portfolio_snapshots.total_equity_usdc` with $10,972.25 (simulating $10,517.65 unallocated Testnet faucet Spot USDC)
        // while `initial_equity_usdc = 1000.0` ($500 Book A, $125 single-asset cap).
        let mut polluted_snap = db.get_latest_portfolio_snapshot().unwrap();
        polluted_snap.total_equity_usdc = 10_972.25;
        polluted_snap.available_cash_usdc = 10_972.25;
        db.save_portfolio_snapshot(&polluted_snap).unwrap();

        // Seed 30 1H bars for SOL/USDC:USDC around $100.0
        let base_ts = 1_760_000_000_000_i64;
        let mut bars = Vec::new();
        for i in 0..30 {
            bars.push(aegis_core::OhlcvBar {
                symbol: "SOL/USDC:USDC".to_string(),
                timeframe: "1h".to_string(),
                timestamp_ms: base_ts + i * 3_600_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 100_000.0,
                funding_rate_hourly: 0.0,
                is_synthetic: 0,
            });
        }
        db.upsert_ohlcv_batch(&bars).unwrap();

        // Run `reconcile_cycle_with_options` in PAPER mode (with `platform_pp50_enabled = false`, `pond_enabled = false`, `risk_caps_reference_scope = BOOK`)
        db.set_config_value("execution_mode", &json!("PAPER"))
            .unwrap();
        db.set_config_value("platform_pp50_enabled", &json!(false))
            .unwrap();
        db.set_config_value("pond_enabled", &json!(false)).unwrap();
        let eval = aegis_core::SignalEvaluation {
            eval_id: None,
            timestamp_ms: base_ts + 29 * 14_400_000,
            base_asset: "SOL".to_string(),
            exec_symbol: "SOL/USDC:USDC".to_string(),
            synth_symbol: "SOL/BTC".to_string(),
            hmm_regime: "BEAR".to_string(),
            prob_bull: 0.05,
            prob_bear: 0.90,
            prob_chop: 0.05,
            snr_score: -1.5,
            snr_synth: -1.5,
            rs_96: -0.05,
            r_usdc_8: -0.02,
            mace_score: -0.85,
            blended_conviction: -0.85,
            rs_rank: 15,
            signal_action: "SHORT".to_string(),
            funding_rate_hourly: 0.0,
            created_at: None,
        };
        let snap_after = exec
            .reconcile_cycle_with_options(&[eval], false, false)
            .unwrap();
        assert!(
            (snap_after.total_equity_usdc - 1000.0).abs() < 5.0,
            "Snapshot total_equity_usdc must be anchored to ~$1,000, got {}",
            snap_after.total_equity_usdc
        );
        let opened = db.get_open_trades().unwrap();
        assert_eq!(opened.len(), 1);
        assert!(
            opened[0].notional_usdc <= 130.0,
            "Book A single-asset notional on $500 book must be capped near $125 (never 22x = $2,742), got ${}",
            opened[0].notional_usdc
        );
    }
}

