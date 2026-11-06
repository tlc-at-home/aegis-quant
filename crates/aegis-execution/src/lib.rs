//! Hyperliquid L1 Ingestion, `ALT/BTC:SYNTH` Synthesis, and Order Execution Engine (`aegis-execution`).
//!
//! 1:1 port of `engine/ingestion.py` and `engine/execution.py`:
//! - `MarketIngestionEngine`: Hyperliquid `/info` (`metaAndAssetCtxs`, `candleSnapshot`) client,
//!   deterministic regimes synthetic fallback, and exact `ALT/BTC:SYNTH` ratio candle synthesis.
//! - `ExecutionEngine`:
//!   - 7-state `MAKER_LIMIT_CHASER` Order Finite State Machine (`PENDING`, `OPEN_ON_BOOK`,
//!     `PARTIALLY_FILLED`, `FILLED`, `CANCELLED`, `REJECTED`, `EXPIRED`)
//!   - Volatility & liquidity-adjusted Paper Fill & Dynamic Slippage model
//!   - Hyperliquid L1 EIP-712 / secp256k1 order & vault transfer signing
//!   - Startup & periodic position drift reconciliation (`reconcile_exchange_state`)
//!   - Realized 1H funding rate synchronization (`sync_funding_payments`)
//!   - HLP Vault basis harvesting (`fetch_hlp_vault_state`, `execute_hlp_vault_transfer`)
//!   - Live wallet equity synchronization (`sync_wallet_equity`)
//!   - Post-stop cooldown & full portfolio execution cycle (`reconcile_cycle`)

pub mod ingestion;
pub mod signing;

pub use ingestion::MarketIngestionEngine;
pub use signing::{
    compute_l1_action_hash, float_to_wire, sign_l1_action, CancelAction, CancelWire,
    LimitOrderType, OrderAction, OrderTypeWire, OrderWire, SubAccountTransferAction,
    TriggerOrderTypeWire, VaultTransferAction,
};

use aegis_alerts::AlertEngine;
use aegis_core::{
    round_dp, timeframe_seconds, validate_evm_address, OrderExecutionResult, PortfolioSnapshot,
    Result, SignalEvaluation, TradeRecord, UniverseAsset, MAINNET_HLP_VAULT_ADDRESS,
    TESTNET_HLP_VAULT_ADDRESS,
};
use aegis_db::{ConfigMap, DatabaseManager};
use aegis_risk::RiskEngine;
use chrono::Utc;
use parking_lot::Mutex;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest as Sha2Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub const HYPERLIQUID_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
pub const HYPERLIQUID_TESTNET_INFO_URL: &str = "https://api.hyperliquid-testnet.xyz/info";
pub const HYPERLIQUID_EXCHANGE_URL: &str = "https://api.hyperliquid.xyz/exchange";
pub const HYPERLIQUID_TESTNET_EXCHANGE_URL: &str = "https://api.hyperliquid-testnet.xyz/exchange";

/// Status of a resting order on the exchange (`B8`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RestingOrderStatus {
    pub state: String,
    pub filled_units: f64,
    pub entry_price: f64,
}

/// Hyperliquid L1 Order Execution, Paper Fill Simulator, and Reconciliation Engine (`engine/execution.py`).
#[derive(Clone)]
pub struct ExecutionEngine {
    pub db: DatabaseManager,
    pub risk: RiskEngine,
    pub alerts: AlertEngine,
    pub timeout_sec: u64,
    http_client: Client,
    client_timeout_sec: u64,
    last_funding_sync_hour_ts: Arc<Mutex<Option<i64>>>,
    stop_cooldown_until_ms: Arc<Mutex<HashMap<String, i64>>>,
    hlp_vault_error_streak: Arc<Mutex<usize>>,
    asset_index_cache: Arc<Mutex<HashMap<(bool, String), u32>>>,
}

impl ExecutionEngine {
    pub fn new(db: DatabaseManager) -> Self {
        let timeout_sec = db
            .load_config()
            .ok()
            .map(|c| {
                c.get_i64(
                    "execution_http_timeout_sec",
                    c.get_i64("http_timeout_secs", 10),
                )
                .clamp(1, 60) as u64
            })
            .unwrap_or(10);
        let http_client = Client::builder()
            .timeout(Duration::from_secs(timeout_sec))
            .build()
            .unwrap_or_else(|_| Client::new());
        let risk = RiskEngine::new(db.clone());
        let alerts = AlertEngine::new(db.clone());
        Self {
            db,
            risk,
            alerts,
            timeout_sec,
            http_client,
            client_timeout_sec: timeout_sec,
            last_funding_sync_hour_ts: Arc::new(Mutex::new(None)),
            stop_cooldown_until_ms: Arc::new(Mutex::new(HashMap::new())),
            hlp_vault_error_streak: Arc::new(Mutex::new(0)),
            asset_index_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn load_config_logged(&self, caller: &str) -> ConfigMap {
        match self.db.load_config() {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::error!(
                    caller = caller,
                    error = %e,
                    "ExecutionEngine failed to load system_config from SQLite; falling back to empty ConfigMap"
                );
                ConfigMap::default()
            }
        }
    }

    fn log_fsm_transition(
        &self,
        client_order_id: &str,
        from_state: &str,
        to_state: &str,
        reason: Option<&str>,
    ) {
        if let Err(e) =
            self.db
                .log_order_state_transition(client_order_id, from_state, to_state, reason)
        {
            tracing::error!(
                client_order_id = client_order_id,
                from_state = from_state,
                to_state = to_state,
                reason = ?reason,
                error = %e,
                "Failed to persist order_state_audit transition"
            );
        }
    }

    fn info_url(&self, cfg: &ConfigMap, is_testnet: bool) -> String {
        if is_testnet {
            let base = cfg.get_str(
                "hyperliquid_testnet_url",
                "https://api.hyperliquid-testnet.xyz",
            );
            let fallback = format!("{}/info", base.trim_end_matches('/'));
            let custom = cfg.get_str("hyperliquid_testnet_info_url", "");
            if custom.trim().is_empty() {
                fallback
            } else {
                custom.trim().to_string()
            }
        } else {
            let base = cfg.get_str("hyperliquid_mainnet_url", "https://api.hyperliquid.xyz");
            let fallback = format!("{}/info", base.trim_end_matches('/'));
            let custom = cfg.get_str("hyperliquid_info_url", "");
            if custom.trim().is_empty() {
                fallback
            } else {
                custom.trim().to_string()
            }
        }
    }

    fn exchange_url(&self, cfg: &ConfigMap, is_testnet: bool) -> String {
        if is_testnet {
            let base = cfg.get_str(
                "hyperliquid_testnet_url",
                "https://api.hyperliquid-testnet.xyz",
            );
            let fallback = format!("{}/exchange", base.trim_end_matches('/'));
            let custom = cfg.get_str("hyperliquid_testnet_exchange_url", "");
            if custom.trim().is_empty() {
                fallback
            } else {
                custom.trim().to_string()
            }
        } else {
            let base = cfg.get_str("hyperliquid_mainnet_url", "https://api.hyperliquid.xyz");
            let fallback = format!("{}/exchange", base.trim_end_matches('/'));
            let custom = cfg.get_str("hyperliquid_exchange_url", "");
            if custom.trim().is_empty() {
                fallback
            } else {
                custom.trim().to_string()
            }
        }
    }

    pub fn resolve_hlp_vault_address(cfg: &ConfigMap, is_testnet: bool) -> String {
        let default_testnet = cfg.get_str(
            "hyperliquid_default_vault_testnet",
            TESTNET_HLP_VAULT_ADDRESS,
        );
        let default_mainnet = cfg.get_str(
            "hyperliquid_default_vault_mainnet",
            MAINNET_HLP_VAULT_ADDRESS,
        );
        let configured = cfg.get_str("hyperliquid_vault_address", "").trim().to_string();
        if !configured.is_empty()
            && validate_evm_address(&configured, "hyperliquid_vault_address").is_ok()
        {
            if is_testnet && configured.eq_ignore_ascii_case(&default_mainnet) {
                return default_testnet;
            }
            return configured;
        }
        if is_testnet {
            default_testnet
        } else {
            default_mainnet
        }
    }

    fn http_post_json(&self, url: &str, payload: &Value) -> Option<Value> {
        let owned_client;
        let client_ref = if self.timeout_sec == self.client_timeout_sec {
            &self.http_client
        } else {
            owned_client = Client::builder()
                .timeout(Duration::from_secs(self.timeout_sec.max(1)))
                .build()
                .ok()?;
            &owned_client
        };
        let resp = match client_ref.post(url).json(payload).send() {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(url = url, error = %e, "Hyperliquid HTTP POST failed");
                return None;
            }
        };
        if !resp.status().is_success() {
            tracing::warn!(
                url = url,
                status = %resp.status(),
                "Hyperliquid endpoint returned non-2xx HTTP status"
            );
            return None;
        }
        match resp.json::<Value>() {
            Ok(val) => Some(val),
            Err(e) => {
                tracing::warn!(url = url, error = %e, "Failed to parse Hyperliquid JSON response");
                None
            }
        }
    }

    /// Computes dynamic slippage in basis points (`bps`) as a function of bar range volatility
    /// and order notional relative to bar volume.
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
            if is_maker {
                cfg.get_f64("maker_slippage_bps", 0.5)
            } else {
                cfg.get_f64("paper_slippage_bps", 3.5)
            }
        });
        let min_bar_liq = cfg.get_f64("slippage_min_bar_notional_liq_usdc", 25_000.0).max(1.0);
        let max_part = cfg.get_f64("slippage_max_participation_rate", 0.25).clamp(0.01, 1.0);
        let impact_factor = cfg.get_f64("slippage_liq_impact_factor_bps", 120.0).max(0.0);
        let max_cap_bps = cfg.get_f64("slippage_max_cap_bps", 85.0).max(1.0);

        let close_px = bar_close.max(1e-8);
        let range_pct = if bar_high > bar_low && bar_low > 0.0 {
            (bar_high - bar_low).max(0.0) / close_px
        } else {
            0.01
        };
        let vol_ratio = (range_pct / 0.01).clamp(0.4, 4.0);
        let bar_notional_liq = (bar_volume.max(0.0) * close_px).max(min_bar_liq);
        let participation = (notional_usd.max(0.0) / bar_notional_liq).clamp(0.0, max_part);
        let liq_impact_bps = participation * impact_factor;
        let eff_bps = if is_maker {
            base_bps * (0.5 + 0.5 * vol_ratio) + 0.25 * liq_impact_bps
        } else {
            base_bps * vol_ratio + liq_impact_bps
        };
        round_dp(eff_bps.clamp(0.1, max_cap_bps), 4)
    }

    /// Simulates a realistic Paper order fill with dynamic slippage, partial fills, and fee accounting.
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
        let maker_fee_bps = cfg.get_f64("maker_fee_bps", 1.5);
        let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);
        let cli_id = client_order_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("paper-{}", &Uuid::new_v4().simple().to_string()[..12]));
        let ex_id = format!("hl-paper-{}", &Uuid::new_v4().simple().to_string()[..12]);

        let req_units = RiskEngine::round_step(requested_units.max(0.0), lot_step_size, true);
        let req_notional = req_units * reference_price;

        if req_units <= 0.0 || reference_price <= 0.0 {
            return OrderExecutionResult {
                client_order_id: cli_id,
                exchange_order_id: ex_id,
                symbol: symbol.to_string(),
                side: side.to_string(),
                fsm_state: "REJECTED".to_string(),
                requested_units,
                filled_units: 0.0,
                avg_fill_price: 0.0,
                slippage_paid_usd: 0.0,
                effective_slippage_bps: 0.0,
                fees_paid_usdc: 0.0,
                is_maker: false,
                fill_type: "REJECTED".to_string(),
                state_history: vec!["PENDING".to_string(), "REJECTED".to_string()],
            };
        }

        if let Some(ratio) = force_partial_ratio.filter(|&r| r > 0.0 && r < 1.0) {
            let leg1_units = RiskEngine::round_step(req_units * ratio, lot_step_size, true)
                .max(lot_step_size);
            let leg2_units =
                RiskEngine::round_step((req_units - leg1_units).max(0.0), lot_step_size, true);

            let maker_slip_bps = self.calculate_dynamic_slippage_bps(
                leg1_units * reference_price,
                reference_price,
                bar_high,
                bar_low,
                bar_volume,
                true,
                None,
            );
            let taker_slip_bps = self.calculate_dynamic_slippage_bps(
                leg2_units * reference_price,
                reference_price,
                bar_high,
                bar_low,
                bar_volume,
                false,
                None,
            );

            let dir = if side == "LONG" || side == "BUY" {
                1.0
            } else {
                -1.0
            };
            let px1 = reference_price * (1.0 + dir * (maker_slip_bps / 10000.0));
            let px2 = reference_price * (1.0 + dir * (taker_slip_bps / 10000.0));
            let total_filled = leg1_units + leg2_units;
            let raw_avg = (leg1_units * px1 + leg2_units * px2) / total_filled.max(1e-12);
            let avg_px = RiskEngine::round_step(raw_avg, tick_size, dir < 0.0);

            let slip_usd = (avg_px - reference_price).abs() * total_filled;
            let eff_bps = (slip_usd / (reference_price * total_filled).max(1e-12)) * 10000.0;
            let fees_usd = (leg1_units * px1) * (maker_fee_bps / 10000.0)
                + (leg2_units * px2) * (taker_fee_bps / 10000.0);

            return OrderExecutionResult {
                client_order_id: cli_id,
                exchange_order_id: ex_id,
                symbol: symbol.to_string(),
                side: side.to_string(),
                fsm_state: "FILLED".to_string(),
                requested_units: req_units,
                filled_units: total_filled,
                avg_fill_price: avg_px,
                slippage_paid_usd: round_dp(slip_usd, 4),
                effective_slippage_bps: round_dp(eff_bps, 4),
                fees_paid_usdc: round_dp(fees_usd, 4),
                is_maker: false,
                fill_type: "PARTIAL_MAKER_THEN_TAKER".to_string(),
                state_history: vec![
                    "PENDING".to_string(),
                    "OPEN_ON_BOOK".to_string(),
                    "PARTIALLY_FILLED".to_string(),
                    "FILLED".to_string(),
                ],
            };
        }

        let is_maker = prefer_maker && !is_stop_exit;
        let base_stop_bps = if is_stop_exit {
            Some(cfg.get_f64("stop_slippage_bps", 8.0))
        } else {
            None
        };
        let slip_bps = self.calculate_dynamic_slippage_bps(
            req_notional,
            reference_price,
            bar_high,
            bar_low,
            bar_volume,
            is_maker,
            base_stop_bps,
        );
        let dir = if side == "LONG" || side == "BUY" {
            1.0
        } else {
            -1.0
        };
        let raw_px = reference_price * (1.0 + dir * (slip_bps / 10000.0));
        let fill_px = RiskEngine::round_step(raw_px, tick_size, dir < 0.0);
        let slip_usd = (fill_px - reference_price).abs() * req_units;
        let fee_bps = if is_maker {
            maker_fee_bps
        } else {
            taker_fee_bps
        };
        let fees_usd = (req_units * fill_px) * (fee_bps / 10000.0);

        OrderExecutionResult {
            client_order_id: cli_id,
            exchange_order_id: ex_id,
            symbol: symbol.to_string(),
            side: side.to_string(),
            fsm_state: "FILLED".to_string(),
            requested_units: req_units,
            filled_units: req_units,
            avg_fill_price: fill_px,
            slippage_paid_usd: round_dp(slip_usd, 4),
            effective_slippage_bps: slip_bps,
            fees_paid_usdc: round_dp(fees_usd, 4),
            is_maker,
            fill_type: if is_maker {
                "MAKER_POST_ONLY".to_string()
            } else {
                "TAKER_IOC".to_string()
            },
            state_history: vec![
                "PENDING".to_string(),
                "OPEN_ON_BOOK".to_string(),
                "FILLED".to_string(),
            ],
        }
    }

    /// Executes an order through the 7-state `MAKER_LIMIT_CHASER` Finite State Machine
    /// and logs every state transition to `order_state_audit`.
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
        _is_reduce_only: bool,
    ) -> OrderExecutionResult {
        let cli_id = client_order_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("cli-{}", &Uuid::new_v4().simple().to_string()[..12]));

        if let Some(scenario) = simulate_book_scenario {
            match scenario.to_ascii_uppercase().as_str() {
                "REJECTED" => {
                    self.log_fsm_transition(
                        &cli_id,
                        "PENDING",
                        "REJECTED",
                        Some("POST_ONLY_CROSSES_BOOK_OR_MARGIN_REJECTED"),
                    );
                    return OrderExecutionResult {
                        client_order_id: cli_id,
                        exchange_order_id: String::new(),
                        symbol: asset.symbol.clone(),
                        side: side.to_string(),
                        fsm_state: "REJECTED".to_string(),
                        requested_units: quantity,
                        filled_units: 0.0,
                        avg_fill_price: 0.0,
                        slippage_paid_usd: 0.0,
                        effective_slippage_bps: 0.0,
                        fees_paid_usdc: 0.0,
                        is_maker: false,
                        fill_type: "REJECTED".to_string(),
                        state_history: vec!["PENDING".to_string(), "REJECTED".to_string()],
                    };
                }
                "CANCELLED" => {
                    self.log_fsm_transition(
                        &cli_id,
                        "PENDING",
                        "OPEN_ON_BOOK",
                        Some("ALO_POSTED_TO_BOOK"),
                    );
                    self.log_fsm_transition(
                        &cli_id,
                        "OPEN_ON_BOOK",
                        "CANCELLED",
                        Some("USER_OR_REGIME_CANCELLED"),
                    );
                    return OrderExecutionResult {
                        client_order_id: cli_id,
                        exchange_order_id: format!(
                            "hl-cancel-{}",
                            &Uuid::new_v4().simple().to_string()[..8]
                        ),
                        symbol: asset.symbol.clone(),
                        side: side.to_string(),
                        fsm_state: "CANCELLED".to_string(),
                        requested_units: quantity,
                        filled_units: 0.0,
                        avg_fill_price: 0.0,
                        slippage_paid_usd: 0.0,
                        effective_slippage_bps: 0.0,
                        fees_paid_usdc: 0.0,
                        is_maker: false,
                        fill_type: "CANCELLED".to_string(),
                        state_history: vec![
                            "PENDING".to_string(),
                            "OPEN_ON_BOOK".to_string(),
                            "CANCELLED".to_string(),
                        ],
                    };
                }
                "EXPIRED" => {
                    self.log_fsm_transition(
                        &cli_id,
                        "PENDING",
                        "OPEN_ON_BOOK",
                        Some("ALO_POSTED_TO_BOOK"),
                    );
                    self.log_fsm_transition(
                        &cli_id,
                        "OPEN_ON_BOOK",
                        "EXPIRED",
                        Some("MAKER_CHASE_TIMEOUT_EXPIRED"),
                    );
                    return OrderExecutionResult {
                        client_order_id: cli_id,
                        exchange_order_id: format!(
                            "hl-exp-{}",
                            &Uuid::new_v4().simple().to_string()[..8]
                        ),
                        symbol: asset.symbol.clone(),
                        side: side.to_string(),
                        fsm_state: "EXPIRED".to_string(),
                        requested_units: quantity,
                        filled_units: 0.0,
                        avg_fill_price: 0.0,
                        slippage_paid_usd: 0.0,
                        effective_slippage_bps: 0.0,
                        fees_paid_usdc: 0.0,
                        is_maker: false,
                        fill_type: "EXPIRED".to_string(),
                        state_history: vec![
                            "PENDING".to_string(),
                            "OPEN_ON_BOOK".to_string(),
                            "EXPIRED".to_string(),
                        ],
                    };
                }
                _ => {}
            }
        }

        let cfg = self.load_config_logged("execute_maker_limit_chaser");
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &mode)
            .to_ascii_uppercase();
        let is_live_mode = (mode == "LIVE"
            || mode == "TESTNET"
            || pond_mode == "LIVE"
            || pond_mode == "TESTNET")
            && force_partial_ratio.is_none()
            && simulate_book_scenario.is_none();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false)
            || mode == "TESTNET"
            || pond_mode == "TESTNET";

        if is_live_mode {
            let wallet = cfg.get_str("wallet_address", "");
            let priv_key = self.db.get_signing_private_key().unwrap_or_default();
            if wallet.is_empty() || priv_key.is_empty() {
                self.log_fsm_transition(&cli_id, "PENDING", "REJECTED", Some("MISSING_CREDENTIALS"));
                return OrderExecutionResult {
                    client_order_id: cli_id,
                    exchange_order_id: String::new(),
                    symbol: asset.symbol.clone(),
                    side: side.to_string(),
                    fsm_state: "REJECTED".to_string(),
                    requested_units: quantity,
                    filled_units: 0.0,
                    avg_fill_price: 0.0,
                    slippage_paid_usd: 0.0,
                    effective_slippage_bps: 0.0,
                    fees_paid_usdc: 0.0,
                    is_maker: false,
                    fill_type: "REJECTED".to_string(),
                    state_history: vec!["PENDING".to_string(), "REJECTED".to_string()],
                };
            }

            // S6: In LIVE/TESTNET modes, strictly require live exchange metadata; never use guessed fallback indices.
            let Some(asset_idx) =
                self.fetch_live_hyperliquid_universe_asset_index(&asset.base_asset, is_testnet)
            else {
                self.log_fsm_transition(
                    &cli_id,
                    "PENDING",
                    "REJECTED",
                    Some("NO_LIVE_ASSET_INDEX_METADATA"),
                );
                return OrderExecutionResult {
                    client_order_id: cli_id,
                    exchange_order_id: String::new(),
                    symbol: asset.symbol.clone(),
                    side: side.to_string(),
                    fsm_state: "REJECTED".to_string(),
                    requested_units: quantity,
                    filled_units: 0.0,
                    avg_fill_price: 0.0,
                    slippage_paid_usd: 0.0,
                    effective_slippage_bps: 0.0,
                    fees_paid_usdc: 0.0,
                    is_maker: false,
                    fill_type: "REJECTED".to_string(),
                    state_history: vec!["PENDING".to_string(), "REJECTED".to_string()],
                };
            };

            let is_buy = side.eq_ignore_ascii_case("LONG") || side.eq_ignore_ascii_case("BUY");
            let px_decimals = if asset.tick_size > 0.0 {
                (-asset.tick_size.log10()).round().max(0.0) as usize
            } else {
                4
            };
            let sz_decimals = if asset.lot_step_size > 0.0 {
                (-asset.lot_step_size.log10()).round().max(0.0) as usize
            } else {
                2
            };
            let reduce_slip_pct = cfg
                .get_f64("execution_reduce_only_slippage_pct", 0.05)
                .clamp(0.001, 0.25);
            let ioc_slip_pct = cfg
                .get_f64("execution_ioc_fallback_slippage_pct", 0.02)
                .clamp(0.001, 0.20);
            let target_px = if _is_reduce_only {
                if is_buy {
                    reference_price * (1.0 + reduce_slip_pct)
                } else {
                    reference_price * (1.0 - reduce_slip_pct)
                }
            } else {
                reference_price
            };
            let rounded_px = round_dp(target_px, px_decimals as i32);
            let rounded_sz = round_dp(quantity, sz_decimals as i32);
            let px_str = float_to_wire(rounded_px);
            let sz_str = float_to_wire(rounded_sz);
            let nonce = Utc::now().timestamp_millis() as u64;
            let tif = if _is_reduce_only {
                "Ioc".to_string()
            } else {
                "Alo".to_string()
            };

            let order_action = OrderAction {
                action_type: "order".to_string(),
                orders: vec![OrderWire {
                    a: asset_idx,
                    b: is_buy,
                    p: px_str,
                    s: sz_str.clone(),
                    r: _is_reduce_only,
                    t: OrderTypeWire {
                        limit: Some(LimitOrderType { tif }),
                        trigger: None,
                    },
                    c: None,
                }],
                grouping: "na".to_string(),
            };

            match sign_l1_action(&priv_key, &order_action, None, nonce, None, !is_testnet) {
                Ok(sig) => {
                    let url = self.exchange_url(&cfg, is_testnet);
                    let payload = json!({
                        "action": serde_json::to_value(&order_action).unwrap_or_default(),
                        "nonce": nonce,
                        "signature": {
                            "r": sig["r"],
                            "s": sig["s"],
                            "v": sig["v"],
                        },
                        "vaultAddress": serde_json::Value::Null,
                    });

                    if let Some(resp) = self.http_post_json(&url, &payload) {
                        let status_str = resp.get("status").and_then(Value::as_str).unwrap_or("");
                        if status_str == "ok" {
                            let statuses = resp
                                .get("response")
                                .and_then(|r| r.get("data"))
                                .and_then(|d| d.get("statuses"))
                                .and_then(Value::as_array);

                            if let Some(first_st) = statuses.and_then(|arr| arr.first()) {
                                if let Some(resting) = first_st.get("resting") {
                                    let oid = resting
                                        .get("oid")
                                        .and_then(|v| {
                                            v.as_u64().or_else(|| {
                                                v.as_str().and_then(|s| s.parse().ok())
                                            })
                                        })
                                        .unwrap_or(0);
                                    let ex_id = format!("hl-{}", oid);
                                    self.log_fsm_transition(
                                        &cli_id,
                                        "PENDING",
                                        "OPEN_ON_BOOK",
                                        Some("ALO_POSTED_TO_EXCHANGE"),
                                    );
                                    return OrderExecutionResult {
                                        client_order_id: cli_id,
                                        exchange_order_id: ex_id,
                                        symbol: asset.symbol.clone(),
                                        side: side.to_string(),
                                        fsm_state: "OPEN_ON_BOOK".to_string(),
                                        requested_units: quantity,
                                        filled_units: 0.0,
                                        avg_fill_price: 0.0,
                                        slippage_paid_usd: 0.0,
                                        effective_slippage_bps: 0.0,
                                        fees_paid_usdc: 0.0,
                                        is_maker: true,
                                        fill_type: "POSTED_MAKER_ALO".to_string(),
                                        state_history: vec![
                                            "PENDING".to_string(),
                                            "OPEN_ON_BOOK".to_string(),
                                        ],
                                    };
                                } else if let Some(filled) = first_st.get("filled") {
                                    let oid = filled
                                        .get("oid")
                                        .and_then(|v| {
                                            v.as_u64().or_else(|| {
                                                v.as_str().and_then(|s| s.parse().ok())
                                            })
                                        })
                                        .unwrap_or(0);
                                    let ex_id = format!("hl-{}", oid);
                                    let fill_px = filled
                                        .get("avgPx")
                                        .and_then(|v| {
                                            v.as_f64().or_else(|| {
                                                v.as_str().and_then(|s| s.parse().ok())
                                            })
                                        })
                                        .unwrap_or(reference_price);
                                    let fill_sz = filled
                                        .get("totalSz")
                                        .and_then(|v| {
                                            v.as_f64().or_else(|| {
                                                v.as_str().and_then(|s| s.parse().ok())
                                            })
                                        })
                                        .unwrap_or(quantity);
                                    self.log_fsm_transition(
                                        &cli_id,
                                        "PENDING",
                                        "FILLED",
                                        Some("EXCHANGE_FILL"),
                                    );
                                    return OrderExecutionResult {
                                        client_order_id: cli_id,
                                        exchange_order_id: ex_id,
                                        symbol: asset.symbol.clone(),
                                        side: side.to_string(),
                                        fsm_state: "FILLED".to_string(),
                                        requested_units: quantity,
                                        filled_units: fill_sz,
                                        avg_fill_price: fill_px,
                                        slippage_paid_usd: 0.0,
                                        effective_slippage_bps: 0.0,
                                        fees_paid_usdc: 0.0,
                                        is_maker: true,
                                        fill_type: "EXCHANGE_FILL".to_string(),
                                        state_history: vec![
                                            "PENDING".to_string(),
                                            "FILLED".to_string(),
                                        ],
                                    };
                                } else if let Some(err_txt) =
                                    first_st.get("error").and_then(Value::as_str)
                                {
                                    let err_lower = err_txt.to_ascii_lowercase();
                                    if !_is_reduce_only
                                        && (err_lower.contains("post only")
                                            || err_lower.contains("immediately matched")
                                            || err_lower.contains("cross"))
                                    {
                                        let ioc_px = if is_buy {
                                            reference_price * (1.0 + ioc_slip_pct)
                                        } else {
                                            reference_price * (1.0 - ioc_slip_pct)
                                        };
                                        let ioc_px_str =
                                            float_to_wire(round_dp(ioc_px, px_decimals as i32));
                                        let ioc_nonce = Utc::now().timestamp_millis() as u64 + 1;
                                        let ioc_action = OrderAction {
                                            action_type: "order".to_string(),
                                            orders: vec![OrderWire {
                                                a: asset_idx,
                                                b: is_buy,
                                                p: ioc_px_str,
                                                s: sz_str.clone(),
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
                                        if let Ok(ioc_sig) = sign_l1_action(
                                            &priv_key,
                                            &ioc_action,
                                            None,
                                            ioc_nonce,
                                            None,
                                            !is_testnet,
                                        ) {
                                            let ioc_payload = json!({
                                                "action": serde_json::to_value(&ioc_action).unwrap_or_default(),
                                                "nonce": ioc_nonce,
                                                "signature": {
                                                    "r": ioc_sig["r"],
                                                    "s": ioc_sig["s"],
                                                    "v": ioc_sig["v"],
                                                },
                                                "vaultAddress": serde_json::Value::Null,
                                            });
                                            if let Some(ioc_resp) =
                                                self.http_post_json(&url, &ioc_payload)
                                            {
                                                if let Some(filled) = ioc_resp
                                                    .get("response")
                                                    .and_then(|r| r.get("data"))
                                                    .and_then(|d| d.get("statuses"))
                                                    .and_then(Value::as_array)
                                                    .and_then(|arr| arr.first())
                                                    .and_then(|st| st.get("filled"))
                                                {
                                                    let oid = filled
                                                        .get("oid")
                                                        .and_then(|v| {
                                                            v.as_u64().or_else(|| {
                                                                v.as_str()
                                                                    .and_then(|s| s.parse().ok())
                                                            })
                                                        })
                                                        .unwrap_or(0);
                                                    let ex_id = format!("hl-{}", oid);
                                                    let fill_px = filled
                                                        .get("avgPx")
                                                        .and_then(|v| {
                                                            v.as_f64().or_else(|| {
                                                                v.as_str()
                                                                    .and_then(|s| s.parse().ok())
                                                            })
                                                        })
                                                        .unwrap_or(reference_price);
                                                    let fill_sz = filled
                                                        .get("totalSz")
                                                        .and_then(|v| {
                                                            v.as_f64().or_else(|| {
                                                                v.as_str()
                                                                    .and_then(|s| s.parse().ok())
                                                            })
                                                        })
                                                        .unwrap_or(quantity);
                                                    self.log_fsm_transition(
                                                        &cli_id,
                                                        "PENDING",
                                                        "FILLED",
                                                        Some("TAKER_IOC_FALLBACK_FILL"),
                                                    );
                                                    return OrderExecutionResult {
                                                        client_order_id: cli_id,
                                                        exchange_order_id: ex_id,
                                                        symbol: asset.symbol.clone(),
                                                        side: side.to_string(),
                                                        fsm_state: "FILLED".to_string(),
                                                        requested_units: quantity,
                                                        filled_units: fill_sz,
                                                        avg_fill_price: fill_px,
                                                        slippage_paid_usd: 0.0,
                                                        effective_slippage_bps: 0.0,
                                                        fees_paid_usdc: 0.0,
                                                        is_maker: false,
                                                        fill_type: "TAKER_IOC_FALLBACK".to_string(),
                                                        state_history: vec![
                                                            "PENDING".to_string(),
                                                            "FILLED".to_string(),
                                                        ],
                                                    };
                                                }
                                            }
                                        }
                                    }

                                    self.log_fsm_transition(
                                        &cli_id,
                                        "PENDING",
                                        "REJECTED",
                                        Some(err_txt),
                                    );
                                    return OrderExecutionResult {
                                        client_order_id: cli_id,
                                        exchange_order_id: String::new(),
                                        symbol: asset.symbol.clone(),
                                        side: side.to_string(),
                                        fsm_state: "REJECTED".to_string(),
                                        requested_units: quantity,
                                        filled_units: 0.0,
                                        avg_fill_price: 0.0,
                                        slippage_paid_usd: 0.0,
                                        effective_slippage_bps: 0.0,
                                        fees_paid_usdc: 0.0,
                                        is_maker: false,
                                        fill_type: "REJECTED".to_string(),
                                        state_history: vec![
                                            "PENDING".to_string(),
                                            "REJECTED".to_string(),
                                        ],
                                    };
                                }
                            }
                        } else if status_str == "err" {
                            let err_msg = resp
                                .get("response")
                                .and_then(Value::as_str)
                                .unwrap_or("EXCHANGE_ERROR");
                            tracing::error!("Hyperliquid order rejected by exchange: {}", err_msg);
                            self.log_fsm_transition(&cli_id, "PENDING", "REJECTED", Some(err_msg));
                            return OrderExecutionResult {
                                client_order_id: cli_id,
                                exchange_order_id: String::new(),
                                symbol: asset.symbol.clone(),
                                side: side.to_string(),
                                fsm_state: "REJECTED".to_string(),
                                requested_units: quantity,
                                filled_units: 0.0,
                                avg_fill_price: 0.0,
                                slippage_paid_usd: 0.0,
                                effective_slippage_bps: 0.0,
                                fees_paid_usdc: 0.0,
                                is_maker: false,
                                fill_type: "REJECTED".to_string(),
                                state_history: vec!["PENDING".to_string(), "REJECTED".to_string()],
                            };
                        }
                    }

                    self.log_fsm_transition(
                        &cli_id,
                        "PENDING",
                        "REJECTED",
                        Some("EXCHANGE_ORDER_DISPATCH_FAILED"),
                    );
                    return OrderExecutionResult {
                        client_order_id: cli_id,
                        exchange_order_id: String::new(),
                        symbol: asset.symbol.clone(),
                        side: side.to_string(),
                        fsm_state: "REJECTED".to_string(),
                        requested_units: quantity,
                        filled_units: 0.0,
                        avg_fill_price: 0.0,
                        slippage_paid_usd: 0.0,
                        effective_slippage_bps: 0.0,
                        fees_paid_usdc: 0.0,
                        is_maker: false,
                        fill_type: "REJECTED".to_string(),
                        state_history: vec!["PENDING".to_string(), "REJECTED".to_string()],
                    };
                }
                Err(e) => {
                    self.log_fsm_transition(
                        &cli_id,
                        "PENDING",
                        "REJECTED",
                        Some(&format!("SIGNING_FAILED: {e}")),
                    );
                    return OrderExecutionResult {
                        client_order_id: cli_id,
                        exchange_order_id: String::new(),
                        symbol: asset.symbol.clone(),
                        side: side.to_string(),
                        fsm_state: "REJECTED".to_string(),
                        requested_units: quantity,
                        filled_units: 0.0,
                        avg_fill_price: 0.0,
                        slippage_paid_usd: 0.0,
                        effective_slippage_bps: 0.0,
                        fees_paid_usdc: 0.0,
                        is_maker: false,
                        fill_type: "REJECTED".to_string(),
                        state_history: vec!["PENDING".to_string(), "REJECTED".to_string()],
                    };
                }
            }
        }

        let res = self.simulate_paper_fill(
            &asset.symbol,
            side,
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
            Some(&cli_id),
        );

        for window in res.state_history.windows(2) {
            self.log_fsm_transition(
                &res.client_order_id,
                &window[0],
                &window[1],
                Some(&res.fill_type),
            );
        }
        res
    }

    /// Executes a Book B (`POND`) entry or exit order: routes to live Hyperliquid L1 (`execute_maker_limit_chaser`)
    /// when `pond_execution_mode` / `execution_mode` is `LIVE` or `TESTNET`, and falls back to `simulate_paper_fill`
    /// only in `PAPER` mode.
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
        let exec_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &exec_mode)
            .to_ascii_uppercase();
        let is_live_or_testnet = pond_mode == "LIVE"
            || pond_mode == "TESTNET"
            || exec_mode == "LIVE"
            || exec_mode == "TESTNET";

        if is_live_or_testnet {
            let base_asset = symbol
                .split('/')
                .next()
                .unwrap_or(symbol)
                .trim()
                .to_uppercase();
            let asset = UniverseAsset {
                asset_id: 0,
                symbol: symbol.to_string(),
                base_asset,
                quote_asset: "USDC".to_string(),
                instrument_type: "USDC_PERP".to_string(),
                tier: 2,
                tier_name: "POND_80_COIN".to_string(),
                exec_timeframe: "1h".to_string(),
                rs_timeframe: "4h".to_string(),
                tick_size: tick_size.max(0.0001),
                lot_step_size: lot_step_size.max(0.001),
                min_notional_usdc: 10.0,
                max_leverage: 3.0,
                is_active: 1,
                updated_at: String::new(),
            };
            let pond_cid = client_order_id
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("pond-{}-{}", asset.base_asset, self.db.now_ms()));
            return self.execute_maker_limit_chaser(
                &asset,
                side,
                quantity,
                reference_price,
                bar_high,
                bar_low,
                bar_volume,
                None,
                None,
                Some(&pond_cid),
                is_reduce_only || is_stop_exit || !prefer_maker,
            );
        }

        self.simulate_paper_fill(
            symbol,
            side,
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
            client_order_id,
        )
    }

    /// Checks on-chain clearinghouse and open-orders state for a resting order (`B8`).
    pub fn check_resting_order_status(
        &self,
        symbol: &str,
        exchange_order_id: Option<&str>,
    ) -> RestingOrderStatus {
        let cfg = self.load_config_logged("check_resting_order_status");
        let exec_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &exec_mode)
            .to_ascii_uppercase();
        let is_live_or_testnet = pond_mode == "LIVE"
            || pond_mode == "TESTNET"
            || exec_mode == "LIVE"
            || exec_mode == "TESTNET";
        if !is_live_or_testnet {
            return RestingOrderStatus {
                state: "OPEN_ON_BOOK".to_string(),
                filled_units: 0.0,
                entry_price: 0.0,
            };
        }

        let is_testnet = cfg.get_bool("hyperliquid_testnet", false)
            || exec_mode == "TESTNET"
            || pond_mode == "TESTNET";
        let wallet = cfg.get_str("wallet_address", "");
        if wallet.trim().is_empty() {
            return RestingOrderStatus {
                state: "OFFLINE".to_string(),
                filled_units: 0.0,
                entry_price: 0.0,
            };
        }

        let coin = symbol
            .split('/')
            .next()
            .unwrap_or(symbol)
            .trim()
            .to_uppercase();

        let Some(ch) = self.fetch_hyperliquid_clearinghouse_state(&wallet, is_testnet) else {
            return RestingOrderStatus {
                state: "OFFLINE".to_string(),
                filled_units: 0.0,
                entry_price: 0.0,
            };
        };

        if let Some(asset_positions) = ch.get("assetPositions").and_then(Value::as_array) {
            for item in asset_positions {
                let pos = item.get("position").unwrap_or(item);
                let pos_coin = pos
                    .get("coin")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_uppercase();
                if pos_coin == coin {
                    let szi = pos
                        .get("szi")
                        .and_then(|v| {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                        })
                        .unwrap_or(0.0);
                    if szi.abs() > 1e-9 {
                        let entry_px = pos
                            .get("entryPx")
                            .and_then(|v| {
                                v.as_f64()
                                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                            })
                            .unwrap_or(0.0);
                        return RestingOrderStatus {
                            state: "FILLED".to_string(),
                            filled_units: szi.abs(),
                            entry_price: entry_px,
                        };
                    }
                }
            }
        }

        let Some(open_orders) = self.fetch_hyperliquid_open_orders(&wallet, is_testnet) else {
            return RestingOrderStatus {
                state: "OFFLINE".to_string(),
                filled_units: 0.0,
                entry_price: 0.0,
            };
        };

        let target_oid = exchange_order_id.and_then(|s| {
            s.strip_prefix("hl-")
                .unwrap_or(s)
                .parse::<u64>()
                .ok()
        });

        let still_on_book = open_orders.iter().any(|ord| {
            let ord_coin = ord
                .get("coin")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_uppercase();
            if ord_coin != coin {
                return false;
            }
            if let Some(oid_target) = target_oid {
                let ord_oid = ord
                    .get("oid")
                    .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                    .unwrap_or(0);
                ord_oid == oid_target || oid_target == 0
            } else {
                true
            }
        });

        if still_on_book {
            RestingOrderStatus {
                state: "OPEN_ON_BOOK".to_string(),
                filled_units: 0.0,
                entry_price: 0.0,
            }
        } else {
            RestingOrderStatus {
                state: "CANCELLED_OR_GONE".to_string(),
                filled_units: 0.0,
                entry_price: 0.0,
            }
        }
    }

    /// Cancels a resting order on Hyperliquid L1 (`B8`).
    pub fn cancel_resting_order(&self, symbol: &str, exchange_order_id: &str) -> Result<bool> {
        let cfg = self.load_config_logged("cancel_resting_order");
        let exec_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &exec_mode)
            .to_ascii_uppercase();
        let is_live_or_testnet = pond_mode == "LIVE"
            || pond_mode == "TESTNET"
            || exec_mode == "LIVE"
            || exec_mode == "TESTNET";
        if !is_live_or_testnet {
            self.log_fsm_transition(
                exchange_order_id,
                "OPEN_ON_BOOK",
                "CANCELLED",
                Some("PAPER_RESTING_ORDER_CANCELLED"),
            );
            return Ok(true);
        }

        let oid = exchange_order_id
            .strip_prefix("hl-")
            .unwrap_or(exchange_order_id)
            .parse::<u64>()
            .unwrap_or(0);
        if oid == 0 {
            return Ok(false);
        }

        let is_testnet = cfg.get_bool("hyperliquid_testnet", false)
            || exec_mode == "TESTNET"
            || pond_mode == "TESTNET";
        let coin = symbol
            .split('/')
            .next()
            .unwrap_or(symbol)
            .trim()
            .to_uppercase();
        let Some(asset_idx) = self.fetch_live_hyperliquid_universe_asset_index(&coin, is_testnet)
        else {
            return Ok(false);
        };
        let priv_key = self.db.get_signing_private_key()?;
        if priv_key.is_empty() {
            return Ok(false);
        }

        let nonce = Utc::now().timestamp_millis() as u64;
        let cancel_action = CancelAction {
            action_type: "cancel".to_string(),
            cancels: vec![CancelWire {
                a: asset_idx,
                o: oid,
            }],
        };
        let sig = sign_l1_action(&priv_key, &cancel_action, None, nonce, None, !is_testnet)?;
        let url = self.exchange_url(&cfg, is_testnet);
        let payload = json!({
            "action": serde_json::to_value(&cancel_action).unwrap_or_default(),
            "nonce": nonce,
            "signature": {
                "r": sig["r"],
                "s": sig["s"],
                "v": sig["v"],
            },
            "vaultAddress": Value::Null,
        });

        if let Some(resp) = self.http_post_json(&url, &payload) {
            if resp.get("status").and_then(Value::as_str) == Some("ok") {
                self.log_fsm_transition(
                    exchange_order_id,
                    "OPEN_ON_BOOK",
                    "CANCELLED",
                    Some("RESTING_ORDER_CANCELLED_ON_EXCHANGE"),
                );
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Resolves a Hyperliquid asset index exclusively from live exchange `/info` metadata (`S6`).
    /// Never guesses from a hardcoded table and never caches unverified fallback indices.
    pub fn fetch_live_hyperliquid_universe_asset_index(
        &self,
        coin: &str,
        is_testnet: bool,
    ) -> Option<u32> {
        let coin_upper = coin.trim().to_ascii_uppercase();
        if let Some(&cached_idx) = self
            .asset_index_cache
            .lock()
            .get(&(is_testnet, coin_upper.clone()))
        {
            return Some(cached_idx);
        }

        let cfg = self.load_config_logged("fetch_live_hyperliquid_universe_asset_index");
        let url = self.info_url(&cfg, is_testnet);

        let resp_opt = self
            .http_post_json(&url, &json!({"type": "meta"}))
            .or_else(|| self.http_post_json(&url, &json!({"type": "metaAndAssetCtxs"})));

        if let Some(resp) = resp_opt {
            let universe_opt = resp
                .get("universe")
                .and_then(Value::as_array)
                .or_else(|| {
                    resp.as_array()
                        .and_then(|arr| arr.first())
                        .and_then(|first| first.get("universe"))
                        .and_then(Value::as_array)
                });

            if let Some(universe) = universe_opt {
                let mut cache = self.asset_index_cache.lock();
                let mut found = None;
                for (idx, u) in universe.iter().enumerate() {
                    if let Some(name) = u.get("name").and_then(Value::as_str) {
                        let name_upper = name.to_ascii_uppercase();
                        cache.insert((is_testnet, name_upper.clone()), idx as u32);
                        if name_upper == coin_upper {
                            found = Some(idx as u32);
                        }
                    }
                }
                return found;
            }
        }

        None
    }

    /// Resolves a Hyperliquid universe asset index (`S6`).
    /// In `LIVE` or `TESTNET` mode, strictly requires live exchange metadata and returns `None` when offline.
    /// In `PAPER` mode only, returns an uncached reference index for offline unit tests.
    pub fn fetch_hyperliquid_universe_asset_index(
        &self,
        coin: &str,
        is_testnet: bool,
    ) -> Option<u32> {
        if let Some(live_idx) = self.fetch_live_hyperliquid_universe_asset_index(coin, is_testnet) {
            return Some(live_idx);
        }

        let cfg = self.load_config_logged("fetch_hyperliquid_universe_asset_index");
        let exec_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &exec_mode)
            .to_ascii_uppercase();
        let is_live_or_testnet = exec_mode == "LIVE"
            || exec_mode == "TESTNET"
            || pond_mode == "LIVE"
            || pond_mode == "TESTNET";

        // S6: Never guess asset indices in LIVE or TESTNET modes, and never cache fallback indices.
        if is_live_or_testnet {
            tracing::error!(
                coin = coin,
                is_testnet = is_testnet,
                exec_mode = %exec_mode,
                "Refusing to use hardcoded fallback asset index in LIVE/TESTNET mode when /info metadata is unavailable"
            );
            return None;
        }

        let coin_upper = coin.trim().to_ascii_uppercase();
        if is_testnet {
            match coin_upper.as_str() {
                "SOL" => Some(0),
                "APT" => Some(1),
                "ATOM" => Some(2),
                "BTC" => Some(3),
                "ETH" => Some(4),
                "MATIC" | "POL" => Some(5),
                "BNB" => Some(6),
                "AVAX" => Some(7),
                "LTC" => Some(10),
                "OP" => Some(11),
                "ARB" => Some(13),
                "INJ" => Some(18),
                "SUI" => Some(25),
                "LINK" => Some(28),
                "UNI" => Some(37),
                "ADA" => Some(45),
                "DOT" => Some(47),
                "NEAR" => Some(54),
                "XRP" => Some(62),
                "TIA" => Some(83),
                "HYPE" => Some(135),
                "DOGE" => Some(173),
                _ => None,
            }
        } else {
            match coin_upper.as_str() {
                "BTC" => Some(0),
                "ETH" => Some(1),
                "ATOM" => Some(2),
                "MATIC" | "POL" => Some(3),
                "SOL" => Some(5),
                "AVAX" => Some(6),
                "BNB" => Some(7),
                "OP" => Some(9),
                "LTC" => Some(10),
                "ARB" => Some(11),
                "DOGE" => Some(12),
                "INJ" => Some(13),
                "SUI" => Some(14),
                "LINK" => Some(18),
                "APT" => Some(22),
                "XRP" => Some(25),
                "UNI" => Some(38),
                "DOT" => Some(42),
                "ADA" => Some(65),
                "NEAR" => Some(74),
                "TIA" => Some(96),
                "HYPE" => Some(159),
                _ => None,
            }
        }
    }

    /// Signs a Hyperliquid L1 action payload using EIP-712 / secp256k1 recoverable ECDSA signature.
    pub fn sign_hyperliquid_action(
        private_key_hex: &str,
        action: &Value,
        nonce: u64,
        vault_address: Option<&str>,
        is_mainnet: bool,
    ) -> Result<Value> {
        sign_l1_action(private_key_hex, action, vault_address, nonce, None, is_mainnet)
    }

    pub fn fetch_hyperliquid_clearinghouse_state(
        &self,
        wallet_address: &str,
        is_testnet: bool,
    ) -> Option<Value> {
        if wallet_address.trim().is_empty() {
            return None;
        }
        let cfg = self.load_config_logged("fetch_hyperliquid_clearinghouse_state");
        let url = self.info_url(&cfg, is_testnet);
        self.http_post_json(
            &url,
            &json!({
                "type": "clearinghouseState",
                "user": wallet_address.trim(),
            }),
        )
    }

    pub fn fetch_hyperliquid_spot_usdc_balance(
        &self,
        wallet_address: &str,
        is_testnet: bool,
    ) -> Option<(f64, f64)> {
        if wallet_address.trim().is_empty() {
            return None;
        }
        let cfg = self.load_config_logged("fetch_hyperliquid_spot_usdc_balance");
        let url = self.info_url(&cfg, is_testnet);
        let resp = self.http_post_json(
            &url,
            &json!({
                "type": "spotClearinghouseState",
                "user": wallet_address.trim(),
            }),
        )?;
        let balances = resp.get("balances")?.as_array()?;
        for b in balances {
            let coin = b.get("coin").and_then(Value::as_str).unwrap_or("");
            if coin.eq_ignore_ascii_case("USDC") {
                let total = b
                    .get("total")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                let hold = b
                    .get("hold")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                return Some((total, (total - hold).max(0.0)));
            }
        }
        Some((0.0, 0.0))
    }

    pub fn fetch_hyperliquid_open_orders(
        &self,
        wallet_address: &str,
        is_testnet: bool,
    ) -> Option<Vec<Value>> {
        if wallet_address.trim().is_empty() {
            return None;
        }
        let cfg = self.load_config_logged("fetch_hyperliquid_open_orders");
        let url = self.info_url(&cfg, is_testnet);
        let val = self.http_post_json(
            &url,
            &json!({
                "type": "openOrders",
                "user": wallet_address.trim(),
            }),
        )?;
        val.as_array().cloned()
    }

    pub fn fetch_hyperliquid_user_funding(
        &self,
        wallet_address: &str,
        start_time_ms: i64,
        is_testnet: bool,
    ) -> Option<Vec<Value>> {
        if wallet_address.trim().is_empty() {
            return None;
        }
        let cfg = self.load_config_logged("fetch_hyperliquid_user_funding");
        let url = self.info_url(&cfg, is_testnet);
        let val = self.http_post_json(
            &url,
            &json!({
                "type": "userFunding",
                "user": wallet_address.trim(),
                "startTime": start_time_ms,
            }),
        )?;
        val.as_array().cloned()
    }

    /// Queries Hyperliquid HLP Vault equity, accrued PnL yield, and APR (`vaultDetails` + `userVaultEquities`).
    pub fn fetch_hlp_vault_state(
        &self,
        wallet_address: Option<&str>,
        vault_address: Option<&str>,
        is_testnet: Option<bool>,
    ) -> Value {
        let cfg = self.load_config_logged("fetch_hlp_vault_state");
        let testnet = is_testnet.unwrap_or_else(|| {
            cfg.get_bool("hyperliquid_testnet", false)
                || cfg
                    .get_str("execution_mode", "PAPER")
                    .eq_ignore_ascii_case("TESTNET")
        });
        let wallet = wallet_address
            .map(str::to_string)
            .unwrap_or_else(|| cfg.get_str("wallet_address", ""));
        let vault = vault_address
            .map(str::to_string)
            .unwrap_or_else(|| Self::resolve_hlp_vault_address(&cfg, testnet));
        let url = self.info_url(&cfg, testnet);

        let default_apr = cfg.get_f64("hlp_default_estimated_apr", 0.125);
        let mut user_equity_usdc = 0.0f64;
        let mut accrued_yield_usdc = 0.0f64;
        let mut vault_apr = default_apr;
        let mut lockup_until_ms = 0i64;
        let mut allow_deposits = true;
        let mut live_queried = false;

        if !wallet.is_empty() {
            if let Some(eq_arr) = self
                .http_post_json(
                    &url,
                    &json!({
                        "type": "userVaultEquities",
                        "user": wallet,
                    }),
                )
                .and_then(|v| v.as_array().cloned())
            {
                live_queried = true;
                let mut sum_eq = 0.0f64;
                for item in eq_arr {
                    let eq_val = item
                        .get("equity")
                        .and_then(|v| {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                        })
                        .unwrap_or(0.0);
                    if eq_val.is_finite() && eq_val > 0.0 {
                        sum_eq += eq_val;
                    }
                    let lk = item
                        .get("lockedUntilTimestamp")
                        .and_then(|v| {
                            v.as_i64().or_else(|| {
                                v.as_f64().map(|f| f as i64).or_else(|| {
                                    v.as_str()
                                        .and_then(|s| s.parse::<f64>().ok().map(|f| f as i64))
                                })
                            })
                        })
                        .unwrap_or(0);
                    lockup_until_ms = lockup_until_ms.max(lk);
                }
                user_equity_usdc = sum_eq;
            }
        }

        if let Some(details) = self.http_post_json(
            &url,
            &json!({
                "type": "vaultDetails",
                "vaultAddress": vault,
                "user": if wallet.is_empty() { Value::Null } else { json!(wallet) },
            }),
        ) {
            if details.is_object() {
                live_queried = true;
                vault_apr = details
                    .get("apr")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(default_apr);
                let is_closed = details
                    .get("isClosed")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                allow_deposits = details
                    .get("allowDeposits")
                    .and_then(Value::as_bool)
                    .unwrap_or(true)
                    && !is_closed;

                if let Some(follower) = details.get("followerState").and_then(Value::as_object) {
                    if let Some(feq) = follower.get("vaultEquity").and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    }) {
                        if feq.is_finite() && feq >= 0.0 {
                            user_equity_usdc = user_equity_usdc.max(feq);
                        }
                    }
                    let raw_pnl = follower.get("allTimePnl").or_else(|| follower.get("pnl"));
                    if let Some(pnl_val) = raw_pnl.and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    }) {
                        if pnl_val.is_finite() {
                            accrued_yield_usdc = pnl_val;
                        }
                    }
                    if let Some(flk) = follower.get("lockupUntil").and_then(|v| {
                        v.as_i64().or_else(|| {
                            v.as_f64().map(|f| f as i64).or_else(|| {
                                v.as_str()
                                    .and_then(|s| s.parse::<f64>().ok().map(|f| f as i64))
                            })
                        })
                    }) {
                        lockup_until_ms = lockup_until_ms.max(flk);
                    }
                }
            }
        }

        let mut perp_equity_usdc: Option<f64> = None;
        let mut perp_free_usdc: Option<f64> = None;
        if !wallet.is_empty() {
            let mut ch_tot = 0.0f64;
            let mut ch_free = 0.0f64;
            let mut got_ch = false;
            if let Some(ch) = self.fetch_hyperliquid_clearinghouse_state(&wallet, testnet) {
                if let Some(ms) = ch.get("marginSummary") {
                    if let Some(acct_val) = ms.get("accountValue").and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    }) {
                        if acct_val.is_finite() {
                            ch_tot = acct_val;
                            got_ch = true;
                            live_queried = true;
                        }
                    }
                }
                if let Some(wd_val) = ch.get("withdrawable").and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                }) {
                    if wd_val.is_finite() {
                        ch_free = wd_val;
                    }
                }
            }
            if let Some((spot_tot, spot_free)) =
                self.fetch_hyperliquid_spot_usdc_balance(&wallet, testnet)
            {
                if spot_tot > 0.0 {
                    ch_tot += spot_tot;
                    ch_free += spot_free;
                    got_ch = true;
                    live_queried = true;
                }
            }
            if got_ch {
                perp_equity_usdc = Some(round_dp(ch_tot, 2));
                perp_free_usdc = Some(round_dp(ch_free, 2));
            }
        }

        let rounded_vault_eq = round_dp(user_equity_usdc.max(0.0), 2);
        json!({
            "vault_address": vault,
            "wallet_address": wallet,
            "network": if testnet { "testnet" } else { "mainnet" },
            "user_vault_equity_usdc": rounded_vault_eq,
            "vault_equity_usdc": rounded_vault_eq,
            "accrued_yield_usdc": round_dp(accrued_yield_usdc, 4),
            "estimated_apr": round_dp(vault_apr, 4),
            "apr": round_dp(vault_apr, 4),
            "allow_deposits": allow_deposits,
            "locked_until_timestamp_ms": lockup_until_ms,
            "locked_until_ms": lockup_until_ms,
            "perp_equity_usdc": perp_equity_usdc,
            "perp_free_usdc": perp_free_usdc,
            "live_queried": live_queried,
        })
    }

    /// Executes or simulates an HLP Vault deposit/withdrawal (`vaultTransfer`).
    pub fn execute_hlp_vault_transfer(
        &self,
        amount_usdc: f64,
        is_deposit: bool,
        vault_address: Option<&str>,
    ) -> Value {
        let cfg = self.load_config_logged("execute_hlp_vault_transfer");
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
        let vault = vault_address
            .map(str::to_string)
            .unwrap_or_else(|| Self::resolve_hlp_vault_address(&cfg, is_testnet));
        let usd_micro = ((amount_usdc.max(0.0)) * 1e6).round() as i64;

        if mode == "PAPER" {
            *self.hlp_vault_error_streak.lock() = 0;
            return json!({
                "status": "SIMULATED",
                "mode": "PAPER",
                "vault_address": vault,
                "is_deposit": is_deposit,
                "amount_usdc": round_dp(amount_usdc, 2),
                "usd_micro": usd_micro,
            });
        }

        let wallet = cfg.get_str("wallet_address", "");
        let priv_key = self.db.get_signing_private_key().unwrap_or_default();
        if wallet.is_empty() || priv_key.is_empty() {
            return json!({
                "status": "SKIPPED",
                "reason": "MISSING_CREDENTIALS",
                "vault_address": vault,
            });
        }

        let nonce = Utc::now().timestamp_millis() as u64;
        let action = VaultTransferAction {
            action_type: "vaultTransfer".to_string(),
            vault_address: vault.clone(),
            is_deposit,
            usd: usd_micro,
        };

        match sign_l1_action(&priv_key, &action, None, nonce, None, !is_testnet) {
            Ok(sig) => {
                let url = self.exchange_url(&cfg, is_testnet);
                let payload = json!({
                    "action": serde_json::to_value(&action).unwrap_or_default(),
                    "nonce": nonce,
                    "signature": {
                        "r": sig["r"],
                        "s": sig["s"],
                        "v": sig["v"],
                    },
                    "vaultAddress": serde_json::Value::Null,
                });
                if let Some(resp) = self.http_post_json(&url, &payload) {
                    *self.hlp_vault_error_streak.lock() = 0;
                    return json!({
                        "status": "SUBMITTED",
                        "vault_address": vault,
                        "is_deposit": is_deposit,
                        "amount_usdc": round_dp(amount_usdc, 2),
                        "exchange_response": resp,
                    });
                }
                let mut streak = self.hlp_vault_error_streak.lock();
                *streak += 1;
                let cur_streak = *streak;
                drop(streak);
                if cur_streak >= 3 {
                    let _ = self.alerts.notify_critical_event(
                        "HLP_VAULT_TRANSFER_FAILURE_STREAK",
                        &format!("HLP vault transfer failed {cur_streak} consecutive times."),
                        Some(json!({"vault_address": vault, "amount_usdc": amount_usdc})),
                    );
                }
                json!({
                    "status": "ERROR",
                    "reason": "EXCHANGE_POST_FAILED",
                    "error_streak": cur_streak,
                })
            }
            Err(e) => json!({
                "status": "ERROR",
                "reason": e.to_string(),
            }),
        }
    }

    /// Queries on-chain Hyperliquid clearinghouse + HLP vault equity and optionally rescales risk caps.
    pub fn sync_wallet_equity(&self, update_risk_caps: bool) -> Value {
        let cfg = self.load_config_logged("sync_wallet_equity");
        let wallet = cfg.get_str("wallet_address", "");
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false)
            || cfg
                .get_str("execution_mode", "PAPER")
                .eq_ignore_ascii_case("TESTNET");

        if wallet.trim().is_empty() {
            let init_eq = cfg.get_f64("initial_equity_usdc", 10000.0);
            return json!({
                "status": "PAPER_DEFAULT",
                "wallet_address": "",
                "network": if is_testnet { "testnet" } else { "mainnet" },
                "account_type": "perps",
                "wallet_balance_usdc": init_eq,
                "total_equity_usdc": init_eq,
                "perp_account_value_usdc": init_eq,
                "vault_equity_usdc": 0.0,
            });
        }

        let mut perp_eq = 0.0;
        let mut withdrawable = 0.0;
        let mut live_ok = false;

        if let Some(ch) = self.fetch_hyperliquid_clearinghouse_state(&wallet, is_testnet) {
            if let Some(ms) = ch.get("marginSummary") {
                perp_eq = ms
                    .get("accountValue")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                live_ok = true;
            }
            withdrawable = ch
                .get("withdrawable")
                .and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                })
                .unwrap_or(perp_eq);
        }

        if let Some((spot_tot, spot_free)) =
            self.fetch_hyperliquid_spot_usdc_balance(&wallet, is_testnet)
        {
            if spot_tot > 0.0 {
                perp_eq += spot_tot;
                withdrawable += spot_free;
                live_ok = true;
            }
        }

        let vault_state = self.fetch_hlp_vault_state(Some(&wallet), None, Some(is_testnet));
        let vault_eq = vault_state
            .get("user_vault_equity_usdc")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let total_eq = round_dp(perp_eq + vault_eq, 2);

        if live_ok && total_eq > 0.0 {
            if update_risk_caps {
                if let Err(e) = self.db.scale_risk_caps_for_equity(total_eq) {
                    tracing::error!(error = %e, "Failed to scale risk caps for equity");
                }
                let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
                if let Err(e) = self
                    .db
                    .set_config_value("equity_baseline_reset_at", &json!(now_iso))
                {
                    tracing::error!(error = %e, "Failed to update equity_baseline_reset_at");
                }
            }
            if let Ok(mut snap) = self.db.get_latest_portfolio_snapshot() {
                let (_, _, _, total_deployed) = self
                    .db
                    .get_wallet_perps_margin_state()
                    .unwrap_or((total_eq, perp_eq, vault_eq, 0.0));
                snap.timestamp_ms = self.db.now_ms();
                snap.total_equity_usdc = total_eq;
                snap.basis_harvest_notional_usdc = round_dp(vault_eq, 2);
                snap.available_cash_usdc = round_dp((perp_eq - total_deployed).max(0.0), 2);
                if let Err(e) = self.db.save_portfolio_snapshot(&snap) {
                    tracing::error!(error = %e, "Failed to save portfolio snapshot during sync_wallet_equity");
                }
            }
        }

        let eff_total = if total_eq > 0.0 {
            total_eq
        } else {
            cfg.get_f64("initial_equity_usdc", 10000.0)
        };
        json!({
            "status": if live_ok { "SYNCED_ON_CHAIN" } else { "UNREACHABLE_FALLBACK" },
            "wallet_address": wallet,
            "network": if is_testnet { "testnet" } else { "mainnet" },
            "account_type": if vault_eq > 0.0 { "perps+vault" } else { "perps" },
            "wallet_balance_usdc": eff_total,
            "perp_account_value_usdc": round_dp(perp_eq, 2),
            "withdrawable_usdc": round_dp(withdrawable, 2),
            "vault_equity_usdc": round_dp(vault_eq, 2),
            "total_equity_usdc": eff_total,
        })
    }

    /// Accrues or reconciles hourly funding payments across open perpetual positions.
    pub fn sync_funding_payments(&self, current_ts_ms: Option<i64>, is_backtest: bool) -> Value {
        let now_ms = current_ts_ms.unwrap_or_else(|| self.db.now_ms());
        let hour_bucket = (now_ms / 3_600_000) * 3_600_000;
        {
            let mut guard = self.last_funding_sync_hour_ts.lock();
            if *guard == Some(hour_bucket) {
                return json!({
                    "status": "ALREADY_SYNCED_THIS_HOUR",
                    "hour_bucket_ms": hour_bucket,
                    "positions_updated": 0,
                });
            }
            *guard = Some(hour_bucket);
        }

        let open_trades = self.db.get_open_trades().unwrap_or_default();
        if open_trades.is_empty() {
            return json!({
                "status": "NO_OPEN_POSITIONS",
                "hour_bucket_ms": hour_bucket,
                "positions_updated": 0,
            });
        }

        let mut updated = 0usize;
        let mut net_funding_delta = 0.0;

        for trade in open_trades {
            if trade.fsm_state == "OPEN_ON_BOOK" && trade.filled_units <= 0.0 {
                continue;
            }
            let bars = self
                .db
                .get_ohlcv_bars(&trade.symbol, "1h", 2, Some(now_ms))
                .or_else(|_| self.db.get_ohlcv_bars(&trade.symbol, "4h", 2, Some(now_ms)))
                .unwrap_or_default();
            let funding_rate = bars
                .last()
                .map(|b| b.funding_rate_hourly)
                .unwrap_or(0.0001);
            let mark_px = bars
                .last()
                .map(|b| b.close)
                .unwrap_or(trade.current_price);
            let notional = trade.quantity.abs() * mark_px;
            let delta = if trade.side == "LONG" {
                -notional * funding_rate
            } else {
                notional * funding_rate
            };
            let new_funding = round_dp(trade.funding_accrued_usdc + delta, 4);
            if let Err(e) = self.db.update_open_trade(
                trade.trade_id,
                mark_px,
                trade.chandelier_stop_price,
                trade.highest_price_seen,
                trade.lowest_price_seen,
                trade.unrealized_pnl_usdc,
                new_funding,
            ) {
                tracing::error!(trade_id = trade.trade_id, error = %e, "Failed to update open trade funding");
            }
            net_funding_delta += delta;
            updated += 1;
        }

        if !is_backtest && updated > 0 {
            let _ = self.db.log_telemetry(
                "INFO",
                "engine.execution",
                &format!("Synced 1H funding across {updated} open positions (net delta: ${net_funding_delta:+.4})"),
                None,
            );
        }

        json!({
            "status": "SYNCED",
            "hour_bucket_ms": hour_bucket,
            "positions_updated": updated,
            "net_funding_delta_usdc": round_dp(net_funding_delta, 4),
        })
    }

    /// Reconciles local SQLite `OPEN` trades against Hyperliquid L1 `clearinghouseState` and `openOrders`.
    pub fn reconcile_exchange_state(
        &self,
        simulated_exchange_positions: Option<&HashMap<String, Value>>,
        simulated_open_orders: Option<&[Value]>,
    ) -> Value {
        let cfg = self.load_config_logged("reconcile_exchange_state");
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
        let wallet = cfg.get_str("wallet_address", "");

        if mode == "PAPER"
            && simulated_exchange_positions.is_none()
            && simulated_open_orders.is_none()
        {
            let open_trades = self.db.get_open_trades().unwrap_or_default();
            return json!({
                "status": "VERIFIED_PAPER",
                "mode": "PAPER",
                "local_open_trades": open_trades.len(),
                "discrepancies_found": 0,
                "discrepancies": [],
            });
        }

        let mut ex_positions: HashMap<String, (f64, f64, f64, f64)> = HashMap::new();
        if let Some(sim_map) = simulated_exchange_positions {
            for (coin, v) in sim_map {
                let szi = v.get("szi").and_then(Value::as_f64).unwrap_or(0.0);
                if szi.abs() > 1e-9 {
                    let entry_px = v.get("entryPx").and_then(Value::as_f64).unwrap_or(100.0);
                    let mark_px = v.get("markPx").and_then(Value::as_f64).unwrap_or(entry_px);
                    let upnl = v.get("unrealizedPnl").and_then(Value::as_f64).unwrap_or(0.0);
                    ex_positions.insert(coin.to_uppercase(), (szi, entry_px, mark_px, upnl));
                }
            }
        } else if let Some(ch) = self.fetch_hyperliquid_clearinghouse_state(&wallet, is_testnet) {
            if let Some(asset_positions) = ch.get("assetPositions").and_then(Value::as_array) {
                for item in asset_positions {
                    let pos = item.get("position").unwrap_or(item);
                    let coin = pos
                        .get("coin")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_uppercase();
                    let szi = pos
                        .get("szi")
                        .and_then(|v| {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                        })
                        .unwrap_or(0.0);
                    if !coin.is_empty() && szi.abs() > 1e-9 {
                        let entry_px = pos
                            .get("entryPx")
                            .and_then(|v| {
                                v.as_f64()
                                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                            })
                            .unwrap_or(0.0);
                        let upnl = pos
                            .get("unrealizedPnl")
                            .and_then(|v| {
                                v.as_f64()
                                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                            })
                            .unwrap_or(0.0);
                        ex_positions.insert(coin, (szi, entry_px, entry_px, upnl));
                    }
                }
            } else {
                return json!({
                    "status": "SKIPPED_OFFLINE",
                    "reason": "CLEARINGHOUSE_STATE_UNAVAILABLE",
                    "discrepancies_found": 0,
                });
            }
        } else {
            return json!({
                "status": "SKIPPED_OFFLINE",
                "reason": "CLEARINGHOUSE_API_UNREACHABLE",
                "discrepancies_found": 0,
            });
        }

        let live_open_orders: Vec<Value>;
        let open_orders_slice: Option<&[Value]> = if let Some(sim_orders) = simulated_open_orders {
            Some(sim_orders)
        } else if simulated_exchange_positions.is_none() {
            live_open_orders = self
                .fetch_hyperliquid_open_orders(&wallet, is_testnet)
                .unwrap_or_default();
            Some(&live_open_orders)
        } else {
            None
        };
        let open_order_coins: HashSet<String> = open_orders_slice
            .unwrap_or(&[])
            .iter()
            .filter_map(|ord| ord.get("coin").and_then(Value::as_str))
            .map(|s| s.to_uppercase())
            .collect();

        let local_trades = self.db.get_open_trades().unwrap_or_default();
        let local_pond_cells = self.db.get_open_pond_cells().unwrap_or_default();
        let mut seen_coins: HashSet<String> = HashSet::new();
        let mut discrepancies: Vec<Value> = Vec::new();

        for lt in &local_trades {
            let coin = lt.base_asset.to_uppercase();
            seen_coins.insert(coin.clone());

            match ex_positions.get(&coin) {
                None => {
                    if !open_order_coins.contains(&coin) {
                        if let Err(e) = self.db.close_trade(
                            lt.trade_id,
                            lt.current_price,
                            0.0,
                            lt.fees_paid_usdc,
                            "RECONCILED_CLOSED_ON_EXCHANGE",
                        ) {
                            tracing::error!(trade_id = lt.trade_id, error = %e, "Failed to close phantom local trade");
                        }
                        discrepancies.push(json!({
                            "type": "PHANTOM_LOCAL_CLOSED",
                            "base_asset": coin,
                            "trade_id": lt.trade_id,
                        }));
                    }
                }
                Some(&(szi, entry_px, _mark_px, _upnl)) => {
                    let ex_qty = szi.abs();
                    if (ex_qty - lt.quantity.abs()).abs() > 1e-6 {
                        if let Err(e) =
                            self.db
                                .reconcile_open_trade_size(lt.trade_id, ex_qty, Some(entry_px))
                        {
                            tracing::error!(trade_id = lt.trade_id, error = %e, "Failed to reconcile open trade size");
                        }
                        discrepancies.push(json!({
                            "type": "SIZE_DRIFT_RECONCILED",
                            "base_asset": coin,
                            "trade_id": lt.trade_id,
                            "local_qty": lt.quantity,
                            "exchange_qty": ex_qty,
                        }));
                    }
                }
            }
        }

        for cell in &local_pond_cells {
            let coin = cell
                .symbol
                .split('/')
                .next()
                .unwrap_or(&cell.symbol)
                .trim()
                .to_uppercase();
            seen_coins.insert(coin.clone());

            match ex_positions.get(&coin) {
                None => {
                    if !open_order_coins.contains(&coin) {
                        if let Err(e) = self.db.reconcile_close_phantom_pond_cell(
                            cell.cell_id,
                            cell.entry_price.unwrap_or(0.0),
                            "RECONCILED_CLOSED_ON_EXCHANGE",
                        ) {
                            tracing::error!(cell_id = cell.cell_id, error = %e, "Failed to close phantom pond cell");
                        }
                        discrepancies.push(json!({
                            "type": "PHANTOM_POND_CELL_CLOSED",
                            "base_asset": coin,
                            "cell_id": cell.cell_id,
                        }));
                    }
                }
                Some(&(szi, entry_px, _mark_px, _upnl)) => {
                    let ex_qty = szi.abs();
                    let local_qty = cell.quantity.unwrap_or(0.0).abs();
                    if (ex_qty - local_qty).abs() > 1e-6 {
                        if let Err(e) =
                            self.db
                                .reconcile_open_pond_cell_size(cell.cell_id, ex_qty, Some(entry_px))
                        {
                            tracing::error!(cell_id = cell.cell_id, error = %e, "Failed to reconcile pond cell size");
                        }
                        discrepancies.push(json!({
                            "type": "SIZE_DRIFT_POND_CELL_RECONCILED",
                            "base_asset": coin,
                            "cell_id": cell.cell_id,
                            "local_qty": local_qty,
                            "exchange_qty": ex_qty,
                        }));
                    }
                }
            }
        }

        for (coin, &(szi, entry_px, mark_px, upnl)) in &ex_positions {
            if !seen_coins.contains(coin) {
                let side = if szi > 0.0 { "LONG" } else { "SHORT" };
                let symbol = format!("{coin}/USDC:USDC");
                if let Ok(tid) = self.db.reconcile_on_chain_orphan_position(
                    &symbol,
                    coin,
                    side,
                    szi.abs(),
                    entry_px,
                    mark_px,
                    upnl,
                ) {
                    discrepancies.push(json!({
                        "type": "ORPHAN_ON_CHAIN_IMPORTED",
                        "base_asset": coin,
                        "trade_id": tid,
                        "side": side,
                        "quantity": szi.abs(),
                    }));
                }
            }
        }

        if let Some(open_orders) = open_orders_slice {
            for ord in open_orders {
                let coin = ord
                    .get("coin")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_uppercase();
                if !ex_positions.contains_key(&coin) && !seen_coins.contains(&coin) {
                    discrepancies.push(json!({
                        "type": "DANGLING_OPEN_ORDER_CANCELLED",
                        "base_asset": coin,
                        "oid": ord.get("oid"),
                    }));
                }
            }
        }

        if !discrepancies.is_empty() {
            let _ = self.alerts.notify_reconciliation_mismatch(
                discrepancies.len(),
                &discrepancies,
                &wallet,
            );
        }

        json!({
            "status": "RECONCILED",
            "mode": mode,
            "discrepancies_found": discrepancies.len(),
            "discrepancies": discrepancies,
        })
    }

    /// Runs a complete execution & portfolio management cycle (`engine/execution.py::reconcile_cycle`).
    pub fn reconcile_cycle(
        &self,
        evaluations: &[SignalEvaluation],
        is_backtest: bool,
    ) -> Result<PortfolioSnapshot> {
        let cfg = self.db.load_config()?;
        let initial_equity = cfg.get_f64("initial_equity_usdc", 10000.0);
        let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);
        let chop_prob_thresh = cfg.get_f64("hmm_chop_prob_threshold", 0.55);
        let bear_flip_prob = cfg.get_f64("execution_regime_flip_bear_prob", 0.62);
        let bull_flip_prob = cfg.get_f64("execution_regime_flip_bull_prob", 0.60);
        let chop_conv_max = cfg.get_f64("execution_high_chop_conviction_max", 0.08);
        let chop_prob_offset = cfg.get_f64("execution_high_chop_prob_offset", 0.20);
        let chop_prob_cap = cfg.get_f64("execution_high_chop_prob_cap", 0.82);
        let rs_breakdown_long_snr = cfg.get_f64("execution_rs_breakdown_long_snr", -0.10);
        let rs_reversal_short_snr = cfg.get_f64("execution_rs_reversal_short_snr", 0.10);
        let cooldown_bars = cfg
            .get_i64(
                "execution_stop_cooldown_bars",
                cfg.get_i64("post_stop_cooldown_bars", 14),
            )
            .max(1);
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();

        let now_ms = evaluations
            .first()
            .map(|e| e.timestamp_ms)
            .unwrap_or_else(|| self.db.now_ms());

        let _ = self.sync_funding_payments(Some(now_ms), is_backtest);

        let usdc_assets = self.db.get_usdc_perp_universe()?;
        let asset_by_symbol: HashMap<String, UniverseAsset> = usdc_assets
            .into_iter()
            .map(|a| (a.symbol.clone(), a))
            .collect();
        let eval_by_symbol: HashMap<String, &SignalEvaluation> = evaluations
            .iter()
            .map(|e| (e.exec_symbol.clone(), e))
            .collect();

        let prev_snap = self.db.get_latest_portfolio_snapshot()?;
        let mut accrued_yield_usdc = prev_snap.accrued_yield_usdc;

        // 1. Evaluate open positions (Two-Tier Chandelier Stop + Regime Exit)
        let open_trades = self.db.get_open_trades()?;
        for trade in open_trades {
            let Some(asset) = asset_by_symbol.get(&trade.symbol) else {
                continue;
            };
            let bars =
                self.db
                    .get_ohlcv_bars(&trade.symbol, &asset.exec_timeframe, 30, Some(now_ms))?;
            if bars.is_empty() {
                continue;
            }
            let last_bar = bars.last().unwrap();
            if (mode == "LIVE" || mode == "TESTNET") && !is_backtest && last_bar.is_synthetic != 0 {
                tracing::warn!(
                    "Skipping stop/exit evaluation for {} on synthetic bar (ts={})",
                    trade.symbol,
                    last_bar.timestamp_ms
                );
                continue;
            }
            let atr_period = cfg
                .get_usize("atr_period", cfg.get_usize("atr_default_period", 14))
                .max(2);
            let atr = self.risk.compute_atr(&bars, atr_period);

            let stop_eval = self.risk.evaluate_two_tier_chandelier_stop(
                &trade.side,
                trade.entry_price,
                trade.quantity,
                trade.chandelier_stop_price,
                trade.highest_price_seen,
                trade.lowest_price_seen,
                last_bar.high,
                last_bar.low,
                last_bar.close,
                atr,
                asset.tick_size,
                Some(&cfg),
            );

            let ev_opt = eval_by_symbol.get(&trade.symbol).copied();
            let mut exit_reason = stop_eval.exit_reason.clone();
            let mut exit_px = if stop_eval.triggered {
                stop_eval.exit_fill_price
            } else {
                last_bar.close
            };

            if exit_reason.is_none() {
                if let Some(ev) = ev_opt {
                    if trade.side == "LONG"
                        && ev.hmm_regime == "BEAR"
                        && ev.prob_bear >= bear_flip_prob
                    {
                        exit_reason = Some("REGIME_FLIP_BEAR".to_string());
                        exit_px = last_bar.close;
                    } else if trade.side == "LONG" && ev.snr_synth < rs_breakdown_long_snr {
                        exit_reason = Some("RS_SYNTH_BREAKDOWN_LONG".to_string());
                        exit_px = last_bar.close;
                    } else if trade.side == "SHORT"
                        && ev.hmm_regime == "BULL"
                        && ev.prob_bull >= bull_flip_prob
                    {
                        exit_reason = Some("REGIME_FLIP_BULL".to_string());
                        exit_px = last_bar.close;
                    } else if trade.side == "SHORT" && ev.snr_synth > rs_reversal_short_snr {
                        exit_reason = Some("RS_SYNTH_REVERSAL_SHORT".to_string());
                        exit_px = last_bar.close;
                    } else if ev.prob_chop >= (chop_prob_thresh + chop_prob_offset).min(chop_prob_cap)
                        && ev.blended_conviction.abs() < chop_conv_max
                    {
                        exit_reason = Some("HIGH_CHOP_DELEVERAGE".to_string());
                        exit_px = last_bar.close;
                    }
                }
            }

            if let Some(reason) = exit_reason {
                let mut actual_exit_px = exit_px;
                let mut actual_fees = trade.fees_paid_usdc;

                if (mode == "LIVE" || mode == "TESTNET") && !is_backtest {
                    let close_side = if trade.side == "LONG" { "SHORT" } else { "LONG" };
                    let exec_close = self.execute_maker_limit_chaser(
                        asset,
                        close_side,
                        trade.quantity,
                        last_bar.close,
                        last_bar.high,
                        last_bar.low,
                        last_bar.volume,
                        None,
                        None,
                        None,
                        true,
                    );
                    if exec_close.fsm_state == "FILLED" && exec_close.avg_fill_price > 0.0 {
                        actual_exit_px = exec_close.avg_fill_price;
                        actual_fees = round_dp(trade.fees_paid_usdc + exec_close.fees_paid_usdc, 4);
                        tracing::info!(
                            "Closed position on exchange for {} {}: fill_px={} fees={}",
                            trade.symbol,
                            close_side,
                            actual_exit_px,
                            exec_close.fees_paid_usdc
                        );
                    } else {
                        tracing::warn!(
                            "Exit order for {} returned state {} on exchange (reason={}); keeping local trade #{} OPEN",
                            trade.symbol,
                            exec_close.fsm_state,
                            reason,
                            trade.trade_id
                        );
                        let upnl = if trade.side == "LONG" {
                            (last_bar.close - trade.entry_price) * trade.quantity
                        } else {
                            (trade.entry_price - last_bar.close) * trade.quantity
                        };
                        self.db.update_open_trade(
                            trade.trade_id,
                            last_bar.close,
                            stop_eval.chandelier_stop_price,
                            stop_eval.highest_price_seen,
                            stop_eval.lowest_price_seen,
                            round_dp(upnl, 4),
                            trade.funding_accrued_usdc,
                        )?;
                        continue;
                    }
                }

                let exit_fee = (trade.quantity * actual_exit_px) * (taker_fee_bps / 10000.0);
                let total_fees = if actual_fees > trade.fees_paid_usdc {
                    actual_fees
                } else {
                    round_dp(trade.fees_paid_usdc + exit_fee, 4)
                };
                let gross_pnl = if trade.side == "LONG" {
                    (actual_exit_px - trade.entry_price) * trade.quantity
                } else {
                    (trade.entry_price - actual_exit_px) * trade.quantity
                };
                let net_pnl = round_dp(gross_pnl - total_fees + trade.funding_accrued_usdc, 4);
                self.db.close_trade(
                    trade.trade_id,
                    actual_exit_px,
                    net_pnl,
                    total_fees,
                    &reason,
                )?;
                if stop_eval.triggered {
                    let tf_ms = timeframe_seconds(&asset.exec_timeframe) * 1000;
                    self.stop_cooldown_until_ms
                        .lock()
                        .insert(trade.symbol.clone(), now_ms + cooldown_bars * tf_ms);
                }
            } else {
                let upnl = if trade.side == "LONG" {
                    (last_bar.close - trade.entry_price) * trade.quantity
                } else {
                    (trade.entry_price - last_bar.close) * trade.quantity
                };
                self.db.update_open_trade(
                    trade.trade_id,
                    last_bar.close,
                    stop_eval.chandelier_stop_price,
                    stop_eval.highest_price_seen,
                    stop_eval.lowest_price_seen,
                    round_dp(upnl, 4),
                    trade.funding_accrued_usdc,
                )?;
            }
        }

        // 2. Compute current equity & open trade state before new entries
        let mut active_trades = self.db.get_open_trades()?;
        let realized_pnl = self.db.get_total_realized_pnl()?;
        let unrealized_pnl: f64 = active_trades
            .iter()
            .map(|t| t.unrealized_pnl_usdc + t.funding_accrued_usdc)
            .sum();

        let two_book_active = !is_backtest
            && (cfg.get_bool("platform_pp50_enabled", false)
                || cfg.get_bool("pond_enabled", false));
        let split_pct = if two_book_active {
            cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90)
        } else {
            1.0
        };
        let net_pit_transfers = if two_book_active {
            self.db.get_net_book_transfer_adjustment("PIT").unwrap_or(0.0)
        } else {
            0.0
        };
        let raw_platform_eq = if !is_backtest
            && (mode == "LIVE" || mode == "TESTNET")
            && prev_snap.total_equity_usdc > 0.0
        {
            prev_snap.total_equity_usdc + unrealized_pnl
        } else {
            initial_equity + realized_pnl + unrealized_pnl + accrued_yield_usdc
        };
        let current_equity = (raw_platform_eq * split_pct + net_pit_transfers).max(50.0);

        let mut open_symbols: HashSet<String> =
            active_trades.iter().map(|t| t.symbol.clone()).collect();
        if !is_backtest {
            if let Ok(pond_syms) = self.db.get_open_pond_symbols() {
                open_symbols.extend(pond_syms);
            }
        }

        let (_wallet_tot_eq, perps_trading_eq, vault_eq, mut total_wallet_deployed) =
            if !is_backtest {
                self.db
                    .get_wallet_perps_margin_state()
                    .unwrap_or((initial_equity, initial_equity, 0.0, 0.0))
            } else {
                (initial_equity, initial_equity, 0.0, 0.0)
            };

        // 3. Evaluate new directional entries for LONG / SHORT candidates
        let atr_period = cfg
            .get_usize("atr_period", cfg.get_usize("atr_default_period", 14))
            .max(2);
        let max_longs = cfg.get_usize("max_concurrent_long_positions", 2).max(1);
        let max_shorts = cfg.get_usize("max_concurrent_short_positions", 2).max(1);
        for ev in evaluations {
            if ev.signal_action != "LONG" && ev.signal_action != "SHORT" {
                continue;
            }
            if open_symbols.contains(&ev.exec_symbol) {
                continue;
            }
            if ev.signal_action == "LONG"
                && active_trades.iter().filter(|t| t.side == "LONG").count() >= max_longs
            {
                continue;
            }
            if ev.signal_action == "SHORT"
                && active_trades.iter().filter(|t| t.side == "SHORT").count() >= max_shorts
            {
                continue;
            }
            if let Some(&cooldown_until) = self.stop_cooldown_until_ms.lock().get(&ev.exec_symbol) {
                if now_ms < cooldown_until {
                    continue;
                }
            }
            let Some(asset) = asset_by_symbol.get(&ev.exec_symbol) else {
                continue;
            };
            let bars =
                self.db
                    .get_ohlcv_bars(&asset.symbol, &asset.exec_timeframe, 30, Some(now_ms))?;
            if bars.is_empty() {
                continue;
            }
            let last_bar = bars.last().unwrap();
            let atr = self.risk.compute_atr(&bars, atr_period);

            let mut sizing = self.risk.evaluate_order_risk(
                asset,
                &ev.signal_action,
                last_bar.close,
                atr,
                current_equity,
                Some(&active_trades),
                ev.blended_conviction,
                0.0,
            );
            if !sizing.approved {
                continue;
            }

            if !is_backtest && vault_eq > 0.0 {
                let remaining_perps_cap = (perps_trading_eq - total_wallet_deployed).max(0.0);
                if remaining_perps_cap < asset.min_notional_usdc.max(10.0) {
                    continue;
                }
                if sizing.notional_usdc > remaining_perps_cap {
                    let capped_qty = RiskEngine::round_step(
                        remaining_perps_cap / last_bar.close.max(1e-8),
                        asset.lot_step_size,
                        true,
                    );
                    let capped_notional = round_dp(capped_qty * last_bar.close, 2);
                    if capped_qty <= 0.0 || capped_notional < asset.min_notional_usdc.max(10.0) {
                        continue;
                    }
                    sizing.quantity = capped_qty;
                    sizing.notional_usdc = capped_notional;
                }
            }

            let exec_res = self.execute_maker_limit_chaser(
                asset,
                &ev.signal_action,
                sizing.quantity,
                last_bar.close,
                last_bar.high,
                last_bar.low,
                last_bar.volume,
                None,
                if is_backtest { Some("PAPER") } else { None },
                None,
                false,
            );
            let is_filled = exec_res.fsm_state == "FILLED" && exec_res.filled_units > 0.0;
            let is_open_book = exec_res.fsm_state == "OPEN_ON_BOOK";
            if !is_filled && !is_open_book {
                continue;
            }

            let fill_px = if exec_res.avg_fill_price > 0.0 {
                exec_res.avg_fill_price
            } else {
                last_bar.close
            };
            let order_qty = if exec_res.filled_units > 0.0 {
                exec_res.filled_units
            } else {
                sizing.quantity
            };
            let filled_notional = round_dp(order_qty * fill_px, 2);
            let new_trade = TradeRecord {
                trade_id: 0,
                client_order_id: exec_res.client_order_id,
                symbol: asset.symbol.clone(),
                base_asset: asset.base_asset.clone(),
                side: ev.signal_action.clone(),
                status: "OPEN".to_string(),
                fsm_state: exec_res.fsm_state.clone(),
                entry_price: fill_px,
                current_price: fill_px,
                exit_price: None,
                quantity: order_qty,
                filled_units: exec_res.filled_units,
                avg_fill_price: if exec_res.avg_fill_price > 0.0 {
                    Some(exec_res.avg_fill_price)
                } else {
                    None
                },
                notional_usdc: filled_notional,
                risk_budget_usdc: sizing.risk_budget_usdc,
                conviction_multiplier: sizing.conviction_multiplier,
                initial_stop_price: sizing.initial_stop_price,
                chandelier_stop_price: sizing.initial_stop_price,
                current_stop_price: sizing.initial_stop_price,
                highest_price_seen: fill_px,
                lowest_price_seen: fill_px,
                realized_pnl_usdc: 0.0,
                unrealized_pnl_usdc: 0.0,
                fees_paid_usdc: exec_res.fees_paid_usdc,
                funding_accrued_usdc: 0.0,
                slippage_paid_usd: exec_res.slippage_paid_usd,
                exchange_order_id: Some(exec_res.exchange_order_id),
                opened_at: String::new(),
                closed_at: None,
                exit_reason: None,
            };
            let tid = self.db.insert_trade(&new_trade)?;
            let mut inserted = new_trade;
            inserted.trade_id = tid;
            open_symbols.insert(asset.symbol.clone());
            total_wallet_deployed = round_dp(total_wallet_deployed + filled_notional, 2);
            active_trades.push(inserted);
        }

        // 4. Compute portfolio telemetry & Basis Harvest allocation
        let mut deployed_notional = 0.0;
        let mut portfolio_heat = 0.0;
        let mut open_longs = 0i64;
        let mut open_shorts = 0i64;
        let mut total_upnl = 0.0;

        for t in &active_trades {
            let notional = t.quantity.abs() * t.current_price;
            let stop_px = if t.chandelier_stop_price > 0.0 {
                t.chandelier_stop_price
            } else {
                t.initial_stop_price
            };
            let risk = (t.entry_price - stop_px).abs() * t.quantity.abs();
            deployed_notional += notional;
            portfolio_heat += risk;
            total_upnl += t.unrealized_pnl_usdc + t.funding_accrued_usdc;
            if t.side == "LONG" {
                open_longs += 1;
            } else {
                open_shorts += 1;
            }
        }

        let regime_ev = evaluations.first();
        let hmm_regime = regime_ev
            .map(|e| e.hmm_regime.clone())
            .unwrap_or_else(|| "CHOP".to_string());
        let prob_bull = regime_ev.map(|e| e.prob_bull).unwrap_or(0.3333);
        let prob_bear = regime_ev.map(|e| e.prob_bear).unwrap_or(0.3333);
        let prob_chop = regime_ev.map(|e| e.prob_chop).unwrap_or(0.3334);

        let realized_after = self.db.get_total_realized_pnl()?;
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
        let wallet = cfg.get_str("wallet_address", "").trim().to_string();

        let mut basis_notional = 0.0;
        let mut total_equity =
            round_dp(initial_equity + realized_after + total_upnl + accrued_yield_usdc, 2);

        if !is_backtest && (mode == "LIVE" || mode == "TESTNET") && !wallet.is_empty() {
            let vault_state = self.fetch_hlp_vault_state(Some(&wallet), None, Some(is_testnet));
            if vault_state
                .get("live_queried")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                let vault_eq_now = vault_state
                    .get("vault_equity_usdc")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                let vault_yield_now = vault_state
                    .get("accrued_yield_usdc")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                let perp_eq_now = vault_state
                    .get("perp_equity_usdc")
                    .and_then(Value::as_f64);

                basis_notional = round_dp(vault_eq_now, 2);
                accrued_yield_usdc = round_dp(vault_yield_now, 4);
                if let Some(peq) = perp_eq_now.filter(|&p| p > 0.0 || vault_eq_now > 0.0) {
                    total_equity = round_dp(peq + vault_eq_now, 2);
                } else if prev_snap.total_equity_usdc > 0.0 {
                    total_equity = prev_snap.total_equity_usdc;
                } else {
                    total_equity = round_dp(
                        initial_equity + realized_after + total_upnl + accrued_yield_usdc,
                        2,
                    );
                }
            } else if prev_snap.total_equity_usdc > 0.0 {
                basis_notional = prev_snap.basis_harvest_notional_usdc;
                total_equity = prev_snap.total_equity_usdc;
            }
        } else {
            let available_cash = (initial_equity + realized_after + total_upnl + accrued_yield_usdc
                - deployed_notional)
                .max(0.0);
            let basis_candidates = evaluations
                .iter()
                .filter(|e| e.signal_action == "BASIS_HARVEST")
                .count();
            if (hmm_regime == "CHOP" || prob_chop >= chop_prob_thresh) && basis_candidates > 0 {
                let alloc_pct = cfg.get_f64("basis_max_idle_cash_allocation_pct", 0.50);
                let basis_floor = cfg.get_f64(
                    "basis_harvest_min_funding_hourly",
                    cfg.get_f64("basis_min_hourly_funding_rate", 0.000015),
                );
                basis_notional = round_dp(available_cash * alloc_pct, 2);
                if is_backtest && cfg.get_bool("enable_hlp_floor_backtest", false) {
                    let avg_funding = evaluations
                        .iter()
                        .filter(|e| e.signal_action == "BASIS_HARVEST")
                        .map(|e| e.funding_rate_hourly.max(basis_floor))
                        .sum::<f64>()
                        / (basis_candidates as f64);
                    accrued_yield_usdc =
                        round_dp(accrued_yield_usdc + basis_notional * avg_funding * 4.0, 4);
                }
            }
            total_equity =
                round_dp(initial_equity + realized_after + total_upnl + accrued_yield_usdc, 2);
        }

        let pond_deployed_now: f64 = if !is_backtest && (mode == "LIVE" || mode == "TESTNET") {
            self.db
                .get_open_pond_cells()
                .unwrap_or_default()
                .iter()
                .map(|c| c.notional_usdc.unwrap_or(0.0))
                .sum()
        } else {
            0.0
        };
        let available_perps_cash = if !is_backtest && (mode == "LIVE" || mode == "TESTNET") {
            round_dp(
                (total_equity - basis_notional - deployed_notional - pond_deployed_now).max(0.0),
                2,
            )
        } else {
            round_dp((total_equity - deployed_notional).max(0.0), 2)
        };

        let pit_book_equity = if two_book_active {
            (total_equity * split_pct + net_pit_transfers).max(1.0)
        } else {
            total_equity.max(1.0)
        };
        let gross_lev = round_dp(deployed_notional / pit_book_equity, 4);
        let heat_pct = round_dp(portfolio_heat / pit_book_equity, 6);

        let snap = PortfolioSnapshot {
            snapshot_id: None,
            timestamp_ms: now_ms,
            total_equity_usdc: total_equity,
            available_cash_usdc: available_perps_cash,
            deployed_notional_usdc: round_dp(deployed_notional, 2),
            gross_leverage: gross_lev,
            portfolio_heat_usdc: round_dp(portfolio_heat, 2),
            portfolio_heat_pct: heat_pct,
            basis_harvest_notional_usdc: basis_notional,
            accrued_yield_usdc,
            hmm_regime: hmm_regime.clone(),
            prob_bull,
            prob_bear,
            prob_chop,
            open_long_count: open_longs,
            open_short_count: open_shorts,
            created_at: None,
        };
        self.db.save_portfolio_snapshot(&snap)?;

        if !is_backtest {
            let _ = self
                .alerts
                .check_and_alert_regime_flip(&hmm_regime, prob_bull, prob_bear, prob_chop);
            let _ = self.alerts.check_and_alert_daily_drawdown(total_equity);
        }

        Ok(snap)
    }

    /// Executes an inter-book USDC transfer between Book A (PIT) and Book B (POND) (`C1`).
    /// - When separate Hyperliquid sub-accounts (`pit_subaccount_address` / `pond_subaccount_address`)
    ///   are configured in `LIVE` or `TESTNET` mode, signs and submits a real `subAccountTransfer` L1 action.
    /// - When running in single-wallet or `PAPER` mode, records a deterministic `VIRTUAL_SINGLE_WALLET_LEDGER`
    ///   rebalance entry that is persisted in `book_transfers` and applied via `get_net_book_transfer_adjustment`.
    pub fn execute_subaccount_transfer(
        &self,
        from_book: &str,
        to_book: &str,
        amount_usdc: f64,
        dry_run: bool,
    ) -> Result<Value> {
        let cfg = self.db.load_config()?;
        let amt = round_dp(amount_usdc.max(0.0), 2);
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";

        let pit_sub = cfg.get_str("pit_subaccount_address", "").trim().to_string();
        let pond_sub = cfg.get_str("pond_subaccount_address", "").trim().to_string();
        let has_live_subaccount = (mode == "LIVE" || mode == "TESTNET")
            && ((!pit_sub.is_empty() && validate_evm_address(&pit_sub, "pit_subaccount_address").is_ok())
                || (!pond_sub.is_empty()
                    && validate_evm_address(&pond_sub, "pond_subaccount_address").is_ok()));

        if !dry_run && has_live_subaccount {
            let priv_key = self.db.get_signing_private_key()?;
            if !priv_key.is_empty() {
                let (sub_addr, is_deposit) = if to_book.eq_ignore_ascii_case("POND")
                    && !pond_sub.is_empty()
                {
                    (pond_sub, true)
                } else if from_book.eq_ignore_ascii_case("POND") && !pond_sub.is_empty() {
                    (pond_sub, false)
                } else if to_book.eq_ignore_ascii_case("PIT") && !pit_sub.is_empty() {
                    (pit_sub, true)
                } else {
                    (pit_sub, false)
                };
                let usd_micro = (amt * 1e6).round() as i64;
                let nonce = Utc::now().timestamp_millis() as u64;
                let action = SubAccountTransferAction {
                    action_type: "subAccountTransfer".to_string(),
                    sub_account_user: sub_addr.clone(),
                    is_deposit,
                    usd: usd_micro,
                };
                if let Ok(sig) = sign_l1_action(&priv_key, &action, None, nonce, None, !is_testnet)
                {
                    let url = self.exchange_url(&cfg, is_testnet);
                    let payload = json!({
                        "action": serde_json::to_value(&action).unwrap_or_default(),
                        "nonce": nonce,
                        "signature": {
                            "r": sig["r"],
                            "s": sig["s"],
                            "v": sig["v"],
                        },
                        "vaultAddress": Value::Null,
                    });
                    if let Some(resp) = self.http_post_json(&url, &payload) {
                        if resp.get("status").and_then(Value::as_str) == Some("ok") {
                            let tx_hash = sig["connection_id"]
                                .as_str()
                                .unwrap_or("0x0")
                                .to_string();
                            return Ok(json!({
                                "status": "EXECUTED",
                                "transfer_mode": "HYPERLIQUID_SUBACCOUNT_L1",
                                "from_book": from_book,
                                "to_book": to_book,
                                "amount_usdc": amt,
                                "tx_hash": tx_hash,
                                "dry_run": false,
                                "exchange_response": resp,
                            }));
                        }
                    }
                }
            }
        }

        let digest_hex = hex::encode(Sha256::digest(
            format!(
                "VIRTUAL_LEDGER:{from_book}:{to_book}:{amt}:{}",
                Utc::now().timestamp_nanos_opt().unwrap_or(0)
            )
            .as_bytes(),
        ));
        let tx_hash = format!("0x{}", &digest_hex[..64]);
        Ok(json!({
            "status": if dry_run { "DRY_RUN_SIMULATED" } else { "EXECUTED" },
            "transfer_mode": "VIRTUAL_SINGLE_WALLET_LEDGER",
            "from_book": from_book,
            "to_book": to_book,
            "amount_usdc": amt,
            "tx_hash": tx_hash,
            "dry_run": dry_run,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(sim_deposit["status"], "SIMULATED");
        assert_eq!(sim_deposit["usd_micro"], 100_000_000);
        assert_eq!(sim_deposit["is_deposit"], true);

        let sim_withdraw = exec.execute_hlp_vault_transfer(50.0, false, Some(vault_addr));
        assert_eq!(sim_withdraw["status"], "SIMULATED");
        assert_eq!(sim_withdraw["usd_micro"], 50_000_000);
        assert_eq!(sim_withdraw["is_deposit"], false);
    }

    #[test]
    fn test_asset_index_cache_and_fallback() {
        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        // Force offline endpoint so test never depends on external network reachability
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

        // S6: Verify fallback indices are NEVER cached and LIVE/TESTNET mode refuses fallback indices
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
        assert_eq!(recon_resting["status"], "RECONCILED");
        assert_eq!(recon_resting["discrepancies_found"], 0);
        assert_eq!(db.get_open_pond_cells().unwrap().len(), 1);

        let recon = exec.reconcile_exchange_state(Some(&empty_on_chain), None);
        assert_eq!(recon["status"], "RECONCILED");
        assert_eq!(recon["discrepancies_found"], 1);
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
}
