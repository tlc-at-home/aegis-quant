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
}

pub type AssetIndexCache = Arc<Mutex<HashMap<(bool, String), (u32, i64)>>>;

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
            let limit_px = if norm_side == "LONG" {
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
        let exec = ExecutionEngine::with_timeout(db, 1);

        // When order is not on openOrders and has no confirmed fills for its oid, it must report GONE (never FILLED from clearinghouse szi)
        let status = exec.check_resting_order_status("SOL/USDC:USDC", Some("hl-123456"));
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
}
