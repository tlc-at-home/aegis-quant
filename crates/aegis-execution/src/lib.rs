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
//!   - 14-bar post-stop cooldown & full portfolio execution cycle (`reconcile_cycle`)

use aegis_alerts::AlertEngine;
use aegis_core::{
    base_prices_usdc, round_dp, timeframe_seconds, validate_evm_address, validate_private_key,
    AegisError, OhlcvBar, OrderExecutionResult, PortfolioSnapshot, Result, SignalEvaluation,
    TradeRecord, UniverseAsset, MAINNET_HLP_VAULT_ADDRESS, TESTNET_HLP_VAULT_ADDRESS,
};
use aegis_db::{ConfigMap, DatabaseManager};
use aegis_risk::RiskEngine;
use chrono::Utc;
use k256::ecdsa::{SigningKey, VerifyingKey};
use parking_lot::Mutex;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest as Sha2Digest, Sha256};
use sha3::Keccak256;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub const HYPERLIQUID_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
pub const HYPERLIQUID_TESTNET_INFO_URL: &str = "https://api.hyperliquid-testnet.xyz/info";
pub const HYPERLIQUID_EXCHANGE_URL: &str = "https://api.hyperliquid.xyz/exchange";
pub const HYPERLIQUID_TESTNET_EXCHANGE_URL: &str = "https://api.hyperliquid-testnet.xyz/exchange";

/// Formats a float into a wire string matching Hyperliquid's `float_to_wire` specification.
pub fn float_to_wire(x: f64) -> String {
    let s = format!("{:.8}", x);
    let mut trimmed = s.trim_end_matches('0');
    if trimmed.ends_with('.') {
        trimmed = &trimmed[..trimmed.len() - 1];
    }
    if trimmed == "-0" || trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LimitOrderType {
    pub tif: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriggerOrderTypeWire {
    #[serde(rename = "triggerPx")]
    pub trigger_px: String,
    #[serde(rename = "isMarket")]
    pub is_market: bool,
    pub tpsl: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderTypeWire {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<LimitOrderType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<TriggerOrderTypeWire>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderWire {
    pub a: u32,
    pub b: bool,
    pub p: String,
    pub s: String,
    pub r: bool,
    pub t: OrderTypeWire,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub orders: Vec<OrderWire>,
    pub grouping: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelWire {
    pub a: u32,
    pub o: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub cancels: Vec<CancelWire>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VaultTransferAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub vault_address: String,
    pub is_deposit: bool,
    pub usd: i64,
}

/// Computes the exact Hyperliquid MessagePack + Keccak256 `action_hash` / phantom agent connection ID.
pub fn compute_l1_action_hash<T: Serialize>(
    action: &T,
    vault_address: Option<&str>,
    nonce: u64,
    expires_after: Option<u64>,
) -> Result<[u8; 32]> {
    let mut data = rmp_serde::encode::to_vec_named(action)
        .map_err(|e| AegisError::Execution(format!("Msgpack serialization error: {e}")))?;
    data.extend_from_slice(&nonce.to_be_bytes());
    match vault_address {
        None => data.push(0x00),
        Some(v) if v.trim().is_empty() => data.push(0x00),
        Some(v) => {
            data.push(0x01);
            let clean = v.strip_prefix("0x").unwrap_or(v);
            let addr_bytes = hex::decode(clean)
                .map_err(|e| AegisError::Validation(format!("Invalid vault address hex: {e}")))?;
            if addr_bytes.len() != 20 {
                return Err(AegisError::Validation(format!(
                    "Vault address must be 20 bytes, got {}",
                    addr_bytes.len()
                )));
            }
            data.extend_from_slice(&addr_bytes);
        }
    }
    if let Some(exp) = expires_after {
        data.push(0x00);
        data.extend_from_slice(&exp.to_be_bytes());
    }
    let hash: [u8; 32] = Keccak256::digest(&data).into();
    Ok(hash)
}

/// Signs an L1 action payload using MessagePack binary serialization, action_hash, and EIP-712 structured data signing.
pub fn sign_l1_action<T: Serialize>(
    private_key_hex: &str,
    action: &T,
    vault_address: Option<&str>,
    nonce: u64,
    expires_after: Option<u64>,
    is_mainnet: bool,
) -> Result<Value> {
    let normalized_pk = validate_private_key(private_key_hex)?;
    let raw_bytes = hex::decode(&normalized_pk[2..])
        .map_err(|e| AegisError::Crypto(format!("Invalid private_key hex: {e}")))?;
    let signing_key = SigningKey::from_slice(&raw_bytes)
        .map_err(|e| AegisError::Crypto(format!("Invalid secp256k1 signing key: {e}")))?;

    let action_h = compute_l1_action_hash(action, vault_address, nonce, expires_after)?;

    // 1. EIP-712 Domain Separator for "Exchange"
    let mut domain_buf = Vec::with_capacity(32 * 5);
    domain_buf.extend_from_slice(&Keccak256::digest(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    ));
    domain_buf.extend_from_slice(&Keccak256::digest(b"Exchange"));
    domain_buf.extend_from_slice(&Keccak256::digest(b"1"));
    let mut chain_id_bytes = [0u8; 32];
    chain_id_bytes[30..32].copy_from_slice(&1337u16.to_be_bytes());
    domain_buf.extend_from_slice(&chain_id_bytes);
    domain_buf.extend_from_slice(&[0u8; 32]);
    let domain_separator: [u8; 32] = Keccak256::digest(&domain_buf).into();

    // 2. Struct Hash for Agent(string source,bytes32 connectionId)
    let mut agent_buf = Vec::with_capacity(32 * 3);
    agent_buf.extend_from_slice(&Keccak256::digest(b"Agent(string source,bytes32 connectionId)"));
    let source_str = if is_mainnet { "a" } else { "b" };
    agent_buf.extend_from_slice(&Keccak256::digest(source_str.as_bytes()));
    agent_buf.extend_from_slice(&action_h);
    let struct_hash: [u8; 32] = Keccak256::digest(&agent_buf).into();

    // 3. EIP-712 Signable Digest
    let mut digest_buf = Vec::with_capacity(2 + 32 + 32);
    digest_buf.extend_from_slice(b"\x19\x01");
    digest_buf.extend_from_slice(&domain_separator);
    digest_buf.extend_from_slice(&struct_hash);
    let digest: [u8; 32] = Keccak256::digest(&digest_buf).into();

    // 4. ECDSA Secp256k1 Recoverable Signature
    let (sig, rec_id) = signing_key
        .sign_prehash_recoverable(&digest)
        .map_err(|e| AegisError::Crypto(format!("ECDSA signing failed: {e}")))?;
    let sig_bytes = sig.to_bytes();
    let r_hex = format!("0x{}", hex::encode(&sig_bytes[..32]));
    let s_hex = format!("0x{}", hex::encode(&sig_bytes[32..64]));
    let v = 27u8 + rec_id.to_byte();

    let verify_key = VerifyingKey::from(&signing_key);
    let uncompressed = verify_key.to_encoded_point(false);
    let pub_bytes = &uncompressed.as_bytes()[1..];
    let pub_hash = Keccak256::digest(pub_bytes);
    let signer_address = format!("0x{}", hex::encode(&pub_hash[12..]));

    Ok(json!({
        "r": r_hex,
        "s": s_hex,
        "v": v,
        "signer": signer_address,
        "connection_id": format!("0x{}", hex::encode(action_h)),
    }))
}

/// Market Data Ingestion & `ALT/BTC:SYNTH` Synthesis Engine (`engine/ingestion.py`).
#[derive(Clone)]
pub struct MarketIngestionEngine {
    pub db: DatabaseManager,
    pub timeout_sec: u64,
}

impl MarketIngestionEngine {
    pub fn new(db: DatabaseManager) -> Self {
        Self { db, timeout_sec: 8 }
    }

    fn info_url(&self) -> String {
        let cfg = self.db.load_config().unwrap_or_default();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false)
            || cfg.get_str("execution_mode", "PAPER").eq_ignore_ascii_case("TESTNET");
        if is_testnet {
            HYPERLIQUID_TESTNET_INFO_URL.to_string()
        } else {
            HYPERLIQUID_INFO_URL.to_string()
        }
    }

    fn http_post_json(&self, url: &str, payload: &Value) -> Option<Value> {
        let client = Client::builder()
            .timeout(Duration::from_secs(self.timeout_sec))
            .build()
            .ok()?;
        let resp = client.post(url).json(payload).send().ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<Value>().ok()
    }

    pub fn fetch_hyperliquid_funding_map(&self) -> HashMap<String, f64> {
        let mut out = HashMap::new();
        let Some(resp) = self.http_post_json(&self.info_url(), &json!({"type": "metaAndAssetCtxs"})) else {
            return out;
        };
        let Some(arr) = resp.as_array() else {
            return out;
        };
        if arr.len() < 2 {
            return out;
        }
        let universe = arr[0]
            .get("universe")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let ctxs = arr[1].as_array().cloned().unwrap_or_default();
        for (u, c) in universe.iter().zip(ctxs.iter()) {
            if let Some(name) = u.get("name").and_then(Value::as_str) {
                let f = c
                    .get("funding")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                out.insert(name.to_uppercase(), f);
            }
        }
        out
    }

    pub fn fetch_hyperliquid_candles(
        &self,
        coin: &str,
        interval: &str,
        lookback_bars: usize,
        funding_hourly: f64,
    ) -> Option<Vec<OhlcvBar>> {
        let tf_sec = timeframe_seconds(interval);
        let now_ms = Utc::now().timestamp_millis();
        let start_ms = now_ms - ((lookback_bars as i64 + 2) * tf_sec * 1000);
        let payload = json!({
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": start_ms,
                "endTime": now_ms,
            }
        });
        let raw = self.http_post_json(&self.info_url(), &payload)?;
        let list = raw.as_array()?;
        if list.is_empty() {
            return None;
        }

        let symbol = format!("{coin}/USDC:USDC");
        let mut bars = Vec::with_capacity(list.len());
        let parse_f = |v: Option<&Value>| -> f64 {
            v.and_then(|x| {
                x.as_f64()
                    .or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .unwrap_or(0.0)
        };

        for item in list {
            let ts = item.get("t").and_then(Value::as_i64).unwrap_or(0);
            if ts <= 0 {
                continue;
            }
            bars.push(OhlcvBar {
                symbol: symbol.clone(),
                timeframe: interval.to_string(),
                timestamp_ms: ts,
                open: parse_f(item.get("o")),
                high: parse_f(item.get("h")),
                low: parse_f(item.get("l")),
                close: parse_f(item.get("c")),
                volume: parse_f(item.get("v")),
                funding_rate_hourly: funding_hourly,
                is_synthetic: 0,
            });
        }
        bars.sort_by_key(|b| b.timestamp_ms);
        if bars.len() > lookback_bars {
            bars = bars[bars.len() - lookback_bars..].to_vec();
        }
        Some(bars)
    }

    /// Generates realistic multi-regime OHLCV bars (`BEAR`, `CHOP`, `BULL`) for offline / sandbox testing.
    #[allow(clippy::too_many_arguments)]
    pub fn generate_synthetic_bars(
        &self,
        symbol: &str,
        timeframe: &str,
        num_bars: usize,
        base_price: f64,
        btc_bars: Option<&[OhlcvBar]>,
        seed_offset: u64,
        end_ts_ms: Option<i64>,
    ) -> Vec<OhlcvBar> {
        let mut digest = Sha256::new();
        digest.update(format!("{symbol}:{timeframe}:{seed_offset}").as_bytes());
        let hash_bytes = digest.finalize();
        let mut rng_state = u64::from_le_bytes(hash_bytes[0..8].try_into().unwrap_or([1; 8]));
        let mut next_norm = || -> f64 {
            // Box-Muller normal variate from deterministic LCG
            rng_state = rng_state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u1 = ((rng_state >> 11) as f64 / ((1u64 << 53) as f64)).clamp(1e-9, 1.0 - 1e-9);
            rng_state = rng_state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u2 = ((rng_state >> 11) as f64 / ((1u64 << 53) as f64)).clamp(1e-9, 1.0 - 1e-9);
            (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
        };

        let tf_sec = timeframe_seconds(timeframe);
        let step_ms = tf_sec * 1000;
        let now_aligned_ms = match end_ts_ms {
            Some(ts) => (ts / step_ms) * step_ms,
            None => (Utc::now().timestamp_millis() / step_ms) * step_ms,
        };
        let start_ms = now_aligned_ms - ((num_bars.saturating_sub(1)) as i64) * step_ms;

        let seg1 = num_bars / 3;
        let seg2 = (2 * num_bars) / 3;
        let tf_scale = ((tf_sec as f64) / 3600.0).sqrt();
        let is_btc = symbol.starts_with("BTC/");
        let mut alpha_tilt = 0.0;
        if !is_btc {
            let leader_rank = (seed_offset % 15) as f64;
            alpha_tilt = (7.0 - leader_rank) * 0.00028 * tf_scale;
        }

        let mut price = base_price * 0.88;
        let mut bars = Vec::with_capacity(num_bars);

        for i in 0..num_bars {
            let ts = start_ms + (i as i64) * step_ms;
            let (drift, vol, base_funding) = if i < seg1 {
                (-0.0014 * tf_scale, 0.011 * tf_scale, -0.00008)
            } else if i < seg2 {
                (0.0001 * tf_scale, 0.006 * tf_scale, 0.00026)
            } else {
                (0.0022 * tf_scale + alpha_tilt, 0.009 * tf_scale, 0.00014)
            };

            let shock = if let Some(b_bars) = btc_bars {
                if i > 0 && i < b_bars.len() {
                    let btc_ret =
                        (b_bars[i].close.max(1e-8) / b_bars[i - 1].close.max(1e-8)).ln();
                    let beta = 1.05 + ((seed_offset % 5) as f64) * 0.08;
                    beta * btc_ret + next_norm() * (vol * 0.55) + alpha_tilt
                } else {
                    drift + next_norm() * vol
                }
            } else {
                drift + next_norm() * vol
            };

            let open_px = price;
            let close_px = (open_px * shock.exp()).max(base_price * 0.05);
            let wick_up = next_norm().abs() * vol * 0.65;
            let wick_dn = next_norm().abs() * vol * 0.65;
            let high_px = open_px.max(close_px) * (1.0 + wick_up);
            let low_px = (open_px.min(close_px) * (1.0 - wick_dn)).max(1e-8);
            let volume =
                (150_000.0 / base_price.max(0.01)) * (1.0 + (shock / vol.max(1e-6)).abs());
            let funding = base_funding + next_norm() * 0.00003;

            bars.push(OhlcvBar {
                symbol: symbol.to_string(),
                timeframe: timeframe.to_string(),
                timestamp_ms: ts,
                open: round_dp(open_px, 8),
                high: round_dp(high_px, 8),
                low: round_dp(low_px, 8),
                close: round_dp(close_px, 8),
                volume: round_dp(volume, 4),
                funding_rate_hourly: round_dp(funding, 6),
                is_synthetic: 1,
            });
            price = close_px;
        }
        bars
    }

    /// Synthesizes `ALT/BTC:SYNTH` ratio OHLCV candles from timestamp-aligned `ALT/USDC` and `BTC/USDC` bars.
    pub fn synthesize_alt_btc_candles(
        alt_usdc_bars: &[OhlcvBar],
        btc_usdc_bars: &[OhlcvBar],
        synth_symbol: &str,
        timeframe: &str,
    ) -> Vec<OhlcvBar> {
        if alt_usdc_bars.is_empty() || btc_usdc_bars.is_empty() {
            return Vec::new();
        }
        let btc_by_ts: HashMap<i64, &OhlcvBar> = btc_usdc_bars
            .iter()
            .filter(|b| b.close > 0.0)
            .map(|b| (b.timestamp_ms, b))
            .collect();

        let mut synth = Vec::with_capacity(alt_usdc_bars.len());
        for a in alt_usdc_bars {
            let Some(b) = btc_by_ts.get(&a.timestamp_ms) else {
                continue;
            };
            if b.open <= 0.0 || b.close <= 0.0 {
                continue;
            }
            let r_open = a.open / b.open;
            let r_close = a.close / b.close;
            let r_high = r_open.max(r_close).max(a.high / b.open.min(b.close).max(1e-8));
            let r_low = r_open.min(r_close).min(a.low / b.open.max(b.close).max(1e-8));
            let rel_funding = a.funding_rate_hourly - b.funding_rate_hourly;

            synth.push(OhlcvBar {
                symbol: synth_symbol.to_string(),
                timeframe: timeframe.to_string(),
                timestamp_ms: a.timestamp_ms,
                open: round_dp(r_open, 10),
                high: round_dp(r_high, 10),
                low: round_dp(r_low, 10),
                close: round_dp(r_close, 10),
                volume: round_dp(a.volume * a.close, 2),
                funding_rate_hourly: round_dp(rel_funding, 6),
                is_synthetic: 1,
            });
        }
        synth
    }

    /// Ingests all active `USDC_PERP` assets and synthesizes all 15 `ALT/BTC:SYNTH` relative-strength pairs.
    pub fn ingest_universe(
        &self,
        lookback_bars: usize,
        force_synthetic: bool,
    ) -> Result<HashMap<String, usize>> {
        let usdc_assets = self.db.get_usdc_perp_universe()?;
        let alt_pairs = self.db.get_alt_pairs_universe()?;
        let base_prices = base_prices_usdc();

        let mut timeframes: HashSet<String> = HashSet::from(["4h".to_string(), "1h".to_string()]);
        for a in &usdc_assets {
            timeframes.insert(a.exec_timeframe.clone());
            timeframes.insert(a.rs_timeframe.clone());
        }

        let funding_map = if force_synthetic {
            HashMap::new()
        } else {
            self.fetch_hyperliquid_funding_map()
        };

        let mut btc_bars_by_tf: HashMap<String, Vec<OhlcvBar>> = HashMap::new();
        let mut total_usdc_bars = 0usize;
        let mut total_synth_bars = 0usize;
        let mut live_api_used = false;

        let mut sorted_tfs: Vec<String> = timeframes.into_iter().collect();
        sorted_tfs.sort();

        for tf in &sorted_tfs {
            let mut bars_opt = None;
            if !force_synthetic {
                let f_rate = funding_map.get("BTC").copied().unwrap_or(0.0001);
                bars_opt = self.fetch_hyperliquid_candles("BTC", tf, lookback_bars, f_rate);
                if bars_opt.is_some() {
                    live_api_used = true;
                }
            }
            let bars = bars_opt.unwrap_or_else(|| {
                self.generate_synthetic_bars(
                    "BTC/USDC:USDC",
                    tf,
                    lookback_bars,
                    *base_prices.get("BTC/USDC:USDC").unwrap_or(&68500.0),
                    None,
                    1,
                    None,
                )
            });
            total_usdc_bars += self.db.upsert_ohlcv_batch(&bars)?;
            btc_bars_by_tf.insert(tf.clone(), bars);
        }

        for (idx, asset) in usdc_assets.iter().enumerate() {
            if asset.base_asset == "BTC" {
                continue;
            }
            let mut req_tfs = vec![asset.exec_timeframe.clone()];
            if asset.rs_timeframe != asset.exec_timeframe {
                req_tfs.push(asset.rs_timeframe.clone());
            }
            let f_rate = funding_map
                .get(&asset.base_asset)
                .copied()
                .unwrap_or(0.00012);
            let ref_px = *base_prices.get(asset.symbol.as_str()).unwrap_or(&10.0);

            for tf in req_tfs {
                let mut bars_opt = None;
                if !force_synthetic {
                    bars_opt = self.fetch_hyperliquid_candles(
                        &asset.base_asset,
                        &tf,
                        lookback_bars,
                        f_rate,
                    );
                }
                let bars = bars_opt.unwrap_or_else(|| {
                    self.generate_synthetic_bars(
                        &asset.symbol,
                        &tf,
                        lookback_bars,
                        ref_px,
                        btc_bars_by_tf.get(&tf).map(Vec::as_slice),
                        (idx + 2) as u64,
                        None,
                    )
                });
                total_usdc_bars += self.db.upsert_ohlcv_batch(&bars)?;
            }
        }

        for pair in &alt_pairs {
            let tf = &pair.rs_timeframe;
            let alt_bars = self
                .db
                .get_ohlcv_bars(&pair.exec_symbol, tf, lookback_bars, None)?;
            let btc_bars = match btc_bars_by_tf.get(tf) {
                Some(b) => b.clone(),
                None => self.db.get_ohlcv_bars("BTC/USDC:USDC", tf, lookback_bars, None)?,
            };
            let synth_bars =
                Self::synthesize_alt_btc_candles(&alt_bars, &btc_bars, &pair.synth_symbol, tf);
            total_synth_bars += self.db.upsert_ohlcv_batch(&synth_bars)?;
        }

        let source_label = if live_api_used {
            "HYPERLIQUID_LIVE_API"
        } else {
            "DETERMINISTIC_SYNTHETIC_FALLBACK"
        };
        let _ = self.db.log_telemetry(
            "INFO",
            "engine.ingestion",
            &format!(
                "Ingested {total_usdc_bars} USDC perp bars and synthesized {total_synth_bars} ALT/BTC:SYNTH bars ({source_label})"
            ),
            Some(json!({
                "usdc_bars": total_usdc_bars,
                "synth_bars": total_synth_bars,
                "source": source_label,
            })),
        );

        Ok(HashMap::from([
            ("usdc_bars_upserted".to_string(), total_usdc_bars),
            ("synth_bars_upserted".to_string(), total_synth_bars),
        ]))
    }
}

/// Hyperliquid L1 Order Execution, Paper Fill Simulator, and Reconciliation Engine (`engine/execution.py`).
#[derive(Clone)]
pub struct ExecutionEngine {
    pub db: DatabaseManager,
    pub risk: RiskEngine,
    pub alerts: AlertEngine,
    pub timeout_sec: u64,
    last_funding_sync_hour_ts: Arc<Mutex<Option<i64>>>,
    stop_cooldown_until_ms: Arc<Mutex<HashMap<String, i64>>>,
    hlp_vault_error_streak: Arc<Mutex<usize>>,
}

impl ExecutionEngine {
    pub fn new(db: DatabaseManager) -> Self {
        let risk = RiskEngine::new(db.clone());
        let alerts = AlertEngine::new(db.clone());
        Self {
            db,
            risk,
            alerts,
            timeout_sec: 10,
            last_funding_sync_hour_ts: Arc::new(Mutex::new(None)),
            stop_cooldown_until_ms: Arc::new(Mutex::new(HashMap::new())),
            hlp_vault_error_streak: Arc::new(Mutex::new(0)),
        }
    }

    pub fn resolve_hlp_vault_address(cfg: &ConfigMap, is_testnet: bool) -> String {
        let configured = cfg.get_str("hyperliquid_vault_address", "").trim().to_string();
        if !configured.is_empty() && validate_evm_address(&configured, "hyperliquid_vault_address").is_ok() {
            if is_testnet && configured.eq_ignore_ascii_case(MAINNET_HLP_VAULT_ADDRESS) {
                return TESTNET_HLP_VAULT_ADDRESS.to_string();
            }
            return configured;
        }
        if is_testnet {
            TESTNET_HLP_VAULT_ADDRESS.to_string()
        } else {
            MAINNET_HLP_VAULT_ADDRESS.to_string()
        }
    }

    fn http_post_json(&self, url: &str, payload: &Value) -> Option<Value> {
        let client = Client::builder()
            .timeout(Duration::from_secs(self.timeout_sec))
            .build()
            .ok()?;
        let resp = client.post(url).json(payload).send().ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<Value>().ok()
    }

    /// Computes dynamic slippage in basis points (`bps`) as a function of bar range volatility
    /// and order notional relative to bar volume.
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
        let cfg = self.db.load_config().unwrap_or_default();
        let base_bps = base_slippage_bps.unwrap_or_else(|| {
            if is_maker {
                cfg.get_f64("maker_slippage_bps", 0.5)
            } else {
                cfg.get_f64("paper_slippage_bps", 3.5)
            }
        });
        let close_px = bar_close.max(1e-8);
        let range_pct = if bar_high > bar_low && bar_low > 0.0 {
            (bar_high - bar_low).max(0.0) / close_px
        } else {
            0.01
        };
        let vol_ratio = (range_pct / 0.01).clamp(0.4, 4.0);
        let bar_notional_liq = (bar_volume.max(0.0) * close_px).max(25_000.0);
        let participation = (notional_usd.max(0.0) / bar_notional_liq).clamp(0.0, 0.25);
        let liq_impact_bps = participation * 120.0;
        let eff_bps = if is_maker {
            base_bps * (0.5 + 0.5 * vol_ratio) + 0.25 * liq_impact_bps
        } else {
            base_bps * vol_ratio + liq_impact_bps
        };
        round_dp(eff_bps.clamp(0.1, 85.0), 4)
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
        let cfg = self.db.load_config().unwrap_or_default();
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
                requested_units: requested_units,
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
                    let _ = self.db.log_order_state_transition(
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
                    let _ = self.db.log_order_state_transition(
                        &cli_id,
                        "PENDING",
                        "OPEN_ON_BOOK",
                        Some("ALO_POSTED_TO_BOOK"),
                    );
                    let _ = self.db.log_order_state_transition(
                        &cli_id,
                        "OPEN_ON_BOOK",
                        "CANCELLED",
                        Some("USER_OR_REGIME_CANCELLED"),
                    );
                    return OrderExecutionResult {
                        client_order_id: cli_id,
                        exchange_order_id: format!("hl-cancel-{}", &Uuid::new_v4().simple().to_string()[..8]),
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
                    let _ = self.db.log_order_state_transition(
                        &cli_id,
                        "PENDING",
                        "OPEN_ON_BOOK",
                        Some("ALO_POSTED_TO_BOOK"),
                    );
                    let _ = self.db.log_order_state_transition(
                        &cli_id,
                        "OPEN_ON_BOOK",
                        "EXPIRED",
                        Some("MAKER_CHASE_TIMEOUT_EXPIRED"),
                    );
                    return OrderExecutionResult {
                        client_order_id: cli_id,
                        exchange_order_id: format!("hl-exp-{}", &Uuid::new_v4().simple().to_string()[..8]),
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

        let cfg = self.db.load_config().unwrap_or_default();
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_live_mode = (mode == "LIVE" || mode == "TESTNET")
            && force_partial_ratio.is_none()
            && simulate_book_scenario.is_none();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";

        if is_live_mode {
            let wallet = cfg.get_str("wallet_address", "");
            let priv_key = cfg.get_str("private_key", "");
            if wallet.is_empty() || priv_key.is_empty() {
                let _ = self.db.log_order_state_transition(
                    &cli_id,
                    "PENDING",
                    "REJECTED",
                    Some("MISSING_CREDENTIALS"),
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

            let Some(asset_idx) =
                self.fetch_hyperliquid_universe_asset_index(&asset.base_asset, is_testnet)
            else {
                let _ = self.db.log_order_state_transition(
                    &cli_id,
                    "PENDING",
                    "REJECTED",
                    Some("UNSUPPORTED_EXCHANGE_ASSET"),
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
            let rounded_px = round_dp(reference_price, px_decimals as i32);
            let rounded_sz = round_dp(quantity, sz_decimals as i32);
            let px_str = float_to_wire(rounded_px);
            let sz_str = float_to_wire(rounded_sz);
            let nonce = Utc::now().timestamp_millis() as u64;

            let order_action = OrderAction {
                action_type: "order".to_string(),
                orders: vec![OrderWire {
                    a: asset_idx,
                    b: is_buy,
                    p: px_str,
                    s: sz_str,
                    r: _is_reduce_only,
                    t: OrderTypeWire {
                        limit: Some(LimitOrderType {
                            tif: "Alo".to_string(),
                        }),
                        trigger: None,
                    },
                    c: None,
                }],
                grouping: "na".to_string(),
            };

            match sign_l1_action(&priv_key, &order_action, None, nonce, None, !is_testnet) {
                Ok(sig) => {
                    let url = if is_testnet {
                        HYPERLIQUID_TESTNET_EXCHANGE_URL
                    } else {
                        HYPERLIQUID_EXCHANGE_URL
                    };
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

                    if let Some(resp) = self.http_post_json(url, &payload) {
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
                                    let _ = self.db.log_order_state_transition(
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
                                    let _ = self.db.log_order_state_transition(
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
                                    let _ = self.db.log_order_state_transition(
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
                            let _ = self.db.log_order_state_transition(
                                &cli_id,
                                "PENDING",
                                "REJECTED",
                                Some(err_msg),
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

                    let _ = self.db.log_order_state_transition(
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
                    let _ = self.db.log_order_state_transition(
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
            let _ = self.db.log_order_state_transition(
                &res.client_order_id,
                &window[0],
                &window[1],
                Some(&res.fill_type),
            );
        }
        res
    }

    pub fn fetch_hyperliquid_universe_asset_index(
        &self,
        coin: &str,
        is_testnet: bool,
    ) -> Option<u32> {
        let url = if is_testnet {
            HYPERLIQUID_TESTNET_INFO_URL
        } else {
            HYPERLIQUID_INFO_URL
        };
        let resp = self.http_post_json(url, &json!({"type": "metaAndAssetCtxs"}))?;
        let arr = resp.as_array()?;
        let universe = arr.first()?.get("universe")?.as_array()?;
        for (idx, u) in universe.iter().enumerate() {
            if let Some(name) = u.get("name").and_then(Value::as_str) {
                if name.eq_ignore_ascii_case(coin) {
                    return Some(idx as u32);
                }
            }
        }
        None
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
        let url = if is_testnet {
            HYPERLIQUID_TESTNET_INFO_URL
        } else {
            HYPERLIQUID_INFO_URL
        };
        self.http_post_json(
            url,
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
        let url = if is_testnet {
            HYPERLIQUID_TESTNET_INFO_URL
        } else {
            HYPERLIQUID_INFO_URL
        };
        let resp = self.http_post_json(
            url,
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
        let url = if is_testnet {
            HYPERLIQUID_TESTNET_INFO_URL
        } else {
            HYPERLIQUID_INFO_URL
        };
        let val = self.http_post_json(
            url,
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
        let url = if is_testnet {
            HYPERLIQUID_TESTNET_INFO_URL
        } else {
            HYPERLIQUID_INFO_URL
        };
        let val = self.http_post_json(
            url,
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
        let cfg = self.db.load_config().unwrap_or_default();
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
        let url = if testnet {
            HYPERLIQUID_TESTNET_INFO_URL
        } else {
            HYPERLIQUID_INFO_URL
        };

        let mut user_equity_usdc = 0.0f64;
        let mut accrued_yield_usdc = 0.0f64;
        let mut vault_apr = 0.125f64;
        let mut lockup_until_ms = 0i64;
        let mut allow_deposits = true;
        let mut live_queried = false;

        if !wallet.is_empty() {
            if let Some(eq_arr) = self
                .http_post_json(
                    url,
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
                                v.as_f64()
                                    .map(|f| f as i64)
                                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok().map(|f| f as i64)))
                            })
                        })
                        .unwrap_or(0);
                    lockup_until_ms = lockup_until_ms.max(lk);
                }
                user_equity_usdc = sum_eq;
            }
        }

        if let Some(details) = self.http_post_json(
            url,
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
                    .unwrap_or(0.125);
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
                            v.as_f64()
                                .map(|f| f as i64)
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok().map(|f| f as i64)))
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
        let cfg = self.db.load_config().unwrap_or_default();
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
        let priv_key = cfg.get_str("private_key", "");
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
                let url = if is_testnet {
                    HYPERLIQUID_TESTNET_EXCHANGE_URL
                } else {
                    HYPERLIQUID_EXCHANGE_URL
                };
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
                if let Some(resp) = self.http_post_json(url, &payload) {
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
        let cfg = self.db.load_config().unwrap_or_default();
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

        if live_ok && total_eq > 0.0 && update_risk_caps {
            let _ = self.db.scale_risk_caps_for_equity(total_eq);
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
            // LONG pays positive funding; SHORT receives positive funding
            let delta = if trade.side == "LONG" {
                -notional * funding_rate
            } else {
                notional * funding_rate
            };
            let new_funding = round_dp(trade.funding_accrued_usdc + delta, 4);
            let _ = self.db.update_open_trade(
                trade.trade_id,
                mark_px,
                trade.chandelier_stop_price,
                trade.highest_price_seen,
                trade.lowest_price_seen,
                trade.unrealized_pnl_usdc,
                new_funding,
            );
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
        let cfg = self.db.load_config().unwrap_or_default();
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
        let wallet = cfg.get_str("wallet_address", "");

        if mode == "PAPER" && simulated_exchange_positions.is_none() && simulated_open_orders.is_none() {
            let open_trades = self.db.get_open_trades().unwrap_or_default();
            return json!({
                "status": "VERIFIED_PAPER",
                "mode": "PAPER",
                "local_open_trades": open_trades.len(),
                "discrepancies_found": 0,
                "discrepancies": [],
            });
        }

        // Build map of exchange positions: base_asset -> (szi, entry_px, mark_px, upnl)
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

        let local_trades = self.db.get_open_trades().unwrap_or_default();
        let mut seen_coins: HashSet<String> = HashSet::new();
        let mut discrepancies: Vec<Value> = Vec::new();

        for lt in &local_trades {
            let coin = lt.base_asset.to_uppercase();
            seen_coins.insert(coin.clone());

            match ex_positions.get(&coin) {
                None => {
                    // Phantom local trade -> close in SQLite
                    let _ = self.db.close_trade(
                        lt.trade_id,
                        lt.current_price,
                        0.0,
                        lt.fees_paid_usdc,
                        "RECONCILED_CLOSED_ON_EXCHANGE",
                    );
                    discrepancies.push(json!({
                        "type": "PHANTOM_LOCAL_CLOSED",
                        "base_asset": coin,
                        "trade_id": lt.trade_id,
                    }));
                }
                Some(&(szi, entry_px, _mark_px, _upnl)) => {
                    let ex_qty = szi.abs();
                    if (ex_qty - lt.quantity.abs()).abs() > 1e-6 {
                        let _ = self.db.reconcile_open_trade_size(
                            lt.trade_id,
                            ex_qty,
                            Some(entry_px),
                        );
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

        if let Some(open_orders) = simulated_open_orders {
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
            let atr = self.risk.compute_atr(&bars, 14);

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
                    if trade.side == "LONG" && ev.hmm_regime == "BEAR" && ev.prob_bear >= 0.62 {
                        exit_reason = Some("REGIME_FLIP_BEAR".to_string());
                        exit_px = last_bar.close;
                    } else if trade.side == "SHORT" && ev.hmm_regime == "BULL" && ev.prob_bull >= 0.60 {
                        exit_reason = Some("REGIME_FLIP_BULL".to_string());
                        exit_px = last_bar.close;
                    } else if ev.prob_chop >= (chop_prob_thresh + 0.20).min(0.82)
                        && ev.blended_conviction.abs() < 0.08
                    {
                        exit_reason = Some("HIGH_CHOP_DELEVERAGE".to_string());
                        exit_px = last_bar.close;
                    }
                }
            }

            if let Some(reason) = exit_reason {
                let exit_fee = (trade.quantity * exit_px) * (taker_fee_bps / 10000.0);
                let total_fees = round_dp(trade.fees_paid_usdc + exit_fee, 4);
                let gross_pnl = if trade.side == "LONG" {
                    (exit_px - trade.entry_price) * trade.quantity
                } else {
                    (trade.entry_price - exit_px) * trade.quantity
                };
                let net_pnl =
                    round_dp(gross_pnl - total_fees + trade.funding_accrued_usdc, 4);
                self.db.close_trade(
                    trade.trade_id,
                    exit_px,
                    net_pnl,
                    total_fees,
                    &reason,
                )?;
                if stop_eval.triggered {
                    let tf_ms = timeframe_seconds(&asset.exec_timeframe) * 1000;
                    self.stop_cooldown_until_ms
                        .lock()
                        .insert(trade.symbol.clone(), now_ms + 14 * tf_ms);
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
        let current_equity = (initial_equity + realized_pnl + unrealized_pnl + accrued_yield_usdc)
            .max(100.0);

        let open_symbols: HashSet<String> =
            active_trades.iter().map(|t| t.symbol.clone()).collect();

        // 3. Evaluate new directional entries for LONG / SHORT candidates
        for ev in evaluations {
            if ev.signal_action != "LONG" && ev.signal_action != "SHORT" {
                continue;
            }
            if open_symbols.contains(&ev.exec_symbol) {
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
            let atr = self.risk.compute_atr(&bars, 14);

            let sizing = self.risk.evaluate_order_risk(
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
            if exec_res.fsm_state != "FILLED" || exec_res.filled_units <= 0.0 {
                continue;
            }

            let fill_px = exec_res.avg_fill_price;
            let filled_qty = exec_res.filled_units;
            let new_trade = TradeRecord {
                trade_id: 0,
                client_order_id: exec_res.client_order_id,
                symbol: asset.symbol.clone(),
                base_asset: asset.base_asset.clone(),
                side: ev.signal_action.clone(),
                status: "OPEN".to_string(),
                fsm_state: "FILLED".to_string(),
                entry_price: fill_px,
                current_price: fill_px,
                exit_price: None,
                quantity: filled_qty,
                filled_units: filled_qty,
                avg_fill_price: Some(fill_px),
                notional_usdc: round_dp(filled_qty * fill_px, 2),
                risk_budget_usdc: sizing.risk_budget_usdc,
                conviction_multiplier: sizing.conviction_multiplier,
                initial_stop_price: sizing.initial_stop_price,
                chandelier_stop_price: sizing.initial_stop_price,
                current_stop_price: sizing.initial_stop_price,
                highest_price_seen: fill_px.max(last_bar.high),
                lowest_price_seen: fill_px.min(last_bar.low),
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
                } else {
                    total_equity = round_dp(
                        initial_equity + realized_after + total_upnl + accrued_yield_usdc,
                        2,
                    );
                }
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
                let alloc_pct = cfg.get_f64("basis_max_idle_cash_allocation_pct", 0.35);
                basis_notional = round_dp(available_cash * alloc_pct, 2);
                if is_backtest && cfg.get_bool("enable_hlp_floor_backtest", false) {
                    let avg_funding = evaluations
                        .iter()
                        .filter(|e| e.signal_action == "BASIS_HARVEST")
                        .map(|e| e.funding_rate_hourly.max(0.00015))
                        .sum::<f64>()
                        / (basis_candidates as f64);
                    accrued_yield_usdc =
                        round_dp(accrued_yield_usdc + basis_notional * avg_funding * 4.0, 4);
                }
            }
            total_equity =
                round_dp(initial_equity + realized_after + total_upnl + accrued_yield_usdc, 2);
        }

        let gross_lev = round_dp(deployed_notional / total_equity.max(1.0), 4);
        let heat_pct = round_dp(portfolio_heat / total_equity.max(1.0), 6);

        let snap = PortfolioSnapshot {
            snapshot_id: None,
            timestamp_ms: now_ms,
            total_equity_usdc: total_equity,
            available_cash_usdc: round_dp((total_equity - deployed_notional).max(0.0), 2),
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
        assert!(partial.state_history.contains(&"PARTIALLY_FILLED".to_string()));

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

        // Mainnet signature test vector from hyperliquid-python-sdk signing_test.py
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

        // Testnet signature test vector from hyperliquid-python-sdk signing_test.py
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

        // 1. Test Deposit Action
        let deposit_action = VaultTransferAction {
            action_type: "vaultTransfer".to_string(),
            vault_address: vault_addr.to_string(),
            is_deposit: true,
            usd: 100_000_000, // 100 USDC in micro-units
        };

        // Verify MessagePack encoding contains exact required keys
        let msgpack_bytes = rmp_serde::encode::to_vec_named(&deposit_action).unwrap();
        assert!(!msgpack_bytes.is_empty());

        let deposit_hash = compute_l1_action_hash(&deposit_action, None, 1700000000000, None).unwrap();
        assert_eq!(deposit_hash.len(), 32);

        let sig_deposit_mainnet = sign_l1_action(pk, &deposit_action, None, 1700000000000, None, true).unwrap();
        assert!(sig_deposit_mainnet["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig_deposit_mainnet["s"].as_str().unwrap().starts_with("0x"));
        let v_dep_main = sig_deposit_mainnet["v"].as_u64().unwrap();
        assert!(v_dep_main == 27 || v_dep_main == 28);

        let sig_deposit_testnet = sign_l1_action(pk, &deposit_action, None, 1700000000000, None, false).unwrap();
        assert!(sig_deposit_testnet["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig_deposit_testnet["s"].as_str().unwrap().starts_with("0x"));
        let v_dep_test = sig_deposit_testnet["v"].as_u64().unwrap();
        assert!(v_dep_test == 27 || v_dep_test == 28);

        // 2. Test Withdrawal Action
        let withdraw_action = VaultTransferAction {
            action_type: "vaultTransfer".to_string(),
            vault_address: vault_addr.to_string(),
            is_deposit: false,
            usd: 50_000_000, // 50 USDC in micro-units
        };

        let withdraw_hash = compute_l1_action_hash(&withdraw_action, None, 1700000000000, None).unwrap();
        assert_eq!(withdraw_hash.len(), 32);
        assert_ne!(deposit_hash, withdraw_hash);

        let sig_withdraw_testnet = sign_l1_action(pk, &withdraw_action, None, 1700000000000, None, false).unwrap();
        assert!(sig_withdraw_testnet["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig_withdraw_testnet["s"].as_str().unwrap().starts_with("0x"));

        // 3. Test Paper Simulation in ExecutionEngine
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
}
