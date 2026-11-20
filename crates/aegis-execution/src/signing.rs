//! Hyperliquid L1 EIP-712 / MessagePack / secp256k1 action signing & wire formatting (`aegis-execution::signing`).

pub use aegis_core::{
    derive_evm_address_from_private_key, validate_evm_address, validate_private_key,
    verify_signer_matches_wallet,
};
use aegis_core::{now_ms, round_dp, AegisError, OrderSide, Result};
use aegis_db::DatabaseManager;
use k256::ecdsa::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use zeroize::Zeroizing;

/// Hyperliquid L1 oracle price band margin (`±3.95%`), safely inside the exchange's `±4.0%`
/// (`Price too far from oracle`) rejection boundary.
pub const HL_ORACLE_BAND_PCT: f64 = 0.0395;

static LAST_NONCE_HIGH_WATER: AtomicI64 = AtomicI64::new(0);
static NONCE_DB_SEEDED: AtomicBool = AtomicBool::new(false);
static LAST_NONCE_PERSISTED_MS: AtomicI64 = AtomicI64::new(0);
const NONCE_LAZY_PERSIST_INTERVAL_MS: i64 = 30_000;

/// Generates a strictly monotonic Hyperliquid L1 nonce backed by an in-process `AtomicI64`
/// high-water floor (seeded once from `now_ms().max(db_nonce)` and persisted lazily to MariaDB).
pub fn next_monotonic_nonce(db: Option<&DatabaseManager>) -> u64 {
    let wall_ms = now_ms();
    if !NONCE_DB_SEEDED.load(Ordering::Acquire) {
        if let Some(d) = db {
            if let Ok(cfg) = d.load_config() {
                let db_floor = cfg.get_i64("hyperliquid_last_nonce", 0).max(0);
                if db_floor > 0 {
                    LAST_NONCE_HIGH_WATER.fetch_max(db_floor, Ordering::SeqCst);
                }
            }
            NONCE_DB_SEEDED.store(true, Ordering::Release);
        }
    }

    let mut prev = LAST_NONCE_HIGH_WATER.load(Ordering::SeqCst);
    let next = loop {
        let n = if wall_ms > prev {
            wall_ms
        } else {
            prev.saturating_add(1)
        };
        match LAST_NONCE_HIGH_WATER.compare_exchange(prev, n, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break n,
            Err(actual) => prev = actual,
        }
    };

    if let Some(d) = db {
        if !d.is_ephemeral_test_db() {
            let last_persist = LAST_NONCE_PERSISTED_MS.load(Ordering::Relaxed);
            if wall_ms.saturating_sub(last_persist) >= NONCE_LAZY_PERSIST_INTERVAL_MS
                && LAST_NONCE_PERSISTED_MS
                    .compare_exchange(last_persist, wall_ms, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
            {
                let _ = d.set_config_value(
                    "hyperliquid_last_nonce",
                    &next.to_string(),
                    Some("Monotonic Hyperliquid L1 EIP-712 action nonce"),
                );
            }
        }
    }
    next as u64
}

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

/// Infers Hyperliquid `szDecimals` (`0..=6`) from `lot_step_size` (`10^-szDecimals`).
pub fn infer_sz_decimals_from_lot_step(lot_step_size: f64) -> u32 {
    if !lot_step_size.is_finite() || lot_step_size <= 0.0 {
        return 3;
    }
    (-lot_step_size.log10()).round().clamp(0.0, 6.0) as u32
}

/// Infers the Hyperliquid L1 perpetual tick size from `mark_price` and `sz_decimals`
/// (5 significant figures and at most `6 - sz_decimals` decimal places).
pub fn infer_tick_size(mark_price: f64, sz_decimals: u32) -> f64 {
    if !mark_price.is_finite() || mark_price <= 0.0 {
        return 0.01;
    }
    let max_perp_decimals = 6usize.saturating_sub(sz_decimals as usize);
    let mag = mark_price.log10().floor() as i32;
    let sig_fig_decimals = (4 - mag).max(0) as usize;
    let allowed_decimals = sig_fig_decimals.min(max_perp_decimals);
    10_f64.powi(-(allowed_decimals as i32)).max(1e-8)
}

/// Snaps a price to the nearest valid tick increment.
pub fn snap_to_tick(price: f64, tick_size: f64) -> f64 {
    if !price.is_finite() || price <= 0.0 {
        return 0.0;
    }
    let tick = if tick_size.is_finite() && tick_size > 0.0 {
        tick_size
    } else {
        0.01
    };
    let decimals = (-tick.log10()).round().clamp(0.0, 8.0) as i32;
    round_dp((price / tick).round() * tick, decimals)
}

/// Snaps an order size down (`floor == true`) or nearest to `lot_step_size` and `sz_decimals`.
pub fn snap_to_lot(size: f64, lot_step_size: f64, sz_decimals: u32, floor: bool) -> f64 {
    if !size.is_finite() || size <= 0.0 {
        return 0.0;
    }
    let lot = if lot_step_size.is_finite() && lot_step_size > 0.0 {
        lot_step_size
    } else {
        10_f64.powi(-(sz_decimals.min(6) as i32))
    };
    let snapped = if floor {
        ((size / lot) + 1e-9).floor() * lot
    } else {
        (size / lot).round() * lot
    };
    round_dp(snapped.max(0.0), sz_decimals.min(8) as i32)
}

/// Formats a perpetual limit price for Hyperliquid L1 wire transmission:
/// - Snaps to `tick_size` if `tick_size > 0`
/// - Enforces at most 5 significant figures (with integer prices allowed for `>= 100_000`)
/// - Enforces at most `6 - sz_decimals` decimal places for perpetuals
pub fn format_hl_wire_price(price: f64, tick_size: f64, sz_decimals: u32) -> String {
    if !price.is_finite() || price <= 0.0 {
        return "0".to_string();
    }
    let max_perp_decimals = 6usize.saturating_sub(sz_decimals as usize);
    let mag = price.log10().floor() as i32;
    let sig_fig_decimals = (4 - mag).max(0) as usize;
    let allowed_decimals = sig_fig_decimals.min(max_perp_decimals);
    let min_wire_step = 10_f64.powi(-(allowed_decimals as i32));
    let effective_tick = if tick_size.is_finite() && tick_size > 0.0 {
        tick_size.max(min_wire_step)
    } else {
        min_wire_step
    };
    let mut p = (price / effective_tick).round() * effective_tick;
    if p <= 0.0 {
        p = price;
    }
    float_to_wire(round_dp(p, allowed_decimals as i32))
}

/// Formats a perpetual order size (`s`) for Hyperliquid L1 wire transmission:
/// - Snaps to `lot_step_size` if `lot_step_size > 0`
/// - Rounds to `sz_decimals` decimal places
pub fn format_hl_wire_size(size: f64, lot_step_size: f64, sz_decimals: u32) -> String {
    if !size.is_finite() || size <= 0.0 {
        return "0".to_string();
    }
    let s = if lot_step_size.is_finite() && lot_step_size > 0.0 {
        (size / lot_step_size).round() * lot_step_size
    } else {
        size
    };
    let dec = (sz_decimals as usize).min(8);
    float_to_wire(round_dp(s, dec as i32))
}

/// Formats a perpetual order size (`s`) floored to `lot_step_size` so `reduce_only`
/// orders never round above the remaining position quantity on the exchange.
pub fn format_hl_wire_size_floor(size: f64, lot_step_size: f64, sz_decimals: u32) -> String {
    if !size.is_finite() || size <= 0.0 {
        return "0".to_string();
    }
    let s = if lot_step_size.is_finite() && lot_step_size > 0.0 {
        ((size / lot_step_size) + 1e-9).floor() * lot_step_size
    } else {
        size
    };
    let dec = (sz_decimals as usize).min(8);
    float_to_wire(round_dp(s.max(0.0), dec as i32))
}

/// Derives a deterministic 128-bit `0x`-prefixed 32-hex-character Hyperliquid client order ID (`cloid`)
/// from an internal `coid` string.
pub fn derive_hl_cloid_hex(coid: &str) -> String {
    let hash = Keccak256::digest(coid.as_bytes());
    format!("0x{}", hex::encode(&hash[..16]))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LimitOrderType {
    pub tif: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriggerOrderTypeWire {
    #[serde(rename = "isMarket")]
    pub is_market: bool,
    #[serde(rename = "triggerPx")]
    pub trigger_px: String,
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
pub struct CancelByCloidWire {
    pub asset: u32,
    pub cloid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelByCloidAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub cancels: Vec<CancelByCloidWire>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SubAccountTransferAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub sub_account_user: String,
    pub is_deposit: bool,
    pub usd: i64,
}

/// Builds a Hyperliquid L1 limit `OrderWire` (`"Alo"`, `"Ioc"`, or `"Gtc"`).
#[allow(clippy::too_many_arguments)]
pub fn build_limit_order_wire(
    asset_index: u32,
    is_buy: bool,
    limit_price: f64,
    quantity: f64,
    tick_size: f64,
    lot_step_size: f64,
    sz_decimals: u32,
    reduce_only: bool,
    tif: &str,
    client_order_id: Option<&str>,
) -> OrderWire {
    let p = format_hl_wire_price(limit_price, tick_size, sz_decimals);
    let s = if reduce_only {
        format_hl_wire_size_floor(quantity, lot_step_size, sz_decimals)
    } else {
        format_hl_wire_size(quantity, lot_step_size, sz_decimals)
    };
    let c = client_order_id
        .filter(|id| !id.trim().is_empty())
        .map(derive_hl_cloid_hex);
    OrderWire {
        a: asset_index,
        b: is_buy,
        p,
        s,
        r: reduce_only,
        t: OrderTypeWire {
            limit: Some(LimitOrderType {
                tif: tif.to_string(),
            }),
            trigger: None,
        },
        c,
    }
}

/// Builds a Hyperliquid L1 reduce-only trigger `OrderWire` (`tpsl = "sl"` or `"tp"`).
#[allow(clippy::too_many_arguments)]
fn build_trigger_tpsl_order_wire(
    asset_index: u32,
    position_side: OrderSide,
    quantity: f64,
    trigger_price: f64,
    tick_size: f64,
    lot_step_size: f64,
    sz_decimals: u32,
    tpsl: &str,
    client_order_id: Option<&str>,
) -> OrderWire {
    // Trigger order closes the open position: SELL (b=false) for LONG, BUY (b=true) for SHORT
    let close_is_buy = !position_side.is_buy();
    let trigger_px_str = format_hl_wire_price(trigger_price, tick_size, sz_decimals);
    let limit_fallback = if close_is_buy {
        trigger_price * (1.0 + HL_ORACLE_BAND_PCT)
    } else {
        (trigger_price * (1.0 - HL_ORACLE_BAND_PCT)).max(tick_size.max(1e-8))
    };
    let limit_px_str = format_hl_wire_price(limit_fallback, tick_size, sz_decimals);
    let mut sz_str = format_hl_wire_size_floor(quantity, lot_step_size, sz_decimals);
    if sz_str == "0" {
        sz_str = format_hl_wire_size(quantity, lot_step_size, sz_decimals);
    }
    let c = client_order_id
        .filter(|id| !id.trim().is_empty())
        .map(derive_hl_cloid_hex);
    OrderWire {
        a: asset_index,
        b: close_is_buy,
        p: limit_px_str,
        s: sz_str,
        r: true,
        t: OrderTypeWire {
            limit: None,
            trigger: Some(TriggerOrderTypeWire {
                is_market: true,
                trigger_px: trigger_px_str,
                tpsl: tpsl.to_string(),
            }),
        },
        c,
    }
}

/// Builds a Hyperliquid L1 protective stop-loss trigger `OrderWire` (`reduce_only = true`, `tpsl = "sl"`).
#[allow(clippy::too_many_arguments)]
pub fn build_protective_stop_order_wire(
    asset_index: u32,
    position_side: OrderSide,
    quantity: f64,
    trigger_price: f64,
    tick_size: f64,
    lot_step_size: f64,
    sz_decimals: u32,
    client_order_id: Option<&str>,
) -> OrderWire {
    build_trigger_tpsl_order_wire(
        asset_index,
        position_side,
        quantity,
        trigger_price,
        tick_size,
        lot_step_size,
        sz_decimals,
        "sl",
        client_order_id,
    )
}

/// Builds a Hyperliquid L1 take-profit trigger `OrderWire` (`reduce_only = true`, `tpsl = "tp"`).
#[allow(clippy::too_many_arguments)]
pub fn build_take_profit_order_wire(
    asset_index: u32,
    position_side: OrderSide,
    quantity: f64,
    trigger_price: f64,
    tick_size: f64,
    lot_step_size: f64,
    sz_decimals: u32,
    client_order_id: Option<&str>,
) -> OrderWire {
    build_trigger_tpsl_order_wire(
        asset_index,
        position_side,
        quantity,
        trigger_price,
        tick_size,
        lot_step_size,
        sz_decimals,
        "tp",
        client_order_id,
    )
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
/// Zeroizes intermediate normalized key strings and raw secret key bytes on drop.
pub fn sign_l1_action<T: Serialize>(
    private_key_hex: &str,
    action: &T,
    vault_address: Option<&str>,
    nonce: u64,
    expires_after: Option<u64>,
    is_mainnet: bool,
) -> Result<Value> {
    if private_key_hex.trim().is_empty() {
        return Err(AegisError::Crypto(
            "Private key cannot be empty for L1 action signing".to_string(),
        ));
    }
    let normalized_pk = Zeroizing::new(validate_private_key(private_key_hex)?);
    let hex_body = normalized_pk
        .strip_prefix("0x")
        .or_else(|| normalized_pk.strip_prefix("0X"))
        .unwrap_or(normalized_pk.as_str());
    if hex_body.len() != 64 {
        return Err(AegisError::Crypto(format!(
            "Normalized private key must contain 64 hex chars, got {}",
            hex_body.len()
        )));
    }
    let raw_bytes = Zeroizing::new(
        hex::decode(hex_body)
            .map_err(|e| AegisError::Crypto(format!("Invalid private_key hex: {e}")))?,
    );
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
    agent_buf.extend_from_slice(&Keccak256::digest(
        b"Agent(string source,bytes32 connectionId)",
    ));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eip712_sign_l1_action_and_protective_stop_wire() {
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let order = build_limit_order_wire(
            0,
            true,
            92450.5,
            0.05,
            0.5,
            0.0001,
            4,
            false,
            "Alo",
            Some("v2-btc-long-1"),
        );
        assert_eq!(order.p, "92451");
        assert_eq!(order.s, "0.05");
        assert!(order.c.as_ref().unwrap().starts_with("0x"));
        assert_eq!(order.c.as_ref().unwrap().len(), 34);

        let stop = build_protective_stop_order_wire(
            3,
            OrderSide::Long,
            14.528,
            176.82,
            0.01,
            0.01,
            2,
            Some("sl-sol-1"),
        );
        assert!(!stop.b);
        assert!(stop.r);
        assert_eq!(stop.s, "14.52");
        assert_eq!(stop.t.trigger.as_ref().unwrap().trigger_px, "176.82");
        assert_eq!(stop.t.trigger.as_ref().unwrap().tpsl, "sl");

        let tp = build_take_profit_order_wire(
            3,
            OrderSide::Long,
            14.528,
            199.15,
            0.01,
            0.01,
            2,
            Some("tp-sol-1"),
        );
        assert!(!tp.b);
        assert!(tp.r);
        assert_eq!(tp.s, "14.52");
        assert_eq!(tp.t.trigger.as_ref().unwrap().trigger_px, "199.15");
        assert_eq!(tp.t.trigger.as_ref().unwrap().tpsl, "tp");

        let action = OrderAction {
            action_type: "order".to_string(),
            orders: vec![order, stop, tp],
            grouping: "na".to_string(),
        };
        let n1 = next_monotonic_nonce(None);
        let n2 = next_monotonic_nonce(None);
        assert!(n2 > n1);

        let sig = sign_l1_action(pk, &action, None, n1, None, false).unwrap();
        assert_eq!(
            sig["signer"].as_str().unwrap(),
            "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23"
        );
        assert!(sig["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig["s"].as_str().unwrap().starts_with("0x"));
        let v = sig["v"].as_u64().unwrap();
        assert!(v == 27 || v == 28);
    }
}
