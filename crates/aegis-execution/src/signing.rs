//! Hyperliquid L1 EIP-712 / MessagePack / secp256k1 action signing (`aegis-execution::signing`).

pub use aegis_core::{
    derive_evm_address_from_private_key, validate_private_key, verify_signer_matches_wallet,
};
use aegis_core::{round_dp, AegisError, Result};
use k256::ecdsa::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use zeroize::Zeroizing;

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

/// Formats a perpetual limit price for Hyperliquid L1 wire transmission (`NH-9`):
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

/// Formats a perpetual order size (`s`) for Hyperliquid L1 wire transmission (`NH-9`):
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

/// Formats a perpetual order size (`s`) floored to `lot_step_size` (`HI-7`) so `reduce_only`
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
/// from an internal `coid` string (`HI-1`, `ME-6`).
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
/// Zeroizes intermediate normalized key strings and raw secret key bytes on drop (`S10`, `NM-11`).
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
