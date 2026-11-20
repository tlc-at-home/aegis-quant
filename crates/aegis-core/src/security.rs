//! Cryptographic validation, secp256k1/EVM address derivation, redaction, and dataset provenance (`aegis-core::security`).

use crate::errors::{AegisError, Result};
use k256::ecdsa::{SigningKey, VerifyingKey};
use sha2::{Digest as Sha2Digest, Sha256};
use sha3::Keccak256;
use std::fs;
use std::path::Path;

pub const SENSITIVE_CONFIG_KEYS: &[&str] = &[
    "private_key",
    "gemini_api_key",
    "telegram_bot_token",
    "discord_webhook_url",
    "web_api_token",
];

/// Returns `true` if `key` is a sensitive secret or identifier that must be redacted in logs and REST echoes.
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

/// Security-critical config keys that cannot be weakened or mutated via the Web API.
pub const IMMUTABLE_SECURITY_CONFIG_KEYS: &[&str] = &[
    "enforce_master_key",
    "web_auth_enabled",
    "web_require_auth",
    "web_allow_remote_bind",
    "web_bind_host",
    "web_csp_policy",
    "web_cors_allowed_origins",
    "web_allowed_hosts",
    "web_trust_proxy_headers",
    "web_cookie_secure",
    "web_api_token",
    "allow_web_live_mode",
    "telegram_api_base_url",
    "hyperliquid_testnet",
    "hyperliquid_mainnet_url",
    "hyperliquid_testnet_url",
    "hyperliquid_info_url",
    "hyperliquid_testnet_info_url",
    "hyperliquid_exchange_url",
    "hyperliquid_testnet_exchange_url",
    "hyperliquid_allowed_hosts",
    "live_vault_info_json",
];

/// Returns true if `key` is an immutable security policy key that cannot be modified via the Web API.
pub fn is_immutable_security_config_key(key: &str) -> bool {
    let k = key.trim().to_ascii_lowercase();
    IMMUTABLE_SECURITY_CONFIG_KEYS.contains(&k.as_str())
}

/// Validates a 42-char `0x`-prefixed EVM hexadecimal address and normalizes it to lowercase.
pub fn validate_evm_address(addr: &str) -> Result<String> {
    let trimmed = addr.trim();
    let hex_part = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .ok_or_else(|| {
            AegisError::Validation("EVM address must start with '0x' prefix".to_string())
        })?;
    if hex_part.len() != 40 || !hex_part.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(AegisError::Validation(
            "EVM address must contain exactly 40 hexadecimal characters after '0x'".to_string(),
        ));
    }
    Ok(format!("0x{}", hex_part.to_ascii_lowercase()))
}

/// Validates a 32-byte secp256k1 private key hex string (`0x`-prefixed or raw 64 hex chars).
pub fn validate_private_key(pk_hex: &str) -> Result<String> {
    let trimmed = pk_hex.trim();
    let hex_part = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    if hex_part.len() != 64 || !hex_part.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(AegisError::Validation(
            "Private key must be 64 hexadecimal characters (32 bytes)".to_string(),
        ));
    }
    let bytes = hex::decode(hex_part)
        .map_err(|e| AegisError::Validation(format!("Invalid private key hex: {e}")))?;
    let _ = SigningKey::from_slice(&bytes).map_err(|e| {
        AegisError::Validation(format!("Invalid secp256k1 private key scalar: {e}"))
    })?;
    Ok(format!("0x{}", hex_part.to_ascii_lowercase()))
}

/// Derives the lowercase `0x`-prefixed 20-byte EVM address from a secp256k1 private key hex string.
pub fn derive_evm_address_from_private_key(pk_hex: &str) -> Result<String> {
    let norm = validate_private_key(pk_hex)?;
    let raw = hex::decode(&norm[2..])
        .map_err(|e| AegisError::Crypto(format!("Private key hex decode failed: {e}")))?;
    let signing_key = SigningKey::from_slice(&raw)
        .map_err(|e| AegisError::Crypto(format!("Invalid secp256k1 key: {e}")))?;
    let verify_key = VerifyingKey::from(&signing_key);
    let uncompressed = verify_key.to_encoded_point(false);
    let pub_bytes = &uncompressed.as_bytes()[1..];
    let digest = Keccak256::digest(pub_bytes);
    Ok(format!("0x{}", hex::encode(&digest[12..])))
}

/// Verifies or derives the signer EVM address for a configured wallet + private key pair.
/// Note: Hyperliquid allows Trade-Only API Agent wallets whose signer address differs from the master vault/wallet address.
pub fn verify_signer_matches_wallet(wallet_address: &str, private_key: &str) -> Result<String> {
    let _wallet = validate_evm_address(wallet_address)?;
    derive_evm_address_from_private_key(private_key)
}

/// Masks a sensitive identifier (wallet address, chat ID, token) for safe display.
pub fn mask_identifier(raw: &str) -> String {
    let s = raw.trim();
    if s.is_empty() {
        return String::new();
    }
    let char_count = s.chars().count();
    if char_count <= 6 {
        return "***".to_string();
    }
    if char_count <= 12 {
        let prefix: String = s.chars().take(2).collect();
        let suffix: String = s.chars().skip(char_count - 2).collect();
        return format!("{prefix}***{suffix}");
    }
    let prefix: String = s.chars().take(6).collect();
    let suffix: String = s.chars().skip(char_count - 4).collect();
    format!("{prefix}...{suffix}")
}

/// Redacts Fernet tokens (`gAAAAAB...`) and raw 64-hex private keys from log/telemetry strings.
pub fn sanitize_telemetry_message(msg: &str) -> String {
    let mut out = Vec::new();
    for token in msg.split_whitespace() {
        let clean = token.trim_matches(|c: char| {
            matches!(
                c,
                '"' | '\'' | ',' | ';' | '(' | ')' | '[' | ']' | '{' | '}'
            )
        });
        if clean.starts_with("gAAAAAB") || clean.starts_with("ENC:gAAAAAB") {
            out.push("[REDACTED_FERNET_TOKEN]".to_string());
        } else if (clean.len() == 64 && clean.chars().all(|c| c.is_ascii_hexdigit()))
            || (clean.len() == 66
                && (clean.starts_with("0x") || clean.starts_with("0X"))
                && clean[2..].chars().all(|c| c.is_ascii_hexdigit()))
        {
            out.push("[REDACTED_HEX_KEY]".to_string());
        } else {
            out.push(token.to_string());
        }
    }
    out.join(" ")
}

/// Computes and optionally verifies the SHA-256 checksum of a dataset file against `<path>.sha256`.
pub fn verify_data_provenance(path: &Path) -> Result<String> {
    let bytes = fs::read(path).map_err(|e| {
        AegisError::Validation(format!(
            "Failed to read dataset file '{}': {e}",
            path.display()
        ))
    })?;
    let actual_hex = hex::encode(Sha256::digest(&bytes));
    let sha_path = path.with_extension(format!(
        "{}.sha256",
        path.extension().and_then(|s| s.to_str()).unwrap_or("")
    ));
    if sha_path.is_file() {
        if let Ok(expected_raw) = fs::read_to_string(&sha_path) {
            if let Some(expected_hex) = expected_raw.split_whitespace().next() {
                if !expected_hex.eq_ignore_ascii_case(&actual_hex) {
                    return Err(AegisError::SecurityViolation(format!(
                        "Dataset SHA-256 mismatch for '{}': expected {}, got {}",
                        path.display(),
                        expected_hex,
                        actual_hex
                    )));
                }
            }
        }
    }
    Ok(actual_hex)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evm_and_secp256k1_validation() {
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let norm_pk = validate_private_key(pk).unwrap();
        assert_eq!(norm_pk, pk);
        let addr = derive_evm_address_from_private_key(pk).unwrap();
        assert_eq!(addr, "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23");
        assert!(validate_evm_address(&addr).is_ok());
    }

    #[test]
    fn test_immutable_keys_and_utf8_mask_identifier() {
        assert!(is_immutable_security_config_key("web_require_auth"));
        assert!(is_immutable_security_config_key("hyperliquid_testnet"));
        assert!(is_immutable_security_config_key("web_trust_proxy_headers"));
        assert!(is_immutable_security_config_key("web_cookie_secure"));
        assert!(is_immutable_security_config_key("allow_web_live_mode"));
        assert!(is_immutable_security_config_key(
            " WEB_TRUST_PROXY_HEADERS "
        ));
        assert!(is_immutable_security_config_key("ALLOW_WEB_LIVE_MODE"));
        assert!(is_immutable_security_config_key("live_vault_info_json"));
        assert!(is_immutable_security_config_key(" LIVE_VAULT_INFO_JSON "));
        assert_eq!(mask_identifier(""), "");
        assert_eq!(mask_identifier("αβγδ"), "***");
        assert_eq!(mask_identifier("🔥🚀αβγδε"), "🔥🚀***δε");
        assert_eq!(mask_identifier("🔥🚀αβγδεζηθικλμ"), "🔥🚀αβγδ...ικλμ");
        // Variation-selector emoji never panics on char-boundary slicing
        assert!(!mask_identifier("🔥🛡️αβγδε").is_empty());
    }
}
