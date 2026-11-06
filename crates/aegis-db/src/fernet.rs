//! Wire-compatible Fernet (AES-128-CBC + HMAC-SHA256) cipher with automatic key zeroization.
//!
//! Matches Python `cryptography.fernet.Fernet` (`0x80 || timestamp_be64 || iv_16 || ciphertext || hmac_sha256_32`)
//! so encrypted secrets (`gAAAAAB...`) in `data/aegis_quant.db` are 100% interoperable between Rust and Python.

use aegis_core::{AegisError, Result};
use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use zeroize::{Zeroize, ZeroizeOnDrop};

type Aes128CbcEnc = cbc::Encryptor<aes::Aes128>;
type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;
type HmacSha256 = Hmac<Sha256>;

use std::os::unix::fs::PermissionsExt;
use std::sync::OnceLock;

const LEGACY_PUBLIC_FERNET_SEED_V1: &[u8] = b"aegis-quant-v0.2.0-rc1-fernet-master-key";
const LEGACY_PUBLIC_FERNET_SEED_V2: &[u8] = b"aegis-quant-v0.2.0-local-fernet-master-key-seed";

static EPHEMERAL_MEMORY_KEY: OnceLock<String> = OnceLock::new();

/// Returns the ephemeral per-process CSPRNG Fernet key used exclusively for isolated `:memory:` databases.
pub fn default_host_fernet_key_b64() -> String {
    EPHEMERAL_MEMORY_KEY
        .get_or_init(generate_fernet_key)
        .clone()
}

/// Returns true if `candidate_b64` matches any retired hardcoded fallback seed (`S1`).
pub fn is_legacy_public_fallback_key(candidate_b64: &str) -> bool {
    let trimmed = candidate_b64.trim();
    let legacy1 = URL_SAFE.encode(Sha256::digest(LEGACY_PUBLIC_FERNET_SEED_V1));
    let legacy2 = URL_SAFE.encode(Sha256::digest(LEGACY_PUBLIC_FERNET_SEED_V2));
    trimmed == legacy1 || trimmed == legacy2
}

/// Generate a fresh CSPRNG 32-byte URL-safe base64 Fernet master key.
pub fn generate_fernet_key() -> String {
    let mut raw = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut raw);
    let encoded = URL_SAFE.encode(raw);
    raw.zeroize();
    encoded
}

/// Normalize a raw key string/bytes into a valid 44-char URL-safe base64 Fernet key (decoding to 32 bytes).
pub fn normalize_fernet_key(raw_key: &[u8]) -> String {
    let trimmed = trim_ascii(raw_key);
    if trimmed.is_empty() {
        return default_host_fernet_key_b64();
    }
    if let Ok(decoded) = URL_SAFE.decode(trimmed) {
        if decoded.len() == 32 {
            return String::from_utf8_lossy(trimmed).into_owned();
        }
    }
    let digest = Sha256::digest(trimmed);
    URL_SAFE.encode(digest)
}

pub fn is_usable_env_key(raw_val: &str) -> bool {
    let cleaned = raw_val.trim();
    if cleaned.is_empty() {
        return false;
    }
    !(cleaned.starts_with('<') && cleaned.ends_with('>'))
}

/// Return the canonical `.aegis_master_key` file path adjacent to `db_path` (`S4`).
pub fn master_key_file_for_db(db_path: &Path) -> std::path::PathBuf {
    let parent = db_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(".aegis_master_key")
}

/// Write a normalized Fernet master key to `key_file` with `0600` permissions (`S4`, `S5`).
pub fn write_master_key_file(key_file: &Path, key_b64: &str) -> Result<()> {
    if let Some(parent) = key_file.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(key_file, format!("{}\n", key_b64.trim()))?;
    let _ = fs::set_permissions(key_file, fs::Permissions::from_mode(0o600));
    Ok(())
}

/// Ensures that a file-backed database has an external key source or provisions `<db_dir>/.aegis_master_key` (`0600`).
pub fn ensure_master_key_for_db(db_path: &Path) -> Result<String> {
    let (key, has_ext) = resolve_fernet_master_key(None, Some(db_path));
    if has_ext {
        return Ok(key);
    }
    if db_path.to_string_lossy() == ":memory:" {
        return Ok(key);
    }
    let key_file = master_key_file_for_db(db_path);
    let fresh_key = generate_fernet_key();
    write_master_key_file(&key_file, &fresh_key)?;
    Ok(fresh_key)
}

/// Resolve the AES-128 Fernet master key from:
/// 1. Explicit `master_key` argument
/// 2. systemd `LoadCredentialEncrypted=` / `LoadCredential=` (`$CREDENTIALS_DIRECTORY`)
/// 3. Environment variable (`AEGIS_MASTER_KEY` or `FERNET_MASTER_KEY`)
/// 4. Local keyfile (`<db_dir>/.aegis_master_key`, chmod 0600)
/// 5. Ephemeral per-process CSPRNG key (only for `:memory:` test instances; marked `external = false`)
pub fn resolve_fernet_master_key(master_key: Option<&str>, db_path: Option<&Path>) -> (String, bool) {
    if let Some(explicit) = master_key {
        if !explicit.trim().is_empty() {
            let norm = normalize_fernet_key(explicit.as_bytes());
            if !is_legacy_public_fallback_key(&norm) {
                return (norm, true);
            }
        }
    }

    if let Ok(cred_dir) = std::env::var("CREDENTIALS_DIRECTORY") {
        let cred_dir = cred_dir.trim();
        if !cred_dir.is_empty() {
            let dir_path = Path::new(cred_dir);
            if dir_path.is_dir() {
                for name in ["aegis_master_key", "master_key", "fernet_key"] {
                    let cand = dir_path.join(name);
                    if cand.is_file() {
                        if let Ok(bytes) = fs::read(&cand) {
                            if !trim_ascii(&bytes).is_empty() {
                                let norm = normalize_fernet_key(&bytes);
                                if !is_legacy_public_fallback_key(&norm) {
                                    return (norm, true);
                                }
                            }
                        }
                    }
                }
                if let Ok(entries) = fs::read_dir(dir_path) {
                    let mut paths: Vec<_> = entries.flatten().map(|e| e.path()).collect();
                    paths.sort();
                    for child in paths {
                        if child.is_file() {
                            if let Ok(bytes) = fs::read(&child) {
                                if !trim_ascii(&bytes).is_empty() {
                                    let norm = normalize_fernet_key(&bytes);
                                    if !is_legacy_public_fallback_key(&norm) {
                                        return (norm, true);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let env_key = std::env::var("AEGIS_MASTER_KEY")
        .ok()
        .filter(|s| is_usable_env_key(s))
        .or_else(|| {
            std::env::var("FERNET_MASTER_KEY")
                .ok()
                .filter(|s| is_usable_env_key(s))
        });
    if let Some(k) = env_key {
        let norm = normalize_fernet_key(k.as_bytes());
        if !is_legacy_public_fallback_key(&norm) {
            return (norm, true);
        }
    }

    if let Some(p) = db_path {
        if p.to_string_lossy() != ":memory:" {
            if let Some(parent) = p.parent() {
                let key_file = parent.join(".aegis_master_key");
                if key_file.is_file() {
                    if let Ok(bytes) = fs::read(&key_file) {
                        if !trim_ascii(&bytes).is_empty() {
                            let norm = normalize_fernet_key(&bytes);
                            if !is_legacy_public_fallback_key(&norm) {
                                return (norm, true);
                            }
                        }
                    }
                }
            }
        }
    }

    (default_host_fernet_key_b64(), false)
}

/// Zeroizing Fernet AES-128-CBC + HMAC-SHA256 cipher.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct FernetCipher {
    signing_key: [u8; 16],
    encryption_key: [u8; 16],
    key_b64: String,
}

impl FernetCipher {
    pub fn from_key_str(raw_key: &str) -> Result<Self> {
        let norm_b64 = normalize_fernet_key(raw_key.as_bytes());
        if is_legacy_public_fallback_key(&norm_b64) {
            return Err(AegisError::SecurityViolation(
                "Refusing to initialize FernetCipher with retired public fallback key seed (S1)."
                    .to_string(),
            ));
        }
        let decoded = URL_SAFE
            .decode(norm_b64.as_bytes())
            .map_err(|e| AegisError::Crypto(format!("Invalid Fernet base64 key: {e}")))?;
        if decoded.len() != 32 {
            return Err(AegisError::Crypto(
                "Fernet key must decode to 32 bytes (16 signing + 16 encryption)".to_string(),
            ));
        }
        let mut signing_key = [0u8; 16];
        let mut encryption_key = [0u8; 16];
        signing_key.copy_from_slice(&decoded[..16]);
        encryption_key.copy_from_slice(&decoded[16..32]);
        Ok(Self {
            signing_key,
            encryption_key,
            key_b64: norm_b64,
        })
    }

    pub fn key_b64(&self) -> &str {
        &self.key_b64
    }

    /// Encrypt plaintext into a URL-safe base64 Fernet token (`gAAAAAB...`).
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<String> {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut iv = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut iv);

        let pad_len = ((plaintext.len() / 16) + 1) * 16;
        let mut buf = vec![0u8; pad_len];
        buf[..plaintext.len()].copy_from_slice(plaintext);

        let ciphertext = Aes128CbcEnc::new(&self.encryption_key.into(), &iv.into())
            .encrypt_padded_mut::<Pkcs7>(&mut buf, plaintext.len())
            .map_err(|e| AegisError::Crypto(format!("AES-128-CBC encryption error: {e}")))?;

        let mut payload = Vec::with_capacity(1 + 8 + 16 + ciphertext.len() + 32);
        payload.push(0x80); // Fernet version byte
        payload.extend_from_slice(&now_secs.to_be_bytes());
        payload.extend_from_slice(&iv);
        payload.extend_from_slice(ciphertext);

        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.signing_key)
            .map_err(|e| AegisError::Crypto(format!("HMAC init error: {e}")))?;
        mac.update(&payload);
        let tag = mac.finalize().into_bytes();
        payload.extend_from_slice(&tag);

        Ok(URL_SAFE.encode(&payload))
    }

    /// Decrypt a URL-safe base64 Fernet token (`gAAAAAB...`) and verify its HMAC-SHA256 tag.
    pub fn decrypt(&self, token_str: &str) -> Result<String> {
        let clean = token_str.trim();
        let raw_token = clean.strip_prefix("ENC:").unwrap_or(clean);
        let data = URL_SAFE
            .decode(raw_token.as_bytes())
            .map_err(|e| AegisError::Crypto(format!("Invalid Fernet token base64: {e}")))?;

        // Minimum length: 1 (ver) + 8 (ts) + 16 (iv) + 16 (1 block) + 32 (hmac) = 73 bytes
        if data.len() < 73 || data[0] != 0x80 {
            return Err(AegisError::Crypto("Malformed Fernet token header".to_string()));
        }
        let body_end = data.len() - 32;
        let signed_portion = &data[..body_end];
        let tag = &data[body_end..];

        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.signing_key)
            .map_err(|e| AegisError::Crypto(format!("HMAC init error: {e}")))?;
        mac.update(signed_portion);
        mac.verify_slice(tag).map_err(|_| {
            AegisError::Crypto(
                "Failed to decrypt Fernet-encrypted configuration value: invalid master key or corrupted token.".to_string(),
            )
        })?;

        let iv = &data[9..25];
        let ciphertext = &data[25..body_end];
        if ciphertext.is_empty() || ciphertext.len() % 16 != 0 {
            return Err(AegisError::Crypto(
                "Invalid Fernet ciphertext block length".to_string(),
            ));
        }

        let mut buf = ciphertext.to_vec();
        let decrypted = Aes128CbcDec::new(&self.encryption_key.into(), iv.into())
            .decrypt_padded_mut::<Pkcs7>(&mut buf)
            .map_err(|_| AegisError::Crypto("Invalid PKCS7 padding in Fernet token".to_string()))?;

        String::from_utf8(decrypted.to_vec())
            .map_err(|e| AegisError::Crypto(format!("Decrypted token is not valid UTF-8: {e}")))
    }
}

pub fn is_encrypted_value(val: &str) -> bool {
    let s = val.trim();
    s.starts_with("gAAAAAB") || s.starts_with("ENC:gAAAAAB")
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map(|i| i + 1)
        .unwrap_or(start);
    &bytes[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fernet_roundtrip_and_tamper_detection() {
        let key = default_host_fernet_key_b64();
        let cipher = FernetCipher::from_key_str(&key).unwrap();
        let secret = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let token = cipher.encrypt(secret.as_bytes()).unwrap();
        assert!(token.starts_with("gAAAAAB"));
        assert!(is_encrypted_value(&token));

        let plain = cipher.decrypt(&token).unwrap();
        assert_eq!(plain, secret);

        // Wrong key fails HMAC verification
        let other = FernetCipher::from_key_str("custom-other-master-key").unwrap();
        assert!(other.decrypt(&token).is_err());
    }
}
