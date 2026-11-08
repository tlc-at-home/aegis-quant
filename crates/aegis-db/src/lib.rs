//! SQLite WAL connection manager, schema/migration runner, Fernet secret encryption,
//! and centralized variable loader (`system_config`) for Aegis Quant.

pub mod config_ops;
pub mod fernet;
pub mod pond_ops;
pub mod schema_ops;
pub mod scoped;
pub mod trade_ops;

pub use config_ops::{cast_config_value, ConfigMap};
pub use scoped::{BookScopedDb, PitConfig, PondConfig};

use crate::fernet::{
    is_encrypted_value, is_legacy_public_fallback_key, is_usable_env_key, master_key_file_for_db,
    normalize_fernet_key, resolve_fernet_master_key, write_master_key_file, FernetCipher,
};
use aegis_core::{resolve_project_root, AegisError, Result, SENSITIVE_CONFIG_KEYS};
use parking_lot::{Mutex, RwLock};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;
use zeroize::Zeroizing;

/// Cached Fernet key and cipher state (`R5`).
#[derive(Clone)]
pub(crate) struct CachedFernetState {
    pub(crate) key_b64: String,
    pub(crate) is_explicit: bool,
    pub(crate) env_override: Option<String>,
    pub(crate) cipher: FernetCipher,
}

/// Thread-safe SQLite Write-Ahead Logging (WAL) and `:memory:` database manager.
#[derive(Clone)]
pub struct DatabaseManager {
    pub db_path: PathBuf,
    pub schema_path: PathBuf,
    pub(crate) is_memory: bool,
    pub(crate) memory_conn: Option<Arc<Mutex<Connection>>>,
    pub(crate) conn_pool: Arc<Mutex<Vec<Connection>>>,
    pub(crate) pit_timestamp_ms: Arc<AtomicI64>,
    pub(crate) explicit_master_key: Arc<Mutex<Option<String>>>,
    pub(crate) cipher_cache: Arc<RwLock<Option<CachedFernetState>>>,
}

impl DatabaseManager {
    pub fn new(db_path: impl AsRef<Path>) -> Self {
        let project_root = resolve_project_root();
        let schema_path = project_root.join("sql").join("schema_and_seed.sql");
        Self::with_paths(db_path, schema_path, None)
    }

    pub fn default_db() -> Self {
        let project_root = resolve_project_root();
        Self::new(project_root.join("data").join("aegis_quant.db"))
    }

    pub fn memory() -> Self {
        Self::new(":memory:")
    }

    pub fn with_paths(
        db_path: impl AsRef<Path>,
        schema_path: impl AsRef<Path>,
        master_key: Option<String>,
    ) -> Self {
        let raw_path = db_path.as_ref().to_string_lossy().trim().to_string();
        let is_memory = raw_path == ":memory:" || raw_path.starts_with("file::memory:");
        let resolved_db_path = if is_memory {
            PathBuf::from(":memory:")
        } else {
            let p = PathBuf::from(&raw_path);
            if let Some(parent) = p.parent() {
                let _ = fs::create_dir_all(parent);
            }
            p
        };

        let memory_conn = if is_memory {
            let conn = Connection::open_in_memory().expect("Failed to open :memory: SQLite");
            let _ = conn.execute_batch(
                "PRAGMA temp_store = MEMORY; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;",
            );
            Some(Arc::new(Mutex::new(conn)))
        } else {
            None
        };

        Self {
            db_path: resolved_db_path,
            schema_path: schema_path.as_ref().to_path_buf(),
            is_memory,
            memory_conn,
            conn_pool: Arc::new(Mutex::new(Vec::with_capacity(8))),
            pit_timestamp_ms: Arc::new(AtomicI64::new(-1)),
            explicit_master_key: Arc::new(Mutex::new(master_key)),
            cipher_cache: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set_master_key(&self, master_key: Option<String>) {
        *self.explicit_master_key.lock() = master_key;
        *self.cipher_cache.write() = None;
    }

    pub fn set_pit_timestamp_ms(&self, ts: Option<i64>) {
        self.pit_timestamp_ms
            .store(ts.unwrap_or(-1), Ordering::SeqCst);
    }

    pub fn get_pit_timestamp_ms(&self) -> Option<i64> {
        let v = self.pit_timestamp_ms.load(Ordering::SeqCst);
        if v >= 0 {
            Some(v)
        } else {
            None
        }
    }

    pub fn now_ms(&self) -> i64 {
        self.get_pit_timestamp_ms()
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis())
    }

    pub(crate) fn apply_configured_pragmas(conn: &Connection) {
        let mut busy_ms: u64 = 15000;
        let mut sync_mode = "NORMAL".to_string();
        let mut cache_kib: i64 = -32000;
        let mut mmap_bytes: i64 = 268_435_456;

        if let Ok(mut stmt) = conn.prepare(
            "SELECT config_key, config_value FROM system_config
             WHERE config_key IN (
                 'sqlite_busy_timeout_ms',
                 'sqlite_synchronous_mode',
                 'sqlite_cache_size_kib',
                 'sqlite_mmap_size_bytes'
             );",
        ) {
            if let Ok(rows) = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                ))
            }) {
                for row in rows.flatten() {
                    match row.0.as_str() {
                        "sqlite_busy_timeout_ms" => {
                            if let Ok(v) = row.1.trim().parse::<u64>() {
                                busy_ms = v.clamp(1000, 120_000);
                            }
                        }
                        "sqlite_synchronous_mode" => {
                            let m = row.1.trim().to_ascii_uppercase();
                            if matches!(m.as_str(), "OFF" | "NORMAL" | "FULL" | "EXTRA") {
                                sync_mode = m;
                            }
                        }
                        "sqlite_cache_size_kib" => {
                            if let Ok(v) = row.1.trim().parse::<i64>() {
                                cache_kib = v;
                            }
                        }
                        "sqlite_mmap_size_bytes" => {
                            if let Ok(v) = row.1.trim().parse::<i64>() {
                                mmap_bytes = v.max(0);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        let _ = conn.busy_timeout(Duration::from_millis(busy_ms));
        let pragma_sql = format!(
            "PRAGMA synchronous = {sync_mode};
             PRAGMA cache_size = {cache_kib};
             PRAGMA mmap_size = {mmap_bytes};"
        );
        let _ = conn.execute_batch(&pragma_sql);
    }

    fn open_configured_file_conn(&self) -> Result<Connection> {
        let mut last_err = None;
        for attempt in 0..16 {
            match Connection::open(&self.db_path) {
                Ok(conn) => {
                    let _ = conn.busy_timeout(Duration::from_millis(15000));
                    match conn.execute_batch(
                        "PRAGMA temp_store = MEMORY;
                         PRAGMA journal_mode = WAL;
                         PRAGMA synchronous = NORMAL;
                         PRAGMA foreign_keys = ON;",
                    ) {
                        Ok(()) => {
                            Self::apply_configured_pragmas(&conn);
                            return Ok(conn);
                        }
                        Err(e) => {
                            last_err = Some(e);
                            if attempt < 15 {
                                thread::sleep(Duration::from_millis(250));
                            }
                        }
                    }
                }
                Err(e) => {
                    last_err = Some(e);
                    if attempt < 15 {
                        thread::sleep(Duration::from_millis(250));
                    }
                }
            }
        }
        Err(AegisError::Database(format!(
            "Failed to open SQLite connection at {}: {:?}",
            self.db_path.display(),
            last_err
        )))
    }

    /// Execute a closure with a pooled SQLite WAL or `:memory:` connection (`R2`).
    pub fn with_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        if let Some(mem_arc) = &self.memory_conn {
            let conn = mem_arc.lock();
            return f(&conn);
        }

        let conn = {
            let mut pool = self.conn_pool.lock();
            pool.pop()
        };
        let conn = match conn {
            Some(c) => c,
            None => self.open_configured_file_conn()?,
        };

        let result = f(&conn);

        if conn.is_autocommit() {
            let mut pool = self.conn_pool.lock();
            if pool.len() < 8 {
                pool.push(conn);
            }
        }
        result
    }

    pub(crate) fn resolve_cached_fernet_state(&self) -> Result<CachedFernetState> {
        let current_env = std::env::var("AEGIS_MASTER_KEY")
            .ok()
            .filter(|s| is_usable_env_key(s))
            .or_else(|| {
                std::env::var("FERNET_MASTER_KEY")
                    .ok()
                    .filter(|s| is_usable_env_key(s))
            })
            .map(|s| s.trim().to_string());

        if let Some(cached) = self.cipher_cache.read().clone() {
            if cached.env_override == current_env {
                return Ok(cached);
            }
        }

        let explicit = self.explicit_master_key.lock().clone();
        let eff_db_path = if self.is_memory {
            None
        } else {
            Some(self.db_path.as_path())
        };
        let (key_b64, is_explicit) = resolve_fernet_master_key(explicit.as_deref(), eff_db_path);
        if is_legacy_public_fallback_key(&key_b64) {
            return Err(AegisError::SecurityViolation(
                "FATAL SECURITY VIOLATION: Retired public hardcoded fallback master key detected. Rotate or provision a CSPRNG .aegis_master_key."
                    .to_string(),
            ));
        }
        let cipher = FernetCipher::from_key_str(&key_b64)?;
        let state = CachedFernetState {
            key_b64,
            is_explicit,
            env_override: current_env,
            cipher,
        };
        *self.cipher_cache.write() = Some(state.clone());
        Ok(state)
    }

    /// Retrieve or derive the Fernet encryption master key, enforcing mandatory explicit key
    /// presence (`S1`, `R5`).
    pub fn get_master_fernet_key(&self) -> Result<String> {
        let cfg = self.load_config_opts(false)?;
        let exec_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let enforce_key = cfg.get_bool("enforce_master_key", true);

        let state = self.resolve_cached_fernet_state()?;
        if !self.is_memory
            && (enforce_key || exec_mode == "LIVE" || exec_mode == "TESTNET")
            && !state.is_explicit
        {
            let msg = format!(
                "FATAL SECURITY VIOLATION: Execution mode is '{exec_mode}' (enforce_master_key={enforce_key}) \
                 but no explicit AEGIS_MASTER_KEY environment variable, systemd credential, or .aegis_master_key \
                 keyfile was provided."
            );
            let _ = self.log_telemetry("CRITICAL", "database", &msg, None);
            return Err(AegisError::SecurityViolation(msg));
        }
        Ok(state.key_b64)
    }

    pub(crate) fn get_fernet(&self) -> Result<FernetCipher> {
        let _ = self.get_master_fernet_key()?;
        Ok(self.resolve_cached_fernet_state()?.cipher)
    }

    pub fn encrypt_config_value(&self, plaintext: &str) -> Result<String> {
        let val = plaintext.trim();
        if val.is_empty() {
            return Ok(String::new());
        }
        let cipher = self.get_fernet()?;
        let raw_token = val.strip_prefix("ENC:").unwrap_or(val);
        if raw_token.starts_with("gAAAAAB") && cipher.decrypt(raw_token).is_ok() {
            return Ok(raw_token.to_string());
        }
        cipher.encrypt(val.as_bytes())
    }

    /// Decrypt a Fernet-encrypted `system_config` value without any silent fallback-key downgrade (`S2`).
    pub fn decrypt_config_value(&self, raw_value: &str) -> Result<String> {
        let s = raw_value.trim();
        if s.is_empty() {
            return Ok(String::new());
        }
        let token = s.strip_prefix("ENC:").unwrap_or(s);
        if !token.starts_with("gAAAAAB") {
            return Ok(s.to_string());
        }
        let cipher = self.get_fernet()?;
        cipher.decrypt(token)
    }

    pub fn try_decrypt_or_empty(&self, raw_value: &str) -> String {
        let s = raw_value.trim();
        if s.is_empty() {
            return String::new();
        }
        if !is_encrypted_value(s) {
            return s.to_string();
        }
        self.decrypt_config_value(s).unwrap_or_default()
    }

    /// Return the decrypted wallet `private_key` wrapped in `Zeroizing<String>` so secret bytes
    /// are wiped from memory on drop (`S10`).
    pub fn get_signing_private_key(&self) -> Result<Zeroizing<String>> {
        let raw = self.get_raw_config_value("private_key")?.unwrap_or_default();
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(Zeroizing::new(String::new()));
        }
        let plain = self.decrypt_config_value(trimmed)?;
        Ok(Zeroizing::new(plain))
    }

    pub fn rotate_master_key(&self, new_master_key: &str) -> Result<Value> {
        self.rotate_master_key_with_old(None, new_master_key)
    }

    /// Atomically rotate the Fernet master key across all sensitive `system_config` rows,
    /// honouring an optional explicit `old_master_key`, verifying round-trip decryption before
    /// committing, and updating `<db_dir>/.aegis_master_key` (`S5`).
    pub fn rotate_master_key_with_old(
        &self,
        old_master_key: Option<&str>,
        new_master_key: &str,
    ) -> Result<Value> {
        let new_trimmed = new_master_key.trim();
        if new_trimmed.is_empty() {
            return Err(AegisError::Validation(
                "New master key cannot be empty.".to_string(),
            ));
        }
        if is_legacy_public_fallback_key(new_trimmed) {
            return Err(AegisError::SecurityViolation(
                "Refusing to rotate to retired public fallback master key.".to_string(),
            ));
        }
        let new_b64 = normalize_fernet_key(new_trimmed.as_bytes());
        let new_cipher = FernetCipher::from_key_str(&new_b64)?;

        let old_cipher = match old_master_key.map(str::trim).filter(|s| !s.is_empty()) {
            Some(old_raw) => {
                let old_b64 = normalize_fernet_key(old_raw.as_bytes());
                FernetCipher::from_key_str(&old_b64)?
            }
            None => self.resolve_cached_fernet_state()?.cipher,
        };

        let sensitive: HashSet<&str> = SENSITIVE_CONFIG_KEYS.iter().copied().collect();

        let rows: Vec<(String, String)> = self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT config_key, config_value FROM system_config;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                    ))
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })?;

        let mut updates: Vec<(String, String)> = Vec::new();
        for (k, raw_v) in rows {
            let trimmed = raw_v.trim();
            if !sensitive.contains(k.as_str()) || trimmed.is_empty() {
                continue;
            }
            let plain = Zeroizing::new(if is_encrypted_value(trimmed) {
                let token = trimmed.strip_prefix("ENC:").unwrap_or(trimmed);
                old_cipher.decrypt(token).map_err(|e| {
                    AegisError::Crypto(format!(
                        "Failed to decrypt sensitive key '{k}' with old master key during rotation: {e}"
                    ))
                })?
            } else {
                trimmed.to_string()
            });
            if plain.is_empty() {
                continue;
            }
            let enc_v = new_cipher.encrypt(plain.as_bytes())?;
            let verified = Zeroizing::new(new_cipher.decrypt(&enc_v)?);
            if *verified != *plain {
                return Err(AegisError::Crypto(format!(
                    "Round-trip verification failed for rotated key '{k}'"
                )));
            }
            updates.push((k, enc_v));
        }

        self.with_conn(|conn| {
            let tx = conn
                .unchecked_transaction()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            for (k, enc_v) in &updates {
                tx.execute(
                    "UPDATE system_config
                     SET config_value = ?1,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                     WHERE config_key = ?2;",
                    params![enc_v, k],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            }
            tx.commit()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })?;

        let mut re_encrypted: Vec<String> = updates.into_iter().map(|(k, _)| k).collect();
        re_encrypted.sort();

        if !self.is_memory {
            let key_path = master_key_file_for_db(&self.db_path);
            write_master_key_file(&key_path, &new_b64)?;
        }

        *self.explicit_master_key.lock() = Some(new_b64);
        *self.cipher_cache.write() = None;

        Ok(json!({
            "status": "ROTATED",
            "re_encrypted_keys": re_encrypted,
            "rotated_count": re_encrypted.len(),
        }))
    }

    /// Verify Fernet AES-128-CBC + HMAC-SHA256 encryption at rest using an isolated `:memory:`
    /// database for write/read round-trips so live credentials in `self` are never mutated (`S9`).
    pub fn test_encryption(&self) -> Result<Value> {
        let active_key = self.get_master_fernet_key()?;
        let cipher = self.get_fernet()?;

        // 1. Read-only verification of any existing sensitive keys on `self`
        for &key in SENSITIVE_CONFIG_KEYS {
            if let Some(raw_val) = self.get_raw_config_value(key)? {
                let trimmed = raw_val.trim();
                if !trimmed.is_empty() {
                    if !is_encrypted_value(trimmed) {
                        return Err(AegisError::Crypto(format!(
                            "Sensitive key '{key}' is stored unencrypted on disk."
                        )));
                    }
                    let token = trimmed.strip_prefix("ENC:").unwrap_or(trimmed);
                    let _ = Zeroizing::new(cipher.decrypt(token)?);
                }
            }
        }

        // 2. Isolated `:memory:` write/read round-trip test using the active key
        let iso_db = Self::with_paths(":memory:", &self.schema_path, Some(active_key));
        iso_db.initialize_schema(false)?;

        let sample_priv = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let sample_gemini = "AIzaSyAegisQuantFernetVerifiedKey030rc1";

        iso_db.set_config_value("private_key", &json!(sample_priv))?;
        iso_db.set_config_value("gemini_api_key", &json!(sample_gemini))?;

        let raw_priv_on_disk = iso_db.get_raw_config_value("private_key")?.unwrap_or_default();
        let raw_gemini_on_disk = iso_db
            .get_raw_config_value("gemini_api_key")?
            .unwrap_or_default();

        let priv_enc_ok = is_encrypted_value(&raw_priv_on_disk) && raw_priv_on_disk != sample_priv;
        let gemini_enc_ok =
            is_encrypted_value(&raw_gemini_on_disk) && raw_gemini_on_disk != sample_gemini;

        let dec_cfg = iso_db.load_config()?;
        let dec_priv = iso_db.get_signing_private_key()?;
        let priv_dec_ok =
            *dec_priv == sample_priv && dec_cfg.get_str("private_key", "") != sample_priv;
        let gemini_dec_ok = dec_cfg.get_str("gemini_api_key", "") == sample_gemini;

        if !(priv_enc_ok && gemini_enc_ok && priv_dec_ok && gemini_dec_ok) {
            return Err(AegisError::Crypto(
                "Fernet AES-128 configuration encryption verification failed.".to_string(),
            ));
        }

        let sample_prefix: String = raw_gemini_on_disk.chars().take(24).collect();
        let report = json!({
            "status": "VERIFIED",
            "cipher": "AES-128-CBC + HMAC-SHA256 (Fernet)",
            "encrypted_keys_verified": ["private_key", "gemini_api_key"],
            "ciphertext_at_rest_sample": format!("{sample_prefix}..."),
            "is_fernet_token": true,
            "unencrypted_on_disk": false,
            "transparent_decryption_verified": true,
            "isolated_self_test": true,
            "schema_version": self.get_schema_version()?,
        });
        let _ = self.log_telemetry(
            "INFO",
            "engine.database",
            "Verified Fernet AES-128 configuration encryption at rest (isolated self-test)",
            Some(report.clone()),
        );
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const VALID_WALLET: &str = "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23";
    const VALID_PK: &str = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";

    #[test]
    fn test_execution_routing_mode_consistency_and_transitions() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        // 1. Require credentials when enabling TESTNET or LIVE with require_credentials_for_live=true
        let err_missing = db.configure_hyperliquid_credentials(
            None,
            None,
            Some(true),
            Some("TESTNET"),
            None,
            None,
            true,
        );
        assert!(err_missing.is_err());

        // 2. Reject conflicting execution_mode='TESTNET' + hyperliquid_testnet=false
        let err_conflict = db.configure_hyperliquid_credentials(
            Some(VALID_WALLET),
            Some(VALID_PK),
            Some(false),
            Some("TESTNET"),
            None,
            None,
            true,
        );
        assert!(err_conflict.is_err());

        // 3. Configure TESTNET -> execution_mode must remain "TESTNET" (not overwritten to "LIVE")
        let st_testnet = db
            .configure_hyperliquid_credentials(
                Some(VALID_WALLET),
                Some(VALID_PK),
                Some(true),
                Some("TESTNET"),
                None,
                None,
                true,
            )
            .unwrap();
        assert_eq!(st_testnet["execution_mode"], "TESTNET");
        assert_eq!(st_testnet["hyperliquid_testnet"], true);
        assert_eq!(st_testnet["network"], "testnet");
        assert_eq!(
            st_testnet["api_endpoint"],
            "https://api.hyperliquid-testnet.xyz"
        );
        assert_eq!(st_testnet["active_routing"], "LIVE_TESTNET");
        assert_eq!(st_testnet["environment"], "sandbox");

        let cfg = db.load_config().unwrap();
        assert_eq!(cfg.get_str("execution_mode", ""), "TESTNET");
        assert!(cfg.get_bool("hyperliquid_testnet", false));
        assert_eq!(cfg.get_str("pond_execution_mode", ""), "TESTNET");
        assert_eq!(cfg.get_str("environment", ""), "sandbox");

        // 4. Transition to LIVE (Mainnet) via set_config_value("execution_mode", "LIVE")
        db.set_config_value("execution_mode", &json!("LIVE")).unwrap();
        let st_live = db.get_credentials_status().unwrap();
        assert_eq!(st_live["execution_mode"], "LIVE");
        assert_eq!(st_live["hyperliquid_testnet"], false);
        assert_eq!(st_live["network"], "mainnet");
        assert_eq!(st_live["api_endpoint"], "https://api.hyperliquid.xyz");
        assert_eq!(st_live["active_routing"], "LIVE_MAINNET");
        assert_eq!(st_live["environment"], "production");

        let cfg_live = db.load_config().unwrap();
        assert_eq!(cfg_live.get_str("execution_mode", ""), "LIVE");
        assert!(!cfg_live.get_bool("hyperliquid_testnet", true));
        assert_eq!(cfg_live.get_str("pond_execution_mode", ""), "LIVE");
        assert_eq!(cfg_live.get_str("environment", ""), "production");

        // 5. Toggle hyperliquid_testnet=true while in LIVE -> automatically reconciles to TESTNET
        db.set_config_value("hyperliquid_testnet", &json!(true))
            .unwrap();
        let st_re_testnet = db.get_credentials_status().unwrap();
        assert_eq!(st_re_testnet["execution_mode"], "TESTNET");
        assert_eq!(st_re_testnet["hyperliquid_testnet"], true);
        assert_eq!(st_re_testnet["active_routing"], "LIVE_TESTNET");
        assert_eq!(st_re_testnet["environment"], "sandbox");

        // 6. Transition back to PAPER
        db.set_config_value("execution_mode", &json!("PAPER"))
            .unwrap();
        let st_paper = db.get_credentials_status().unwrap();
        assert_eq!(st_paper["execution_mode"], "PAPER");
        assert_eq!(st_paper["active_routing"], "PAPER_SIMULATION");
        assert_eq!(st_paper["environment"], "sandbox");
        let cfg_paper = db.load_config().unwrap();
        assert_eq!(cfg_paper.get_str("pond_execution_mode", ""), "PAPER");
    }

    #[test]
    fn test_reconcile_legacy_contradictory_live_testnet_config() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        db.configure_hyperliquid_credentials(
            Some(VALID_WALLET),
            Some(VALID_PK),
            Some(true),
            Some("PAPER"),
            None,
            None,
            false,
        )
        .unwrap();

        // Simulate legacy contradictory DB state: execution_mode='LIVE', hyperliquid_testnet='true', pond_execution_mode='PAPER'
        db.with_conn(|conn| {
            conn.execute(
                "UPDATE system_config SET config_value = 'LIVE' WHERE config_key = 'execution_mode';",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE system_config SET config_value = 'true' WHERE config_key = 'hyperliquid_testnet';",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE system_config SET config_value = 'PAPER' WHERE config_key = 'pond_execution_mode';",
                [],
            )
            .unwrap();
            Ok(())
        })
        .unwrap();

        let creds = db.get_credentials_status().unwrap();
        assert_eq!(creds["execution_mode"], "TESTNET");
        assert_eq!(creds["hyperliquid_testnet"], true);
        assert_eq!(creds["active_routing"], "LIVE_TESTNET");
        assert_eq!(creds["api_endpoint"], "https://api.hyperliquid-testnet.xyz");

        let rows = db.get_config_rows(true).unwrap();
        let exec_row = rows
            .iter()
            .find(|r| r["config_key"] == "execution_mode")
            .unwrap();
        let pond_row = rows
            .iter()
            .find(|r| r["config_key"] == "pond_execution_mode")
            .unwrap();
        assert_eq!(exec_row["config_value"], "TESTNET");
        assert_eq!(pond_row["config_value"], "TESTNET");
        assert!(exec_row["description"]
            .as_str()
            .unwrap()
            .contains("TESTNET"));
        assert!(pond_row["description"].as_str().unwrap().contains("LIVE"));
    }

    #[test]
    fn test_pond_execution_mode_and_environment_synchronization() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        db.configure_hyperliquid_credentials(
            Some(VALID_WALLET),
            Some(VALID_PK),
            Some(true),
            Some("PAPER"),
            None,
            None,
            false,
        )
        .unwrap();

        db.set_config_value("pond_execution_mode", &json!("TESTNET"))
            .unwrap();
        let st_tn = db.get_credentials_status().unwrap();
        assert_eq!(st_tn["execution_mode"], "TESTNET");
        assert_eq!(st_tn["pond_execution_mode"], "TESTNET");
        assert_eq!(st_tn["hyperliquid_testnet"], true);
        assert_eq!(st_tn["active_routing"], "LIVE_TESTNET");
        assert_eq!(st_tn["environment"], "sandbox");

        assert!(db
            .set_config_value("environment", &json!("production"))
            .is_err());
        assert!(db
            .set_config_value("environment", &json!("sandbox"))
            .is_ok());

        db.set_config_value("pond_execution_mode", &json!("LIVE"))
            .unwrap();
        let st_live = db.get_credentials_status().unwrap();
        assert_eq!(st_live["execution_mode"], "LIVE");
        assert_eq!(st_live["pond_execution_mode"], "LIVE");
        assert_eq!(st_live["hyperliquid_testnet"], false);
        assert_eq!(st_live["active_routing"], "LIVE_MAINNET");
        assert_eq!(st_live["environment"], "production");

        assert!(db
            .set_config_value("environment", &json!("sandbox"))
            .is_err());

        db.set_config_value("pond_execution_mode", &json!("PAPER"))
            .unwrap();
        let st_paper = db.get_credentials_status().unwrap();
        assert_eq!(st_paper["execution_mode"], "PAPER");
        assert_eq!(st_paper["pond_execution_mode"], "PAPER");
        assert_eq!(st_paper["active_routing"], "PAPER_SIMULATION");
        assert_eq!(st_paper["environment"], "sandbox");
    }

    #[test]
    fn test_reconcile_out_of_sync_pond_and_environment_on_load_config() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        db.with_conn(|conn| {
            conn.execute(
                "UPDATE system_config SET config_value = 'TESTNET' WHERE config_key = 'execution_mode';",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE system_config SET config_value = 'true' WHERE config_key = 'hyperliquid_testnet';",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE system_config SET config_value = 'PAPER' WHERE config_key = 'pond_execution_mode';",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE system_config SET config_value = 'production' WHERE config_key = 'environment';",
                [],
            )
            .unwrap();
            Ok(())
        })
        .unwrap();

        let cfg_tn = db.load_config().unwrap();
        assert_eq!(cfg_tn.get_str("execution_mode", ""), "TESTNET");
        assert_eq!(cfg_tn.get_str("pond_execution_mode", ""), "TESTNET");
        assert!(cfg_tn.get_bool("hyperliquid_testnet", false));
        assert_eq!(cfg_tn.get_str("environment", ""), "sandbox");

        db.with_conn(|conn| {
            conn.execute(
                "UPDATE system_config SET config_value = 'LIVE' WHERE config_key = 'execution_mode';",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE system_config SET config_value = 'false' WHERE config_key = 'hyperliquid_testnet';",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE system_config SET config_value = 'PAPER' WHERE config_key = 'pond_execution_mode';",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE system_config SET config_value = 'sandbox' WHERE config_key = 'environment';",
                [],
            )
            .unwrap();
            Ok(())
        })
        .unwrap();

        let cfg_live = db.load_config().unwrap();
        assert_eq!(cfg_live.get_str("execution_mode", ""), "LIVE");
        assert_eq!(cfg_live.get_str("pond_execution_mode", ""), "LIVE");
        assert!(!cfg_live.get_bool("hyperliquid_testnet", true));
        assert_eq!(cfg_live.get_str("environment", ""), "production");
    }

    #[test]
    fn test_set_config_batch_deterministic_routing_and_credentials() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        let mut batch = HashMap::new();
        batch.insert("execution_mode".to_string(), json!("TESTNET"));
        batch.insert("hyperliquid_testnet".to_string(), json!(true));
        batch.insert("wallet_address".to_string(), json!(VALID_WALLET));
        batch.insert("private_key".to_string(), json!(VALID_PK));
        batch.insert("environment".to_string(), json!("sandbox"));

        let updated = db.set_config_batch(&batch, false).unwrap();
        assert_eq!(updated.len(), 5);

        let st = db.get_credentials_status().unwrap();
        assert_eq!(st["execution_mode"], "TESTNET");
        assert_eq!(st["pond_execution_mode"], "TESTNET");
        assert_eq!(st["hyperliquid_testnet"], true);
        assert_eq!(st["active_routing"], "LIVE_TESTNET");

        let mut to_paper = HashMap::new();
        to_paper.insert("execution_mode".to_string(), json!("PAPER"));
        to_paper.insert("hyperliquid_testnet".to_string(), json!(false));
        db.set_config_batch(&to_paper, false).unwrap();

        let st_paper = db.get_credentials_status().unwrap();
        assert_eq!(st_paper["execution_mode"], "PAPER");
        assert_eq!(st_paper["pond_execution_mode"], "PAPER");
        assert_eq!(st_paper["hyperliquid_testnet"], false);
        assert_eq!(st_paper["active_routing"], "PAPER_SIMULATION");
        assert_eq!(st_paper["environment"], "sandbox");
    }

    #[test]
    fn test_isolated_encryption_self_test_never_mutates_live_credentials() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        // Ensure private_key is empty initially
        assert_eq!(
            db.get_raw_config_value("private_key").unwrap().unwrap_or_default(),
            ""
        );

        let rep = db.test_encryption().unwrap();
        assert_eq!(rep["status"], "VERIFIED");
        assert_eq!(rep["isolated_self_test"], true);

        // Live DB must still have empty private_key and gemini_api_key
        assert_eq!(
            db.get_raw_config_value("private_key").unwrap().unwrap_or_default(),
            ""
        );
        assert_eq!(
            db.get_raw_config_value("gemini_api_key")
                .unwrap()
                .unwrap_or_default(),
            ""
        );
    }

    #[test]
    fn test_rotate_master_key_with_old_and_no_silent_fallback_downgrade() {
        let key1 = crate::fernet::generate_fernet_key();
        let key2 = crate::fernet::generate_fernet_key();
        let wrong_key = crate::fernet::generate_fernet_key();

        let db = DatabaseManager::with_paths(
            ":memory:",
            resolve_project_root().join("sql").join("schema_and_seed.sql"),
            Some(key1.clone()),
        );
        db.initialize_schema(true).unwrap();

        db.set_config_value("private_key", &json!(VALID_PK)).unwrap();
        let raw_before = db.get_raw_config_value("private_key").unwrap().unwrap();
        assert!(is_encrypted_value(&raw_before));

        // Rotating with wrong old_master_key must fail without corrupting data
        assert!(db
            .rotate_master_key_with_old(Some(&wrong_key), &key2)
            .is_err());

        // Rotating with correct old_master_key succeeds and re-encrypts
        let rot = db
            .rotate_master_key_with_old(Some(&key1), &key2)
            .unwrap();
        assert_eq!(rot["status"], "ROTATED");
        assert_eq!(rot["rotated_count"], 1);

        let dec = db.get_signing_private_key().unwrap();
        assert_eq!(&*dec, VALID_PK);

        // Old ciphertext cannot be decrypted under new key (no silent fallback)
        assert!(db.decrypt_config_value(&raw_before).is_err());
    }

    #[test]
    fn test_telemetry_redacts_sensitive_keys_and_net_book_transfers() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        db.log_telemetry(
            "INFO",
            "web.config",
            "Updated system_config (1 keys): private_key=0xdeadbeef",
            Some(json!({"private_key": "0xdeadbeef", "safe_key": "ok"})),
        )
        .unwrap();

        let logs = db.get_recent_logs(5).unwrap();
        let msg = logs[0]["message"].as_str().unwrap();
        let details = logs[0]["details_json"].as_str().unwrap();
        assert!(!msg.contains("0xdeadbeef"));
        assert!(msg.contains("[REDACTED]"));
        assert!(!details.contains("0xdeadbeef"));
        assert!(details.contains("[REDACTED]"));
    }

    #[test]
    fn test_monotonic_nonce_settle_lock_ttl_and_order_fsm_validation() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        // 1. NH-11: Strictly monotonic nonces across consecutive rapid calls
        let mut prev = 0u64;
        for _ in 0..20 {
            let next = db.next_hyperliquid_nonce().unwrap();
            assert!(next > prev);
            prev = next;
        }

        // 2. NH-10 / NM-3: Atomic settle lock mutual exclusion + TTL auto-expiry
        db.set_pit_timestamp_ms(Some(1_000_000));
        db.try_acquire_settle_lock("settle-worker-1").unwrap();
        assert!(db.is_settle_gate_active().unwrap());
        assert!(db.try_acquire_settle_lock("settle-worker-2").is_err());

        // Advance time beyond 300,000ms TTL -> lock auto-expires
        db.set_pit_timestamp_ms(Some(1_350_000));
        assert!(!db.is_settle_gate_active().unwrap());
        db.try_acquire_settle_lock("settle-worker-2").unwrap();
        db.release_settle_lock().unwrap();
        assert!(!db.is_settle_gate_active().unwrap());
        db.set_pit_timestamp_ms(None);

        // 3. NM-14: transition_order_state validates FSM transitions & insert_trade is idempotent on client_order_id
        db.transition_order_state(
            "cli-fsm-test-1",
            "OPEN_ON_BOOK",
            Some("PENDING"),
            Some("POSTED"),
            Some("oid-101"),
            None,
            Some(0.0),
            None,
            None,
            None,
        )
        .unwrap();
        db.transition_order_state(
            "cli-fsm-test-1",
            "FILLED",
            Some("OPEN_ON_BOOK"),
            Some("MATCHED"),
            Some("oid-101"),
            Some(175.0),
            Some(2.0),
            Some(0.0),
            Some(0.05),
            Some("OPEN"),
        )
        .unwrap();
        // Terminal FILLED -> OPEN_ON_BOOK or CANCELLED must fail
        assert!(db
            .transition_order_state(
                "cli-fsm-test-1",
                "OPEN_ON_BOOK",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .is_err());

        // 4. NH-12: Signer address derivation & mismatch rejection
        let derived_addr = aegis_core::derive_evm_address_from_private_key(VALID_PK).unwrap();
        let mismatched_wallet = "0xef377c1d5444786236aa34fa72e5879372e0eaec";
        assert_eq!(derived_addr, VALID_WALLET);
        assert!(aegis_core::verify_signer_matches_wallet(VALID_PK, &derived_addr, None).is_ok());
        assert!(aegis_core::verify_signer_matches_wallet(VALID_PK, mismatched_wallet, None).is_err());
        assert!(aegis_core::verify_signer_matches_wallet(
            VALID_PK,
            mismatched_wallet,
            Some(&derived_addr)
        )
        .is_ok());
    }

    #[test]
    fn test_nh5_canonical_iso_timestamps_and_same_day_transfer_equity() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        let ts = aegis_core::now_iso8601_utc();
        assert!(ts.contains('T') && ts.ends_with('Z'));

        db.insert_book_transfer(&aegis_core::BookTransfer {
            transfer_id: 0,
            settle_date: "2026-11-06".to_string(),
            from_book: "PIT".to_string(),
            to_book: "POND".to_string(),
            amount_usdc: 250.0,
            pre_from_usdc: Some(5250.0),
            pre_to_usdc: Some(4750.0),
            post_from_usdc: Some(5000.0),
            post_to_usdc: Some(5000.0),
            drift_bps_before: Some(250.0),
            drift_bps_after: Some(0.0),
            blocked_remainder_usdc: 0.0,
            status: "COMPLETED".to_string(),
            attempts: 1,
            tx_ref: Some("VIRTUAL_SINGLE_WALLET_LEDGER".to_string()),
            created_at: ts,
        })
        .unwrap();

        let pit_adj = db.get_net_book_transfer_adjustment("PIT").unwrap();
        let pond_adj = db.get_net_book_transfer_adjustment("POND").unwrap();
        assert!((pit_adj - (-250.0)).abs() < 1e-6);
        assert!((pond_adj - 250.0).abs() < 1e-6);
    }

    #[test]
    fn test_nh11_nh12_monotonic_db_nonce_and_signer_validation() {
        test_monotonic_nonce_settle_lock_ttl_and_order_fsm_validation();
    }

    #[test]
    fn test_r3_b1_h1_h2_h3_atomic_pond_cell_mutations_and_state_guards() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        let mut cell = aegis_core::PondCell {
            cell_id: 0,
            symbol: "SOL/USDC:USDC".to_string(),
            champion_id: 1,
            state: "OPEN".to_string(),
            cycle_opened_id: None,
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(150.0),
            quantity: Some(2.0),
            notional_usdc: Some(300.0),
            risk_budget_usdc: Some(30.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.8),
            funding_rate_entry: Some(0.0001),
            initial_stop: Some(160.0),
            chandelier_stop: Some(162.0),
            highest_price_seen: Some(150.0),
            lowest_price_seen: Some(150.0),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.10,
            exit_price: None,
            exit_reason: None,
            opened_at: Some("2026-11-08T00:00:00Z".to_string()),
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-11-08T00:00:00Z".to_string(),
        };
        let cid = db.upsert_pond_cell(&cell).unwrap();
        cell.cell_id = cid;

        // R3-B1: Funding accrual is idempotent within the same UTC hour bucket
        let first_accrual = db
            .accrue_pond_cell_funding_for_hour(cid, 498_000, -0.05)
            .unwrap();
        assert!(first_accrual);
        let duplicate_accrual = db
            .accrue_pond_cell_funding_for_hour(cid, 498_000, -0.05)
            .unwrap();
        assert!(!duplicate_accrual);
        let next_hour_accrual = db
            .accrue_pond_cell_funding_for_hour(cid, 498_001, -0.05)
            .unwrap();
        assert!(next_hour_accrual);

        // R3-H1: Targeted watermark update preserves accrued funding (-0.10)
        db.update_open_pond_cell_watermarks(
            cid,
            "TRAILED",
            Some(145.0),
            Some(160.0),
            Some(150.0),
        )
        .unwrap();
        let active = db.get_open_pond_cells().unwrap();
        assert_eq!(active.len(), 1);
        assert!((active[0].funding_accrued_usdc - (-0.10)).abs() < 1e-9);
        assert_eq!(active[0].state, "TRAILED");

        // R3-H1: Close open pond cell atomically
        let closed = db
            .close_open_pond_cell(
                cid,
                "COOLDOWN",
                Some(145.0),
                Some(160.0),
                Some(140.0),
                140.0,
                20.0,
                0.05,
                Some("TIER3_FUNDING_NORMALISATION"),
                "2026-11-08T02:00:00Z",
                "2026-11-09T02:00:00Z",
            )
            .unwrap();
        assert!(closed);

        // R3-H1 & R3-H3: Stale upsert or reconcile_open_pond_cell_size cannot reopen a CLOSED/COOLDOWN cell
        db.upsert_pond_cell(&cell).unwrap();
        assert!(db.get_open_pond_cells().unwrap().is_empty());
        let reconciled = db
            .reconcile_open_pond_cell_size(cid, 2.0, Some(150.0))
            .unwrap();
        assert!(!reconciled);
    }

    #[test]
    fn test_r3_b2_b5_h13_h15_h21_security_and_validation_guards() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        // R3-B2 & R3-B5: Multi-secret telemetry message & embedded Telegram bot URL redaction
        db.log_telemetry(
            "ERROR",
            "alerts",
            "Failed POST https://api.telegram.org/bot123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11/sendMessage private_key=0x1111 gemini_api_key=AIzaSecret999",
            Some(json!({
                "error": "url=https://discord.com/api/webhooks/12345/secretToken999",
                "telegram_bot_token": "123456:ABC-DEF"
            })),
        )
        .unwrap();
        let logs = db.get_recent_logs(1).unwrap();
        let msg = logs[0]["message"].as_str().unwrap();
        let details = logs[0]["details_json"].as_str().unwrap();
        assert!(!msg.contains("ABC-DEF1234ghIkl"));
        assert!(!msg.contains("0x1111"));
        assert!(!msg.contains("AIzaSecret999"));
        assert!(!details.contains("secretToken999"));
        assert!(!details.contains("123456:ABC-DEF"));

        // R3-H13: Reject inverted min > max risk bounds
        assert!(db
            .set_config_value("min_risk_per_trade_usdc", &json!(9999.0))
            .is_err());
        let mut bad_batch = HashMap::new();
        bad_batch.insert("pond_min_risk_usdc".to_string(), json!(500.0));
        bad_batch.insert("pond_max_risk_usdc".to_string(), json!(100.0));
        assert!(db.set_config_batch(&bad_batch, false).is_err());

        // R3-H21: Unconditional masking in get_config_rows(false) and masked echo protection
        db.set_config_value("gemini_api_key", &json!("AIzaRealSecretKey123456"))
            .unwrap();
        let rows_unmasked_flag = db.get_config_rows(false).unwrap();
        let gk_row = rows_unmasked_flag
            .iter()
            .find(|r| r["config_key"] == "gemini_api_key")
            .unwrap();
        let masked_echo = gk_row["config_value"].as_str().unwrap().to_string();
        assert!(masked_echo.contains("..."));
        assert!(!masked_echo.contains("RealSecretKey"));

        // Saving masked echo back must preserve the real secret in DB
        db.set_config_value("gemini_api_key", &json!(masked_echo))
            .unwrap();
        let cfg = db.load_config().unwrap();
        assert_eq!(cfg.get_str("gemini_api_key", ""), "AIzaRealSecretKey123456");

        // R3-H15: resolve_reference_price returns Ok(None) when no market data exists
        assert_eq!(db.resolve_reference_price("UNKNOWN/USDC:USDC").unwrap(), None);
    }
}

