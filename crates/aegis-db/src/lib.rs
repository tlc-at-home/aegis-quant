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

/// RAII guard that recycles an acquired SQLite connection back to `conn_pool` upon drop,
/// ensuring panic safety during unwinding (`R6-N3`).
struct PooledConnGuard<'a> {
    pool: &'a Arc<Mutex<Vec<Connection>>>,
    conn: Option<Connection>,
}

impl<'a> Drop for PooledConnGuard<'a> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // If the connection was left inside an active transaction due to panic or error,
            // attempt a clean rollback before recycling.
            if !conn.is_autocommit() {
                let _ = conn.execute_batch("ROLLBACK;");
            }
            if conn.is_autocommit() {
                let mut pool = self.pool.lock();
                if pool.len() < 8 {
                    pool.push(conn);
                }
            }
        }
    }
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

    /// Execute a closure with a pooled SQLite WAL or `:memory:` connection (`R2`, `R6-N3`, `R7-QA`).
    ///
    /// Invariants:
    /// - When invoked along the `load_config()` / `reconcile_routing_config()` read path, closures
    ///   must never mutate any table other than `system_config` (`R7-B1`, `R7-QA`).
    /// - Do not nest `with_conn` or `with_immediate_tx` inside an active `with_conn` closure.
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

        let guard = PooledConnGuard {
            pool: &self.conn_pool,
            conn: Some(conn),
        };

        f(guard.conn.as_ref().expect("conn present in PooledConnGuard"))
    }

    /// Execute a closure inside a `BEGIN IMMEDIATE;` write transaction with automatic rollback and recycling (`R6-N3`).
    ///
    /// Invariant: Acquires its own pooled connection (or `:memory:` mutex), so callers must not
    /// nest `with_immediate_tx` or `with_conn` inside an active `with_conn` / `with_immediate_tx` closure.
    pub fn with_immediate_tx<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN IMMEDIATE;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let res = f(conn);
            match res {
                Ok(val) => {
                    conn.execute_batch("COMMIT;")
                        .map_err(|e| AegisError::Database(e.to_string()))?;
                    Ok(val)
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK;");
                    Err(e)
                }
            }
        })
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

        self.with_immediate_tx(|conn| {
            for (k, enc_v) in &updates {
                conn.execute(
                    "UPDATE system_config
                     SET config_value = ?1,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                     WHERE config_key = ?2;",
                    params![enc_v, k],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            }
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
            resting_order_id: None,
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

    #[test]
    fn test_r4_remediation_db_invariants() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        assert_eq!(db.get_schema_version().unwrap(), 12);

        // R4-N8: get_existing_web_api_token is strictly read-only when unset
        assert_eq!(db.get_existing_web_api_token().unwrap(), None);
        let generated = db.get_or_create_web_api_token().unwrap();
        assert_eq!(db.get_existing_web_api_token().unwrap(), Some(generated));

        // R4-N3: Real secret containing '...' (not matching exact mask shapes) is saved, while exact mask shapes are skipped
        db.set_config_value("gemini_api_key", &json!("my...custom...secret...key"))
            .unwrap();
        assert_eq!(
            db.load_config().unwrap().get_str("gemini_api_key", ""),
            "my...custom...secret...key"
        );
        // 13-char exact mask shape 6 + "..." + 4 is skipped
        db.set_config_value("gemini_api_key", &json!("my...c...key1"))
            .unwrap();
        assert_eq!(
            db.load_config().unwrap().get_str("gemini_api_key", ""),
            "my...custom...secret...key"
        );

        // R3-N13 & R4-M1: Timeframe and tick interval validation at config write boundary
        assert!(db.set_config_value("hmm_timeframe", &json!("invalid_tf")).is_err());
        assert!(db
            .set_config_value("calibration_timeframes_csv", &json!("1h,badtf"))
            .is_err());
        assert!(db
            .set_config_value("pit_tick_interval_seconds", &json!(0))
            .is_err());
        db.set_config_value("pit_tick_interval_seconds", &json!(600))
            .unwrap();

        // R4-N4 & R3-N8: PIT hourly funding accrual idempotency & open trade fee subtraction in compute_pit_equity_and_idle_usdc
        let mut trade = aegis_core::TradeRecord {
            trade_id: 0,
            client_order_id: "cli-r4-pit-1".to_string(),
            symbol: "BTC/USDC:USDC".to_string(),
            base_asset: "BTC".to_string(),
            side: "LONG".to_string(),
            status: "OPEN".to_string(),
            fsm_state: "FILLED".to_string(),
            entry_price: 90_000.0,
            current_price: 90_000.0,
            exit_price: None,
            quantity: 0.1,
            filled_units: 0.1,
            avg_fill_price: Some(90_000.0),
            notional_usdc: 9_000.0,
            risk_budget_usdc: 200.0,
            conviction_multiplier: 1.0,
            initial_stop_price: 88_000.0,
            chandelier_stop_price: 88_000.0,
            current_stop_price: 88_000.0,
            highest_price_seen: 90_000.0,
            lowest_price_seen: 90_000.0,
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc: 50.0,
            fees_paid_usdc: 4.5,
            funding_accrued_usdc: 0.0,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some("sim-pit-1".to_string()),
            opened_at: "2026-11-08T00:00:00Z".to_string(),
            closed_at: None,
            exit_reason: None,
        };
        let tid = db.insert_trade(&trade).unwrap();
        trade.trade_id = tid;

        assert!(db
            .accrue_trade_funding_for_hour(tid, 500_000, 90_500.0, 50.0, -1.25)
            .unwrap());
        // Duplicate hour bucket must be a no-op
        assert!(!db
            .accrue_trade_funding_for_hour(tid, 500_000, 90_500.0, 50.0, -1.25)
            .unwrap());
        // Next hour bucket accrues
        assert!(db
            .accrue_trade_funding_for_hour(tid, 500_001, 90_500.0, 50.0, -0.75)
            .unwrap());
        let open_trades = db.get_open_trades().unwrap();
        assert_eq!(open_trades.len(), 1);
        assert!((open_trades[0].funding_accrued_usdc - (-2.0)).abs() < 1e-9);

        // Check compute_pit_equity_and_idle_usdc: baseline 5000 + upnl 50 + funding (-2.0) - fees (4.5) = 5043.5
        let (pit_eq, _pit_idle) = db.compute_pit_equity_and_idle_usdc().unwrap();
        assert!((pit_eq - 5043.5).abs() < 1e-6);

        // R4-N11: Armed cell resting_order_id persistence and lifecycle cleanup
        let armed_cell = aegis_core::PondCell {
            cell_id: 0,
            symbol: "ETH/USDC:USDC".to_string(),
            champion_id: 1,
            state: "ARMED".to_string(),
            cycle_opened_id: None,
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(3000.0),
            quantity: Some(0.5),
            notional_usdc: Some(1500.0),
            risk_budget_usdc: Some(40.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.5),
            funding_rate_entry: Some(0.0002),
            initial_stop: Some(3100.0),
            chandelier_stop: Some(3120.0),
            highest_price_seen: Some(3000.0),
            lowest_price_seen: Some(3000.0),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            exit_price: None,
            exit_reason: None,
            resting_order_id: Some("987654321".to_string()),
            opened_at: Some("2026-11-08T00:00:00Z".to_string()),
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-11-08T00:00:00Z".to_string(),
        };
        let armed_id = db.upsert_pond_cell(&armed_cell).unwrap();
        let armed_list = db.get_armed_pond_cells().unwrap();
        assert_eq!(armed_list.len(), 1);
        assert_eq!(
            armed_list[0].resting_order_id.as_deref(),
            Some("987654321")
        );
        assert_eq!(armed_list[0].exit_reason, None);
        // R4-N10: update_open_pond_cell_watermarks and reduce_open_pond_cell_partial_exit must return Err without panicking when passed CLOSED
        assert!(db
            .update_open_pond_cell_watermarks(armed_id, "CLOSED", None, None, None)
            .is_err());
        assert!(db
            .reduce_open_pond_cell_partial_exit(
                armed_id, "CLOSED", None, None, None, 0.1, 100.0, 0.0, 0.0
            )
            .is_err());

        // R4-H1 & R4-QB: PARTIAL / AMBIGUOUS book transfers excluded from virtual ledger and surfaced by alert check
        let transfer_id = db
            .insert_book_transfer_with_mode(
                &aegis_core::BookTransfer {
                    transfer_id: 0,
                    settle_date: "2026-11-08T00:05:00Z".to_string(),
                    from_book: "PIT".to_string(),
                    to_book: "POND".to_string(),
                    amount_usdc: 300.0,
                    pre_from_usdc: Some(5300.0),
                    pre_to_usdc: Some(4700.0),
                    post_from_usdc: Some(5300.0),
                    post_to_usdc: Some(4700.0),
                    drift_bps_before: Some(300.0),
                    drift_bps_after: Some(300.0),
                    blocked_remainder_usdc: 0.0,
                    status: "PENDING".to_string(),
                    attempts: 1,
                    tx_ref: None,
                    created_at: "2026-11-08T00:05:00Z".to_string(),
                },
                "LIVE",
            )
            .unwrap();
        db.update_book_transfer_leg_progress(transfer_id, 1, Some("leg1:pit->main:300"), None)
            .unwrap();
        let reconciled_count = db.reconcile_pending_book_transfers().unwrap();
        assert_eq!(reconciled_count, 1);

        // Must NOT be counted in virtual ledger adjustment
        assert!((db.get_net_book_transfer_adjustment("PIT").unwrap()).abs() < 1e-9);
        assert!((db.get_net_book_transfer_adjustment("POND").unwrap()).abs() < 1e-9);

        // Stranded transfer alert fires once and respects cooldown on immediate second check
        let stranded_first = db.check_and_record_stranded_transfer_alert().unwrap();
        assert_eq!(stranded_first.len(), 1);
        assert_eq!(stranded_first[0].status, "PARTIAL");
        let stranded_second = db.check_and_record_stranded_transfer_alert().unwrap();
        assert!(stranded_second.is_empty());

        // R4-H2: Pre-v9 historical dry-run PARTIAL_IDLE_CAPPED row with virtual tx_ref = '0x...' and legs_completed = 0
        // in TESTNET/LIVE mode without subaccount addresses configured is reclassified to DRY_RUN by migration 009
        db.set_config_value("execution_mode", &json!("TESTNET"))
            .unwrap();
        let hist_id = db
            .insert_book_transfer_with_mode(
                &aegis_core::BookTransfer {
                    transfer_id: 0,
                    settle_date: "2026-11-07T00:05:00Z".to_string(),
                    from_book: "PIT".to_string(),
                    to_book: "POND".to_string(),
                    amount_usdc: 250.0,
                    pre_from_usdc: Some(5500.0),
                    pre_to_usdc: Some(4500.0),
                    post_from_usdc: Some(5250.0),
                    post_to_usdc: Some(4750.0),
                    drift_bps_before: Some(500.0),
                    drift_bps_after: Some(250.0),
                    blocked_remainder_usdc: 250.0,
                    status: "PARTIAL_IDLE_CAPPED".to_string(),
                    attempts: 1,
                    tx_ref: Some(
                        "0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
                            .to_string(),
                    ),
                    created_at: "2026-11-07T00:05:00Z".to_string(),
                },
                "LIVE",
            )
            .unwrap();
        db.with_conn(|conn| {
            conn.execute(
                "UPDATE system_config SET config_value = '8' WHERE config_key = 'schema_version';",
                [],
            )
            .unwrap();
            let mig_009 = include_str!("../../../sql/migrations/009_v030_round4_remediation.sql");
            let filtered: String = mig_009
                .lines()
                .filter(|l| !l.contains("ADD COLUMN last_funding_hour_bucket") && !l.contains("ADD COLUMN resting_order_id"))
                .collect::<Vec<_>>()
                .join("\n");
            conn.execute_batch(&filtered).unwrap();
            Ok(())
        })
        .unwrap();
        let rec_hist = db
            .get_recent_book_transfers(10)
            .unwrap()
            .into_iter()
            .find(|r| r.transfer_id == hist_id)
            .unwrap();
        assert_eq!(rec_hist.status, "DRY_RUN");
        assert!((db.get_net_book_transfer_adjustment("PIT").unwrap()).abs() < 1e-9);
    }

    #[test]
    fn test_r5_m1_m2_n2_transactional_migration_crash_recovery_and_paper_dry_run_reclassification() {
        use rusqlite::OptionalExtension;
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        assert_eq!(db.get_schema_version().unwrap(), 12);

        // 1. R5-M1: Transactional migration rollback on mid-script failure preserves existing tables
        db.with_conn(|conn| {
            let failing_script = "
                CREATE TABLE r5_temp_test (id INTEGER PRIMARY KEY);
                INSERT INTO r5_temp_test (id) VALUES (1);
                SELECT * FROM non_existent_table_that_forces_rollback;
            ";
            assert!(DatabaseManager::execute_sql_transactional(conn, failing_script).is_err());
            let table_exists: Option<String> = conn
                .query_row(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='r5_temp_test';",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .unwrap();
            assert!(
                table_exists.is_none(),
                "Transactional migration runner must roll back all DDL/DML on error (R5-M1)"
            );
            Ok(())
        })
        .unwrap();

        // 2. R5-M1: Pre-migration crash recovery for both v9 table-rebuild crash windows
        db.with_conn(|conn| {
            // Crash Window A: both `book_transfers` and `book_transfers_v9` exist
            conn.execute_batch("CREATE TABLE book_transfers_v9 (transfer_id INTEGER PRIMARY KEY);")
                .unwrap();
            DatabaseManager::repair_interrupted_table_rebuilds(conn).unwrap();
            let v9_after_a: Option<String> = conn
                .query_row(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='book_transfers_v9';",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .unwrap();
            assert!(v9_after_a.is_none());

            // Crash Window B: `book_transfers` was dropped before `book_transfers_v9` was renamed
            conn.execute_batch(
                "DROP VIEW IF EXISTS v_book_transfers_recent;
                 ALTER TABLE book_transfers RENAME TO book_transfers_v9;",
            )
            .unwrap();
            DatabaseManager::repair_interrupted_table_rebuilds(conn).unwrap();
            let main_after_b: Option<String> = conn
                .query_row(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='book_transfers';",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .unwrap();
            assert_eq!(main_after_b.as_deref(), Some("book_transfers"));
            Ok(())
        })
        .unwrap();

        // 3. R5-M2 & R5-N2: Round-2-era PAPER-mode `PARTIAL_IDLE_CAPPED` row with plain `0x...` tx_ref
        // is detected by `audit_suspect_dry_run_book_transfers` and reclassified to `DRY_RUN` by migration 010,
        // and open trades with `last_funding_hour_bucket = 0` are backfilled to the current UTC hour bucket.
        db.set_config_value("execution_mode", &json!("PAPER"))
            .unwrap();
        let paper_hist_id = db
            .insert_book_transfer_with_mode(
                &aegis_core::BookTransfer {
                    transfer_id: 0,
                    settle_date: "2026-11-06T00:05:00Z".to_string(),
                    from_book: "PIT".to_string(),
                    to_book: "POND".to_string(),
                    amount_usdc: 325.0,
                    pre_from_usdc: Some(5500.0),
                    pre_to_usdc: Some(4500.0),
                    post_from_usdc: Some(5175.0),
                    post_to_usdc: Some(4825.0),
                    drift_bps_before: Some(500.0),
                    drift_bps_after: Some(175.0),
                    blocked_remainder_usdc: 175.0,
                    status: "PARTIAL_IDLE_CAPPED".to_string(),
                    attempts: 1,
                    tx_ref: Some(
                        "0x111122223333444455556666777788889999aaaabbbbccccddddeeeeffff0000"
                            .to_string(),
                    ),
                    created_at: "2026-11-06T00:05:00Z".to_string(),
                },
                "LIVE",
            )
            .unwrap();
        let suspect_before = db.audit_suspect_dry_run_book_transfers().unwrap();
        assert_eq!(suspect_before.len(), 1);
        assert_eq!(suspect_before[0].transfer_id, paper_hist_id);

        // Also insert an OPEN trade whose `last_funding_hour_bucket` is 0 (simulating a pre-v10 open trade)
        let trade = aegis_core::TradeRecord {
            trade_id: 0,
            client_order_id: "r5-n2-open-trade".to_string(),
            symbol: "BTC/USDC:USDC".to_string(),
            base_asset: "BTC".to_string(),
            side: "LONG".to_string(),
            status: "OPEN".to_string(),
            fsm_state: "FILLED".to_string(),
            entry_price: 90_000.0,
            current_price: 90_100.0,
            exit_price: None,
            quantity: 0.1,
            filled_units: 0.1,
            avg_fill_price: Some(90_000.0),
            notional_usdc: 9_000.0,
            risk_budget_usdc: 50.0,
            conviction_multiplier: 1.0,
            initial_stop_price: 88_000.0,
            chandelier_stop_price: 88_500.0,
            current_stop_price: 88_500.0,
            highest_price_seen: 90_100.0,
            lowest_price_seen: 90_000.0,
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc: 10.0,
            fees_paid_usdc: 1.0,
            funding_accrued_usdc: 2.5,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some("sim-r5-n2".to_string()),
            opened_at: "2026-11-06T00:00:00Z".to_string(),
            closed_at: None,
            exit_reason: None,
        };
        let tid = db.insert_trade(&trade).unwrap();

        // Simulate pre-v10 migration state (v9) before applying migration 010 transactionally (R8-S1)
        db.with_conn(|conn| {
            conn.execute_batch(
                "UPDATE system_config SET config_value = '9' WHERE config_key = 'schema_version';
                 DELETE FROM schema_migrations WHERE version >= 10;",
            )
            .unwrap();
            Ok(())
        })
        .unwrap();
        let mig_010_rep = db
            .apply_migration("sql/migrations/010_v030_round5_remediation.sql")
            .unwrap();
        assert_eq!(
            mig_010_rep.get("already_applied").and_then(|v| v.as_bool()),
            Some(false)
        );

        let suspect_after = db.audit_suspect_dry_run_book_transfers().unwrap();
        assert!(
            suspect_after.is_empty(),
            "Migration 010 must reclassify Round-2-era PAPER-mode PARTIAL_IDLE_CAPPED dry-run rows (R5-M2)"
        );
        assert!((db.get_net_book_transfer_adjustment("PIT").unwrap()).abs() < 1e-9);

        // R5-N2: Because migration 010 backfilled `last_funding_hour_bucket` to the current UTC hour bucket,
        // attempting to accrue the current UTC hour again must be a no-op!
        let cur_hour_bucket = db.now_ms() / 3_600_000;
        assert!(
            !db.accrue_trade_funding_for_hour(tid, cur_hour_bucket, 90_100.0, 10.0, -1.0)
                .unwrap(),
            "Open trade backfilled by migration 010 must not double-accrue the current hour bucket (R5-N2)"
        );
    }

    #[test]
    fn test_r6_n3_pooled_conn_guard_panic_safety() {
        let db_path = std::env::temp_dir().join(format!("panic_safe_{}.db", uuid::Uuid::new_v4()));
        let _ = std::fs::remove_file(&db_path);
        let db = DatabaseManager::new(&db_path);
        db.initialize_schema(false).unwrap();

        // Prime the pool with a normal connection
        db.with_conn(|conn| {
            let _ = conn.execute_batch("SELECT 1;");
            Ok(())
        })
        .unwrap();

        assert_eq!(
            db.conn_pool.lock().len(),
            1,
            "Pool should contain 1 connection after prime"
        );

        // Cause a panic inside with_conn and catch it with catch_unwind
        let panic_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<()> = db.with_conn(|conn| {
                let _ = conn.execute_batch("SELECT 1;");
                panic!("deliberate panic inside with_conn closure");
            });
        }));
        assert!(panic_res.is_err(), "Panic must occur");

        // R6-N3: PooledConnGuard must have recycled the connection to conn_pool
        assert_eq!(
            db.conn_pool.lock().len(),
            1,
            "PooledConnGuard must recycle the connection back to pool on panic unwind (R6-N3)"
        );

        // Also verify that a panic inside an open transaction executes ROLLBACK before recycling
        let tx_panic_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<()> = db.with_conn(|conn| {
                conn.execute_batch("BEGIN TRANSACTION;").unwrap();
                panic!("deliberate panic inside open transaction");
            });
        }));
        assert!(tx_panic_res.is_err(), "Panic must occur");

        assert_eq!(
            db.conn_pool.lock().len(),
            1,
            "Pooled connection must be rolled back and recycled to pool"
        );

        // Ensure the recycled connection is clean and usable
        let check_res = db.with_conn(|conn| {
            assert!(conn.is_autocommit(), "Connection must be back in autocommit mode");
            Ok(42)
        });
        assert_eq!(check_res.unwrap(), 42);
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
    }

    #[test]
    fn test_r6_n4_reclassify_dry_run_transfers_before() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        // Insert two PARTIAL_IDLE_CAPPED transfers at different timestamps
        let id1 = db
            .insert_book_transfer_with_mode(
                &aegis_core::BookTransfer {
                    transfer_id: 0,
                    settle_date: "2026-11-05".to_string(),
                    from_book: "PIT".to_string(),
                    to_book: "POND".to_string(),
                    amount_usdc: 100.0,
                    pre_from_usdc: Some(1000.0),
                    pre_to_usdc: Some(500.0),
                    post_from_usdc: Some(900.0),
                    post_to_usdc: Some(600.0),
                    drift_bps_before: Some(50.0),
                    drift_bps_after: Some(10.0),
                    blocked_remainder_usdc: 50.0,
                    status: "PARTIAL_IDLE_CAPPED".to_string(),
                    attempts: 0,
                    tx_ref: Some("live-tx-old".to_string()),
                    created_at: "2026-11-05T10:00:00Z".to_string(),
                },
                "LIVE",
            )
            .unwrap();
        db.with_conn(|conn| {
            conn.execute(
                "UPDATE book_transfers SET created_at = '2026-11-05T10:00:00Z' WHERE transfer_id = ?1;",
                params![id1],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
        .unwrap();

        let id2 = db
            .insert_book_transfer_with_mode(
                &aegis_core::BookTransfer {
                    transfer_id: 0,
                    settle_date: "2026-11-08".to_string(),
                    from_book: "POND".to_string(),
                    to_book: "PIT".to_string(),
                    amount_usdc: 200.0,
                    pre_from_usdc: Some(600.0),
                    pre_to_usdc: Some(1100.0),
                    post_from_usdc: Some(400.0),
                    post_to_usdc: Some(1300.0),
                    drift_bps_before: Some(40.0),
                    drift_bps_after: Some(5.0),
                    blocked_remainder_usdc: 100.0,
                    status: "PARTIAL_IDLE_CAPPED".to_string(),
                    attempts: 0,
                    tx_ref: Some("live-tx-new".to_string()),
                    created_at: "2026-11-08T15:00:00Z".to_string(),
                },
                "LIVE",
            )
            .unwrap();
        db.with_conn(|conn| {
            conn.execute(
                "UPDATE book_transfers SET created_at = '2026-11-08T15:00:00Z' WHERE transfer_id = ?1;",
                params![id2],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
        .unwrap();

        // Invalid timestamp should return validation error
        assert!(db.reclassify_dry_run_transfers_before("not-a-timestamp").is_err());

        // Reclassify cutoff between id1 and id2
        let count1 = db
            .reclassify_dry_run_transfers_before("2026-11-06T00:00:00Z")
            .unwrap();
        assert_eq!(count1, 1, "Only id1 should be reclassified");

        let transfers = db.get_recent_book_transfers(10).unwrap();
        let t1 = transfers.iter().find(|t| t.transfer_id == id1).unwrap();
        let t2 = transfers.iter().find(|t| t.transfer_id == id2).unwrap();
        assert_eq!(t1.status, "DRY_RUN");
        assert_eq!(t2.status, "PARTIAL_IDLE_CAPPED");

        // Reclassify up to cutoff after id2
        let count2 = db
            .reclassify_dry_run_transfers_before("2026-11-09T00:00:00Z")
            .unwrap();
        assert_eq!(count2, 1, "id2 should now be reclassified");

        let transfers_after = db.get_recent_book_transfers(10).unwrap();
        let t2_after = transfers_after.iter().find(|t| t.transfer_id == id2).unwrap();
        assert_eq!(t2_after.status, "DRY_RUN");
    }

    #[test]
    fn test_r7_b1_s2_s3_n4_config_read_purity_and_migration_011_stop_tightening() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        assert_eq!(db.get_schema_version().unwrap(), 12);

        // 1. R7-B1 & R7-QA: Operator sets custom chandelier_atr_multiplier_long = 2.0
        // and inserts an OPEN trade with a wide 4H stop (e.g. $86,000 on a $90,000 BTC LONG).
        // Calling load_config(), get_config_rows(), and get_credentials_status() MUST NOT
        // overwrite the operator's config or mutate ANY trades row!
        db.set_config_value("chandelier_atr_multiplier_long", &json!(2.0))
            .unwrap();
        let pit_tid = db
            .insert_trade(&aegis_core::TradeRecord {
                trade_id: 0,
                client_order_id: "hl-paper-pit-long-1".to_string(),
                symbol: "BTC/USDC:USDC".to_string(),
                base_asset: "BTC".to_string(),
                side: "LONG".to_string(),
                status: "OPEN".to_string(),
                fsm_state: "FILLED".to_string(),
                entry_price: 90_000.0,
                current_price: 90_000.0,
                exit_price: None,
                quantity: -0.1,     // R7-N4: negative input must be normalized to +0.1
                filled_units: -0.1, // R7-N4: negative input must be normalized to +0.1
                avg_fill_price: Some(90_000.0),
                notional_usdc: 9_000.0,
                risk_budget_usdc: 400.0,
                conviction_multiplier: 1.0,
                initial_stop_price: 86_000.0,
                chandelier_stop_price: 86_000.0,
                current_stop_price: 86_000.0,
                highest_price_seen: 90_000.0,
                lowest_price_seen: 90_000.0,
                realized_pnl_usdc: 0.0,
                unrealized_pnl_usdc: 0.0,
                fees_paid_usdc: 1.0,
                funding_accrued_usdc: 0.0,
                slippage_paid_usd: 0.0,
                exchange_order_id: Some("sim-pit-long-1".to_string()),
                opened_at: "2026-11-09T00:00:00Z".to_string(),
                closed_at: None,
                exit_reason: None,
            })
            .unwrap();

        let cfg = db.load_config().unwrap();
        let _rows = db.get_config_rows(true).unwrap();
        let _creds = db.get_credentials_status().unwrap();
        assert!(
            (cfg.get_f64("chandelier_atr_multiplier_long", 0.0) - 2.0).abs() < 1e-9,
            "load_config() must never overwrite operator-configured chandelier_atr_multiplier_long (R7-B1)"
        );

        let open_trades = db.get_open_trades().unwrap();
        let pit_trade = open_trades.iter().find(|t| t.trade_id == pit_tid).unwrap();
        assert!(
            (pit_trade.current_stop_price - 86_000.0).abs() < 1e-6
                && (pit_trade.chandelier_stop_price - 86_000.0).abs() < 1e-6,
            "load_config() / reconcile_routing_config() must NEVER mutate open trades stop prices (R7-B1, R7-QA)"
        );
        assert!(
            (pit_trade.quantity - 0.1).abs() < 1e-9 && (pit_trade.filled_units - 0.1).abs() < 1e-9,
            "insert_trade must enforce non-negative quantity and filled_units (R7-N4)"
        );

        // 2. R7-S2: Migration 011 one-time stop-band normalization applies ONLY to
        // `onchain-reconciled-%` open rows when schema_version < 11, and leaves `hl-%` / `hl-paper-%` untouched.
        db.reconcile_on_chain_orphan_position(
            "ETH/USDC:USDC",
            "ETH",
            "LONG",
            1.0,
            3_000.0,
            3_000.0,
            0.0,
        )
        .unwrap();
        // Simulate a legacy wide stop ($2,760.00) on the onchain-reconciled ETH trade before v11
        let eth_tid = db
            .get_open_trades()
            .unwrap()
            .into_iter()
            .find(|t| t.symbol == "ETH/USDC:USDC")
            .unwrap()
            .trade_id;
        db.update_open_trade(
            eth_tid, 3_000.0, 2_760.0, 3_000.0, 3_000.0, 0.0, 0.0,
        )
        .unwrap();
        db.with_conn(|conn| {
            conn.execute_batch(
                "UPDATE system_config SET config_value = '10' WHERE config_key = 'schema_version';
                 DELETE FROM schema_migrations WHERE version >= 11;",
            )
            .unwrap();
            Ok(())
        })
        .unwrap();
        db.apply_migration("sql/migrations/011_v030_round7_stop_tightening.sql")
            .unwrap();
        assert_eq!(db.get_schema_version().unwrap(), 11);

        let after_mig = db.get_open_trades().unwrap();
        let pit_after = after_mig.iter().find(|t| t.trade_id == pit_tid).unwrap();
        let eth_after = after_mig.iter().find(|t| t.trade_id == eth_tid).unwrap();
        assert!(
            (pit_after.current_stop_price - 86_000.0).abs() < 1e-6,
            "Migration 011 must not touch hl-paper-% or hl-% strategy trades (R7-B1, R7-S2)"
        );
        assert!(
            (eth_after.current_stop_price - 2_976.0).abs() < 1e-3,
            "Migration 011 must tighten legacy wide onchain-reconciled-% stop ($2,760.00) to 0.8% cap ($2,976.00)"
        );

        // Verify that if an operator sets chandelier_atr_multiplier_long = 3.0 at v11 (or post-b392d2b),
        // re-running run_migrations() or apply_migration(011) preserves 3.0 untouched (R7-S2, R8-S1, R8-S2).
        db.set_config_value("chandelier_atr_multiplier_long", &json!(3.0))
            .unwrap();
        let newly_applied = db.run_migrations().unwrap();
        assert_eq!(
            newly_applied,
            vec!["sql/migrations/012_v030_round9_remediation.sql".to_string()],
            "run_migrations() from v11 must report ONLY migration 012 as newly applied (R8-S2)"
        );
        assert_eq!(
            db.run_migrations().unwrap(),
            Vec::<String>::new(),
            "run_migrations() when already at v12 must return empty Vec (R8-S2)"
        );
        // R8-S1: If an operator widens the ETH stop at v12 and calls apply_migration(011) directly,
        // target_num (11) <= max_applied_ver (12), so the SQL script must NOT re-execute!
        db.update_open_trade(
            eth_tid, 3_000.0, 2_760.0, 3_000.0, 3_000.0, 0.0, 0.0,
        )
        .unwrap();
        let rep_011 = db
            .apply_migration("sql/migrations/011_v030_round7_stop_tightening.sql")
            .unwrap();
        assert_eq!(
            rep_011.get("already_applied").and_then(|v| v.as_bool()),
            Some(true),
            "apply_migration(011) at v12 must report already_applied = true (R8-S1)"
        );
        let eth_after_011 = db
            .get_open_trades()
            .unwrap()
            .into_iter()
            .find(|t| t.trade_id == eth_tid)
            .unwrap();
        assert!(
            (eth_after_011.current_stop_price - 2_760.0).abs() < 1e-6,
            "apply_migration(011) when max_applied_ver=12 must skip re-running target SQL script (R8-S1)"
        );
        let cfg_after_remig = db.load_config().unwrap();
        assert!(
            (cfg_after_remig.get_f64("chandelier_atr_multiplier_long", 0.0) - 3.0).abs() < 1e-9,
            "Re-running migrations at v11/v12 must never overwrite operator-calibrated 3.0 stop multiplier (R7-S2)"
        );
        assert!(
            (cfg_after_remig.get_f64("testnet_faucet_offset_usdc", 0.0) - 10_000.0).abs() < 1e-6,
            "Migration 012 must seed testnet_faucet_offset_usdc = 10000.0 (R9-N2)"
        );

        // 3. R7-S3: close_trade and cancel_resting_trade enforce status predicates and return bool
        assert!(
            db.close_trade(pit_tid, 91_000.0, 100.0, 1.0, "TEST_CLOSE")
                .unwrap(),
            "First close_trade on OPEN trade must return true (R7-S3)"
        );
        assert!(
            !db.close_trade(pit_tid, 99_000.0, 900.0, 9.0, "OVERWRITE_ATTEMPT")
                .unwrap(),
            "Second close_trade on already-CLOSED trade must return false and not overwrite PnL/exit_reason (R7-S3)"
        );
        let closed_pit = db
            .get_recent_trades(10)
            .unwrap()
            .into_iter()
            .find(|t| t.trade_id == pit_tid)
            .unwrap();
        assert_eq!(closed_pit.exit_reason.as_deref(), Some("TEST_CLOSE"));
        assert!((closed_pit.realized_pnl_usdc - 100.0).abs() < 1e-6);
    }

    #[test]
    fn test_legacy_trades_check_constraint_rebuild_allows_pending_status() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        // Simulate a legacy database whose `trades` table still has
        // `CHECK (status IN ('OPEN', 'CLOSED', 'CANCELLED'))` (without 'PENDING'),
        // AND has both a dependent view (`v_portfolio_summary`) AND child foreign-key tables
        // (`funding_payments`, `order_events`) with `PRAGMA foreign_keys = ON`.
        db.with_conn(|conn| {
            conn.execute_batch(
                "PRAGMA foreign_keys = OFF;
                 DROP VIEW IF EXISTS v_portfolio_summary;
                 ALTER TABLE trades RENAME TO trades_tmp_legacy;
                 CREATE TABLE trades (
                     trade_id                INTEGER PRIMARY KEY AUTOINCREMENT,
                     client_order_id         TEXT    NOT NULL UNIQUE,
                     symbol                  TEXT    NOT NULL,
                     base_asset              TEXT    NOT NULL,
                     side                    TEXT    NOT NULL CHECK (side IN ('LONG', 'SHORT')),
                     status                  TEXT    NOT NULL DEFAULT 'OPEN'
                                                     CHECK (status IN ('OPEN', 'CLOSED', 'CANCELLED')),
                     fsm_state               TEXT    NOT NULL DEFAULT 'FILLED'
                                                     CHECK (fsm_state IN (
                                                         'IDLE', 'VALIDATED', 'MAKER_QUOTING', 'OPEN_ON_BOOK',
                                                         'PARTIAL_FILL', 'CHASES_EXHAUSTED', 'IOC_SWEEP',
                                                         'FILLED', 'CANCELLED', 'REJECTED', 'HALTED'
                                                     )),
                     entry_price             REAL    NOT NULL CHECK (entry_price > 0.0),
                     current_price           REAL    NOT NULL CHECK (current_price > 0.0),
                     exit_price              REAL,
                     quantity                REAL    NOT NULL CHECK (quantity > 0.0),
                     filled_units            REAL    NOT NULL DEFAULT 0.0 CHECK (filled_units >= 0.0),
                     avg_fill_price          REAL,
                     notional_usdc           REAL    NOT NULL CHECK (notional_usdc > 0.0),
                     risk_budget_usdc        REAL    NOT NULL DEFAULT 50.0,
                     conviction_multiplier   REAL    NOT NULL DEFAULT 1.0,
                     initial_stop_price      REAL    NOT NULL CHECK (initial_stop_price > 0.0),
                     chandelier_stop_price   REAL    NOT NULL CHECK (chandelier_stop_price > 0.0),
                     current_stop_price      REAL    NOT NULL DEFAULT 0.0,
                     highest_price_seen      REAL    NOT NULL CHECK (highest_price_seen > 0.0),
                     lowest_price_seen       REAL    NOT NULL CHECK (lowest_price_seen > 0.0),
                     realized_pnl_usdc       REAL    NOT NULL DEFAULT 0.0,
                     unrealized_pnl_usdc     REAL    NOT NULL DEFAULT 0.0,
                     fees_paid_usdc          REAL    NOT NULL DEFAULT 0.0,
                     funding_accrued_usdc    REAL    NOT NULL DEFAULT 0.0,
                     slippage_paid_usd       REAL    NOT NULL DEFAULT 0.0,
                     exchange_order_id       TEXT,
                     opened_at               TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                     closed_at               TEXT,
                     exit_reason             TEXT
                 );
                 DROP TABLE trades_tmp_legacy;
                 CREATE VIEW IF NOT EXISTS v_portfolio_summary AS
                 SELECT COUNT(*) AS open_count FROM trades WHERE status = 'OPEN';
                 CREATE TABLE IF NOT EXISTS funding_payments (
                     payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                     trade_id INTEGER NOT NULL REFERENCES trades(trade_id),
                     amount_usdc REAL NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS order_events (
                     event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                     trade_id INTEGER NOT NULL REFERENCES trades(trade_id),
                     event_type TEXT NOT NULL
                 );
                 PRAGMA foreign_keys = ON;",
            )
            .unwrap();
            Ok(())
        })
        .unwrap();

        let existing_open = aegis_core::TradeRecord {
            trade_id: 0,
            client_order_id: "hl-open-1".to_string(),
            symbol: "BTC/USDC:USDC".to_string(),
            base_asset: "BTC".to_string(),
            side: "LONG".to_string(),
            status: "OPEN".to_string(),
            fsm_state: "FILLED".to_string(),
            entry_price: 90_000.0,
            current_price: 90_000.0,
            exit_price: None,
            quantity: 0.01,
            filled_units: 0.01,
            avg_fill_price: Some(90_000.0),
            notional_usdc: 900.0,
            risk_budget_usdc: 10.0,
            conviction_multiplier: 1.0,
            initial_stop_price: 87_000.0,
            chandelier_stop_price: 87_000.0,
            current_stop_price: 87_000.0,
            highest_price_seen: 90_000.0,
            lowest_price_seen: 90_000.0,
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.1,
            funding_accrued_usdc: 0.0,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some("hl-111".to_string()),
            opened_at: "2026-11-09T15:00:00Z".to_string(),
            closed_at: None,
            exit_reason: None,
        };
        let open_id = db.insert_trade(&existing_open).unwrap();

        // Insert child FK rows referencing `open_id` with `PRAGMA foreign_keys = ON`.
        db.with_conn(|conn| {
            conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            conn.execute(
                "INSERT INTO funding_payments (trade_id, amount_usdc) VALUES (?1, -0.25);",
                rusqlite::params![open_id],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO order_events (trade_id, event_type) VALUES (?1, 'FILLED');",
                rusqlite::params![open_id],
            )
            .unwrap();
            Ok(())
        })
        .unwrap();

        // Inserting a resting `PENDING` (`OPEN_ON_BOOK`) trade must self-heal the legacy CHECK constraint
        // even with `PRAGMA foreign_keys = ON`, child FK rows, and dependent views.
        let mut pending_trade = existing_open.clone();
        pending_trade.client_order_id = "hl-pending-near".to_string();
        pending_trade.symbol = "NEAR/USDC:USDC".to_string();
        pending_trade.base_asset = "NEAR".to_string();
        pending_trade.side = "SHORT".to_string();
        pending_trade.status = "PENDING".to_string();
        pending_trade.fsm_state = "OPEN_ON_BOOK".to_string();
        pending_trade.entry_price = 3.612;
        pending_trade.current_price = 3.612;
        pending_trade.quantity = 34.6;
        pending_trade.filled_units = 0.0;
        pending_trade.avg_fill_price = None;
        pending_trade.notional_usdc = 124.98;
        pending_trade.risk_budget_usdc = 4.7748;
        pending_trade.initial_stop_price = 3.75;
        pending_trade.chandelier_stop_price = 3.75;
        pending_trade.current_stop_price = 3.75;
        pending_trade.highest_price_seen = 3.612;
        pending_trade.lowest_price_seen = 3.612;
        pending_trade.exchange_order_id = Some("hl-64920976942".to_string());

        let pending_id = db
            .insert_trade(&pending_trade)
            .expect("insert_trade must auto-upgrade legacy trades CHECK constraint with FK rows and views");
        assert!(pending_id > open_id);

        // R9-QA: Verify telemetry_logs recorded the schema self-heal rebuild under component `db.schema_self_heal`
        let logs = db.get_recent_logs(20).unwrap();
        assert!(
            logs.iter().any(|l| {
                l.get("component").and_then(|v| v.as_str()) == Some("db.schema_self_heal")
                    && l.get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .contains("PENDING")
            }),
            "ensure_trades_pending_status_check must emit db.schema_self_heal telemetry when rebuilding trades table (R9-QA)"
        );

        // Verify child FK rows and dependent view survived the table rebuild
        db.with_conn(|conn| {
            let fk_funding: i64 = conn
                .query_row("SELECT COUNT(*) FROM funding_payments WHERE trade_id = ?1;", rusqlite::params![open_id], |r| r.get(0))
                .unwrap();
            let fk_events: i64 = conn
                .query_row("SELECT COUNT(*) FROM order_events WHERE trade_id = ?1;", rusqlite::params![open_id], |r| r.get(0))
                .unwrap();
            let view_open_cnt: i64 = conn
                .query_row("SELECT open_count FROM v_portfolio_summary;", [], |r| r.get(0))
                .unwrap();
            assert_eq!(fk_funding, 1);
            assert_eq!(fk_events, 1);
            assert_eq!(view_open_cnt, 1);
            Ok(())
        })
        .unwrap();

        // Verify cross-book symbol exclusion includes PENDING PIT symbols
        let wallet_syms = db.get_all_open_wallet_symbols().unwrap();
        assert!(wallet_syms.contains("BTC/USDC:USDC"));
        assert!(wallet_syms.contains("NEAR/USDC:USDC"));

        let resting = db.get_resting_pit_trades().unwrap();
        assert_eq!(resting.len(), 1);
        assert_eq!(resting[0].trade_id, pending_id);
        assert_eq!(resting[0].status, "PENDING");
        assert_eq!(
            resting[0].exchange_order_id.as_deref(),
            Some("hl-64920976942")
        );

        let open_after = db.get_open_trades().unwrap();
        assert_eq!(open_after.len(), 1);
        assert_eq!(open_after[0].trade_id, open_id);

        // Promote the resting trade on a 50% partial fill (17.3 NEAR @ 3.612) and verify:
        // 1. status transitions to OPEN, quantity = 17.3, risk_budget_usdc rescaled to |3.612 - 3.75| * 17.3 = 2.3874
        // 2. order_state_audit records the OPEN_ON_BOOK -> FILLED transition
        assert!(db
            .promote_resting_trade_to_open(pending_id, 17.3, 3.612, 0.01, 0.0)
            .unwrap());
        let open_promoted = db
            .get_open_trades()
            .unwrap()
            .into_iter()
            .find(|t| t.trade_id == pending_id)
            .unwrap();
        assert_eq!(open_promoted.status, "OPEN");
        assert_eq!(open_promoted.fsm_state, "FILLED");
        assert!((open_promoted.quantity - 17.3).abs() < 1e-6);
        assert!((open_promoted.risk_budget_usdc - 2.3874).abs() < 1e-4);

        // R9-N5: Verify reconcile_open_trade_size rescales risk_budget_usdc in both Some(entry_px) and None branches
        assert!(db
            .reconcile_open_trade_size(pending_id, 10.0, Some(3.60))
            .unwrap());
        let resized_some = db
            .get_open_trades()
            .unwrap()
            .into_iter()
            .find(|t| t.trade_id == pending_id)
            .unwrap();
        assert!(
            (resized_some.risk_budget_usdc - 1.5).abs() < 1e-4,
            "reconcile_open_trade_size(Some(3.60)) must rescale risk_budget_usdc to |3.60 - 3.75| * 10 = 1.50 (R9-N5)"
        );
        assert!(db.reconcile_open_trade_size(pending_id, 20.0, None).unwrap());
        let resized_none = db
            .get_open_trades()
            .unwrap()
            .into_iter()
            .find(|t| t.trade_id == pending_id)
            .unwrap();
        assert!(
            (resized_none.risk_budget_usdc - 3.0).abs() < 1e-4,
            "reconcile_open_trade_size(None) must rescale risk_budget_usdc to |3.60 - 3.75| * 20 = 3.00 (R9-N5)"
        );

        // R9-N1: Verify cancel_resting_trade cancels a resting trade whose fsm_state is 'PENDING' as well as 'OPEN_ON_BOOK'
        let mut pending_fsm_trade = pending_trade.clone();
        pending_fsm_trade.client_order_id = "hl-pending-fsm-sol".to_string();
        pending_fsm_trade.symbol = "SOL/USDC:USDC".to_string();
        pending_fsm_trade.base_asset = "SOL".to_string();
        pending_fsm_trade.status = "PENDING".to_string();
        pending_fsm_trade.fsm_state = "OPEN_ON_BOOK".to_string();
        let sol_pending_id = db.insert_trade(&pending_fsm_trade).unwrap();
        db.with_conn(|conn| {
            conn.execute_batch("PRAGMA ignore_check_constraints = ON;")
                .unwrap();
            conn.execute(
                "UPDATE trades SET fsm_state = 'PENDING' WHERE trade_id = ?1;",
                rusqlite::params![sol_pending_id],
            )
            .unwrap();
            conn.execute_batch("PRAGMA ignore_check_constraints = OFF;")
                .unwrap();
            Ok(())
        })
        .unwrap();
        assert!(
            db.cancel_resting_trade(sol_pending_id, "TEST_CANCEL_PENDING_FSM")
                .unwrap(),
            "cancel_resting_trade must cancel resting trades with fsm_state IN ('OPEN_ON_BOOK', 'PENDING') (R9-N1)"
        );

        let audit_rows = db.get_order_state_audit(Some("hl-pending-near"), 10).unwrap();
        assert!(audit_rows.iter().any(|r| {
            r.get("client_order_id").and_then(|v| v.as_str()) == Some("hl-pending-near")
                && r.get("from_state").and_then(|v| v.as_str()) == Some("OPEN_ON_BOOK")
                && r.get("to_state").and_then(|v| v.as_str()) == Some("FILLED")
        }));
    }
}

