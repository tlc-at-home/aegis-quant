//! SQLite WAL connection manager, schema/migration runner, Fernet secret encryption,
//! and centralized variable loader (`system_config`) for Aegis Quant.

pub mod fernet;

use crate::fernet::{
    default_host_fernet_key_b64, is_encrypted_value, normalize_fernet_key,
    resolve_fernet_master_key, FernetCipher,
};
use aegis_core::{
    mask_secret, resolve_project_root, round_dp, validate_evm_address, validate_private_key,
    AegisError, AltPairMetadata, ExecutionMode, OhlcvBar, OrderFsmState, PortfolioSnapshot, Result,
    SignalEvaluation, TradeRecord, UniverseAsset, SENSITIVE_CONFIG_KEYS,
};
use chrono::Utc;
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

/// Typed configuration dictionary loaded from `system_config`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfigMap {
    pub values: HashMap<String, Value>,
}

impl ConfigMap {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    pub fn insert(&mut self, k: impl Into<String>, v: Value) {
        self.values.insert(k.into(), v);
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.values.get(key)
    }

    pub fn get_str(&self, key: &str, default: &str) -> String {
        match self.values.get(key) {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Bool(b)) => b.to_string(),
            Some(Value::Number(n)) => n.to_string(),
            _ => default.to_string(),
        }
    }

    pub fn get_f64(&self, key: &str, default: f64) -> f64 {
        match self.values.get(key) {
            Some(Value::Number(n)) => n.as_f64().unwrap_or(default),
            Some(Value::String(s)) => s.trim().parse::<f64>().unwrap_or(default),
            _ => default,
        }
    }

    pub fn get_i64(&self, key: &str, default: i64) -> i64 {
        match self.values.get(key) {
            Some(Value::Number(n)) => n
                .as_i64()
                .or_else(|| n.as_f64().map(|f| f as i64))
                .unwrap_or(default),
            Some(Value::String(s)) => s
                .trim()
                .parse::<f64>()
                .map(|f| f as i64)
                .unwrap_or(default),
            _ => default,
        }
    }

    pub fn get_usize(&self, key: &str, default: usize) -> usize {
        self.get_i64(key, default as i64).max(0) as usize
    }

    pub fn get_bool(&self, key: &str, default: bool) -> bool {
        match self.values.get(key) {
            Some(Value::Bool(b)) => *b,
            Some(Value::String(s)) => matches!(
                s.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "on" | "testnet"
            ),
            Some(Value::Number(n)) => n.as_i64().unwrap_or(0) != 0,
            _ => default,
        }
    }
}

pub fn cast_config_value(raw_val: &str, value_type: &str) -> Value {
    let vtype = value_type.trim().to_ascii_lowercase();
    match vtype.as_str() {
        "int" => {
            let f = raw_val.trim().parse::<f64>().unwrap_or(0.0);
            json!(if f.is_finite() { f as i64 } else { 0 })
        }
        "float" => {
            let f = raw_val.trim().parse::<f64>().unwrap_or(0.0);
            json!(if f.is_finite() { f } else { 0.0 })
        }
        "bool" => {
            let b = matches!(
                raw_val.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "on"
            );
            json!(b)
        }
        _ => json!(raw_val),
    }
}

fn alert_config_defaults() -> HashMap<&'static str, (&'static str, &'static str, &'static str, &'static str)> {
    HashMap::from([
        (
            "telegram_bot_token",
            (
                "",
                "string",
                "alerts",
                "Telegram Bot API token for real-time operational notifications (Fernet AES-128 encrypted)",
            ),
        ),
        (
            "telegram_chat_id",
            (
                "",
                "string",
                "alerts",
                "Telegram target chat ID for real-time operational notifications",
            ),
        ),
        (
            "discord_webhook_url",
            (
                "",
                "string",
                "alerts",
                "Discord webhook URL for real-time operational notifications (Fernet AES-128 encrypted)",
            ),
        ),
        (
            "max_daily_drawdown_alert_pct",
            (
                "0.03",
                "float",
                "alerts",
                "Daily portfolio drawdown threshold (3.0%) triggering critical Telegram alert",
            ),
        ),
    ])
}

/// Thread-safe SQLite Write-Ahead Logging (WAL) and `:memory:` database manager.
#[derive(Clone)]
pub struct DatabaseManager {
    pub db_path: PathBuf,
    pub schema_path: PathBuf,
    is_memory: bool,
    memory_conn: Option<Arc<Mutex<Connection>>>,
    pit_timestamp_ms: Arc<AtomicI64>,
    explicit_master_key: Arc<Mutex<Option<String>>>,
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
            pit_timestamp_ms: Arc::new(AtomicI64::new(-1)),
            explicit_master_key: Arc::new(Mutex::new(master_key)),
        }
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
            .unwrap_or_else(|| Utc::now().timestamp_millis())
    }

    /// Execute a closure with a configured SQLite WAL or `:memory:` connection inside a transaction.
    pub fn with_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        if let Some(mem_arc) = &self.memory_conn {
            let conn = mem_arc.lock();
            return f(&conn);
        }

        let mut last_err = None;
        for attempt in 0..16 {
            match Connection::open(&self.db_path) {
                Ok(conn) => {
                    let _ = conn.busy_timeout(Duration::from_millis(15000));
                    let _ = conn.execute_batch(
                        "PRAGMA temp_store = MEMORY;
                         PRAGMA journal_mode = WAL;
                         PRAGMA synchronous = NORMAL;
                         PRAGMA foreign_keys = ON;",
                    );
                    return f(&conn);
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

    /// Execute multi-statement SQL idempotently, ignoring duplicate column errors on `ALTER TABLE`.
    fn execute_sql_idempotent(conn: &Connection, sql_script: &str) -> Result<()> {
        let mut buffer = Vec::new();
        for line in sql_script.lines() {
            let stripped = line.trim();
            if stripped.is_empty() && buffer.is_empty() {
                continue;
            }
            if stripped.starts_with("--") && buffer.is_empty() {
                continue;
            }
            buffer.push(line);
            let candidate = buffer.join("\n");
            if candidate.trim_end().ends_with(';') {
                buffer.clear();
                let clean: String = candidate
                    .lines()
                    .filter(|ln| !ln.trim().starts_with("--"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let clean = clean.trim();
                if clean.is_empty() {
                    continue;
                }
                if let Err(e) = conn.execute_batch(clean) {
                    let msg = e.to_string().to_ascii_lowercase();
                    if msg.contains("duplicate column name") {
                        continue;
                    }
                    return Err(AegisError::Database(format!("SQL error: {e}")));
                }
            }
        }
        if !buffer.is_empty() {
            let rem: String = buffer
                .into_iter()
                .filter(|ln| !ln.trim().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");
            let rem = rem.trim();
            if !rem.is_empty() {
                if let Err(e) = conn.execute_batch(rem) {
                    if !e.to_string().to_ascii_lowercase().contains("duplicate column name") {
                        return Err(AegisError::Database(format!("SQL error: {e}")));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn init_db(&self) -> Result<Value> {
        if !self.schema_path.exists() {
            return Err(AegisError::NotFound(format!(
                "Schema SQL file not found: {}",
                self.schema_path.display()
            )));
        }
        let mut sql_script = fs::read_to_string(&self.schema_path)?;
        if self.is_memory {
            sql_script = sql_script.replace("PRAGMA journal_mode = WAL;", "");
        }
        self.with_conn(|conn| {
            conn.execute_batch(&sql_script)
                .map_err(|e| AegisError::Database(format!("Failed to init_db: {e}")))?;
            Ok(())
        })?;
        self.verify_integrity()
    }

    pub fn initialize_database(&self) -> Result<Value> {
        let mut integrity = self.init_db()?;
        let root = resolve_project_root();
        let migrations = [
            "sql/migrations/001_v020_config_expansion.sql",
            "sql/migrations/002_v020_order_fsm.sql",
            "sql/migrations/003_v020_schema_versioning.sql",
            "sql/migrations/004_v020_rc2_remediation.sql",
        ];
        for rel in migrations {
            let p = root.join(rel);
            if p.exists() {
                integrity = self.apply_migration(&p)?;
            }
        }
        Ok(integrity)
    }

    pub fn initialize_schema(&self, force: bool) -> Result<Value> {
        if !force {
            if let Ok(ver) = self.get_schema_version() {
                if ver >= 4 {
                    return self.verify_integrity();
                }
            }
        }
        self.initialize_database()
    }

    pub fn run_migrations(&self) -> Result<Vec<String>> {
        let root = resolve_project_root();
        let migrations = [
            "sql/migrations/001_v020_config_expansion.sql",
            "sql/migrations/002_v020_order_fsm.sql",
            "sql/migrations/003_v020_schema_versioning.sql",
            "sql/migrations/004_v020_rc2_remediation.sql",
        ];
        let mut applied = Vec::new();
        for rel in migrations {
            let p = root.join(rel);
            if p.exists() {
                self.apply_migration(&p)?;
                applied.push(rel.to_string());
            }
        }
        Ok(applied)
    }

    pub fn apply_migration(&self, migration_path: impl AsRef<Path>) -> Result<Value> {
        let root = resolve_project_root();
        let mut mig_path = migration_path.as_ref().to_path_buf();
        if !mig_path.exists() {
            let candidate = root.join(&mig_path);
            if candidate.exists() {
                mig_path = candidate;
            } else {
                return Err(AegisError::NotFound(format!(
                    "Migration SQL file not found: {}",
                    migration_path.as_ref().display()
                )));
            }
        }

        let has_cfg: bool = self.with_conn(|conn| {
            let name: Option<String> = conn
                .query_row(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='system_config';",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(name.is_some())
        })?;
        if !has_cfg {
            self.init_db()?;
        }

        let file_name = mig_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        let sql_script = fs::read_to_string(&mig_path)?;

        self.with_conn(|conn| {
            if file_name.starts_with("003_") || file_name.starts_with("004_") {
                for prereq in [
                    "sql/migrations/001_v020_config_expansion.sql",
                    "sql/migrations/002_v020_order_fsm.sql",
                ] {
                    let p = root.join(prereq);
                    if p.exists() {
                        if let Ok(s) = fs::read_to_string(&p) {
                            Self::execute_sql_idempotent(conn, &s)?;
                        }
                    }
                }
                if file_name.starts_with("004_") {
                    let p3 = root.join("sql/migrations/003_v020_schema_versioning.sql");
                    if p3.exists() {
                        if let Ok(s) = fs::read_to_string(&p3) {
                            Self::execute_sql_idempotent(conn, &s)?;
                        }
                    }
                }
            }
            Self::execute_sql_idempotent(conn, &sql_script)?;

            let has_mig_table: Option<String> = conn
                .query_row(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations';",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;

            if has_mig_table.is_some() && file_name.len() >= 3 {
                if let Ok(ver_num) = file_name[..3].parse::<i64>() {
                    let stem = mig_path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or(&file_name);
                    let _ = conn.execute(
                        "INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description)
                         VALUES (?1, CAST(strftime('%s', 'now') AS INTEGER), ?2);",
                        params![ver_num, format!("Migration {:03}: {stem}", ver_num)],
                    );
                }
            }
            Ok(())
        })?;

        let integrity = self.verify_integrity()?;
        let _ = self.log_telemetry(
            "INFO",
            "engine.database",
            &format!("Applied SQLite migration {file_name}"),
            Some(integrity.clone()),
        );

        let mut out = integrity;
        if let Some(obj) = out.as_object_mut() {
            obj.insert("status".to_string(), json!("APPLIED"));
            obj.insert("migration_file".to_string(), json!(file_name));
        }
        Ok(out)
    }

    pub fn verify_integrity(&self) -> Result<Value> {
        self.with_conn(|conn| {
            let journal_mode: String = conn
                .query_row("PRAGMA journal_mode;", [], |r| r.get(0))
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let config_count: i64 = conn
                .query_row("SELECT COUNT(*) FROM system_config;", [], |r| r.get(0))
                .unwrap_or(0);
            let usdc_perp_count: i64 = conn
                .query_row("SELECT COUNT(*) FROM v_universe_usdc_perp;", [], |r| r.get(0))
                .unwrap_or(0);
            let btc_synth_count: i64 = conn
                .query_row("SELECT COUNT(*) FROM v_universe_btc_synth;", [], |r| r.get(0))
                .unwrap_or(0);
            let alt_pairs_count: i64 = conn
                .query_row("SELECT COUNT(*) FROM v_universe_alt_pairs;", [], |r| r.get(0))
                .unwrap_or(0);
            let total_universe: i64 = conn
                .query_row("SELECT COUNT(*) FROM universe_assets;", [], |r| r.get(0))
                .unwrap_or(0);
            let has_mig: Option<String> = conn
                .query_row(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations';",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .unwrap_or(None);
            let schema_version: i64 = if has_mig.is_some() {
                conn.query_row(
                    "SELECT COALESCE(MAX(version), 0) FROM schema_migrations;",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0)
            } else {
                0
            };

            Ok(json!({
                "journal_mode": journal_mode.to_ascii_lowercase(),
                "system_config_count": config_count,
                "v_universe_usdc_perp_count": usdc_perp_count,
                "v_universe_btc_synth_count": btc_synth_count,
                "v_universe_alt_pairs_count": alt_pairs_count,
                "universe_assets_total": total_universe,
                "schema_version": schema_version,
            }))
        })
    }

    pub fn get_schema_version(&self) -> Result<i64> {
        let v = self.verify_integrity()?;
        Ok(v.get("schema_version").and_then(|x| x.as_i64()).unwrap_or(0))
    }

    /// Retrieve or derive the Fernet encryption master key, enforcing mandatory explicit key
    /// presence in `LIVE` and `TESTNET` modes when `enforce_master_key` is enabled.
    pub fn get_master_fernet_key(&self) -> Result<String> {
        let cfg = self.load_config_opts(false)?;
        let exec_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let enforce_key = cfg.get_bool("enforce_master_key", true);

        let explicit = self.explicit_master_key.lock().clone();
        let root_db_dir = resolve_project_root().join("data").join("aegis_quant.db");
        let eff_db_path = if self.is_memory && (exec_mode == "LIVE" || exec_mode == "TESTNET") {
            Some(root_db_dir.as_path())
        } else if self.is_memory {
            None
        } else {
            Some(self.db_path.as_path())
        };

        let (key_b64, is_explicit) = resolve_fernet_master_key(explicit.as_deref(), eff_db_path);
        if !self.is_memory
            && (exec_mode == "LIVE" || exec_mode == "TESTNET")
            && enforce_key
            && !is_explicit
        {
            let msg = format!(
                "FATAL SECURITY VIOLATION: Execution mode is '{exec_mode}' but no explicit \
                 AEGIS_MASTER_KEY environment variable or systemd credential was provided. \
                 Refusing to run with the hardcoded fallback key."
            );
            let _ = self.log_telemetry("CRITICAL", "database", &msg, None);
            return Err(AegisError::SecurityViolation(msg));
        }
        Ok(key_b64)
    }

    fn get_fernet(&self) -> Result<FernetCipher> {
        let explicit = self.explicit_master_key.lock().clone();
        let key_b64 = match self.get_master_fernet_key() {
            Ok(k) => k,
            Err(AegisError::SecurityViolation(msg)) => {
                return Err(AegisError::SecurityViolation(msg))
            }
            Err(_) => {
                resolve_fernet_master_key(explicit.as_deref(), Some(&self.db_path)).0
            }
        };
        FernetCipher::from_key_str(&key_b64)
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
        match cipher.decrypt(token) {
            Ok(plain) => Ok(plain),
            Err(e) => {
                let is_explicit = self.explicit_master_key.lock().is_some();
                let default_b64 = default_host_fernet_key_b64();
                if !is_explicit && cipher.key_b64() != default_b64 {
                    if let Ok(fallback) = FernetCipher::from_key_str(&default_b64) {
                        if let Ok(plain) = fallback.decrypt(token) {
                            return Ok(plain);
                        }
                    }
                }
                Err(e)
            }
        }
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

    pub fn rotate_master_key(&self, new_master_key: &str) -> Result<Value> {
        let new_b64 = normalize_fernet_key(new_master_key.as_bytes());
        let new_cipher = FernetCipher::from_key_str(&new_b64)?;
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

        let mut re_encrypted = Vec::new();
        for (k, raw_v) in rows {
            if !sensitive.contains(k.as_str()) || raw_v.trim().is_empty() {
                continue;
            }
            let plain = self.try_decrypt_or_empty(&raw_v);
            if plain.is_empty() {
                continue;
            }
            let enc_v = new_cipher.encrypt(plain.as_bytes())?;
            self.with_conn(|conn| {
                conn.execute(
                    "UPDATE system_config
                     SET config_value = ?1,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                     WHERE config_key = ?2;",
                    params![enc_v, k],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(())
            })?;
            re_encrypted.push(k);
        }
        re_encrypted.sort();
        *self.explicit_master_key.lock() = Some(new_master_key.to_string());
        Ok(json!({
            "status": "ROTATED",
            "re_encrypted_keys": re_encrypted,
        }))
    }

    pub fn get_raw_config_value(&self, key: &str) -> Result<Option<String>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT config_value FROM system_config WHERE config_key = ?1;",
                params![key],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| AegisError::Database(e.to_string()))
        })
    }

    pub fn load_config(&self) -> Result<ConfigMap> {
        self.load_config_opts(true)
    }

    pub fn load_config_opts(&self, decrypt: bool) -> Result<ConfigMap> {
        let rows: Vec<(String, String, String)> = self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT config_key, config_value, value_type FROM system_config;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                        r.get::<_, Option<String>>(2)?
                            .unwrap_or_else(|| "string".to_string()),
                    ))
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })?;

        let mut map = ConfigMap::new();
        for (k, raw_v, vtype) in rows {
            let eff_val = if decrypt && is_encrypted_value(&raw_v) {
                self.try_decrypt_or_empty(&raw_v)
            } else {
                raw_v
            };
            map.insert(k, cast_config_value(&eff_val, &vtype));
        }
        Ok(map)
    }

    pub fn get_config_rows(&self, mask_secrets: bool) -> Result<Vec<Value>> {
        let sensitive: HashSet<&str> = SENSITIVE_CONFIG_KEYS.iter().copied().collect();
        let rows: Vec<(String, String, String, String, String, String)> =
            self.with_conn(|conn| {
                let mut stmt = conn
                    .prepare(
                        "SELECT config_key, config_value, value_type, category, description, updated_at
                         FROM system_config
                         ORDER BY category ASC, config_key ASC;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let iter = stmt
                    .query_map([], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                            r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                            r.get::<_, Option<String>>(3)?.unwrap_or_default(),
                            r.get::<_, Option<String>>(4)?.unwrap_or_default(),
                            r.get::<_, Option<String>>(5)?.unwrap_or_default(),
                        ))
                    })
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(iter.flatten().collect())
            })?;

        let mut out = Vec::with_capacity(rows.len());
        for (key, raw_val, vtype, category, description, updated_at) in rows {
            let display_val = if sensitive.contains(key.as_str()) && !raw_val.is_empty() {
                let plain = if is_encrypted_value(&raw_val) {
                    self.try_decrypt_or_empty(&raw_val)
                } else {
                    raw_val
                };
                if mask_secrets {
                    mask_secret(&plain)
                } else {
                    plain
                }
            } else {
                raw_val
            };
            out.push(json!({
                "config_key": key,
                "config_value": display_val,
                "value_type": vtype,
                "category": category,
                "description": description,
                "updated_at": updated_at,
            }));
        }
        Ok(out)
    }

    pub fn set_config_value(&self, key: &str, value: &Value) -> Result<bool> {
        let val_str = match value {
            Value::String(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            Value::Null => {
                return Err(AegisError::Validation(format!(
                    "Invalid null value for '{key}'"
                )))
            }
            other => other.to_string(),
        };

        // Route credential keys through validation
        match key {
            "wallet_address" => {
                self.configure_hyperliquid_credentials(
                    Some(&val_str),
                    None,
                    None,
                    None,
                    None,
                    None,
                    false,
                )?;
                return Ok(true);
            }
            "private_key" => {
                self.configure_hyperliquid_credentials(
                    None,
                    Some(&val_str),
                    None,
                    None,
                    None,
                    None,
                    false,
                )?;
                return Ok(true);
            }
            "hyperliquid_vault_address" => {
                self.configure_hyperliquid_credentials(
                    None,
                    None,
                    None,
                    None,
                    Some(&val_str),
                    None,
                    false,
                )?;
                return Ok(true);
            }
            "gemini_api_key" => {
                self.configure_hyperliquid_credentials(
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(&val_str),
                    false,
                )?;
                return Ok(true);
            }
            "hyperliquid_testnet" => {
                let b = match value {
                    Value::Bool(b) => *b,
                    Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                        "true" | "1" | "yes" | "on" | "testnet" => true,
                        "false" | "0" | "no" | "off" | "mainnet" => false,
                        _ => {
                            return Err(AegisError::Validation(format!(
                                "Invalid hyperliquid_testnet value '{s}': use true or false."
                            )))
                        }
                    },
                    _ => {
                        return Err(AegisError::Validation(
                            "Invalid hyperliquid_testnet value: use true or false.".to_string(),
                        ))
                    }
                };
                self.configure_hyperliquid_credentials(
                    None,
                    None,
                    Some(b),
                    None,
                    None,
                    None,
                    false,
                )?;
                return Ok(true);
            }
            "execution_mode" => {
                let req_creds = self.get_schema_version().unwrap_or(0) < 4;
                self.configure_hyperliquid_credentials(
                    None,
                    None,
                    None,
                    Some(&val_str),
                    None,
                    None,
                    req_creds,
                )?;
                return Ok(true);
            }
            _ => {}
        }

        let defaults = alert_config_defaults();
        let vtype_opt: Option<String> = self.with_conn(|conn| {
            let mut row: Option<String> = conn
                .query_row(
                    "SELECT value_type FROM system_config WHERE config_key = ?1;",
                    params![key],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            if row.is_none() {
                if let Some((def_val, def_type, def_cat, def_desc)) = defaults.get(key) {
                    let _ = conn.execute(
                        "INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description)
                         VALUES (?1, ?2, ?3, ?4, ?5);",
                        params![key, def_val, def_type, def_cat, def_desc],
                    );
                    row = Some((*def_type).to_string());
                }
            }
            Ok(row)
        })?;

        let Some(raw_vtype) = vtype_opt else {
            return Ok(false);
        };
        let vtype = raw_vtype.trim().to_ascii_lowercase();
        let str_val = match vtype.as_str() {
            "bool" => match value {
                Value::Bool(b) => {
                    if *b {
                        "true".to_string()
                    } else {
                        "false".to_string()
                    }
                }
                Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                    "true" | "1" | "yes" | "on" => "true".to_string(),
                    "false" | "0" | "no" | "off" => "false".to_string(),
                    _ => {
                        return Err(AegisError::Validation(format!(
                            "Invalid boolean value for '{key}': {value}"
                        )))
                    }
                },
                _ => {
                    return Err(AegisError::Validation(format!(
                        "Invalid boolean value for '{key}': {value}"
                    )))
                }
            },
            "int" => {
                if value.is_boolean() {
                    return Err(AegisError::Validation(format!(
                        "Invalid integer value for '{key}': {value}"
                    )));
                }
                let f = val_str.trim().parse::<f64>().map_err(|_| {
                    AegisError::Validation(format!("Invalid integer value for '{key}': {value}"))
                })?;
                if !f.is_finite() || f.fract() != 0.0 {
                    return Err(AegisError::Validation(format!(
                        "Invalid integer value for '{key}': {value}"
                    )));
                }
                let int_val = f as i64;
                if key == "polling_interval_seconds" && int_val < 1 {
                    return Err(AegisError::Validation(
                        "Invalid polling_interval_seconds: must be >= 1 second.".to_string(),
                    ));
                }
                int_val.to_string()
            }
            "float" => {
                if value.is_boolean() {
                    return Err(AegisError::Validation(format!(
                        "Invalid float value for '{key}': {value}"
                    )));
                }
                let f = val_str.trim().parse::<f64>().map_err(|_| {
                    AegisError::Validation(format!("Invalid float value for '{key}': {value}"))
                })?;
                if !f.is_finite() {
                    return Err(AegisError::Validation(format!(
                        "Invalid float value for '{key}': {value}"
                    )));
                }
                if key == "max_daily_drawdown_alert_pct" && (f <= 0.0 || f >= 1.0) {
                    return Err(AegisError::Validation(
                        "Invalid max_daily_drawdown_alert_pct: must be a fraction in (0.0, 1.0), e.g. 0.03 for 3%."
                            .to_string(),
                    ));
                }
                if key == "initial_equity_usdc" && f <= 0.0 {
                    return Err(AegisError::Validation(
                        "Invalid initial_equity_usdc: must be > 0.0.".to_string(),
                    ));
                }
                f.to_string()
            }
            _ => {
                let s = val_str.trim().to_string();
                if s.is_empty() && !defaults.contains_key(key) {
                    return Err(AegisError::Validation(format!(
                        "Invalid empty string value for '{key}'"
                    )));
                }
                s
            }
        };

        let is_sensitive = SENSITIVE_CONFIG_KEYS.contains(&key);
        let stored_val = if is_sensitive && !str_val.is_empty() {
            self.encrypt_config_value(&str_val)?
        } else {
            str_val.clone()
        };

        self.with_conn(|conn| {
            let updated = conn
                .execute(
                    "UPDATE system_config
                     SET config_value = ?1,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                     WHERE config_key = ?2;",
                    params![stored_val, key],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;

            if key == "initial_equity_usdc" && updated > 0 {
                if let Ok(new_base_eq) = str_val.parse::<f64>() {
                    let trade_cnt: i64 = conn
                        .query_row("SELECT COUNT(*) FROM trades;", [], |r| r.get(0))
                        .unwrap_or(0);
                    if trade_cnt == 0 {
                        let _ = conn.execute(
                            "UPDATE portfolio_snapshots
                             SET total_equity_usdc = ?1,
                                 available_cash_usdc = ?2,
                                 accrued_yield_usdc = 0.0,
                                 basis_harvest_notional_usdc = 0.0
                             WHERE deployed_notional_usdc = 0.0;",
                            params![new_base_eq, new_base_eq],
                        );
                    }
                }
            }
            Ok(updated > 0)
        })
    }

    pub fn scale_risk_caps_for_equity(&self, initial_equity_usdc: f64) -> Result<HashMap<String, f64>> {
        if !initial_equity_usdc.is_finite() || initial_equity_usdc <= 0.0 {
            return Err(AegisError::Validation(format!(
                "Invalid initial_equity_usdc '{initial_equity_usdc}': must be > 0.0."
            )));
        }
        let cfg = self.load_config_opts(false)?;
        let heat_pct = cfg.get_f64("max_portfolio_heat_pct", 0.04);
        let single_pct = cfg.get_f64("max_single_asset_pct", 0.25);
        let gross_lev = cfg.get_f64("max_gross_leverage", 1.5);
        let scale_min = cfg.get_f64("conviction_scale_min", 0.5);
        let scale_max = cfg.get_f64("conviction_scale_max", 1.5);
        let baseline_risk = round_dp(initial_equity_usdc * 0.01, 2);

        let pairs = [
            ("initial_equity_usdc", round_dp(initial_equity_usdc, 2)),
            ("baseline_risk_per_trade_usdc", baseline_risk),
            ("min_risk_per_trade_usdc", round_dp(baseline_risk * scale_min, 2)),
            ("max_risk_per_trade_usdc", round_dp(baseline_risk * scale_max, 2)),
            ("max_portfolio_heat_usdc", round_dp(initial_equity_usdc * heat_pct, 2)),
            ("max_single_asset_notional_usdc", round_dp(initial_equity_usdc * single_pct, 2)),
            ("max_gross_leverage_notional_usdc", round_dp(initial_equity_usdc * gross_lev, 2)),
        ];

        let mut out = HashMap::new();
        for (k, v) in pairs {
            self.set_config_value(k, &json!(v))?;
            out.insert(k.to_string(), v);
        }
        Ok(out)
    }

    pub fn set_config_batch(
        &self,
        updates: &HashMap<String, Value>,
        scale_risk_caps: bool,
    ) -> Result<Vec<String>> {
        let mut updated_keys = Vec::new();
        for (k, v) in updates {
            let ok = self.set_config_value(k, v)?;
            if !ok {
                return Err(AegisError::NotFound(format!("Unknown config_key: {k}")));
            }
            updated_keys.push(k.clone());
        }
        if scale_risk_caps {
            if let Some(eq_val) = updates.get("initial_equity_usdc") {
                let eq = eq_val
                    .as_f64()
                    .or_else(|| eq_val.as_str().and_then(|s| s.parse::<f64>().ok()))
                    .unwrap_or(10000.0);
                let scaled = self.scale_risk_caps_for_equity(eq)?;
                for sk in scaled.keys() {
                    if !updated_keys.contains(sk) {
                        updated_keys.push(sk.clone());
                    }
                }
            }
        }
        Ok(updated_keys)
    }

    pub fn get_credentials_status(&self) -> Result<Value> {
        let cfg = self.load_config()?;
        let wallet = cfg.get_str("wallet_address", "");
        let priv_key = self.try_decrypt_or_empty(&cfg.get_str("private_key", ""));
        let vault = cfg.get_str("hyperliquid_vault_address", "");
        let gemini_key = self.try_decrypt_or_empty(&cfg.get_str("gemini_api_key", ""));
        let tg_token = self.try_decrypt_or_empty(&cfg.get_str("telegram_bot_token", ""));
        let tg_chat_id = cfg.get_str("telegram_chat_id", "");
        let dd_alert_pct = cfg.get_f64("max_daily_drawdown_alert_pct", 0.03);

        let raw_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || raw_mode == "TESTNET";

        let wallet_valid = !wallet.is_empty() && validate_evm_address(&wallet, "wallet_address").is_ok();
        let priv_valid = !priv_key.is_empty() && validate_private_key(&priv_key).is_ok();
        let credentials_ready = wallet_valid && priv_valid;

        let network = if is_testnet { "testnet" } else { "mainnet" };
        let api_endpoint = if is_testnet {
            "https://api.hyperliquid-testnet.xyz"
        } else {
            "https://api.hyperliquid.xyz"
        };
        let active_routing = if (raw_mode == "LIVE" || raw_mode == "TESTNET") && credentials_ready {
            if is_testnet {
                "LIVE_TESTNET"
            } else {
                "LIVE_MAINNET"
            }
        } else {
            "PAPER_SIMULATION"
        };

        Ok(json!({
            "exchange_id": cfg.get_str("exchange_id", "hyperliquid"),
            "environment": cfg.get_str("environment", "sandbox"),
            "execution_mode": raw_mode,
            "hyperliquid_testnet": is_testnet,
            "network": network,
            "api_endpoint": api_endpoint,
            "wallet_address": wallet,
            "wallet_configured": wallet_valid,
            "private_key_masked": mask_secret(&priv_key),
            "private_key_configured": priv_valid,
            "hyperliquid_vault_address": vault,
            "gemini_api_key_masked": mask_secret(&gemini_key),
            "gemini_api_key_configured": !gemini_key.is_empty(),
            "credentials_ready": credentials_ready,
            "active_routing": active_routing,
            "alert_channel": "TELEGRAM",
            "telegram_bot_token_masked": mask_secret(&tg_token),
            "telegram_bot_token_configured": !tg_token.is_empty(),
            "telegram_chat_id": tg_chat_id,
            "telegram_alerts_ready": !tg_token.is_empty() && !tg_chat_id.is_empty(),
            "max_daily_drawdown_alert_pct": dd_alert_pct,
        }))
    }

    pub fn configure_telegram_alerts(
        &self,
        telegram_bot_token: Option<&str>,
        telegram_chat_id: Option<&str>,
        max_daily_drawdown_alert_pct: Option<f64>,
    ) -> Result<Value> {
        if let Some(tok) = telegram_bot_token {
            let plain = if is_encrypted_value(tok) {
                self.decrypt_config_value(tok)?
            } else {
                tok.trim().to_string()
            };
            self.set_config_value("telegram_bot_token", &json!(plain))?;
        }
        if let Some(cid) = telegram_chat_id {
            self.set_config_value("telegram_chat_id", &json!(cid.trim()))?;
        }
        if let Some(pct) = max_daily_drawdown_alert_pct {
            if !pct.is_finite() || pct <= 0.0 || pct >= 1.0 {
                return Err(AegisError::Validation(
                    "Invalid max_daily_drawdown_alert_pct: must be a fraction in (0.0, 1.0), e.g. 0.03 for 3%."
                        .to_string(),
                ));
            }
            self.set_config_value("max_daily_drawdown_alert_pct", &json!(pct))?;
        }
        self.get_credentials_status()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn configure_hyperliquid_credentials(
        &self,
        wallet_address: Option<&str>,
        private_key: Option<&str>,
        hyperliquid_testnet: Option<bool>,
        execution_mode: Option<&str>,
        hyperliquid_vault_address: Option<&str>,
        gemini_api_key: Option<&str>,
        require_credentials_for_live: bool,
    ) -> Result<Value> {
        let mut updates: HashMap<&str, String> = HashMap::new();
        if let Some(w) = wallet_address {
            updates.insert("wallet_address", validate_evm_address(w, "wallet_address")?);
        }
        if let Some(pk) = private_key {
            let raw_pk = if is_encrypted_value(pk) {
                self.decrypt_config_value(pk)?
            } else {
                pk.to_string()
            };
            updates.insert("private_key", validate_private_key(&raw_pk)?);
        }
        if let Some(v) = hyperliquid_vault_address {
            updates.insert(
                "hyperliquid_vault_address",
                validate_evm_address(v, "hyperliquid_vault_address")?,
            );
        }
        if let Some(gk) = gemini_api_key {
            let raw_gk = if is_encrypted_value(gk) {
                self.decrypt_config_value(gk)?
            } else {
                gk.trim().to_string()
            };
            updates.insert("gemini_api_key", raw_gk);
        }
        if let Some(tn) = hyperliquid_testnet {
            updates.insert(
                "hyperliquid_testnet",
                if tn { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(mode_str) = execution_mode {
            let mode = ExecutionMode::parse(mode_str)?;
            if mode == ExecutionMode::Testnet {
                if updates.get("hyperliquid_testnet").map(String::as_str) == Some("false") {
                    return Err(AegisError::Validation(
                        "Conflicting configuration: execution_mode='TESTNET' cannot be combined with hyperliquid_testnet=False."
                            .to_string(),
                    ));
                }
                updates.insert("execution_mode", "LIVE".to_string());
                updates.insert("hyperliquid_testnet", "true".to_string());
            } else {
                updates.insert("execution_mode", mode.as_str().to_string());
            }
        }

        let current_cfg = self.load_config()?;
        let eff_wallet = updates
            .get("wallet_address")
            .cloned()
            .unwrap_or_else(|| current_cfg.get_str("wallet_address", ""));
        let eff_priv = updates
            .get("private_key")
            .cloned()
            .unwrap_or_else(|| current_cfg.get_str("private_key", ""));
        let eff_mode = updates
            .get("execution_mode")
            .cloned()
            .unwrap_or_else(|| current_cfg.get_str("execution_mode", "PAPER"));

        if require_credentials_for_live && (eff_mode == "LIVE" || eff_mode == "TESTNET") {
            let valid_w = !eff_wallet.is_empty()
                && validate_evm_address(&eff_wallet, "wallet_address").is_ok();
            let valid_p = !eff_priv.is_empty() && validate_private_key(&eff_priv).is_ok();
            if !(valid_w && valid_p) {
                return Err(AegisError::Validation(
                    "Cannot enable LIVE execution_mode without both a valid wallet_address and private_key configured."
                        .to_string(),
                ));
            }
        }

        for (k, v) in updates {
            let stored = if SENSITIVE_CONFIG_KEYS.contains(&k) && !v.is_empty() {
                self.encrypt_config_value(&v)?
            } else {
                v
            };
            self.with_conn(|conn| {
                conn.execute(
                    "UPDATE system_config
                     SET config_value = ?1,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                     WHERE config_key = ?2;",
                    params![stored, k],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(())
            })?;
        }
        self.get_credentials_status()
    }

    pub fn test_encryption(&self) -> Result<Value> {
        let sample_priv = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let sample_gemini = "AIzaSyAegisQuantFernetVerifiedKey020rc1";

        let prior_priv_raw = self.get_raw_config_value("private_key")?.unwrap_or_default();
        let prior_gemini_raw = self.get_raw_config_value("gemini_api_key")?.unwrap_or_default();

        let prior_priv_dec = self.try_decrypt_or_empty(&prior_priv_raw);
        let prior_gemini_dec = self.try_decrypt_or_empty(&prior_gemini_raw);

        let target_priv = if prior_priv_dec.is_empty() {
            sample_priv
        } else {
            &prior_priv_dec
        };
        let target_gemini = if prior_gemini_dec.is_empty() {
            sample_gemini
        } else {
            &prior_gemini_dec
        };

        self.set_config_value("private_key", &json!(target_priv))?;
        self.set_config_value("gemini_api_key", &json!(target_gemini))?;

        let raw_priv_on_disk = self.get_raw_config_value("private_key")?.unwrap_or_default();
        let raw_gemini_on_disk = self.get_raw_config_value("gemini_api_key")?.unwrap_or_default();

        let priv_enc_ok = is_encrypted_value(&raw_priv_on_disk) && raw_priv_on_disk != target_priv;
        let gemini_enc_ok =
            is_encrypted_value(&raw_gemini_on_disk) && raw_gemini_on_disk != target_gemini;

        let dec_cfg = self.load_config()?;
        let priv_dec_ok = dec_cfg.get_str("private_key", "") == target_priv;
        let gemini_dec_ok = dec_cfg.get_str("gemini_api_key", "") == target_gemini;

        if prior_priv_dec.is_empty() {
            self.set_config_value("private_key", &json!(""))?;
        }

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
            "schema_version": self.get_schema_version()?,
        });
        let _ = self.log_telemetry(
            "INFO",
            "engine.database",
            "Verified Fernet AES-128 configuration encryption at rest",
            Some(report.clone()),
        );
        Ok(report)
    }

    pub fn get_usdc_perp_universe(&self) -> Result<Vec<UniverseAsset>> {
        self.query_universe_view("SELECT * FROM v_universe_usdc_perp;")
    }

    pub fn get_btc_synth_universe(&self) -> Result<Vec<UniverseAsset>> {
        self.query_universe_view("SELECT * FROM v_universe_btc_synth;")
    }

    fn query_universe_view(&self, sql: &str) -> Result<Vec<UniverseAsset>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| {
                    Ok(UniverseAsset {
                        asset_id: r.get("asset_id")?,
                        symbol: r.get("symbol")?,
                        base_asset: r.get("base_asset")?,
                        quote_asset: r.get("quote_asset")?,
                        instrument_type: r.get("instrument_type")?,
                        tier: r.get("tier")?,
                        tier_name: r.get("tier_name")?,
                        exec_timeframe: r.get("exec_timeframe")?,
                        rs_timeframe: r.get("rs_timeframe")?,
                        tick_size: r.get("tick_size")?,
                        lot_step_size: r.get("lot_step_size")?,
                        min_notional_usdc: r.get("min_notional_usdc")?,
                        max_leverage: r.get("max_leverage")?,
                        is_active: r.get("is_active")?,
                        updated_at: r.get("updated_at")?,
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn get_alt_pairs_universe(&self) -> Result<Vec<AltPairMetadata>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT * FROM v_universe_alt_pairs;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| {
                    Ok(AltPairMetadata {
                        base_asset: r.get("base_asset")?,
                        tier: r.get("tier")?,
                        tier_name: r.get("tier_name")?,
                        exec_symbol: r.get("exec_symbol")?,
                        synth_symbol: r.get("synth_symbol")?,
                        exec_timeframe: r.get("exec_timeframe")?,
                        rs_timeframe: r.get("rs_timeframe")?,
                        tick_size: r.get("tick_size")?,
                        lot_step_size: r.get("lot_step_size")?,
                        min_notional_usdc: r.get("min_notional_usdc")?,
                        max_leverage: r.get("max_leverage")?,
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn update_asset_timeframes(
        &self,
        base_asset: &str,
        exec_timeframe: &str,
        rs_timeframe: &str,
    ) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE universe_assets
                 SET exec_timeframe = ?1,
                     rs_timeframe = ?2,
                     updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                 WHERE base_asset = ?3;",
                params![exec_timeframe, rs_timeframe, base_asset],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn upsert_ohlcv_batch(&self, bars: &[OhlcvBar]) -> Result<usize> {
        if bars.is_empty() {
            return Ok(0);
        }
        self.with_conn(|conn| {
            let tx = conn
                .unchecked_transaction()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            {
                let mut stmt = tx
                    .prepare_cached(
                        "INSERT INTO market_ohlcv (
                            symbol, timeframe, timestamp_ms, open, high, low, close,
                            volume, funding_rate_hourly, is_synthetic
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                        ON CONFLICT(symbol, timeframe, timestamp_ms) DO UPDATE SET
                            open = excluded.open,
                            high = excluded.high,
                            low = excluded.low,
                            close = excluded.close,
                            volume = excluded.volume,
                            funding_rate_hourly = excluded.funding_rate_hourly,
                            is_synthetic = excluded.is_synthetic;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                for b in bars {
                    stmt.execute(params![
                        b.symbol,
                        b.timeframe,
                        b.timestamp_ms,
                        b.open,
                        b.high,
                        b.low,
                        b.close,
                        b.volume,
                        b.funding_rate_hourly,
                        b.is_synthetic,
                    ])
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                }
            }
            tx.commit()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(bars.len())
        })
    }

    pub fn get_ohlcv_bars(
        &self,
        symbol: &str,
        timeframe: &str,
        limit: usize,
        max_timestamp_ms: Option<i64>,
    ) -> Result<Vec<OhlcvBar>> {
        let eff_max = max_timestamp_ms.or_else(|| self.get_pit_timestamp_ms());
        self.with_conn(|conn| {
            let mut bars: Vec<OhlcvBar> = if let Some(max_ts) = eff_max {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT timestamp_ms, open, high, low, close, volume,
                                funding_rate_hourly, is_synthetic
                         FROM market_ohlcv
                         WHERE symbol = ?1 AND timeframe = ?2 AND timestamp_ms <= ?3
                         ORDER BY timestamp_ms DESC
                         LIMIT ?4;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let rows = stmt
                    .query_map(params![symbol, timeframe, max_ts, limit as i64], |r| {
                        Ok(OhlcvBar {
                            symbol: symbol.to_string(),
                            timeframe: timeframe.to_string(),
                            timestamp_ms: r.get(0)?,
                            open: r.get(1)?,
                            high: r.get(2)?,
                            low: r.get(3)?,
                            close: r.get(4)?,
                            volume: r.get(5)?,
                            funding_rate_hourly: r.get(6)?,
                            is_synthetic: r.get(7)?,
                        })
                    })
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                rows.flatten().collect()
            } else {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT timestamp_ms, open, high, low, close, volume,
                                funding_rate_hourly, is_synthetic
                         FROM market_ohlcv
                         WHERE symbol = ?1 AND timeframe = ?2
                         ORDER BY timestamp_ms DESC
                         LIMIT ?3;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let rows = stmt
                    .query_map(params![symbol, timeframe, limit as i64], |r| {
                        Ok(OhlcvBar {
                            symbol: symbol.to_string(),
                            timeframe: timeframe.to_string(),
                            timestamp_ms: r.get(0)?,
                            open: r.get(1)?,
                            high: r.get(2)?,
                            low: r.get(3)?,
                            close: r.get(4)?,
                            volume: r.get(5)?,
                            funding_rate_hourly: r.get(6)?,
                            is_synthetic: r.get(7)?,
                        })
                    })
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                rows.flatten().collect()
            };
            bars.reverse();
            Ok(bars)
        })
    }

    pub fn save_signal_evaluations(&self, evaluations: &[SignalEvaluation]) -> Result<()> {
        if evaluations.is_empty() {
            return Ok(());
        }
        self.with_conn(|conn| {
            let tx = conn
                .unchecked_transaction()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            {
                let mut del_stmt = tx
                    .prepare_cached(
                        "DELETE FROM signal_evaluations WHERE timestamp_ms = ?1 AND base_asset = ?2;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let mut stmt = tx
                    .prepare_cached(
                        "INSERT INTO signal_evaluations (
                            timestamp_ms, base_asset, exec_symbol, synth_symbol,
                            hmm_regime, prob_bull, prob_bear, prob_chop,
                            snr_score, mace_score, blended_conviction,
                            rs_rank, signal_action, funding_rate_hourly
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14);",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                for ev in evaluations {
                    del_stmt
                        .execute(params![ev.timestamp_ms, ev.base_asset])
                        .map_err(|e| AegisError::Database(e.to_string()))?;
                    stmt.execute(params![
                        ev.timestamp_ms,
                        ev.base_asset,
                        ev.exec_symbol,
                        ev.synth_symbol,
                        ev.hmm_regime,
                        ev.prob_bull,
                        ev.prob_bear,
                        ev.prob_chop,
                        ev.snr_score,
                        ev.mace_score,
                        ev.blended_conviction,
                        ev.rs_rank,
                        ev.signal_action,
                        ev.funding_rate_hourly,
                    ])
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                }
            }
            tx.commit()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn get_latest_signals(&self, limit: usize) -> Result<Vec<SignalEvaluation>> {
        self.with_conn(|conn| {
            let max_ts: Option<i64> = conn
                .query_row(
                    "SELECT MAX(timestamp_ms) FROM signal_evaluations;",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?
                .flatten();
            let Some(ts) = max_ts else {
                return Ok(Vec::new());
            };
            let mut stmt = conn
                .prepare(
                    "SELECT eval_id, timestamp_ms, base_asset, exec_symbol, synth_symbol,
                            hmm_regime, prob_bull, prob_bear, prob_chop,
                            snr_score, mace_score, blended_conviction,
                            rs_rank, signal_action, funding_rate_hourly, created_at
                     FROM signal_evaluations
                     WHERE eval_id IN (
                         SELECT MAX(eval_id)
                         FROM signal_evaluations
                         WHERE timestamp_ms = ?1
                         GROUP BY base_asset
                     )
                     ORDER BY rs_rank ASC
                     LIMIT ?2;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map(params![ts, limit as i64], |r| {
                    Ok(SignalEvaluation {
                        eval_id: Some(r.get(0)?),
                        timestamp_ms: r.get(1)?,
                        base_asset: r.get(2)?,
                        exec_symbol: r.get(3)?,
                        synth_symbol: r.get(4)?,
                        hmm_regime: r.get(5)?,
                        prob_bull: r.get(6)?,
                        prob_bear: r.get(7)?,
                        prob_chop: r.get(8)?,
                        snr_score: r.get(9)?,
                        snr_synth: r.get(9)?,
                        rs_96: 0.0,
                        r_usdc_8: 0.0,
                        mace_score: r.get(10)?,
                        blended_conviction: r.get(11)?,
                        rs_rank: r.get(12)?,
                        signal_action: r.get(13)?,
                        funding_rate_hourly: r.get(14)?,
                        created_at: r.get(15)?,
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    fn map_trade_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<TradeRecord> {
        let trade_id: i64 = r.get("trade_id")?;
        let ex_id: Option<String> = r.get("exchange_order_id").ok().flatten();
        let raw_cli: Option<String> = r.get("client_order_id").ok().flatten();
        let client_order_id = raw_cli
            .or_else(|| ex_id.clone())
            .unwrap_or_else(|| format!("trade-{trade_id}"));
        let entry_price: f64 = r.get("entry_price")?;
        let quantity: f64 = r.get("quantity")?;
        let init_stop: f64 = r.get("initial_stop_price")?;
        let chand_stop: f64 = r.get("chandelier_stop_price").unwrap_or(init_stop);
        let fsm_state: String = r
            .get::<_, Option<String>>("fsm_state")
            .ok()
            .flatten()
            .unwrap_or_else(|| "FILLED".to_string());
        let avg_fill_price: Option<f64> = r
            .get::<_, Option<f64>>("avg_fill_price")
            .ok()
            .flatten()
            .or(Some(entry_price));
        let filled_units: f64 = r
            .get::<_, Option<f64>>("filled_units")
            .ok()
            .flatten()
            .unwrap_or(quantity);
        let slippage_paid_usd: f64 = r
            .get::<_, Option<f64>>("slippage_paid_usd")
            .ok()
            .flatten()
            .unwrap_or(0.0);

        Ok(TradeRecord {
            trade_id,
            client_order_id,
            symbol: r.get("symbol")?,
            base_asset: r.get("base_asset")?,
            side: r.get("side")?,
            status: r.get("status")?,
            fsm_state,
            entry_price,
            current_price: r.get("current_price")?,
            exit_price: r.get("exit_price").ok().flatten(),
            quantity,
            filled_units,
            avg_fill_price,
            notional_usdc: r.get("notional_usdc")?,
            risk_budget_usdc: r.get("risk_budget_usdc")?,
            conviction_multiplier: r.get("conviction_multiplier")?,
            initial_stop_price: init_stop,
            chandelier_stop_price: chand_stop,
            current_stop_price: chand_stop,
            highest_price_seen: r.get("highest_price_seen")?,
            lowest_price_seen: r.get("lowest_price_seen")?,
            realized_pnl_usdc: r.get("realized_pnl_usdc")?,
            unrealized_pnl_usdc: r.get("unrealized_pnl_usdc")?,
            fees_paid_usdc: r.get("fees_paid_usdc")?,
            funding_accrued_usdc: r.get("funding_accrued_usdc")?,
            slippage_paid_usd,
            exchange_order_id: ex_id,
            opened_at: r.get("opened_at")?,
            closed_at: r.get("closed_at").ok().flatten(),
            exit_reason: r.get("exit_reason").ok().flatten(),
        })
    }

    pub fn get_open_trades(&self) -> Result<Vec<TradeRecord>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT * FROM trades WHERE status = 'OPEN' ORDER BY opened_at ASC;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], Self::map_trade_row)
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn get_recent_trades(&self, limit: usize) -> Result<Vec<TradeRecord>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT * FROM trades ORDER BY trade_id DESC LIMIT ?1;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map(params![limit as i64], Self::map_trade_row)
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn insert_trade(&self, trade: &TradeRecord) -> Result<i64> {
        let _ = OrderFsmState::parse(&trade.fsm_state)?;
        let cli_id = if trade.client_order_id.trim().is_empty() {
            format!("cli-{}", &Uuid::new_v4().simple().to_string()[..12])
        } else {
            trade.client_order_id.clone()
        };
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO trades (
                    client_order_id, symbol, base_asset, side, status, fsm_state,
                    entry_price, current_price, quantity, notional_usdc,
                    risk_budget_usdc, conviction_multiplier,
                    initial_stop_price, chandelier_stop_price,
                    highest_price_seen, lowest_price_seen,
                    fees_paid_usdc, funding_accrued_usdc, unrealized_pnl_usdc,
                    exchange_order_id, avg_fill_price, filled_units, slippage_paid_usd
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6,
                    ?7, ?8, ?9, ?10,
                    ?11, ?12,
                    ?13, ?14,
                    ?15, ?16,
                    ?17, ?18, ?19,
                    ?20, ?21, ?22, ?23
                );",
                params![
                    cli_id,
                    trade.symbol,
                    trade.base_asset,
                    trade.side,
                    trade.status,
                    trade.fsm_state,
                    trade.entry_price,
                    trade.current_price,
                    trade.quantity,
                    trade.notional_usdc,
                    trade.risk_budget_usdc,
                    trade.conviction_multiplier,
                    trade.initial_stop_price,
                    trade.chandelier_stop_price,
                    trade.highest_price_seen,
                    trade.lowest_price_seen,
                    trade.fees_paid_usdc,
                    trade.funding_accrued_usdc,
                    trade.unrealized_pnl_usdc,
                    trade.exchange_order_id,
                    trade.avg_fill_price,
                    trade.filled_units,
                    trade.slippage_paid_usd,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(conn.last_insert_rowid())
        })
    }

    pub fn log_order_state_transition(
        &self,
        client_order_id: &str,
        from_state: &str,
        to_state: &str,
        reason: Option<&str>,
    ) -> Result<i64> {
        let ts = self.now_ms();
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO order_state_audit (client_order_id, from_state, to_state, timestamp_utc, reason)
                 VALUES (?1, ?2, ?3, ?4, ?5);",
                params![client_order_id, from_state, to_state, ts, reason],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(conn.last_insert_rowid())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn transition_order_state(
        &self,
        client_order_id: &str,
        to_state: &str,
        from_state: Option<&str>,
        reason: Option<&str>,
        exchange_order_id: Option<&str>,
        avg_fill_price: Option<f64>,
        filled_units: Option<f64>,
        slippage_paid_usd: Option<f64>,
        fees_paid_usdc: Option<f64>,
        status: Option<&str>,
    ) -> Result<()> {
        let target_fsm = OrderFsmState::parse(to_state)?;
        let ts = self.now_ms();
        self.with_conn(|conn| {
            let existing: Option<(i64, String)> = conn
                .query_row(
                    "SELECT trade_id, COALESCE(fsm_state, 'PENDING')
                     FROM trades
                     WHERE client_order_id = ?1 OR exchange_order_id = ?1
                     ORDER BY trade_id DESC LIMIT 1;",
                    params![client_order_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;

            let prior_state = from_state
                .map(str::to_string)
                .or_else(|| existing.as_ref().map(|(_, s)| s.clone()))
                .unwrap_or_else(|| "PENDING".to_string());

            if let Some((trade_id, _)) = existing {
                conn.execute(
                    "UPDATE trades
                     SET fsm_state = ?1,
                         exchange_order_id = COALESCE(?2, exchange_order_id),
                         avg_fill_price = COALESCE(?3, avg_fill_price),
                         entry_price = COALESCE(?3, entry_price),
                         current_price = COALESCE(?3, current_price),
                         filled_units = COALESCE(?4, filled_units),
                         slippage_paid_usd = COALESCE(?5, slippage_paid_usd),
                         fees_paid_usdc = COALESCE(?6, fees_paid_usdc),
                         status = COALESCE(?7, status)
                     WHERE trade_id = ?8;",
                    params![
                        target_fsm.as_str(),
                        exchange_order_id,
                        avg_fill_price,
                        filled_units,
                        slippage_paid_usd,
                        fees_paid_usdc,
                        status,
                        trade_id,
                    ],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            }

            conn.execute(
                "INSERT INTO order_state_audit (client_order_id, from_state, to_state, timestamp_utc, reason)
                 VALUES (?1, ?2, ?3, ?4, ?5);",
                params![client_order_id, prior_state, target_fsm.as_str(), ts, reason],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn update_open_trade(
        &self,
        trade_id: i64,
        current_price: f64,
        chandelier_stop_price: f64,
        highest_price_seen: f64,
        lowest_price_seen: f64,
        unrealized_pnl_usdc: f64,
        funding_accrued_usdc: f64,
    ) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE trades
                 SET current_price = ?1,
                     chandelier_stop_price = ?2,
                     highest_price_seen = ?3,
                     lowest_price_seen = ?4,
                     unrealized_pnl_usdc = ?5,
                     funding_accrued_usdc = ?6
                 WHERE trade_id = ?7 AND status = 'OPEN';",
                params![
                    current_price,
                    chandelier_stop_price,
                    highest_price_seen,
                    lowest_price_seen,
                    unrealized_pnl_usdc,
                    funding_accrued_usdc,
                    trade_id,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn close_trade(
        &self,
        trade_id: i64,
        exit_price: f64,
        realized_pnl_usdc: f64,
        fees_paid_usdc: f64,
        exit_reason: &str,
    ) -> Result<()> {
        let closed_iso = chrono::DateTime::from_timestamp_millis(self.now_ms())
            .unwrap_or_else(Utc::now)
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        self.with_conn(|conn| {
            conn.execute(
                "UPDATE trades
                 SET status = 'CLOSED',
                     current_price = ?1,
                     exit_price = ?1,
                     realized_pnl_usdc = ?2,
                     unrealized_pnl_usdc = 0.0,
                     fees_paid_usdc = ?3,
                     closed_at = ?4,
                     exit_reason = ?5
                 WHERE trade_id = ?6;",
                params![
                    exit_price,
                    realized_pnl_usdc,
                    fees_paid_usdc,
                    closed_iso,
                    exit_reason,
                    trade_id
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn get_total_realized_pnl(&self) -> Result<f64> {
        self.with_conn(|conn| {
            let pnl: f64 = conn
                .query_row(
                    "SELECT COALESCE(SUM(realized_pnl_usdc), 0.0) FROM trades WHERE status = 'CLOSED';",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0.0);
            Ok(pnl)
        })
    }

    pub fn save_portfolio_snapshot(&self, snap: &PortfolioSnapshot) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO portfolio_snapshots (
                    timestamp_ms, total_equity_usdc, available_cash_usdc,
                    deployed_notional_usdc, gross_leverage,
                    portfolio_heat_usdc, portfolio_heat_pct,
                    basis_harvest_notional_usdc, accrued_yield_usdc,
                    hmm_regime, prob_bull, prob_bear, prob_chop,
                    open_long_count, open_short_count
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15);",
                params![
                    snap.timestamp_ms,
                    snap.total_equity_usdc,
                    snap.available_cash_usdc,
                    snap.deployed_notional_usdc,
                    snap.gross_leverage,
                    snap.portfolio_heat_usdc,
                    snap.portfolio_heat_pct,
                    snap.basis_harvest_notional_usdc,
                    snap.accrued_yield_usdc,
                    snap.hmm_regime,
                    snap.prob_bull,
                    snap.prob_bear,
                    snap.prob_chop,
                    snap.open_long_count,
                    snap.open_short_count,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn get_latest_portfolio_snapshot(&self) -> Result<PortfolioSnapshot> {
        self.with_conn(|conn| {
            let row = conn
                .query_row(
                    "SELECT snapshot_id, timestamp_ms, total_equity_usdc, available_cash_usdc,
                            deployed_notional_usdc, gross_leverage, portfolio_heat_usdc,
                            portfolio_heat_pct, basis_harvest_notional_usdc, accrued_yield_usdc,
                            hmm_regime, prob_bull, prob_bear, prob_chop,
                            open_long_count, open_short_count, created_at
                     FROM portfolio_snapshots
                     ORDER BY snapshot_id DESC LIMIT 1;",
                    [],
                    |r| {
                        Ok(PortfolioSnapshot {
                            snapshot_id: Some(r.get(0)?),
                            timestamp_ms: r.get(1)?,
                            total_equity_usdc: r.get(2)?,
                            available_cash_usdc: r.get(3)?,
                            deployed_notional_usdc: r.get(4)?,
                            gross_leverage: r.get(5)?,
                            portfolio_heat_usdc: r.get(6)?,
                            portfolio_heat_pct: r.get(7)?,
                            basis_harvest_notional_usdc: r.get(8)?,
                            accrued_yield_usdc: r.get(9)?,
                            hmm_regime: r.get(10)?,
                            prob_bull: r.get(11)?,
                            prob_bear: r.get(12)?,
                            prob_chop: r.get(13)?,
                            open_long_count: r.get(14)?,
                            open_short_count: r.get(15)?,
                            created_at: r.get(16).ok(),
                        })
                    },
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;

            if let Some(s) = row {
                return Ok(s);
            }
            let init_eq: f64 = conn
                .query_row(
                    "SELECT config_value FROM system_config WHERE config_key = 'initial_equity_usdc';",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(10000.0);

            Ok(PortfolioSnapshot {
                snapshot_id: None,
                timestamp_ms: self.now_ms(),
                total_equity_usdc: init_eq,
                available_cash_usdc: init_eq,
                deployed_notional_usdc: 0.0,
                gross_leverage: 0.0,
                portfolio_heat_usdc: 0.0,
                portfolio_heat_pct: 0.0,
                basis_harvest_notional_usdc: 0.0,
                accrued_yield_usdc: 0.0,
                hmm_regime: "CHOP".to_string(),
                prob_bull: 0.3333,
                prob_bear: 0.3333,
                prob_chop: 0.3334,
                open_long_count: 0,
                open_short_count: 0,
                created_at: None,
            })
        })
    }

    pub fn log_telemetry(
        &self,
        level: &str,
        component: &str,
        message: &str,
        details: Option<Value>,
    ) -> Result<()> {
        let details_str = details.map(|v| v.to_string());
        let ts = Utc::now().timestamp_millis();
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO telemetry_logs (timestamp_ms, level, component, message, details_json)
                 VALUES (?1, ?2, ?3, ?4, ?5);",
                params![ts, level.to_ascii_uppercase(), component, message, details_str],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn get_recent_logs(&self, limit: usize) -> Result<Vec<Value>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT log_id, timestamp_ms, level, component, message, details_json, created_at
                     FROM telemetry_logs
                     ORDER BY log_id DESC
                     LIMIT ?1;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map(params![limit as i64], |r| {
                    Ok(json!({
                        "log_id": r.get::<_, i64>(0)?,
                        "timestamp_ms": r.get::<_, i64>(1)?,
                        "level": r.get::<_, String>(2)?,
                        "component": r.get::<_, String>(3)?,
                        "message": r.get::<_, String>(4)?,
                        "details_json": r.get::<_, Option<String>>(5)?,
                        "created_at": r.get::<_, String>(6)?,
                    }))
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn reconcile_open_trade_size(
        &self,
        trade_id: i64,
        exchange_size: f64,
        exchange_entry_price: Option<f64>,
    ) -> Result<()> {
        let size = exchange_size.abs();
        self.with_conn(|conn| {
            if let Some(px) = exchange_entry_price.filter(|&p| p > 0.0) {
                let notional = size * px;
                conn.execute(
                    "UPDATE trades
                     SET quantity = ?1,
                         filled_units = ?1,
                         entry_price = ?2,
                         avg_fill_price = ?2,
                         notional_usdc = ?3
                     WHERE trade_id = ?4 AND status = 'OPEN';",
                    params![size, px, notional, trade_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            } else {
                conn.execute(
                    "UPDATE trades
                     SET quantity = ?1,
                         filled_units = ?1,
                         notional_usdc = ?1 * entry_price
                     WHERE trade_id = ?2 AND status = 'OPEN';",
                    params![size, trade_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            }
            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reconcile_on_chain_orphan_position(
        &self,
        symbol: &str,
        base_asset: &str,
        side: &str,
        quantity: f64,
        entry_price: f64,
        mark_price: f64,
        unrealized_pnl_usdc: f64,
    ) -> Result<i64> {
        let size = quantity.abs();
        let px = if entry_price > 0.0 {
            entry_price
        } else {
            mark_price
        };
        let notional = size * px;
        let stop_mult = if side == "LONG" { 0.91 } else { 1.09 };
        let default_stop = round_dp(px * stop_mult, 6);
        let cli_id = format!("reconciled-{}", &Uuid::new_v4().simple().to_string()[..12]);
        let ex_id = format!("onchain-reconciled-{base_asset}-{}", self.now_ms());

        let rec = TradeRecord {
            trade_id: 0,
            client_order_id: cli_id.clone(),
            symbol: symbol.to_string(),
            base_asset: base_asset.to_string(),
            side: side.to_string(),
            status: "OPEN".to_string(),
            fsm_state: "FILLED".to_string(),
            entry_price: px,
            current_price: if mark_price > 0.0 { mark_price } else { px },
            exit_price: None,
            quantity: size,
            filled_units: size,
            avg_fill_price: Some(px),
            notional_usdc: notional,
            risk_budget_usdc: round_dp((px - default_stop).abs() * size, 2),
            conviction_multiplier: 1.0,
            initial_stop_price: default_stop,
            chandelier_stop_price: default_stop,
            current_stop_price: default_stop,
            highest_price_seen: px.max(mark_price),
            lowest_price_seen: if mark_price > 0.0 {
                px.min(mark_price)
            } else {
                px
            },
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc,
            fees_paid_usdc: 0.0,
            funding_accrued_usdc: 0.0,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some(ex_id),
            opened_at: String::new(),
            closed_at: None,
            exit_reason: None,
        };
        let tid = self.insert_trade(&rec)?;
        let _ = self.log_order_state_transition(
            &cli_id,
            "PENDING",
            "FILLED",
            Some("ON_CHAIN_ORPHAN_RECONCILED"),
        );
        Ok(tid)
    }

    pub fn get_order_state_audit(
        &self,
        client_order_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        self.with_conn(|conn| {
            if let Some(cid) = client_order_id {
                let mut stmt = conn
                    .prepare(
                        "SELECT id, client_order_id, from_state, to_state, timestamp_utc, reason
                         FROM order_state_audit
                         WHERE client_order_id = ?1
                         ORDER BY id ASC
                         LIMIT ?2;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let iter = stmt
                    .query_map(params![cid, limit as i64], |r| {
                        Ok(json!({
                            "id": r.get::<_, i64>(0)?,
                            "client_order_id": r.get::<_, String>(1)?,
                            "from_state": r.get::<_, String>(2)?,
                            "to_state": r.get::<_, String>(3)?,
                            "timestamp_utc": r.get::<_, i64>(4)?,
                            "reason": r.get::<_, Option<String>>(5)?,
                        }))
                    })
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(iter.flatten().collect())
            } else {
                let mut stmt = conn
                    .prepare(
                        "SELECT id, client_order_id, from_state, to_state, timestamp_utc, reason
                         FROM order_state_audit
                         ORDER BY id DESC
                         LIMIT ?1;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let iter = stmt
                    .query_map(params![limit as i64], |r| {
                        Ok(json!({
                            "id": r.get::<_, i64>(0)?,
                            "client_order_id": r.get::<_, String>(1)?,
                            "from_state": r.get::<_, String>(2)?,
                            "to_state": r.get::<_, String>(3)?,
                            "timestamp_utc": r.get::<_, i64>(4)?,
                            "reason": r.get::<_, Option<String>>(5)?,
                        }))
                    })
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(iter.flatten().collect())
            }
        })
    }

    /// Creates an isolated `:memory:` SQLite database and copies `system_config` and `universe_assets`
    /// from `self` so backtests never mutate the live/paper WAL database.
    pub fn clone_to_memory(&self) -> Result<DatabaseManager> {
        let mem_db = Self::with_paths(":memory:", &self.schema_path, None);
        mem_db.initialize_schema(false)?;

        let cfg_rows: Vec<(String, String, String, String, String)> = self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT config_key, config_value, value_type, category, description FROM system_config;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(rows.flatten().collect())
        })?;

        mem_db.with_conn(|conn| {
            for (k, v, vt, cat, desc) in cfg_rows {
                let _ = conn.execute(
                    "INSERT INTO system_config (config_key, config_value, value_type, category, description)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(config_key) DO UPDATE SET
                        config_value = excluded.config_value,
                        value_type = excluded.value_type,
                        category = excluded.category,
                        description = excluded.description;",
                    params![k, v, vt, cat, desc],
                );
            }
            Ok(())
        })?;

        Ok(mem_db)
    }
}
