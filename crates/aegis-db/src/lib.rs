//! SQLite WAL connection manager, schema/migration runner, Fernet secret encryption,
//! and centralized variable loader (`system_config`) for Aegis Quant.

pub mod fernet;
pub mod pond_ops;
pub mod scoped;
pub mod trade_ops;

pub use scoped::{BookScopedDb, PitConfig, PondConfig};

use crate::fernet::{
    ensure_master_key_for_db, is_encrypted_value, is_legacy_public_fallback_key,
    is_usable_env_key, master_key_file_for_db, normalize_fernet_key, resolve_fernet_master_key,
    write_master_key_file, FernetCipher,
};
use aegis_core::{
    mask_secret, resolve_project_root, round_dp, validate_evm_address, validate_private_key,
    AegisError, ExecutionMode, PondDirectionPolicy, Result, IMMUTABLE_SECURITY_CONFIG_KEYS,
    SENSITIVE_CONFIG_KEYS,
};
use parking_lot::{Mutex, RwLock};
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
use zeroize::Zeroizing;

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

fn alert_config_defaults(
) -> HashMap<&'static str, (&'static str, &'static str, &'static str, &'static str)> {
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
        (
            "hyperliquid_info_url",
            (
                "",
                "string",
                "execution",
                "Optional override for Hyperliquid Mainnet /info endpoint URL",
            ),
        ),
        (
            "hyperliquid_testnet_info_url",
            (
                "",
                "string",
                "execution",
                "Optional override for Hyperliquid Testnet /info endpoint URL",
            ),
        ),
        (
            "hyperliquid_exchange_url",
            (
                "",
                "string",
                "execution",
                "Optional override for Hyperliquid Mainnet /exchange endpoint URL",
            ),
        ),
        (
            "hyperliquid_testnet_exchange_url",
            (
                "",
                "string",
                "execution",
                "Optional override for Hyperliquid Testnet /exchange endpoint URL",
            ),
        ),
        (
            "hyperliquid_mainnet_api_url",
            (
                "",
                "string",
                "execution",
                "Optional override for Hyperliquid Mainnet API base URL",
            ),
        ),
        (
            "hyperliquid_testnet_api_url",
            (
                "",
                "string",
                "execution",
                "Optional override for Hyperliquid Testnet API base URL",
            ),
        ),
        (
            "equity_baseline_reset_at",
            (
                "",
                "string",
                "risk",
                "ISO-8601 timestamp of last manual equity baseline reset",
            ),
        ),
    ])
}

/// Cached Fernet key and cipher state (`R5`).
#[derive(Clone)]
struct CachedFernetState {
    key_b64: String,
    is_explicit: bool,
    env_override: Option<String>,
    cipher: FernetCipher,
}

/// Thread-safe SQLite Write-Ahead Logging (WAL) and `:memory:` database manager.
#[derive(Clone)]
pub struct DatabaseManager {
    pub db_path: PathBuf,
    pub schema_path: PathBuf,
    is_memory: bool,
    memory_conn: Option<Arc<Mutex<Connection>>>,
    conn_pool: Arc<Mutex<Vec<Connection>>>,
    pit_timestamp_ms: Arc<AtomicI64>,
    explicit_master_key: Arc<Mutex<Option<String>>>,
    cipher_cache: Arc<RwLock<Option<CachedFernetState>>>,
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

    fn apply_configured_pragmas(conn: &Connection) {
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
        if !self.is_memory {
            let _ = ensure_master_key_for_db(&self.db_path);
        }
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
            "sql/migrations/005_v030_pp50_two_book.sql",
            "sql/migrations/006_v030_review_remediation.sql",
        ];
        for rel in migrations {
            let p = root.join(rel);
            if p.exists() {
                integrity = self.apply_migration(&p)?;
            }
        }
        self.reconcile_routing_config();
        let _ = self.redact_sensitive_telemetry_logs();
        let _ = self.with_conn(|conn| {
            Self::apply_configured_pragmas(conn);
            Ok(())
        });
        Ok(integrity)
    }

    pub fn initialize_schema(&self, force: bool) -> Result<Value> {
        if !self.is_memory {
            let _ = ensure_master_key_for_db(&self.db_path);
        }
        if !force {
            if let Ok(ver) = self.get_schema_version() {
                if ver >= 6 {
                    self.reconcile_routing_config();
                    return self.verify_integrity();
                }
            }
        } else {
            let _ = self.with_conn(|conn| {
                for table in [
                    "order_state_audit",
                    "trades",
                    "signal_evaluations",
                    "portfolio_snapshots",
                    "telemetry_logs",
                    "pond_cells",
                    "pond_scan_cycles",
                    "pond_champions",
                    "book_transfers",
                    "pp50_platform_snapshots",
                    "system_config",
                    "books",
                ] {
                    let _ = conn.execute(&format!("DELETE FROM {table};"), []);
                }
                let _ = conn.execute(
                    "DELETE FROM market_ohlcv
                     WHERE symbol NOT IN (SELECT symbol FROM universe_assets WHERE is_active = 1)
                        OR timeframe != '4h';",
                    [],
                );
                Ok(())
            });
        }
        let res = self.initialize_database()?;
        self.reconcile_routing_config();
        if force && !self.is_memory {
            self.conn_pool.lock().clear();
            let _ = self.with_conn(|conn| {
                let _ = conn.execute_batch("VACUUM; PRAGMA wal_checkpoint(TRUNCATE);");
                Ok(())
            });
        }
        Ok(res)
    }

    /// Reconciles legacy or inconsistent execution routing mode rows in `system_config`
    /// so that `execution_mode`, `hyperliquid_testnet`, `pond_execution_mode`, `environment`,
    /// and descriptions never contradict each other across `PAPER`, `TESTNET`, and `LIVE`.
    pub fn reconcile_routing_config(&self) {
        let _ = self.with_conn(|conn| {
            let mut stmt = match conn.prepare(
                "SELECT config_key, config_value, description
                 FROM system_config
                 WHERE config_key IN ('execution_mode', 'pond_execution_mode', 'hyperliquid_testnet', 'environment');",
            ) {
                Ok(s) => s,
                Err(_) => return Ok(()),
            };
            let rows: Vec<(String, String, String)> = match stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                    r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                ))
            }) {
                Ok(iter) => iter.flatten().collect(),
                Err(_) => return Ok(()),
            };
            if rows.is_empty() {
                return Ok(());
            }

            let mut cur_exec_mode: Option<String> = None;
            let mut cur_pond_mode: Option<String> = None;
            let mut cur_testnet: Option<String> = None;
            let mut cur_env: Option<String> = None;
            let mut cur_exec_desc: Option<String> = None;
            let mut cur_pond_desc: Option<String> = None;

            for (k, v, d) in rows {
                match k.as_str() {
                    "execution_mode" => {
                        cur_exec_mode = Some(v);
                        cur_exec_desc = Some(d);
                    }
                    "pond_execution_mode" => {
                        cur_pond_mode = Some(v);
                        cur_pond_desc = Some(d);
                    }
                    "hyperliquid_testnet" => {
                        cur_testnet = Some(v);
                    }
                    "environment" => {
                        cur_env = Some(v);
                    }
                    _ => {}
                }
            }

            let raw_mode = cur_exec_mode
                .as_deref()
                .unwrap_or("PAPER")
                .trim()
                .to_ascii_uppercase();
            let is_tn = matches!(
                cur_testnet
                    .as_deref()
                    .unwrap_or("true")
                    .trim()
                    .to_ascii_lowercase()
                    .as_str(),
                "true" | "1" | "yes" | "on" | "testnet"
            );

            let (target_mode, target_tn, target_env) =
                if raw_mode == "TESTNET" || (raw_mode == "LIVE" && is_tn) {
                    ("TESTNET", "true", "sandbox")
                } else if raw_mode == "LIVE" {
                    ("LIVE", "false", "production")
                } else {
                    ("PAPER", if is_tn { "true" } else { "false" }, "sandbox")
                };

            if cur_exec_mode.as_deref() != Some(target_mode) {
                let _ = conn.execute(
                    "UPDATE system_config
                     SET config_value = ?1,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                     WHERE config_key = 'execution_mode';",
                    params![target_mode],
                );
            }
            if let Some(ref pm) = cur_pond_mode {
                if pm != target_mode {
                    let _ = conn.execute(
                        "UPDATE system_config
                         SET config_value = ?1,
                             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                         WHERE config_key = 'pond_execution_mode';",
                        params![target_mode],
                    );
                }
            }
            if cur_testnet.as_deref() != Some(target_tn) {
                let _ = conn.execute(
                    "UPDATE system_config
                     SET config_value = ?1,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                     WHERE config_key = 'hyperliquid_testnet';",
                    params![target_tn],
                );
            }
            if let Some(ref env_val) = cur_env {
                if env_val != target_env {
                    let _ = conn.execute(
                        "UPDATE system_config
                         SET config_value = ?1,
                             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                         WHERE config_key = 'environment';",
                        params![target_env],
                    );
                }
            }

            let target_exec_desc =
                "Execution routing mode: PAPER, TESTNET, or LIVE Web3 DEX routing";
            if let Some(ref d) = cur_exec_desc {
                if d != target_exec_desc {
                    let _ = conn.execute(
                        "UPDATE system_config
                         SET description = ?1
                         WHERE config_key = 'execution_mode';",
                        params![target_exec_desc],
                    );
                }
            }
            let target_pond_desc =
                "Pond execution routing mode: PAPER, TESTNET, or LIVE Web3 DEX routing";
            if let Some(ref d) = cur_pond_desc {
                if d != target_pond_desc {
                    let _ = conn.execute(
                        "UPDATE system_config
                         SET description = ?1
                         WHERE config_key = 'pond_execution_mode';",
                        params![target_pond_desc],
                    );
                }
            }
            Ok(())
        });
    }

    pub fn run_migrations(&self) -> Result<Vec<String>> {
        let root = resolve_project_root();
        let migrations = [
            "sql/migrations/001_v020_config_expansion.sql",
            "sql/migrations/002_v020_order_fsm.sql",
            "sql/migrations/003_v020_schema_versioning.sql",
            "sql/migrations/004_v020_rc2_remediation.sql",
            "sql/migrations/005_v030_pp50_two_book.sql",
            "sql/migrations/006_v030_review_remediation.sql",
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
            if file_name.starts_with("003_")
                || file_name.starts_with("004_")
                || file_name.starts_with("005_")
                || file_name.starts_with("006_")
            {
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
                if file_name.starts_with("004_")
                    || file_name.starts_with("005_")
                    || file_name.starts_with("006_")
                {
                    let p3 = root.join("sql/migrations/003_v020_schema_versioning.sql");
                    if p3.exists() {
                        if let Ok(s) = fs::read_to_string(&p3) {
                            Self::execute_sql_idempotent(conn, &s)?;
                        }
                    }
                }
                if file_name.starts_with("005_") || file_name.starts_with("006_") {
                    let p4 = root.join("sql/migrations/004_v020_rc2_remediation.sql");
                    if p4.exists() {
                        if let Ok(s) = fs::read_to_string(&p4) {
                            Self::execute_sql_idempotent(conn, &s)?;
                        }
                    }
                }
                if file_name.starts_with("006_") {
                    let p5 = root.join("sql/migrations/005_v030_pp50_two_book.sql");
                    if p5.exists() {
                        if let Ok(s) = fs::read_to_string(&p5) {
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

        self.reconcile_routing_config();
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
            let pond_config_count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM system_config WHERE category IN ('pond', 'settling') OR config_key LIKE 'platform_pp50%' OR config_key = 'platform_settle_gate_enabled';",
                    [],
                    |r| r.get(0),
                )
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
            let books_count: i64 = conn
                .query_row("SELECT COUNT(*) FROM books;", [], |r| r.get(0))
                .unwrap_or(0);
            let pond_universe_count: i64 = conn
                .query_row("SELECT COUNT(*) FROM pond_universe;", [], |r| r.get(0))
                .unwrap_or(0);
            let pond_champions_count: i64 = conn
                .query_row("SELECT COUNT(*) FROM pond_champions;", [], |r| r.get(0))
                .unwrap_or(0);
            let pond_cells_open_count: i64 = conn
                .query_row("SELECT COUNT(*) FROM v_pond_cells_open;", [], |r| r.get(0))
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
                "pond_config_count": pond_config_count,
                "v_universe_usdc_perp_count": usdc_perp_count,
                "v_universe_btc_synth_count": btc_synth_count,
                "v_universe_alt_pairs_count": alt_pairs_count,
                "universe_assets_total": total_universe,
                "books_count": books_count,
                "pond_universe_count": pond_universe_count,
                "pond_champions_count": pond_champions_count,
                "pond_cells_open_count": pond_cells_open_count,
                "schema_version": schema_version,
            }))
        })
    }

    pub fn get_schema_version(&self) -> Result<i64> {
        let v = self.verify_integrity()?;
        Ok(v.get("schema_version").and_then(|x| x.as_i64()).unwrap_or(0))
    }

    fn resolve_cached_fernet_state(&self) -> Result<CachedFernetState> {
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

    fn get_fernet(&self) -> Result<FernetCipher> {
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
        self.reconcile_routing_config();
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

        let cipher_opt = if decrypt {
            self.resolve_cached_fernet_state().ok().map(|s| s.cipher)
        } else {
            None
        };

        let mut map = ConfigMap::new();
        for (k, raw_v, vtype) in rows {
            // S10: Never decrypt `private_key` into general-purpose `ConfigMap` clones;
            // signing callers must use `get_signing_private_key()` which returns `Zeroizing<String>`.
            let eff_val = if k == "private_key" {
                raw_v
            } else if decrypt && is_encrypted_value(&raw_v) {
                let s = raw_v.trim();
                let token = s.strip_prefix("ENC:").unwrap_or(s);
                if let Some(ref c) = cipher_opt {
                    c.decrypt(token).unwrap_or_default()
                } else {
                    self.try_decrypt_or_empty(&raw_v)
                }
            } else {
                raw_v
            };
            map.insert(k, cast_config_value(&eff_val, &vtype));
        }
        Ok(map)
    }

    pub fn get_config_rows(&self, mask_secrets: bool) -> Result<Vec<Value>> {
        self.reconcile_routing_config();
        let sensitive: HashSet<&str> = SENSITIVE_CONFIG_KEYS.iter().copied().collect();
        let immutable: HashSet<&str> = IMMUTABLE_SECURITY_CONFIG_KEYS.iter().copied().collect();
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
                "immutable": immutable.contains(key.as_str()),
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

        // Route credential and routing-mode keys through unified validation & synchronization
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
            "execution_mode" | "pond_execution_mode" => {
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
            "environment" => {
                let norm_env = val_str.trim().to_ascii_lowercase();
                if norm_env != "sandbox" && norm_env != "production" {
                    return Err(AegisError::Validation(format!(
                        "Invalid environment '{val_str}': must be 'sandbox' or 'production'."
                    )));
                }
                let cur_cfg = self.load_config_opts(false)?;
                let cur_mode = cur_cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
                let expected_env =
                    if cur_mode == "LIVE" && !cur_cfg.get_bool("hyperliquid_testnet", false) {
                        "production"
                    } else {
                        "sandbox"
                    };
                if norm_env != expected_env {
                    return Err(AegisError::Validation(format!(
                        "Conflicting configuration: environment='{norm_env}' conflicts with execution_mode='{cur_mode}' (expected '{expected_env}'). Change execution_mode instead."
                    )));
                }
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
                if key == "settling_split_pct" && (f <= 0.0 || f >= 1.0) {
                    return Err(AegisError::Validation(
                        "Invalid settling_split_pct: must be in (0.0, 1.0).".to_string(),
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
                if key == "pond_direction_policy" {
                    let pol = PondDirectionPolicy::parse(&s)?;
                    pol.as_str().to_string()
                } else if key == "environment" {
                    s.to_ascii_lowercase()
                } else {
                    s
                }
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
                                 available_cash_usdc = ROUND(MAX(0.0, ?1 - COALESCE(basis_harvest_notional_usdc, 0.0)), 2),
                                 accrued_yield_usdc = 0.0
                             WHERE deployed_notional_usdc = 0.0;",
                            params![new_base_eq],
                        );
                    }
                }
            }
            Ok(updated > 0)
        })
    }

    pub fn scale_risk_caps_for_equity(
        &self,
        initial_equity_usdc: f64,
    ) -> Result<HashMap<String, f64>> {
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

        let split_pct = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
        let pit_book_eq = round_dp(initial_equity_usdc * split_pct, 2);
        let baseline_risk = round_dp(pit_book_eq * 0.01, 2);

        let pond_book_eq = round_dp(initial_equity_usdc * (1.0 - split_pct), 2);
        let pond_heat_pct = cfg.get_f64("pond_max_heat_pct", 0.04);
        let pond_cell_pct = cfg.get_f64("pond_max_cell_pct", 0.25);
        let pond_scale_min = cfg.get_f64("pond_conviction_scale_min", 0.5);
        let pond_scale_max = cfg.get_f64("pond_conviction_scale_max", 1.5);
        let pond_baseline_risk = round_dp(pond_book_eq * 0.01, 2);

        let pairs = [
            ("initial_equity_usdc", round_dp(initial_equity_usdc, 2)),
            ("baseline_risk_per_trade_usdc", baseline_risk),
            ("min_risk_per_trade_usdc", round_dp(baseline_risk * scale_min, 2)),
            ("max_risk_per_trade_usdc", round_dp(baseline_risk * scale_max, 2)),
            ("max_portfolio_heat_usdc", round_dp(pit_book_eq * heat_pct, 2)),
            (
                "max_single_asset_notional_usdc",
                round_dp(pit_book_eq * single_pct, 2),
            ),
            (
                "max_gross_leverage_notional_usdc",
                round_dp(pit_book_eq * gross_lev, 2),
            ),
            ("pond_baseline_risk_usdc", pond_baseline_risk),
            (
                "pond_min_risk_usdc",
                round_dp(pond_baseline_risk * pond_scale_min, 2),
            ),
            (
                "pond_max_risk_usdc",
                round_dp(pond_baseline_risk * pond_scale_max, 2),
            ),
            ("pond_max_heat_usdc", round_dp(pond_book_eq * pond_heat_pct, 2)),
            (
                "pond_max_cell_notional_usdc",
                round_dp(pond_book_eq * pond_cell_pct, 2),
            ),
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
        let mut ordered_keys: Vec<&String> = updates.keys().collect();
        ordered_keys.sort_by(|a, b| {
            let prio = |k: &str| -> u8 {
                match k {
                    "wallet_address"
                    | "private_key"
                    | "hyperliquid_vault_address"
                    | "gemini_api_key" => 0,
                    "hyperliquid_testnet" => 2,
                    "pond_execution_mode" => 3,
                    "execution_mode" => 4,
                    "environment" => 5,
                    _ => 1,
                }
            };
            prio(a.as_str()).cmp(&prio(b.as_str())).then_with(|| a.cmp(b))
        });

        let mut updated_keys = Vec::new();
        for k in ordered_keys {
            let v = &updates[k];
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
        self.reconcile_routing_config();
        let cfg = self.load_config()?;
        let wallet = cfg.get_str("wallet_address", "");
        let priv_key = self.get_signing_private_key().unwrap_or_default();
        let vault = cfg.get_str("hyperliquid_vault_address", "");
        let gemini_key =
            Zeroizing::new(self.try_decrypt_or_empty(&cfg.get_str("gemini_api_key", "")));
        let tg_token =
            Zeroizing::new(self.try_decrypt_or_empty(&cfg.get_str("telegram_bot_token", "")));
        let tg_chat_id = cfg.get_str("telegram_chat_id", "");
        let dd_alert_pct = cfg.get_f64("max_daily_drawdown_alert_pct", 0.03);

        let raw_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let testnet_flag = cfg.get_bool("hyperliquid_testnet", false);
        let effective_mode = if raw_mode == "TESTNET" || (raw_mode == "LIVE" && testnet_flag) {
            "TESTNET".to_string()
        } else if raw_mode == "LIVE" {
            "LIVE".to_string()
        } else {
            "PAPER".to_string()
        };
        let is_testnet = match effective_mode.as_str() {
            "TESTNET" => true,
            "LIVE" => false,
            _ => testnet_flag,
        };

        let wallet_valid =
            !wallet.is_empty() && validate_evm_address(&wallet, "wallet_address").is_ok();
        let priv_valid = !priv_key.is_empty() && validate_private_key(&priv_key).is_ok();
        let credentials_ready = wallet_valid && priv_valid;

        let network = if is_testnet { "testnet" } else { "mainnet" };
        let api_endpoint = if is_testnet {
            let fallback = cfg.get_str(
                "hyperliquid_testnet_url",
                "https://api.hyperliquid-testnet.xyz",
            );
            let custom = cfg.get_str("hyperliquid_testnet_api_url", "");
            if custom.trim().is_empty() {
                fallback
            } else {
                custom.trim().to_string()
            }
        } else {
            let fallback = cfg.get_str("hyperliquid_mainnet_url", "https://api.hyperliquid.xyz");
            let custom = cfg.get_str("hyperliquid_mainnet_api_url", "");
            if custom.trim().is_empty() {
                fallback
            } else {
                custom.trim().to_string()
            }
        };
        let active_routing =
            if (effective_mode == "LIVE" || effective_mode == "TESTNET") && credentials_ready {
                if is_testnet {
                    "LIVE_TESTNET"
                } else {
                    "LIVE_MAINNET"
                }
            } else {
                "PAPER_SIMULATION"
            };

        let pond_mode = cfg.get_str("pond_execution_mode", &effective_mode);
        Ok(json!({
            "exchange_id": cfg.get_str("exchange_id", "hyperliquid"),
            "environment": cfg.get_str("environment", "sandbox"),
            "execution_mode": effective_mode,
            "pond_execution_mode": pond_mode,
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
            let plain = Zeroizing::new(if is_encrypted_value(tok) {
                self.decrypt_config_value(tok)?
            } else {
                tok.trim().to_string()
            });
            self.set_config_value("telegram_bot_token", &json!(&*plain))?;
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
            let raw_pk = Zeroizing::new(if is_encrypted_value(pk) {
                self.decrypt_config_value(pk)?
            } else {
                pk.to_string()
            });
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
            match mode {
                ExecutionMode::Testnet => {
                    if updates.get("hyperliquid_testnet").map(String::as_str) == Some("false") {
                        return Err(AegisError::Validation(
                            "Conflicting configuration: execution_mode='TESTNET' cannot be combined with hyperliquid_testnet=False."
                                .to_string(),
                        ));
                    }
                    updates.insert("execution_mode", "TESTNET".to_string());
                    updates.insert("hyperliquid_testnet", "true".to_string());
                    updates.insert("pond_execution_mode", "TESTNET".to_string());
                    updates.insert("environment", "sandbox".to_string());
                }
                ExecutionMode::Live => {
                    if updates.get("hyperliquid_testnet").map(String::as_str) == Some("true") {
                        updates.insert("execution_mode", "TESTNET".to_string());
                        updates.insert("hyperliquid_testnet", "true".to_string());
                        updates.insert("pond_execution_mode", "TESTNET".to_string());
                        updates.insert("environment", "sandbox".to_string());
                    } else {
                        updates.insert("execution_mode", "LIVE".to_string());
                        updates.insert("hyperliquid_testnet", "false".to_string());
                        updates.insert("pond_execution_mode", "LIVE".to_string());
                        updates.insert("environment", "production".to_string());
                    }
                }
                ExecutionMode::Paper => {
                    updates.insert("execution_mode", "PAPER".to_string());
                    updates.insert("pond_execution_mode", "PAPER".to_string());
                    updates.insert("environment", "sandbox".to_string());
                }
            }
        } else if let Some(tn) = hyperliquid_testnet {
            let cur_cfg = self.load_config_opts(false)?;
            let cur_mode = cur_cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
            if tn && cur_mode == "LIVE" {
                updates.insert("execution_mode", "TESTNET".to_string());
                updates.insert("pond_execution_mode", "TESTNET".to_string());
                updates.insert("environment", "sandbox".to_string());
            } else if !tn && cur_mode == "TESTNET" {
                updates.insert("execution_mode", "LIVE".to_string());
                updates.insert("pond_execution_mode", "LIVE".to_string());
                updates.insert("environment", "production".to_string());
            }
        }

        let current_cfg = self.load_config()?;
        let eff_wallet = updates
            .get("wallet_address")
            .cloned()
            .unwrap_or_else(|| current_cfg.get_str("wallet_address", ""));
        let eff_priv = match updates.get("private_key") {
            Some(pk) => Zeroizing::new(pk.clone()),
            None => self.get_signing_private_key().unwrap_or_default(),
        };
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

    const VALID_WALLET: &str = "0xef377c1d5444786236aa34fa72e5879372e0eaec";
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
}
