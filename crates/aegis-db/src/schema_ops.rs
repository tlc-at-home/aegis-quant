//! SQLite schema initialization, consolidated schema build runner (`sql/schema_and_seed.sql`),
//! routing mode reconciliation, cross-process advisory cycle lock, automated `VACUUM INTO` backups,
//! and database integrity verification.

use crate::fernet::ensure_master_key_for_db;
use crate::DatabaseManager;
use aegis_core::{resolve_project_root, AegisError, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Canonical unified schema version built by `sql/schema_and_seed.sql`.
pub const CANONICAL_SCHEMA_VERSION: i64 = 16;

/// Single canonical SQLite schema build & seed script replacing legacy incremental migrations.
pub const ALL_MIGRATION_FILES: &[&str] = &["sql/schema_and_seed.sql"];

/// RAII guard for the cross-process SQLite advisory cycle lock with background lease-renewal heartbeat (`HI-4`, `R18-L1`).
pub struct DbCycleLockGuard {
    db: DatabaseManager,
    owner: String,
    stop_flag: Arc<AtomicBool>,
    renew_handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for DbCycleLockGuard {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Release);
        if let Some(handle) = self.renew_handle.take() {
            handle.thread().unpark();
            let _ = handle.join();
        }
        let _ = self.db.release_cycle_lock(&self.owner);
    }
}

impl DatabaseManager {
    /// Repairs any half-completed table rebuilds left behind if a prior non-transactional
    /// migration crashed mid-rebuild (`R5-M1`).
    ///
    /// Specifically handles the two crash windows of the v9 `book_transfers` table rebuild:
    /// - Crash Window A (after creating and populating `book_transfers_v9`, before `DROP TABLE book_transfers`):
    ///   both `book_transfers` and `book_transfers_v9` exist -> drops the partial staging table `book_transfers_v9`.
    /// - Crash Window B (after `DROP TABLE book_transfers`, before `ALTER TABLE book_transfers_v9 RENAME TO book_transfers`):
    ///   only `book_transfers_v9` exists -> completes the rename to `book_transfers` and recreates `v_book_transfers_recent`.
    pub(crate) fn repair_interrupted_table_rebuilds(conn: &Connection) -> Result<()> {
        let has_main: bool = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='book_transfers';",
                [],
                |_| Ok(true),
            )
            .optional()
            .map_err(|e| AegisError::Database(e.to_string()))?
            .unwrap_or(false);
        let has_v9: bool = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='book_transfers_v9';",
                [],
                |_| Ok(true),
            )
            .optional()
            .map_err(|e| AegisError::Database(e.to_string()))?
            .unwrap_or(false);

        if has_v9 && !has_main {
            conn.execute_batch(
                "ALTER TABLE book_transfers_v9 RENAME TO book_transfers;
                 CREATE VIEW IF NOT EXISTS v_book_transfers_recent AS
                 SELECT * FROM book_transfers ORDER BY transfer_id DESC LIMIT 30;",
            )
            .map_err(|e| {
                AegisError::Database(format!("Failed to recover book_transfers_v9: {e}"))
            })?;
        } else if has_v9 && has_main {
            conn.execute_batch("DROP TABLE IF EXISTS book_transfers_v9;")
                .map_err(|e| {
                    AegisError::Database(format!("Failed to drop stale book_transfers_v9: {e}"))
                })?;
        }
        Self::ensure_trades_pending_status_check(conn)?;
        Ok(())
    }

    /// Execute a multi-statement migration script atomically inside a single SQLite transaction (`R5-M1`).
    /// If any statement fails (other than idempotent `duplicate column name` on `ALTER TABLE ADD COLUMN`),
    /// the entire migration script rolls back cleanly.
    pub(crate) fn execute_sql_transactional(conn: &Connection, sql_script: &str) -> Result<()> {
        Self::repair_interrupted_table_rebuilds(conn)?;
        let began_tx = conn.is_autocommit();
        if began_tx {
            conn.execute_batch("BEGIN IMMEDIATE;")
                .map_err(|e| AegisError::Database(format!("Failed to begin migration tx: {e}")))?;
        }
        match Self::execute_sql_idempotent(conn, sql_script) {
            Ok(()) => {
                if began_tx {
                    conn.execute_batch("COMMIT;").map_err(|e| {
                        let _ = conn.execute_batch("ROLLBACK;");
                        AegisError::Database(format!("Failed to commit migration tx: {e}"))
                    })?;
                }
                Ok(())
            }
            Err(e) => {
                if began_tx {
                    let _ = conn.execute_batch("ROLLBACK;");
                }
                Err(e)
            }
        }
    }

    /// Execute multi-statement SQL idempotently, ignoring duplicate column errors on `ALTER TABLE`.
    pub(crate) fn execute_sql_idempotent(conn: &Connection, sql_script: &str) -> Result<()> {
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
                let lower_clean = clean.to_ascii_lowercase();
                if !conn.is_autocommit()
                    && (lower_clean.starts_with("pragma journal_mode")
                        || lower_clean.starts_with("pragma synchronous"))
                {
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
                let lower_rem = rem.to_ascii_lowercase();
                if conn.is_autocommit()
                    || (!lower_rem.starts_with("pragma journal_mode")
                        && !lower_rem.starts_with("pragma synchronous"))
                {
                    if let Err(e) = conn.execute_batch(rem) {
                        if !e
                            .to_string()
                            .to_ascii_lowercase()
                            .contains("duplicate column name")
                        {
                            return Err(AegisError::Database(format!("SQL error: {e}")));
                        }
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
        let sql_script = fs::read_to_string(&self.schema_path)?;
        let is_mem = self.is_memory;
        self.with_conn(|conn| {
            let _ = conn.execute_batch("PRAGMA busy_timeout = 15000; PRAGMA foreign_keys = ON;");
            if !is_mem {
                let _ =
                    conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;");
            }
            Self::execute_sql_transactional(conn, &sql_script)
                .map_err(|e| AegisError::Database(format!("Failed to init_db: {e}")))?;
            Ok(())
        })?;
        self.verify_integrity()
    }

    pub fn initialize_database(&self) -> Result<Value> {
        let integrity = self.init_db()?;
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
                if ver >= CANONICAL_SCHEMA_VERSION {
                    let _ = self.with_conn(Self::repair_interrupted_table_rebuilds);
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
                    "pond_universe",
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
    ///
    /// Invariant (`R7-B1`, `R7-QA`):
    /// This function is invoked along the `load_config()` read path and MUST NEVER read or
    /// mutate any table other than `system_config`, nor overwrite operator-calibrated risk/stop keys.
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

            // Consolidate to a single `hyperliquid_vault_address` key and auto-switch standard HLP vault per network
            let has_legacy_vault_keys: bool = conn
                .query_row(
                    "SELECT 1 FROM system_config WHERE config_key IN (
                        'hyperliquid_default_vault_mainnet',
                        'hyperliquid_default_vault_testnet',
                        'hyperliquid_mainnet_vault_address',
                        'hyperliquid_testnet_vault_address'
                    ) LIMIT 1;",
                    [],
                    |_| Ok(true),
                )
                .optional()
                .ok()
                .flatten()
                .unwrap_or(false);
            if has_legacy_vault_keys {
                let _ = conn.execute(
                    "DELETE FROM system_config WHERE config_key IN (
                        'hyperliquid_default_vault_mainnet',
                        'hyperliquid_default_vault_testnet',
                        'hyperliquid_mainnet_vault_address',
                        'hyperliquid_testnet_vault_address'
                    );",
                    [],
                );
            }
            let cur_vault: Option<String> = conn
                .query_row(
                    "SELECT config_value FROM system_config WHERE config_key = 'hyperliquid_vault_address';",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .ok()
                .flatten();
            let target_vault = if target_tn == "true" {
                aegis_core::DEFAULT_TESTNET_HLP_VAULT_ADDRESS
            } else {
                aegis_core::DEFAULT_MAINNET_HLP_VAULT_ADDRESS
            };
            let vault_trimmed = cur_vault.as_deref().unwrap_or("").trim();
            if (vault_trimmed.is_empty()
                || vault_trimmed.eq_ignore_ascii_case(aegis_core::DEFAULT_TESTNET_HLP_VAULT_ADDRESS)
                || vault_trimmed.eq_ignore_ascii_case(aegis_core::DEFAULT_MAINNET_HLP_VAULT_ADDRESS))
                && vault_trimmed != target_vault
            {
                let _ = conn.execute(
                    "INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
                     VALUES ('hyperliquid_vault_address', ?1, 'string', 'credentials', 'Optional Hyperliquid sub-account or HLP vault address (auto-switches Mainnet/Testnet)', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                     ON CONFLICT(config_key) DO UPDATE SET
                        config_value = excluded.config_value,
                        updated_at = excluded.updated_at;",
                    params![target_vault],
                );
            }
            Ok(())
        });
    }

    pub fn run_migrations(&self) -> Result<Vec<String>> {
        let cur_ver = self.get_schema_version().unwrap_or(0).max(0);
        if cur_ver < CANONICAL_SCHEMA_VERSION {
            self.apply_migration("sql/schema_and_seed.sql")?;
            Ok(vec!["sql/schema_and_seed.sql".to_string()])
        } else {
            Ok(Vec::new())
        }
    }

    pub fn apply_migration(&self, migration_path: impl AsRef<Path>) -> Result<Value> {
        let root = resolve_project_root();
        let req_path = migration_path.as_ref().to_path_buf();
        let req_file_name = req_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("schema_and_seed.sql")
            .to_string();
        let mig_path = if req_path.exists() {
            req_path.clone()
        } else if root.join(&req_path).exists() {
            root.join(&req_path)
        } else if (req_file_name == "schema_and_seed.sql"
            || req_path.to_string_lossy().contains("sql/migrations/"))
            && self.schema_path.exists()
        {
            if req_path.to_string_lossy().contains("sql/migrations/")
                || req_file_name != "schema_and_seed.sql"
            {
                tracing::warn!(
                    legacy_path = %req_path.display(),
                    redirected_to = %self.schema_path.display(),
                    "Deprecated migration path passed to apply_migration; redirecting to canonical sql/schema_and_seed.sql (R15-QA(a))"
                );
            }
            self.schema_path.clone()
        } else {
            return Err(AegisError::NotFound(format!(
                "Migration SQL file not found: {}",
                migration_path.as_ref().display()
            )));
        };

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

        let file_name = req_file_name;
        let sql_script = fs::read_to_string(&mig_path)?;

        let mut actually_executed = false;
        self.with_conn(|conn| {
            let target_num = file_name
                .get(..3)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
            let effective_target = if target_num > 0 {
                target_num
            } else {
                CANONICAL_SCHEMA_VERSION as usize
            };
            let has_mig_table_before: bool = conn
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations';",
                    [],
                    |_| Ok(true),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?
                .unwrap_or(false);
            let max_applied_ver: usize = if has_mig_table_before {
                conn.query_row(
                    "SELECT COALESCE(MAX(version), 0) FROM schema_migrations;",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap_or(0)
                .max(0) as usize
            } else {
                0
            };

            if target_num == 0 && max_applied_ver < CANONICAL_SCHEMA_VERSION as usize {
                Self::execute_sql_transactional(conn, &sql_script)?;
                actually_executed = true;
            } else if target_num > max_applied_ver {
                Self::execute_sql_transactional(conn, &sql_script)?;
                actually_executed = true;
                if target_num < CANONICAL_SCHEMA_VERSION as usize {
                    let _ = conn.execute(
                        "DELETE FROM schema_migrations WHERE version > ?1;",
                        params![target_num as i64],
                    );
                    let _ = conn.execute(
                        "UPDATE system_config
                         SET config_value = ?1,
                             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                         WHERE config_key = 'schema_version';",
                        params![target_num.to_string()],
                    );
                }
            } else {
                Self::repair_interrupted_table_rebuilds(conn)?;
            }

            let has_mig_table: Option<String> = conn
                .query_row(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations';",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;

            if has_mig_table.is_some() && target_num > 0 {
                for v in (max_applied_ver + 1)..=effective_target {
                    let _ = conn.execute(
                        "INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description)
                         VALUES (?1, CAST(strftime('%s', 'now') AS INTEGER), ?2);",
                        params![
                            v as i64,
                            format!("Consolidated Schema v{:02}: {file_name}", v)
                        ],
                    );
                }
            }
            Ok(())
        })?;

        self.reconcile_routing_config();
        let integrity = self.verify_integrity()?;
        if actually_executed {
            let _ = self.log_telemetry(
                "INFO",
                "engine.database",
                &format!("Applied SQLite schema build {file_name}"),
                Some(integrity.clone()),
            );
        }

        let mut out = integrity;
        if let Some(obj) = out.as_object_mut() {
            obj.insert("status".to_string(), json!("APPLIED"));
            obj.insert("already_applied".to_string(), json!(!actually_executed));
            obj.insert("migration_file".to_string(), json!(file_name));
        }
        Ok(out)
    }

    /// Cross-process database advisory cycle lock (`HI-4`).
    ///
    /// Acquires an exclusive lease in `system_config` (`engine_cycle_lock_owner`, `engine_cycle_lock_expires_ms`)
    /// inside an atomic `BEGIN IMMEDIATE` transaction so concurrent CLI (`run`, `daemon`) and Web UI
    /// (`api_trigger_cycle`, `api_trigger_pond_scan`, `api_close_trade`) processes never execute
    /// overlapping strategy cycles against the same SQLite database.
    pub fn try_acquire_cycle_lock(&self, owner: &str, ttl_ms: i64) -> Result<bool> {
        let now_ms = self.now_ms();
        let clean_owner = owner.trim();
        let owner_val = if clean_owner.is_empty() {
            "aegis-engine"
        } else {
            clean_owner
        };
        let eff_ttl = ttl_ms.max(200);
        let expires_ms = now_ms.saturating_add(eff_ttl);

        self.with_conn(|conn| {
            let began_tx = conn.is_autocommit();
            if began_tx {
                conn.execute_batch("BEGIN IMMEDIATE;")
                    .map_err(|e| AegisError::Database(e.to_string()))?;
            }
            let res: Result<bool> = (|| {
                let cur_owner: String = conn
                    .query_row(
                        "SELECT config_value FROM system_config WHERE config_key = 'engine_cycle_lock_owner';",
                        [],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(|e| AegisError::Database(e.to_string()))?
                    .unwrap_or_default();
                let cur_exp: i64 = conn
                    .query_row(
                        "SELECT config_value FROM system_config WHERE config_key = 'engine_cycle_lock_expires_ms';",
                        [],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|e| AegisError::Database(e.to_string()))?
                    .and_then(|s| s.trim().parse::<i64>().ok())
                    .unwrap_or(0);

                if !cur_owner.trim().is_empty() && cur_owner.trim() != owner_val && cur_exp > now_ms {
                    return Ok(false);
                }

                for (k, v, vt, desc) in [
                    (
                        "engine_cycle_lock_owner",
                        owner_val.to_string(),
                        "string",
                        "Owner identifier of the cross-process database advisory cycle lock (HI-4)",
                    ),
                    (
                        "engine_cycle_lock_expires_ms",
                        expires_ms.to_string(),
                        "int",
                        "Epoch expiration timestamp (ms) of the cross-process database advisory cycle lock (HI-4)",
                    ),
                ] {
                    conn.execute(
                        "INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
                         VALUES (?1, ?2, ?3, 'system', ?4, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                         ON CONFLICT(config_key) DO UPDATE SET
                            config_value = excluded.config_value,
                            updated_at = excluded.updated_at;",
                        params![k, v, vt, desc],
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                }
                Ok(true)
            })();
            match res {
                Ok(acquired) => {
                    if began_tx {
                        conn.execute_batch("COMMIT;")
                            .map_err(|e| AegisError::Database(e.to_string()))?;
                    }
                    Ok(acquired)
                }
                Err(e) => {
                    if began_tx {
                        let _ = conn.execute_batch("ROLLBACK;");
                    }
                    Err(e)
                }
            }
        })
    }

    /// Release the cross-process database advisory cycle lock if held by `owner` (or expired).
    pub fn release_cycle_lock(&self, owner: &str) -> Result<()> {
        let now_ms = self.now_ms();
        let clean_owner = owner.trim();
        let owner_val = if clean_owner.is_empty() {
            "aegis-engine"
        } else {
            clean_owner
        };
        self.with_conn(|conn| {
            let began_tx = conn.is_autocommit();
            if began_tx {
                conn.execute_batch("BEGIN IMMEDIATE;")
                    .map_err(|e| AegisError::Database(e.to_string()))?;
            }
            let cur_owner: String = conn
                .query_row(
                    "SELECT config_value FROM system_config WHERE config_key = 'engine_cycle_lock_owner';",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .unwrap_or(None)
                .unwrap_or_default();
            let cur_exp: i64 = conn
                .query_row(
                    "SELECT config_value FROM system_config WHERE config_key = 'engine_cycle_lock_expires_ms';",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .optional()
                .unwrap_or(None)
                .and_then(|s| s.trim().parse::<i64>().ok())
                .unwrap_or(0);

            if cur_owner.trim().is_empty()
                || cur_owner.trim() == owner_val
                || cur_exp <= now_ms
            {
                let _ = conn.execute(
                    "UPDATE system_config
                     SET config_value = '',
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                     WHERE config_key = 'engine_cycle_lock_owner';",
                    [],
                );
                let _ = conn.execute(
                    "UPDATE system_config
                     SET config_value = '0',
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                     WHERE config_key = 'engine_cycle_lock_expires_ms';",
                    [],
                );
            }
            if began_tx {
                conn.execute_batch("COMMIT;")
                    .map_err(|e| AegisError::Database(e.to_string()))?;
            }
            Ok(())
        })
    }

    /// Renews an active cross-process database advisory cycle lock lease if still owned by `owner` (`R18-L1`).
    pub fn renew_cycle_lock(&self, owner: &str, ttl_ms: i64) -> Result<bool> {
        // Never block background renewal ticks on file_swap_lock.write() during Step 3 backup copy.
        if self.file_swap_lock.try_read().is_none() {
            return Ok(false);
        }
        let clean_owner = owner.trim();
        let owner_val = if clean_owner.is_empty() {
            "aegis-engine"
        } else {
            clean_owner
        };
        let eff_ttl = ttl_ms.max(200);
        self.with_conn(|conn| {
            let began_tx = conn.is_autocommit();
            if began_tx {
                conn.execute_batch("BEGIN IMMEDIATE;")
                    .map_err(|e| AegisError::Database(e.to_string()))?;
            }
            let res: Result<bool> = (|| {
                let cur_owner: String = conn
                    .query_row(
                        "SELECT config_value FROM system_config WHERE config_key = 'engine_cycle_lock_owner';",
                        [],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(|e| AegisError::Database(e.to_string()))?
                    .unwrap_or_default();
                if cur_owner.trim() != owner_val {
                    return Ok(false);
                }
                let expires_ms = self.now_ms().saturating_add(eff_ttl);
                conn.execute(
                    "INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
                     VALUES ('engine_cycle_lock_expires_ms', ?1, 'int', 'system', 'Epoch expiration timestamp (ms) of the cross-process database advisory cycle lock (HI-4)', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                     ON CONFLICT(config_key) DO UPDATE SET
                        config_value = excluded.config_value,
                        updated_at = excluded.updated_at;",
                    params![expires_ms.to_string()],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(true)
            })();
            match res {
                Ok(renewed) => {
                    if began_tx {
                        conn.execute_batch("COMMIT;")
                            .map_err(|e| AegisError::Database(e.to_string()))?;
                    }
                    Ok(renewed)
                }
                Err(e) => {
                    if began_tx {
                        let _ = conn.execute_batch("ROLLBACK;");
                    }
                    Err(e)
                }
            }
        })
    }

    /// Computes a size-proportional initial cycle lock lease TTL (ms) for database restore (`R18-L1`).
    ///
    /// Bounds: `[120_000, 3_600_000]` ms (`2 min` .. `60 min`), assuming a conservative
    /// `10 MiB/s` copy/snapshot rate with a `4x` safety multiplier.
    pub fn recommended_restore_lock_ttl_ms(&self, backup_path: Option<&Path>) -> i64 {
        let live_db_bytes = fs::metadata(&self.db_path).map(|m| m.len()).unwrap_or(0);
        let backup_bytes = backup_path
            .and_then(|p| fs::metadata(p).ok())
            .map(|m| m.len())
            .unwrap_or(0);
        let total_bytes = live_db_bytes.saturating_add(backup_bytes);
        let estimated_ms = ((total_bytes as u128).saturating_mul(4_000) / 10_485_760) as i64;
        estimated_ms.clamp(120_000, 3_600_000)
    }

    /// Acquire the cross-process database advisory cycle lock and return an RAII guard with background heartbeat renewal (`HI-4`, `R18-L1`).
    pub fn acquire_cycle_lock_guard(
        &self,
        owner: &str,
        ttl_ms: i64,
    ) -> Result<Option<DbCycleLockGuard>> {
        let eff_ttl = ttl_ms.max(200);
        if self.try_acquire_cycle_lock(owner, eff_ttl)? {
            let clean_owner = owner.trim();
            let owner_val = if clean_owner.is_empty() {
                "aegis-engine".to_string()
            } else {
                clean_owner.to_string()
            };
            let stop_flag = Arc::new(AtomicBool::new(false));
            let stop_bg = Arc::clone(&stop_flag);
            let db_bg = self.clone();
            let owner_bg = owner_val.clone();
            let renew_interval =
                std::time::Duration::from_millis((eff_ttl / 3).clamp(50, 30_000) as u64);
            let renew_handle = std::thread::Builder::new()
                .name(format!("aegis-cycle-lock-{}", owner_val))
                .spawn(move || {
                    while !stop_bg.load(Ordering::Acquire) {
                        std::thread::park_timeout(renew_interval);
                        if stop_bg.load(Ordering::Acquire) {
                            break;
                        }
                        let _ = db_bg.renew_cycle_lock(&owner_bg, eff_ttl);
                    }
                })
                .ok();
            Ok(Some(DbCycleLockGuard {
                db: self.clone(),
                owner: owner_val,
                stop_flag,
                renew_handle,
            }))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn query_active_cycle_lock(conn: &Connection, now_ms: i64) -> Option<(String, i64)> {
        let cur_owner: String = conn
            .query_row(
                "SELECT config_value FROM system_config WHERE config_key = 'engine_cycle_lock_owner';",
                [],
                |r| r.get(0),
            )
            .optional()
            .ok()
            .flatten()
            .unwrap_or_default();
        let cur_exp: i64 = conn
            .query_row(
                "SELECT config_value FROM system_config WHERE config_key = 'engine_cycle_lock_expires_ms';",
                [],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .ok()
            .flatten()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(0);
        if !cur_owner.trim().is_empty() && cur_exp > now_ms {
            return Some((cur_owner.trim().to_string(), cur_exp));
        }

        let has_lock_table: bool = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='db_cycle_lock';",
                [],
                |_| Ok(true),
            )
            .optional()
            .ok()
            .flatten()
            .unwrap_or(false);
        if has_lock_table {
            if let Ok(Some((owner, exp))) = conn
                .query_row(
                    "SELECT COALESCE(owner, 'db_cycle_lock'), COALESCE(expires_at_ms, 0)
                     FROM db_cycle_lock
                     WHERE COALESCE(expires_at_ms, 0) > ?1
                     LIMIT 1;",
                    params![now_ms],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
                )
                .optional()
            {
                if !owner.trim().is_empty() {
                    return Some((owner.trim().to_string(), exp));
                }
            }
        }
        None
    }

    /// Returns the canonical backup directory for this database (`R16-U1`).
    pub fn backup_directory(&self) -> PathBuf {
        if self.is_memory {
            std::env::temp_dir().join("aegis_backups")
        } else {
            self.db_path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("data"))
                .join("backups")
        }
    }

    /// Resolves the CLI binary display path for operator-facing restore hints (`R17-N3`).
    pub(crate) fn resolve_cli_binary_display() -> String {
        if let Ok(exe) = std::env::current_exe() {
            if exe.file_stem().and_then(|s| s.to_str()) == Some("aegis-quant") {
                return exe.display().to_string();
            }
        }
        "./bin/aegis-quant".to_string()
    }

    /// Computes a lower-case hex SHA-256 digest of `path` by streaming in 1 MiB chunks (`R17-L2`).
    pub fn compute_file_sha256_streaming(path: &Path) -> Result<String> {
        use std::io::Read;
        let mut file = fs::File::open(path)?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 1_048_576];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let digest = hasher.finalize();
        Ok(digest.iter().map(|b| format!("{b:02x}")).collect())
    }

    /// Lists available `.db` backup files in `backup_directory()` sorted newest-first (`R16-U1`, `R17-N3`).
    pub fn list_sqlite_backups(&self) -> Result<Vec<Value>> {
        let backup_dir = self.backup_directory();
        if !backup_dir.exists() {
            return Ok(Vec::new());
        }
        let cfg = self.load_config_opts(false).unwrap_or_default();
        let ret_days = cfg.get_i64("backup_retention_days", 7).clamp(1, 90);
        let retention_ms = ret_days.saturating_mul(86_400_000);
        let cli_bin = Self::resolve_cli_binary_display();

        let mut rows: Vec<(i64, String, Value)> = Vec::new();
        let entries = fs::read_dir(&backup_dir)?;
        for entry in entries.flatten() {
            let p = entry.path();
            let Ok(sym_meta) = fs::symlink_metadata(&p) else {
                continue;
            };
            if sym_meta.file_type().is_symlink() || !sym_meta.file_type().is_file() {
                continue;
            }
            let Some(fname) = p.file_name().and_then(|s| s.to_str()).map(str::to_string) else {
                continue;
            };
            if !fname.ends_with(".db") {
                continue;
            }
            let size_bytes = sym_meta.len();
            if size_bytes == 0 {
                continue;
            }
            let modified_sys = sym_meta
                .modified()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            let created_at_ms = modified_sys
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let created_at = aegis_core::format_ms_iso8601_utc(created_at_ms);
            let expires_at_ms = created_at_ms.saturating_add(retention_ms);
            let expires_at = aegis_core::format_ms_iso8601_utc(expires_at_ms);
            let is_pre_restore_snapshot = fname.starts_with("aegis_quant_pre_restore_");
            let restore_cli_command = format!(
                "{cli_bin} --db {} db --restore {} --confirm",
                self.db_path.display(),
                p.display()
            );

            rows.push((
                created_at_ms,
                fname.clone(),
                json!({
                    "filename": fname,
                    "path": p.display().to_string(),
                    "size_bytes": size_bytes,
                    "created_at": created_at,
                    "created_at_ms": created_at_ms,
                    "expires_at": expires_at,
                    "expires_at_ms": expires_at_ms,
                    "is_pre_restore_snapshot": is_pre_restore_snapshot,
                    "restore_cli_command": restore_cli_command,
                }),
            ));
        }

        rows.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        Ok(rows.into_iter().map(|(_, _, val)| val).collect())
    }

    /// Resolves a bare `.db` filename inside `backup_directory()`, rejecting path traversal,
    /// separators, symlinks, non-`.db` extensions, or paths outside the canonical backup directory (`R16-U1`).
    pub fn resolve_whitelisted_backup_path(&self, bare_filename: &str) -> Result<PathBuf> {
        let trimmed = bare_filename.trim();
        if trimmed.is_empty()
            || trimmed.contains('/')
            || trimmed.contains('\\')
            || trimmed.contains("..")
            || trimmed.contains(':')
            || trimmed.contains('\0')
            || !trimmed.ends_with(".db")
            || trimmed == ".db"
        {
            return Err(AegisError::Validation(format!(
                "Invalid backup filename '{trimmed}': must be a bare .db filename without path separators or '..' (R16-U1)"
            )));
        }
        if Path::new(trimmed).file_name().and_then(|s| s.to_str()) != Some(trimmed) {
            return Err(AegisError::Validation(format!(
                "Invalid backup filename '{trimmed}': must not contain directory components (R16-U1)"
            )));
        }

        let backup_dir = self.backup_directory();
        fs::create_dir_all(&backup_dir)?;
        let canon_dir = fs::canonicalize(&backup_dir).map_err(|e| {
            AegisError::Database(format!(
                "Failed to canonicalize backup directory {}: {e}",
                backup_dir.display()
            ))
        })?;

        let candidate = backup_dir.join(trimmed);
        let sym_meta = match fs::symlink_metadata(&candidate) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(AegisError::NotFound(format!(
                    "Backup file not found: {trimmed}"
                )));
            }
            Err(e) => return Err(AegisError::Io(e)),
        };

        if sym_meta.file_type().is_symlink() {
            return Err(AegisError::Validation(format!(
                "Refusing symlink backup file '{trimmed}' (R16-U1)"
            )));
        }
        if !sym_meta.file_type().is_file() {
            return Err(AegisError::Validation(format!(
                "Backup path '{trimmed}' is not a regular file (R16-U1)"
            )));
        }

        let canon_file = fs::canonicalize(&candidate).map_err(|e| {
            AegisError::Validation(format!(
                "Failed to canonicalize backup file '{trimmed}': {e}"
            ))
        })?;
        if canon_file.parent() != Some(canon_dir.as_path()) || !canon_file.starts_with(&canon_dir) {
            return Err(AegisError::Validation(format!(
                "Backup file '{trimmed}' resolved outside whitelisted backup directory (R16-U1)"
            )));
        }

        Ok(canon_file)
    }

    /// Inspects a whitelisted backup file read-only, computing its SHA-256 hex digest, verifying
    /// `PRAGMA integrity_check;`, and returning table row counts for Step 1 restore preview (`R16-U1`, `R17-L2`).
    pub fn inspect_sqlite_backup_preview(&self, bare_filename: &str) -> Result<Value> {
        let canon_path = self.resolve_whitelisted_backup_path(bare_filename)?;
        let fname = canon_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(bare_filename.trim())
            .to_string();
        let meta = fs::metadata(&canon_path)?;
        let size_bytes = meta.len();
        if size_bytes == 0 {
            return Err(AegisError::Validation(format!(
                "Backup file '{fname}' is empty (R16-U1)"
            )));
        }
        let modified_sys = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let created_at_ms = modified_sys
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let created_at = aegis_core::format_ms_iso8601_utc(created_at_ms);

        let sha256 = Self::compute_file_sha256_streaming(&canon_path)?;

        let ro_flags =
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let conn = Connection::open_with_flags(&canon_path, ro_flags).map_err(|e| {
            AegisError::Database(format!("Failed to open backup '{fname}' read-only: {e}"))
        })?;
        let integrity_check: String = conn
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .map_err(|e| AegisError::Database(format!("Backup integrity_check failed: {e}")))?;
        if !integrity_check.trim().eq_ignore_ascii_case("ok") {
            return Err(AegisError::Validation(format!(
                "Backup '{fname}' failed PRAGMA integrity_check: {integrity_check}"
            )));
        }

        let schema_version: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations;",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        let count_table = |table: &str| -> i64 {
            conn.query_row(&format!("SELECT COUNT(*) FROM {table};"), [], |r| r.get(0))
                .unwrap_or(0)
        };
        let trades_count = count_table("trades");
        let pond_cells_count = count_table("pond_cells");
        let snapshots_count = count_table("portfolio_snapshots");
        let config_count = count_table("system_config");

        Ok(json!({
            "filename": fname,
            "path": canon_path.display().to_string(),
            "size_bytes": size_bytes,
            "created_at": created_at,
            "created_at_ms": created_at_ms,
            "sha256": sha256,
            "schema_version": schema_version,
            "integrity_check": integrity_check,
            "row_counts": {
                "trades": trades_count,
                "pond_cells": pond_cells_count,
                "portfolio_snapshots": snapshots_count,
                "system_config": config_count,
            }
        }))
    }

    /// Evaluates optional backup preview inspection and WebUI restore preconditions in a single gate (`R16-U1`, `R17-M1`).
    #[allow(clippy::type_complexity)]
    pub fn evaluate_web_restore_gate(
        &self,
        preview_filename: Option<&str>,
    ) -> Result<(Option<Result<Value>>, bool, Vec<String>, Value)> {
        let preview_res = preview_filename.map(|fname| self.inspect_sqlite_backup_preview(fname));
        let (restore_allowed, disabled_reasons, preconditions) =
            self.check_web_restore_preconditions()?;
        Ok((
            preview_res,
            restore_allowed,
            disabled_reasons,
            preconditions,
        ))
    }

    /// Evaluates all safety preconditions required before a WebUI database restore (`R16-U1`, `R17-M1`, `R17-L1`, `R17-N1`).
    pub fn check_web_restore_preconditions(&self) -> Result<(bool, Vec<String>, Value)> {
        let now_ms = self.now_ms();
        let ext_pid_reason =
            self.detect_external_pid_holding_db_cached(std::time::Duration::from_millis(30));
        self.with_conn(|conn| {
            let exec_mode: String = conn
                .query_row(
                    "SELECT config_value FROM system_config WHERE config_key = 'execution_mode';",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?
                .unwrap_or_else(|| "PAPER".to_string())
                .trim()
                .to_ascii_uppercase();

            let pond_mode: String = conn
                .query_row(
                    "SELECT config_value FROM system_config WHERE config_key = 'pond_execution_mode';",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?
                .unwrap_or_else(|| exec_mode.clone())
                .trim()
                .to_ascii_uppercase();

            let active_pit_trades: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM trades WHERE status IN ('OPEN', 'PENDING');",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0);

            let active_pond_cells: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM pond_cells WHERE state IN ('ARMED', 'OPEN', 'TRAILED');",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0);

            let pit_stop_oids: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM trades WHERE status IN ('OPEN', 'PENDING') AND TRIM(COALESCE(protective_stop_oid, '')) != '';",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            let pond_stop_oids: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM pond_cells WHERE state IN ('ARMED', 'OPEN', 'TRAILED') AND TRIM(COALESCE(protective_stop_oid, '')) != '';",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            let working_protective_stops = pit_stop_oids + pond_stop_oids;

            let active_lock = Self::query_active_cycle_lock(conn, now_ms);

            let mut disabled_reasons = Vec::new();
            if exec_mode == "LIVE" {
                disabled_reasons.push(
                    "Book A (PIT) execution_mode is LIVE; switch to PAPER or TESTNET before restoring via WebUI (R16-U1)"
                        .to_string(),
                );
            }
            if pond_mode == "LIVE" {
                disabled_reasons.push(
                    "Book B (POND) pond_execution_mode is LIVE; switch to PAPER or TESTNET before restoring via WebUI (R16-U1)"
                        .to_string(),
                );
            }
            if active_pit_trades > 0 {
                disabled_reasons.push(format!(
                    "{active_pit_trades} active Book A (PIT) trade(s) in OPEN/PENDING status; close or cancel all positions before restoring (R16-U1)"
                ));
            }
            if active_pond_cells > 0 {
                disabled_reasons.push(format!(
                    "{active_pond_cells} active Book B (POND) cell(s) in ARMED/OPEN/TRAILED state; close all cells before restoring (R16-U1)"
                ));
            }
            if working_protective_stops > 0 {
                disabled_reasons.push(format!(
                    "{working_protective_stops} working protective_stop_oid order(s) present; cancel all working stop orders before restoring (R16-U1)"
                ));
            }
            if let Some((ref owner, exp_ms)) = active_lock {
                disabled_reasons.push(format!(
                    "Active db_cycle_lock held by '{owner}' (expires_ms={exp_ms}); wait for active cycle to finish before restoring (R16-U1)"
                ));
            }
            if let Some(ref ext_reason) = ext_pid_reason {
                disabled_reasons.push(format!(
                    "External process holds open descriptor to database ({ext_reason}); stop external aegis-quant / aegis-pit / aegis-pond daemons before restoring via WebUI (R17-M1)"
                ));
            }

            let restore_allowed = disabled_reasons.is_empty();
            let preconditions = json!({
                "restore_allowed": restore_allowed,
                "execution_mode": exec_mode,
                "pond_execution_mode": pond_mode,
                "active_pit_trades": active_pit_trades,
                "active_pond_cells": active_pond_cells,
                "working_protective_stop_orders": working_protective_stops,
                "active_cycle_lock": active_lock.is_some(),
                "cycle_lock_owner": active_lock.as_ref().map(|(o, _)| o.clone()),
                "cycle_lock_expires_ms": active_lock.as_ref().map(|(_, exp)| *exp),
                "external_process_detected": ext_pid_reason.is_some(),
                "external_process_detail": ext_pid_reason,
                "pool_generation": self.pool_generation(),
                "discarded_stale_connections": self.discarded_stale_connections_count(),
            });

            Ok((restore_allowed, disabled_reasons, preconditions))
        })
    }

    /// Scans `/proc/<pid>/fd` on Linux to detect whether any external OS process (`pid != self`)
    /// holds an open file descriptor to `<db>`, `<db>-wal`, or `<db>-shm` (`R16-M2`, `R17-M1`).
    pub fn detect_external_pid_holding_db(&self) -> Option<String> {
        if self.is_memory || self.db_path == Path::new(":memory:") || !self.db_path.exists() {
            return None;
        }

        #[cfg(target_os = "linux")]
        {
            let self_pid = std::process::id();
            let canon_db = fs::canonicalize(&self.db_path).unwrap_or_else(|_| self.db_path.clone());
            let mut target_paths = Vec::with_capacity(6);
            for base in [&self.db_path, &canon_db] {
                let s = base.display().to_string();
                target_paths.push(s.clone());
                target_paths.push(format!("{s}-wal"));
                target_paths.push(format!("{s}-shm"));
            }

            if let Ok(proc_entries) = fs::read_dir("/proc") {
                for entry in proc_entries.flatten() {
                    let fname = entry.file_name();
                    let Some(pid_str) = fname.to_str() else {
                        continue;
                    };
                    let Ok(pid) = pid_str.parse::<u32>() else {
                        continue;
                    };
                    if pid == self_pid {
                        continue;
                    }
                    let fd_dir = entry.path().join("fd");
                    let Ok(fds) = fs::read_dir(&fd_dir) else {
                        continue;
                    };
                    for fd_entry in fds.flatten() {
                        if let Ok(link_target) = fs::read_link(fd_entry.path()) {
                            let link_str = link_target.to_string_lossy();
                            let clean_link =
                                link_str.strip_suffix(" (deleted)").unwrap_or(&link_str);
                            if target_paths.iter().any(|tp| tp == clean_link) {
                                return Some(format!(
                                    "external process PID {pid} holds open descriptor to {link_str} (R16-M2 / R17-M1)"
                                ));
                            }
                        }
                    }
                }
            }
        }

        None
    }

    /// Cached wrapper around `detect_external_pid_holding_db` for high-frequency read-only UI polling (`R18-S8`).
    pub fn detect_external_pid_holding_db_cached(
        &self,
        ttl: std::time::Duration,
    ) -> Option<String> {
        let now = std::time::Instant::now();
        {
            let guard = self.ext_pid_scan_cache.lock();
            if let Some((ts, ref cached)) = *guard {
                if now.duration_since(ts) <= ttl {
                    return cached.clone();
                }
            }
        }
        let fresh = self.detect_external_pid_holding_db();
        {
            let mut guard = self.ext_pid_scan_cache.lock();
            *guard = Some((now, fresh.clone()));
        }
        fresh
    }

    /// Detects whether any external process or active cycle is currently using this SQLite database (`R16-M2`, `R17-M1`, `R17-L3`).
    ///
    /// Checks:
    /// (a) External PID in `/proc` holding DB, `-wal`, or `-shm` open;
    /// (b) Unexpired row/lease in `db_cycle_lock` (`engine_cycle_lock_owner` / `engine_cycle_lock_expires_ms`),
    ///     failing closed on non-empty DB open failure after a 25ms retry (`R17-L3`);
    /// (c) When `self.conn_pool.lock().is_empty()`, `<db>-shm` existing on disk with `len() > 0`
    ///     and modified within the last `120` seconds.
    pub fn detect_live_database_activity(&self) -> Result<Option<String>> {
        if self.is_memory || self.db_path == Path::new(":memory:") || !self.db_path.exists() {
            return Ok(None);
        }

        let pool_is_empty = self.conn_pool.lock().is_empty();

        // (a) External PID in /proc holding DB / WAL / SHM open
        if let Some(ext_reason) = self.detect_external_pid_holding_db() {
            return Ok(Some(ext_reason));
        }

        // (b) Unexpired cycle lock in db_cycle_lock / system_config (R17-L3: retry once, then fail closed)
        if fs::metadata(&self.db_path)
            .map(|m| m.len() > 0)
            .unwrap_or(false)
        {
            let now_ms = self.now_ms();
            let ro_flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
            let ro_conn_res = match Connection::open_with_flags(&self.db_path, ro_flags) {
                Ok(c) => Ok(c),
                Err(_) => {
                    std::thread::sleep(std::time::Duration::from_millis(25));
                    Connection::open_with_flags(&self.db_path, ro_flags)
                }
            };
            match ro_conn_res {
                Ok(ro_conn) => {
                    let _ = ro_conn.busy_timeout(std::time::Duration::from_millis(2000));
                    if let Some((owner, exp_ms)) = Self::query_active_cycle_lock(&ro_conn, now_ms) {
                        return Ok(Some(format!(
                            "unexpired db_cycle_lock held by '{owner}' until {exp_ms} ms (R16-M2)"
                        )));
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        db_path = %self.db_path.display(),
                        error = %err,
                        "Failed to open read-only connection to probe db_cycle_lock; failing closed (R17-L3)"
                    );
                    return Ok(Some(format!(
                        "failed to open read-only connection to probe db_cycle_lock on {}: {err} (R17-L3)",
                        self.db_path.display()
                    )));
                }
            }
        }

        // (c) When conn_pool is empty, <db>-shm on disk with len() > 0 modified within 120s
        if pool_is_empty {
            let shm_path = PathBuf::from(format!("{}-shm", self.db_path.display()));
            if let Ok(meta) = fs::metadata(&shm_path) {
                if meta.len() > 0 {
                    let recently_modified = meta
                        .modified()
                        .ok()
                        .and_then(|m| std::time::SystemTime::now().duration_since(m).ok())
                        .map(|age| age <= std::time::Duration::from_secs(120))
                        .unwrap_or(true);
                    if recently_modified {
                        return Ok(Some(format!(
                            "active SQLite WAL shared-memory sidecar {} ({} bytes, modified within 120s) while local pool is empty (R16-M2)",
                            shm_path.display(),
                            meta.len()
                        )));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Create an atomic online SQLite backup via `VACUUM INTO` and prune backups older than `retention_days` (`ME-19`, `R15-N9`).
    pub fn create_sqlite_backup(
        &self,
        backup_dir: Option<&Path>,
        retention_days: Option<u64>,
    ) -> Result<Value> {
        let cfg = self.load_config().unwrap_or_default();
        let ret_days = retention_days
            .unwrap_or_else(|| cfg.get_i64("backup_retention_days", 7).max(1) as u64)
            .clamp(1, 90);

        let target_dir: PathBuf = match backup_dir {
            Some(d) => d.to_path_buf(),
            None => {
                if self.is_memory {
                    std::env::temp_dir().join("aegis_backups")
                } else {
                    self.db_path
                        .parent()
                        .unwrap_or_else(|| Path::new("data"))
                        .join("backups")
                }
            }
        };
        fs::create_dir_all(&target_dir).map_err(|e| {
            AegisError::Database(format!(
                "Failed to create backup directory {}: {e}",
                target_dir.display()
            ))
        })?;

        let now_dt = chrono::Utc::now();
        let file_name = format!(
            "aegis_quant_{}_{:03}.db",
            now_dt.format("%Y%m%d_%H%M%S"),
            now_dt.timestamp_subsec_millis()
        );
        let backup_path = target_dir.join(&file_name);
        if backup_path.exists() {
            let _ = fs::remove_file(&backup_path);
        }
        let backup_path_str = backup_path.to_string_lossy().to_string();

        self.with_conn(|conn| {
            conn.execute("VACUUM INTO ?1;", params![backup_path_str])
                .map_err(|e| AegisError::Database(format!("VACUUM INTO failed: {e}")))?;
            Ok(())
        })?;

        let size_bytes = fs::metadata(&backup_path).map(|m| m.len()).unwrap_or(0);
        if size_bytes == 0 {
            return Err(AegisError::Database(format!(
                "Backup file {} is empty after VACUUM INTO",
                backup_path.display()
            )));
        }

        // Prune backup snapshots older than retention_days
        let cutoff = std::time::SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(
                ret_days.saturating_mul(86_400),
            ))
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let mut pruned_count = 0usize;
        let mut retained_count = 1usize;
        if let Ok(entries) = fs::read_dir(&target_dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p == backup_path {
                    continue;
                }
                let fname = p.file_name().and_then(|s| s.to_str()).unwrap_or_default();
                if fname.starts_with("aegis_quant_") && fname.ends_with(".db") {
                    let mut pruned = false;
                    if let Ok(meta) = entry.metadata() {
                        if let Ok(modified) = meta.modified() {
                            if modified < cutoff && fs::remove_file(&p).is_ok() {
                                pruned_count += 1;
                                pruned = true;
                            }
                        }
                    }
                    if !pruned {
                        retained_count += 1;
                    }
                }
            }
        }

        let now_ms = now_dt.timestamp_millis();
        let _ = self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
                 VALUES ('last_sqlite_backup_ms', ?1, 'int', 'system', 'Epoch timestamp (ms) of the most recent automated or manual SQLite backup (R15-N10)', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                 ON CONFLICT(config_key) DO UPDATE SET
                    config_value = excluded.config_value,
                    updated_at = excluded.updated_at;",
                params![now_ms.to_string()],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        });

        let summary = json!({
            "status": "OK",
            "backup_path": backup_path.display().to_string(),
            "file_name": file_name,
            "size_bytes": size_bytes,
            "retention_days": ret_days,
            "retained_count": retained_count,
            "pruned_count": pruned_count,
            "pruned_files": pruned_count,
            "created_at": now_dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "last_sqlite_backup_ms": now_ms,
        });
        let _ = self.log_telemetry(
            "INFO",
            "engine.backup",
            &format!(
                "Created SQLite VACUUM INTO backup {} ({} bytes, pruned={})",
                file_name, size_bytes, pruned_count
            ),
            Some(summary.clone()),
        );
        Ok(summary)
    }

    /// Restore the active SQLite database from an existing backup file after verifying
    /// `PRAGMA integrity_check;`, taking a pre-restore safety snapshot, running `rusqlite::backup::Backup`,
    /// re-running canonical migrations, and verifying `PRAGMA quick_check;` (`ME-19`, `R16-M2`).
    ///
    /// Returns the path of the pre-restore safety snapshot.
    pub fn restore_sqlite_backup(&self, backup_path: &Path) -> Result<PathBuf> {
        self.restore_sqlite_backup_with_options(backup_path, false, false)
    }

    /// Applies post-copy `db_cycle_lock` invariants directly on a restored database connection (`R18-N1`, `R18-N2`).
    ///
    /// - When `preserve_web_restore_exp` is `Some(exp_ms)` (an in-service quiesced WebUI restore where
    ///   `"web.restore"` held the active lease before Step 3), re-asserts `engine_cycle_lock_owner = 'web.restore'`
    ///   and `engine_cycle_lock_expires_ms = exp_ms` before `file_swap_lock.write()` is released (`R18-N1`).
    /// - When `preserve_web_restore_exp` is `None` (CLI restore or unheld lock), unconditionally clears any
    ///   stale `engine_cycle_lock_owner` and `engine_cycle_lock_expires_ms` inherited from the backup snapshot (`R18-N2`).
    fn apply_post_copy_cycle_lock_state(conn: &Connection, preserve_web_restore_exp: Option<i64>) {
        let has_cfg: bool = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='system_config';",
                [],
                |_| Ok(true),
            )
            .optional()
            .ok()
            .flatten()
            .unwrap_or(false);
        if !has_cfg {
            return;
        }
        if let Some(exp_ms) = preserve_web_restore_exp {
            for (k, v, vt, desc) in [
                (
                    "engine_cycle_lock_owner",
                    "web.restore".to_string(),
                    "string",
                    "Owner identifier of the cross-process database advisory cycle lock (HI-4)",
                ),
                (
                    "engine_cycle_lock_expires_ms",
                    exp_ms.to_string(),
                    "int",
                    "Epoch expiration timestamp (ms) of the cross-process database advisory cycle lock (HI-4)",
                ),
            ] {
                let _ = conn.execute(
                    "INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
                     VALUES (?1, ?2, ?3, 'system', ?4, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                     ON CONFLICT(config_key) DO UPDATE SET
                        config_value = excluded.config_value,
                        updated_at = excluded.updated_at;",
                    params![k, v, vt, desc],
                );
            }
        } else {
            let _ = conn.execute(
                "UPDATE system_config
                 SET config_value = '',
                     updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                 WHERE config_key = 'engine_cycle_lock_owner';",
                [],
            );
            let _ = conn.execute(
                "UPDATE system_config
                 SET config_value = '0',
                     updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                 WHERE config_key = 'engine_cycle_lock_expires_ms';",
                [],
            );
        }
    }

    /// Restore the active SQLite database from an existing backup file with explicit live-activity
    /// override and in-service quiescence controls (`R16-M1`, `R16-M2`, `R16-U1`, `R17-M1`, `R18-N1`, `R18-N2`, `P2-C`).
    ///
    /// Returns the path of the pre-restore safety snapshot.
    pub fn restore_sqlite_backup_with_options(
        &self,
        backup_path: &Path,
        allow_live_override: bool,
        in_service_quiesced: bool,
    ) -> Result<PathBuf> {
        if self.is_memory {
            return Err(AegisError::Validation(
                "Cannot restore a file backup into an :memory: SQLite database".to_string(),
            ));
        }
        if !backup_path.exists() {
            return Err(AegisError::NotFound(format!(
                "Backup file not found: {}",
                backup_path.display()
            )));
        }
        let meta = fs::metadata(backup_path)?;
        if meta.len() == 0 {
            return Err(AegisError::Database(format!(
                "Backup file {} is empty",
                backup_path.display()
            )));
        }

        if !allow_live_override {
            if in_service_quiesced {
                if let Some(ext_reason) = self.detect_external_pid_holding_db() {
                    return Err(AegisError::Validation(format!(
                        "Refusing to restore SQLite backup while external process holds database open ({ext_reason}); stop external daemons or pass --force to override (R17-M1)"
                    )));
                }
            } else if let Some(reason) = self.detect_live_database_activity()? {
                return Err(AegisError::Validation(format!(
                    "Refusing to restore SQLite backup while live database activity is detected ({reason}); stop all running daemons or pass --force to override (R16-M2)"
                )));
            }
        }

        // 1. Verify backup integrity on a read-only connection before touching the live DB
        let ro_flags =
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let src_conn = Connection::open_with_flags(backup_path, ro_flags).map_err(|e| {
            AegisError::Database(format!(
                "Failed to open backup {}: {e}",
                backup_path.display()
            ))
        })?;
        let integrity: String = src_conn
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .map_err(|e| AegisError::Database(format!("Backup integrity_check failed: {e}")))?;
        if !integrity.trim().eq_ignore_ascii_case("ok") {
            return Err(AegisError::Database(format!(
                "Backup {} failed integrity_check: {integrity}",
                backup_path.display()
            )));
        }

        // 2. Create a pre-restore safety snapshot of the live DB
        let backup_dir = self.backup_directory();
        fs::create_dir_all(&backup_dir)?;
        let now_dt = chrono::Utc::now();
        let pre_restore_path = backup_dir.join(format!(
            "aegis_quant_pre_restore_{}_{:03}.db",
            now_dt.format("%Y%m%d_%H%M%S"),
            now_dt.timestamp_subsec_millis()
        ));
        if pre_restore_path.exists() {
            let _ = fs::remove_file(&pre_restore_path);
        }
        if self.db_path.exists() {
            let pre_str = pre_restore_path.to_string_lossy().to_string();
            self.with_conn(|conn| {
                conn.execute("VACUUM INTO ?1;", params![pre_str])
                    .map_err(|e| {
                        AegisError::Database(format!(
                            "Failed to create pre-restore safety snapshot: {e}"
                        ))
                    })?;
                Ok(())
            })?;
        }

        // Check whether caller ("web.restore") holds an active cycle lock prior to Step 3 overwrite (R18-N1)
        let preserve_web_restore_exp: Option<i64> = if in_service_quiesced && self.db_path.exists()
        {
            let now_ms = self.now_ms();
            self.with_conn(|conn| {
                Ok(
                    Self::query_active_cycle_lock(conn, now_ms).and_then(|(owner, exp)| {
                        if owner == "web.restore" {
                            Some(exp.max(now_ms.saturating_add(120_000)))
                        } else {
                            None
                        }
                    }),
                )
            })
            .ok()
            .flatten()
        } else {
            None
        };

        // 3. Hold file_swap_lock.write() while bumping pool generation, clearing pooled connections
        // and WAL/SHM sidecars, running rusqlite::backup::Backup, and enforcing post-copy lock state (R18-N1 / R18-N2).
        let (new_gen, cleared_pool) = {
            let _swap_guard = self.file_swap_lock.write();
            let (new_gen, cleared_pool) = self.bump_generation_and_clear_pool_sidecars();
            {
                let mut dst_conn = Connection::open(&self.db_path).map_err(|e| {
                    AegisError::Database(format!(
                        "Failed to open target DB {} for restore: {e}",
                        self.db_path.display()
                    ))
                })?;
                let backup =
                    rusqlite::backup::Backup::new(&src_conn, &mut dst_conn).map_err(|e| {
                        AegisError::Database(format!("Failed to initialize SQLite Backup: {e}"))
                    })?;
                backup
                    .run_to_completion(256, std::time::Duration::from_millis(5), None)
                    .map_err(|e| {
                        AegisError::Database(format!("SQLite backup restore failed: {e}"))
                    })?;
                drop(backup);
                Self::apply_post_copy_cycle_lock_state(&dst_conn, preserve_web_restore_exp);
            }
            (new_gen, cleared_pool)
        };
        drop(src_conn);

        // 4. Heal any split-brain sidecars, ensure schema parity, re-assert post-copy lock state, and verify quick_check
        let _ = self.check_and_heal_wal_split_brain();
        self.run_migrations()?;
        let _ = self.with_conn(|conn| {
            Self::apply_post_copy_cycle_lock_state(conn, preserve_web_restore_exp);
            Ok(())
        });
        let quick: String = self.with_conn(|conn| {
            conn.query_row("PRAGMA quick_check;", [], |r| r.get(0))
                .map_err(|e| AegisError::Database(format!("Post-restore quick_check failed: {e}")))
        })?;
        if !quick.trim().eq_ignore_ascii_case("ok") {
            return Err(AegisError::Database(format!(
                "Post-restore quick_check returned '{quick}'"
            )));
        }

        let _ = self.log_telemetry(
            "WARN",
            "engine.backup",
            &format!(
                "SQLITE_BACKUP_RESTORED from {} (pre_restore_snapshot={}, pool_generation={new_gen})",
                backup_path.display(),
                pre_restore_path.display()
            ),
            Some(json!({
                "backup_path": backup_path.display().to_string(),
                "pre_restore_snapshot": pre_restore_path.display().to_string(),
                "pool_generation": new_gen,
                "cleared_idle_connections": cleared_pool,
                "allow_live_override": allow_live_override,
                "in_service_quiesced": in_service_quiesced,
            })),
        );
        Ok(pre_restore_path)
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
        Ok(v.get("schema_version")
            .and_then(|x| x.as_i64())
            .unwrap_or(0))
    }
}
