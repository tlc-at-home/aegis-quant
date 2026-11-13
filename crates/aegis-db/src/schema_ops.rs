//! SQLite schema initialization, consolidated schema build runner (`sql/schema_and_seed.sql`),
//! routing mode reconciliation, cross-process advisory cycle lock, automated `VACUUM INTO` backups,
//! and database integrity verification.

use crate::fernet::ensure_master_key_for_db;
use crate::DatabaseManager;
use aegis_core::{resolve_project_root, AegisError, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};

/// Canonical unified schema version built by `sql/schema_and_seed.sql`.
pub const CANONICAL_SCHEMA_VERSION: i64 = 15;

/// Single canonical SQLite schema build & seed script replacing legacy incremental migrations.
pub const ALL_MIGRATION_FILES: &[&str] = &["sql/schema_and_seed.sql"];

/// RAII guard for the cross-process SQLite advisory cycle lock (`HI-4`).
pub struct DbCycleLockGuard {
    db: DatabaseManager,
    owner: String,
}

impl Drop for DbCycleLockGuard {
    fn drop(&mut self) {
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
        let eff_ttl = ttl_ms.max(1_000);
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

    /// Acquire the cross-process database advisory cycle lock and return an RAII guard (`HI-4`).
    pub fn acquire_cycle_lock_guard(
        &self,
        owner: &str,
        ttl_ms: i64,
    ) -> Result<Option<DbCycleLockGuard>> {
        if self.try_acquire_cycle_lock(owner, ttl_ms)? {
            Ok(Some(DbCycleLockGuard {
                db: self.clone(),
                owner: owner.trim().to_string(),
            }))
        } else {
            Ok(None)
        }
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
    /// re-running canonical migrations, and verifying `PRAGMA quick_check;` (`ME-19`).
    ///
    /// Returns the path of the pre-restore safety snapshot.
    pub fn restore_sqlite_backup(&self, backup_path: &Path) -> Result<PathBuf> {
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
        let backup_dir = self
            .db_path
            .parent()
            .unwrap_or_else(|| Path::new("data"))
            .join("backups");
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

        // 3. Drop pooled connections and stale sidecars, then restore via rusqlite::backup::Backup
        {
            let mut pool = self.conn_pool.lock();
            pool.clear();
        }
        for suffix in ["-wal", "-shm"] {
            let sidecar = PathBuf::from(format!("{}{}", self.db_path.display(), suffix));
            if sidecar.exists() {
                let _ = fs::remove_file(&sidecar);
            }
        }
        {
            let mut dst_conn = Connection::open(&self.db_path).map_err(|e| {
                AegisError::Database(format!(
                    "Failed to open target DB {} for restore: {e}",
                    self.db_path.display()
                ))
            })?;
            let backup = rusqlite::backup::Backup::new(&src_conn, &mut dst_conn).map_err(|e| {
                AegisError::Database(format!("Failed to initialize SQLite Backup: {e}"))
            })?;
            backup
                .run_to_completion(256, std::time::Duration::from_millis(5), None)
                .map_err(|e| AegisError::Database(format!("SQLite backup restore failed: {e}")))?;
        }
        drop(src_conn);

        // 4. Ensure schema parity and verify quick_check on the restored DB
        self.run_migrations()?;
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
                "SQLITE_BACKUP_RESTORED from {} (pre_restore_snapshot={})",
                backup_path.display(),
                pre_restore_path.display()
            ),
            Some(json!({
                "backup_path": backup_path.display().to_string(),
                "pre_restore_snapshot": pre_restore_path.display().to_string(),
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
