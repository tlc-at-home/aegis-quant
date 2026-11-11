//! SQLite schema initialization, transactional idempotent migration runner (001..012),
//! routing mode reconciliation, and database integrity verification.

use crate::fernet::ensure_master_key_for_db;
use crate::DatabaseManager;
use aegis_core::{resolve_project_root, AegisError, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::fs;
use std::path::Path;

/// Ordered list of SQLite schema migration files (`001..012`).
///
/// Invariants (`R5-M1`, `R7-S2`):
/// 1. Each migration file is executed atomically inside a SQLite transaction
///    (`execute_sql_transactional`), so migration scripts must not include
///    non-transactional statements such as `VACUUM` or `PRAGMA journal_mode`.
/// 2. Numbered migration files are strictly immutable once shipped; any follow-up
///    schema, config-seed, or data repairs must be placed in a new additive
///    numbered migration file (never edited retroactively).
pub const ALL_MIGRATION_FILES: &[&str] = &[
    "sql/migrations/001_v020_config_expansion.sql",
    "sql/migrations/002_v020_order_fsm.sql",
    "sql/migrations/003_v020_schema_versioning.sql",
    "sql/migrations/004_v020_rc2_remediation.sql",
    "sql/migrations/005_v030_pp50_two_book.sql",
    "sql/migrations/006_v030_review_remediation.sql",
    "sql/migrations/007_v030_round2_remediation.sql",
    "sql/migrations/008_v030_round3_remediation.sql",
    "sql/migrations/009_v030_round4_remediation.sql",
    "sql/migrations/010_v030_round5_remediation.sql",
    "sql/migrations/011_v030_round7_stop_tightening.sql",
    "sql/migrations/012_v030_round9_remediation.sql",
];

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
            .map_err(|e| AegisError::Database(format!("Failed to recover book_transfers_v9: {e}")))?;
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
        for rel in ALL_MIGRATION_FILES {
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
                if ver >= ALL_MIGRATION_FILES.len() as i64 {
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
        let root = resolve_project_root();
        let cur_ver = self.get_schema_version().unwrap_or(0).max(0) as usize;
        let mut applied = Vec::new();
        for (idx, rel) in ALL_MIGRATION_FILES.iter().enumerate() {
            let p = root.join(rel);
            if p.exists() && idx + 1 > cur_ver {
                self.apply_migration(&p)?;
                applied.push((*rel).to_string());
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

        let mut actually_executed = false;
        self.with_conn(|conn| {
            let target_num = file_name
                .get(..3)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
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
            let mut replayed_prereqs: Vec<(i64, String)> = Vec::new();
            if target_num >= 2 {
                for (idx, prereq) in ALL_MIGRATION_FILES.iter().enumerate() {
                    let prereq_num = idx + 1;
                    if prereq_num > max_applied_ver && prereq_num < target_num {
                        let p = root.join(prereq);
                        if p.exists() {
                            if let Ok(s) = fs::read_to_string(&p) {
                                Self::execute_sql_transactional(conn, &s)?;
                                let prereq_stem = p
                                    .file_stem()
                                    .and_then(|st| st.to_str())
                                    .unwrap_or(prereq)
                                    .to_string();
                                let _ = conn.execute(
                                    "INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description)
                                     VALUES (?1, CAST(strftime('%s', 'now') AS INTEGER), ?2);",
                                    params![
                                        prereq_num as i64,
                                        format!("Migration {:03}: {prereq_stem}", prereq_num)
                                    ],
                                );
                                replayed_prereqs.push((prereq_num as i64, prereq_stem));
                            }
                        }
                    }
                }
            }
            if target_num == 0 || target_num > max_applied_ver {
                Self::execute_sql_transactional(conn, &sql_script)?;
                actually_executed = true;
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

            if has_mig_table.is_some() {
                for (prereq_ver, prereq_stem) in &replayed_prereqs {
                    let _ = conn.execute(
                        "INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description)
                         VALUES (?1, CAST(strftime('%s', 'now') AS INTEGER), ?2);",
                        params![
                            prereq_ver,
                            format!("Migration {:03}: {prereq_stem}", prereq_ver)
                        ],
                    );
                }
                if file_name.len() >= 3 {
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
            }
            Ok(())
        })?;

        self.reconcile_routing_config();
        let integrity = self.verify_integrity()?;
        if actually_executed {
            let _ = self.log_telemetry(
                "INFO",
                "engine.database",
                &format!("Applied SQLite migration {file_name}"),
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
}
