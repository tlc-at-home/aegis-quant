//! MariaDB 13.0 Rolling / 12.3 LTS InnoDB Connection Pool & Schema Engine (`aegis-db::engine`).

use crate::fernet::{
    is_enforce_master_key_disabled_env, is_enforce_master_key_env, master_key_file_for_db,
    resolve_fernet_master_key, FernetCipher,
};
use aegis_core::{now_ms, resolve_project_root, AegisError, Result};
use mysql::params;
use mysql::prelude::*;
use mysql::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, PooledConn};
use parking_lot::{RwLock, RwLockReadGuard};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub const DEFAULT_MARIADB_URL: &str = "mysql://aegis:aegis_v2_local@127.0.0.1:3306/aegis_quant";
pub const CANONICAL_V2_SCHEMA_SQL: &str = include_str!("../../../sql/schema_v2_mariadb.sql");

static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(1);

fn redact_db_url(raw_url: &str) -> String {
    let s = raw_url.trim();
    if let Some(scheme_end) = s.find("://") {
        let after_scheme = scheme_end + 3;
        if let Some(at_rel) = s[after_scheme..].rfind('@') {
            let at_idx = after_scheme + at_rel;
            let userinfo = &s[after_scheme..at_idx];
            if let Some(colon_rel) = userinfo.find(':') {
                let user = &userinfo[..colon_rel];
                return format!("{}{user}:***{}", &s[..after_scheme], &s[at_idx..]);
            }
        }
    }
    s.to_string()
}

struct EphemeralDbDropGuard {
    pool: Pool,
    db_name: String,
}

impl Drop for EphemeralDbDropGuard {
    fn drop(&mut self) {
        if let Ok(mut conn) = self.pool.get_conn() {
            let _ = conn.query_drop(format!("DROP DATABASE IF EXISTS `{}`", self.db_name));
        }
    }
}

/// Thread-safe MariaDB connection pool and Fernet vault manager.
#[derive(Clone)]
pub struct DatabaseManager {
    pool: Pool,
    db_name: String,
    key_file_anchor: PathBuf,
    fernet: Arc<RwLock<FernetCipher>>,
    has_external_master_key: bool,
    ephemeral_drop_guard: Option<Arc<EphemeralDbDropGuard>>,
}

impl DatabaseManager {
    /// Connects to the production MariaDB `aegis_quant` database (or an isolated test schema if `db_hint` is `":memory:"`).
    pub fn new(db_hint: Option<&Path>, explicit_master_key: Option<&str>) -> Result<Self> {
        let hint_str = db_hint
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        if hint_str == ":memory:" || hint_str.contains("test_memory") {
            return Self::new_isolated_test(explicit_master_key);
        }

        let root = resolve_project_root();
        let anchor = db_hint
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| root.join("data").join("aegis_quant.db"));
        let (key_b64, has_ext) = resolve_fernet_master_key(explicit_master_key, Some(&anchor));
        if !has_ext && is_enforce_master_key_env() {
            return Err(AegisError::SecurityViolation(format!(
                "Fernet master key missing while AEGIS_ENFORCE_MASTER_KEY is enabled: provision '{}' or set AEGIS_MASTER_KEY",
                master_key_file_for_db(&anchor).display()
            )));
        }
        let fernet = FernetCipher::from_key_str(&key_b64)?;

        let db_url = std::env::var("AEGIS_DB_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_MARIADB_URL.to_string());

        let base_opts = Opts::from_url(&db_url).map_err(|e| {
            AegisError::Database(format!(
                "Invalid MariaDB URL '{}': {e}",
                redact_db_url(&db_url)
            ))
        })?;
        let db_name = base_opts.get_db_name().unwrap_or("aegis_quant").to_string();

        let pool = Self::build_pool(&base_opts, Some(&db_name))?;
        if !has_ext && !is_enforce_master_key_disabled_env() {
            let enforce = Self::query_enforce_master_key(&pool).unwrap_or(true);
            if enforce {
                return Err(AegisError::SecurityViolation(format!(
                    "Fernet master key missing while enforce_master_key=true: provision '{}' or set AEGIS_MASTER_KEY",
                    master_key_file_for_db(&anchor).display()
                )));
            }
        }
        Ok(Self {
            pool,
            db_name,
            key_file_anchor: anchor,
            fernet: Arc::new(RwLock::new(fernet)),
            has_external_master_key: has_ext,
            ephemeral_drop_guard: None,
        })
    }

    fn query_enforce_master_key(pool: &Pool) -> Option<bool> {
        let mut conn = pool.get_conn().ok()?;
        let raw: Option<String> = conn
            .query_first(
                "SELECT config_value FROM system_config WHERE config_key = 'enforce_master_key' LIMIT 1",
            )
            .ok()
            .flatten();
        raw.map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "on"
            )
        })
    }

    /// Creates an isolated MariaDB test database (`aegis_quant_test_<pid>_<seq>`) backed by real MariaDB 13.0 InnoDB & SQL Views.
    pub fn new_isolated_test(explicit_master_key: Option<&str>) -> Result<Self> {
        let seq = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        let ts = (aegis_core::now_ms() as u64) % 1_000_000;
        let test_db_name = format!("aegis_quant_test_{pid}_{ts}_{seq}");

        let (key_b64, has_ext) = resolve_fernet_master_key(explicit_master_key, None);
        let fernet = FernetCipher::from_key_str(&key_b64)?;

        let db_url = std::env::var("AEGIS_DB_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_MARIADB_URL.to_string());
        let base_opts = Opts::from_url(&db_url).map_err(|e| {
            AegisError::Database(format!(
                "Invalid MariaDB URL '{}': {e}",
                redact_db_url(&db_url)
            ))
        })?;

        // First connect without a default DB to create the isolated test database
        let bootstrap_pool = Self::build_pool(&base_opts, None)?;
        {
            let mut conn = bootstrap_pool
                .get_conn()
                .map_err(|e| AegisError::Database(format!("MariaDB connection error: {e}")))?;
            conn.query_drop(format!(
                "CREATE DATABASE IF NOT EXISTS `{test_db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            ))
            .map_err(|e| {
                AegisError::Database(format!(
                    "Failed to create isolated test DB `{test_db_name}`: {e}"
                ))
            })?;
        }

        let pool = Self::build_pool(&base_opts, Some(&test_db_name))?;
        let guard = Arc::new(EphemeralDbDropGuard {
            pool: pool.clone(),
            db_name: test_db_name.clone(),
        });
        let mgr = Self {
            pool,
            db_name: test_db_name,
            key_file_anchor: PathBuf::from(":memory:"),
            fernet: Arc::new(RwLock::new(fernet)),
            has_external_master_key: has_ext,
            ephemeral_drop_guard: Some(guard),
        };
        mgr.init_schema(false)?;
        Ok(mgr)
    }

    fn build_pool(base_opts: &Opts, db_name: Option<&str>) -> Result<Pool> {
        let pool_constraints = PoolConstraints::new(1, 32).unwrap_or_default();
        let pool_opts = PoolOpts::default().with_constraints(pool_constraints);
        let mut builder = OptsBuilder::from_opts(base_opts.clone())
            .tcp_nodelay(true)
            .pool_opts(pool_opts)
            .db_name(db_name.map(|s| s.to_string()));

        let socket_path = Path::new("/var/lib/mysql/mysql.sock");
        if socket_path.exists()
            && matches!(
                base_opts.get_ip_or_hostname().as_ref(),
                "127.0.0.1" | "localhost"
            )
        {
            builder = builder.socket(Some("/var/lib/mysql/mysql.sock"));
        }

        Pool::new(builder)
            .map_err(|e| AegisError::Database(format!("Failed to initialize MariaDB pool: {e}")))
    }

    pub fn get_conn(&self) -> Result<PooledConn> {
        self.pool
            .get_conn()
            .map_err(|e| AegisError::Database(format!("MariaDB pool get_conn failed: {e}")))
    }

    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    pub fn fernet(&self) -> RwLockReadGuard<'_, FernetCipher> {
        self.fernet.read()
    }

    pub(crate) fn update_fernet_cipher(&self, cipher: FernetCipher) {
        *self.fernet.write() = cipher;
    }

    pub fn has_external_master_key(&self) -> bool {
        self.has_external_master_key
    }

    pub fn key_file_anchor(&self) -> &Path {
        &self.key_file_anchor
    }

    pub fn is_ephemeral_test_db(&self) -> bool {
        self.ephemeral_drop_guard.is_some()
    }

    /// Queries MariaDB server version string (e.g. `"13.0.2-MariaDB"`).
    pub fn server_version(&self) -> Result<String> {
        let mut conn = self.get_conn()?;
        let ver: Option<String> = conn
            .query_first("SELECT VERSION()")
            .map_err(|e| AegisError::Database(format!("SELECT VERSION() failed: {e}")))?;
        Ok(ver.unwrap_or_else(|| "unknown".to_string()))
    }

    /// Initializes all 10 InnoDB tables, Foreign Keys, Composite Indexes, and 7 SQL Views,
    /// seeds default v2.0 `system_config` rows (with `risk_tier = "extreme"`), and migrates
    /// any preserved encrypted credentials from `data/aegis_quant.db` if present.
    pub fn init_schema(&self, force_recreate: bool) -> Result<()> {
        let mut conn = self.get_conn()?;

        if force_recreate {
            let views = [
                "v_priority_watchlist",
                "v_assets_sleeve_s3",
                "v_assets_sleeve_s2",
                "v_assets_sleeve_s1",
                "v_assets_in_cooldown",
                "v_assets_ordered",
                "v_assets_held",
            ];
            for v in views {
                let _ = conn.query_drop(format!("DROP VIEW IF EXISTS `{v}`"));
            }
            let tables = [
                "alert_queue",
                "telemetry_logs",
                "portfolio_snapshots",
                "order_audit",
                "positions",
                "sleeve_signals",
                "ohlcv_bars",
                "asset_cooldowns",
                "exchange_assets",
                "system_config",
            ];
            for t in tables {
                let _ = conn.query_drop(format!("DROP TABLE IF EXISTS `{t}`"));
            }
        }

        // Strip comment lines first, then split by ';' and execute statements excluding CREATE DATABASE / USE
        let uncommented_sql: String = CANONICAL_V2_SCHEMA_SQL
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");

        let mut migrated_columns = false;
        for raw_stmt in uncommented_sql.split(';') {
            let trimmed = raw_stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            let upper = trimmed.to_ascii_uppercase();
            if upper.starts_with("CREATE DATABASE") || upper.starts_with("USE ") {
                continue;
            }
            if !migrated_columns && upper.starts_with("CREATE OR REPLACE VIEW") {
                migrated_columns = true;
                conn.query_drop(
                    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS take_profit_px DOUBLE NOT NULL DEFAULT 0.0 AFTER protective_stop_oid",
                )
                .map_err(|e| AegisError::Database(format!("Schema migration take_profit_px failed: {e}")))?;
                conn.query_drop(
                    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS take_profit_oid VARCHAR(64) NULL AFTER take_profit_px",
                )
                .map_err(|e| AegisError::Database(format!("Schema migration take_profit_oid failed: {e}")))?;
                Self::migrate_order_audit_fk(&mut conn)?;
                Self::ensure_composite_indexes(&mut conn)?;
            }
            conn.query_drop(trimmed).map_err(|e| {
                AegisError::Database(format!("Schema DDL execution failed on `{trimmed}`: {e}"))
            })?;
        }

        if !migrated_columns {
            Self::migrate_order_audit_fk(&mut conn)?;
            Self::ensure_composite_indexes(&mut conn)?;
        }

        drop(conn);
        self.backfill_risk_capital_cap_config()?;
        self.seed_default_config()?;

        if !self.is_ephemeral_test_db() && self.needs_legacy_sqlite_migration()? {
            let sqlite_candidate = if self.key_file_anchor.is_file() {
                Some(self.key_file_anchor.clone())
            } else {
                let fallback = resolve_project_root().join("data").join("aegis_quant.db");
                fallback.is_file().then_some(fallback)
            };
            if let Some(sqlite_path) = sqlite_candidate {
                let _ = self.migrate_legacy_sqlite_credentials(&sqlite_path);
            }
        }

        Ok(())
    }

    fn migrate_order_audit_fk(conn: &mut PooledConn) -> Result<()> {
        let is_nullable: Option<String> = conn
            .query_first(
                r#"SELECT IS_NULLABLE
                   FROM information_schema.COLUMNS
                   WHERE TABLE_SCHEMA = DATABASE()
                     AND TABLE_NAME = 'order_audit'
                     AND COLUMN_NAME = 'position_id'
                   LIMIT 1"#,
            )
            .map_err(|e| {
                AegisError::Database(format!(
                    "Failed checking order_audit.position_id nullability: {e}"
                ))
            })?;

        let delete_rule: Option<String> = conn
            .query_first(
                r#"SELECT DELETE_RULE
                   FROM information_schema.REFERENTIAL_CONSTRAINTS
                   WHERE CONSTRAINT_SCHEMA = DATABASE()
                     AND TABLE_NAME = 'order_audit'
                     AND CONSTRAINT_NAME = 'fk_audit_position'
                   LIMIT 1"#,
            )
            .map_err(|e| {
                AegisError::Database(format!("Failed checking fk_audit_position rule: {e}"))
            })?;

        let nullable_ok = is_nullable
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case("YES"));
        let fk_ok = delete_rule
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case("SET NULL"));

        if nullable_ok && fk_ok {
            return Ok(());
        }

        if delete_rule.is_some() {
            conn.query_drop("ALTER TABLE order_audit DROP FOREIGN KEY fk_audit_position")
                .map_err(|e| {
                    AegisError::Database(format!("Failed dropping legacy fk_audit_position: {e}"))
                })?;
        }
        if !nullable_ok {
            conn.query_drop("ALTER TABLE order_audit MODIFY COLUMN position_id BIGINT NULL")
                .map_err(|e| {
                    AegisError::Database(format!("Failed making order_audit.position_id NULL: {e}"))
                })?;
        }
        conn.query_drop(
            "ALTER TABLE order_audit ADD CONSTRAINT fk_audit_position FOREIGN KEY (position_id) REFERENCES positions(id) ON DELETE SET NULL",
        )
        .map_err(|e| {
            AegisError::Database(format!("Failed adding fk_audit_position ON DELETE SET NULL: {e}"))
        })?;
        Ok(())
    }

    fn ensure_composite_indexes(conn: &mut PooledConn) -> Result<()> {
        let indexes = [
            (
                "positions",
                "idx_positions_status_closed",
                "CREATE INDEX IF NOT EXISTS idx_positions_status_closed ON positions (status, closed_at_ms DESC)",
            ),
            (
                "positions",
                "idx_positions_asset_status",
                "CREATE INDEX IF NOT EXISTS idx_positions_asset_status ON positions (asset_id, status)",
            ),
            (
                "order_audit",
                "idx_audit_created_id",
                "CREATE INDEX IF NOT EXISTS idx_audit_created_id ON order_audit (created_at_ms DESC, id DESC)",
            ),
            (
                "portfolio_snapshots",
                "idx_snapshots_created_id",
                "CREATE INDEX IF NOT EXISTS idx_snapshots_created_id ON portfolio_snapshots (created_at_ms DESC, id DESC)",
            ),
            (
                "telemetry_logs",
                "idx_telemetry_created_id",
                "CREATE INDEX IF NOT EXISTS idx_telemetry_created_id ON telemetry_logs (created_at_ms DESC, id DESC)",
            ),
            (
                "alert_queue",
                "idx_alerts_event_created",
                "CREATE INDEX IF NOT EXISTS idx_alerts_event_created ON alert_queue (event_type, created_at_ms DESC)",
            ),
            (
                "asset_cooldowns",
                "idx_cooldowns_asset_active_exp",
                "CREATE INDEX IF NOT EXISTS idx_cooldowns_asset_active_exp ON asset_cooldowns (asset_id, is_active, expires_at_ms)",
            ),
        ];

        for (table, idx_name, ddl) in indexes {
            let exists: Option<mysql::Row> = conn
                .query_first(format!(
                    "SHOW INDEX FROM `{table}` WHERE Key_name = '{idx_name}'"
                ))
                .map_err(|e| {
                    AegisError::Database(format!(
                        "SHOW INDEX failed on `{table}`.`{idx_name}`: {e}"
                    ))
                })?;
            if exists.is_none() {
                conn.query_drop(ddl).map_err(|e| {
                    AegisError::Database(format!(
                        "Failed creating index `{idx_name}` on `{table}`: {e}"
                    ))
                })?;
            }
        }
        Ok(())
    }

    fn backfill_risk_capital_cap_config(&self) -> Result<()> {
        let mut conn = self.get_conn()?;
        let cap_exists: Option<String> = conn
            .query_first(
                "SELECT config_value FROM system_config WHERE config_key = 'risk_capital_cap_usdc' LIMIT 1",
            )
            .map_err(|e| AegisError::Database(format!("backfill cap lookup failed: {e}")))?;

        if cap_exists.is_none() {
            let legacy_cap: Option<String> = conn
                .query_first(
                    "SELECT config_value FROM system_config WHERE config_key = 'live_exchange_equity_usdc' LIMIT 1",
                )
                .map_err(|e| AegisError::Database(format!("backfill legacy equity lookup failed: {e}")))?;
            if let Some(raw_val) = legacy_cap {
                if let Ok(v) = raw_val.trim().parse::<f64>() {
                    if v.is_finite() && v > 10.0 {
                        let ts = now_ms();
                        conn.exec_drop(
                            r#"INSERT IGNORE INTO system_config
                               (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
                               VALUES ('risk_capital_cap_usdc', :val, 'float', 'Optional user portfolio capital sizing cap in USDC (0 disables cap)', :ts, :ts)"#,
                            params! { "val" => raw_val.trim(), "ts" => ts },
                        )
                        .map_err(|e| {
                            AegisError::Database(format!("Failed backfilling risk_capital_cap_usdc: {e}"))
                        })?;
                    }
                }
            }
        }
        Ok(())
    }

    fn needs_legacy_sqlite_migration(&self) -> Result<bool> {
        let mut conn = self.get_conn()?;
        let creds_count: Option<i64> = conn
            .query_first(
                r#"SELECT COUNT(*) FROM system_config
                   WHERE config_key IN ('wallet_address', 'private_key', 'legacy_sqlite_migrated')
                     AND TRIM(config_value) != ''"#,
            )
            .map_err(|e| {
                AegisError::Database(format!("Failed checking MariaDB credential presence: {e}"))
            })?;
        Ok(creds_count.unwrap_or(0) == 0)
    }

    /// Lists all base tables and SQL views in the active MariaDB database.
    pub fn list_tables_and_views(&self) -> Result<(Vec<String>, Vec<String>)> {
        let mut conn = self.get_conn()?;
        let rows: Vec<(String, String)> = conn
            .query("SHOW FULL TABLES")
            .map_err(|e| AegisError::Database(format!("SHOW FULL TABLES failed: {e}")))?;
        let mut tables = Vec::new();
        let mut views = Vec::new();
        for (name, kind) in rows {
            if kind.eq_ignore_ascii_case("VIEW") {
                views.push(name);
            } else {
                tables.push(name);
            }
        }
        tables.sort();
        views.sort();
        Ok((tables, views))
    }
}
