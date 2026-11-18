//! MariaDB 13.0 Rolling / 12.3 LTS InnoDB Connection Pool & Schema Engine (`aegis-db::engine`).

use crate::fernet::{resolve_fernet_master_key, FernetCipher};
use aegis_core::{resolve_project_root, AegisError, Result};
use mysql::prelude::*;
use mysql::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, PooledConn};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub const DEFAULT_MARIADB_URL: &str = "mysql://aegis:aegis_v2_local@127.0.0.1:3306/aegis_quant";
pub const CANONICAL_V2_SCHEMA_SQL: &str = include_str!("../../../sql/schema_v2_mariadb.sql");

static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(1);

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
    fernet: FernetCipher,
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
        let fernet = FernetCipher::from_key_str(&key_b64)?;

        let db_url = std::env::var("AEGIS_DB_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_MARIADB_URL.to_string());

        let base_opts = Opts::from_url(&db_url)
            .map_err(|e| AegisError::Database(format!("Invalid MariaDB URL '{db_url}': {e}")))?;
        let db_name = base_opts.get_db_name().unwrap_or("aegis_quant").to_string();

        let pool = Self::build_pool(&base_opts, Some(&db_name))?;
        Ok(Self {
            pool,
            db_name,
            key_file_anchor: anchor,
            fernet,
            has_external_master_key: has_ext,
            ephemeral_drop_guard: None,
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
        let base_opts = Opts::from_url(&db_url)
            .map_err(|e| AegisError::Database(format!("Invalid MariaDB URL '{db_url}': {e}")))?;

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
            fernet,
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

    pub fn fernet(&self) -> &FernetCipher {
        &self.fernet
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

        for raw_stmt in uncommented_sql.split(';') {
            let trimmed = raw_stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            let upper = trimmed.to_ascii_uppercase();
            if upper.starts_with("CREATE DATABASE") || upper.starts_with("USE ") {
                continue;
            }
            conn.query_drop(trimmed).map_err(|e| {
                AegisError::Database(format!("Schema DDL execution failed on `{trimmed}`: {e}"))
            })?;
        }

        drop(conn);
        self.seed_default_config()?;

        if !self.is_ephemeral_test_db() {
            let sqlite_candidate = if self.key_file_anchor.is_file() {
                Some(self.key_file_anchor.clone())
            } else {
                let fallback = resolve_project_root().join("data").join("aegis_quant.db");
                let host_fallback = PathBuf::from("/home/tony/dev/AEGIS-LOCAL/data/aegis_quant.db");
                if fallback.is_file() {
                    Some(fallback)
                } else if host_fallback.is_file() {
                    Some(host_fallback)
                } else {
                    None
                }
            };
            if let Some(sqlite_path) = sqlite_candidate {
                let _ = self.migrate_legacy_sqlite_credentials(&sqlite_path);
            }
        }

        Ok(())
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
