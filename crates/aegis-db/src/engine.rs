//! MariaDB 13.0 Rolling / 12.3 LTS InnoDB Connection Pool & Schema Engine (`aegis-db::engine`).

use crate::fernet::{
    is_allow_legacy_passphrase_key_env, is_enforce_master_key_disabled_env,
    is_enforce_master_key_env, master_key_file_for_db, resolve_fernet_master_key,
    validate_fernet_key, FernetCipher,
};
use crate::portfolio_repo::PnlSummaryCacheEntry;
use aegis_core::{now_ms, resolve_project_root, AegisError, Result};
use mysql::params;
use mysql::prelude::*;
use mysql::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, PooledConn};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use zeroize::Zeroize;

pub const CANONICAL_V2_SCHEMA_SQL: &str = include_str!("../../../sql/schema_v2_mariadb.sql");
pub const SCHEMA_MIGRATION_VERSION: &str = "2.2.0";

/// Canonical list of all 11 InnoDB tables in topological creation order
/// (reverse order is valid FK drop order).
pub const CANONICAL_TABLES: [&str; 11] = [
    "system_config",
    "exchange_assets",
    "asset_cooldowns",
    "ohlcv_bars",
    "sleeve_signals",
    "positions",
    "order_audit",
    "portfolio_snapshots",
    "telemetry_logs",
    "alert_queue",
    "funding_events",
];

static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Authorization token required by [`DatabaseManager::reset_schema_destructive`] (`R14-H6`).
#[derive(Debug, Clone)]
pub struct DestructiveResetToken {
    confirm_db_name: Option<String>,
    ephemeral_only: bool,
}

impl DestructiveResetToken {
    /// Authorizes dropping and recreating all tables/views on a named non-ephemeral database.
    pub fn confirm_for_db(db_name: impl Into<String>) -> Self {
        Self {
            confirm_db_name: Some(db_name.into()),
            ephemeral_only: false,
        }
    }

    /// Authorizes dropping and recreating tables/views strictly on ephemeral test databases.
    pub fn ephemeral_test_only() -> Self {
        Self {
            confirm_db_name: None,
            ephemeral_only: true,
        }
    }
}

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

fn resolve_production_db_url() -> Result<String> {
    if let Ok(mut raw) = std::env::var("AEGIS_DB_URL") {
        let trimmed = raw.trim().to_string();
        raw.zeroize();
        if !trimmed.is_empty() {
            return Ok(trimmed);
        }
    }
    if let Ok(cred_dir) = std::env::var("CREDENTIALS_DIRECTORY") {
        let cred_dir = cred_dir.trim();
        if !cred_dir.is_empty() {
            let cand = Path::new(cred_dir).join("aegis_db_url");
            if cand.is_file() {
                if let Ok(mut content) = fs::read_to_string(&cand) {
                    let trimmed = content.trim().to_string();
                    content.zeroize();
                    if !trimmed.is_empty() {
                        return Ok(trimmed);
                    }
                }
            }
        }
    }
    Err(AegisError::Config(
        "Database connection URL missing: set AEGIS_DB_URL or provision $CREDENTIALS_DIRECTORY/aegis_db_url"
            .to_string(),
    ))
}

fn resolve_test_db_url() -> Result<String> {
    for var_name in ["AEGIS_TEST_DB_URL", "AEGIS_DB_URL"] {
        if let Ok(mut raw) = std::env::var(var_name) {
            let trimmed = raw.trim().to_string();
            raw.zeroize();
            if !trimmed.is_empty() {
                return Ok(trimmed);
            }
        }
    }
    Err(AegisError::Config(
        "Isolated test database URL missing: set AEGIS_TEST_DB_URL or AEGIS_DB_URL".to_string(),
    ))
}

fn isolated_test_db_name(base_opts: &Opts, pid: u32, ts: u64, seq: u64) -> String {
    if base_opts.get_db_name().map(str::trim) == Some("test") {
        format!("test_aegis_quant_{pid}_{ts}_{seq}")
    } else {
        format!("aegis_quant_test_{pid}_{ts}_{seq}")
    }
}

fn validate_explicit_master_key_if_present(explicit_master_key: Option<&str>) -> Result<()> {
    if let Some(raw) = explicit_master_key {
        if !is_allow_legacy_passphrase_key_env() {
            let _ = validate_fernet_key(raw.as_bytes())?;
        } else if raw.trim().is_empty() {
            return Err(AegisError::Crypto(
                "Explicit Fernet master key cannot be empty".to_string(),
            ));
        }
    }
    Ok(())
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
    pub(crate) pnl_cache: Arc<Mutex<Option<PnlSummaryCacheEntry>>>,
}

impl DatabaseManager {
    /// Connects to a production/configured MariaDB database (`AEGIS_DB_URL` or
    /// `$CREDENTIALS_DIRECTORY/aegis_db_url`), using `anchor` solely as the path anchor for
    /// `.aegis_master_key` and optional explicit legacy SQLite migration (`R14-M20`, `R14-L10`).
    pub fn connect(anchor: Option<&Path>, explicit_master_key: Option<&str>) -> Result<Self> {
        if anchor.is_some_and(|p| p.to_string_lossy() == ":memory:") {
            return Err(AegisError::Validation(
                "DatabaseManager::connect does not accept ':memory:'; use DatabaseManager::new_isolated_test"
                    .to_string(),
            ));
        }
        validate_explicit_master_key_if_present(explicit_master_key)?;

        let root = resolve_project_root();
        let anchor_buf = anchor
            .map(Path::to_path_buf)
            .unwrap_or_else(|| root.join("data").join("aegis_quant.db"));
        let (key_b64, has_ext) = resolve_fernet_master_key(explicit_master_key, Some(&anchor_buf));
        if !has_ext && is_enforce_master_key_env() {
            return Err(AegisError::SecurityViolation(format!(
                "Fernet master key missing while AEGIS_ENFORCE_MASTER_KEY is enabled: provision '{}' or set AEGIS_MASTER_KEY",
                master_key_file_for_db(&anchor_buf).display()
            )));
        }
        let fernet = FernetCipher::from_key_str(&key_b64)?;

        let db_url = resolve_production_db_url()?;
        let base_opts = Opts::from_url(&db_url).map_err(|e| {
            AegisError::Database(format!(
                "Invalid MariaDB URL '{}': {e}",
                redact_db_url(&db_url)
            ))
        })?;
        let db_name = base_opts
            .get_db_name()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AegisError::Config(
                    "AEGIS_DB_URL must include a database name in its path".to_string(),
                )
            })?
            .to_string();

        let pool = Self::build_pool(&base_opts, Some(&db_name))?;
        if !has_ext && !is_enforce_master_key_disabled_env() {
            let enforce = Self::query_enforce_master_key(&pool).unwrap_or(true);
            if enforce {
                return Err(AegisError::SecurityViolation(format!(
                    "Fernet master key missing while enforce_master_key=true: provision '{}' or set AEGIS_MASTER_KEY",
                    master_key_file_for_db(&anchor_buf).display()
                )));
            }
        }
        Ok(Self {
            pool,
            db_name,
            key_file_anchor: anchor_buf,
            fernet: Arc::new(RwLock::new(fernet)),
            has_external_master_key: has_ext,
            ephemeral_drop_guard: None,
            pnl_cache: Arc::new(Mutex::new(None)),
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

    /// Creates an isolated MariaDB test database (`test_aegis_quant_<pid>_<ts>_<seq>` when
    /// targeting the `test` schema, or `aegis_quant_test_<pid>_<ts>_<seq>` otherwise)
    /// backed by real MariaDB 13.0 InnoDB & SQL Views (`AEGIS_TEST_DB_URL` or `AEGIS_DB_URL`).
    pub fn new_isolated_test(explicit_master_key: Option<&str>) -> Result<Self> {
        validate_explicit_master_key_if_present(explicit_master_key)?;

        let (key_b64, has_ext) = resolve_fernet_master_key(explicit_master_key, None);
        let fernet = FernetCipher::from_key_str(&key_b64)?;

        let db_url = resolve_test_db_url()?;
        let base_opts = Opts::from_url(&db_url).map_err(|e| {
            AegisError::Database(format!(
                "Invalid MariaDB URL '{}': {e}",
                redact_db_url(&db_url)
            ))
        })?;

        let seq = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        let ts = (now_ms() as u64) % 1_000_000;
        let test_db_name = isolated_test_db_name(&base_opts, pid, ts, seq);

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
            pnl_cache: Arc::new(Mutex::new(None)),
        };
        mgr.init_schema()?;
        Ok(mgr)
    }

    fn build_pool(base_opts: &Opts, db_name: Option<&str>) -> Result<Pool> {
        let pool_constraints = PoolConstraints::new(1, 32).unwrap_or_default();
        let pool_opts = PoolOpts::default().with_constraints(pool_constraints);
        let mut builder = OptsBuilder::from_opts(base_opts.clone())
            .tcp_nodelay(true)
            .pool_opts(pool_opts)
            .db_name(db_name.map(str::to_string));

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

    pub(crate) fn fernet_write(&self) -> RwLockWriteGuard<'_, FernetCipher> {
        self.fernet.write()
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

    /// Invalidates the in-memory `compute_pnl_summary` TTL cache.
    pub fn invalidate_pnl_cache(&self) {
        *self.pnl_cache.lock() = None;
    }

    /// Queries MariaDB server version string (e.g. `"13.0.2-MariaDB"`).
    pub fn server_version(&self) -> Result<String> {
        let mut conn = self.get_conn()?;
        let ver: Option<String> = conn
            .query_first("SELECT VERSION()")
            .map_err(|e| AegisError::Database(format!("SELECT VERSION() failed: {e}")))?;
        Ok(ver.unwrap_or_else(|| "unknown".to_string()))
    }

    /// Strictly non-destructive, idempotent schema initialization: ensures all 11 InnoDB tables,
    /// Foreign Keys, `CHECK` constraints, Composite Indexes, and 7 SQL Views exist, executes
    /// versioned schema migrations (`2.2.0`), and seeds default `system_config` keys without
    /// overwriting existing customized values (`R14-H6`, `R14-L25`).
    pub fn init_schema(&self) -> Result<()> {
        let mut conn = self.get_conn()?;
        let uncommented_sql: String = CANONICAL_V2_SCHEMA_SQL
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");

        let mut migrated_before_views = false;
        for raw_stmt in uncommented_sql.split(';') {
            let trimmed = raw_stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            let upper = trimmed.to_ascii_uppercase();
            if upper.starts_with("CREATE DATABASE") || upper.starts_with("USE ") {
                continue;
            }
            if !migrated_before_views && upper.starts_with("CREATE OR REPLACE VIEW") {
                migrated_before_views = true;
                Self::migrate_order_audit_fk(&mut conn)?;
                Self::run_schema_migrations(&mut conn)?;
            }
            conn.query_drop(trimmed).map_err(|e| {
                AegisError::Database(format!("Schema DDL execution failed on `{trimmed}`: {e}"))
            })?;
        }

        if !migrated_before_views {
            Self::migrate_order_audit_fk(&mut conn)?;
            Self::run_schema_migrations(&mut conn)?;
        }

        drop(conn);
        self.backfill_risk_capital_cap_config()?;
        self.seed_default_config()?;
        self.invalidate_pnl_cache();
        Ok(())
    }

    /// Destructively drops all 7 views and all 11 canonical tables (in reverse FK order) and
    /// re-initializes the schema (`R14-H6`). Fails closed unless `token` explicitly matches
    /// [`Self::db_name`] (for non-ephemeral databases) or the database is ephemeral.
    pub fn reset_schema_destructive(&self, token: DestructiveResetToken) -> Result<()> {
        validate_destructive_reset_token(self.is_ephemeral_test_db(), &self.db_name, &token)?;

        tracing::warn!(
            db_name = %self.db_name,
            ephemeral = self.is_ephemeral_test_db(),
            "Executing destructive schema reset (DROP VIEW/TABLE)"
        );

        let mut conn = self.get_conn()?;
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
            conn.query_drop(format!("DROP VIEW IF EXISTS `{v}`"))
                .map_err(|e| AegisError::Database(format!("Failed dropping view `{v}`: {e}")))?;
        }
        for t in CANONICAL_TABLES.iter().rev() {
            conn.query_drop(format!("DROP TABLE IF EXISTS `{t}`"))
                .map_err(|e| AegisError::Database(format!("Failed dropping table `{t}`: {e}")))?;
        }
        drop(conn);

        self.init_schema()
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

    /// Idempotent versioned schema migration (`2.2.0`):
    /// - Adds `positions.entry_r0`, `take_profit_*`, `portfolio_snapshots.uncapped_equity_usdc`, `alert_queue.dispatched_at_ms`.
    /// - Backfills `positions.closed_at_ms` and `positions.entry_r0`, and pre-clamps any invalid legacy rows before `CHECK`s.
    /// - Batch-converts `DOUBLE -> DECIMAL` columns per table via `information_schema.COLUMNS`.
    /// - Adds missing `CHECK` constraints via `information_schema.TABLE_CONSTRAINTS`.
    /// - Drops the 6 redundant/dead indexes (`sleeve_signals` x5, `portfolio_snapshots.idx_snapshots_created`).
    fn run_schema_migrations(conn: &mut PooledConn) -> Result<()> {
        let applied_ver: Option<String> = conn
            .query_first(
                "SELECT config_value FROM system_config WHERE config_key = 'schema_migration_version' LIMIT 1",
            )
            .map_err(|e| {
                AegisError::Database(format!("Failed checking schema_migration_version: {e}"))
            })?;
        if applied_ver.as_deref().map(str::trim) == Some(SCHEMA_MIGRATION_VERSION) {
            return Ok(());
        }

        // 1. Ensure newly added columns exist on legacy tables
        let add_column_ddls = [
            "ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_r0 DECIMAL(20,8) NOT NULL DEFAULT 0.00000000 AFTER protective_stop_px",
            "ALTER TABLE positions ADD COLUMN IF NOT EXISTS take_profit_px DECIMAL(20,8) NOT NULL DEFAULT 0.00000000 AFTER protective_stop_oid",
            "ALTER TABLE positions ADD COLUMN IF NOT EXISTS take_profit_oid VARCHAR(64) NULL AFTER take_profit_px",
            "ALTER TABLE portfolio_snapshots ADD COLUMN IF NOT EXISTS uncapped_equity_usdc DECIMAL(20,8) NOT NULL DEFAULT 0.00000000 AFTER total_equity_usdc",
            "ALTER TABLE alert_queue ADD COLUMN IF NOT EXISTS dispatched_at_ms BIGINT NULL AFTER next_retry_ms",
        ];
        for ddl in add_column_ddls {
            conn.query_drop(ddl).map_err(|e| {
                AegisError::Database(format!("Column migration `{ddl}` failed: {e}"))
            })?;
        }

        // 2. Pre-clamp legacy invalid values and backfill closed_at_ms before DECIMAL/CHECK enforcement
        let pre_clamp_sqls = [
            "UPDATE positions SET closed_at_ms = updated_at_ms WHERE status = 'CLOSED' AND closed_at_ms IS NULL",
            r#"UPDATE positions
               SET requested_quantity = GREATEST(0, requested_quantity),
                   filled_quantity = GREATEST(0, filled_quantity),
                   entry_price = CASE WHEN entry_price <= 0 THEN 0.00000001 ELSE entry_price END,
                   current_price = GREATEST(0, current_price),
                   exit_price = CASE WHEN exit_price IS NOT NULL AND exit_price <= 0 THEN NULL ELSE exit_price END,
                   protective_stop_px = GREATEST(0, protective_stop_px),
                   entry_r0 = GREATEST(0, entry_r0),
                   take_profit_px = GREATEST(0, take_profit_px),
                   highest_price_seen = GREATEST(0, highest_price_seen),
                   lowest_price_seen = GREATEST(0, lowest_price_seen),
                   reserved_margin_usdc = GREATEST(0, reserved_margin_usdc),
                   committed_notional_usdc = GREATEST(0, committed_notional_usdc),
                   fsm_state = CASE
                       WHEN fsm_state IN ('NEW','WORKING_ALO','CHASING_ALO','CROSSING_IOC','FILLED_OPEN','STOP_TRIGGERED','CLOSED','CANCELLED')
                       THEN fsm_state ELSE 'CLOSED' END
               WHERE requested_quantity < 0 OR filled_quantity < 0 OR entry_price <= 0 OR current_price < 0
                  OR (exit_price IS NOT NULL AND exit_price <= 0) OR protective_stop_px < 0 OR entry_r0 < 0 OR take_profit_px < 0
                  OR highest_price_seen < 0 OR lowest_price_seen < 0 OR reserved_margin_usdc < 0
                  OR committed_notional_usdc < 0
                  OR fsm_state NOT IN ('NEW','WORKING_ALO','CHASING_ALO','CROSSING_IOC','FILLED_OPEN','STOP_TRIGGERED','CLOSED','CANCELLED')"#,
            r#"UPDATE exchange_assets
               SET tick_size = CASE WHEN tick_size <= 0 THEN 0.01 ELSE tick_size END,
                   lot_step_size = CASE WHEN lot_step_size <= 0 THEN 0.01 ELSE lot_step_size END,
                   max_leverage = CASE WHEN max_leverage <= 0 THEN 20.0 ELSE max_leverage END,
                   mark_price = GREATEST(0, mark_price),
                   mid_price = GREATEST(0, mid_price),
                   oracle_price = GREATEST(0, oracle_price),
                   prev_day_price = GREATEST(0, prev_day_price),
                   day_volume_usdc = GREATEST(0, day_volume_usdc),
                   open_interest_usdc = GREATEST(0, open_interest_usdc),
                   is_core_eligible = IF(is_core_eligible != 0, 1, 0),
                   is_event_eligible = IF(is_event_eligible != 0, 1, 0),
                   is_priority_watched = IF(is_priority_watched != 0, 1, 0)
               WHERE tick_size <= 0 OR lot_step_size <= 0 OR max_leverage <= 0
                  OR mark_price < 0 OR mid_price < 0 OR oracle_price < 0 OR prev_day_price < 0
                  OR day_volume_usdc < 0 OR open_interest_usdc < 0
                  OR is_core_eligible NOT IN (0,1) OR is_event_eligible NOT IN (0,1) OR is_priority_watched NOT IN (0,1)"#,
            r#"UPDATE asset_cooldowns
               SET is_active = IF(is_active != 0, 1, 0),
                   expires_at_ms = GREATEST(expires_at_ms, triggered_at_ms)
               WHERE is_active NOT IN (0,1) OR expires_at_ms < triggered_at_ms"#,
            r#"DELETE FROM ohlcv_bars
               WHERE timeframe NOT IN ('1m','5m','15m','30m','1h','2h','4h','8h','12h','1d')"#,
            r#"UPDATE ohlcv_bars
               SET open = IF(open <= 0, 0.00000001, open),
                   low = IF(low <= 0, 0.00000001, low),
                   close = IF(close <= 0, 0.00000001, close),
                   high = GREATEST(IF(high <= 0, 0.00000001, high), IF(low <= 0, 0.00000001, low)),
                   volume = GREATEST(0, volume)
               WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 OR high < low OR volume < 0"#,
            r#"UPDATE sleeve_signals
               SET mark_price = GREATEST(0, mark_price),
                   day_volume_usdc = GREATEST(0, day_volume_usdc)
               WHERE mark_price < 0 OR day_volume_usdc < 0"#,
            r#"UPDATE order_audit
               SET requested_qty = GREATEST(0, requested_qty),
                   filled_qty = GREATEST(0, filled_qty),
                   limit_price = GREATEST(0, limit_price),
                   avg_fill_price = GREATEST(0, avg_fill_price),
                   reserved_usdc = GREATEST(0, reserved_usdc),
                   committed_usdc = GREATEST(0, committed_usdc)
               WHERE requested_qty < 0 OR filled_qty < 0 OR limit_price < 0
                  OR avg_fill_price < 0 OR reserved_usdc < 0 OR committed_usdc < 0"#,
            r#"UPDATE portfolio_snapshots
               SET available_cash_usdc = GREATEST(0, available_cash_usdc),
                   reserved_order_cash_usdc = GREATEST(0, reserved_order_cash_usdc),
                   committed_notional_usdc = GREATEST(0, committed_notional_usdc),
                   gross_leverage_x = GREATEST(0, gross_leverage_x),
                   s1_gross_weight = GREATEST(0, s1_gross_weight),
                   s2_gross_weight = GREATEST(0, s2_gross_weight),
                   s3_gross_weight = GREATEST(0, s3_gross_weight),
                   working_order_count = GREATEST(0, working_order_count),
                   open_long_count = GREATEST(0, open_long_count),
                   open_short_count = GREATEST(0, open_short_count),
                   eligible_coin_count = GREATEST(0, eligible_coin_count),
                   leverage_multiplier = IF(leverage_multiplier <= 0, 1.0, leverage_multiplier),
                   market_breadth_bull = LEAST(1.0, GREATEST(0.0, market_breadth_bull))
               WHERE available_cash_usdc < 0 OR reserved_order_cash_usdc < 0 OR committed_notional_usdc < 0
                  OR gross_leverage_x < 0 OR s1_gross_weight < 0 OR s2_gross_weight < 0 OR s3_gross_weight < 0
                  OR working_order_count < 0 OR open_long_count < 0 OR open_short_count < 0
                  OR eligible_coin_count < 0 OR leverage_multiplier <= 0
                  OR market_breadth_bull < 0 OR market_breadth_bull > 1"#,
            r#"UPDATE telemetry_logs
               SET level = CASE
                   WHEN UPPER(TRIM(level)) IN ('TRACE','DEBUG','INFO','WARN','WARNING','ERROR','CRITICAL')
                   THEN UPPER(TRIM(level)) ELSE 'INFO' END
               WHERE level NOT IN ('TRACE','DEBUG','INFO','WARN','WARNING','ERROR','CRITICAL')"#,
            r#"UPDATE alert_queue
               SET status = CASE
                   WHEN UPPER(TRIM(status)) IN ('PENDING','DISPATCHING','PARTIAL','DELIVERED','SKIPPED','FAILED')
                   THEN UPPER(TRIM(status)) ELSE 'FAILED' END,
                   attempts = GREATEST(0, attempts)
               WHERE status NOT IN ('PENDING','DISPATCHING','PARTIAL','DELIVERED','SKIPPED','FAILED')
                  OR attempts < 0"#,
        ];
        for sql in pre_clamp_sqls {
            conn.query_drop(sql)
                .map_err(|e| AegisError::Database(format!("Pre-migration clamp failed: {e}")))?;
        }

        // 3. Batch DOUBLE -> DECIMAL column conversions per table
        Self::migrate_double_columns_to_decimal(conn)?;

        // 3b. Backfill frozen entry_r0 on legacy positions where entry_r0 <= 0 AND entry_price > 0
        conn.query_drop(
            r#"UPDATE positions
               SET entry_r0 = ROUND(
                   CASE
                       WHEN protective_stop_px > 0
                            AND ((side = 'LONG' AND protective_stop_px < entry_price)
                                 OR (side = 'SHORT' AND protective_stop_px > entry_price))
                       THEN ABS(entry_price - protective_stop_px)
                       WHEN take_profit_px > 0
                       THEN ABS(take_profit_px - entry_price) * (2.2 / 2.8)
                       ELSE entry_price * 0.04
                   END,
                   8
               )
               WHERE entry_r0 <= 0 AND entry_price > 0"#,
        )
        .map_err(|e| AegisError::Database(format!("Failed backfilling positions.entry_r0: {e}")))?;

        // 4. Add CHECK constraints idempotently
        Self::ensure_check_constraints(conn)?;

        // 5. Drop 6 redundant/dead indexes idempotently
        Self::drop_dead_indexes(conn)?;

        // 6. Persist applied migration version
        let ts = now_ms();
        conn.exec_drop(
            r#"INSERT INTO system_config
               (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
               VALUES ('schema_migration_version', :ver, 'string', 'Applied MariaDB relational schema migration version (read-only)', :ts, :ts)
               ON DUPLICATE KEY UPDATE
               config_value = VALUES(config_value),
               updated_at_ms = VALUES(updated_at_ms)"#,
            params! { "ver" => SCHEMA_MIGRATION_VERSION, "ts" => ts },
        )
        .map_err(|e| {
            AegisError::Database(format!("Failed recording schema_migration_version: {e}"))
        })?;

        Ok(())
    }

    fn migrate_double_columns_to_decimal(conn: &mut PooledConn) -> Result<()> {
        let targets: &[(&str, &str, &str)] = &[
            (
                "exchange_assets",
                "tick_size",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.01000000",
            ),
            (
                "exchange_assets",
                "lot_step_size",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.01000000",
            ),
            (
                "exchange_assets",
                "max_leverage",
                "DECIMAL(20,8) NOT NULL DEFAULT 20.00000000",
            ),
            (
                "exchange_assets",
                "mark_price",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "exchange_assets",
                "mid_price",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "exchange_assets",
                "oracle_price",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "exchange_assets",
                "funding_rate_hourly",
                "DECIMAL(20,10) NOT NULL DEFAULT 0.0000000000",
            ),
            (
                "exchange_assets",
                "day_volume_usdc",
                "DECIMAL(24,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "exchange_assets",
                "open_interest_usdc",
                "DECIMAL(24,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "exchange_assets",
                "prev_day_price",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            ("ohlcv_bars", "open", "DECIMAL(20,8) NOT NULL"),
            ("ohlcv_bars", "high", "DECIMAL(20,8) NOT NULL"),
            ("ohlcv_bars", "low", "DECIMAL(20,8) NOT NULL"),
            ("ohlcv_bars", "close", "DECIMAL(20,8) NOT NULL"),
            ("ohlcv_bars", "volume", "DECIMAL(24,8) NOT NULL"),
            (
                "ohlcv_bars",
                "funding_rate",
                "DECIMAL(20,10) NOT NULL DEFAULT 0.0000000000",
            ),
            ("sleeve_signals", "mark_price", "DECIMAL(20,8) NOT NULL"),
            (
                "sleeve_signals",
                "day_volume_usdc",
                "DECIMAL(24,8) NOT NULL",
            ),
            (
                "sleeve_signals",
                "funding_rate_hourly",
                "DECIMAL(20,10) NOT NULL",
            ),
            (
                "sleeve_signals",
                "w_s1_carry_mom",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "sleeve_signals",
                "w_s2_trend_breakout",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "sleeve_signals",
                "w_s3_event_reversion",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "sleeve_signals",
                "raw_combined_weight",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "sleeve_signals",
                "target_weight",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "sleeve_signals",
                "current_weight",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "sleeve_signals",
                "delta_weight",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            ("positions", "entry_price", "DECIMAL(20,8) NOT NULL"),
            ("positions", "current_price", "DECIMAL(20,8) NOT NULL"),
            ("positions", "exit_price", "DECIMAL(20,8) NULL"),
            ("positions", "requested_quantity", "DECIMAL(20,8) NOT NULL"),
            (
                "positions",
                "filled_quantity",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "reserved_margin_usdc",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "committed_notional_usdc",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "portfolio_weight",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "s1_weight_contrib",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "s2_weight_contrib",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "s3_weight_contrib",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "protective_stop_px",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "entry_r0",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "take_profit_px",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "highest_price_seen",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "lowest_price_seen",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "realized_pnl_usdc",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "unrealized_pnl_usdc",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "fees_paid_usdc",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "funding_accrued_usdc",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "positions",
                "slippage_paid_usdc",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            ("order_audit", "requested_qty", "DECIMAL(20,8) NOT NULL"),
            ("order_audit", "filled_qty", "DECIMAL(20,8) NOT NULL"),
            ("order_audit", "limit_price", "DECIMAL(20,8) NOT NULL"),
            ("order_audit", "avg_fill_price", "DECIMAL(20,8) NOT NULL"),
            (
                "order_audit",
                "reserved_usdc",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "order_audit",
                "committed_usdc",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "order_audit",
                "fees_usdc",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "portfolio_snapshots",
                "total_equity_usdc",
                "DECIMAL(20,8) NOT NULL",
            ),
            (
                "portfolio_snapshots",
                "uncapped_equity_usdc",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "portfolio_snapshots",
                "available_cash_usdc",
                "DECIMAL(20,8) NOT NULL",
            ),
            (
                "portfolio_snapshots",
                "reserved_order_cash_usdc",
                "DECIMAL(20,8) NOT NULL",
            ),
            (
                "portfolio_snapshots",
                "committed_notional_usdc",
                "DECIMAL(20,8) NOT NULL",
            ),
            (
                "portfolio_snapshots",
                "net_notional_usdc",
                "DECIMAL(20,8) NOT NULL",
            ),
            (
                "portfolio_snapshots",
                "gross_leverage_x",
                "DECIMAL(20,8) NOT NULL",
            ),
            (
                "portfolio_snapshots",
                "net_leverage_x",
                "DECIMAL(20,8) NOT NULL",
            ),
            (
                "portfolio_snapshots",
                "market_beta_exposure",
                "DECIMAL(20,8) NOT NULL",
            ),
            (
                "portfolio_snapshots",
                "market_breadth_bull",
                "DECIMAL(20,8) NOT NULL",
            ),
            (
                "portfolio_snapshots",
                "leverage_multiplier",
                "DECIMAL(20,8) NOT NULL",
            ),
            (
                "portfolio_snapshots",
                "s1_gross_weight",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "portfolio_snapshots",
                "s2_gross_weight",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "portfolio_snapshots",
                "s3_gross_weight",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            ("funding_events", "usdc", "DECIMAL(20,8) NOT NULL"),
            ("funding_events", "remaining_usdc", "DECIMAL(20,8) NOT NULL"),
            (
                "funding_events",
                "position_szi",
                "DECIMAL(20,8) NOT NULL DEFAULT 0.00000000",
            ),
            (
                "funding_events",
                "funding_rate",
                "DECIMAL(20,10) NOT NULL DEFAULT 0.0000000000",
            ),
        ];

        let col_rows: Vec<(String, String, String)> = conn
            .query(
                r#"SELECT TABLE_NAME, COLUMN_NAME, LOWER(DATA_TYPE)
                   FROM information_schema.COLUMNS
                   WHERE TABLE_SCHEMA = DATABASE()"#,
            )
            .map_err(|e| {
                AegisError::Database(format!("Failed querying information_schema.COLUMNS: {e}"))
            })?;

        let col_types: HashMap<(String, String), String> = col_rows
            .into_iter()
            .map(|(t, c, dt)| ((t, c), dt))
            .collect();

        let mut alters_by_table: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for &(table, col, def) in targets {
            let key = (table.to_string(), col.to_string());
            if col_types.get(&key).is_some_and(|dt| dt == "double") {
                alters_by_table
                    .entry(table)
                    .or_default()
                    .push(format!("MODIFY COLUMN `{col}` {def}"));
            }
        }

        for (table, clauses) in alters_by_table {
            let ddl = format!("ALTER TABLE `{table}` {}", clauses.join(", "));
            conn.query_drop(&ddl).map_err(|e| {
                AegisError::Database(format!(
                    "DOUBLE -> DECIMAL migration failed on `{table}`: {e}"
                ))
            })?;
        }
        Ok(())
    }

    fn ensure_check_constraints(conn: &mut PooledConn) -> Result<()> {
        let checks: &[(&str, &str, &str)] = &[
            (
                "exchange_assets",
                "chk_assets_positive",
                "tick_size > 0 AND lot_step_size > 0 AND max_leverage > 0",
            ),
            (
                "exchange_assets",
                "chk_assets_nonneg",
                "mark_price >= 0 AND mid_price >= 0 AND oracle_price >= 0 AND prev_day_price >= 0 AND day_volume_usdc >= 0 AND open_interest_usdc >= 0",
            ),
            (
                "exchange_assets",
                "chk_assets_flags",
                "is_core_eligible IN (0, 1) AND is_event_eligible IN (0, 1) AND is_priority_watched IN (0, 1)",
            ),
            ("asset_cooldowns", "chk_cooldowns_active", "is_active IN (0, 1)"),
            (
                "asset_cooldowns",
                "chk_cooldowns_window",
                "expires_at_ms >= triggered_at_ms",
            ),
            (
                "ohlcv_bars",
                "chk_bars_ohlcv",
                "open > 0 AND high > 0 AND low > 0 AND close > 0 AND high >= low AND volume >= 0",
            ),
            (
                "ohlcv_bars",
                "chk_bars_timeframe",
                "timeframe IN ('1m','5m','15m','30m','1h','2h','4h','8h','12h','1d')",
            ),
            (
                "sleeve_signals",
                "chk_signals_nonneg",
                "mark_price >= 0 AND day_volume_usdc >= 0",
            ),
            (
                "positions",
                "chk_positions_qty",
                "requested_quantity >= 0 AND filled_quantity >= 0",
            ),
            (
                "positions",
                "chk_positions_prices",
                "entry_price > 0 AND current_price >= 0 AND (exit_price IS NULL OR exit_price > 0) AND protective_stop_px >= 0 AND take_profit_px >= 0 AND highest_price_seen >= 0 AND lowest_price_seen >= 0",
            ),
            (
                "positions",
                "chk_positions_margin",
                "reserved_margin_usdc >= 0 AND committed_notional_usdc >= 0",
            ),
            ("positions", "chk_positions_entry_r0", "entry_r0 >= 0"),
            (
                "positions",
                "chk_positions_fsm",
                "fsm_state IN ('NEW','WORKING_ALO','CHASING_ALO','CROSSING_IOC','FILLED_OPEN','STOP_TRIGGERED','CLOSED','CANCELLED')",
            ),
            (
                "order_audit",
                "chk_audit_nonneg",
                "requested_qty >= 0 AND filled_qty >= 0 AND limit_price >= 0 AND avg_fill_price >= 0 AND reserved_usdc >= 0 AND committed_usdc >= 0",
            ),
            (
                "portfolio_snapshots",
                "chk_snapshots_nonneg",
                "available_cash_usdc >= 0 AND reserved_order_cash_usdc >= 0 AND committed_notional_usdc >= 0 AND gross_leverage_x >= 0 AND s1_gross_weight >= 0 AND s2_gross_weight >= 0 AND s3_gross_weight >= 0 AND working_order_count >= 0 AND open_long_count >= 0 AND open_short_count >= 0 AND eligible_coin_count >= 0",
            ),
            (
                "portfolio_snapshots",
                "chk_snapshots_lev_mult",
                "leverage_multiplier > 0",
            ),
            (
                "portfolio_snapshots",
                "chk_snapshots_breadth",
                "market_breadth_bull BETWEEN 0 AND 1",
            ),
            (
                "telemetry_logs",
                "chk_telemetry_level",
                "level IN ('TRACE','DEBUG','INFO','WARN','WARNING','ERROR','CRITICAL')",
            ),
            (
                "alert_queue",
                "chk_alerts_status",
                "status IN ('PENDING','DISPATCHING','PARTIAL','DELIVERED','SKIPPED','FAILED')",
            ),
            ("alert_queue", "chk_alerts_attempts", "attempts >= 0"),
        ];

        let existing_rows: Vec<(String, String)> = conn
            .query(
                r#"SELECT TABLE_NAME, CONSTRAINT_NAME
                   FROM information_schema.TABLE_CONSTRAINTS
                   WHERE CONSTRAINT_SCHEMA = DATABASE()
                     AND CONSTRAINT_TYPE = 'CHECK'"#,
            )
            .map_err(|e| AegisError::Database(format!("Failed querying CHECK constraints: {e}")))?;
        let existing: HashSet<(String, String)> = existing_rows.into_iter().collect();

        let mut alters_by_table: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for &(table, name, expr) in checks {
            if !existing.contains(&(table.to_string(), name.to_string())) {
                alters_by_table
                    .entry(table)
                    .or_default()
                    .push(format!("ADD CONSTRAINT `{name}` CHECK ({expr})"));
            }
        }

        for (table, clauses) in alters_by_table {
            let ddl = format!("ALTER TABLE `{table}` {}", clauses.join(", "));
            conn.query_drop(&ddl).map_err(|e| {
                AegisError::Database(format!("Adding CHECK constraints on `{table}` failed: {e}"))
            })?;
        }
        Ok(())
    }

    fn drop_dead_indexes(conn: &mut PooledConn) -> Result<()> {
        let dead_indexes: &[(&str, &str)] = &[
            ("sleeve_signals", "idx_signals_target_weight"),
            ("sleeve_signals", "idx_signals_s1"),
            ("sleeve_signals", "idx_signals_s2"),
            ("sleeve_signals", "idx_signals_s3"),
            ("sleeve_signals", "idx_signals_event_trigger"),
            ("portfolio_snapshots", "idx_snapshots_created"),
        ];

        let existing_rows: Vec<(String, String)> = conn
            .query(
                r#"SELECT DISTINCT TABLE_NAME, INDEX_NAME
                   FROM information_schema.STATISTICS
                   WHERE TABLE_SCHEMA = DATABASE()"#,
            )
            .map_err(|e| {
                AegisError::Database(format!(
                    "Failed querying information_schema.STATISTICS: {e}"
                ))
            })?;
        let existing: HashSet<(String, String)> = existing_rows.into_iter().collect();

        for &(table, idx) in dead_indexes {
            if existing.contains(&(table.to_string(), idx.to_string())) {
                conn.query_drop(format!("DROP INDEX `{idx}` ON `{table}`"))
                    .map_err(|e| {
                        AegisError::Database(format!(
                            "Failed dropping dead index `{idx}` on `{table}`: {e}"
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

    /// Lists all base tables and SQL views in the active MariaDB database.
    pub fn list_tables_and_views(&self) -> Result<(Vec<String>, Vec<String>)> {
        let mut conn = self.get_conn()?;
        Self::list_tables_and_views_with_conn(&mut conn)
    }

    pub(crate) fn list_tables_and_views_with_conn(
        conn: &mut PooledConn,
    ) -> Result<(Vec<String>, Vec<String>)> {
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

pub(crate) fn validate_destructive_reset_token(
    is_ephemeral: bool,
    db_name: &str,
    token: &DestructiveResetToken,
) -> Result<()> {
    if is_ephemeral {
        return Ok(());
    }
    if token.ephemeral_only {
        return Err(AegisError::SecurityViolation(
            "Refusing destructive schema reset on non-ephemeral database with ephemeral_test_only token"
                .to_string(),
        ));
    }
    match token.confirm_db_name.as_deref() {
        Some(name) if name == db_name => Ok(()),
        Some(other) => Err(AegisError::SecurityViolation(format!(
            "Destructive schema reset rejected: token db_name '{other}' does not match '{db_name}'"
        ))),
        None => Err(AegisError::SecurityViolation(
            "Destructive schema reset requires DestructiveResetToken::confirm_for_db".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_r14_h6_validate_destructive_reset_token_guards_non_ephemeral() {
        let ephemeral_only = DestructiveResetToken::ephemeral_test_only();
        assert!(matches!(
            validate_destructive_reset_token(false, "aegis_quant", &ephemeral_only),
            Err(AegisError::SecurityViolation(_))
        ));

        let wrong_db = DestructiveResetToken::confirm_for_db("other_db");
        assert!(matches!(
            validate_destructive_reset_token(false, "aegis_quant", &wrong_db),
            Err(AegisError::SecurityViolation(_))
        ));

        let matching_db = DestructiveResetToken::confirm_for_db("aegis_quant");
        assert!(validate_destructive_reset_token(false, "aegis_quant", &matching_db).is_ok());

        assert!(validate_destructive_reset_token(true, "aegis_test_42", &ephemeral_only).is_ok());
    }

    #[test]
    fn test_r14_l10_connect_rejects_in_memory_anchor_and_invalid_explicit_key() {
        assert!(DatabaseManager::connect(Some(Path::new(":memory:")), None).is_err());
        assert!(DatabaseManager::connect(Some(Path::new("test_memory.db")), None).is_err());
        assert!(DatabaseManager::connect(None, Some("not-a-valid-fernet-key")).is_err());
        assert!(DatabaseManager::new_isolated_test(Some("not-a-valid-fernet-key")).is_err());
    }

    #[test]
    fn test_isolated_test_db_name_uses_test_prefix_only_for_test_schema() {
        let test_opts = Opts::from_url("mysql://localhost/test").unwrap();
        assert_eq!(
            isolated_test_db_name(&test_opts, 12, 34, 5),
            "test_aegis_quant_12_34_5"
        );

        let ci_opts = Opts::from_url("mysql://aegis:pw@localhost/aegis_quant").unwrap();
        assert_eq!(
            isolated_test_db_name(&ci_opts, 12, 34, 5),
            "aegis_quant_test_12_34_5"
        );

        let root_opts = Opts::from_url("mysql://localhost").unwrap();
        assert_eq!(
            isolated_test_db_name(&root_opts, 12, 34, 5),
            "aegis_quant_test_12_34_5"
        );
    }
}
