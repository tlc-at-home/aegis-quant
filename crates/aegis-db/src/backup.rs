//! MariaDB 13.0 Rolling / 12.3 LTS logical state backup & verification (`aegis-db::backup`).

use crate::engine::{DatabaseManager, CANONICAL_TABLES};
use crate::row_decode::row_get_i64;
use aegis_core::{now_ms, resolve_project_root, AegisError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

static BACKUP_TMP_SEQ: AtomicU64 = AtomicU64::new(1);

/// Summary report returned when verifying the latest MariaDB logical state backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupVerificationReport {
    pub backup_path: PathBuf,
    pub verified: bool,
    pub version: String,
    pub database: String,
    pub server_version: String,
    pub timestamp_ms: i64,
    pub table_counts: BTreeMap<String, u64>,
}

/// Creates a point-in-time logical JSON backup of MariaDB `aegis_quant` state in `data/backups/`.
pub fn create_mariadb_backup(db: &DatabaseManager) -> Result<PathBuf> {
    db.create_mariadb_backup()
}

/// Verifies the most recent MariaDB logical JSON backup in `data/backups/`.
pub fn verify_latest_backup(db: &DatabaseManager) -> Result<BackupVerificationReport> {
    db.verify_latest_backup()
}

impl DatabaseManager {
    /// Creates a point-in-time logical JSON snapshot of all 11 MariaDB `aegis_quant` tables
    /// (including `funding_events`) and views in `data/backups/` with `0700` dir and atomic `0600` file
    /// permissions (`R14-M16`, Sec. 10).
    pub fn create_mariadb_backup(&self) -> Result<PathBuf> {
        let backup_dir = self.resolve_backup_dir();
        fs::create_dir_all(&backup_dir)?;
        #[cfg(unix)]
        {
            fs::set_permissions(&backup_dir, fs::Permissions::from_mode(0o700))?;
        }

        let ts = now_ms();
        let filename = format!("aegis_quant_v2_mariadb_{ts}.json");
        let path = backup_dir.join(&filename);
        let pid = std::process::id();
        let seq = BACKUP_TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp_path = backup_dir.join(format!(".{filename}.tmp.{pid}.{seq}"));

        let table_counts = self.collect_table_row_counts()?;
        let server_ver = self.server_version().unwrap_or_default();

        let payload: Value = json!({
            "version": aegis_core::PLATFORM_VERSION,
            "database": self.db_name(),
            "server_version": server_ver,
            "timestamp_ms": ts,
            "table_counts": table_counts,
            "portfolio": self.get_latest_portfolio_snapshot()?,
            "assets_held": self.get_assets_held()?,
            "assets_ordered": self.get_assets_ordered()?,
            "assets_in_cooldown": self.get_assets_in_cooldown()?,
            "priority_watchlist": self.get_priority_watchlist()?,
            "funding_events": self.list_all_funding_events()?,
        });
        let encoded = serde_json::to_string_pretty(&payload)?;

        let mut opts = OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            opts.mode(0o600);
        }

        let write_res = (|| -> Result<()> {
            let mut file = opts.open(&tmp_path)?;
            file.write_all(encoded.as_bytes())?;
            file.sync_all()?;
            Ok(())
        })();
        if let Err(e) = write_res {
            let _ = fs::remove_file(&tmp_path);
            return Err(e);
        }

        if let Err(e) = fs::rename(&tmp_path, &path) {
            let _ = fs::remove_file(&tmp_path);
            return Err(e.into());
        }
        #[cfg(unix)]
        {
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        }

        self.set_config_value(
            "last_db_backup_ms",
            &ts.to_string(),
            Some("Timestamp of latest v2 MariaDB state backup"),
        )?;
        Ok(path)
    }

    /// Alias for `create_mariadb_backup`.
    pub fn create_state_backup(&self) -> Result<PathBuf> {
        self.create_mariadb_backup()
    }

    /// Verifies the latest MariaDB JSON backup file in `data/backups/`, ensuring all 11 canonical
    /// tables and `funding_events` payload are present (`R14-M16`).
    pub fn verify_latest_backup(&self) -> Result<BackupVerificationReport> {
        let backup_dir = self.resolve_backup_dir();
        if !backup_dir.is_dir() {
            return Err(AegisError::Database(format!(
                "Backup directory {} does not exist",
                backup_dir.display()
            )));
        }

        let mut candidates: Vec<PathBuf> = fs::read_dir(&backup_dir)?
            .filter_map(|e| e.ok().map(|entry| entry.path()))
            .filter(|p| {
                p.is_file()
                    && p.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                        n.starts_with("aegis_quant_v2_mariadb_") && n.ends_with(".json")
                    })
            })
            .collect();

        candidates.sort();
        let latest = candidates.pop().ok_or_else(|| {
            AegisError::Database(format!(
                "No MariaDB v2 backup files found in {}",
                backup_dir.display()
            ))
        })?;

        let raw = fs::read_to_string(&latest)?;
        let parsed: Value = serde_json::from_str(&raw)?;

        let version = parsed
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let database = parsed
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let server_version = parsed
            .get("server_version")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let timestamp_ms = parsed
            .get("timestamp_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or_default();

        let mut table_counts = BTreeMap::new();
        if let Some(obj) = parsed.get("table_counts").and_then(|v| v.as_object()) {
            for (k, v) in obj {
                if let Some(cnt) = v.as_u64() {
                    table_counts.insert(k.clone(), cnt);
                }
            }
        }

        let all_tables_present = CANONICAL_TABLES
            .iter()
            .all(|t| table_counts.contains_key(*t));

        let verified = (matches!(version.as_str(), "2.0.0" | "2.1.0")
            || version == aegis_core::PLATFORM_VERSION)
            && !database.is_empty()
            && timestamp_ms > 0
            && parsed.get("portfolio").is_some()
            && parsed.get("funding_events").is_some()
            && all_tables_present;

        if !verified {
            return Err(AegisError::Database(format!(
                "Backup file {} failed structural verification",
                latest.display()
            )));
        }

        Ok(BackupVerificationReport {
            backup_path: latest,
            verified,
            version,
            database,
            server_version,
            timestamp_ms,
            table_counts,
        })
    }

    /// Collects row counts for all 11 canonical MariaDB tables (`R14-M16`).
    pub fn collect_table_row_counts(&self) -> Result<BTreeMap<String, u64>> {
        use mysql::prelude::Queryable;
        let mut conn = self.get_conn()?;
        let mut counts = BTreeMap::new();
        for t in CANONICAL_TABLES {
            let row: Option<mysql::Row> =
                conn.query_first(format!("SELECT COUNT(*) FROM `{t}`"))
                    .map_err(|e| AegisError::Database(format!("COUNT(*) on `{t}` failed: {e}")))?;
            let cnt = row
                .map(|r| row_get_i64(&r, 0, 0).max(0) as u64)
                .unwrap_or(0);
            counts.insert(t.to_string(), cnt);
        }
        Ok(counts)
    }

    fn resolve_backup_dir(&self) -> PathBuf {
        let anchor = self.key_file_anchor();
        if anchor != Path::new(":memory:") {
            if let Some(parent) = anchor.parent() {
                if parent.is_dir() {
                    return parent.join("backups");
                }
            }
        }
        resolve_project_root().join("data").join("backups")
    }
}
