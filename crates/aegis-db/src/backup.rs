//! MariaDB 13.0 Rolling / 12.3 LTS logical state backup & verification (`aegis-db::backup`).

use crate::engine::DatabaseManager;
use aegis_core::{now_ms, resolve_project_root, AegisError, Result};
use mysql::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

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
    /// Creates a point-in-time logical JSON snapshot of MariaDB `aegis_quant` tables and views in `data/backups/`.
    pub fn create_mariadb_backup(&self) -> Result<PathBuf> {
        let backup_dir = self.resolve_backup_dir();
        fs::create_dir_all(&backup_dir)?;

        let ts = now_ms();
        let filename = format!("aegis_quant_v2_mariadb_{ts}.json");
        let path = backup_dir.join(filename);

        let table_counts = self.collect_table_row_counts()?;
        let server_ver = self.server_version().unwrap_or_default();

        let payload: Value = json!({
            "version": "2.0.0",
            "database": self.db_name(),
            "server_version": server_ver,
            "timestamp_ms": ts,
            "table_counts": table_counts,
            "portfolio": self.get_latest_portfolio_snapshot()?,
            "assets_held": self.get_assets_held()?,
            "assets_ordered": self.get_assets_ordered()?,
            "assets_in_cooldown": self.get_assets_in_cooldown()?,
            "priority_watchlist": self.get_priority_watchlist()?,
        });

        fs::write(&path, serde_json::to_string_pretty(&payload)?)?;
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

    /// Verifies the latest MariaDB JSON backup file in `data/backups/`.
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

        let verified = version == "2.0.0"
            && !database.is_empty()
            && timestamp_ms > 0
            && parsed.get("portfolio").is_some();

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

    /// Collects row counts for all 10 MariaDB tables.
    pub fn collect_table_row_counts(&self) -> Result<BTreeMap<String, u64>> {
        let tables = [
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
        ];
        let mut conn = self.get_conn()?;
        let mut counts = BTreeMap::new();
        for t in tables {
            let cnt: Option<u64> = conn
                .query_first(format!("SELECT COUNT(*) FROM `{t}`"))
                .map_err(|e| AegisError::Database(format!("COUNT(*) on `{t}` failed: {e}")))?;
            counts.insert(t.to_string(), cnt.unwrap_or(0));
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
