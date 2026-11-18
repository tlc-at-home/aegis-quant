//! Asset cooldowns repository (`aegis-db::cooldowns_repo`).

use crate::engine::DatabaseManager;
use aegis_core::{now_ms, AegisError, Result};
use mysql::params;
use mysql::prelude::*;

impl DatabaseManager {
    /// Inserts a post-stop, volatility shock, or churn-guard cooldown for `asset_id`.
    pub fn insert_asset_cooldown(
        &self,
        asset_id: i64,
        cooldown_type: &str,
        reason: &str,
        duration_sec: i64,
    ) -> Result<i64> {
        let ts = now_ms();
        let expires_at_ms = ts + duration_sec.max(1) * 1000;
        let mut conn = self.get_conn()?;

        conn.exec_drop(
            r#"INSERT INTO asset_cooldowns
               (asset_id, cooldown_type, reason, triggered_at_ms, expires_at_ms, is_active, created_at_ms, updated_at_ms)
               VALUES (:aid, :ctype, :reason, :ts, :exp, 1, :ts, :ts)"#,
            params! {
                "aid" => asset_id,
                "ctype" => cooldown_type,
                "reason" => reason,
                "ts" => ts,
                "exp" => expires_at_ms,
            },
        )
        .map_err(|e| AegisError::Database(format!("insert_asset_cooldown failed: {e}")))?;

        Ok(conn.last_insert_id() as i64)
    }

    /// Marks any expired cooldowns (`expires_at_ms <= now_ms()`) as `is_active = 0`.
    pub fn expire_finished_cooldowns(&self) -> Result<u64> {
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        conn.exec_drop(
            r#"UPDATE asset_cooldowns
               SET is_active = 0, updated_at_ms = :ts
               WHERE is_active = 1 AND expires_at_ms <= :ts"#,
            params! { "ts" => ts },
        )
        .map_err(|e| AegisError::Database(format!("expire_finished_cooldowns failed: {e}")))?;
        Ok(conn.affected_rows())
    }

    /// Returns true if `asset_id` currently has an active, non-expired cooldown.
    pub fn is_asset_in_cooldown(&self, asset_id: i64) -> Result<bool> {
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let count: Option<i64> = conn
            .exec_first(
                r#"SELECT COUNT(*) FROM asset_cooldowns
                   WHERE asset_id = :aid AND is_active = 1 AND expires_at_ms > :ts"#,
                params! { "aid" => asset_id, "ts" => ts },
            )
            .map_err(|e| AegisError::Database(format!("is_asset_in_cooldown failed: {e}")))?;
        Ok(count.unwrap_or(0) > 0)
    }
}
