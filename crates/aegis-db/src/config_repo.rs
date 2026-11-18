//! System configuration & Fernet AES-128-CBC vault repository (`aegis-db::config_repo`).

use crate::engine::DatabaseManager;
use crate::fernet::{is_encrypted_value, FernetCipher};
use aegis_core::{
    default_v2_system_config_seeds, is_sensitive_config_key, now_ms, AegisError, ConfigMap, Result,
    SystemConfigRow,
};
use mysql::params;
use mysql::prelude::*;
use std::collections::HashMap;
use std::path::Path;

impl DatabaseManager {
    /// Seeds all default v2.0 `system_config` entries (`risk_tier = "extreme"`, `target_leverage_multiplier = "1.85"`, etc.)
    /// without overwriting existing customized values.
    pub fn seed_default_config(&self) -> Result<()> {
        let mut conn = self.get_conn()?;
        let ts = now_ms();
        for (k, v, vtype, desc) in default_v2_system_config_seeds() {
            conn.exec_drop(
                r#"INSERT IGNORE INTO system_config
                   (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
                   VALUES (:key, :val, :vtype, :desc, :ts, :ts)"#,
                params! {
                    "key" => k,
                    "val" => v,
                    "vtype" => vtype,
                    "desc" => desc,
                    "ts" => ts,
                },
            )
            .map_err(|e| AegisError::Database(format!("Failed to seed config key '{k}': {e}")))?;
        }
        Ok(())
    }

    /// Automatically migrates preserved wallet, private key, Telegram, Discord, and Web API credentials
    /// from legacy SQLite `data/aegis_quant.db` into MariaDB `system_config`.
    pub fn migrate_legacy_sqlite_credentials(&self, sqlite_path: &Path) -> Result<usize> {
        if !sqlite_path.is_file() {
            return Ok(0);
        }
        let sqlite_conn = match rusqlite::Connection::open_with_flags(
            sqlite_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        ) {
            Ok(c) => c,
            Err(_) => return Ok(0),
        };

        let keys_to_preserve = [
            "wallet_address",
            "private_key",
            "hyperliquid_agent_address",
            "hyperliquid_vault_address",
            "telegram_bot_token",
            "telegram_chat_id",
            "telegram_api_base_url",
            "discord_webhook_url",
            "web_api_token",
            "execution_mode",
            "hyperliquid_testnet",
        ];

        let mut stmt = match sqlite_conn
            .prepare("SELECT config_key, config_value, value_type, description FROM system_config")
        {
            Ok(s) => s,
            Err(_) => return Ok(0),
        };

        let rows = match stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)
                    .unwrap_or_else(|_| "string".to_string()),
                row.get::<_, String>(3).unwrap_or_default(),
            ))
        }) {
            Ok(r) => r,
            Err(_) => return Ok(0),
        };

        let mut migrated = 0usize;
        let mut conn = self.get_conn()?;
        let ts = now_ms();

        for row in rows.flatten() {
            let (k, v, vtype, desc) = row;
            if !keys_to_preserve.contains(&k.as_str()) || v.trim().is_empty() {
                continue;
            }
            // Only overwrite if current MariaDB value is empty
            let current: Option<String> = conn
                .exec_first(
                    "SELECT config_value FROM system_config WHERE config_key = :k",
                    params! { "k" => &k },
                )
                .map_err(|e| AegisError::Database(format!("Config lookup error: {e}")))?;

            let should_write = match current {
                None => true,
                Some(ref cur) if cur.trim().is_empty() => true,
                _ => false,
            };

            if should_write {
                let effective_type = if is_sensitive_config_key(&k) {
                    "secret"
                } else {
                    vtype.as_str()
                };
                conn.exec_drop(
                    r#"INSERT INTO system_config
                       (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
                       VALUES (:k, :v, :vt, :d, :ts, :ts)
                       ON DUPLICATE KEY UPDATE
                       config_value = VALUES(config_value),
                       value_type = VALUES(value_type),
                       updated_at_ms = VALUES(updated_at_ms)"#,
                    params! {
                        "k" => &k,
                        "v" => &v,
                        "vt" => effective_type,
                        "d" => &desc,
                        "ts" => ts,
                    },
                )
                .map_err(|e| {
                    AegisError::Database(format!("Failed to migrate key '{k}' to MariaDB: {e}"))
                })?;
                migrated += 1;
            }
        }
        Ok(migrated)
    }

    /// Loads all `system_config` rows, transparently decrypting any Fernet-encrypted values (`gAAAAAB...`).
    pub fn load_config(&self) -> Result<ConfigMap> {
        let mut conn = self.get_conn()?;
        let rows: Vec<(String, String)> = conn
            .query("SELECT config_key, config_value FROM system_config")
            .map_err(|e| AegisError::Database(format!("Failed to query system_config: {e}")))?;

        let mut map = HashMap::with_capacity(rows.len());
        for (k, raw_v) in rows {
            let val = if is_encrypted_value(&raw_v) {
                self.fernet().decrypt(&raw_v)?
            } else {
                raw_v
            };
            map.insert(k, val);
        }
        Ok(ConfigMap::from_map(map))
    }

    /// Loads config with fallback to default `ConfigMap` on error.
    pub fn load_config_or_default(&self) -> ConfigMap {
        self.load_config().unwrap_or_default()
    }

    /// Lists raw `SystemConfigRow` entries (without decrypting secrets; suitable for Web UI display with redaction).
    pub fn list_system_config_rows(&self) -> Result<Vec<SystemConfigRow>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<(i64, String, String, String, String, i64, i64)> = conn
            .query(
                r#"SELECT id, config_key, config_value, value_type, description, created_at_ms, updated_at_ms
                   FROM system_config
                   ORDER BY config_key ASC"#,
            )
            .map_err(|e| AegisError::Database(format!("Failed to list system_config: {e}")))?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    config_key,
                    config_value,
                    value_type,
                    description,
                    created_at_ms,
                    updated_at_ms,
                )| {
                    SystemConfigRow {
                        id,
                        config_key,
                        config_value,
                        value_type,
                        description,
                        created_at_ms,
                        updated_at_ms,
                    }
                },
            )
            .collect())
    }

    /// Updates or inserts a configuration key, automatically Fernet-encrypting sensitive keys (`private_key`, `telegram_bot_token`, etc.).
    pub fn set_config_value(
        &self,
        key: &str,
        value: &str,
        description: Option<&str>,
    ) -> Result<()> {
        let clean_key = key.trim();
        if clean_key.is_empty() {
            return Err(AegisError::Validation(
                "Config key cannot be empty".to_string(),
            ));
        }
        let is_secret = is_sensitive_config_key(clean_key) && clean_key != "telegram_chat_id";
        let stored_val = if is_secret && !value.trim().is_empty() && !is_encrypted_value(value) {
            self.fernet().encrypt(value.trim().as_bytes())?
        } else {
            value.trim().to_string()
        };
        let vtype = if is_secret { "secret" } else { "string" };
        let desc = description.unwrap_or("");
        let ts = now_ms();

        let mut conn = self.get_conn()?;
        conn.exec_drop(
            r#"INSERT INTO system_config
               (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
               VALUES (:k, :v, :vt, :d, :ts, :ts)
               ON DUPLICATE KEY UPDATE
               config_value = VALUES(config_value),
               updated_at_ms = VALUES(updated_at_ms)"#,
            params! {
                "k" => clean_key,
                "v" => stored_val,
                "vt" => vtype,
                "d" => desc,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("Failed to set config '{clean_key}': {e}")))?;
        Ok(())
    }

    /// Atomically rotates the Fernet master key across all encrypted rows in `system_config`.
    pub fn rotate_fernet_master_key(&self, old_key: &str, new_key: &str) -> Result<usize> {
        let old_cipher = FernetCipher::from_key_str(old_key)?;
        let new_cipher = FernetCipher::from_key_str(new_key)?;
        let mut conn = self.get_conn()?;
        let rows: Vec<(String, String)> = conn
            .query("SELECT config_key, config_value FROM system_config")
            .map_err(|e| AegisError::Database(format!("Failed to read system_config: {e}")))?;

        let ts = now_ms();
        let mut rotated = 0usize;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("Failed to start key rotation tx: {e}")))?;

        for (k, raw_v) in rows {
            if is_encrypted_value(&raw_v) {
                let plain = old_cipher.decrypt(&raw_v)?;
                let re_enc = new_cipher.encrypt(plain.as_bytes())?;
                tx.exec_drop(
                    "UPDATE system_config SET config_value = :v, updated_at_ms = :ts WHERE config_key = :k",
                    params! { "v" => re_enc, "ts" => ts, "k" => k },
                )
                .map_err(|e| AegisError::Database(format!("Key rotation update error: {e}")))?;
                rotated += 1;
            }
        }
        tx.commit()
            .map_err(|e| AegisError::Database(format!("Key rotation commit error: {e}")))?;
        Ok(rotated)
    }
}
