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
use zeroize::Zeroize;

const CRITICAL_SECRET_KEYS: &[&str] = &["web_api_token", "private_key"];

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
            let (k, mut v, vtype, desc) = row;
            let clean_k = k.trim().to_ascii_lowercase();
            let trimmed_v = v.trim();
            if !keys_to_preserve.contains(&clean_k.as_str()) || trimmed_v.is_empty() {
                v.zeroize();
                continue;
            }
            // Only overwrite if current MariaDB value is empty
            let current_res: Result<Option<String>> = conn
                .exec_first(
                    "SELECT config_value FROM system_config WHERE config_key = :k",
                    params! { "k" => &clean_k },
                )
                .map_err(|e| AegisError::Database(format!("Config lookup error: {e}")));
            let current = match current_res {
                Ok(c) => c,
                Err(e) => {
                    v.zeroize();
                    return Err(e);
                }
            };

            let should_write = match current {
                None => true,
                Some(ref cur) if cur.trim().is_empty() => true,
                _ => false,
            };

            if should_write {
                let is_secret = is_sensitive_config_key(&clean_k) && clean_k != "telegram_chat_id";
                let looks_encrypted =
                    is_encrypted_value(trimmed_v) || trimmed_v.starts_with("ENC:");

                let mut stored_val = if looks_encrypted {
                    match self.fernet().decrypt(trimmed_v) {
                        Ok(mut plain) => {
                            plain.zeroize();
                            trimmed_v.to_string()
                        }
                        Err(e) => {
                            tracing::error!(
                                config_key = %clean_k,
                                error = %e,
                                "Skipping undecryptable legacy SQLite credential during migration"
                            );
                            v.zeroize();
                            continue;
                        }
                    }
                } else if is_secret {
                    match self.fernet().encrypt(trimmed_v.as_bytes()) {
                        Ok(enc) => enc,
                        Err(e) => {
                            v.zeroize();
                            return Err(e);
                        }
                    }
                } else {
                    trimmed_v.to_string()
                };

                let effective_type = if is_secret {
                    "secret"
                } else if clean_k == "telegram_chat_id" {
                    "string"
                } else {
                    vtype.as_str()
                };
                let write_res = conn
                    .exec_drop(
                        r#"INSERT INTO system_config
                           (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
                           VALUES (:k, :v, :vt, :d, :ts, :ts)
                           ON DUPLICATE KEY UPDATE
                           config_key = VALUES(config_key),
                           config_value = VALUES(config_value),
                           value_type = VALUES(value_type),
                           updated_at_ms = VALUES(updated_at_ms)"#,
                        params! {
                            "k" => &clean_k,
                            "v" => &stored_val,
                            "vt" => effective_type,
                            "d" => &desc,
                            "ts" => ts,
                        },
                    )
                    .map_err(|e| {
                        AegisError::Database(format!(
                            "Failed to migrate key '{clean_k}' to MariaDB: {e}"
                        ))
                    });
                stored_val.zeroize();
                v.zeroize();
                write_res?;
                migrated += 1;
            } else {
                v.zeroize();
            }
        }
        Ok(migrated)
    }

    /// Loads all `system_config` rows, transparently decrypting any Fernet-encrypted values (`gAAAAAB...`).
    /// Fails closed for critical secrets (`web_api_token`, `private_key`), while isolating non-critical
    /// per-row decryption failures so a single corrupt non-critical entry never drops the whole map.
    pub fn load_config(&self) -> Result<ConfigMap> {
        let mut conn = self.get_conn()?;
        let rows: Vec<(String, String)> = conn
            .query("SELECT config_key, config_value FROM system_config")
            .map_err(|e| AegisError::Database(format!("Failed to query system_config: {e}")))?;

        let mut map = HashMap::with_capacity(rows.len());
        let cipher = self.fernet();
        for (k, raw_v) in rows {
            let clean_k = k.trim().to_ascii_lowercase();
            if is_encrypted_value(&raw_v) || raw_v.trim().starts_with("ENC:") {
                match cipher.decrypt(&raw_v) {
                    Ok(val) => {
                        map.insert(clean_k, val);
                    }
                    Err(e) => {
                        if CRITICAL_SECRET_KEYS.contains(&clean_k.as_str()) {
                            for (_, mut v) in map {
                                v.zeroize();
                            }
                            return Err(AegisError::Crypto(format!(
                                "Failed to decrypt critical secret '{clean_k}': {e}"
                            )));
                        }
                        tracing::warn!(
                            config_key = %clean_k,
                            error = %e,
                            "Skipping undecryptable system_config entry during load_config"
                        );
                    }
                }
            } else {
                map.insert(clean_k, raw_v);
            }
        }
        Ok(ConfigMap::from_map(map))
    }

    /// Loads config with fallback to default `ConfigMap` on DB query error.
    pub fn load_config_or_default(&self) -> ConfigMap {
        match self.load_config() {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::warn!(error = %e, "load_config failed; falling back to empty ConfigMap");
                ConfigMap::default()
            }
        }
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
        let clean_key = key.trim().to_ascii_lowercase();
        if clean_key.is_empty() {
            return Err(AegisError::Validation(
                "Config key cannot be empty".to_string(),
            ));
        }
        let trimmed_val = value.trim();
        let looks_encrypted = is_encrypted_value(trimmed_val) || trimmed_val.starts_with("ENC:");
        if looks_encrypted {
            let mut decrypted = self.fernet().decrypt(trimmed_val).map_err(|e| {
                AegisError::Validation(format!(
                    "Invalid or undecryptable Fernet ciphertext for '{clean_key}': {e}"
                ))
            })?;
            decrypted.zeroize();
        }
        let is_secret = is_sensitive_config_key(&clean_key) && clean_key != "telegram_chat_id";
        let stored_val = if is_secret && !trimmed_val.is_empty() && !looks_encrypted {
            self.fernet().encrypt(trimmed_val.as_bytes())?
        } else {
            trimmed_val.to_string()
        };
        let vtype = if is_secret { "secret" } else { "string" };
        let desc = description.unwrap_or("");
        let ts = now_ms();

        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("Failed to start set_config tx: {e}")))?;

        let cap_changed = if clean_key == "risk_capital_cap_usdc" {
            let prev: Option<String> = tx
                .query_first(
                    "SELECT config_value FROM system_config WHERE config_key = 'risk_capital_cap_usdc' FOR UPDATE",
                )
                .map_err(|e| AegisError::Database(format!("Failed checking prior risk_capital_cap_usdc: {e}")))?;
            match prev {
                None => true,
                Some(old_raw) => {
                    let old_t = old_raw.trim();
                    match (old_t.parse::<f64>(), trimmed_val.parse::<f64>()) {
                        (Ok(a), Ok(b)) if a.is_finite() && b.is_finite() => (a - b).abs() > 1e-9,
                        _ => old_t != trimmed_val,
                    }
                }
            }
        } else {
            false
        };

        tx.exec_drop(
            r#"INSERT INTO system_config
               (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
               VALUES (:k, :v, :vt, :d, :ts, :ts)
               ON DUPLICATE KEY UPDATE
               config_key = VALUES(config_key),
               config_value = VALUES(config_value),
               updated_at_ms = VALUES(updated_at_ms)"#,
            params! {
                "k" => &clean_key,
                "v" => stored_val,
                "vt" => vtype,
                "d" => desc,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("Failed to set config '{clean_key}': {e}")))?;

        if cap_changed {
            tx.exec_drop(
                r#"INSERT INTO system_config
                   (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
                   VALUES ('risk_capital_cap_updated_at_ms', :cap_ts, 'int', 'Cutoff timestamp (ms) updated when risk_capital_cap_usdc changes', :ts, :ts)
                   ON DUPLICATE KEY UPDATE
                   config_value = VALUES(config_value),
                   updated_at_ms = VALUES(updated_at_ms)"#,
                params! {
                    "cap_ts" => ts.to_string(),
                    "ts" => ts,
                },
            )
            .map_err(|e| {
                AegisError::Database(format!(
                    "Failed updating risk_capital_cap_updated_at_ms: {e}"
                ))
            })?;
        }

        tx.commit()
            .map_err(|e| AegisError::Database(format!("set_config_value commit failed: {e}")))?;
        Ok(())
    }

    /// Atomically rotates the Fernet master key across all encrypted rows in `system_config`
    /// and updates the active in-memory `FernetCipher`.
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
                let mut plain = old_cipher.decrypt(&raw_v)?;
                let re_enc = new_cipher.encrypt(plain.as_bytes())?;
                plain.zeroize();
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
        self.update_fernet_cipher(new_cipher);
        Ok(rotated)
    }
}
