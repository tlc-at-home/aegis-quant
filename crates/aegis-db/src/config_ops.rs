//! Centralized `system_config` loader, credential/routing validation, atomic Hyperliquid nonce
//! allocation (`NH-11`), and TTL-bounded settling mutex lock (`NH-10`, `NM-3`).

use crate::fernet::is_encrypted_value;
use crate::DatabaseManager;
use aegis_core::{
    derive_evm_address_from_private_key, mask_identifier, mask_secret, round_dp,
    validate_evm_address, validate_private_key, verify_signer_matches_wallet, AegisError,
    ExecutionMode, PondDirectionPolicy, Result, IMMUTABLE_SECURITY_CONFIG_KEYS,
    SENSITIVE_CONFIG_KEYS,
};
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use zeroize::Zeroizing;

pub use aegis_core::ConfigMap;

pub fn cast_config_value(raw_val: &str, value_type: &str) -> Value {
    let vtype = value_type.trim().to_ascii_lowercase();
    match vtype.as_str() {
        "int" => {
            let f = raw_val.trim().parse::<f64>().unwrap_or(0.0);
            json!(if f.is_finite() { f as i64 } else { 0 })
        }
        "float" => {
            let f = raw_val.trim().parse::<f64>().unwrap_or(0.0);
            json!(if f.is_finite() { f } else { 0.0 })
        }
        "bool" => {
            let b = matches!(
                raw_val.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "on"
            );
            json!(b)
        }
        _ => json!(raw_val),
    }
}

fn alert_config_defaults(
) -> HashMap<&'static str, (&'static str, &'static str, &'static str, &'static str)> {
    HashMap::from([
        (
            "telegram_bot_token",
            (
                "",
                "string",
                "alerts",
                "Telegram Bot API token for real-time operational notifications (Fernet AES-128 encrypted)",
            ),
        ),
        (
            "telegram_chat_id",
            (
                "",
                "string",
                "alerts",
                "Telegram target chat ID for real-time operational notifications",
            ),
        ),
        (
            "discord_webhook_url",
            (
                "",
                "string",
                "alerts",
                "Discord webhook URL for real-time operational notifications (Fernet AES-128 encrypted)",
            ),
        ),
        (
            "max_daily_drawdown_alert_pct",
            (
                "0.03",
                "float",
                "alerts",
                "Daily portfolio drawdown threshold (3.0%) triggering critical Telegram alert",
            ),
        ),
        (
            "hyperliquid_info_url",
            (
                "",
                "string",
                "execution",
                "Optional override for Hyperliquid Mainnet /info endpoint URL",
            ),
        ),
        (
            "hyperliquid_testnet_info_url",
            (
                "",
                "string",
                "execution",
                "Optional override for Hyperliquid Testnet /info endpoint URL",
            ),
        ),
        (
            "hyperliquid_exchange_url",
            (
                "",
                "string",
                "execution",
                "Optional override for Hyperliquid Mainnet /exchange endpoint URL",
            ),
        ),
        (
            "hyperliquid_testnet_exchange_url",
            (
                "",
                "string",
                "execution",
                "Optional override for Hyperliquid Testnet /exchange endpoint URL",
            ),
        ),
        (
            "hyperliquid_mainnet_api_url",
            (
                "",
                "string",
                "execution",
                "Optional override for Hyperliquid Mainnet API base URL",
            ),
        ),
        (
            "hyperliquid_testnet_api_url",
            (
                "",
                "string",
                "execution",
                "Optional override for Hyperliquid Testnet API base URL",
            ),
        ),
        (
            "hyperliquid_mainnet_url",
            (
                "https://api.hyperliquid.xyz",
                "string",
                "execution",
                "Hyperliquid L1 Mainnet REST API base URL",
            ),
        ),
        (
            "hyperliquid_testnet_url",
            (
                "https://api.hyperliquid-testnet.xyz",
                "string",
                "execution",
                "Hyperliquid L1 Testnet REST API base URL",
            ),
        ),
        (
            "equity_baseline_reset_at",
            (
                "",
                "string",
                "risk",
                "ISO-8601 timestamp of last manual equity baseline reset",
            ),
        ),
        (
            "risk_caps_reference_scope",
            (
                "BOOK",
                "string",
                "risk",
                "Reference equity scope for PIT dollar risk caps: BOOK (50% split) or PLATFORM (100%)",
            ),
        ),
        (
            "hyperliquid_agent_address",
            (
                "",
                "string",
                "credentials",
                "Optional delegated Hyperliquid API Agent address when signing with an agent key",
            ),
        ),
        (
            "web_api_token",
            (
                "",
                "string",
                "credentials",
                "Optional static Web API Bearer token (Fernet-encrypted at rest when set)",
            ),
        ),
        (
            "dashboard_api_token",
            (
                "",
                "string",
                "credentials",
                "Optional legacy dashboard API token",
            ),
        ),
        (
            "settling_deadband_bps",
            (
                "50.0",
                "float",
                "settling",
                "Dead-band tolerance in basis points before capital transfer is triggered",
            ),
        ),
        (
            "web_allow_remote_bind",
            (
                "false",
                "bool",
                "security",
                "Allow binding the web server to non-loopback interfaces",
            ),
        ),
        (
            "web_cors_allowed_origins",
            (
                "",
                "string",
                "security",
                "Comma-separated allowed CORS origins for the web dashboard",
            ),
        ),
        (
            "web_cookie_secure",
            (
                "false",
                "bool",
                "security",
                "Enforce Secure flag on session cookies",
            ),
        ),
        (
            "calibration_timeframes_csv",
            (
                "1h,4h",
                "string",
                "calibration",
                "Comma-separated list of candidate timeframes evaluated during walk-forward calibration",
            ),
        ),
        (
            "default_timeframe",
            (
                "1h",
                "string",
                "execution",
                "Default execution bar timeframe",
            ),
        ),
        (
            "exec_timeframe",
            (
                "1h",
                "string",
                "execution",
                "Default per-asset execution timeframe",
            ),
        ),
        (
            "rs_timeframe",
            (
                "4h",
                "string",
                "signals",
                "Default relative-strength timeframe",
            ),
        ),
        (
            "chandelier_atr_multiplier_long",
            (
                "1.2",
                "float",
                "stops",
                "Chandelier trailing stop ATR multiplier for Long positions",
            ),
        ),
        (
            "chandelier_atr_multiplier_short",
            (
                "1.2",
                "float",
                "stops",
                "Inverted Chandelier trailing stop ATR multiplier for Short positions",
            ),
        ),
        (
            "chandelier_min_atr_pct",
            (
                "0.004",
                "float",
                "stops",
                "Minimum effective ATR floor as fraction of price (0.4%)",
            ),
        ),
        (
            "chandelier_wick_buffer_atr",
            (
                "0.35",
                "float",
                "stops",
                "Intrabar wick overshoot buffer in ATR multiples before Tier-1 emergency stop",
            ),
        ),
        (
            "chandelier_wick_overshoot_atr",
            (
                "0.35",
                "float",
                "stops",
                "Intrabar wick overshoot buffer in ATR multiples before Tier-1 emergency stop",
            ),
        ),
        (
            "chandelier_trail_atr_multiplier",
            (
                "0.45",
                "float",
                "stops",
                "Tight current-price trailing stop distance in ATR multiples",
            ),
        ),
        (
            "chandelier_trail_max_pct",
            (
                "0.008",
                "float",
                "stops",
                "Maximum trailing stop distance as fraction of current price (0.8%)",
            ),
        ),
        (
            "orphan_reconcile_stop_pct",
            (
                "0.015",
                "float",
                "stops",
                "Initial protective stop distance fraction for reconciled on-chain orphan positions (1.5%)",
            ),
        ),
        (
            "long_break_even_trigger_atr",
            (
                "0.8",
                "float",
                "stops",
                "Favorable excursion in ATR multiples to lock break-even on Long positions",
            ),
        ),
        (
            "long_break_even_lock_atr",
            (
                "0.25",
                "float",
                "stops",
                "Locked profit buffer in ATR multiples above entry once Long break-even triggers",
            ),
        ),
        (
            "short_break_even_trigger_atr",
            (
                "0.8",
                "float",
                "stops",
                "Favorable excursion in ATR multiples to lock break-even on Short positions",
            ),
        ),
        (
            "short_break_even_lock_atr",
            (
                "0.25",
                "float",
                "stops",
                "Locked profit buffer in ATR multiples below entry once Short break-even triggers",
            ),
        ),
        (
            "testnet_faucet_offset_usdc",
            (
                "10000.0",
                "float",
                "risk",
                "Unallocated Hyperliquid Testnet Faucet Spot USDC grant to exclude from Perps equity and risk-cap scaling",
            ),
        ),
    ])
}

impl DatabaseManager {
    pub fn get_raw_config_value(&self, key: &str) -> Result<Option<String>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT config_value FROM system_config WHERE config_key = ?1;",
                params![key],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| AegisError::Database(e.to_string()))
        })
    }

    pub fn load_config(&self) -> Result<ConfigMap> {
        self.reconcile_routing_config();
        self.load_config_opts(true)
    }

    /// Load `ConfigMap` and log a warning to `telemetry_logs` + `tracing` if DB loading fails (`NQ-2`, `Q8`).
    pub fn load_config_logged(&self, component: &str) -> ConfigMap {
        match self.load_config() {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::warn!(component = component, error = %e, "Failed to load system_config; using defaults");
                let _ = self.log_telemetry(
                    "WARNING",
                    component,
                    &format!("Failed to load system_config; falling back to defaults: {e}"),
                    None,
                );
                ConfigMap::default()
            }
        }
    }

    /// Alias for `load_config_logged` (`NQ-2`, `Q8`).
    pub fn load_config_or_log_default(&self, component: &str) -> ConfigMap {
        self.load_config_logged(component)
    }

    /// Resolve `settling_deadband_bps` from `ConfigMap` (`NM-2`).
    /// Prefers `settling_deadband_bps` / `settling_tolerance_bps`, or converts legacy `settling_drift_deadband_pct` when set.
    pub fn resolve_settling_deadband_bps(cfg: &ConfigMap) -> f64 {
        let deadband = cfg.get_f64("settling_deadband_bps", 50.0);
        let tol = cfg.get_f64("settling_tolerance_bps", deadband);
        if (tol - 50.0).abs() > 1e-6 && (deadband - 50.0).abs() <= 1e-6 {
            return tol.max(0.0);
        }
        if cfg.get("settling_deadband_bps").is_some() {
            return deadband.max(0.0);
        }
        if cfg.get("settling_tolerance_bps").is_some() {
            return tol.max(0.0);
        }
        if cfg.get("settling_drift_deadband_pct").is_some() {
            let pct = cfg.get_f64("settling_drift_deadband_pct", 0.005).max(0.0);
            return if pct <= 1.0 { pct * 10_000.0 } else { pct * 100.0 };
        }
        50.0
    }

    /// Allocate a strictly monotonic millisecond nonce for Hyperliquid L1 actions across Pit, Pond, and Settle (`NH-11`).
    pub fn next_hyperliquid_nonce(&self) -> Result<u64> {
        let wall_ms = chrono::Utc::now().timestamp_millis().max(1);
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN IMMEDIATE;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let res = (|| -> Result<u64> {
                let prev_str: Option<String> = conn
                    .query_row(
                        "SELECT config_value FROM system_config WHERE config_key = 'hyperliquid_last_nonce_ms';",
                        [],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let prev_nonce = prev_str
                    .and_then(|s| s.trim().parse::<i64>().ok())
                    .unwrap_or(0);
                let next_nonce = wall_ms.max(prev_nonce + 1);
                conn.execute(
                    "INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
                     VALUES ('hyperliquid_last_nonce_ms', ?1, 'int', 'execution', 'Strictly monotonic millisecond nonce counter for Hyperliquid L1 actions', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                     ON CONFLICT(config_key) DO UPDATE SET
                        config_value = excluded.config_value,
                        updated_at = excluded.updated_at;",
                    params![next_nonce.to_string()],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(next_nonce as u64)
            })();
            match res {
                Ok(n) => {
                    conn.execute_batch("COMMIT;")
                        .map_err(|e| AegisError::Database(e.to_string()))?;
                    Ok(n)
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK;");
                    Err(e)
                }
            }
        })
    }

    /// Atomically acquire the daily settling gate lock with TTL expiry protection (`NH-10`, `NM-3`).
    pub fn try_acquire_settle_lock(&self, holder: &str) -> Result<()> {
        let now_ms = self.now_ms();
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN IMMEDIATE;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let res = (|| -> Result<()> {
                let gate_raw: String = conn
                    .query_row(
                        "SELECT config_value FROM system_config WHERE config_key = 'platform_settle_gate_enabled';",
                        [],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(|e| AegisError::Database(e.to_string()))?
                    .unwrap_or_else(|| "false".to_string());
                let armed_at_ms: i64 = conn
                    .query_row(
                        "SELECT config_value FROM system_config WHERE config_key = 'platform_settle_gate_armed_at_ms';",
                        [],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|e| AegisError::Database(e.to_string()))?
                    .and_then(|s| s.trim().parse::<i64>().ok())
                    .unwrap_or(0);
                let ttl_ms: i64 = conn
                    .query_row(
                        "SELECT config_value FROM system_config WHERE config_key = 'settle_gate_ttl_ms';",
                        [],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|e| AegisError::Database(e.to_string()))?
                    .and_then(|s| s.trim().parse::<i64>().ok())
                    .unwrap_or(300_000)
                    .max(1_000);
                let cur_holder: String = conn
                    .query_row(
                        "SELECT config_value FROM system_config WHERE config_key = 'platform_settle_lock_holder';",
                        [],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(|e| AegisError::Database(e.to_string()))?
                    .unwrap_or_default();

                let is_enabled = matches!(
                    gate_raw.trim().to_ascii_lowercase().as_str(),
                    "true" | "1" | "yes" | "on"
                );
                let is_unexpired = armed_at_ms > 0 && (now_ms - armed_at_ms) < ttl_ms;
                if is_enabled && is_unexpired && !cur_holder.trim().is_empty() {
                    return Err(AegisError::Validation(format!(
                        "Daily settling lock is already held by '{cur_holder}' (armed {}ms ago, TTL {}ms).",
                        now_ms - armed_at_ms,
                        ttl_ms
                    )));
                }

                for (k, v, vt, desc) in [
                    (
                        "platform_settle_gate_enabled",
                        "true".to_string(),
                        "bool",
                        "True while the 00:05 UTC daily settling job is actively transferring USDC",
                    ),
                    (
                        "platform_settle_gate_armed_at_ms",
                        now_ms.to_string(),
                        "int",
                        "Epoch timestamp (ms) when platform_settle_gate was last armed",
                    ),
                    (
                        "platform_settle_lock_holder",
                        holder.trim().to_string(),
                        "string",
                        "Owner identifier of the active daily settling mutex lock",
                    ),
                ] {
                    conn.execute(
                        "INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
                         VALUES (?1, ?2, ?3, 'settling', ?4, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                         ON CONFLICT(config_key) DO UPDATE SET
                            config_value = excluded.config_value,
                            updated_at = excluded.updated_at;",
                        params![k, v, vt, desc],
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                }
                Ok(())
            })();
            match res {
                Ok(()) => {
                    conn.execute_batch("COMMIT;")
                        .map_err(|e| AegisError::Database(e.to_string()))?;
                    Ok(())
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK;");
                    Err(e)
                }
            }
        })
    }

    /// Release the daily settling gate lock (`NH-10`, `NM-3`).
    pub fn release_settle_lock(&self) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN IMMEDIATE;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            for (k, v) in [
                ("platform_settle_gate_enabled", "false"),
                ("platform_settle_gate_armed_at_ms", "0"),
                ("platform_settle_lock_holder", ""),
            ] {
                let _ = conn.execute(
                    "UPDATE system_config
                     SET config_value = ?1,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                     WHERE config_key = ?2;",
                    params![v, k],
                );
            }
            conn.execute_batch("COMMIT;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    /// Check whether `platform_settle_gate_enabled` is active and auto-disarm any stale lock exceeding `settle_gate_ttl_ms` (`NH-10`).
    pub fn is_settle_gate_active(&self) -> Result<bool> {
        let cfg = self.load_config_opts(false)?;
        if !cfg.get_bool("platform_settle_gate_enabled", false) {
            return Ok(false);
        }
        let armed_at_ms = cfg.get_i64("platform_settle_gate_armed_at_ms", 0);
        let ttl_ms = cfg.get_i64("settle_gate_ttl_ms", 300_000).max(1_000);
        let now_ms = self.now_ms();
        if armed_at_ms > 0 && (now_ms - armed_at_ms) >= ttl_ms {
            let _ = self.release_settle_lock();
            let _ = self.log_telemetry(
                "WARNING",
                "engine.settle",
                &format!(
                    "Auto-disarmed stale platform_settle_gate_enabled lock (armed_at_ms={armed_at_ms}, elapsed_ms={}, ttl_ms={ttl_ms})",
                    now_ms - armed_at_ms
                ),
                None,
            );
            return Ok(false);
        }
        Ok(true)
    }

    /// Convenience helper returning true when new entries are frozen by the daily settling gate (`NH-10`).
    pub fn is_entry_frozen(&self) -> bool {
        self.is_settle_gate_active().unwrap_or(false)
    }

    pub fn load_config_opts(&self, decrypt: bool) -> Result<ConfigMap> {
        let rows: Vec<(String, String, String)> = self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT config_key, config_value, value_type FROM system_config;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                        r.get::<_, Option<String>>(2)?
                            .unwrap_or_else(|| "string".to_string()),
                    ))
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })?;

        let cipher_opt = if decrypt {
            self.resolve_cached_fernet_state().ok().map(|s| s.cipher)
        } else {
            None
        };

        let mut map = ConfigMap::new();
        for (k, raw_v, vtype) in rows {
            // S10: Never decrypt `private_key` into general-purpose `ConfigMap` clones;
            // signing callers must use `get_signing_private_key()` which returns `Zeroizing<String>`.
            let eff_val = if k == "private_key" {
                raw_v
            } else if decrypt && is_encrypted_value(&raw_v) {
                let s = raw_v.trim();
                let token = s.strip_prefix("ENC:").unwrap_or(s);
                if let Some(ref c) = cipher_opt {
                    c.decrypt(token).unwrap_or_default()
                } else {
                    self.try_decrypt_or_empty(&raw_v)
                }
            } else {
                raw_v
            };
            map.insert(k, cast_config_value(&eff_val, &vtype));
        }
        Ok(map)
    }
}

const MIN_MAX_FLOAT_PAIRS: &[(&str, &str)] = &[
    ("min_risk_per_trade_usdc", "max_risk_per_trade_usdc"),
    ("pond_min_risk_usdc", "pond_max_risk_usdc"),
    ("conviction_scale_min", "conviction_scale_max"),
    ("pond_conviction_scale_min", "pond_conviction_scale_max"),
];

/// Returns `true` only when `s` matches one of the exact mask shapes emitted by
/// [`mask_secret`] (`<6 chars>...<4 chars>` or `"********"`), [`mask_identifier`]
/// (`<3 chars>...<3 chars>`, `<2 chars>***<2 chars>`, or `"****"`), or `"[REDACTED]"` (`R3-H21`, `R4-N3`).
pub fn is_masked_secret_echo(s: &str) -> bool {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed.eq_ignore_ascii_case("[REDACTED]") || trimmed == "********" || trimmed == "****"
    {
        return true;
    }
    let chars: Vec<char> = trimmed.chars().collect();
    if chars.len() == 7 && chars[2..5] == ['*', '*', '*'] {
        return true;
    }
    if chars.len() == 9 && chars[3..6] == ['.', '.', '.'] {
        return true;
    }
    if chars.len() == 13 && chars[6..9] == ['.', '.', '.'] {
        return true;
    }
    false
}

impl DatabaseManager {
    pub fn get_config_rows(&self, _mask_secrets: bool) -> Result<Vec<Value>> {
        self.reconcile_routing_config();
        let sensitive: HashSet<&str> = SENSITIVE_CONFIG_KEYS.iter().copied().collect();
        let immutable: HashSet<&str> = IMMUTABLE_SECURITY_CONFIG_KEYS.iter().copied().collect();
        let rows: Vec<(String, String, String, String, String, String)> =
            self.with_conn(|conn| {
                let mut stmt = conn
                    .prepare(
                        "SELECT config_key, config_value, value_type, category, description, updated_at
                         FROM system_config
                         ORDER BY category ASC, config_key ASC;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let iter = stmt
                    .query_map([], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                            r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                            r.get::<_, Option<String>>(3)?.unwrap_or_default(),
                            r.get::<_, Option<String>>(4)?.unwrap_or_default(),
                            r.get::<_, Option<String>>(5)?.unwrap_or_default(),
                        ))
                    })
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(iter.flatten().collect())
            })?;

        let mut out = Vec::with_capacity(rows.len());
        for (key, raw_val, vtype, category, description, updated_at) in rows {
            // R3-H21: Always mask sensitive secrets and identifiers when serializing config rows.
            let display_val = if sensitive.contains(key.as_str()) && !raw_val.is_empty() {
                let plain = Zeroizing::new(if is_encrypted_value(&raw_val) {
                    self.try_decrypt_or_empty(&raw_val)
                } else {
                    raw_val
                });
                mask_secret(&plain)
            } else if key == "telegram_chat_id" && !raw_val.is_empty() {
                mask_identifier(&raw_val)
            } else {
                raw_val
            };
            out.push(json!({
                "config_key": key,
                "config_value": display_val,
                "value_type": vtype,
                "category": category,
                "description": description,
                "updated_at": updated_at,
                "immutable": immutable.contains(key.as_str()),
            }));
        }
        Ok(out)
    }

    pub fn set_config_value(&self, key: &str, value: &Value) -> Result<bool> {
        self.set_config_value_with_opts(key, value, true)
    }

    fn set_config_value_with_opts(
        &self,
        key: &str,
        value: &Value,
        check_pair_bounds: bool,
    ) -> Result<bool> {
        let val_str = match value {
            Value::String(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            Value::Null => {
                return Err(AegisError::Validation(format!(
                    "Invalid null value for '{key}'"
                )))
            }
            other => other.to_string(),
        };

        // R3-H21 / R4-N3: Reject/ignore masked echo values so UI round-trips never overwrite real secrets.
        if (SENSITIVE_CONFIG_KEYS.contains(&key) || key == "telegram_chat_id")
            && is_masked_secret_echo(&val_str)
        {
            tracing::debug!(
                config_key = key,
                "Skipping system_config write for masked secret echo value"
            );
            return Ok(true);
        }

        // Route credential and routing-mode keys through unified validation & synchronization
        match key {
            "wallet_address" => {
                self.configure_hyperliquid_credentials(
                    Some(&val_str),
                    None,
                    None,
                    None,
                    None,
                    None,
                    false,
                )?;
                return Ok(true);
            }
            "private_key" => {
                self.configure_hyperliquid_credentials(
                    None,
                    Some(&val_str),
                    None,
                    None,
                    None,
                    None,
                    false,
                )?;
                return Ok(true);
            }
            "hyperliquid_vault_address" => {
                self.configure_hyperliquid_credentials(
                    None,
                    None,
                    None,
                    None,
                    Some(&val_str),
                    None,
                    false,
                )?;
                return Ok(true);
            }
            "gemini_api_key" => {
                self.configure_hyperliquid_credentials(
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(&val_str),
                    false,
                )?;
                return Ok(true);
            }
            "hyperliquid_testnet" => {
                let b = match value {
                    Value::Bool(b) => *b,
                    Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                        "true" | "1" | "yes" | "on" | "testnet" => true,
                        "false" | "0" | "no" | "off" | "mainnet" => false,
                        _ => {
                            return Err(AegisError::Validation(format!(
                                "Invalid hyperliquid_testnet value '{s}': use true or false."
                            )))
                        }
                    },
                    _ => {
                        return Err(AegisError::Validation(
                            "Invalid hyperliquid_testnet value: use true or false.".to_string(),
                        ))
                    }
                };
                self.configure_hyperliquid_credentials(
                    None,
                    None,
                    Some(b),
                    None,
                    None,
                    None,
                    false,
                )?;
                return Ok(true);
            }
            "execution_mode" | "pond_execution_mode" => {
                let req_creds = self.get_schema_version().unwrap_or(0) < 4;
                self.configure_hyperliquid_credentials(
                    None,
                    None,
                    None,
                    Some(&val_str),
                    None,
                    None,
                    req_creds,
                )?;
                return Ok(true);
            }
            "environment" => {
                let norm_env = val_str.trim().to_ascii_lowercase();
                if norm_env != "sandbox" && norm_env != "production" {
                    return Err(AegisError::Validation(format!(
                        "Invalid environment '{val_str}': must be 'sandbox' or 'production'."
                    )));
                }
                let cur_cfg = self.load_config_opts(false)?;
                let cur_mode = cur_cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
                let expected_env =
                    if cur_mode == "LIVE" && !cur_cfg.get_bool("hyperliquid_testnet", false) {
                        "production"
                    } else {
                        "sandbox"
                    };
                if norm_env != expected_env {
                    return Err(AegisError::Validation(format!(
                        "Conflicting configuration: environment='{norm_env}' conflicts with execution_mode='{cur_mode}' (expected '{expected_env}'). Change execution_mode instead."
                    )));
                }
            }
            "settling_drift_deadband_pct" => {
                let pct = val_str.trim().parse::<f64>().map_err(|_| {
                    AegisError::Validation(format!(
                        "Invalid float value for 'settling_drift_deadband_pct': {value}"
                    ))
                })?;
                if !pct.is_finite() || pct < 0.0 {
                    return Err(AegisError::Validation(format!(
                        "Invalid float value for 'settling_drift_deadband_pct': {value}"
                    )));
                }
                let bps = if pct <= 1.0 {
                    pct * 10_000.0
                } else {
                    pct * 100.0
                };
                return self.set_config_value_with_opts(
                    "settling_deadband_bps",
                    &json!(bps),
                    check_pair_bounds,
                );
            }
            _ => {}
        }

        let defaults = alert_config_defaults();
        let vtype_opt: Option<String> = self.with_conn(|conn| {
            let mut row: Option<String> = conn
                .query_row(
                    "SELECT value_type FROM system_config WHERE config_key = ?1;",
                    params![key],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            if row.is_none() {
                if let Some((def_val, def_type, def_cat, def_desc)) = defaults.get(key) {
                    let _ = conn.execute(
                        "INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description)
                         VALUES (?1, ?2, ?3, ?4, ?5);",
                        params![key, def_val, def_type, def_cat, def_desc],
                    );
                    row = Some((*def_type).to_string());
                }
            }
            Ok(row)
        })?;

        let Some(raw_vtype) = vtype_opt else {
            return Ok(false);
        };
        let vtype = raw_vtype.trim().to_ascii_lowercase();
        let str_val = match vtype.as_str() {
            "bool" => match value {
                Value::Bool(b) => {
                    if *b {
                        "true".to_string()
                    } else {
                        "false".to_string()
                    }
                }
                Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                    "true" | "1" | "yes" | "on" => "true".to_string(),
                    "false" | "0" | "no" | "off" => "false".to_string(),
                    _ => {
                        return Err(AegisError::Validation(format!(
                            "Invalid boolean value for '{key}': {value}"
                        )))
                    }
                },
                _ => {
                    return Err(AegisError::Validation(format!(
                        "Invalid boolean value for '{key}': {value}"
                    )))
                }
            },
            "int" => {
                if value.is_boolean() {
                    return Err(AegisError::Validation(format!(
                        "Invalid integer value for '{key}': {value}"
                    )));
                }
                let f = val_str.trim().parse::<f64>().map_err(|_| {
                    AegisError::Validation(format!("Invalid integer value for '{key}': {value}"))
                })?;
                if !f.is_finite() || f.fract() != 0.0 {
                    return Err(AegisError::Validation(format!(
                        "Invalid integer value for '{key}': {value}"
                    )));
                }
                let int_val = f as i64;
                if matches!(
                    key,
                    "polling_interval_seconds"
                        | "pit_tick_interval_seconds"
                        | "pond_tick_interval_seconds"
                ) && int_val < 1
                {
                    return Err(AegisError::Validation(format!(
                        "Invalid {key}: must be >= 1 second."
                    )));
                }
                int_val.to_string()
            }
            "float" => {
                if value.is_boolean() {
                    return Err(AegisError::Validation(format!(
                        "Invalid float value for '{key}': {value}"
                    )));
                }
                let f = val_str.trim().parse::<f64>().map_err(|_| {
                    AegisError::Validation(format!("Invalid float value for '{key}': {value}"))
                })?;
                if !f.is_finite() {
                    return Err(AegisError::Validation(format!(
                        "Invalid float value for '{key}': {value}"
                    )));
                }
                if key == "max_daily_drawdown_alert_pct" && (f <= 0.0 || f >= 1.0) {
                    return Err(AegisError::Validation(
                        "Invalid max_daily_drawdown_alert_pct: must be a fraction in (0.0, 1.0), e.g. 0.03 for 3%."
                            .to_string(),
                    ));
                }
                if key == "initial_equity_usdc" && f <= 0.0 {
                    return Err(AegisError::Validation(
                        "Invalid initial_equity_usdc: must be > 0.0.".to_string(),
                    ));
                }
                if key == "settling_split_pct" && (f <= 0.0 || f >= 1.0) {
                    return Err(AegisError::Validation(
                        "Invalid settling_split_pct: must be in (0.0, 1.0).".to_string(),
                    ));
                }
                if check_pair_bounds {
                    for &(min_key, max_key) in MIN_MAX_FLOAT_PAIRS {
                        if key == min_key {
                            if f <= 0.0 {
                                return Err(AegisError::Validation(format!(
                                    "Invalid {min_key} ({f}): must be > 0.0."
                                )));
                            }
                            let cur_cfg = self.load_config_opts(false)?;
                            let max_val = cur_cfg.get_f64(max_key, f64::INFINITY);
                            if f > max_val {
                                return Err(AegisError::Validation(format!(
                                    "Invalid {min_key} ({f}): cannot exceed {max_key} ({max_val})."
                                )));
                            }
                        } else if key == max_key {
                            if f <= 0.0 {
                                return Err(AegisError::Validation(format!(
                                    "Invalid {max_key} ({f}): must be > 0.0."
                                )));
                            }
                            let cur_cfg = self.load_config_opts(false)?;
                            let min_val = cur_cfg.get_f64(min_key, 0.0);
                            if f < min_val {
                                return Err(AegisError::Validation(format!(
                                    "Invalid {max_key} ({f}): cannot be less than {min_key} ({min_val})."
                                )));
                            }
                        }
                    }
                }
                f.to_string()
            }
            _ => {
                let s = val_str.trim().to_string();
                if s.is_empty() && !defaults.contains_key(key) {
                    return Err(AegisError::Validation(format!(
                        "Invalid empty string value for '{key}'"
                    )));
                }
                if key == "pond_direction_policy" {
                    let pol = PondDirectionPolicy::parse(&s)?;
                    pol.as_str().to_string()
                } else if key == "environment" {
                    s.to_ascii_lowercase()
                } else if key == "hyperliquid_agent_address" {
                    validate_evm_address(&s, "hyperliquid_agent_address")?
                } else if matches!(
                    key,
                    "hmm_timeframe" | "default_timeframe" | "exec_timeframe" | "rs_timeframe"
                ) {
                    aegis_core::parse_timeframe_seconds(&s)?;
                    s.to_ascii_lowercase()
                } else if key == "calibration_timeframes_csv" {
                    aegis_core::validate_timeframe_csv(&s)?
                } else {
                    s
                }
            }
        };

        let is_sensitive = SENSITIVE_CONFIG_KEYS.contains(&key);
        let stored_val = if is_sensitive && !str_val.is_empty() {
            self.encrypt_config_value(&str_val)?
        } else {
            str_val.clone()
        };

        self.with_conn(|conn| {
            let updated = conn
                .execute(
                    "UPDATE system_config
                     SET config_value = ?1,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                     WHERE config_key = ?2;",
                    params![stored_val, key],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;

            if (key == "settling_deadband_bps" || key == "settling_tolerance_bps") && updated > 0 {
                let sibling_key = if key == "settling_deadband_bps" {
                    "settling_tolerance_bps"
                } else {
                    "settling_deadband_bps"
                };
                let _ = conn.execute(
                    "UPDATE system_config
                     SET config_value = ?1,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                     WHERE config_key = ?2;",
                    params![stored_val, sibling_key],
                );
            }

            if key == "initial_equity_usdc" && updated > 0 {
                if let Ok(new_base_eq) = str_val.parse::<f64>() {
                    let trade_cnt: i64 = conn
                        .query_row("SELECT COUNT(*) FROM trades;", [], |r| r.get(0))
                        .unwrap_or(0);
                    if trade_cnt == 0 {
                        let _ = conn.execute(
                            "UPDATE portfolio_snapshots
                             SET total_equity_usdc = ?1,
                                 available_cash_usdc = ROUND(MAX(0.0, ?1 - COALESCE(basis_harvest_notional_usdc, 0.0)), 2),
                                 accrued_yield_usdc = 0.0
                             WHERE deployed_notional_usdc = 0.0;",
                            params![new_base_eq],
                        );
                    }
                }
            }
            if key == "platform_settle_gate_enabled" && updated > 0 {
                let arm_ms = if str_val == "true" {
                    self.now_ms().to_string()
                } else {
                    "0".to_string()
                };
                let _ = conn.execute(
                    "INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
                     VALUES ('platform_settle_gate_armed_at_ms', ?1, 'int', 'settling', 'Epoch timestamp (ms) when platform_settle_gate was last armed', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                     ON CONFLICT(config_key) DO UPDATE SET
                        config_value = excluded.config_value,
                        updated_at = excluded.updated_at;",
                    params![arm_ms],
                );
            }
            Ok(updated > 0)
        })
    }

    pub fn scale_risk_caps_for_equity(
        &self,
        initial_equity_usdc: f64,
    ) -> Result<HashMap<String, f64>> {
        if !initial_equity_usdc.is_finite() || initial_equity_usdc <= 0.0 {
            return Err(AegisError::Validation(format!(
                "Invalid initial_equity_usdc '{initial_equity_usdc}': must be > 0.0."
            )));
        }
        let cfg = self.load_config_opts(false)?;
        let heat_pct = cfg.get_f64("max_portfolio_heat_pct", 0.04);
        let single_pct = cfg.get_f64("max_single_asset_pct", 0.25);
        let gross_lev = cfg.get_f64("max_gross_leverage", 1.5);
        let raw_scale_min = cfg.get_f64("conviction_scale_min", 0.5);
        let raw_scale_max = cfg.get_f64("conviction_scale_max", 1.5);
        let (scale_min, scale_max) = if raw_scale_min <= raw_scale_max {
            (raw_scale_min, raw_scale_max)
        } else {
            (raw_scale_max, raw_scale_min)
        };

        let split_pct = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
        let pit_book_eq = round_dp(initial_equity_usdc * split_pct, 2);
        let baseline_risk = round_dp(pit_book_eq * 0.01, 2);

        let pond_book_eq = round_dp(initial_equity_usdc * (1.0 - split_pct), 2);
        let pond_heat_pct = cfg.get_f64("pond_max_heat_pct", 0.04);
        let pond_cell_pct = cfg.get_f64("pond_max_cell_pct", 0.25);
        let raw_pond_scale_min = cfg.get_f64("pond_conviction_scale_min", 0.5);
        let raw_pond_scale_max = cfg.get_f64("pond_conviction_scale_max", 1.5);
        let (pond_scale_min, pond_scale_max) = if raw_pond_scale_min <= raw_pond_scale_max {
            (raw_pond_scale_min, raw_pond_scale_max)
        } else {
            (raw_pond_scale_max, raw_pond_scale_min)
        };
        let pond_baseline_risk = round_dp(pond_book_eq * 0.01, 2);

        let pairs = [
            ("initial_equity_usdc", round_dp(initial_equity_usdc, 2)),
            ("baseline_risk_per_trade_usdc", baseline_risk),
            ("min_risk_per_trade_usdc", round_dp(baseline_risk * scale_min, 2)),
            ("max_risk_per_trade_usdc", round_dp(baseline_risk * scale_max, 2)),
            ("max_portfolio_heat_usdc", round_dp(pit_book_eq * heat_pct, 2)),
            (
                "max_single_asset_notional_usdc",
                round_dp(pit_book_eq * single_pct, 2),
            ),
            (
                "max_gross_leverage_notional_usdc",
                round_dp(pit_book_eq * gross_lev, 2),
            ),
            ("pond_baseline_risk_usdc", pond_baseline_risk),
            (
                "pond_min_risk_usdc",
                round_dp(pond_baseline_risk * pond_scale_min, 2),
            ),
            (
                "pond_max_risk_usdc",
                round_dp(pond_baseline_risk * pond_scale_max, 2),
            ),
            ("pond_max_heat_usdc", round_dp(pond_book_eq * pond_heat_pct, 2)),
            (
                "pond_max_cell_notional_usdc",
                round_dp(pond_book_eq * pond_cell_pct, 2),
            ),
        ];

        let mut out = HashMap::new();
        for (k, v) in pairs {
            self.set_config_value_with_opts(k, &json!(v), false)?;
            out.insert(k.to_string(), v);
        }
        let _ = self.set_config_value_with_opts("risk_caps_reference_scope", &json!("BOOK"), false);
        Ok(out)
    }

    pub fn set_config_batch(
        &self,
        updates: &HashMap<String, Value>,
        scale_risk_caps: bool,
    ) -> Result<Vec<String>> {
        // R3-H13: Validate min <= max pairs across the combined batch + existing DB config before mutating.
        let cur_cfg = self.load_config_opts(false)?;
        let parse_opt_f64 = |val: &Value, key: &str| -> Result<f64> {
            if val.is_boolean() {
                return Err(AegisError::Validation(format!(
                    "Invalid float value for '{key}': {val}"
                )));
            }
            let f = match val {
                Value::Number(n) => n.as_f64().ok_or_else(|| {
                    AegisError::Validation(format!("Invalid float value for '{key}': {val}"))
                })?,
                Value::String(s) => s.trim().parse::<f64>().map_err(|_| {
                    AegisError::Validation(format!("Invalid float value for '{key}': {val}"))
                })?,
                _ => {
                    return Err(AegisError::Validation(format!(
                        "Invalid float value for '{key}': {val}"
                    )))
                }
            };
            if !f.is_finite() {
                return Err(AegisError::Validation(format!(
                    "Invalid float value for '{key}': {val}"
                )));
            }
            Ok(f)
        };
        for &(min_key, max_key) in MIN_MAX_FLOAT_PAIRS {
            if updates.contains_key(min_key) || updates.contains_key(max_key) {
                let eff_min = match updates.get(min_key) {
                    Some(v) => parse_opt_f64(v, min_key)?,
                    None => cur_cfg.get_f64(min_key, 0.0),
                };
                let eff_max = match updates.get(max_key) {
                    Some(v) => parse_opt_f64(v, max_key)?,
                    None => cur_cfg.get_f64(max_key, f64::INFINITY),
                };
                if eff_min <= 0.0 {
                    return Err(AegisError::Validation(format!(
                        "Invalid {min_key} ({eff_min}): must be > 0.0."
                    )));
                }
                if eff_max <= 0.0 {
                    return Err(AegisError::Validation(format!(
                        "Invalid {max_key} ({eff_max}): must be > 0.0."
                    )));
                }
                if eff_min > eff_max {
                    return Err(AegisError::Validation(format!(
                        "Invalid {min_key} ({eff_min}) / {max_key} ({eff_max}): min cannot exceed max."
                    )));
                }
            }
        }

        let mut ordered_keys: Vec<&String> = updates.keys().collect();
        ordered_keys.sort_by(|a, b| {
            let prio = |k: &str| -> u8 {
                match k {
                    "wallet_address"
                    | "private_key"
                    | "hyperliquid_agent_address"
                    | "hyperliquid_vault_address"
                    | "gemini_api_key" => 0,
                    "hyperliquid_testnet" => 2,
                    "pond_execution_mode" => 3,
                    "execution_mode" => 4,
                    "environment" => 5,
                    _ => 1,
                }
            };
            prio(a.as_str()).cmp(&prio(b.as_str())).then_with(|| a.cmp(b))
        });

        let mut updated_keys = Vec::new();
        for k in ordered_keys {
            let v = &updates[k];
            let ok = self.set_config_value_with_opts(k, v, false)?;
            if !ok {
                return Err(AegisError::NotFound(format!("Unknown config_key: {k}")));
            }
            updated_keys.push(k.clone());
        }
        if scale_risk_caps {
            if let Some(eq_val) = updates.get("initial_equity_usdc") {
                let eq = eq_val
                    .as_f64()
                    .or_else(|| eq_val.as_str().and_then(|s| s.parse::<f64>().ok()))
                    .unwrap_or(10000.0);
                let scaled = self.scale_risk_caps_for_equity(eq)?;
                for sk in scaled.keys() {
                    if !updated_keys.contains(sk) {
                        updated_keys.push(sk.clone());
                    }
                }
            }
        }
        Ok(updated_keys)
    }

    /// Verify that the configured `private_key` derives to `wallet_address` or `hyperliquid_agent_address` (`NH-12`).
    pub fn validate_signer_binding(&self) -> Result<String> {
        let cfg = self.load_config()?;
        let wallet = cfg.get_str("wallet_address", "");
        let mut agent = cfg.get_str("hyperliquid_agent_address", "");
        let priv_key = self.get_signing_private_key()?;
        if agent.trim().is_empty() {
            if let Ok(derived) = derive_evm_address_from_private_key(&priv_key) {
                if !wallet.trim().is_empty() && !derived.eq_ignore_ascii_case(&wallet) {
                    agent = derived.clone();
                    let _ = self.with_conn(|conn| {
                        let _ = conn.execute(
                            "INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
                             VALUES ('hyperliquid_agent_address', ?1, 'string', 'credentials', 'Delegated Hyperliquid API Agent address derived from private_key', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                             ON CONFLICT(config_key) DO UPDATE SET
                                config_value = excluded.config_value,
                                updated_at = excluded.updated_at;",
                            params![derived],
                        );
                        Ok(())
                    });
                }
            }
        }
        verify_signer_matches_wallet(
            &priv_key,
            &wallet,
            if agent.trim().is_empty() {
                None
            } else {
                Some(agent.trim())
            },
        )
    }

    /// Guard transitions to `LIVE` or `TESTNET` execution mode by requiring valid `wallet_address`
    /// and `private_key` credentials and matching signer address (`NM-2`, `NH-12`).
    pub fn validate_execution_mode_transition(
        &self,
        target_mode: &str,
        pending_wallet: Option<&str>,
        pending_private_key: Option<&str>,
    ) -> Result<()> {
        let parsed = ExecutionMode::parse(target_mode)?;
        if parsed == ExecutionMode::Paper {
            return Ok(());
        }
        let cfg = self.load_config()?;
        let eff_wallet = pending_wallet
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| cfg.get_str("wallet_address", ""));
        let eff_priv = match pending_private_key.map(str::trim).filter(|s| !s.is_empty()) {
            Some(pk) => Zeroizing::new(pk.to_string()),
            None => self.get_signing_private_key().unwrap_or_default(),
        };
        let mut eff_agent = cfg.get_str("hyperliquid_agent_address", "");
        let valid_w = !eff_wallet.is_empty()
            && validate_evm_address(&eff_wallet, "wallet_address").is_ok();
        let valid_p = !eff_priv.is_empty() && validate_private_key(&eff_priv).is_ok();
        if !(valid_w && valid_p) {
            return Err(AegisError::Validation(
                "Cannot enable LIVE execution_mode without both a valid wallet_address and private_key configured."
                    .to_string(),
            ));
        }
        if eff_agent.trim().is_empty() {
            if let Ok(derived) = derive_evm_address_from_private_key(&eff_priv) {
                if !derived.eq_ignore_ascii_case(&eff_wallet) {
                    eff_agent = derived;
                }
            }
        }
        verify_signer_matches_wallet(
            &eff_priv,
            &eff_wallet,
            if eff_agent.trim().is_empty() {
                None
            } else {
                Some(eff_agent.trim())
            },
        )?;
        Ok(())
    }

    pub fn get_credentials_status(&self) -> Result<Value> {
        self.reconcile_routing_config();
        let cfg = self.load_config()?;
        let wallet = cfg.get_str("wallet_address", "");
        let mut agent_addr = cfg.get_str("hyperliquid_agent_address", "");
        let priv_key = self.get_signing_private_key().unwrap_or_default();
        let raw_vault = cfg.get_str("hyperliquid_vault_address", "");
        let gemini_key =
            Zeroizing::new(self.try_decrypt_or_empty(&cfg.get_str("gemini_api_key", "")));
        let tg_token =
            Zeroizing::new(self.try_decrypt_or_empty(&cfg.get_str("telegram_bot_token", "")));
        let tg_chat_id = cfg.get_str("telegram_chat_id", "");
        let dd_alert_pct = cfg.get_f64("max_daily_drawdown_alert_pct", 0.03);

        let raw_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let testnet_flag = cfg.get_bool("hyperliquid_testnet", false);
        let effective_mode = if raw_mode == "TESTNET" || (raw_mode == "LIVE" && testnet_flag) {
            "TESTNET".to_string()
        } else if raw_mode == "LIVE" {
            "LIVE".to_string()
        } else {
            "PAPER".to_string()
        };
        let is_testnet = match effective_mode.as_str() {
            "TESTNET" => true,
            "LIVE" => false,
            _ => testnet_flag,
        };
        let vault = if raw_vault.trim().is_empty() {
            if is_testnet {
                aegis_core::DEFAULT_TESTNET_HLP_VAULT_ADDRESS.to_string()
            } else {
                aegis_core::DEFAULT_MAINNET_HLP_VAULT_ADDRESS.to_string()
            }
        } else {
            raw_vault
        };

        let wallet_valid =
            !wallet.is_empty() && validate_evm_address(&wallet, "wallet_address").is_ok();
        let priv_valid = !priv_key.is_empty() && validate_private_key(&priv_key).is_ok();
        let signer_address = if priv_valid {
            derive_evm_address_from_private_key(&priv_key).unwrap_or_default()
        } else {
            String::new()
        };
        if !signer_address.is_empty()
            && !wallet.is_empty()
            && !signer_address.eq_ignore_ascii_case(&wallet)
            && agent_addr.trim().is_empty()
        {
            agent_addr = signer_address.clone();
        }
        let signer_matches_wallet = !signer_address.is_empty()
            && (signer_address.eq_ignore_ascii_case(&wallet)
                || (!agent_addr.trim().is_empty()
                    && signer_address.eq_ignore_ascii_case(agent_addr.trim())));
        let credentials_ready = wallet_valid && priv_valid;

        let network = if is_testnet { "testnet" } else { "mainnet" };
        let api_endpoint = if is_testnet {
            let fallback = cfg.get_str(
                "hyperliquid_testnet_url",
                "https://api.hyperliquid-testnet.xyz",
            );
            let custom = cfg.get_str("hyperliquid_testnet_api_url", "");
            if custom.trim().is_empty() {
                fallback
            } else {
                custom.trim().to_string()
            }
        } else {
            let fallback = cfg.get_str("hyperliquid_mainnet_url", "https://api.hyperliquid.xyz");
            let custom = cfg.get_str("hyperliquid_mainnet_api_url", "");
            if custom.trim().is_empty() {
                fallback
            } else {
                custom.trim().to_string()
            }
        };
        let active_routing =
            if (effective_mode == "LIVE" || effective_mode == "TESTNET") && credentials_ready {
                if is_testnet {
                    "LIVE_TESTNET"
                } else {
                    "LIVE_MAINNET"
                }
            } else {
                "PAPER_SIMULATION"
            };

        let pond_mode = cfg.get_str("pond_execution_mode", &effective_mode);
        Ok(json!({
            "exchange_id": cfg.get_str("exchange_id", "hyperliquid"),
            "environment": cfg.get_str("environment", "sandbox"),
            "execution_mode": effective_mode,
            "pond_execution_mode": pond_mode,
            "hyperliquid_testnet": is_testnet,
            "network": network,
            "api_endpoint": api_endpoint,
            "wallet_address": wallet,
            "wallet_configured": wallet_valid,
            "signer_address": signer_address,
            "signer_matches_wallet": signer_matches_wallet,
            "hyperliquid_agent_address": agent_addr,
            "private_key_masked": mask_secret(&priv_key),
            "private_key_configured": priv_valid,
            "hyperliquid_vault_address": vault,
            "gemini_api_key_masked": mask_secret(&gemini_key),
            "gemini_api_key_configured": !gemini_key.is_empty(),
            "credentials_ready": credentials_ready,
            "active_routing": active_routing,
            "alert_channel": "TELEGRAM",
            "telegram_bot_token_masked": mask_secret(&tg_token),
            "telegram_bot_token_configured": !tg_token.is_empty(),
            "telegram_chat_id": mask_identifier(&tg_chat_id),
            "telegram_chat_id_masked": mask_identifier(&tg_chat_id),
            "telegram_chat_id_configured": !tg_chat_id.is_empty(),
            "telegram_alerts_ready": !tg_token.is_empty() && !tg_chat_id.is_empty(),
            "max_daily_drawdown_alert_pct": dd_alert_pct,
        }))
    }

    pub fn configure_telegram_alerts(
        &self,
        telegram_bot_token: Option<&str>,
        telegram_chat_id: Option<&str>,
        max_daily_drawdown_alert_pct: Option<f64>,
    ) -> Result<Value> {
        if let Some(tok) = telegram_bot_token {
            let trimmed_tok = tok.trim();
            if !is_masked_secret_echo(trimmed_tok) {
                let plain = Zeroizing::new(if is_encrypted_value(trimmed_tok) {
                    self.decrypt_config_value(trimmed_tok)?
                } else {
                    trimmed_tok.to_string()
                });
                self.set_config_value("telegram_bot_token", &json!(&*plain))?;
            }
        }
        if let Some(cid) = telegram_chat_id {
            let trimmed = cid.trim();
            if !trimmed.is_empty() && !is_masked_secret_echo(trimmed) {
                self.set_config_value("telegram_chat_id", &json!(trimmed))?;
            }
        }
        if let Some(pct) = max_daily_drawdown_alert_pct {
            if !pct.is_finite() || pct <= 0.0 || pct >= 1.0 {
                return Err(AegisError::Validation(
                    "Invalid max_daily_drawdown_alert_pct: must be a fraction in (0.0, 1.0), e.g. 0.03 for 3%."
                        .to_string(),
                ));
            }
            self.set_config_value("max_daily_drawdown_alert_pct", &json!(pct))?;
        }
        self.get_credentials_status()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn configure_hyperliquid_credentials(
        &self,
        wallet_address: Option<&str>,
        private_key: Option<&str>,
        hyperliquid_testnet: Option<bool>,
        execution_mode: Option<&str>,
        hyperliquid_vault_address: Option<&str>,
        gemini_api_key: Option<&str>,
        require_credentials_for_live: bool,
    ) -> Result<Value> {
        let mut updates: HashMap<&str, String> = HashMap::new();
        if let Some(w) = wallet_address {
            updates.insert("wallet_address", validate_evm_address(w, "wallet_address")?);
        }
        if let Some(pk) = private_key {
            let trimmed_pk = pk.trim();
            if !is_masked_secret_echo(trimmed_pk) {
                let raw_pk = Zeroizing::new(if is_encrypted_value(trimmed_pk) {
                    self.decrypt_config_value(trimmed_pk)?
                } else {
                    trimmed_pk.to_string()
                });
                updates.insert("private_key", validate_private_key(&raw_pk)?);
            }
        }
        if let Some(v) = hyperliquid_vault_address {
            updates.insert(
                "hyperliquid_vault_address",
                validate_evm_address(v, "hyperliquid_vault_address")?,
            );
        }
        if let Some(gk) = gemini_api_key {
            let trimmed_gk = gk.trim();
            if !is_masked_secret_echo(trimmed_gk) {
                let raw_gk = if is_encrypted_value(trimmed_gk) {
                    self.decrypt_config_value(trimmed_gk)?
                } else {
                    trimmed_gk.to_string()
                };
                updates.insert("gemini_api_key", raw_gk);
            }
        }
        if let Some(tn) = hyperliquid_testnet {
            updates.insert(
                "hyperliquid_testnet",
                if tn { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(mode_str) = execution_mode {
            let mode = ExecutionMode::parse(mode_str)?;
            match mode {
                ExecutionMode::Testnet => {
                    if updates.get("hyperliquid_testnet").map(String::as_str) == Some("false") {
                        return Err(AegisError::Validation(
                            "Conflicting configuration: execution_mode='TESTNET' cannot be combined with hyperliquid_testnet=False."
                                .to_string(),
                        ));
                    }
                    updates.insert("execution_mode", "TESTNET".to_string());
                    updates.insert("hyperliquid_testnet", "true".to_string());
                    updates.insert("pond_execution_mode", "TESTNET".to_string());
                    updates.insert("environment", "sandbox".to_string());
                }
                ExecutionMode::Live => {
                    if updates.get("hyperliquid_testnet").map(String::as_str) == Some("true") {
                        updates.insert("execution_mode", "TESTNET".to_string());
                        updates.insert("hyperliquid_testnet", "true".to_string());
                        updates.insert("pond_execution_mode", "TESTNET".to_string());
                        updates.insert("environment", "sandbox".to_string());
                    } else {
                        updates.insert("execution_mode", "LIVE".to_string());
                        updates.insert("hyperliquid_testnet", "false".to_string());
                        updates.insert("pond_execution_mode", "LIVE".to_string());
                        updates.insert("environment", "production".to_string());
                    }
                }
                ExecutionMode::Paper => {
                    updates.insert("execution_mode", "PAPER".to_string());
                    updates.insert("pond_execution_mode", "PAPER".to_string());
                    updates.insert("environment", "sandbox".to_string());
                }
            }
        } else if let Some(tn) = hyperliquid_testnet {
            let cur_cfg = self.load_config_opts(false)?;
            let cur_mode = cur_cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
            if tn && cur_mode == "LIVE" {
                updates.insert("execution_mode", "TESTNET".to_string());
                updates.insert("pond_execution_mode", "TESTNET".to_string());
                updates.insert("environment", "sandbox".to_string());
            } else if !tn && cur_mode == "TESTNET" {
                updates.insert("execution_mode", "LIVE".to_string());
                updates.insert("pond_execution_mode", "LIVE".to_string());
                updates.insert("environment", "production".to_string());
            }
        }

        let current_cfg = self.load_config()?;
        let eff_wallet = updates
            .get("wallet_address")
            .cloned()
            .unwrap_or_else(|| current_cfg.get_str("wallet_address", ""));
        let eff_priv = match updates.get("private_key") {
            Some(pk) => Zeroizing::new(pk.clone()),
            None => self.get_signing_private_key().unwrap_or_default(),
        };
        let eff_mode = updates
            .get("execution_mode")
            .cloned()
            .unwrap_or_else(|| current_cfg.get_str("execution_mode", "PAPER"));
        let mut eff_agent = current_cfg.get_str("hyperliquid_agent_address", "");

        if !eff_wallet.is_empty() && !eff_priv.is_empty() {
            let derived = derive_evm_address_from_private_key(&eff_priv)?;
            if derived.eq_ignore_ascii_case(&eff_wallet) {
                eff_agent.clear();
                updates.insert("hyperliquid_agent_address", String::new());
            } else {
                eff_agent = derived.clone();
                updates.insert("hyperliquid_agent_address", derived);
            }
            verify_signer_matches_wallet(
                &eff_priv,
                &eff_wallet,
                if eff_agent.trim().is_empty() {
                    None
                } else {
                    Some(eff_agent.trim())
                },
            )?;
        }

        if require_credentials_for_live && (eff_mode == "LIVE" || eff_mode == "TESTNET") {
            let valid_w = !eff_wallet.is_empty()
                && validate_evm_address(&eff_wallet, "wallet_address").is_ok();
            let valid_p = !eff_priv.is_empty() && validate_private_key(&eff_priv).is_ok();
            if !(valid_w && valid_p) {
                return Err(AegisError::Validation(
                    "Cannot enable LIVE execution_mode without both a valid wallet_address and private_key configured."
                        .to_string(),
                ));
            }
            verify_signer_matches_wallet(
                &eff_priv,
                &eff_wallet,
                if eff_agent.trim().is_empty() {
                    None
                } else {
                    Some(eff_agent.trim())
                },
            )?;
        }

        for (k, v) in updates {
            let stored = if SENSITIVE_CONFIG_KEYS.contains(&k) && !v.is_empty() {
                self.encrypt_config_value(&v)?
            } else {
                v
            };
            let (vtype, cat, desc) = match k {
                "hyperliquid_testnet" => ("bool", "credentials", "Route orders to Hyperliquid testnet when true"),
                "execution_mode" => ("string", "platform", "Execution routing mode: PAPER, TESTNET, or LIVE Web3 DEX routing"),
                "pond_execution_mode" => ("string", "platform", "Pond execution routing mode: PAPER, TESTNET, or LIVE Web3 DEX routing"),
                "environment" => ("string", "platform", "Deployment target environment (sandbox or production)"),
                "wallet_address" => ("string", "credentials", "Main Arbitrum Wallet Address holding USDC collateral"),
                "private_key" => ("string", "credentials", "Trade-Only API Agent Wallet Private Key (no withdrawal permissions)"),
                "hyperliquid_agent_address" => ("string", "credentials", "Delegated Hyperliquid API Agent address derived from private_key"),
                "hyperliquid_vault_address" => ("string", "credentials", "Optional Hyperliquid sub-account or HLP vault address"),
                "gemini_api_key" => ("string", "credentials", "Google Gemini API key for quantitative telemetry diagnostics"),
                _ => ("string", "credentials", ""),
            };
            self.with_conn(|conn| {
                conn.execute(
                    "INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                     ON CONFLICT(config_key) DO UPDATE SET
                        config_value = excluded.config_value,
                        updated_at = excluded.updated_at;",
                    params![k, stored, vtype, cat, desc],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(())
            })?;
        }
        self.get_credentials_status()
    }

    /// Read-only inspection of the configured `web_api_token` (or `dashboard_api_token`) without
    /// generating or writing a new token when none is configured (`R4-N8`).
    pub fn get_existing_web_api_token(&self) -> Result<Option<String>> {
        if let Ok(tok) = std::env::var("AEGIS_WEB_TOKEN") {
            let trimmed = tok.trim().to_string();
            if !trimmed.is_empty() {
                return Ok(Some(trimmed));
            }
        }
        let cfg = self.load_config()?;
        for key in ["web_api_token", "dashboard_api_token"] {
            let tok = cfg.get_str(key, "");
            if !tok.trim().is_empty() {
                return Ok(Some(tok.trim().to_string()));
            }
        }
        Ok(None)
    }

    /// Retrieves and decrypts the active `web_api_token` (or `dashboard_api_token`) from `system_config`,
    /// or generates and persists a new 32-byte CSPRNG hex token if none is configured.
    pub fn get_or_create_web_api_token(&self) -> Result<String> {
        if let Some(existing) = self.get_existing_web_api_token()? {
            return Ok(existing);
        }
        use rand::RngCore;
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        let generated: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        self.set_config_value("web_api_token", &json!(generated))?;
        Ok(generated)
    }
}
