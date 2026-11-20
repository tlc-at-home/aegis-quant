//! Typed JSON DTOs and MariaDB 13.0 + 7 SQL Views dashboard payload builders (`aegis-web::payloads`).

use aegis_core::{
    is_immutable_security_config_key, is_sensitive_config_key, mask_identifier, now_iso8601_utc,
    now_ms, round_dp, AegisError, CooldownAssetView, ExchangeAsset, HeldAssetView,
    OrderAuditRecord, OrderedAssetView, PnlSummary, PortfolioSnapshot, PriorityWatchView, Result,
    RiskTier, SleeveS1AssetView, SleeveS2AssetView, SleeveS3AssetView, SystemConfigRow,
    TelemetryLogRecord, PLATFORM_VERSION,
};
use aegis_db::DatabaseManager;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

type ViewsHealthEntry = (Instant, Vec<String>, Vec<String>);
type ViewsHealthCache = Mutex<HashMap<String, ViewsHealthEntry>>;

static VIEWS_HEALTH_CACHE: OnceLock<ViewsHealthCache> = OnceLock::new();

const VIEWS_CACHE_TTL: Duration = Duration::from_secs(30);

fn get_cached_tables_and_views(db: &DatabaseManager) -> Result<(Vec<String>, Vec<String>)> {
    let db_key = db.db_name().to_string();
    let cache = VIEWS_HEALTH_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    {
        let guard = cache.lock();
        if let Some((captured_at, tables, views)) = guard.get(&db_key) {
            if captured_at.elapsed() < VIEWS_CACHE_TTL {
                return Ok((tables.clone(), views.clone()));
            }
        }
    }
    let (tables, views) = db.list_tables_and_views()?;
    cache
        .lock()
        .insert(db_key, (Instant::now(), tables.clone(), views.clone()));
    Ok((tables, views))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiErrorBody {
    pub error: String,
    pub detail: String,
}

/// Response payload for `GET /api/v1/health`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthPayload {
    pub status: String,
    pub platform_version: String,
    pub mariadb_version: String,
    pub database_name: String,
    pub tables_count: usize,
    pub views_healthy_count: usize,
    pub views: Vec<String>,
    pub risk_tier: String,
    pub target_leverage_multiplier: f64,
    pub risk_tier_badge: String,
    pub execution_mode: String,
    pub loop_cadence: String,
    pub priority_monitor_interval_ms: i64,
    pub scanner_interval_ms: i64,
    pub timestamp_utc: String,
    pub timestamp_ms: i64,
}

/// Unified response payload for `GET /api/v1/dashboard`, `GET /api/v1/state`, and `/api/v1/stream`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardPayload {
    pub platform_version: String,
    pub mariadb_version: String,
    pub views_healthy_count: usize,
    pub execution_mode: String,
    pub risk_tier: String,
    pub target_leverage_multiplier: f64,
    pub risk_tier_badge: String,
    pub loop_cadence: String,
    pub risk_capital_cap_updated_at_ms: i64,
    pub max_open_positions: usize,
    pub active_cap_open_count: usize,
    pub pre_cap_open_count: usize,
    pub pnl_summary: PnlSummary,
    pub snapshot: PortfolioSnapshot,
    pub snapshots_history: Vec<PortfolioSnapshot>,
    pub v_assets_held: Vec<HeldAssetView>,
    pub v_assets_ordered: Vec<OrderedAssetView>,
    pub v_assets_in_cooldown: Vec<CooldownAssetView>,
    pub v_assets_sleeve_s1: Vec<SleeveS1AssetView>,
    pub v_assets_sleeve_s2: Vec<SleeveS2AssetView>,
    pub v_assets_sleeve_s3: Vec<SleeveS3AssetView>,
    pub v_priority_watchlist: Vec<PriorityWatchView>,
    pub exchange_assets: Vec<ExchangeAsset>,
    pub order_audit: Vec<OrderAuditRecord>,
    pub telemetry_logs: Vec<TelemetryLogRecord>,
    pub system_config: Vec<SystemConfigRow>,
    pub timestamp_ms: i64,
}

/// Request payload for `POST /api/v1/risk-tier`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskTierUpdateRequest {
    pub risk_tier: String,
    #[serde(default)]
    pub custom_leverage_multiplier: Option<f64>,
}

/// Response payload for `POST /api/v1/risk-tier`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskTierUpdateResponse {
    pub status: String,
    pub risk_tier: String,
    pub target_leverage_multiplier: f64,
    pub display_badge: String,
    pub updated_at_ms: i64,
}

/// Request payload for `POST /api/v1/config` (supports single `config_key`/`config_value` or batch `updates`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfigUpdateRequest {
    #[serde(default)]
    pub config_key: Option<String>,
    #[serde(default)]
    pub config_value: Option<Value>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub updates: Option<HashMap<String, Value>>,
}

/// Response payload for `POST /api/v1/config`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigUpdateResponse {
    pub status: String,
    pub updated_keys: Vec<String>,
    pub updated_at_ms: i64,
}

/// Request payload for `POST /api/v1/positions/close`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClosePositionRequest {
    #[serde(default)]
    pub position_id: Option<i64>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub cooldown_sec: Option<i64>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub close_all: bool,
}

/// Response payload for `POST /api/v1/positions/close`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosePositionResponse {
    pub status: String,
    pub position_id: i64,
    pub asset_id: i64,
    pub symbol: String,
    pub previous_status: String,
    pub new_status: String,
    pub realized_pnl_usdc: f64,
    pub cooldown_applied_sec: i64,
    pub closed_count: usize,
    pub updated_at_ms: i64,
}

/// Request payload for `POST /api/v1/reconcile`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReconcileRequest {
    #[serde(default)]
    pub close_legacy_positions: bool,
}

/// Converts a JSON `Value` into a clean configuration string.
pub fn json_value_to_config_string(val: &Value) -> String {
    match val {
        Value::String(s) => s.trim().to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn parse_f64_bounded(key: &str, raw: &str, min: f64, max: f64) -> Result<f64> {
    let val = raw.trim().parse::<f64>().map_err(|_| {
        AegisError::Validation(format!("'{key}' must be a valid number, got '{raw}'"))
    })?;
    if !val.is_finite() || val < min || val > max {
        return Err(AegisError::Validation(format!(
            "'{key}' must be between {min} and {max}, got {val}"
        )));
    }
    Ok(val)
}

fn parse_i64_bounded(key: &str, raw: &str, min: i64, max: i64) -> Result<i64> {
    let val = raw.trim().parse::<i64>().map_err(|_| {
        AegisError::Validation(format!("'{key}' must be a valid integer, got '{raw}'"))
    })?;
    if val < min || val > max {
        return Err(AegisError::Validation(format!(
            "'{key}' must be between {min} and {max}, got {val}"
        )));
    }
    Ok(val)
}

fn parse_bool_strict(key: &str, raw: &str) -> Result<()> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "false" => Ok(()),
        _ => Err(AegisError::Validation(format!(
            "'{key}' must be 'true' or 'false', got '{raw}'"
        ))),
    }
}

/// Validates a configuration update before persisting to MariaDB `system_config`.
/// Enforces key syntax, immutable security blocks, numeric/enum bounds, and rejects raw `"gAAAAAB"` strings.
pub fn validate_config_key_value(key: &str, value_str: &str) -> Result<()> {
    let clean_key = key.trim();
    let clean_val = value_str.trim();

    if clean_key.is_empty() || clean_key.len() > 128 {
        return Err(AegisError::Validation(
            "config_key must be between 1 and 128 characters".to_string(),
        ));
    }
    if !clean_key
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic())
        || !clean_key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(AegisError::Validation(format!(
            "Invalid config_key '{clean_key}': must start with an ASCII letter and contain only [A-Za-z0-9_]"
        )));
    }

    let lower_key = clean_key.to_ascii_lowercase();
    if is_immutable_security_config_key(clean_key)
        || matches!(
            lower_key.as_str(),
            "web_require_auth"
                | "hyperliquid_testnet"
                | "platform_version"
                | "risk_capital_cap_updated_at_ms"
                | "hyperliquid_last_nonce"
                | "last_db_backup_ms"
                | "last_entry_order_ms"
        )
    {
        return Err(AegisError::SecurityViolation(format!(
            "Configuration key '{clean_key}' is an immutable security or internal state key and cannot be modified via Web API"
        )));
    }

    if clean_val.starts_with("gAAAAAB") || clean_val.starts_with("ENC:") {
        return Err(AegisError::Validation(format!(
            "Configuration value for '{clean_key}' cannot be a raw encrypted ciphertext envelope"
        )));
    }

    match lower_key.as_str() {
        "execution_mode" => {
            if !matches!(
                clean_val.to_ascii_uppercase().as_str(),
                "PAPER" | "TESTNET" | "LIVE"
            ) {
                return Err(AegisError::Validation(format!(
                    "Invalid execution_mode '{clean_val}': expected PAPER, TESTNET, or LIVE"
                )));
            }
        }
        "risk_tier" => {
            if !matches!(
                clean_val.to_ascii_lowercase().as_str(),
                "conservative" | "standard" | "aggressive" | "extreme" | "custom"
            ) {
                return Err(AegisError::Validation(format!(
                    "Invalid risk_tier '{clean_val}': expected conservative, standard, aggressive, extreme, or custom"
                )));
            }
        }
        "alert_provider" => {
            if !matches!(
                clean_val.to_ascii_lowercase().as_str(),
                "telegram" | "discord" | "both"
            ) {
                return Err(AegisError::Validation(format!(
                    "Invalid alert_provider '{clean_val}': expected telegram, discord, or both"
                )));
            }
        }
        "auto_rebalance_enabled" => {
            parse_bool_strict(clean_key, clean_val)?;
        }
        "wallet_address" | "hyperliquid_agent_address" | "hyperliquid_vault_address" => {
            if !clean_val.is_empty() {
                let _ = aegis_core::validate_evm_address(clean_val)?;
            }
        }
        "private_key" => {
            if clean_val.is_empty() {
                return Err(AegisError::Validation(
                    "private_key cannot be cleared to empty via Web API".to_string(),
                ));
            }
            let _ = aegis_core::validate_private_key(clean_val)?;
        }
        "telegram_bot_token" => {
            if clean_val.is_empty()
                || !clean_val.contains(':')
                || clean_val.chars().any(char::is_whitespace)
            {
                return Err(AegisError::Validation(
                    "telegram_bot_token must be a valid non-empty bot token containing ':'"
                        .to_string(),
                ));
            }
        }
        "telegram_chat_id" => {
            if !clean_val.is_empty()
                && (clean_val.len() > 64
                    || clean_val.chars().any(char::is_whitespace)
                    || clean_val.contains('<')
                    || clean_val.contains('>'))
            {
                return Err(AegisError::Validation(
                    "Invalid telegram_chat_id format".to_string(),
                ));
            }
        }
        "discord_webhook_url" => {
            if !clean_val.is_empty() {
                let _ = aegis_alerts::validate_discord_webhook_url(clean_val)?;
            }
        }
        "gemini_api_key" => {
            if clean_val.is_empty() || clean_val.chars().any(char::is_whitespace) {
                return Err(AegisError::Validation(
                    "gemini_api_key must be non-empty without whitespace".to_string(),
                ));
            }
        }
        "target_leverage_multiplier" => {
            parse_f64_bounded(clean_key, clean_val, 0.10, 3.00)?;
        }
        "max_gross_leverage_x" => {
            parse_f64_bounded(clean_key, clean_val, 0.20, 5.00)?;
        }
        "max_single_asset_weight" => {
            parse_f64_bounded(clean_key, clean_val, 0.01, 1.00)?;
        }
        "min_rebalance_delta_weight" => {
            parse_f64_bounded(clean_key, clean_val, 0.001, 0.50)?;
        }
        "min_order_notional_usdc" => {
            parse_f64_bounded(clean_key, clean_val, 10.0, 100_000.0)?;
        }
        "initial_paper_equity_usdc" => {
            parse_f64_bounded(clean_key, clean_val, 100.0, 100_000_000.0)?;
        }
        "live_exchange_equity_usdc" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 100_000_000.0)?;
        }
        "risk_capital_cap_usdc" => {
            let v = parse_f64_bounded(clean_key, clean_val, 0.0, 100_000_000.0)?;
            if v > 0.0 && v < 10.0 {
                return Err(AegisError::Validation(
                    "risk_capital_cap_usdc must be 0 (disabled) or at least $10.00".to_string(),
                ));
            }
        }
        "core_min_day_volume_usdc" | "event_min_day_volume_usdc" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 10_000_000_000.0)?;
        }
        "s1_carry_mom_budget" | "s2_trend_breakout_budget" | "s3_event_reversion_budget" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 1.50)?;
        }
        "protective_stop_atr_mult" => {
            parse_f64_bounded(clean_key, clean_val, 0.5, 15.0)?;
        }
        "protective_stop_default_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.002, 0.50)?;
        }
        "take_profit_atr_mult" => {
            parse_f64_bounded(clean_key, clean_val, 0.5, 30.0)?;
        }
        "take_profit_default_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.002, 2.00)?;
        }
        "alo_chase_threshold_bps" => {
            parse_f64_bounded(clean_key, clean_val, 0.1, 500.0)?;
        }
        "maker_fee_bps" | "taker_fee_bps" => {
            parse_f64_bounded(clean_key, clean_val, 0.0, 100.0)?;
        }
        "max_entry_spread_bps" => {
            parse_f64_bounded(clean_key, clean_val, 1.0, 1000.0)?;
        }
        "max_daily_drawdown_pct" => {
            parse_f64_bounded(clean_key, clean_val, 0.01, 0.95)?;
        }
        "max_open_positions" => {
            parse_i64_bounded(clean_key, clean_val, 1, 50)?;
        }
        "min_entry_interval_sec" | "min_position_hold_sec" => {
            parse_i64_bounded(clean_key, clean_val, 0, 86_400)?;
        }
        "scanner_interval_ms" => {
            parse_i64_bounded(clean_key, clean_val, 500, 60_000)?;
        }
        "priority_monitor_interval_ms" => {
            parse_i64_bounded(clean_key, clean_val, 250, 10_000)?;
        }
        "stop_out_cooldown_sec" => {
            parse_i64_bounded(clean_key, clean_val, 60, 604_800)?;
        }
        "max_alo_chase_ticks" => {
            parse_i64_bounded(clean_key, clean_val, 1, 50)?;
        }
        "alert_dedup_window_ms" => {
            parse_i64_bounded(clean_key, clean_val, 0, 86_400_000)?;
        }
        "alert_http_timeout_sec" => {
            parse_i64_bounded(clean_key, clean_val, 2, 60)?;
        }
        "telemetry_queue_capacity" => {
            parse_i64_bounded(clean_key, clean_val, 64, 65_536)?;
        }
        "web_server_port" => {
            parse_i64_bounded(clean_key, clean_val, 1024, 65535)?;
        }
        _ => {
            return Err(AegisError::Validation(format!(
                "Unknown or unsupported configuration key '{clean_key}'"
            )));
        }
    }
    Ok(())
}

/// Builds `HealthPayload` by querying MariaDB 13.0 version, tables/views inventory, and active `system_config`.
/// Propagates MariaDB connectivity or schema errors rather than swallowing them.
pub fn build_health_payload(db: &DatabaseManager) -> Result<HealthPayload> {
    let mariadb_version = db.server_version()?;
    let (tables, views) = db.list_tables_and_views()?;
    let cfg = db.load_config_or_default();
    let tier = cfg.risk_tier();
    let mult = cfg.target_leverage_multiplier();
    let prio_ms = cfg.get_i64("priority_monitor_interval_ms", 1000);
    let scan_ms = cfg.get_i64("scanner_interval_ms", 3000);

    Ok(HealthPayload {
        status: "ok".to_string(),
        platform_version: PLATFORM_VERSION.to_string(),
        mariadb_version,
        database_name: db.db_name().to_string(),
        tables_count: tables.len(),
        views_healthy_count: views.len(),
        views,
        risk_tier: tier.as_str().to_string(),
        target_leverage_multiplier: mult,
        risk_tier_badge: tier.display_badge(Some(mult)),
        execution_mode: cfg.execution_mode().as_str().to_string(),
        loop_cadence: "1s Priority / 3s Scanner".to_string(),
        priority_monitor_interval_ms: prio_ms,
        scanner_interval_ms: scan_ms,
        timestamp_utc: now_iso8601_utc(),
        timestamp_ms: now_ms(),
    })
}

/// Builds the unified `DashboardPayload` querying MariaDB 13.0 tables and all 7 SQL Views.
/// Uses a 30-second cache for the `SHOW FULL TABLES` view-health check and performs zero writes.
pub fn build_dashboard_payload(db: &DatabaseManager) -> Result<DashboardPayload> {
    let mariadb_version = db.server_version()?;
    let (_, views) = get_cached_tables_and_views(db)?;
    let cfg = db.load_config_or_default();
    let tier = cfg.risk_tier();
    let mult = cfg.target_leverage_multiplier();

    let v_assets_held = db.get_assets_held().unwrap_or_default();
    let v_assets_ordered = db.get_assets_ordered().unwrap_or_default();
    let v_assets_in_cooldown = db.get_assets_in_cooldown().unwrap_or_default();
    let v_assets_sleeve_s1 = db.get_sleeve_s1_view().unwrap_or_default();
    let v_assets_sleeve_s2 = db.get_sleeve_s2_view().unwrap_or_default();
    let v_assets_sleeve_s3 = db.get_sleeve_s3_view().unwrap_or_default();
    let v_priority_watchlist = db.get_priority_watchlist().unwrap_or_default();
    let exchange_assets = db.list_exchange_assets().unwrap_or_default();
    let order_audit = db.list_recent_order_audits(50).unwrap_or_default();
    let telemetry_logs = db.list_recent_telemetry(50).unwrap_or_default();

    let mut snapshot = db.get_latest_portfolio_snapshot()?;
    // Ensure snapshot reflects live risk tier & live working/open counts if synthesized or updated
    snapshot.risk_tier = tier.as_str().to_string();
    snapshot.leverage_multiplier = mult;
    if snapshot.id == 0 {
        let reserved: f64 = v_assets_ordered
            .iter()
            .map(|o| o.reserved_margin_usdc)
            .sum();
        let committed: f64 = v_assets_held
            .iter()
            .map(|h| h.committed_notional_usdc)
            .sum();
        let upnl: f64 = v_assets_held.iter().map(|h| h.unrealized_pnl_usdc).sum();
        let live_eq = cfg.get_f64("live_exchange_equity_usdc", 0.0);
        let base_eq = if live_eq > 10.0 {
            live_eq
        } else {
            cfg.get_f64("initial_paper_equity_usdc", 10_000.0)
        };
        let total_eq = round_dp(base_eq + upnl, 2);
        let avail_cash = round_dp((total_eq - committed - reserved).max(0.0), 2);
        snapshot.total_equity_usdc = total_eq;
        snapshot.available_cash_usdc = avail_cash;
        snapshot.reserved_order_cash_usdc = round_dp(reserved, 2);
        snapshot.committed_notional_usdc = round_dp(committed, 2);
        snapshot.gross_leverage_x = if total_eq > 0.0 {
            round_dp(committed / total_eq, 3)
        } else {
            0.0
        };
        snapshot.working_order_count = v_assets_ordered.len() as i32;
        snapshot.open_long_count = v_assets_held
            .iter()
            .filter(|h| h.side.eq_ignore_ascii_case("LONG"))
            .count() as i32;
        snapshot.open_short_count = v_assets_held
            .iter()
            .filter(|h| h.side.eq_ignore_ascii_case("SHORT"))
            .count() as i32;
        snapshot.eligible_coin_count = exchange_assets
            .iter()
            .filter(|a| a.is_core_eligible || a.is_event_eligible)
            .count() as i32;
    }

    let mut snapshots_history = db.list_portfolio_snapshots(120).unwrap_or_default();
    if snapshots_history.is_empty() {
        snapshots_history.push(snapshot.clone());
    }

    let current_upnl: f64 = v_assets_held.iter().map(|h| h.unrealized_pnl_usdc).sum();
    let open_fees_funding: f64 = v_assets_held
        .iter()
        .map(|h| h.funding_accrued_usdc - h.fees_paid_usdc)
        .sum();
    let pnl_summary = db
        .compute_pnl_summary(current_upnl, open_fees_funding, snapshot.total_equity_usdc)
        .unwrap_or_default();

    let raw_configs = db.list_system_config_rows().unwrap_or_default();
    let system_config: Vec<SystemConfigRow> = raw_configs
        .into_iter()
        .map(|mut r| {
            if is_sensitive_config_key(&r.config_key) || r.value_type.eq_ignore_ascii_case("secret")
            {
                if r.config_value.trim().is_empty() {
                    r.config_value = "[UNSET]".to_string();
                } else if r.config_key.trim().eq_ignore_ascii_case("telegram_chat_id") {
                    r.config_value = mask_identifier(&r.config_value);
                } else {
                    r.config_value = "[FERNET_ENCRYPTED_SECRET]".to_string();
                }
            } else if r.config_key.trim().eq_ignore_ascii_case("wallet_address")
                && !r.config_value.trim().is_empty()
            {
                r.config_value = mask_identifier(&r.config_value);
            }
            r
        })
        .collect();

    let cap_epoch_ms = if cfg.execution_mode().is_live_or_testnet() {
        cfg.get_i64("risk_capital_cap_updated_at_ms", 0)
    } else {
        0
    };
    let max_open_positions = cfg.get_usize("max_open_positions", 6).max(1);
    let active_cap_open_count = v_assets_held
        .iter()
        .filter(|h| cap_epoch_ms <= 0 || h.opened_at_ms.unwrap_or(h.updated_at_ms) >= cap_epoch_ms)
        .count();
    let pre_cap_open_count = v_assets_held.len().saturating_sub(active_cap_open_count);

    Ok(DashboardPayload {
        platform_version: PLATFORM_VERSION.to_string(),
        mariadb_version,
        views_healthy_count: views.len(),
        execution_mode: cfg.execution_mode().as_str().to_string(),
        risk_tier: tier.as_str().to_string(),
        target_leverage_multiplier: mult,
        risk_tier_badge: tier.display_badge(Some(mult)),
        loop_cadence: "1s Priority / 3s Scanner".to_string(),
        risk_capital_cap_updated_at_ms: cap_epoch_ms,
        max_open_positions,
        active_cap_open_count,
        pre_cap_open_count,
        pnl_summary,
        snapshot,
        snapshots_history,
        v_assets_held,
        v_assets_ordered,
        v_assets_in_cooldown,
        v_assets_sleeve_s1,
        v_assets_sleeve_s2,
        v_assets_sleeve_s3,
        v_priority_watchlist,
        exchange_assets,
        order_audit,
        telemetry_logs,
        system_config,
        timestamp_ms: now_ms(),
    })
}

/// Closes an `OPEN` holding or cancels a `WORKING` order in MariaDB (and on Hyperliquid L1 via `ExecutionEngine`) and optionally inserts an asset cooldown.
pub fn execute_close_position_or_order(
    db: &DatabaseManager,
    req: &ClosePositionRequest,
) -> Result<ClosePositionResponse> {
    let reason = req
        .reason
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("MANUAL_WEB_CLOSE");
    let cooldown_sec = req.cooldown_sec.unwrap_or(3600).max(0);
    let engine = aegis_execution::ExecutionEngine::new(db.clone());

    if req.close_all {
        let held = db.get_assets_held().unwrap_or_default();
        let ordered = db.get_assets_ordered().unwrap_or_default();
        let total_targets = held.len() + ordered.len();
        let mut total_rpnl = 0.0;
        let mut closed_count = 0usize;
        let mut last_err: Option<AegisError> = None;

        for o in &ordered {
            match engine.close_or_cancel_position(o.position_id, reason, Some(cooldown_sec)) {
                Ok(rep) => {
                    if matches!(rep.final_status.as_str(), "CLOSED" | "CANCELLED") {
                        total_rpnl += rep.realized_pnl_usdc;
                        closed_count += 1;
                    }
                }
                Err(e) => last_err = Some(e),
            }
        }
        for h in &held {
            match engine.close_or_cancel_position(h.position_id, reason, Some(cooldown_sec)) {
                Ok(rep) => {
                    if matches!(rep.final_status.as_str(), "CLOSED" | "CANCELLED") {
                        total_rpnl += rep.realized_pnl_usdc;
                        closed_count += 1;
                    }
                }
                Err(e) => last_err = Some(e),
            }
        }

        if total_targets > 0 && closed_count == 0 {
            if let Some(err) = last_err {
                return Err(err);
            }
        }

        return Ok(ClosePositionResponse {
            status: "ok".to_string(),
            position_id: 0,
            asset_id: 0,
            symbol: "ALL".to_string(),
            previous_status: "OPEN".to_string(),
            new_status: if closed_count > 0 || total_targets == 0 {
                "CLOSED".to_string()
            } else {
                "OPEN".to_string()
            },
            realized_pnl_usdc: round_dp(total_rpnl, 4),
            cooldown_applied_sec: cooldown_sec,
            closed_count,
            updated_at_ms: now_ms(),
        });
    }

    let report = if let Some(pid) = req.position_id {
        match engine.close_or_cancel_position(pid, reason, Some(cooldown_sec)) {
            Ok(r) => r,
            Err(e) => {
                if let Some(ref raw_sym) = req.symbol {
                    if !raw_sym.trim().is_empty() {
                        engine.close_or_cancel_by_symbol(raw_sym, reason, Some(cooldown_sec))?
                    } else {
                        return Err(e);
                    }
                } else {
                    return Err(e);
                }
            }
        }
    } else if let Some(ref raw_sym) = req.symbol {
        engine.close_or_cancel_by_symbol(raw_sym, reason, Some(cooldown_sec))?
    } else {
        return Err(AegisError::Validation(
            "Either position_id, symbol, or close_all=true is required to close a position"
                .to_string(),
        ));
    };

    Ok(ClosePositionResponse {
        status: "ok".to_string(),
        position_id: report.position_id,
        asset_id: report.asset_id,
        symbol: report.symbol,
        previous_status: report.previous_status,
        new_status: report.final_status,
        realized_pnl_usdc: report.realized_pnl_usdc,
        cooldown_applied_sec: report.cooldown_inserted_sec,
        closed_count: 1,
        updated_at_ms: now_ms(),
    })
}

/// Applies a dynamic `RiskTier` update (`Conservative 0.65x`, `Standard 1.00x`, `Aggressive 1.35x`, `Extreme 1.85x`, `Custom`)
/// to MariaDB `system_config` and latest `portfolio_snapshots`.
pub fn execute_risk_tier_switch(
    db: &DatabaseManager,
    req: &RiskTierUpdateRequest,
) -> Result<RiskTierUpdateResponse> {
    let raw = req.risk_tier.trim().to_ascii_lowercase();
    let valid = matches!(
        raw.as_str(),
        "conservative"
            | "0.65x"
            | "standard"
            | "1.00x"
            | "1.0x"
            | "aggressive"
            | "1.35x"
            | "extreme"
            | "1.85x"
            | "custom"
    );
    if !valid {
        return Err(AegisError::Validation(format!(
            "Invalid risk_tier '{}': expected conservative, standard, aggressive, extreme, or custom",
            req.risk_tier
        )));
    }
    let tier = RiskTier::parse(&raw);
    if tier == RiskTier::Custom {
        if let Some(m) = req.custom_leverage_multiplier {
            if !m.is_finite() || !(0.10..=3.00).contains(&m) {
                return Err(AegisError::Validation(format!(
                    "custom_leverage_multiplier must be between 0.10 and 3.00, got {m}"
                )));
            }
        }
    }
    let mult = tier.leverage_multiplier(req.custom_leverage_multiplier);

    db.set_config_value(
        "risk_tier",
        tier.as_str(),
        Some("Active portfolio risk tier updated via Web UI"),
    )?;
    db.set_config_value(
        "target_leverage_multiplier",
        &format!("{mult:.2}"),
        Some("Active target leverage multiplier L_tier"),
    )?;

    // Immediately enforce max_open_positions & target_leverage_multiplier cap upon switching risk tier
    let cfg = db.load_config_or_default();
    let engine = aegis_execution::ExecutionEngine::new(db.clone());
    let base_eq = engine.resolve_effective_base_equity_usdc(&cfg);
    let _ = engine.enforce_leverage_and_position_caps();
    let _ = engine.record_portfolio_snapshot(Some(base_eq), None);

    let badge = tier.display_badge(Some(mult));
    let _ = db.log_telemetry(
        "INFO",
        "aegis-web.risk_tier",
        &format!("Switched portfolio risk tier to {} ({:.2}x)", badge, mult),
        None,
    );

    Ok(RiskTierUpdateResponse {
        status: "ok".to_string(),
        risk_tier: tier.as_str().to_string(),
        target_leverage_multiplier: mult,
        display_badge: badge,
        updated_at_ms: now_ms(),
    })
}

/// Converts an `AegisError` into an HTTP JSON response with appropriate status code.
pub fn map_aegis_error_response(err: AegisError) -> Response {
    let status = match err {
        AegisError::Validation(_) | AegisError::Config(_) => StatusCode::BAD_REQUEST,
        AegisError::SecurityViolation(_) => StatusCode::FORBIDDEN,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let body = ApiErrorBody {
        error: status.canonical_reason().unwrap_or("Error").to_string(),
        detail: err.to_string(),
    };
    (status, Json(body)).into_response()
}
