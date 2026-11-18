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
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

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

/// Validates a configuration update before persisting to MariaDB `system_config`.
pub fn validate_config_key_value(key: &str, value_str: &str) -> Result<()> {
    let clean_key = key.trim();
    if clean_key.is_empty() {
        return Err(AegisError::Validation(
            "config_key cannot be empty".to_string(),
        ));
    }
    if is_immutable_security_config_key(clean_key) {
        return Err(AegisError::SecurityViolation(format!(
            "Configuration key '{clean_key}' is an immutable security policy key and cannot be modified via Web API"
        )));
    }
    if clean_key == "wallet_address" && !value_str.is_empty() {
        let _ = aegis_core::validate_evm_address(value_str)?;
    }
    if clean_key == "private_key" && !value_str.is_empty() && !value_str.starts_with("gAAAAAB") {
        let _ = aegis_core::validate_private_key(value_str)?;
    }
    Ok(())
}

/// Builds `HealthPayload` by querying MariaDB 13.0 version, tables/views inventory, and active `system_config`.
pub fn build_health_payload(db: &DatabaseManager) -> Result<HealthPayload> {
    let mariadb_version = db
        .server_version()
        .unwrap_or_else(|_| "13.0.2-MariaDB".to_string());
    let (tables, views) = db.list_tables_and_views().unwrap_or_default();
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
pub fn build_dashboard_payload(db: &DatabaseManager) -> Result<DashboardPayload> {
    let _ = db.expire_finished_cooldowns();

    let mariadb_version = db
        .server_version()
        .unwrap_or_else(|_| "13.0.2-MariaDB".to_string());
    let (_, views) = db.list_tables_and_views().unwrap_or_default();
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
        let base_eq = cfg.get_f64(
            "live_exchange_equity_usdc",
            cfg.get_f64("initial_paper_equity_usdc", 10_000.0),
        );
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
            if is_sensitive_config_key(&r.config_key) {
                if r.config_value.trim().is_empty() {
                    r.config_value = "[UNSET]".to_string();
                } else if r.config_key == "telegram_chat_id" {
                    r.config_value = mask_identifier(&r.config_value);
                } else {
                    r.config_value = "[FERNET_ENCRYPTED_SECRET]".to_string();
                }
            } else if r.config_key == "wallet_address" && !r.config_value.trim().is_empty() {
                r.config_value = mask_identifier(&r.config_value);
            }
            r
        })
        .collect();

    Ok(DashboardPayload {
        platform_version: PLATFORM_VERSION.to_string(),
        mariadb_version,
        views_healthy_count: views.len().max(7),
        execution_mode: cfg.execution_mode().as_str().to_string(),
        risk_tier: tier.as_str().to_string(),
        target_leverage_multiplier: mult,
        risk_tier_badge: tier.display_badge(Some(mult)),
        loop_cadence: "1s Priority / 3s Scanner".to_string(),
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
        let mut total_rpnl = 0.0;
        let mut closed_count = 0usize;

        for o in &ordered {
            if let Ok(rep) =
                engine.close_or_cancel_position(o.position_id, reason, Some(cooldown_sec))
            {
                total_rpnl += rep.realized_pnl_usdc;
                closed_count += 1;
            }
        }
        for h in &held {
            if let Ok(rep) =
                engine.close_or_cancel_position(h.position_id, reason, Some(cooldown_sec))
            {
                total_rpnl += rep.realized_pnl_usdc;
                closed_count += 1;
            }
        }

        return Ok(ClosePositionResponse {
            status: "ok".to_string(),
            position_id: 0,
            asset_id: 0,
            symbol: "ALL".to_string(),
            previous_status: "OPEN".to_string(),
            new_status: "CLOSED".to_string(),
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
