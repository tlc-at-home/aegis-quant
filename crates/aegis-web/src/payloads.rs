//! Typed JSON DTOs and MariaDB 13.0 + 7 SQL Views dashboard payload builders (`aegis-web::payloads`).

use aegis_core::{
    build_vault_info_payload_from_config, compute_drawdown_breaker_status,
    compute_equity_basis_summary, is_sensitive_config_key, mask_identifier, now_iso8601_utc,
    now_ms, partition_cap_epoch_holdings, round_dp, sanitize_telemetry_message,
    synthesize_fallback_snapshot, validate_core_config_key_value, AegisError, ClosedPositionView,
    ConfigMap, CooldownAssetView, DrawdownBreakerStatus, EquityBasisSummary, ExchangeAsset,
    HeldAssetView, OrderAuditRecord, OrderedAssetView, PnlSummary, PortfolioSnapshot,
    PriorityMonitorHealth, PriorityWatchView, Result, RiskTier, SleeveS1AssetView,
    SleeveS2AssetView, SleeveS3AssetView, SystemConfigRow, TelemetryLogRecord, TradingRunMode,
    VaultInfoPayload, VaultPositionInfo, IMMUTABLE_SECURITY_CONFIG_KEYS, PLATFORM_VERSION,
};
use aegis_db::DatabaseManager;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    OnceLock,
};

static SNAPSHOT_SEQ: AtomicU64 = AtomicU64::new(1);
static ERROR_CORRELATION_SEQ: AtomicU64 = AtomicU64::new(1);
static SERVER_BOOT_ID: OnceLock<i64> = OnceLock::new();

/// Critical dashboard sections whose failure triggers HTTP `503 Service Unavailable`
/// on `/api/v1/dashboard` and `/api/v1/state` (`R14-M22`).
pub const CRITICAL_DASHBOARD_SECTIONS: &[&str] = &[
    "database",
    "system_config",
    "portfolio_snapshot",
    "v_assets_held",
    "v_assets_ordered",
];

fn default_true() -> bool {
    true
}

/// Returns the monotonic server boot identifier for frontend restart detection (`R14-M24`).
pub fn server_boot_id() -> i64 {
    *SERVER_BOOT_ID.get_or_init(now_ms)
}

/// Allocates the next monotonic snapshot sequence number across REST and SSE (`R14-M24`).
pub fn next_snapshot_seq() -> u64 {
    SNAPSHOT_SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Generates a short unique correlation reference ID for sanitized 500/503 API errors (`R14-L16`).
pub fn next_error_correlation_id() -> String {
    let seq = ERROR_CORRELATION_SEQ.fetch_add(1, Ordering::Relaxed) & 0xffff;
    format!("err-{:x}-{seq:04x}", now_ms() as u64)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiErrorBody {
    pub error: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
}

/// Explicit degraded SSE event emitted on MariaDB / critical-view outage (`R14-M22`).
/// Intentionally omits `v_assets_held`, `v_assets_ordered`, and `snapshot` so connected
/// frontends preserve dimmed last-known holdings rather than wiping them to empty arrays.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DegradedDashboardEvent {
    pub snapshot_seq: u64,
    pub generated_at_ms: i64,
    pub server_boot_id: i64,
    pub timestamp_ms: i64,
    pub degraded: bool,
    pub database_reachable: bool,
    pub degraded_sections: Vec<String>,
    pub degraded_errors: Vec<String>,
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
    #[serde(default)]
    pub trading_run_mode: String,
    #[serde(default)]
    pub trading_run_mode_badge: String,
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
    #[serde(default)]
    pub trading_run_mode: String,
    #[serde(default)]
    pub trading_run_mode_badge: String,
    #[serde(default)]
    pub trading_run_mode_reason: String,
    #[serde(default)]
    pub trading_run_mode_updated_at_ms: i64,
    pub risk_tier: String,
    pub target_leverage_multiplier: f64,
    pub risk_tier_badge: String,
    pub loop_cadence: String,
    pub risk_capital_cap_updated_at_ms: i64,
    pub max_open_positions: usize,
    pub active_cap_open_count: usize,
    pub pre_cap_open_count: usize,
    pub pnl_summary: PnlSummary,
    pub vault_info: VaultInfoPayload,
    #[serde(default)]
    pub equity_basis: EquityBasisSummary,
    #[serde(default)]
    pub drawdown_breaker: DrawdownBreakerStatus,
    #[serde(default)]
    pub attached_tpsl_order_count: usize,
    #[serde(default)]
    pub exchange_open_orders_count: usize,
    pub snapshot: PortfolioSnapshot,
    pub snapshots_history: Vec<PortfolioSnapshot>,
    pub v_assets_held: Vec<HeldAssetView>,
    pub v_assets_ordered: Vec<OrderedAssetView>,
    pub recent_closed_positions: Vec<ClosedPositionView>,
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
    #[serde(default)]
    pub snapshot_seq: u64,
    #[serde(default)]
    pub generated_at_ms: i64,
    #[serde(default)]
    pub server_boot_id: i64,
    #[serde(default)]
    pub degraded: bool,
    #[serde(default = "default_true")]
    pub database_reachable: bool,
    #[serde(default)]
    pub degraded_sections: Vec<String>,
    #[serde(default)]
    pub degraded_errors: Vec<String>,
    #[serde(default)]
    pub immutable_config_keys: Vec<String>,
    #[serde(default)]
    pub priority_monitor_health: PriorityMonitorHealth,
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

/// Request payload for `POST /api/v1/run-mode`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunModeUpdateRequest {
    pub run_mode: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub flatten_open_positions: bool,
    #[serde(default)]
    pub cooldown_sec: Option<i64>,
}

/// Response payload for `POST /api/v1/run-mode`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunModeUpdateResponse {
    pub status: String,
    pub previous_mode: String,
    pub trading_run_mode: String,
    pub requested_mode: String,
    pub display_badge: String,
    pub fallback_to_halted: bool,
    pub working_orders_cancelled: usize,
    pub open_positions_closed: usize,
    pub remaining_open_positions: usize,
    pub remaining_working_orders: usize,
    pub realized_pnl_usdc: f64,
    pub cooldown_applied_sec: i64,
    pub reason: String,
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

/// Request payload for `POST /api/v1/vault/inspect`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VaultInspectRequest {
    pub vault_address: String,
}

/// Response payload for `POST /api/v1/vault/inspect`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultInspectResponse {
    pub status: String,
    pub execution_mode: String,
    pub is_testnet: bool,
    pub found: bool,
    pub vault: Option<VaultPositionInfo>,
    pub inspected_at_ms: i64,
}

/// Response payload for `POST /api/v1/vault/sync`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultSyncResponse {
    pub status: String,
    pub execution_mode: String,
    pub synced: bool,
    pub vault_info: VaultInfoPayload,
    pub synced_at_ms: i64,
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
/// Delegates canonical rules to [`aegis_core::validate_core_config_key_value`] (including W2
/// scale-in spacing & reduce breakeven hysteresis keys: `scale_in_min_atr_mult`,
/// `scale_in_min_spacing_bps`, `scale_in_reduce_cooldown_sec`, `reduce_min_net_edge_bps`)
/// and enforces SSRF checks on `discord_webhook_url`.
pub fn validate_config_key_value(key: &str, value_str: &str) -> Result<()> {
    validate_core_config_key_value(key, value_str)?;
    if key.trim().eq_ignore_ascii_case("discord_webhook_url") {
        let clean_val = value_str.trim();
        if !clean_val.is_empty() {
            let _ = aegis_alerts::validate_discord_webhook_url(clean_val)?;
        }
    }
    Ok(())
}

/// Builds `HealthPayload` by querying MariaDB 13.0 version, tables/views inventory, and active `system_config`.
/// Propagates MariaDB connectivity or schema errors rather than swallowing them.
pub fn build_health_payload(db: &DatabaseManager) -> Result<HealthPayload> {
    let mariadb_version = db.server_version()?;
    let (tables, views) = db.list_tables_and_views()?;
    let cfg = db.load_config()?;
    let tier = cfg.risk_tier();
    let mult = cfg.target_leverage_multiplier();
    let run_mode = cfg.trading_run_mode();
    let prio_ms = cfg.get_i64("priority_monitor_interval_ms", 1000);
    let scan_ms = cfg.get_i64("scanner_interval_ms", 3000);
    let loop_cadence = if run_mode.suspends_background_loops() {
        "STANDBY (Loops Paused)".to_string()
    } else {
        "1s Priority / 3s Scanner".to_string()
    };

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
        trading_run_mode: run_mode.as_str().to_string(),
        trading_run_mode_badge: run_mode.display_badge().to_string(),
        loop_cadence,
        priority_monitor_interval_ms: prio_ms,
        scanner_interval_ms: scan_ms,
        timestamp_utc: now_iso8601_utc(),
        timestamp_ms: now_ms(),
    })
}

/// Builds the `vault_info` payload purely from cached `system_config` (`live_vault_info_json`)
/// without ever issuing network calls inside HTTP/SSE handlers.
/// Always returns zeros + `vaults: []` in PAPER mode or before initial vault sync.
/// Vault equity is strictly segregated from perp equity (`snapshot.total_equity_usdc` is untouched).
pub fn build_vault_info_payload(
    cfg: &ConfigMap,
    perp_equity_usdc: f64,
    now_ms_val: i64,
) -> VaultInfoPayload {
    build_vault_info_payload_from_config(cfg, perp_equity_usdc, now_ms_val)
}

fn collect_section<T: Default>(
    res: Result<T>,
    section: &str,
    degraded_sections: &mut Vec<String>,
    degraded_errors: &mut Vec<String>,
) -> T {
    match res {
        Ok(val) => val,
        Err(err) => {
            let cid = next_error_correlation_id();
            tracing::error!(
                correlation_id = %cid,
                section = section,
                error = %err,
                "Dashboard section query failed"
            );
            degraded_sections.push(section.to_string());
            degraded_errors.push(format!("Section '{section}' unavailable (ref: {cid})"));
            T::default()
        }
    }
}

/// Builds a sanitized HTTP `503 Service Unavailable` JSON body for `/api/v1/dashboard` and `/api/v1/state` (`R14-M22`, `R14-L16`).
pub fn build_degraded_dashboard_body(
    database_reachable: bool,
    degraded_sections: Vec<String>,
    degraded_errors: Vec<String>,
    correlation_id: String,
    snapshot_seq: u64,
    generated_at_ms: i64,
    server_boot_id: i64,
) -> Value {
    json!({
        "error": "Service Unavailable",
        "detail": if database_reachable {
            "Critical dashboard telemetry sections are unavailable. Reference server logs with correlation_id."
        } else {
            "MariaDB database unavailable. Reference server logs with correlation_id."
        },
        "correlation_id": correlation_id,
        "degraded": true,
        "database_reachable": database_reachable,
        "degraded_sections": degraded_sections,
        "degraded_errors": degraded_errors,
        "snapshot_seq": snapshot_seq,
        "generated_at_ms": generated_at_ms,
        "server_boot_id": server_boot_id,
    })
}

/// Builds a sanitized SSE `DegradedDashboardEvent` JSON string omitting `v_assets_held`/`v_assets_ordered`/`snapshot`.
pub fn build_degraded_sse_event_json(
    database_reachable: bool,
    degraded_sections: Vec<String>,
    degraded_errors: Vec<String>,
    snapshot_seq: u64,
    generated_at_ms: i64,
    boot_id: i64,
) -> String {
    let ev = DegradedDashboardEvent {
        snapshot_seq,
        generated_at_ms,
        server_boot_id: boot_id,
        timestamp_ms: generated_at_ms,
        degraded: true,
        database_reachable,
        degraded_sections,
        degraded_errors,
    };
    serde_json::to_string(&ev).unwrap_or_else(|_| {
        format!(
            r#"{{"snapshot_seq":{snapshot_seq},"generated_at_ms":{generated_at_ms},"server_boot_id":{boot_id},"timestamp_ms":{generated_at_ms},"degraded":true,"database_reachable":{database_reachable},"degraded_sections":["database"],"degraded_errors":["Telemetry temporarily unavailable"]}}"#
        )
    })
}

/// Returns `true` when any section in `degraded_sections` is a critical dashboard section.
pub fn has_critical_degradation(payload: &DashboardPayload) -> bool {
    !payload.database_reachable
        || payload
            .degraded_sections
            .iter()
            .any(|s| CRITICAL_DASHBOARD_SECTIONS.contains(&s.as_str()))
}

/// Builds the unified `DashboardPayload` over a single pooled read-only MariaDB connection
/// (`load_dashboard_read_bundle`), tracking section failures without swallowing DB outages (`R14-M22`, `R14-M28`).
pub fn build_dashboard_payload(db: &DatabaseManager) -> Result<DashboardPayload> {
    let snapshot_seq = next_snapshot_seq();
    let generated_at_ms = now_ms();
    let boot_id = server_boot_id();

    let bundle = db.load_dashboard_read_bundle()?;
    let mariadb_version = bundle.mariadb_version?;
    let cfg = bundle.config?;
    let latest_snapshot_opt = bundle.latest_snapshot?;

    let mut degraded_sections = Vec::new();
    let mut degraded_errors = Vec::new();

    let (_, views) = collect_section(
        bundle.tables_and_views,
        "tables_and_views",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    let tier = cfg.risk_tier();
    let mult = cfg.target_leverage_multiplier();
    let ts = generated_at_ms;

    let v_assets_held = collect_section(
        bundle.assets_held,
        "v_assets_held",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    let v_assets_ordered = collect_section(
        bundle.assets_ordered,
        "v_assets_ordered",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    let recent_closed_positions = collect_section(
        bundle.recent_closed_positions,
        "recent_closed_positions",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    let v_assets_in_cooldown = collect_section(
        bundle.assets_in_cooldown,
        "v_assets_in_cooldown",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    let v_assets_sleeve_s1 = collect_section(
        bundle.sleeve_s1,
        "v_assets_sleeve_s1",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    let v_assets_sleeve_s2 = collect_section(
        bundle.sleeve_s2,
        "v_assets_sleeve_s2",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    let v_assets_sleeve_s3 = collect_section(
        bundle.sleeve_s3,
        "v_assets_sleeve_s3",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    let v_priority_watchlist = collect_section(
        bundle.priority_watchlist,
        "v_priority_watchlist",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    let exchange_assets = collect_section(
        bundle.exchange_assets,
        "exchange_assets",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    let order_audit = collect_section(
        bundle.order_audit,
        "order_audit",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    let telemetry_logs = collect_section(
        bundle.telemetry_logs,
        "telemetry_logs",
        &mut degraded_sections,
        &mut degraded_errors,
    );

    let is_live_or_testnet = cfg.execution_mode().is_live_or_testnet();
    let live_eq = cfg.get_f64("live_exchange_equity_usdc", 0.0);

    let mut snapshot = match latest_snapshot_opt {
        Some(mut s) if s.id > 0 => {
            s.risk_tier = tier.as_str().to_string();
            s.leverage_multiplier = mult;
            s
        }
        _ => synthesize_fallback_snapshot(
            &cfg,
            &v_assets_held,
            &v_assets_ordered,
            &exchange_assets,
            ts,
        ),
    };
    snapshot.risk_tier = tier.as_str().to_string();
    snapshot.leverage_multiplier = mult;

    let mut snapshots_history = collect_section(
        bundle.snapshots_history,
        "snapshots_history",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    if snapshots_history.is_empty() {
        snapshots_history.push(snapshot.clone());
    }

    let current_upnl: f64 = v_assets_held.iter().map(|h| h.unrealized_pnl_usdc).sum();
    let open_fees_funding: f64 = v_assets_held
        .iter()
        .map(|h| h.funding_accrued_usdc - h.fees_paid_usdc)
        .sum();
    let pnl_summary = collect_section(
        db.compute_pnl_summary(current_upnl, open_fees_funding, snapshot.total_equity_usdc),
        "pnl_summary",
        &mut degraded_sections,
        &mut degraded_errors,
    );

    let raw_configs = collect_section(
        bundle.system_config_rows,
        "system_config",
        &mut degraded_sections,
        &mut degraded_errors,
    );
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

    let cap_epoch_ms = if is_live_or_testnet {
        cfg.get_i64("risk_capital_cap_updated_at_ms", 0)
    } else {
        0
    };
    let max_open_positions = cfg.get_usize("max_open_positions", 6).max(1);
    let (active_cap_open_count, pre_cap_open_count) =
        partition_cap_epoch_holdings(&v_assets_held, cap_epoch_ms);

    let perp_wallet_equity_usdc = if is_live_or_testnet && live_eq > 0.0 {
        round_dp(live_eq, 2)
    } else {
        round_dp(snapshot.total_equity_usdc, 2)
    };
    let vault_info = build_vault_info_payload(&cfg, perp_wallet_equity_usdc, ts);
    let equity_basis =
        compute_equity_basis_summary(&cfg, &snapshot, vault_info.total_vault_equity_usdc, ts);
    let synced_at_ms = equity_basis.synced_at_ms;

    // Evaluate true 24h uncapped drawdown (never truncated by cap_epoch_ms)
    let dd_window_start = ts - 86_400_000;
    let max_dd_frac = cfg
        .get_f64("max_daily_drawdown_pct", 0.15)
        .clamp(0.01, 0.95);
    let min_halt = cfg.get_f64("min_equity_halt_usdc", 50.0);
    let dd_basis = if is_live_or_testnet {
        "UNCAPPED_WALLET_EQUITY"
    } else {
        "PAPER_EQUITY"
    };
    let dd_peak_res = if is_live_or_testnet {
        db.get_peak_and_latest_uncapped_equity_since(dd_window_start)
    } else {
        db.get_peak_and_latest_equity_since(dd_window_start)
    };
    let dd_peak_latest = collect_section(
        dd_peak_res,
        "drawdown_breaker",
        &mut degraded_sections,
        &mut degraded_errors,
    );
    let drawdown_breaker = compute_drawdown_breaker_status(
        dd_peak_latest,
        max_dd_frac,
        min_halt,
        dd_basis,
        dd_window_start,
    );

    let attached_tpsl_order_count: usize = v_assets_held
        .iter()
        .map(|h| {
            let has_sl = h
                .protective_stop_oid
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty());
            let has_tp = h
                .take_profit_oid
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty());
            usize::from(has_sl) + usize::from(has_tp)
        })
        .sum();
    let fallback_open_orders = v_assets_ordered.len() + attached_tpsl_order_count;
    let exchange_open_orders_count = if is_live_or_testnet && synced_at_ms > 0 {
        cfg.get_usize("live_exchange_open_orders_count", fallback_open_orders)
    } else {
        fallback_open_orders
    };

    let run_mode = cfg.trading_run_mode();
    let run_mode_reason = cfg.get_str("trading_run_mode_reason", "");
    let run_mode_updated_at_ms = cfg.get_i64("trading_run_mode_updated_at_ms", 0);

    let mut priority_monitor_health: PriorityMonitorHealth = cfg
        .get("priority_monitor_health_json")
        .and_then(|raw| serde_json::from_str::<PriorityMonitorHealth>(raw.trim()).ok())
        .unwrap_or_default();
    let live_tick_ms = db.live_priority_tick_ms();
    if live_tick_ms > priority_monitor_health.last_tick_at_ms
        && (priority_monitor_health.status == "HEALTHY" || priority_monitor_health.status == "IDLE")
    {
        priority_monitor_health.last_tick_at_ms = live_tick_ms;
        if priority_monitor_health.status == "IDLE" {
            priority_monitor_health.status = "HEALTHY".to_string();
        }
    }
    let prio_interval_ms = cfg.get_i64("priority_monitor_interval_ms", 1000).max(250);
    let stale_threshold_ms =
        (prio_interval_ms * 15).max(aegis_execution::HEALTH_PERSIST_HEARTBEAT_MS * 3 + 15_000);
    if run_mode.suspends_background_loops()
        && v_assets_held.is_empty()
        && v_assets_ordered.is_empty()
    {
        priority_monitor_health.is_stale = false;
        priority_monitor_health.unmonitored_alert_active = false;
        priority_monitor_health.status = "STANDBY".to_string();
    } else if priority_monitor_health.last_tick_at_ms > 0
        && ts.saturating_sub(priority_monitor_health.last_tick_at_ms) > stale_threshold_ms
        && (!v_assets_held.is_empty() || !v_assets_ordered.is_empty())
    {
        priority_monitor_health.is_stale = true;
        if priority_monitor_health.status == "HEALTHY" || priority_monitor_health.status == "IDLE" {
            priority_monitor_health.status = "UNMONITORED".to_string();
        }
    }

    let immutable_config_keys: Vec<String> = IMMUTABLE_SECURITY_CONFIG_KEYS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    let degraded = !degraded_sections.is_empty();
    let loop_cadence = if run_mode.suspends_background_loops() {
        "STANDBY (Loops Paused)".to_string()
    } else {
        "1s Priority / 3s Scanner".to_string()
    };

    Ok(DashboardPayload {
        platform_version: PLATFORM_VERSION.to_string(),
        mariadb_version,
        views_healthy_count: views.len(),
        execution_mode: cfg.execution_mode().as_str().to_string(),
        trading_run_mode: run_mode.as_str().to_string(),
        trading_run_mode_badge: run_mode.display_badge().to_string(),
        trading_run_mode_reason: run_mode_reason,
        trading_run_mode_updated_at_ms: run_mode_updated_at_ms,
        risk_tier: tier.as_str().to_string(),
        target_leverage_multiplier: mult,
        risk_tier_badge: tier.display_badge(Some(mult)),
        loop_cadence,
        risk_capital_cap_updated_at_ms: cap_epoch_ms,
        max_open_positions,
        active_cap_open_count,
        pre_cap_open_count,
        pnl_summary,
        vault_info,
        equity_basis,
        drawdown_breaker,
        attached_tpsl_order_count,
        exchange_open_orders_count,
        snapshot,
        snapshots_history,
        v_assets_held,
        v_assets_ordered,
        recent_closed_positions,
        v_assets_in_cooldown,
        v_assets_sleeve_s1,
        v_assets_sleeve_s2,
        v_assets_sleeve_s3,
        v_priority_watchlist,
        exchange_assets,
        order_audit,
        telemetry_logs,
        system_config,
        timestamp_ms: ts,
        snapshot_seq,
        generated_at_ms,
        server_boot_id: boot_id,
        degraded,
        database_reachable: true,
        degraded_sections,
        degraded_errors,
        immutable_config_keys,
        priority_monitor_health,
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
        let held = db.get_assets_held()?;
        let ordered = db.get_assets_ordered()?;
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
    let risk_pct = tier.default_risk_per_trade_equity_pct(Some(mult));
    let max_w = tier.default_max_single_asset_weight(Some(mult));

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
    db.set_config_value(
        "risk_per_trade_equity_pct",
        &format!("{risk_pct:.4}"),
        Some("Per-trade equity risk budget synchronized with risk_tier"),
    )?;
    db.set_config_value(
        "max_single_asset_weight",
        &format!("{max_w:.4}"),
        Some("Single-asset weight cap synchronized with risk_tier"),
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

/// Applies an operator `TradingRunMode` transition (`ACTIVE`, `DRAIN`, `PAUSED`, `HALTED`, `STANDBY`)
/// via `ExecutionEngine::set_trading_run_mode`.
pub fn execute_run_mode_switch(
    db: &DatabaseManager,
    req: &RunModeUpdateRequest,
) -> Result<RunModeUpdateResponse> {
    let target_mode = TradingRunMode::parse_strict(&req.run_mode)?;
    let engine = aegis_execution::ExecutionEngine::new(db.clone());
    let rep = engine.set_trading_run_mode(&aegis_execution::RunModeTransitionRequest {
        mode: target_mode,
        reason: req.reason.clone(),
        flatten_open_positions: req.flatten_open_positions,
        cooldown_seconds: req.cooldown_sec,
    })?;
    let eff_mode = TradingRunMode::parse_strict(&rep.effective_mode)?;
    Ok(RunModeUpdateResponse {
        status: "ok".to_string(),
        previous_mode: rep.previous_mode,
        trading_run_mode: rep.effective_mode,
        requested_mode: rep.requested_mode,
        display_badge: eff_mode.display_badge().to_string(),
        fallback_to_halted: rep.fallback_to_halted,
        working_orders_cancelled: rep.working_orders_cancelled,
        open_positions_closed: rep.open_positions_closed,
        remaining_open_positions: rep.remaining_open_positions,
        remaining_working_orders: rep.remaining_working_orders,
        realized_pnl_usdc: rep.realized_pnl_usdc,
        cooldown_applied_sec: rep.cooldown_applied_sec,
        reason: rep.reason,
        updated_at_ms: rep.updated_at_ms,
    })
}

/// Queries live Hyperliquid `/info {"type":"vaultDetails"}` for an arbitrary validated EVM vault
/// address without mutating `system_config` or cached `live_vault_info_json`.
pub fn execute_vault_inspect(
    db: &DatabaseManager,
    req: &VaultInspectRequest,
) -> Result<VaultInspectResponse> {
    let valid_addr = aegis_core::validate_evm_address(&req.vault_address)?;
    let cfg = db.load_config_or_default();
    let mode = cfg.execution_mode();
    if !mode.is_live_or_testnet() {
        return Err(AegisError::Validation(
            "Live vault inspection is unavailable in PAPER mode; switch execution_mode to TESTNET or LIVE"
                .to_string(),
        ));
    }

    let is_testnet = aegis_execution::is_testnet_configured(&cfg);
    let wallet = cfg.get_str("wallet_address", "");
    let http = aegis_execution::HyperliquidHttpClient::new(8);
    let details_opt = http.fetch_vault_details(&valid_addr, wallet.trim(), &cfg)?;
    let ts = now_ms();
    let (found, vault) = match details_opt {
        Some(details) => {
            let parsed =
                aegis_execution::parse_vault_position_info(&valid_addr, &details, None, ts);
            (true, Some(parsed))
        }
        None => (false, None),
    };

    Ok(VaultInspectResponse {
        status: "ok".to_string(),
        execution_mode: mode.as_str().to_string(),
        is_testnet,
        found,
        vault,
        inspected_at_ms: ts,
    })
}

/// Forces an immediate Hyperliquid vault sync (`min_interval_ms = 0`) in TESTNET/LIVE mode
/// strictly without holding `execution_lock()` over network HTTP IO (`R14-H2`).
pub fn execute_vault_sync(db: &DatabaseManager) -> Result<VaultSyncResponse> {
    let cfg = db.load_config_or_default();
    let mode = cfg.execution_mode();
    let ts = now_ms();
    if !mode.is_live_or_testnet() {
        let snap_eq = db
            .get_latest_portfolio_snapshot()
            .map(|s| s.total_equity_usdc)
            .unwrap_or(0.0);
        let vault_info = build_vault_info_payload(&cfg, snap_eq, ts);
        let synced_at_ms = vault_info.synced_at_ms;
        return Ok(VaultSyncResponse {
            status: "ok".to_string(),
            execution_mode: mode.as_str().to_string(),
            synced: false,
            vault_info,
            synced_at_ms,
        });
    }

    let synced_opt = {
        let fresh_cfg = db.load_config_or_default();
        if !fresh_cfg.execution_mode().is_live_or_testnet() {
            None
        } else {
            let reconciler = aegis_execution::CutoverReconciler::new(
                db.clone(),
                aegis_execution::HyperliquidHttpClient::new(8),
            );
            reconciler.sync_vault_info_with_interval(&fresh_cfg, ts, 0, None)
        }
    };

    let post_cfg = db.load_config_or_default();
    let post_mode = post_cfg.execution_mode();
    let live_eq = post_cfg.get_f64("live_exchange_equity_usdc", 0.0);
    let perp_wallet_eq = if post_mode.is_live_or_testnet() && live_eq > 0.0 {
        round_dp(live_eq, 2)
    } else {
        round_dp(
            db.get_latest_portfolio_snapshot()
                .map(|s| s.total_equity_usdc)
                .unwrap_or(0.0),
            2,
        )
    };
    let vault_info = build_vault_info_payload(&post_cfg, perp_wallet_eq, ts);

    match synced_opt {
        Some(snapshot) => {
            let _ = db.log_telemetry(
                "INFO",
                "aegis-web.vault_sync",
                &format!(
                    "Manual vault sync completed: {} vault(s), total_vault_equity=${:.2}",
                    snapshot.vault_count, snapshot.total_vault_equity_usdc
                ),
                None,
            );
            Ok(VaultSyncResponse {
                status: "ok".to_string(),
                execution_mode: post_mode.as_str().to_string(),
                synced: true,
                vault_info,
                synced_at_ms: snapshot.synced_at_ms,
            })
        }
        None => {
            let synced_at_ms = vault_info.synced_at_ms;
            Ok(VaultSyncResponse {
                status: "ok".to_string(),
                execution_mode: post_mode.as_str().to_string(),
                synced: false,
                vault_info,
                synced_at_ms,
            })
        }
    }
}

/// Converts an `AegisError` into a sanitized HTTP JSON response with appropriate status code (`R14-L16`).
/// Logs full error diagnostics internally under a correlation ID and never leaks raw SQL/DSNs/paths.
pub fn map_aegis_error_response(err: AegisError) -> Response {
    match err {
        AegisError::Validation(ref msg) | AegisError::Config(ref msg) => {
            let body = ApiErrorBody {
                error: StatusCode::BAD_REQUEST
                    .canonical_reason()
                    .unwrap_or("Bad Request")
                    .to_string(),
                detail: sanitize_telemetry_message(msg),
                correlation_id: None,
            };
            (StatusCode::BAD_REQUEST, Json(body)).into_response()
        }
        AegisError::SecurityViolation(ref msg) => {
            let body = ApiErrorBody {
                error: StatusCode::FORBIDDEN
                    .canonical_reason()
                    .unwrap_or("Forbidden")
                    .to_string(),
                detail: sanitize_telemetry_message(msg),
                correlation_id: None,
            };
            (StatusCode::FORBIDDEN, Json(body)).into_response()
        }
        AegisError::Database(_) => {
            let cid = next_error_correlation_id();
            tracing::error!(correlation_id = %cid, error = %err, "Database API error");
            let body = ApiErrorBody {
                error: StatusCode::SERVICE_UNAVAILABLE
                    .canonical_reason()
                    .unwrap_or("Service Unavailable")
                    .to_string(),
                detail:
                    "Database operation unavailable. Reference server logs with correlation_id."
                        .to_string(),
                correlation_id: Some(cid),
            };
            (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
        }
        _ => {
            let cid = next_error_correlation_id();
            tracing::error!(correlation_id = %cid, error = %err, "Internal API error");
            let body = ApiErrorBody {
                error: StatusCode::INTERNAL_SERVER_ERROR
                    .canonical_reason()
                    .unwrap_or("Internal Server Error")
                    .to_string(),
                detail:
                    "Internal server error occurred. Reference server logs with correlation_id."
                        .to_string(),
                correlation_id: Some(cid),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_w2_scale_in_and_reduce_hysteresis_config_keys() {
        assert!(validate_config_key_value("scale_in_min_atr_mult", "0.25").is_ok());
        assert!(validate_config_key_value("scale_in_min_atr_mult", "5.5").is_err());
        assert!(validate_config_key_value("scale_in_min_spacing_bps", "15.0").is_ok());
        assert!(validate_config_key_value("scale_in_min_spacing_bps", "-5.0").is_err());
        assert!(validate_config_key_value("scale_in_reduce_cooldown_sec", "900").is_ok());
        assert!(validate_config_key_value("scale_in_reduce_cooldown_sec", "90000").is_err());
        assert!(validate_config_key_value("reduce_min_net_edge_bps", "8.0").is_ok());
        assert!(validate_config_key_value("reduce_min_net_edge_bps", "600.0").is_err());
    }
}
