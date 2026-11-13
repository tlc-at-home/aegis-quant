//! HTTP and SSE route handlers for `aegis-web`.

use crate::{
    constant_time_eq, extract_cookie_token, generate_ephemeral_session_token,
    payloads::{
        build_dashboard_payload, build_pond_payload, build_portfolio_payload, build_pp50_payload,
        enrich_pond_cells, enrich_signals, map_err_response, run_db_blocking, AlertsUpdatePayload,
        AlertsUpdateResponse, ApiErrorBody, AuthLoginPayload, AuthLoginResponse,
        BacktestRequestPayload, BacktestRunResponse, BatchConfigUpdateResponse,
        CalibrateRunResponse, CloseTradePayload, CloseTradeResponse, ConfigBatchUpdatePayload,
        ConfigUpdatePayload, CredentialsUpdatePayload, CredentialsUpdateResponse,
        CycleTriggerResponse, DbBackupRequestPayload, DbBackupTriggerResponse, HealthPayload,
        LimitQuery, PondScanTriggerResponse, PortfolioSummaryPayload, SettleRequestPayload,
        SettleTriggerResponse, SingleConfigUpdateResponse, StatusPayload, WalletSyncPayload,
        WalletSyncResponse,
    },
    AppState,
};
use aegis_alerts::AlertEngine;
use aegis_backtest::BacktestEngine;
pub use crate::payloads::validate_config_update;
use aegis_core::PLATFORM_VERSION;
use aegis_execution::ExecutionEngine;
use aegis_pit::PitOrchestrator;
use aegis_pond::PondOrchestrator;
use aegis_settle::{SettleController, SettleOverrideInput};
use aegis_signals::WalkForwardCalibrator;
use axum::{
    extract::{Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    Json,
};
use serde_json::{json, Value};
use std::{convert::Infallible, time::Duration};
use tokio_stream::{wrappers::IntervalStream, StreamExt};

pub const EMBEDDED_INDEX_HTML: &str = include_str!("../../../web/static/index.html");
pub const EMBEDDED_APP_JS: &str = include_str!("../../../web/static/app.js");

pub async fn serve_index(State(state): State<AppState>) -> Response {
    let index_path = state.static_dir.join("index.html");
    let html_content = match tokio::fs::read_to_string(&index_path).await {
        Ok(content) => content,
        Err(_) => EMBEDDED_INDEX_HTML.to_string(),
    };
    let mut resp = Html(html_content).into_response();
    if let Ok(hv) = HeaderValue::from_str("no-store") {
        resp.headers_mut().insert(header::CACHE_CONTROL, hv);
    }
    resp
}

pub async fn serve_app_js(State(state): State<AppState>) -> Response {
    let js_path = state.static_dir.join("app.js");
    let js_content = match tokio::fs::read_to_string(&js_path).await {
        Ok(content) => content,
        Err(_) => EMBEDDED_APP_JS.to_string(),
    };
    let mut resp = (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        js_content,
    )
        .into_response();
    if let Ok(hv) = HeaderValue::from_str("no-store") {
        resp.headers_mut().insert(header::CACHE_CONTROL, hv);
    }
    resp
}

pub async fn api_auth_login(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<AuthLoginPayload>,
) -> Response {
    let client_key = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or("local").trim().to_string())
        .unwrap_or_else(|| "local".to_string());

    // ME-14: Rate-limit login failures (>= 5 failures within 60s -> 429 Too Many Requests)
    {
        let mut failures = state.login_failures.lock();
        if let Some((count, window_start)) = failures.get_mut(&client_key) {
            if window_start.elapsed() > Duration::from_secs(60) {
                *count = 0;
                *window_start = std::time::Instant::now();
            } else if *count >= 5 {
                let body = ApiErrorBody {
                    error: "Too Many Requests".to_string(),
                    detail: "Too many failed authentication attempts; retry in 60 seconds"
                        .to_string(),
                };
                return (StatusCode::TOO_MANY_REQUESTS, Json(body)).into_response();
            }
        }
    }

    let candidate = payload.token.trim().to_string();
    if candidate.is_empty() {
        let mut failures = state.login_failures.lock();
        let entry = failures
            .entry(client_key)
            .or_insert_with(|| (0, std::time::Instant::now()));
        if entry.1.elapsed() > Duration::from_secs(60) {
            *entry = (1, std::time::Instant::now());
        } else {
            entry.0 = entry.0.saturating_add(1);
        }
        let body = ApiErrorBody {
            error: "Unauthorized".to_string(),
            detail: "Authentication token is required".to_string(),
        };
        return (StatusCode::UNAUTHORIZED, Json(body)).into_response();
    }

    let env_token = std::env::var("AEGIS_WEB_TOKEN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let db_cfg = state.db.load_config_logged("aegis-web.api_auth_login");
    let db_token = ["web_api_token", "dashboard_api_token"]
        .into_iter()
        .map(|k| db_cfg.get_str(k, ""))
        .find(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_string());

    let mut valid_tokens = vec![state.session_token.clone()];
    if let Some(t) = env_token {
        valid_tokens.push(t);
    } else if let Some(t) = db_token {
        valid_tokens.push(t);
    }

    let is_valid = valid_tokens
        .iter()
        .any(|expected| constant_time_eq(&candidate, expected));

    if !is_valid {
        let mut failures = state.login_failures.lock();
        let entry = failures
            .entry(client_key)
            .or_insert_with(|| (0, std::time::Instant::now()));
        if entry.1.elapsed() > Duration::from_secs(60) {
            *entry = (1, std::time::Instant::now());
        } else {
            entry.0 = entry.0.saturating_add(1);
        }
        let body = ApiErrorBody {
            error: "Unauthorized".to_string(),
            detail: "Invalid API authentication token".to_string(),
        };
        return (StatusCode::UNAUTHORIZED, Json(body)).into_response();
    }

    state.login_failures.lock().remove(&client_key);

    // ME-13: Mint a per-session nonce with 24-hour TTL (86,400s)
    let session_nonce = generate_ephemeral_session_token();
    let expires_at_ms = state.db.now_ms() + 86_400_000;
    {
        let now_ms = state.db.now_ms();
        let mut sessions = state.active_sessions.lock();
        sessions.retain(|_, exp| *exp > now_ms);
        sessions.insert(session_nonce.clone(), expires_at_ms);
    }

    let cookie_secure = db_cfg.get_bool("web_cookie_secure", false)
        || db_cfg
            .get_str("environment", "sandbox")
            .trim()
            .eq_ignore_ascii_case("production");

    let mut cookie = format!(
        "aegis_session={session_nonce}; HttpOnly; SameSite=Strict; Path=/; Max-Age=86400"
    );
    if cookie_secure {
        cookie.push_str("; Secure");
    }

    let mut resp = (
        StatusCode::OK,
        Json(AuthLoginResponse {
            status: "authenticated",
            cookie_secure,
        }),
    )
        .into_response();
    if let Ok(hv) = HeaderValue::from_str(&cookie) {
        resp.headers_mut().insert(header::SET_COOKIE, hv);
    }
    resp
}

pub async fn api_auth_logout(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(cookie_tok) = extract_cookie_token(&headers, "aegis_session") {
        state.active_sessions.lock().remove(&cookie_tok);
    }
    let db_cfg = state.db.load_config_logged("aegis-web.api_auth_logout");
    let cookie_secure = db_cfg.get_bool("web_cookie_secure", false)
        || db_cfg
            .get_str("environment", "sandbox")
            .trim()
            .eq_ignore_ascii_case("production");
    let mut cookie = "aegis_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0".to_string();
    if cookie_secure {
        cookie.push_str("; Secure");
    }
    let mut resp = (
        StatusCode::OK,
        Json(json!({
            "status": "logged_out"
        })),
    )
        .into_response();
    if let Ok(hv) = HeaderValue::from_str(&cookie) {
        resp.headers_mut().insert(header::SET_COOKIE, hv);
    }
    resp
}

pub async fn api_health(State(state): State<AppState>) -> impl IntoResponse {
    let schema_version = state.db.get_schema_version().unwrap_or(-1);
    let cfg = state.db.load_config_logged("aegis-web.api_health");
    let exec_mode = cfg
        .get_str("execution_mode", "PAPER")
        .trim()
        .to_ascii_uppercase();
    let pond_mode = cfg
        .get_str("pond_execution_mode", &exec_mode)
        .trim()
        .to_ascii_uppercase();
    let is_live_or_testnet = matches!(exec_mode.as_str(), "LIVE" | "TESTNET")
        || matches!(pond_mode.as_str(), "LIVE" | "TESTNET");
    let max_bar_staleness_ms = cfg
        .get_i64("max_bar_staleness_ms", 21_600_000)
        .max(60_000);
    let now_ms = state.db.now_ms();
    let last_ms: i64 = state
        .db
        .with_conn(|conn| {
            let ts: Option<i64> = conn
                .query_row("SELECT MAX(timestamp_ms) FROM market_ohlcv;", [], |r| r.get(0))
                .unwrap_or(None);
            Ok(ts.unwrap_or(0))
        })
        .unwrap_or(0);
    let data_fresh = if is_live_or_testnet {
        last_ms > 0 && now_ms.saturating_sub(last_ms) <= max_bar_staleness_ms
    } else {
        last_ms > 0
    };
    let degraded = schema_version < 0 || (is_live_or_testnet && !data_fresh);
    let http_status = if degraded {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    (
        http_status,
        Json(HealthPayload {
            status: if degraded { "DEGRADED" } else { "ok" },
            service: "aegis-quant-pp50-rust",
            version: PLATFORM_VERSION,
            schema_version,
            runtime: "RUST_NATIVE",
            architecture: "PP-50 Dual-Book (Pit + Pond + Daily 00:05 UTC Settling)",
            data_fresh,
            last_bar_timestamp_ms: last_ms,
            max_bar_staleness_ms,
        }),
    )
}

pub async fn api_status(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| {
        let cfg = db.load_config_logged("aegis-web.api_status");
        let schema_version = db.get_schema_version()?;
        let settle_gate_active = db.is_settle_gate_active().unwrap_or(false);
        let pp50 = build_pp50_payload(&db)?;
        Ok(StatusPayload {
            status: "ok",
            service: "aegis-quant-pp50-rust",
            version: PLATFORM_VERSION,
            schema_version,
            execution_mode: cfg.get_str("execution_mode", "PAPER"),
            pond_execution_mode: cfg.get_str("pond_execution_mode", "PAPER"),
            environment: cfg.get_str("environment", "sandbox"),
            settle_gate_active,
            total_platform_equity_usdc: pp50.total_platform_equity_usdc,
            pit_equity_usdc: pp50.pit.equity_usdc,
            pond_equity_usdc: pp50.pond.equity_usdc,
        })
    })
    .await
    {
        Ok(st) => Json(st).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_dashboard(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| build_dashboard_payload(&db)).await {
        Ok(data) => Json(data).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_portfolio(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| {
        let portfolio = build_portfolio_payload(&db)?;
        let open_trades = db.get_open_trades()?;
        let credentials = db.get_credentials_status()?;
        Ok(PortfolioSummaryPayload {
            portfolio,
            open_trades,
            credentials,
        })
    })
    .await
    {
        Ok(res) => Json(res).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_pond(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| build_pond_payload(&db)).await {
        Ok(data) => Json(data).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_pond_universe(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| Ok(db.get_pond_universe(false)?)).await {
        Ok(universe) => Json(universe).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_pond_cells(
    State(state): State<AppState>,
    Query(q): Query<LimitQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    match run_db_blocking(state.db.clone(), move |db| {
        let raw_cells = db.get_pond_cells(limit)?;
        Ok(enrich_pond_cells(&db, raw_cells))
    })
    .await
    {
        Ok(cells) => Json(cells).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_pond_cycles(
    State(state): State<AppState>,
    Query(q): Query<LimitQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(30).clamp(1, 200);
    match run_db_blocking(state.db.clone(), move |db| {
        Ok(db.get_recent_pond_scan_cycles(limit)?)
    })
    .await
    {
        Ok(cycles) => Json(cycles).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_settle(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| build_pp50_payload(&db)).await {
        Ok(data) => Json(data).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_settle_run(
    State(state): State<AppState>,
    payload: Option<Json<SettleRequestPayload>>,
) -> Response {
    let req = payload.map(|p| p.0).unwrap_or_default();
    let dry_run = req.dry_run.unwrap_or(true);

    let has_manual_override = req.pit_equity_usdc.is_some()
        || req.pond_equity_usdc.is_some()
        || req.pit_idle_usdc.is_some()
        || req.pond_idle_usdc.is_some()
        || req.rich_idle_usdc.is_some();

    if !dry_run {
        if has_manual_override {
            return map_err_response(
                aegis_core::AegisError::Validation(
                    "manual equity/idle overrides are forbidden for non-dry-run settling (NH-3)"
                        .to_string(),
                )
                .into(),
            );
        }
        let cfg = state.db.load_config_logged("aegis-web.api_settle_run");
        let exec_mode = cfg.get_str("execution_mode", "PAPER");
        let pond_mode = cfg.get_str("pond_execution_mode", "PAPER");
        if (!exec_mode.eq_ignore_ascii_case("PAPER") || !pond_mode.eq_ignore_ascii_case("PAPER"))
            && req.confirm_live_settle.as_deref() != Some("CONFIRM_LIVE_SETTLE")
        {
            return map_err_response(
                aegis_core::AegisError::Validation(
                    "non-dry-run settling in LIVE or TESTNET mode requires confirm_live_settle == 'CONFIRM_LIVE_SETTLE' (NH-3)"
                        .to_string(),
                )
                .into(),
            );
        }
    }

    let _guard = state.cycle_lock.lock().await;

    match run_db_blocking(state.db.clone(), move |db| {
        let rich_idle = req.rich_idle_usdc.or_else(|| {
            match (req.pit_equity_usdc, req.pond_equity_usdc) {
                (Some(pit), Some(pond)) if pit >= pond => req.pit_idle_usdc,
                (Some(_), Some(_)) => req.pond_idle_usdc,
                _ => req.pit_idle_usdc.or(req.pond_idle_usdc),
            }
        });
        let settle_ctrl = SettleController::new(db.clone(), None);
        let report = settle_ctrl.run_settling(SettleOverrideInput {
            pit_equity_usdc: req.pit_equity_usdc,
            pond_equity_usdc: req.pond_equity_usdc,
            rich_idle_usdc: rich_idle,
            day_index: req.day_index,
            dry_run,
        })?;
        let dashboard = build_dashboard_payload(&db)?;
        Ok(SettleTriggerResponse {
            status: "ok",
            settle: report,
            dashboard,
        })
    })
    .await
    {
        Ok(res) => Json(res).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_signals(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| {
        let cfg = db.load_config_logged("aegis-web.api_signals");
        let entry_thresh = cfg.get_f64("ensemble_entry_threshold", 0.65);
        Ok(enrich_signals(db.get_latest_signals(30)?, entry_thresh))
    })
    .await
    {
        Ok(signals) => Json(signals).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_trades(State(state): State<AppState>, Query(q): Query<LimitQuery>) -> Response {
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    match run_db_blocking(state.db.clone(), move |db| Ok(db.get_recent_trades(limit)?)).await {
        Ok(trades) => Json(trades).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_config(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| Ok(db.get_config_rows(true)?)).await {
        Ok(cfg) => Json(cfg).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_logs(State(state): State<AppState>, Query(q): Query<LimitQuery>) -> Response {
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    match run_db_blocking(state.db.clone(), move |db| Ok(db.get_recent_logs(limit)?)).await {
        Ok(logs) => Json(logs).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_credentials(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| Ok(db.get_credentials_status()?)).await {
        Ok(cred) => Json(cred).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_alerts(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| Ok(db.get_credentials_status()?)).await {
        Ok(cred) => Json(cred).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_update_config(
    State(state): State<AppState>,
    Json(payload): Json<ConfigUpdatePayload>,
) -> Response {
    let val_str = match &payload.config_value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    };

    if let Err(e) = validate_config_update(&payload.config_key, &val_str) {
        return map_err_response(e.into());
    }

    let key = payload.config_key.clone();
    let val_json = payload.config_value.clone();
    let val_clone = val_str.clone();
    let scale_caps = payload.scale_risk_caps;

    match run_db_blocking(state.db.clone(), move |db| {
        if key == "execution_mode" || key == "pond_execution_mode" {
            db.validate_execution_mode_transition(&val_clone, None, None)?;
        }
        if key == "initial_equity_usdc" && scale_caps {
            let new_eq = val_clone.parse::<f64>().map_err(|_| {
                aegis_core::AegisError::Validation("Invalid initial_equity_usdc".to_string())
            })?;
            db.scale_risk_caps_for_equity(new_eq)?;
        } else {
            let updated = db.set_config_value(&key, &val_json)?;
            if !updated {
                return Err(aegis_core::AegisError::Validation(format!(
                    "Unknown configuration key '{key}'"
                ))
                .into());
            }
        }
        let display_val = if aegis_core::is_sensitive_config_key(&key) {
            "[REDACTED]".to_string()
        } else {
            val_clone.clone()
        };
        let _ = db.log_telemetry(
            "INFO",
            "WEB_API",
            &format!("Config updated via REST API: {} = {}", key, display_val),
            None,
        );
        Ok(SingleConfigUpdateResponse {
            status: "ok",
            key,
            value: display_val,
        })
    })
    .await
    {
        Ok(res) => {
            state.invalidate_auth_cache();
            Json(res).into_response()
        }
        Err(e) => map_err_response(e),
    }
}

pub async fn api_update_config_batch(
    State(state): State<AppState>,
    Json(payload): Json<ConfigBatchUpdatePayload>,
) -> Response {
    for (k, v) in &payload.updates {
        let s = match v {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            other => other.to_string(),
        };
        if let Err(e) = validate_config_update(k, &s) {
            return map_err_response(e.into());
        }
    }

    let updates = payload.updates;
    let scale_caps = payload.scale_risk_caps;
    match run_db_blocking(state.db.clone(), move |db| {
        let pending_wallet = updates.get("wallet_address").and_then(|v| v.as_str());
        let pending_pk = updates.get("private_key").and_then(|v| v.as_str());
        for mode_key in ["execution_mode", "pond_execution_mode"] {
            if let Some(m_val) = updates.get(mode_key) {
                let m_str = m_val
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| m_val.to_string());
                db.validate_execution_mode_transition(&m_str, pending_wallet, pending_pk)?;
            }
        }
        let updated_keys = db.set_config_batch(&updates, scale_caps)?;
        Ok(BatchConfigUpdateResponse {
            status: "ok",
            updated_count: updated_keys.len(),
        })
    })
    .await
    {
        Ok(res) => {
            state.invalidate_auth_cache();
            Json(res).into_response()
        }
        Err(e) => map_err_response(e),
    }
}

pub async fn api_update_credentials(
    State(state): State<AppState>,
    Json(payload): Json<CredentialsUpdatePayload>,
) -> Response {
    match run_db_blocking(state.db.clone(), move |db| {
        let wallet = payload
            .wallet_address
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let pk = payload
            .private_key
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let Some(v) = payload
            .hyperliquid_vault_address
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let cur_vault = db
                .get_raw_config_value("hyperliquid_vault_address")
                .ok()
                .flatten()
                .unwrap_or_default();
            if !v.eq_ignore_ascii_case(cur_vault.trim()) {
                return Err(aegis_core::AegisError::SecurityViolation(
                    "Security policy key 'hyperliquid_vault_address' is immutable and cannot be modified via the Web API"
                        .to_string(),
                )
                .into());
            }
        }
        let gemini = payload
            .gemini_api_key
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let exec_mode = payload
            .execution_mode
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());

        let _ = db.configure_hyperliquid_credentials(
            wallet,
            pk,
            payload.hyperliquid_testnet,
            exec_mode,
            None,
            gemini,
            true,
        )?;

        if let Some(pond_mode) = payload
            .pond_execution_mode
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            db.validate_execution_mode_transition(pond_mode, wallet, pk)?;
            db.set_config_value("pond_execution_mode", &json!(pond_mode))?;
        }

        let status = db.get_credentials_status()?;
        Ok(CredentialsUpdateResponse {
            status: "ok",
            credentials: status,
        })
    })
    .await
    {
        Ok(res) => {
            state.invalidate_auth_cache();
            Json(res).into_response()
        }
        Err(e) => map_err_response(e),
    }
}

pub async fn api_update_alerts(
    State(state): State<AppState>,
    Json(payload): Json<AlertsUpdatePayload>,
) -> Response {
    match run_db_blocking(state.db.clone(), move |db| {
        let tok = payload
            .telegram_bot_token
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let cid = payload.telegram_chat_id.as_deref().map(str::trim);
        let status = db.configure_telegram_alerts(tok, cid, payload.max_daily_drawdown_alert_pct)?;
        Ok(AlertsUpdateResponse {
            status: "ok",
            alerts: status,
        })
    })
    .await
    {
        Ok(res) => {
            state.invalidate_auth_cache();
            Json(res).into_response()
        }
        Err(e) => map_err_response(e),
    }
}

pub async fn api_test_alert(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| {
        let mut alerts = AlertEngine::new(db);
        Ok(alerts.run_test_alert(false)?)
    })
    .await
    {
        Ok(res) => Json(res).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_update_wallet_sync(
    State(state): State<AppState>,
    payload: Option<Json<WalletSyncPayload>>,
) -> Response {
    let wallet_override = payload
        .and_then(|p| p.0.wallet_address)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    match run_db_blocking(state.db.clone(), move |db| {
        if let Some(ref w) = wallet_override {
            db.set_config_value("wallet_address", &json!(w))?;
        }
        let exec = ExecutionEngine::new(db.clone());
        let sync_res = exec.sync_wallet_equity(true);
        let cred = db.get_credentials_status()?;
        Ok(WalletSyncResponse {
            status: "ok",
            sync: serde_json::to_value(&sync_res).unwrap_or_else(|_| json!({})),
            credentials: cred,
        })
    })
    .await
    {
        Ok(res) => Json(res).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_trigger_cycle(State(state): State<AppState>) -> Response {
    let _guard = state.cycle_lock.lock().await;
    let Some(_db_lock) = state
        .db
        .acquire_cycle_lock_guard("web-api.cycle", 120_000)
        .ok()
        .flatten()
    else {
        let body = ApiErrorBody {
            error: "Conflict".to_string(),
            detail: "Another execution cycle is currently holding the advisory lock".to_string(),
        };
        return (StatusCode::CONFLICT, Json(body)).into_response();
    };
    match run_db_blocking(state.db.clone(), |db| {
        let pit = PitOrchestrator::new(db.clone(), None);
        let pit_summary = pit.run_cycle(true, false)?;
        let pond = PondOrchestrator::new(db.clone(), None);
        let _ = pond.run_scan_cycle(false);
        let dashboard = build_dashboard_payload(&db)?;
        Ok(CycleTriggerResponse {
            status: "ok",
            cycle: serde_json::to_value(&pit_summary).unwrap_or_else(|_| json!({})),
            dashboard,
        })
    })
    .await
    {
        Ok(res) => Json(res).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_trigger_pond_scan(State(state): State<AppState>) -> Response {
    let _guard = state.cycle_lock.lock().await;
    let Some(_db_lock) = state
        .db
        .acquire_cycle_lock_guard("web-api.pond_scan", 120_000)
        .ok()
        .flatten()
    else {
        let body = ApiErrorBody {
            error: "Conflict".to_string(),
            detail: "Another execution cycle is currently holding the advisory lock".to_string(),
        };
        return (StatusCode::CONFLICT, Json(body)).into_response();
    };
    match run_db_blocking(state.db.clone(), |db| {
        let pond = PondOrchestrator::new(db.clone(), None);
        let scan_outcome = pond.run_scan_cycle(true)?;
        let dashboard = build_dashboard_payload(&db)?;
        Ok(PondScanTriggerResponse {
            status: "ok",
            scan: scan_outcome,
            dashboard,
        })
    })
    .await
    {
        Ok(res) => Json(res).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_close_trade(
    State(state): State<AppState>,
    Json(payload): Json<CloseTradePayload>,
) -> Response {
    let _guard = state.cycle_lock.lock().await;
    let Some(_db_lock) = state
        .db
        .acquire_cycle_lock_guard("web-api.close_trade", 60_000)
        .ok()
        .flatten()
    else {
        let body = ApiErrorBody {
            error: "Conflict".to_string(),
            detail: "Another execution cycle is currently holding the advisory lock".to_string(),
        };
        return (StatusCode::CONFLICT, Json(body)).into_response();
    };
    match run_db_blocking(state.db.clone(), move |db| {
        if payload.trade_id.is_some() && payload.cell_id.is_some() {
            return Err(aegis_core::AegisError::Validation(
                "Specify either trade_id or cell_id, not both".to_string(),
            )
            .into());
        }
        let exec = ExecutionEngine::new(db.clone());
        let reason = payload.reason.as_deref();
        let close_result = if let Some(trade_id) = payload.trade_id {
            exec.close_open_trade_manual(trade_id, reason)?
        } else if let Some(cell_id) = payload.cell_id {
            exec.close_open_pond_cell_manual(cell_id, reason)?
        } else {
            return Err(aegis_core::AegisError::Validation(
                "Either trade_id or cell_id must be provided".to_string(),
            )
            .into());
        };
        let dashboard = build_dashboard_payload(&db)?;
        Ok(CloseTradeResponse {
            status: "ok",
            close_result,
            dashboard,
        })
    })
    .await
    {
        Ok(res) => Json(res).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_trigger_db_backup(
    State(state): State<AppState>,
    payload: Option<Json<DbBackupRequestPayload>>,
) -> Response {
    let retention_days = payload.and_then(|p| p.0.retention_days);
    match run_db_blocking(state.db.clone(), move |db| {
        let backup = db.create_sqlite_backup(None, retention_days)?;
        let dashboard = build_dashboard_payload(&db)?;
        Ok(DbBackupTriggerResponse {
            status: "ok",
            backup,
            dashboard,
        })
    })
    .await
    {
        Ok(res) => Json(res).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_trigger_backtest(
    State(state): State<AppState>,
    payload: Option<Json<BacktestRequestPayload>>,
) -> Response {
    let days = payload
        .as_ref()
        .and_then(|p| p.days)
        .unwrap_or(365)
        .clamp(30, 1095);
    let use_real = payload
        .as_ref()
        .and_then(|p| p.use_real_history)
        .unwrap_or(true);

    match run_db_blocking(state.db.clone(), move |db| {
        let bt = BacktestEngine::new(db, days);
        let report = bt.run_backtest(None, None, use_real)?;
        let formatted = BacktestEngine::format_tear_sheet(&report);
        Ok(BacktestRunResponse {
            status: "ok",
            report,
            formatted_report: formatted,
        })
    })
    .await
    {
        Ok(res) => Json(res).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_trigger_calibrate(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| {
        let calibrator = WalkForwardCalibrator::new(db);
        let report = calibrator.calibrate_universe(true)?;
        Ok(CalibrateRunResponse {
            status: "ok",
            calibration_report: serde_json::to_value(report)?,
        })
    })
    .await
    {
        Ok(res) => Json(res).into_response(),
        Err(e) => map_err_response(e),
    }
}

pub async fn api_stream(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let interval = tokio::time::interval(Duration::from_secs(3));
    let db = state.db.clone();
    let sse_cache = state.sse_payload_cache.clone();
    let stream = IntervalStream::new(interval).then(move |_| {
        let db_inner = db.clone();
        let cache_inner = sse_cache.clone();
        async move {
            // ME-17: Share cached SSE snapshot across concurrent subscribers within 2s window
            if let Some((cached_at, ref payload_str)) = *cache_inner.lock() {
                if cached_at.elapsed() < Duration::from_secs(2) {
                    return Ok::<Event, Infallible>(Event::default().data(payload_str.clone()));
                }
            }
            let data = match run_db_blocking(db_inner, |pool| build_dashboard_payload(&pool)).await
            {
                Ok(d) => {
                    let s = serde_json::to_string(&d).unwrap_or_else(|_| "{}".to_string());
                    *cache_inner.lock() = Some((std::time::Instant::now(), s.clone()));
                    s
                }
                Err(e) => {
                    tracing::error!(error = %e, "SSE stream payload error");
                    serde_json::to_string(&ApiErrorBody {
                        error: "Internal server error".to_string(),
                        detail: "Stream snapshot error".to_string(),
                    })
                    .unwrap_or_else(|_| "{}".to_string())
                }
            };
            Ok::<Event, Infallible>(Event::default().data(data))
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}
