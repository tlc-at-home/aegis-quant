//! HTTP and SSE route handlers for `aegis-web`.

use crate::{
    constant_time_eq,
    payloads::{
        build_dashboard_payload, build_pond_payload, build_portfolio_payload, build_pp50_payload,
        enrich_pond_cells, enrich_signals, map_err_response, run_db_blocking, AlertsUpdatePayload,
        AlertsUpdateResponse, ApiErrorBody, AuthLoginPayload, AuthLoginResponse,
        BacktestRequestPayload, BacktestRunResponse, BatchConfigUpdateResponse,
        CalibrateRunResponse, ConfigBatchUpdatePayload, ConfigUpdatePayload,
        CredentialsUpdatePayload, CredentialsUpdateResponse, CycleTriggerResponse, HealthPayload,
        LimitQuery, PondScanTriggerResponse, PortfolioSummaryPayload, SettleRequestPayload,
        SettleTriggerResponse, SingleConfigUpdateResponse, StatusPayload, WalletSyncPayload,
        WalletSyncResponse,
    },
    AppState,
};
use aegis_alerts::AlertEngine;
use aegis_backtest::BacktestEngine;
use aegis_core::{is_immutable_security_config_key, PLATFORM_VERSION};
use aegis_execution::ExecutionEngine;
use aegis_pit::PitOrchestrator;
use aegis_pond::PondOrchestrator;
use aegis_settle::{SettleController, SettleOverrideInput};
use aegis_signals::WalkForwardCalibrator;
use anyhow::anyhow;
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

pub fn validate_config_update(key: &str, val_str: &str) -> Result<(), String> {
    let trimmed_key = key.trim();
    if trimmed_key.is_empty() {
        return Err("Validation error: Configuration key must not be empty".to_string());
    }
    if is_immutable_security_config_key(trimmed_key) {
        return Err(format!(
            "Forbidden: Security policy key '{trimmed_key}' is immutable and cannot be modified via the Web API"
        ));
    }
    if val_str.trim().is_empty()
        && !matches!(
            trimmed_key,
            "telegram_bot_token"
                | "telegram_chat_id"
                | "discord_webhook_url"
                | "gemini_api_key"
                | "hyperliquid_agent_address"
        )
    {
        return Err(format!(
            "Validation error: Configuration value for '{trimmed_key}' must not be empty"
        ));
    }
    Ok(())
}

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
    Json(payload): Json<AuthLoginPayload>,
) -> Response {
    let candidate = payload.token.trim().to_string();
    if candidate.is_empty() {
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
        let body = ApiErrorBody {
            error: "Unauthorized".to_string(),
            detail: "Invalid API authentication token".to_string(),
        };
        return (StatusCode::UNAUTHORIZED, Json(body)).into_response();
    }

    let cookie_secure = db_cfg.get_bool("web_cookie_secure", false)
        || db_cfg
            .get_str("environment", "sandbox")
            .trim()
            .eq_ignore_ascii_case("production");

    let mut cookie = format!(
        "aegis_session={}; HttpOnly; SameSite=Strict; Path=/",
        state.session_token
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

pub async fn api_auth_logout(State(state): State<AppState>) -> Response {
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
    let schema_version = state.db.get_schema_version().unwrap_or(0);
    Json(HealthPayload {
        status: "ok",
        service: "aegis-quant-pp50-rust",
        version: PLATFORM_VERSION,
        schema_version,
        runtime: "RUST_NATIVE",
        architecture: "PP-50 Dual-Book (Pit + Pond + Daily 00:05 UTC Settling)",
    })
}

pub async fn api_status(State(state): State<AppState>) -> Response {
    match run_db_blocking(state.db.clone(), |db| {
        let cfg = db.load_config_logged("aegis-web.api_status");
        let schema_version = db.get_schema_version().unwrap_or(0);
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
            return map_err_response(anyhow!(
                "Validation error: manual equity/idle overrides are forbidden for non-dry-run settling (NH-3)"
            ));
        }
        let cfg = state.db.load_config_logged("aegis-web.api_settle_run");
        let exec_mode = cfg.get_str("execution_mode", "PAPER");
        let pond_mode = cfg.get_str("pond_execution_mode", "PAPER");
        if (!exec_mode.eq_ignore_ascii_case("PAPER") || !pond_mode.eq_ignore_ascii_case("PAPER"))
            && req.confirm_live_settle.as_deref() != Some("CONFIRM_LIVE_SETTLE")
        {
            return map_err_response(anyhow!(
                "Validation error: non-dry-run settling in LIVE or TESTNET mode requires confirm_live_settle == 'CONFIRM_LIVE_SETTLE' (NH-3)"
            ));
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
        return map_err_response(anyhow!(e));
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
            let new_eq = val_clone
                .parse::<f64>()
                .map_err(|_| anyhow!("Validation error: Invalid initial_equity_usdc"))?;
            db.scale_risk_caps_for_equity(new_eq)?;
        } else {
            let updated = db.set_config_value(&key, &val_json)?;
            if !updated {
                return Err(anyhow!(
                    "Validation error: Unknown configuration key '{key}'"
                ));
            }
        }
        let _ = db.log_telemetry(
            "INFO",
            "WEB_API",
            &format!("Config updated via REST API: {} = {}", key, val_clone),
            None,
        );
        Ok(SingleConfigUpdateResponse {
            status: "ok",
            key,
            value: val_clone,
        })
    })
    .await
    {
        Ok(res) => Json(res).into_response(),
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
            return map_err_response(anyhow!(e));
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
        Ok(res) => Json(res).into_response(),
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
                return Err(anyhow!(
                    "Forbidden: Security policy key 'hyperliquid_vault_address' is immutable and cannot be modified via the Web API"
                ));
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
        Ok(res) => Json(res).into_response(),
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
        Ok(res) => Json(res).into_response(),
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
            sync: sync_res,
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
    let stream = IntervalStream::new(interval).then(move |_| {
        let db_inner = db.clone();
        async move {
            let data = match run_db_blocking(db_inner, |pool| build_dashboard_payload(&pool)).await
            {
                Ok(d) => serde_json::to_string(&d).unwrap_or_else(|_| "{}".to_string()),
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
