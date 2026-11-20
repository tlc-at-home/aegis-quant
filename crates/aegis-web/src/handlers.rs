//! REST and static asset route handlers for Aegis Quant v2.0 (`aegis-web::handlers`).

use crate::auth::{
    auth_error_response, constant_time_eq, extract_cookie_token, too_many_requests_response,
    AuthLoginPayload, AuthLoginResponse,
};
use crate::payloads::{
    build_dashboard_payload, build_health_payload, execute_close_position_or_order,
    execute_risk_tier_switch, json_value_to_config_string, map_aegis_error_response,
    validate_config_key_value, ClosePositionRequest, ConfigUpdateRequest, ConfigUpdateResponse,
    ReconcileRequest, RiskTierUpdateRequest,
};
use crate::server::{AppState, ClientIpKey, SESSION_TTL_SECS};
use aegis_alerts::send_test_alert_async;
use aegis_core::{now_ms, AegisError, ExecutionMode};
use axum::{
    extract::{Extension, State},
    http::{header, HeaderMap, HeaderValue, StatusCode, Uri},
    response::{Html, IntoResponse, Response},
    Json,
};
use serde_json::json;

pub const EMBEDDED_INDEX_HTML: &str = include_str!("../../../web/static/index.html");
pub const EMBEDDED_STYLES_CSS: &str = include_str!("../../../web/static/styles.css");
pub const EMBEDDED_APP_JS: &str = include_str!("../../../web/static/app.js");

/// `GET /` and `GET /index.html` — Serves `web/static/index.html`.
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

/// `GET /styles.css` and `GET /static/styles.css` — Serves `web/static/styles.css`.
pub async fn serve_styles_css(State(state): State<AppState>) -> Response {
    let css_path = state.static_dir.join("styles.css");
    let css_content = match tokio::fs::read_to_string(&css_path).await {
        Ok(content) => content,
        Err(_) => EMBEDDED_STYLES_CSS.to_string(),
    };
    let mut resp = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        css_content,
    )
        .into_response();
    if let Ok(hv) = HeaderValue::from_str("no-store") {
        resp.headers_mut().insert(header::CACHE_CONTROL, hv);
    }
    resp
}

/// `GET /app.js` and `GET /static/app.js` — Serves `web/static/app.js`.
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

/// Converts a `build_health_payload` result into `200 OK` or `503 Service Unavailable`.
pub fn health_result_to_response(
    res: aegis_core::Result<crate::payloads::HealthPayload>,
) -> Response {
    match res {
        Ok(payload) => (StatusCode::OK, Json(payload)).into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "unavailable",
                "error": "Service Unavailable",
                "detail": e.to_string(),
            })),
        )
            .into_response(),
    }
}

/// `GET /api/v1/health` — Returns MariaDB 13.0 version, 7 SQL Views status, risk tier, execution mode, and loop cadence.
/// Returns HTTP `503 Service Unavailable` whenever MariaDB ping/query fails.
pub async fn get_health_handler(State(state): State<AppState>) -> Response {
    let db = state.db.clone();
    let res = tokio::task::spawn_blocking(move || build_health_payload(&db)).await;
    match res {
        Ok(inner) => health_result_to_response(inner),
        Err(e) => {
            health_result_to_response(Err(AegisError::Database(format!("Health task error: {e}"))))
        }
    }
}

/// `GET /api/v1/dashboard` and `GET /api/v1/state` — Returns unified MariaDB 13.0 + 7 SQL Views dashboard payload.
pub async fn get_dashboard_handler(State(state): State<AppState>) -> Response {
    let db = state.db.clone();
    let res = tokio::task::spawn_blocking(move || build_dashboard_payload(&db)).await;
    match res {
        Ok(Ok(payload)) => (StatusCode::OK, Json(payload)).into_response(),
        Ok(Err(e)) => map_aegis_error_response(e),
        Err(e) => {
            map_aegis_error_response(AegisError::Database(format!("Dashboard task error: {e}")))
        }
    }
}

/// `POST /api/v1/risk-tier` — Dynamically switches `risk_tier` (`conservative` 0.65x, `standard` 1.00x, `aggressive` 1.35x, `extreme` 1.85x, `custom`) and `target_leverage_multiplier` in `system_config`.
pub async fn post_risk_tier_handler(
    State(state): State<AppState>,
    Json(req): Json<RiskTierUpdateRequest>,
) -> Response {
    let db = state.db.clone();
    let res = tokio::task::spawn_blocking(move || execute_risk_tier_switch(&db, &req)).await;
    match res {
        Ok(Ok(payload)) => {
            state.notify_dashboard_refresh();
            (StatusCode::OK, Json(payload)).into_response()
        }
        Ok(Err(e)) => map_aegis_error_response(e),
        Err(e) => {
            map_aegis_error_response(AegisError::Database(format!("Risk tier task error: {e}")))
        }
    }
}

fn normalize_pending_config_write(
    key: &str,
    raw_val: String,
) -> aegis_core::Result<(String, String)> {
    validate_config_key_value(key, &raw_val)?;
    let clean_k = key.trim().to_ascii_lowercase();
    let norm_val = if clean_k == "execution_mode" {
        raw_val.trim().to_ascii_uppercase()
    } else {
        raw_val
    };
    Ok((clean_k, norm_val))
}

/// `POST /api/v1/config` — Updates validated runtime config keys or Fernet-encrypted credentials in `system_config`.
pub async fn post_config_handler(
    State(state): State<AppState>,
    Json(req): Json<ConfigUpdateRequest>,
) -> Response {
    let db = state.db.clone();
    let res = tokio::task::spawn_blocking(move || -> aegis_core::Result<ConfigUpdateResponse> {
        let mut pending_writes: Vec<(String, String, Option<String>)> = Vec::new();

        if let Some(ref k) = req.config_key {
            let val_str = req
                .config_value
                .as_ref()
                .map(json_value_to_config_string)
                .unwrap_or_default();
            let (clean_k, norm_val) = normalize_pending_config_write(k, val_str)?;
            pending_writes.push((clean_k, norm_val, req.description.clone()));
        }

        if let Some(ref map) = req.updates {
            for (k, v) in map {
                let val_str = json_value_to_config_string(v);
                let (clean_k, norm_val) = normalize_pending_config_write(k, val_str)?;
                pending_writes.push((clean_k, norm_val, None));
            }
        }

        if pending_writes.is_empty() {
            return Err(AegisError::Validation(
                "No config_key or updates map provided".to_string(),
            ));
        }

        let _exec_guard = aegis_execution::execution_lock().lock();
        let requested_modes: Vec<&str> = pending_writes
            .iter()
            .filter(|(k, _, _)| k == "execution_mode")
            .map(|(_, v, _)| v.as_str())
            .collect();
        if requested_modes.windows(2).any(|w| w[0] != w[1]) {
            return Err(AegisError::Validation(
                "Conflicting execution_mode values in single config update".to_string(),
            ));
        }

        let mut hyperliquid_testnet_sync: Option<&'static str> = None;
        if let Some(&target_mode_str) = requested_modes.first() {
            let current_cfg = db.load_config()?;
            let current_mode = current_cfg.execution_mode();
            let target_mode = ExecutionMode::parse(target_mode_str);
            if target_mode != current_mode {
                if !db.get_assets_held()?.is_empty() || !db.get_assets_ordered()?.is_empty() {
                    return Err(AegisError::Validation(format!(
                        "Cannot switch execution_mode from {} to {} while OPEN positions or WORKING orders exist",
                        current_mode.as_str(),
                        target_mode.as_str(),
                    )));
                }
                if target_mode == ExecutionMode::Live
                    && !current_cfg.get_bool("allow_web_live_mode", false)
                {
                    return Err(AegisError::SecurityViolation(
                        "Switching execution_mode to LIVE via Web API requires DB-immutable allow_web_live_mode=true".to_string(),
                    ));
                }
                hyperliquid_testnet_sync = Some(if target_mode == ExecutionMode::Live {
                    "false"
                } else {
                    "true"
                });
            }
        }

        let mut updated_keys = Vec::with_capacity(pending_writes.len());
        let mut risk_tier_updated: Option<String> = None;
        let mut explicit_mult_updated = false;
        for (k, val_str, desc) in &pending_writes {
            if k == "risk_tier" {
                risk_tier_updated = Some(val_str.clone());
            } else if k == "target_leverage_multiplier" {
                explicit_mult_updated = true;
            }
            db.set_config_value(k, val_str, desc.as_deref())?;
            updated_keys.push(k.clone());
        }

        if let Some(testnet_val) = hyperliquid_testnet_sync {
            db.set_config_value(
                "hyperliquid_testnet",
                testnet_val,
                Some("Synchronized with execution_mode transition"),
            )?;
            updated_keys.push("hyperliquid_testnet".to_string());
        }

        // Keep target_leverage_multiplier synchronized if risk_tier was changed directly
        if let Some(raw_tier) = risk_tier_updated {
            if !explicit_mult_updated && !raw_tier.eq_ignore_ascii_case("custom") {
                let tier = aegis_core::RiskTier::parse(&raw_tier);
                let mult = tier.leverage_multiplier(None);
                db.set_config_value(
                    "target_leverage_multiplier",
                    &format!("{mult:.2}"),
                    Some("Synchronized with risk_tier update"),
                )?;
                updated_keys.push("target_leverage_multiplier".to_string());
            }
        }

        updated_keys.sort();
        updated_keys.dedup();
        if updated_keys.iter().any(|k| {
            matches!(
                k.as_str(),
                "live_exchange_equity_usdc"
                    | "initial_paper_equity_usdc"
                    | "risk_capital_cap_usdc"
                    | "risk_tier"
                    | "max_open_positions"
                    | "target_leverage_multiplier"
                    | "max_gross_leverage_x"
                    | "max_single_asset_weight"
            )
        }) {
            let cfg = db.load_config_or_default();
            let engine = aegis_execution::ExecutionEngine::new(db.clone());
            let base_eq = engine.resolve_effective_base_equity_usdc(&cfg);
            let _ = engine.enforce_leverage_and_position_caps();
            let _ = engine.record_portfolio_snapshot(Some(base_eq), None);
        }
        let _ = db.log_telemetry(
            "INFO",
            "aegis-web.config",
            &format!("Updated system_config keys: {:?}", updated_keys),
            None,
        );

        Ok(ConfigUpdateResponse {
            status: "ok".to_string(),
            updated_keys,
            updated_at_ms: now_ms(),
        })
    })
    .await;

    match res {
        Ok(Ok(payload)) => {
            state.notify_dashboard_refresh();
            (StatusCode::OK, Json(payload)).into_response()
        }
        Ok(Err(e)) => map_aegis_error_response(e),
        Err(e) => map_aegis_error_response(AegisError::Database(format!("Config task error: {e}"))),
    }
}

/// `POST /api/v1/alerts/test` — Triggers a live formatted HTML Telegram test alert via `aegis-alerts`.
pub async fn post_alerts_test_handler(State(state): State<AppState>) -> Response {
    match send_test_alert_async(&state.db).await {
        Ok(outcome) => {
            let http_status = outcome
                .channels
                .first()
                .and_then(|c| c.get("http_status"))
                .and_then(|v| v.as_u64())
                .unwrap_or(200);
            let body = json!({
                "status": outcome.status,
                "delivered": outcome.is_delivered() || outcome.telegram_sent || outcome.discord_sent,
                "provider": outcome.provider,
                "telegram_sent": outcome.telegram_sent,
                "discord_sent": outcome.discord_sent,
                "http_status": http_status,
                "chat_id": outcome.chat_id,
                "alert_queue_id": outcome.alert_queue_id,
                "detail": outcome.error_detail.clone().or(outcome.reason.clone()).unwrap_or_else(|| "HTML alert dispatched".to_string()),
                "html_message": outcome.html_message,
                "channels": outcome.channels,
            });
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(e) => map_aegis_error_response(e),
    }
}

/// `POST /api/v1/positions/close` — Closes an open position or cancels a working order and optionally sets a manual cooldown.
pub async fn post_positions_close_handler(
    State(state): State<AppState>,
    Json(req): Json<ClosePositionRequest>,
) -> Response {
    let db = state.db.clone();
    let res = tokio::task::spawn_blocking(move || execute_close_position_or_order(&db, &req)).await;
    match res {
        Ok(Ok(payload)) => {
            state.notify_dashboard_refresh();
            (StatusCode::OK, Json(payload)).into_response()
        }
        Ok(Err(e)) => map_aegis_error_response(e),
        Err(e) => map_aegis_error_response(AegisError::Database(format!(
            "Close position task error: {e}"
        ))),
    }
}

/// `POST /api/v1/reconcile` — Runs an immediate 1:1 MariaDB-to-Hyperliquid clearinghouseState & openOrders reconciliation.
pub async fn post_reconcile_handler(
    State(state): State<AppState>,
    req_opt: Option<Json<ReconcileRequest>>,
) -> Response {
    let close_legacy = req_opt
        .map(|Json(r)| r.close_legacy_positions)
        .unwrap_or(false);
    let db = state.db.clone();
    let res = tokio::task::spawn_blocking(move || {
        let engine = aegis_execution::ExecutionEngine::new(db);
        engine.run_cutover_reconciliation(close_legacy)
    })
    .await;

    match res {
        Ok(Ok(report)) => {
            state.notify_dashboard_refresh();
            (StatusCode::OK, Json(report)).into_response()
        }
        Ok(Err(e)) => map_aegis_error_response(e),
        Err(e) => {
            map_aegis_error_response(AegisError::Database(format!("Reconcile task error: {e}")))
        }
    }
}

fn should_set_secure_cookie(state: &AppState, headers: &HeaderMap, uri: &Uri) -> bool {
    if state.cookie_secure {
        return true;
    }
    if uri
        .scheme_str()
        .is_some_and(|s| s.eq_ignore_ascii_case("https"))
    {
        return true;
    }
    if let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) {
        if origin
            .trim()
            .get(..8)
            .is_some_and(|p| p.eq_ignore_ascii_case("https://"))
        {
            return true;
        }
    }
    if state.trust_proxy_headers {
        if let Some(xfp) = headers
            .get("x-forwarded-proto")
            .and_then(|v| v.to_str().ok())
        {
            if xfp
                .split(',')
                .next()
                .is_some_and(|p| p.trim().eq_ignore_ascii_case("https"))
            {
                return true;
            }
        }
        if let Some(fwd) = headers.get(header::FORWARDED).and_then(|v| v.to_str().ok()) {
            for part in fwd.split(';') {
                for kv in part.split(',') {
                    if let Some((k, v)) = kv.trim().split_once('=') {
                        if k.trim().eq_ignore_ascii_case("proto")
                            && v.trim().trim_matches('"').eq_ignore_ascii_case("https")
                        {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

/// `POST /api/v1/auth/login` — Exchanges bearer token for a rotating, expiring HttpOnly session cookie.
pub async fn post_auth_login_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    client_ip_ext: Option<Extension<ClientIpKey>>,
    Json(req): Json<AuthLoginPayload>,
) -> Response {
    let Some(expected) = state
        .configured_api_token
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return auth_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Authentication token is not configured or undecryptable",
        );
    };

    let client_ip = client_ip_ext
        .map(|Extension(k)| k)
        .unwrap_or(ClientIpKey::Local);
    if state.require_bearer_auth {
        if let Err(retry_after) = state.auth_rate_limiter.check_allowed(client_ip) {
            return too_many_requests_response(retry_after);
        }
    }

    if !constant_time_eq(req.token.trim(), expected) {
        if state.require_bearer_auth {
            state.auth_rate_limiter.record_failure(client_ip);
        }
        return auth_error_response(StatusCode::UNAUTHORIZED, "Invalid API token");
    }

    if state.require_bearer_auth {
        state.auth_rate_limiter.reset(client_ip);
    }

    let prior_cookie = extract_cookie_token(&headers);
    let session_tok = state.mint_session(prior_cookie.as_deref());
    let secure_attr = if should_set_secure_cookie(&state, &headers, &uri) {
        "; Secure"
    } else {
        ""
    };
    let cookie = format!(
        "aegis_session={session_tok}; Path=/; HttpOnly; SameSite=Strict; Max-Age={SESSION_TTL_SECS}{secure_attr}"
    );
    let mut resp = (
        StatusCode::OK,
        Json(AuthLoginResponse {
            status: "ok",
            authenticated: true,
        }),
    )
        .into_response();
    if let Ok(hv) = HeaderValue::from_str(&cookie) {
        resp.headers_mut().insert(header::SET_COOKIE, hv);
    }
    resp
}
