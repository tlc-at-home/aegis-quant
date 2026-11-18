//! REST and static asset route handlers for Aegis Quant v2.0 (`aegis-web::handlers`).

use crate::auth::{constant_time_eq, AuthLoginPayload, AuthLoginResponse};
use crate::payloads::{
    build_dashboard_payload, build_health_payload, execute_close_position_or_order,
    execute_risk_tier_switch, json_value_to_config_string, map_aegis_error_response,
    validate_config_key_value, ApiErrorBody, ClosePositionRequest, ConfigUpdateRequest,
    ConfigUpdateResponse, RiskTierUpdateRequest,
};
use crate::server::AppState;
use aegis_alerts::send_test_alert_async;
use aegis_core::{now_ms, AegisError};
use axum::{
    extract::State,
    http::{header, HeaderValue, StatusCode},
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

/// `GET /api/v1/health` — Returns MariaDB 13.0 version, 7 SQL Views status, risk tier (`extreme`, `1.85x`), execution mode, and loop cadence (`1s Priority / 3s Scanner`).
pub async fn get_health_handler(State(state): State<AppState>) -> Response {
    let db = state.db.clone();
    let res = tokio::task::spawn_blocking(move || build_health_payload(&db)).await;
    match res {
        Ok(Ok(payload)) => (StatusCode::OK, Json(payload)).into_response(),
        Ok(Err(e)) => map_aegis_error_response(e),
        Err(e) => map_aegis_error_response(AegisError::Database(format!("Health task error: {e}"))),
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
        Ok(Ok(payload)) => (StatusCode::OK, Json(payload)).into_response(),
        Ok(Err(e)) => map_aegis_error_response(e),
        Err(e) => {
            map_aegis_error_response(AegisError::Database(format!("Risk tier task error: {e}")))
        }
    }
}

/// `POST /api/v1/config` — Updates runtime config keys or Fernet-encrypted credentials in `system_config`.
pub async fn post_config_handler(
    State(state): State<AppState>,
    Json(req): Json<ConfigUpdateRequest>,
) -> Response {
    let db = state.db.clone();
    let res = tokio::task::spawn_blocking(move || -> aegis_core::Result<ConfigUpdateResponse> {
        let mut updated_keys = Vec::new();

        if let Some(ref k) = req.config_key {
            let val_str = req
                .config_value
                .as_ref()
                .map(json_value_to_config_string)
                .unwrap_or_default();
            validate_config_key_value(k, &val_str)?;
            db.set_config_value(k, &val_str, req.description.as_deref())?;
            updated_keys.push(k.trim().to_string());
        }

        if let Some(ref map) = req.updates {
            for (k, v) in map {
                let val_str = json_value_to_config_string(v);
                validate_config_key_value(k, &val_str)?;
                db.set_config_value(k, &val_str, None)?;
                updated_keys.push(k.trim().to_string());
            }
        }

        if updated_keys.is_empty() {
            return Err(AegisError::Validation(
                "No config_key or updates map provided".to_string(),
            ));
        }

        updated_keys.sort();
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
        Ok(Ok(payload)) => (StatusCode::OK, Json(payload)).into_response(),
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
        Ok(Ok(payload)) => (StatusCode::OK, Json(payload)).into_response(),
        Ok(Err(e)) => map_aegis_error_response(e),
        Err(e) => map_aegis_error_response(AegisError::Database(format!(
            "Close position task error: {e}"
        ))),
    }
}

/// `POST /api/v1/auth/login` — Exchanges bearer token for a session cookie.
pub async fn post_auth_login_handler(
    State(state): State<AppState>,
    Json(req): Json<AuthLoginPayload>,
) -> Response {
    let expected = state
        .configured_api_token
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(&state.session_token);
    if !constant_time_eq(req.token.trim(), expected) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ApiErrorBody {
                error: "Unauthorized".to_string(),
                detail: "Invalid API token".to_string(),
            }),
        )
            .into_response();
    }
    let cookie = format!(
        "aegis_session={}; Path=/; HttpOnly; SameSite=Strict",
        state.session_token
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
