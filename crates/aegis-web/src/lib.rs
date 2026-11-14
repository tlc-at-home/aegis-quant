//! Aegis-Quant v0.3.0-rc1 — Rust Axum Web Dashboard & REST/SSE Server (`aegis-web`)
//!
//! Replaces the legacy Python FastAPI web layer with a pure-Rust Axum server:
//! - Serves `web/static/index.html` on `GET /` and `web/static/app.js` on `GET /static/app.js`
//! - Provides all `/api/v1/*` JSON endpoints and `/api/v1/stream` SSE real-time push
//! - Explicit token authentication via `POST /api/v1/auth/login` or `Authorization: Bearer` / `X-Aegis-Token`
//! - Enforces Origin / Sec-Fetch-Site / custom-header CSRF protection on all mutating endpoints

pub mod handlers;
pub mod payloads;

pub use handlers::*;
pub use payloads::*;

use aegis_core::resolve_project_root;
use aegis_db::DatabaseManager;
use anyhow::Result;
use axum::{
    extract::{Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use rand::RngCore;
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

const DEFAULT_CSP_POLICY: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'";

#[derive(Debug, Clone)]
pub struct CachedWebAuthConfig {
    pub fetched_at_ms: i64,
    pub ttl_ms: i64,
    pub web_auth_enabled: bool,
    pub web_allow_query_token_sse: bool,
    pub db_token: Option<String>,
    pub web_cors_allowed_origins: Option<String>,
    pub web_allowed_hosts: String,
}

#[derive(Debug, Clone)]
pub struct PendingRestoreToken {
    pub filename: String,
    pub sha256: String,
    pub created_at: std::time::Instant,
}

#[derive(Clone)]
pub struct AppState {
    pub db: DatabaseManager,
    pub static_dir: PathBuf,
    pub cycle_lock: Arc<Mutex<()>>,
    pub session_token: String,
    pub auth_cache: Arc<parking_lot::Mutex<Option<CachedWebAuthConfig>>>,
    pub active_sessions: Arc<parking_lot::Mutex<std::collections::HashMap<String, i64>>>,
    pub login_failures:
        Arc<parking_lot::Mutex<std::collections::HashMap<String, (u32, std::time::Instant)>>>,
    pub sse_payload_cache: Arc<parking_lot::Mutex<Option<(std::time::Instant, u64, String)>>>,
    pub last_manual_backup_at: Arc<parking_lot::Mutex<Option<std::time::Instant>>>,
    pub restore_in_progress: Arc<std::sync::atomic::AtomicBool>,
    pub pending_restore_tokens:
        Arc<parking_lot::Mutex<std::collections::HashMap<String, PendingRestoreToken>>>,
}

impl AppState {
    pub fn get_cached_auth_config(&self) -> CachedWebAuthConfig {
        let now = self.db.now_ms();
        {
            let guard = self.auth_cache.lock();
            if let Some(cached) = guard.as_ref() {
                if now.saturating_sub(cached.fetched_at_ms) < cached.ttl_ms {
                    return cached.clone();
                }
            }
        }
        let mut db_cfg = self.db.load_config_opts(false).unwrap_or_default();
        if ["web_api_token", "dashboard_api_token"]
            .iter()
            .any(|k| db_cfg.get_str(k, "").trim().starts_with("enc:v1:"))
        {
            db_cfg = self.db.load_config_opts(true).unwrap_or(db_cfg);
        }
        let ttl_ms = db_cfg
            .get_i64("web_auth_cache_ttl_ms", 2000)
            .clamp(0, 60_000);
        let exec_mode = db_cfg
            .get_str("execution_mode", "PAPER")
            .trim()
            .to_ascii_uppercase();
        let pond_mode = db_cfg
            .get_str("pond_execution_mode", &exec_mode)
            .trim()
            .to_ascii_uppercase();
        let force_auth = matches!(exec_mode.as_str(), "LIVE" | "TESTNET")
            || matches!(pond_mode.as_str(), "LIVE" | "TESTNET");
        let raw_auth_enabled = db_cfg.get_bool("web_auth_enabled", true);
        let web_auth_enabled = if force_auth {
            if !raw_auth_enabled {
                tracing::warn!(
                    execution_mode = %exec_mode,
                    pond_execution_mode = %pond_mode,
                    "Ignoring web_auth_enabled=false while execution_mode or pond_execution_mode is LIVE/TESTNET (Table 3 #8)"
                );
            }
            true
        } else {
            raw_auth_enabled
        };
        let web_allow_query_token_sse = db_cfg.get_bool("web_allow_query_token_sse", false);
        let db_token = ["web_api_token", "dashboard_api_token"]
            .into_iter()
            .map(|k| db_cfg.get_str(k, ""))
            .find(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string());
        let cors = db_cfg.get_str("web_cors_allowed_origins", "");
        let web_cors_allowed_origins = if cors.trim().is_empty() {
            None
        } else {
            Some(cors.trim().to_string())
        };
        let hosts = db_cfg.get_str("web_allowed_hosts", "");
        let web_allowed_hosts = if hosts.trim().is_empty() {
            "localhost,127.0.0.1,[::1],aegis-quant-sandbox,192.168.122.254".to_string()
        } else {
            hosts.trim().to_string()
        };
        let fresh = CachedWebAuthConfig {
            fetched_at_ms: now,
            ttl_ms,
            web_auth_enabled,
            web_allow_query_token_sse,
            db_token,
            web_cors_allowed_origins,
            web_allowed_hosts,
        };
        *self.auth_cache.lock() = Some(fresh.clone());
        fresh
    }

    pub fn invalidate_auth_cache(&self) {
        *self.auth_cache.lock() = None;
    }
}

pub fn generate_ephemeral_session_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn resolve_initial_session_token(db: &DatabaseManager) -> String {
    if let Ok(tok) = std::env::var("AEGIS_WEB_TOKEN") {
        let trimmed = tok.trim().to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    let cfg = db.load_config_logged("aegis-web.resolve_initial_session_token");
    for key in ["web_api_token", "dashboard_api_token"] {
        let tok = cfg.get_str(key, "");
        if !tok.trim().is_empty() {
            return tok.trim().to_string();
        }
    }
    let generated = generate_ephemeral_session_token();
    let _ = db.set_config_value("web_api_token", &json!(generated));
    generated
}

pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    if a_bytes.len() != b_bytes.len() || a_bytes.is_empty() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a_bytes.iter().zip(b_bytes.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub(crate) fn extract_cookie_token(
    headers: &axum::http::HeaderMap,
    cookie_name: &str,
) -> Option<String> {
    let cookie_hdr = headers.get(header::COOKIE)?.to_str().ok()?;
    for part in cookie_hdr.split(';') {
        let trimmed = part.trim();
        if let Some(val) = trimmed.strip_prefix(&format!("{cookie_name}=")) {
            let v = val.trim();
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn extract_query_param(query: Option<&str>, key: &str) -> Option<String> {
    let q = query?;
    for pair in q.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key && !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

pub fn is_allowed_cors_origin(origin: &str, configured_origins: Option<&str>) -> bool {
    let trimmed = origin.trim();
    if trimmed.is_empty() {
        return false;
    }
    for prefix in [
        "http://127.0.0.1",
        "http://localhost",
        "https://127.0.0.1",
        "https://localhost",
        "http://aegis-quant-sandbox",
        "https://aegis-quant-sandbox",
    ] {
        if trimmed == prefix || trimmed.starts_with(&format!("{prefix}:")) {
            return true;
        }
    }
    if let Some(cfg_list) = configured_origins {
        for item in cfg_list.split(',') {
            let allowed = item.trim();
            if !allowed.is_empty() && allowed != "*" && allowed.eq_ignore_ascii_case(trimmed) {
                return true;
            }
        }
    }
    false
}

pub fn is_allowed_host(req: &Request, allowed_hosts_csv: &str) -> bool {
    let Some(host_hdr) = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
    else {
        return true;
    };
    let raw_host = host_hdr.trim();
    if raw_host.is_empty() {
        return true;
    }
    let host_only = if raw_host.starts_with('[') {
        if let Some(end_bracket) = raw_host.find(']') {
            &raw_host[..=end_bracket]
        } else {
            raw_host
        }
    } else {
        raw_host.split(':').next().unwrap_or(raw_host)
    };
    let list = if allowed_hosts_csv.trim().is_empty() {
        "localhost,127.0.0.1,[::1],aegis-quant-sandbox,192.168.122.254"
    } else {
        allowed_hosts_csv.trim()
    };
    for item in list.split(',') {
        let allowed = item.trim();
        if !allowed.is_empty() && allowed.eq_ignore_ascii_case(host_only) {
            return true;
        }
    }
    false
}

pub fn is_request_authorized(state: &AppState, req: &Request) -> bool {
    let cached = state.get_cached_auth_config();
    is_request_authorized_with_config(state, req, &cached)
}

fn is_request_authorized_with_config(
    state: &AppState,
    req: &Request,
    auth_cfg: &CachedWebAuthConfig,
) -> bool {
    let path = req.uri().path();
    if req.method() == Method::OPTIONS
        || path == "/"
        || path == "/static/app.js"
        || path == "/app.js"
        || path == "/health"
        || path == "/api/health"
        || path == "/api/v1/health"
        || path == "/api/v1/auth/login"
        || path == "/api/auth/login"
    {
        return true;
    }

    let env_token = std::env::var("AEGIS_WEB_TOKEN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    if !auth_cfg.web_auth_enabled {
        return true;
    }

    let mut valid_tokens = vec![state.session_token.clone()];
    if let Some(t) = env_token {
        valid_tokens.push(t);
    } else if let Some(ref t) = auth_cfg.db_token {
        valid_tokens.push(t.clone());
    }

    let mut header_candidates: Vec<String> = Vec::new();
    if let Some(auth_hdr) = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(bearer) = auth_hdr
            .strip_prefix("Bearer ")
            .or_else(|| auth_hdr.strip_prefix("bearer "))
        {
            header_candidates.push(bearer.trim().to_string());
        }
    }
    if let Some(api_hdr) = req
        .headers()
        .get("x-aegis-token")
        .and_then(|v| v.to_str().ok())
    {
        header_candidates.push(api_hdr.trim().to_string());
    }

    let has_valid_header_token = header_candidates.iter().any(|cand| {
        valid_tokens
            .iter()
            .any(|expected| constant_time_eq(cand, expected))
    });

    let cookie_tok = extract_cookie_token(req.headers(), "aegis_session");
    let has_valid_cookie_token = cookie_tok.as_ref().is_some_and(|cand| {
        let now_ms = state.db.now_ms();
        let mut sessions = state.active_sessions.lock();
        sessions.retain(|_, exp| *exp > now_ms);
        sessions
            .keys()
            .any(|active_nonce| constant_time_eq(cand, active_nonce))
    });

    let is_mutating = matches!(
        *req.method(),
        Method::POST | Method::PUT | Method::DELETE | Method::PATCH
    );

    if is_mutating {
        if let Some(sfs) = req
            .headers()
            .get("sec-fetch-site")
            .and_then(|v| v.to_str().ok())
        {
            if sfs.trim().eq_ignore_ascii_case("cross-site") {
                return false;
            }
        }

        if let Some(origin) = req
            .headers()
            .get(header::ORIGIN)
            .and_then(|v| v.to_str().ok())
        {
            if !is_allowed_cors_origin(origin, auth_cfg.web_cors_allowed_origins.as_deref()) {
                return false;
            }
        }

        if has_valid_header_token {
            return true;
        }

        let has_csrf_custom_header = req
            .headers()
            .get("x-csrf-token")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|s| !s.trim().is_empty())
            || req
                .headers()
                .get("x-requested-with")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|s| !s.trim().is_empty());

        return has_valid_cookie_token && has_csrf_custom_header;
    }

    if has_valid_header_token || has_valid_cookie_token {
        return true;
    }

    if auth_cfg.web_allow_query_token_sse && (path == "/api/v1/stream" || path == "/api/stream") {
        if let Some(q_tok) = extract_query_param(req.uri().query(), "token") {
            return valid_tokens
                .iter()
                .any(|expected| constant_time_eq(&q_tok, expected));
        }
    }

    false
}

async fn security_and_auth_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let origin_hdr = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let auth_cfg = state.get_cached_auth_config();
    let configured_origins = auth_cfg.web_cors_allowed_origins.clone();
    let allowed_hosts = auth_cfg.web_allowed_hosts.clone();

    if !is_allowed_host(&req, &allowed_hosts) {
        let body = ApiErrorBody {
            error: "Forbidden".to_string(),
            detail: "Disallowed Host header (DNS-rebinding protection)".to_string(),
        };
        let mut resp = (StatusCode::FORBIDDEN, Json(body)).into_response();
        attach_security_and_cors_headers(
            resp.headers_mut(),
            origin_hdr.as_deref(),
            configured_origins.as_deref(),
        );
        return resp;
    }

    if req.method() == Method::OPTIONS {
        let mut resp = StatusCode::NO_CONTENT.into_response();
        attach_security_and_cors_headers(
            resp.headers_mut(),
            origin_hdr.as_deref(),
            configured_origins.as_deref(),
        );
        return resp;
    }

    if !is_request_authorized_with_config(&state, &req, &auth_cfg) {
        let body = ApiErrorBody {
            error: "Unauthorized".to_string(),
            detail: "Missing or invalid API authentication token or CSRF header".to_string(),
        };
        let mut resp = (StatusCode::UNAUTHORIZED, Json(body)).into_response();
        attach_security_and_cors_headers(
            resp.headers_mut(),
            origin_hdr.as_deref(),
            configured_origins.as_deref(),
        );
        return resp;
    }

    let path = req.uri().path();
    if state
        .restore_in_progress
        .load(std::sync::atomic::Ordering::Acquire)
        && !matches!(
            path,
            "/api/v1/db/restore" | "/api/db/restore" | "/health" | "/api/health" | "/api/v1/health"
        )
    {
        let body = ApiErrorBody {
            error: "Service Unavailable".to_string(),
            detail: "Database restore is currently in progress; retry shortly".to_string(),
        };
        let mut resp = (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
        attach_security_and_cors_headers(
            resp.headers_mut(),
            origin_hdr.as_deref(),
            configured_origins.as_deref(),
        );
        return resp;
    }

    let mut resp = next.run(req).await;
    attach_security_and_cors_headers(
        resp.headers_mut(),
        origin_hdr.as_deref(),
        configured_origins.as_deref(),
    );
    resp
}

fn attach_security_and_cors_headers(
    headers: &mut axum::http::HeaderMap,
    origin: Option<&str>,
    configured_origins: Option<&str>,
) {
    headers.insert(
        header::HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static(DEFAULT_CSP_POLICY),
    );
    headers.insert(
        header::HeaderName::from_static("x-frame-options"),
        HeaderValue::from_static("DENY"),
    );
    headers.insert(
        header::HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );

    if let Some(orig) = origin {
        if is_allowed_cors_origin(orig, configured_origins) {
            if let Ok(hv) = HeaderValue::from_str(orig) {
                headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, hv);
                headers.insert(
                    header::ACCESS_CONTROL_ALLOW_METHODS,
                    HeaderValue::from_static("GET, POST, OPTIONS"),
                );
                headers.insert(
                    header::ACCESS_CONTROL_ALLOW_HEADERS,
                    HeaderValue::from_static(
                        "Content-Type, Authorization, X-Aegis-Token, X-CSRF-Token, X-Requested-With",
                    ),
                );
                headers.insert(header::VARY, HeaderValue::from_static("Origin"));
            }
        }
    }
}

pub fn build_router(db: DatabaseManager, static_dir: impl AsRef<Path>) -> Router {
    let session_token = resolve_initial_session_token(&db);
    let state = AppState {
        db,
        static_dir: static_dir.as_ref().to_path_buf(),
        cycle_lock: Arc::new(Mutex::new(())),
        session_token,
        auth_cache: Arc::new(parking_lot::Mutex::new(None)),
        active_sessions: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        login_failures: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        sse_payload_cache: Arc::new(parking_lot::Mutex::new(None)),
        last_manual_backup_at: Arc::new(parking_lot::Mutex::new(None)),
        restore_in_progress: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        pending_restore_tokens: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
    };

    Router::new()
        .route("/", get(serve_index))
        .route("/static/app.js", get(serve_app_js))
        .route("/app.js", get(serve_app_js))
        .route("/api/v1/auth/login", post(api_auth_login))
        .route("/api/auth/login", post(api_auth_login))
        .route("/api/v1/auth/logout", post(api_auth_logout))
        .route("/api/auth/logout", post(api_auth_logout))
        .route("/health", get(api_health))
        .route("/api/health", get(api_health))
        .route("/api/v1/health", get(api_health))
        .route("/api/v1/status", get(api_status))
        .route("/api/status", get(api_status))
        .route("/api/v1/dashboard", get(api_dashboard))
        .route("/api/v1/portfolio", get(api_portfolio))
        .route("/api/v1/pond", get(api_pond))
        .route("/api/v1/pond/universe", get(api_pond_universe))
        .route("/api/v1/pond/cells", get(api_pond_cells))
        .route("/api/v1/pond/cells/close", post(api_close_trade))
        .route("/api/v1/pond/cycles", get(api_pond_cycles))
        .route("/api/v1/pond/scan", post(api_trigger_pond_scan))
        .route("/api/v1/settle", get(api_settle))
        .route("/api/v1/settle/run", post(api_settle_run))
        .route("/api/v1/signals", get(api_signals))
        .route("/api/v1/trades", get(api_trades))
        .route("/api/v1/trades/close", post(api_close_trade))
        .route("/api/v1/config", get(api_config).post(api_update_config))
        .route("/api/v1/config/batch", post(api_update_config_batch))
        .route(
            "/api/v1/credentials",
            get(api_credentials).post(api_update_credentials),
        )
        .route("/api/v1/alerts", get(api_alerts).post(api_update_alerts))
        .route("/api/v1/alerts/test", post(api_test_alert))
        .route("/api/v1/wallet/sync", post(api_update_wallet_sync))
        .route("/api/v1/logs", get(api_logs))
        .route("/api/v1/engine/cycle", post(api_trigger_cycle))
        .route("/api/v1/engine/backtest", post(api_trigger_backtest))
        .route("/api/v1/engine/calibrate", post(api_trigger_calibrate))
        .route("/api/v1/db/backup", post(api_trigger_db_backup))
        .route("/api/v1/db/backups", get(api_list_db_backups))
        .route("/api/v1/db/restore", post(api_restore_db_backup))
        .route("/api/v1/stream", get(api_stream))
        // Legacy aliases for backwards compatibility
        .route("/api/dashboard", get(api_dashboard))
        .route("/api/portfolio", get(api_portfolio))
        .route("/api/pond", get(api_pond))
        .route("/api/pond/cells/close", post(api_close_trade))
        .route("/api/pond/scan", post(api_trigger_pond_scan))
        .route("/api/settle", get(api_settle))
        .route("/api/settle/run", post(api_settle_run))
        .route("/api/signals", get(api_signals))
        .route("/api/trades", get(api_trades))
        .route("/api/trades/close", post(api_close_trade))
        .route("/api/config", get(api_config).post(api_update_config))
        .route("/api/config/batch", post(api_update_config_batch))
        .route(
            "/api/credentials",
            get(api_credentials).post(api_update_credentials),
        )
        .route("/api/alerts", get(api_alerts).post(api_update_alerts))
        .route("/api/alerts/test", post(api_test_alert))
        .route("/api/wallet/sync", post(api_update_wallet_sync))
        .route("/api/logs", get(api_logs))
        .route("/api/cycle", post(api_trigger_cycle))
        .route("/api/backtest", post(api_trigger_backtest))
        .route("/api/calibrate", post(api_trigger_calibrate))
        .route("/api/db/backup", post(api_trigger_db_backup))
        .route("/api/db/backups", get(api_list_db_backups))
        .route("/api/db/restore", post(api_restore_db_backup))
        .route("/api/stream", get(api_stream))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            security_and_auth_middleware,
        ))
        .with_state(state)
}

pub async fn run_web_server(
    db: DatabaseManager,
    static_dir: impl AsRef<Path>,
    host: &str,
    port: u16,
) -> Result<()> {
    run_web_server_with_shutdown(db, static_dir, host, port, async {
        #[cfg(unix)]
        {
            if let Ok(mut sigterm) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
                return;
            }
        }
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}

pub async fn run_web_server_with_shutdown<F>(
    db: DatabaseManager,
    static_dir: impl AsRef<Path>,
    host: &str,
    port: u16,
    shutdown_signal: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let app = build_router(db, static_dir);
    let addr = format!("{host}:{port}");
    tracing::info!("Aegis-Quant PP-50 Rust Web Server listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await?;
    Ok(())
}

/// Entrypoint invoked by `aegis-cli` (`aegis-quant`) to serve the embedded/static WebUI & REST/SSE API.
pub async fn serve_web(db: DatabaseManager, host: &str, port: u16) -> Result<()> {
    let static_dir = resolve_project_root().join("web").join("static");
    run_web_server(db, static_dir, host, port).await
}

/// Entrypoint invoked by `aegis-cli` (`aegis-quant`) with an explicit `watch::Receiver<bool>` shutdown signal (`HI-5`).
pub async fn serve_web_with_shutdown(
    db: DatabaseManager,
    host: &str,
    port: u16,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let static_dir = resolve_project_root().join("web").join("static");
    run_web_server_with_shutdown(db, static_dir, host, port, async move {
        while !*shutdown_rx.borrow() {
            if shutdown_rx.changed().await.is_err() {
                break;
            }
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_pit::PitOrchestrator;
    use aegis_settle::{SettleController, SettleOverrideInput};
    use axum::body::Body;
    use tower::ServiceExt;

    #[test]
    fn test_index_html_local_timezone_and_four_pages() {
        let html_path = resolve_project_root().join("web/static/index.html");
        let html = std::fs::read_to_string(&html_path).expect("read web/static/index.html");

        for id in [
            "page-overall",
            "page-pit",
            "page-pond",
            "page-settings",
            "tab-btn-overall",
            "tab-btn-pit",
            "tab-btn-pond",
            "tab-btn-settings",
            "parseUtcToDate",
            "formatLocalTimestamp",
            "formatUtcTimeOfDayToLocal",
            "updateTimezoneIndicators",
            "switchDashboardPage",
            "formatPnlPillHtml",
        ] {
            assert!(
                html.contains(id),
                "index.html missing expected marker: {id}"
            );
        }

        assert!(
            !html.contains("Timestamp (UTC)"),
            "index.html should not contain UTC timestamp column headers"
        );
        assert!(
            !html.contains("Opened (UTC)"),
            "index.html should not contain Opened (UTC) column header"
        );
    }

    #[tokio::test]
    async fn test_dashboard_payload_and_settle_endpoint() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).expect("seed schema");

        let payload = build_dashboard_payload(&db).expect("dashboard payload");
        assert_eq!(payload.schema_version, aegis_db::CANONICAL_SCHEMA_VERSION);
        assert_eq!(payload.pp50.total_platform_equity_usdc, 10000.0);
        assert_eq!(payload.pond.universe.len(), 80);

        let settle_ctrl = SettleController::new(db.clone(), None);
        let report = settle_ctrl
            .run_settling(SettleOverrideInput {
                dry_run: true,
                ..Default::default()
            })
            .expect("dry run settle");
        assert_eq!(report.total_equity_usdc, 10000.0);
    }

    #[tokio::test]
    async fn test_dashboard_pp50_payload_and_four_page_html() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).expect("init schema");

        let payload = build_dashboard_payload(&db).expect("build dashboard payload");
        assert_eq!(payload.schema_version, aegis_db::CANONICAL_SCHEMA_VERSION);

        assert_eq!(payload.pp50.total_platform_equity_usdc, 10000.0);
        assert_eq!(payload.pp50.pit.equity_usdc, 5000.0);
        assert_eq!(payload.pp50.pond.equity_usdc, 5000.0);

        assert_eq!(payload.pond.book, "POND");
        assert_eq!(payload.pond.active_champion_id, "NPS-01");
        assert_eq!(payload.pond.max_concurrent_cells, 8);
        assert_eq!(payload.pond.universe.len(), 80);

        let index_html = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../web/static/index.html"),
        )
        .expect("read index.html");
        assert_eq!(index_html, EMBEDDED_INDEX_HTML);
        for page_id in ["page-overall", "page-pit", "page-pond", "page-settings"] {
            assert!(
                index_html.contains(page_id),
                "index.html missing 4-page section {page_id}"
            );
        }
        assert!(index_html.contains("80-Coin Dynamic Pond Universe"));
        for helper in [
            "parseUtcToDate",
            "formatLocalTimestamp",
            "formatUtcTimeOfDayToLocal",
            "escapeHtml",
        ] {
            assert!(
                index_html.contains(helper),
                "index.html missing helper function {helper}"
            );
        }
    }

    #[tokio::test]
    async fn test_get_index_does_not_mint_session_and_csrf_blocked_b1_nh1() {
        let root = resolve_project_root();
        let db = DatabaseManager::memory();
        db.initialize_schema(true).expect("seed schema");
        db.set_config_value("web_api_token", &json!("test-secret-token-b1-nh1"))
            .expect("set web_api_token");

        let app = build_router(db.clone(), root.join("web/static"));

        // 1. GET / must NOT mint a session cookie or inject a meta session token (B1, NH-1)
        let idx_req = axum::http::Request::builder()
            .method("GET")
            .uri("/")
            .header(header::ORIGIN, "http://127.0.0.1:8080")
            .body(Body::empty())
            .unwrap();
        let idx_resp = app.clone().oneshot(idx_req).await.unwrap();
        assert_eq!(idx_resp.status(), StatusCode::OK);
        assert!(
            idx_resp.headers().get(header::SET_COOKIE).is_none(),
            "GET / must not mint Set-Cookie session token (B1/NH-1)"
        );
        let csp = idx_resp
            .headers()
            .get("content-security-policy")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            !csp.contains("script-src 'self' 'unsafe-inline'"),
            "CSP script-src must not allow 'unsafe-inline' (NL-1): {csp}"
        );
        assert_eq!(idx_resp.headers().get("x-frame-options").unwrap(), "DENY");
        assert_eq!(
            idx_resp.headers().get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert_eq!(
            idx_resp
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .unwrap(),
            "http://127.0.0.1:8080"
        );
        let body_bytes = axum::body::to_bytes(idx_resp.into_body(), 256 * 1024)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body_bytes);
        assert!(
            !body_str.contains("aegis-session-token"),
            "GET / HTML body must not embed session token meta tag (B1)"
        );

        // 2. Unauthenticated GET /api/v1/dashboard must return 401
        let unauth_req = axum::http::Request::builder()
            .method("GET")
            .uri("/api/v1/dashboard")
            .header(header::ORIGIN, "https://evil.example.com")
            .body(Body::empty())
            .unwrap();
        let unauth_resp = app.clone().oneshot(unauth_req).await.unwrap();
        assert_eq!(unauth_resp.status(), StatusCode::UNAUTHORIZED);
        assert!(unauth_resp
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none());

        // 3. Explicit login via POST /api/v1/auth/login with wrong token returns 401
        let bad_login = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/auth/login")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"token":"wrong-token"}"#))
            .unwrap();
        let bad_login_resp = app.clone().oneshot(bad_login).await.unwrap();
        assert_eq!(bad_login_resp.status(), StatusCode::UNAUTHORIZED);

        // 4. Explicit login via POST /api/v1/auth/login with valid token mints session cookie
        let good_login = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/auth/login")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"token":"test-secret-token-b1-nh1"}"#))
            .unwrap();
        let good_login_resp = app.clone().oneshot(good_login).await.unwrap();
        assert_eq!(good_login_resp.status(), StatusCode::OK);
        let set_cookie = good_login_resp
            .headers()
            .get(header::SET_COOKIE)
            .expect("POST /api/v1/auth/login must set session cookie")
            .to_str()
            .unwrap();
        assert!(set_cookie.contains("aegis_session="));
        assert!(set_cookie.contains("HttpOnly"));
        assert!(set_cookie.contains("SameSite=Strict"));
        let cookie_pair = set_cookie.split(';').next().unwrap().to_string();

        // 5. Cookie-only POST without custom CSRF header or with cross-site Origin must be rejected (NH-1)
        let csrf_post = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/config")
            .header(header::COOKIE, &cookie_pair)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"config_key":"settling_deadband_bps","config_value":60}"#,
            ))
            .unwrap();
        let csrf_resp = app.clone().oneshot(csrf_post).await.unwrap();
        assert_eq!(
            csrf_resp.status(),
            StatusCode::UNAUTHORIZED,
            "Cookie-only POST without custom CSRF header must be blocked (NH-1)"
        );

        let cross_origin_post = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/config")
            .header(header::COOKIE, &cookie_pair)
            .header("x-requested-with", "XMLHttpRequest")
            .header(header::ORIGIN, "https://evil.example.com")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"config_key":"settling_deadband_bps","config_value":60}"#,
            ))
            .unwrap();
        let cross_origin_resp = app.clone().oneshot(cross_origin_post).await.unwrap();
        assert_eq!(
            cross_origin_resp.status(),
            StatusCode::UNAUTHORIZED,
            "Cross-origin POST must be blocked (NH-1)"
        );

        // 6. Same-origin POST with session cookie + custom header succeeds
        let valid_post = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/config")
            .header(header::COOKIE, &cookie_pair)
            .header("x-requested-with", "XMLHttpRequest")
            .header(header::ORIGIN, "http://127.0.0.1:8000")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"config_key":"settling_deadband_bps","config_value":60}"#,
            ))
            .unwrap();
        let valid_post_resp = app.oneshot(valid_post).await.unwrap();
        assert_eq!(valid_post_resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_live_settle_rejects_equity_overrides_nh3() {
        let root = resolve_project_root();
        let db = DatabaseManager::memory();
        db.initialize_schema(true).expect("seed schema");
        db.set_config_value("web_api_token", &json!("test-nh3-token"))
            .expect("set token");
        db.set_config_value("execution_mode", &json!("LIVE"))
            .expect("set LIVE mode");

        let app = build_router(db.clone(), root.join("web/static"));

        // Non-dry-run settle with manual equity override in LIVE mode must be rejected with 400 Bad Request
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/settle/run")
            .header(header::AUTHORIZATION, "Bearer test-nh3-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"dry_run":false,"pit_equity_usdc":99999.0,"pond_equity_usdc":1000.0}"#,
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let pit_orch = PitOrchestrator::new(db, None);
        let (pit_eq, _) = pit_orch.compute_pit_equity_and_idle_usdc().unwrap();
        assert_eq!(
            pit_eq, 5000.0,
            "LIVE book equity must not be mutated by rejected override"
        );
    }

    #[test]
    fn test_internal_error_response_is_sanitized_nl3() {
        let resp = map_err_response(anyhow::anyhow!(
            "SQLite internal disk path /var/lib/aegis/secret.db constraint failure"
        ));
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_nh1_nh2_nh3_nm1_web_auth_login_and_immutable_keys() {
        let root = resolve_project_root();
        let db = DatabaseManager::memory();
        db.initialize_schema(true).expect("seed schema");
        db.set_config_value("web_api_token", &json!("test-nh1-nh2-token"))
            .expect("set token");

        let app = build_router(db.clone(), root.join("web/static"));

        // 1. Disallowed Host header must be rejected with 403 Forbidden (NH-1)
        let bad_host_req = axum::http::Request::builder()
            .method("GET")
            .uri("/api/v1/health")
            .header(header::HOST, "attacker.evil.com")
            .body(Body::empty())
            .unwrap();
        let bad_host_resp = app.clone().oneshot(bad_host_req).await.unwrap();
        assert_eq!(bad_host_resp.status(), StatusCode::FORBIDDEN);

        // 2. Immutable config key via POST /api/v1/config must return 403 Forbidden (NH-2, NH-3)
        let immut_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/config")
            .header(header::AUTHORIZATION, "Bearer test-nh1-nh2-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"config_key":"hyperliquid_vault_address","config_value":"0x1111111111111111111111111111111111111111"}"#,
            ))
            .unwrap();
        let immut_resp = app.clone().oneshot(immut_req).await.unwrap();
        assert_eq!(immut_resp.status(), StatusCode::FORBIDDEN);

        // 3. Immutable vault address via POST /api/v1/credentials must also return 403 Forbidden (NH-3)
        let cred_vault_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/credentials")
            .header(header::AUTHORIZATION, "Bearer test-nh1-nh2-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"hyperliquid_vault_address":"0x1111111111111111111111111111111111111111"}"#,
            ))
            .unwrap();
        let cred_vault_resp = app.clone().oneshot(cred_vault_req).await.unwrap();
        assert_eq!(cred_vault_resp.status(), StatusCode::FORBIDDEN);

        // 4. Logout clears the session cookie (NH-1)
        let logout_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/auth/logout")
            .header(header::AUTHORIZATION, "Bearer test-nh1-nh2-token")
            .body(Body::empty())
            .unwrap();
        let logout_resp = app.oneshot(logout_req).await.unwrap();
        assert_eq!(logout_resp.status(), StatusCode::OK);
        let clear_cookie = logout_resp
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(clear_cookie.contains("Max-Age=0"));
    }

    #[tokio::test]
    async fn test_r3_b5_h25_sensitive_config_redaction_and_typed_error_mapping() {
        let root = resolve_project_root();
        let db = DatabaseManager::memory();
        db.initialize_schema(true).expect("seed schema");
        db.set_config_value("web_api_token", &json!("test-r3-b5-token"))
            .expect("set token");

        let app = build_router(db.clone(), root.join("web/static"));

        // 1. Update sensitive keys (`private_key`, `telegram_bot_token`) via POST /api/v1/config (R3-B5, R3-QE #5)
        let secret_pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let pk_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/config")
            .header(header::AUTHORIZATION, "Bearer test-r3-b5-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(format!(
                r#"{{"config_key":"private_key","config_value":"{secret_pk}"}}"#
            )))
            .unwrap();
        let pk_resp = app.clone().oneshot(pk_req).await.unwrap();
        assert_eq!(pk_resp.status(), StatusCode::OK);

        let secret_tg = format!(
            "{}:{}",
            "123456789", "AAH_super_secret_telegram_bot_token_value"
        );
        let tg_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/config")
            .header(header::AUTHORIZATION, "Bearer test-r3-b5-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(format!(
                r#"{{"config_key":"telegram_bot_token","config_value":"{secret_tg}"}}"#
            )))
            .unwrap();
        let tg_resp = app.clone().oneshot(tg_req).await.unwrap();
        assert_eq!(tg_resp.status(), StatusCode::OK);

        // Verify telemetry_logs records `[REDACTED]` and never contains the plaintext secret values (R3-B5)
        let logs = db.get_recent_logs(20).unwrap();
        fn msg_of(l: &serde_json::Value) -> &str {
            l.get("message").and_then(|v| v.as_str()).unwrap_or("")
        }
        assert!(
            logs.iter()
                .any(|l| msg_of(l).contains("private_key = [REDACTED]")),
            "telemetry_logs must record [REDACTED] for private_key update"
        );
        assert!(
            logs.iter()
                .any(|l| msg_of(l).contains("telegram_bot_token = [REDACTED]")),
            "telemetry_logs must record [REDACTED] for telegram_bot_token update"
        );
        for log in &logs {
            let m = msg_of(log);
            assert!(
                !m.contains(secret_pk) && !m.contains(&secret_tg),
                "telemetry_logs must never contain plaintext sensitive config values"
            );
        }

        // 2. Unknown config key returns 400 Bad Request via typed AegisError::Validation (R3-H25)
        let bad_key_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/config")
            .header(header::AUTHORIZATION, "Bearer test-r3-b5-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"config_key":"nonexistent_config_key","config_value":"123"}"#,
            ))
            .unwrap();
        let bad_key_resp = app.oneshot(bad_key_req).await.unwrap();
        assert_eq!(bad_key_resp.status(), StatusCode::BAD_REQUEST);

        // 3. Timeframe validation in validate_config_update (R3-N13)
        assert!(payloads::validate_config_update("hmm_timeframe", "4h").is_ok());
        assert!(payloads::validate_config_update("hmm_timeframe", "3h").is_err());
        assert!(payloads::validate_config_update("calibration_timeframes_csv", "1h,4h").is_ok());
        assert!(payloads::validate_config_update("calibration_timeframes_csv", "1h,3h").is_err());
    }

    #[tokio::test]
    async fn test_manual_take_profit_close_endpoints_and_ui_wiring() {
        let root = resolve_project_root();
        let db = DatabaseManager::memory();
        db.initialize_schema(true).expect("seed schema");
        db.set_config_value("web_api_token", &json!("test-tp-token"))
            .expect("set token");

        // 1. Insert an open trade and an open Pond cell
        db.reconcile_on_chain_orphan_position(
            "TAO/USDC:USDC",
            "TAO",
            "SHORT",
            0.39,
            317.85,
            299.10,
            7.31,
        )
        .expect("insert open trade");
        let open_trades = db.get_open_trades().unwrap();
        let tao = open_trades
            .iter()
            .find(|t| t.symbol == "TAO/USDC:USDC")
            .expect("TAO open trade");

        let cell_id = db
            .upsert_pond_cell(&aegis_core::PondCell {
                cell_id: 0,
                symbol: "NEAR/USDC:USDC".to_string(),
                champion_id: 1,
                state: "OPEN".to_string(),
                cycle_opened_id: None,
                side: "SHORT".to_string(),
                option_analogue: "SHORT_CALL".to_string(),
                entry_price: Some(1.4486),
                quantity: Some(747.8),
                notional_usdc: Some(1083.26),
                risk_budget_usdc: Some(25.0),
                conviction_multiplier: Some(1.0),
                funding_z_entry: Some(0.0),
                funding_rate_entry: Some(0.0),
                initial_stop: Some(1.47),
                chandelier_stop: Some(1.437),
                highest_price_seen: Some(1.4486),
                lowest_price_seen: Some(1.4258),
                funding_accrued_usdc: 0.0,
                realized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.2,
                exit_price: None,
                exit_reason: None,
                opened_at: Some("2026-11-09T05:13:02Z".to_string()),
                closed_at: None,
                cooldown_until: None,
                updated_at: "2026-11-09T05:13:02Z".to_string(),
                resting_order_id: None,
            })
            .expect("insert open pond cell");

        let app = build_router(db.clone(), root.join("web/static"));

        // 2. Close open trade via POST /api/v1/trades/close
        let close_trade_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/trades/close")
            .header(header::AUTHORIZATION, "Bearer test-tp-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(format!(r#"{{"trade_id":{}}}"#, tao.trade_id)))
            .unwrap();
        let close_trade_resp = app.clone().oneshot(close_trade_req).await.unwrap();
        assert_eq!(close_trade_resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(close_trade_resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let close_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(close_json["status"], "ok");
        assert_eq!(
            close_json["close_result"]["exit_reason"],
            "MANUAL_TAKE_PROFIT"
        );

        // 3. Close open Pond cell via POST /api/v1/pond/cells/close
        let close_cell_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/pond/cells/close")
            .header(header::AUTHORIZATION, "Bearer test-tp-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(format!(r#"{{"cell_id":{}}}"#, cell_id)))
            .unwrap();
        let close_cell_resp = app.clone().oneshot(close_cell_req).await.unwrap();
        assert_eq!(close_cell_resp.status(), StatusCode::OK);
        let cell_bytes = axum::body::to_bytes(close_cell_resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let cell_json: serde_json::Value = serde_json::from_slice(&cell_bytes).unwrap();
        assert_eq!(cell_json["status"], "ok");
        assert_eq!(
            cell_json["close_result"]["exit_reason"],
            "MANUAL_TAKE_PROFIT"
        );

        // 4. Edge cases: empty payload and already-closed trade return 400 Bad Request
        let empty_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/trades/close")
            .header(header::AUTHORIZATION, "Bearer test-tp-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{}"#))
            .unwrap();
        let empty_resp = app.clone().oneshot(empty_req).await.unwrap();
        assert_eq!(empty_resp.status(), StatusCode::BAD_REQUEST);

        // R7-N6: Supplying both trade_id and cell_id simultaneously is rejected with 400 Bad Request
        let both_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/trades/close")
            .header(header::AUTHORIZATION, "Bearer test-tp-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(format!(
                r#"{{"trade_id":{},"cell_id":{}}}"#,
                tao.trade_id, cell_id
            )))
            .unwrap();
        let both_resp = app.clone().oneshot(both_req).await.unwrap();
        assert_eq!(both_resp.status(), StatusCode::BAD_REQUEST);

        let dup_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/trades/close")
            .header(header::AUTHORIZATION, "Bearer test-tp-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(format!(r#"{{"trade_id":{}}}"#, tao.trade_id)))
            .unwrap();
        let dup_resp = app.oneshot(dup_req).await.unwrap();
        assert_eq!(dup_resp.status(), StatusCode::BAD_REQUEST);

        // 5. Verify static HTML and JS contain Take Profit button wiring
        let app_js = std::fs::read_to_string(root.join("web/static/app.js")).unwrap();
        assert!(app_js.contains("data-take-profit-trade"));
        assert!(app_js.contains("data-take-profit-cell"));
        assert!(app_js.contains("/api/v1/trades/close"));
        assert!(app_js.contains("/api/v1/pond/cells/close"));
    }

    #[test]
    fn test_dashboard_live_pit_and_pond_pnl_and_notional_consistency() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).expect("seed schema");

        // 1. Insert an open PIT trade and update its mark price
        db.reconcile_on_chain_orphan_position(
            "SOL/USDC:USDC",
            "SOL",
            "SHORT",
            2.0,
            100.0,
            95.0,
            10.0,
        )
        .expect("insert open SOL trade");

        // 2. Insert an open Pond cell with partial-exit realized PnL and fees
        let cell_id = db
            .upsert_pond_cell(&aegis_core::PondCell {
                cell_id: 0,
                symbol: "ETH/USDC:USDC".to_string(),
                champion_id: 1,
                state: "TRAILED".to_string(),
                cycle_opened_id: None,
                side: "LONG".to_string(),
                option_analogue: "SHORT_PUT".to_string(),
                entry_price: Some(2000.0),
                quantity: Some(0.05),
                notional_usdc: Some(100.0),
                risk_budget_usdc: Some(10.0),
                conviction_multiplier: Some(1.0),
                funding_z_entry: Some(-2.2),
                funding_rate_entry: Some(-0.0001),
                initial_stop: Some(1900.0),
                chandelier_stop: Some(1950.0),
                highest_price_seen: Some(2050.0),
                lowest_price_seen: Some(1980.0),
                funding_accrued_usdc: 0.25,
                realized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.05,
                exit_price: None,
                exit_reason: None,
                opened_at: Some("2026-11-10T09:00:00Z".to_string()),
                closed_at: None,
                cooldown_until: None,
                updated_at: "2026-11-10T09:00:00Z".to_string(),
                resting_order_id: None,
            })
            .expect("insert open pond cell");

        db.reduce_open_pond_cell_partial_exit(
            cell_id,
            "TRAILED",
            Some(1950.0),
            Some(2050.0),
            Some(1980.0),
            0.02,
            40.0,
            -5.50,
            0.04,
        )
        .expect("partial exit reduction");

        let payload = build_dashboard_payload(&db).expect("build dashboard payload");
        assert_eq!(payload.portfolio.deployed_notional_usdc, 190.0);
        assert!(payload.portfolio.gross_leverage > 0.0);
        assert!(
            (payload.portfolio.pit_initial_capital_usdc + payload.portfolio.total_pnl_usdc
                - payload.portfolio.pit_equity_usdc)
                .abs()
                < 0.02
        );
        assert_eq!(payload.pond.realized_pnl_usdc, -5.50);
        assert!(
            (payload.pond.initial_capital_usdc + payload.pond.total_pnl_usdc
                - payload.pond.pond_equity_usdc)
                .abs()
                < 0.02
        );
    }

    #[test]
    fn test_r10_qa_r11_qa_twin_parity_invariants() {
        use aegis_core::{round_dp, BookTransfer, PondCell, PortfolioSnapshot, TradeRecord};

        // =========================================================================
        // Family 1: SQL UPDATE Twin Parity (trade_ops.rs vs pond_ops.rs)
        // =========================================================================
        let db1 = DatabaseManager::memory();
        db1.initialize_schema(true).expect("seed schema db1");

        let pit_trade_id = db1
            .insert_trade(&TradeRecord {
                trade_id: 0,
                client_order_id: "twin-pit-1".to_string(),
                symbol: "SOL/USDC:USDC".to_string(),
                base_asset: "SOL".to_string(),
                side: "LONG".to_string(),
                status: "OPEN".to_string(),
                fsm_state: "FILLED".to_string(),
                entry_price: 100.0,
                current_price: 100.0,
                exit_price: None,
                quantity: 10.0,
                filled_units: 10.0,
                avg_fill_price: Some(100.0),
                notional_usdc: 1000.0,
                risk_budget_usdc: 50.0,
                conviction_multiplier: 1.0,
                initial_stop_price: 95.0,
                chandelier_stop_price: 95.0,
                current_stop_price: 95.0,
                highest_price_seen: 100.0,
                lowest_price_seen: 100.0,
                unrealized_pnl_usdc: 0.0,
                realized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.45,
                funding_accrued_usdc: 1.25,
                slippage_paid_usd: 0.0,
                exchange_order_id: Some("hl-pit-1".to_string()),
                opened_at: "2026-11-10T10:00:00Z".to_string(),
                closed_at: None,
                exit_reason: None,
            })
            .expect("insert twin pit trade");

        let pond_cell_id = db1
            .upsert_pond_cell(&PondCell {
                cell_id: 0,
                symbol: "SOL/USDC:USDC".to_string(),
                champion_id: 1,
                state: "OPEN".to_string(),
                cycle_opened_id: None,
                side: "LONG".to_string(),
                option_analogue: "SHORT_PUT".to_string(),
                entry_price: Some(100.0),
                quantity: Some(10.0),
                notional_usdc: Some(1000.0),
                risk_budget_usdc: Some(50.0),
                conviction_multiplier: Some(1.0),
                funding_z_entry: Some(-2.4),
                funding_rate_entry: Some(-0.0001),
                initial_stop: Some(95.0),
                chandelier_stop: Some(95.0),
                highest_price_seen: Some(100.0),
                lowest_price_seen: Some(100.0),
                funding_accrued_usdc: 1.25,
                realized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.45,
                exit_price: None,
                exit_reason: None,
                resting_order_id: None,
                opened_at: Some("2026-11-10T10:00:00Z".to_string()),
                closed_at: None,
                cooldown_until: None,
                updated_at: "2026-11-10T10:00:00Z".to_string(),
            })
            .expect("insert twin pond cell");

        // 1a. Apply identical 40% partial exit (10.0 -> 6.0) to both PIT trade and POND cell at mark_price = 102.0
        db1.upsert_ohlcv_batch(&[aegis_core::OhlcvBar {
            symbol: "SOL/USDC:USDC".to_string(),
            timeframe: "1h".to_string(),
            timestamp_ms: 1_794_310_000_000,
            open: 100.0,
            high: 102.5,
            low: 99.5,
            close: 102.0,
            volume: 1000.0,
            funding_rate_hourly: 0.0,
            is_synthetic: 0,
        }])
        .unwrap();
        db1.reduce_open_trade_partial_exit(pit_trade_id, 6.0, 600.0, 12.0, 0.27, 0.0, 102.0)
            .unwrap();
        assert!(db1
            .reduce_open_pond_cell_partial_exit(
                pond_cell_id,
                "TRAILED",
                Some(95.0),
                Some(102.0),
                Some(100.0),
                6.0,
                600.0,
                12.0,
                0.27,
            )
            .unwrap());

        let t_partial = db1
            .get_open_trades()
            .unwrap()
            .into_iter()
            .find(|t| t.trade_id == pit_trade_id)
            .unwrap();
        let c_partial = db1
            .get_open_pond_cells()
            .unwrap()
            .into_iter()
            .find(|c| c.cell_id == pond_cell_id)
            .unwrap();

        assert_eq!(t_partial.quantity, c_partial.quantity.unwrap());
        assert_eq!(t_partial.notional_usdc, c_partial.notional_usdc.unwrap());
        assert_eq!(
            t_partial.risk_budget_usdc,
            c_partial.risk_budget_usdc.unwrap()
        );
        assert_eq!(t_partial.risk_budget_usdc, 30.0);
        assert_eq!(t_partial.realized_pnl_usdc, c_partial.realized_pnl_usdc);
        assert_eq!(t_partial.realized_pnl_usdc, 12.0);
        assert!(
            (t_partial.fees_paid_usdc - c_partial.fees_paid_usdc).abs() < 1e-9,
            "fees_paid_usdc must match between PIT and POND after partial exit"
        );
        let assert_step_pnl_heat_parity = |db_ref: &DatabaseManager, step: &str| {
            let pit_c = db_ref.compute_pit_pnl_components().unwrap();
            let pond_c = db_ref.compute_pond_pnl_components().unwrap();
            assert_eq!(
                pit_c.realized_pnl_usdc, pond_c.realized_pnl_usdc,
                "{step}: realized_pnl_usdc"
            );
            assert_eq!(
                pit_c.unrealized_pnl_usdc, pond_c.unrealized_pnl_usdc,
                "{step}: unrealized_pnl_usdc"
            );
            assert_eq!(
                pit_c.funding_pnl_usdc, pond_c.funding_pnl_usdc,
                "{step}: funding_pnl_usdc"
            );
            assert_eq!(
                pit_c.open_fees_usdc, pond_c.open_fees_usdc,
                "{step}: open_fees_usdc"
            );
            assert_eq!(
                pit_c.total_net_pnl_usdc, pond_c.total_net_pnl_usdc,
                "{step}: total_net_pnl_usdc"
            );
            assert_eq!(
                pit_c.book_equity_usdc, pond_c.book_equity_usdc,
                "{step}: book_equity_usdc"
            );
            assert_eq!(pit_c.heat_usdc, pond_c.heat_usdc, "{step}: heat_usdc");
        };
        assert_step_pnl_heat_parity(&db1, "step 1a (40% partial exit)");

        // 1b. Apply reconcile_open_trade_size / reconcile_open_pond_cell_size with Some(101.0) (6.0 -> 3.0, entry_price 100.0 -> 101.0)
        // With initial_stop = 95.0, risk_budget_usdc must recompute to |101.0 - 95.0| * 3.0 = 18.0 on both PIT and POND
        assert!(db1
            .reconcile_open_trade_size(pit_trade_id, 3.0, Some(101.0))
            .unwrap());
        assert!(db1
            .reconcile_open_pond_cell_size(pond_cell_id, 3.0, Some(101.0))
            .unwrap());

        let t_some = db1
            .get_open_trades()
            .unwrap()
            .into_iter()
            .find(|t| t.trade_id == pit_trade_id)
            .unwrap();
        let c_some = db1
            .get_open_pond_cells()
            .unwrap()
            .into_iter()
            .find(|c| c.cell_id == pond_cell_id)
            .unwrap();
        assert_eq!(t_some.quantity, c_some.quantity.unwrap());
        assert_eq!(t_some.entry_price, c_some.entry_price.unwrap());
        assert_eq!(t_some.notional_usdc, c_some.notional_usdc.unwrap());
        assert_eq!(t_some.notional_usdc, 303.0);
        assert_eq!(t_some.risk_budget_usdc, c_some.risk_budget_usdc.unwrap());
        assert_eq!(t_some.risk_budget_usdc, 18.0);
        assert_step_pnl_heat_parity(&db1, "step 1b (reconcile_open_*_size Some)");

        // 1c. Apply reconcile_open_trade_size / reconcile_open_pond_cell_size with None (3.0 -> 1.5, entry_price stays 101.0)
        // risk_budget_usdc must recompute to |101.0 - 95.0| * 1.5 = 9.0 on both PIT and POND
        assert!(db1
            .reconcile_open_trade_size(pit_trade_id, 1.5, None)
            .unwrap());
        assert!(db1
            .reconcile_open_pond_cell_size(pond_cell_id, 1.5, None)
            .unwrap());

        let t_none = db1
            .get_open_trades()
            .unwrap()
            .into_iter()
            .find(|t| t.trade_id == pit_trade_id)
            .unwrap();
        let c_none = db1
            .get_open_pond_cells()
            .unwrap()
            .into_iter()
            .find(|c| c.cell_id == pond_cell_id)
            .unwrap();
        assert_eq!(t_none.quantity, c_none.quantity.unwrap());
        assert_eq!(t_none.notional_usdc, c_none.notional_usdc.unwrap());
        assert_eq!(t_none.notional_usdc, 151.5);
        assert_eq!(t_none.risk_budget_usdc, c_none.risk_budget_usdc.unwrap());
        assert_eq!(t_none.risk_budget_usdc, 9.0);
        assert_step_pnl_heat_parity(&db1, "step 1c (reconcile_open_*_size None)");

        // 1d. Close both partially-exited twins at exit_price = 105.0 (final exit fee = 0.08):
        // Verify realized_pnl_usdc transitions cleanly from partial_realized_pnl_usdc into closed_realized_pnl_usdc without double counting
        let final_gross_delta = (105.0 - t_none.entry_price) * t_none.quantity.abs(); // (105 - 101) * 1.5 = 6.0
        let final_fee_delta = 0.08;
        let total_pit_fees = round_dp(t_none.fees_paid_usdc + final_fee_delta, 4); // 0.72 + 0.08 = 0.80
        let total_pit_net_pnl = round_dp(
            final_gross_delta + t_none.realized_pnl_usdc - total_pit_fees
                + t_none.funding_accrued_usdc,
            4,
        ); // 6.0 + 12.0 - 0.80 + 1.25 = 18.45
        assert!(db1
            .close_trade(
                pit_trade_id,
                105.0,
                total_pit_net_pnl,
                total_pit_fees,
                "TEST_CLOSE"
            )
            .unwrap());
        assert!(db1
            .close_open_pond_cell(
                pond_cell_id,
                "COOLDOWN",
                Some(95.0),
                Some(105.0),
                Some(100.0),
                105.0,
                final_gross_delta,
                final_fee_delta,
                Some("TEST_CLOSE"),
                "2026-11-10T12:00:00Z",
                "2026-11-10T16:00:00Z",
            )
            .unwrap());
        let pit_comp_after_close = db1.compute_pit_pnl_components().unwrap();
        let pond_comp_after_close = db1.compute_pond_pnl_components().unwrap();
        assert_eq!(pit_comp_after_close.partial_realized_pnl_usdc, 0.0);
        assert_eq!(pond_comp_after_close.partial_realized_pnl_usdc, 0.0);
        assert_eq!(pit_comp_after_close.closed_realized_pnl_usdc, 18.45);
        assert_eq!(pond_comp_after_close.closed_realized_pnl_usdc, 18.45);
        assert_eq!(
            pit_comp_after_close.realized_pnl_usdc,
            pond_comp_after_close.realized_pnl_usdc
        );
        assert_eq!(
            pit_comp_after_close.total_net_pnl_usdc,
            pond_comp_after_close.total_net_pnl_usdc
        );

        // =========================================================================
        // Family 2 & Family 3: PnL & Equity Accounting Twin Parity + UI Row Formula Parity
        // =========================================================================
        let db2 = DatabaseManager::memory();
        db2.initialize_schema(true).expect("seed schema db2");

        // Configure baseline reset timestamp so closed trades/cells before reset are excluded
        db2.set_config_value(
            "equity_baseline_reset_at",
            &serde_json::json!("2026-11-10T00:00:00Z"),
        )
        .unwrap();

        // Seed market candles for mark-to-market price resolution
        db2.upsert_ohlcv_batch(&[
            aegis_core::OhlcvBar {
                symbol: "BTC/USDC:USDC".to_string(),
                timeframe: "1h".to_string(),
                timestamp_ms: 1_794_312_000_000,
                open: 91_000.0,
                high: 92_000.0,
                low: 90_500.0,
                close: 91_500.0,
                volume: 100.0,
                funding_rate_hourly: 0.0001,
                is_synthetic: 0,
            },
            aegis_core::OhlcvBar {
                symbol: "ETH/USDC:USDC".to_string(),
                timeframe: "1h".to_string(),
                timestamp_ms: 1_794_312_000_000,
                open: 3_050.0,
                high: 3_150.0,
                low: 3_000.0,
                close: 3_100.0,
                volume: 500.0,
                funding_rate_hourly: -0.0001,
                is_synthetic: 0,
            },
        ])
        .unwrap();

        // (1) Completed book_transfers in both directions (PIT -> POND $300, POND -> PIT $120 => net PIT = -180, net POND = +180)
        db2.insert_book_transfer(&BookTransfer {
            transfer_id: 0,
            settle_date: "2026-11-10".to_string(),
            from_book: "PIT".to_string(),
            to_book: "POND".to_string(),
            amount_usdc: 300.0,
            pre_from_usdc: Some(5000.0),
            pre_to_usdc: Some(5000.0),
            post_from_usdc: Some(4700.0),
            post_to_usdc: Some(5300.0),
            drift_bps_before: Some(300.0),
            drift_bps_after: Some(0.0),
            blocked_remainder_usdc: 0.0,
            status: "COMPLETED".to_string(),
            attempts: 1,
            tx_ref: Some("TWIN_PARITY_TX_1".to_string()),
            created_at: "2026-11-10T01:00:00Z".to_string(),
        })
        .unwrap();
        db2.insert_book_transfer(&BookTransfer {
            transfer_id: 0,
            settle_date: "2026-11-11".to_string(),
            from_book: "POND".to_string(),
            to_book: "PIT".to_string(),
            amount_usdc: 120.0,
            pre_from_usdc: Some(5300.0),
            pre_to_usdc: Some(4700.0),
            post_from_usdc: Some(5180.0),
            post_to_usdc: Some(4820.0),
            drift_bps_before: Some(120.0),
            drift_bps_after: Some(0.0),
            blocked_remainder_usdc: 0.0,
            status: "COMPLETED".to_string(),
            attempts: 1,
            tx_ref: Some("TWIN_PARITY_TX_2".to_string()),
            created_at: "2026-11-10T02:00:00Z".to_string(),
        })
        .unwrap();

        // (2) Pre-reset closed PIT trade & POND cell (must be excluded by equity_baseline_reset_at)
        let pre_reset_pit_id = db2
            .insert_trade(&TradeRecord {
                trade_id: 0,
                client_order_id: "pit-closed-pre-reset".to_string(),
                symbol: "SOL/USDC:USDC".to_string(),
                base_asset: "SOL".to_string(),
                side: "LONG".to_string(),
                status: "CLOSED".to_string(),
                fsm_state: "FILLED".to_string(),
                entry_price: 100.0,
                current_price: 90.0,
                exit_price: Some(90.0),
                quantity: 5.0,
                filled_units: 5.0,
                avg_fill_price: Some(100.0),
                notional_usdc: 500.0,
                risk_budget_usdc: 25.0,
                conviction_multiplier: 1.0,
                initial_stop_price: 90.0,
                chandelier_stop_price: 90.0,
                current_stop_price: 90.0,
                highest_price_seen: 100.0,
                lowest_price_seen: 90.0,
                unrealized_pnl_usdc: 0.0,
                realized_pnl_usdc: -50.0,
                fees_paid_usdc: 0.5,
                funding_accrued_usdc: 0.0,
                slippage_paid_usd: 0.0,
                exchange_order_id: None,
                opened_at: "2026-11-09T01:00:00Z".to_string(),
                closed_at: Some("2026-11-09T05:00:00Z".to_string()),
                exit_reason: Some("CHANDELIER_STOP".to_string()),
            })
            .unwrap();
        let pre_reset_pond_id = db2
            .upsert_pond_cell(&PondCell {
                cell_id: 0,
                symbol: "SOL/USDC:USDC".to_string(),
                champion_id: 1,
                state: "CLOSED".to_string(),
                cycle_opened_id: None,
                side: "SHORT".to_string(),
                option_analogue: "SHORT_CALL".to_string(),
                entry_price: Some(100.0),
                quantity: Some(5.0),
                notional_usdc: Some(500.0),
                risk_budget_usdc: Some(25.0),
                conviction_multiplier: Some(1.0),
                funding_z_entry: Some(2.5),
                funding_rate_entry: Some(0.0001),
                initial_stop: Some(110.0),
                chandelier_stop: Some(110.0),
                highest_price_seen: Some(110.0),
                lowest_price_seen: Some(100.0),
                funding_accrued_usdc: 0.0,
                realized_pnl_usdc: -45.0,
                fees_paid_usdc: 0.4,
                exit_price: Some(109.0),
                exit_reason: Some("STOP_LOSS".to_string()),
                resting_order_id: None,
                opened_at: Some("2026-11-09T01:00:00Z".to_string()),
                closed_at: Some("2026-11-09T06:00:00Z".to_string()),
                cooldown_until: None,
                updated_at: "2026-11-09T06:00:00Z".to_string(),
            })
            .unwrap();

        // (3) Post-reset closed PIT trade & POND cell (included in realized PnL)
        db2.insert_trade(&TradeRecord {
            trade_id: 0,
            client_order_id: "pit-closed-post-reset".to_string(),
            symbol: "AVAX/USDC:USDC".to_string(),
            base_asset: "AVAX".to_string(),
            side: "LONG".to_string(),
            status: "CLOSED".to_string(),
            fsm_state: "FILLED".to_string(),
            entry_price: 25.0,
            current_price: 28.0,
            exit_price: Some(28.0),
            quantity: 10.0,
            filled_units: 10.0,
            avg_fill_price: Some(25.0),
            notional_usdc: 250.0,
            risk_budget_usdc: 15.0,
            conviction_multiplier: 1.0,
            initial_stop_price: 23.5,
            chandelier_stop_price: 27.0,
            current_stop_price: 27.0,
            highest_price_seen: 28.5,
            lowest_price_seen: 25.0,
            unrealized_pnl_usdc: 0.0,
            realized_pnl_usdc: 29.40,
            fees_paid_usdc: 0.60,
            funding_accrued_usdc: 0.15,
            slippage_paid_usd: 0.0,
            exchange_order_id: None,
            opened_at: "2026-11-10T03:00:00Z".to_string(),
            closed_at: Some("2026-11-10T06:00:00Z".to_string()),
            exit_reason: Some("TAKE_PROFIT".to_string()),
        })
        .unwrap();
        db2.upsert_pond_cell(&PondCell {
            cell_id: 0,
            symbol: "LINK/USDC:USDC".to_string(),
            champion_id: 1,
            state: "CLOSED".to_string(),
            cycle_opened_id: None,
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(15.0),
            quantity: Some(20.0),
            notional_usdc: Some(300.0),
            risk_budget_usdc: Some(20.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.6),
            funding_rate_entry: Some(0.0001),
            initial_stop: Some(16.0),
            chandelier_stop: Some(14.2),
            highest_price_seen: Some(15.0),
            lowest_price_seen: Some(13.8),
            funding_accrued_usdc: 0.85,
            realized_pnl_usdc: 18.20,
            fees_paid_usdc: 0.30,
            exit_price: Some(14.1),
            exit_reason: Some("TIER3_FUNDING_NORMALISATION".to_string()),
            resting_order_id: None,
            opened_at: Some("2026-11-10T03:00:00Z".to_string()),
            closed_at: Some("2026-11-10T07:00:00Z".to_string()),
            cooldown_until: None,
            updated_at: "2026-11-10T07:00:00Z".to_string(),
        })
        .unwrap();

        // (4) Open PIT trade & POND cell with partial-exit realized_pnl_usdc, fees_paid_usdc, and funding_accrued_usdc
        db2.insert_trade(&TradeRecord {
            trade_id: 0,
            client_order_id: "pit-open-btc".to_string(),
            symbol: "BTC/USDC:USDC".to_string(),
            base_asset: "BTC".to_string(),
            side: "LONG".to_string(),
            status: "OPEN".to_string(),
            fsm_state: "FILLED".to_string(),
            entry_price: 90_000.0,
            current_price: 91_500.0,
            exit_price: None,
            quantity: 0.02,
            filled_units: 0.02,
            avg_fill_price: Some(90_000.0),
            notional_usdc: 1_800.0,
            risk_budget_usdc: 40.0,
            conviction_multiplier: 1.0,
            initial_stop_price: 88_000.0,
            chandelier_stop_price: 89_500.0,
            current_stop_price: 89_500.0,
            highest_price_seen: 92_000.0,
            lowest_price_seen: 90_000.0,
            unrealized_pnl_usdc: 30.0,
            realized_pnl_usdc: 14.50,
            fees_paid_usdc: 0.85,
            funding_accrued_usdc: -1.10,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some("hl-btc-open".to_string()),
            opened_at: "2026-11-10T08:00:00Z".to_string(),
            closed_at: None,
            exit_reason: None,
        })
        .unwrap();
        db2.upsert_pond_cell(&PondCell {
            cell_id: 0,
            symbol: "ETH/USDC:USDC".to_string(),
            champion_id: 1,
            state: "TRAILED".to_string(),
            cycle_opened_id: None,
            side: "LONG".to_string(),
            option_analogue: "SHORT_PUT".to_string(),
            entry_price: Some(3_000.0),
            quantity: Some(0.5),
            notional_usdc: Some(1_500.0),
            risk_budget_usdc: Some(45.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(-2.4),
            funding_rate_entry: Some(-0.0001),
            initial_stop: Some(2_910.0),
            chandelier_stop: Some(2_980.0),
            highest_price_seen: Some(3_150.0),
            lowest_price_seen: Some(3_000.0),
            funding_accrued_usdc: 2.35,
            realized_pnl_usdc: 9.75,
            fees_paid_usdc: 0.65,
            exit_price: None,
            exit_reason: None,
            resting_order_id: None,
            opened_at: Some("2026-11-10T08:00:00Z".to_string()),
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-11-10T09:00:00Z".to_string(),
        })
        .unwrap();

        // (5) Resting orders on both books (PENDING PIT trade and ARMED Pond cell)
        db2.insert_trade(&TradeRecord {
            trade_id: 0,
            client_order_id: "pit-pending-sui".to_string(),
            symbol: "SUI/USDC:USDC".to_string(),
            base_asset: "SUI".to_string(),
            side: "LONG".to_string(),
            status: "PENDING".to_string(),
            fsm_state: "OPEN_ON_BOOK".to_string(),
            entry_price: 3.0,
            current_price: 3.0,
            exit_price: None,
            quantity: 100.0,
            filled_units: 0.0,
            avg_fill_price: None,
            notional_usdc: 300.0,
            risk_budget_usdc: 15.0,
            conviction_multiplier: 1.0,
            initial_stop_price: 2.85,
            chandelier_stop_price: 2.85,
            current_stop_price: 2.85,
            highest_price_seen: 3.0,
            lowest_price_seen: 3.0,
            unrealized_pnl_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            funding_accrued_usdc: 0.0,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some("hl-sui-resting".to_string()),
            opened_at: "2026-11-10T09:30:00Z".to_string(),
            closed_at: None,
            exit_reason: None,
        })
        .unwrap();
        db2.upsert_pond_cell(&PondCell {
            cell_id: 0,
            symbol: "DOGE/USDC:USDC".to_string(),
            champion_id: 1,
            state: "ARMED".to_string(),
            cycle_opened_id: None,
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(0.25),
            quantity: Some(1_000.0),
            notional_usdc: Some(250.0),
            risk_budget_usdc: Some(12.5),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.3),
            funding_rate_entry: Some(0.0001),
            initial_stop: Some(0.2625),
            chandelier_stop: Some(0.2625),
            highest_price_seen: Some(0.25),
            lowest_price_seen: Some(0.25),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            exit_price: None,
            exit_reason: None,
            resting_order_id: Some("hl-doge-resting".to_string()),
            opened_at: Some("2026-11-10T09:30:00Z".to_string()),
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-11-10T09:30:00Z".to_string(),
        })
        .unwrap();

        // (6) Non-zero accrued_yield_usdc on the latest portfolio_snapshots row
        db2.save_portfolio_snapshot(&PortfolioSnapshot {
            snapshot_id: None,
            timestamp_ms: 1_794_312_000_000,
            total_equity_usdc: 10_000.0,
            available_cash_usdc: 6_450.0,
            deployed_notional_usdc: 3_550.0,
            gross_leverage: 0.355,
            portfolio_heat_usdc: 80.0,
            portfolio_heat_pct: 0.008,
            basis_harvest_notional_usdc: 0.0,
            accrued_yield_usdc: 3.45,
            hmm_regime: "BULL_TREND".to_string(),
            prob_bull: 0.70,
            prob_bear: 0.10,
            prob_chop: 0.20,
            open_long_count: 1,
            open_short_count: 0,
            created_at: Some("2026-11-10T09:45:00Z".to_string()),
        })
        .unwrap();

        let verify_family_2_and_3 = |db_ref: &DatabaseManager, min_closed_ts: &str| {
            let cfg = db_ref.load_config().unwrap();
            let pit_init = cfg.get_f64("pit_initial_capital_usdc", 5000.0);
            let pond_init = cfg.get_f64("pond_initial_capital_usdc", 5000.0);
            let net_transfers_pit = db_ref.get_net_book_transfer_adjustment("PIT").unwrap();
            let net_transfers_pond = db_ref.get_net_book_transfer_adjustment("POND").unwrap();
            assert_eq!(net_transfers_pit, -180.0);
            assert_eq!(net_transfers_pond, 180.0);

            let (pit_eq, pit_idle) = db_ref.compute_pit_equity_and_idle_usdc().unwrap();
            let payload_pit = crate::payloads::build_portfolio_payload(db_ref).unwrap();
            let (pond_eq, pond_idle) = db_ref.compute_pond_equity_and_idle_usdc().unwrap();
            let payload_pond = crate::payloads::build_pond_payload(db_ref).unwrap();
            let pp50 = crate::payloads::build_pp50_payload(db_ref).unwrap();

            // Family 2 — Book A (PIT) Twin Parity
            assert_eq!(
                pit_eq,
                round_dp(
                    (pit_init + net_transfers_pit + payload_pit.total_pnl_usdc).max(0.0),
                    2
                ),
                "PIT equity must equal round_dp(pit_init + net_transfers_pit + payload_pit.total_pnl_usdc, 2)"
            );
            assert_eq!(
                pit_idle,
                round_dp((pit_eq - payload_pit.deployed_notional_usdc).max(0.0), 2),
                "PIT idle capital must equal round_dp(pit_eq - payload_pit.deployed_notional_usdc, 2)"
            );

            // Family 2 — Book B (POND) Twin Parity
            assert_eq!(
                pond_eq,
                round_dp(
                    (pond_init + net_transfers_pond + payload_pond.total_pnl_usdc).max(0.0),
                    2
                ),
                "POND equity must equal round_dp(pond_init + net_transfers_pond + payload_pond.total_pnl_usdc, 2)"
            );
            assert_eq!(
                pond_idle,
                round_dp((pond_eq - payload_pond.deployed_notional_usdc).max(0.0), 2),
                "POND idle capital must equal round_dp(pond_eq - payload_pond.deployed_notional_usdc, 2)"
            );

            // Family 2 — PP50 Combined Platform Parity
            assert_eq!(
                pp50.total_platform_equity_usdc,
                round_dp(pit_eq + pond_eq, 2),
                "PP50 total_platform_equity_usdc must equal round_dp(pit_eq + pond_eq, 2)"
            );
            assert_eq!(
                pp50.total_platform_pnl_usdc,
                round_dp(payload_pit.total_pnl_usdc + payload_pond.total_pnl_usdc, 4),
                "PP50 total_platform_pnl_usdc must equal round_dp(payload_pit.total_pnl_usdc + payload_pond.total_pnl_usdc, 4)"
            );

            // Family 3 — UI Row-Level PnL Formula Parity (R11-N2)
            let dash = build_dashboard_payload(db_ref).unwrap();
            let pit_open_rows_net_sum: f64 = dash
                .open_trades
                .iter()
                .filter(|t| t.status == "OPEN")
                .map(|t| {
                    t.unrealized_pnl_usdc + t.funding_accrued_usdc - t.fees_paid_usdc
                        + t.realized_pnl_usdc
                })
                .sum();
            let pit_closed_rows_since_reset_sum: f64 = dash
                .recent_trades
                .iter()
                .filter(|t| {
                    t.status == "CLOSED" && t.closed_at.as_deref().unwrap_or("") >= min_closed_ts
                })
                .map(|t| t.realized_pnl_usdc)
                .sum();
            assert_eq!(
                round_dp(
                    pit_open_rows_net_sum
                        + pit_closed_rows_since_reset_sum
                        + payload_pit.accrued_yield_usdc,
                    4
                ),
                payload_pit.total_pnl_usdc,
                "Sum of PIT UI row-level net PnL + closed realized PnL since reset + yield must equal payload_pit.total_pnl_usdc (R11-N2 / R11-QA)"
            );

            let pond_open_rows_net_sum: f64 = payload_pond
                .open_cells
                .iter()
                .filter(|c| matches!(c.cell.state.as_str(), "OPEN" | "TRAILED"))
                .map(|c| {
                    c.unrealized_pnl_usdc + c.cell.funding_accrued_usdc - c.cell.fees_paid_usdc
                        + c.cell.realized_pnl_usdc
                })
                .sum();
            let pond_closed_rows_since_reset_sum: f64 = payload_pond
                .recent_cells
                .iter()
                .filter(|c| {
                    matches!(c.cell.state.as_str(), "CLOSED" | "COOLDOWN")
                        && c.cell.closed_at.as_deref().unwrap_or("") >= min_closed_ts
                })
                .map(|c| c.cell.realized_pnl_usdc)
                .sum();
            assert_eq!(
                round_dp(pond_open_rows_net_sum + pond_closed_rows_since_reset_sum, 4),
                payload_pond.total_pnl_usdc,
                "Sum of POND UI row-level net PnL + closed realized PnL since reset must equal payload_pond.total_pnl_usdc (R11-QA)"
            );
        };

        // Pass A: With equity_baseline_reset_at = "2026-11-10T00:00:00Z" (pre-reset closed rows excluded)
        verify_family_2_and_3(&db2, "2026-11-10T00:00:00Z");

        // Pass B: Without equity_baseline_reset_at (verifying non-zero accrued_yield_usdc = 3.45 is included)
        db2.with_conn(|conn| {
            conn.execute(
                "DELETE FROM trades WHERE trade_id = ?1;",
                [pre_reset_pit_id],
            )
            .unwrap();
            conn.execute(
                "DELETE FROM pond_cells WHERE cell_id = ?1;",
                [pre_reset_pond_id],
            )
            .unwrap();
            Ok(())
        })
        .unwrap();
        db2.set_config_value("equity_baseline_reset_at", &serde_json::json!(""))
            .unwrap();
        assert_eq!(
            crate::payloads::build_portfolio_payload(&db2)
                .unwrap()
                .accrued_yield_usdc,
            3.45
        );
        verify_family_2_and_3(&db2, "");

        let app_js_src =
            std::fs::read_to_string(aegis_core::resolve_project_root().join("web/static/app.js"))
                .expect("read web/static/app.js");
        assert!(
            app_js_src.contains(
                "function computeRowPnl(row, isOpen = true) {\n  if (!row) return 0;\n  return isOpen\n    ? ((row.unrealized_pnl_usdc || 0) + (row.realized_pnl_usdc || 0) + (row.funding_accrued_usdc || 0) - (row.fees_paid_usdc || 0))\n    : (row.realized_pnl_usdc || 0);\n}"
            ),
            "web/static/app.js must define shared computeRowPnl(row, isOpen) helper (R11-N2, R11-QA, R12-QA)"
        );
        assert!(
            app_js_src.contains("const upnl = computeRowPnl(t, true);"),
            "web/static/app.js openTrades row must delegate to computeRowPnl(t, true) (R12-QA)"
        );
        assert!(
            app_js_src.contains("const pnl = computeRowPnl(t, tradeState === 'OPEN');"),
            "web/static/app.js recentTrades row must delegate to computeRowPnl(t, tradeState === 'OPEN') (R12-QA)"
        );
        assert!(
            app_js_src.contains("const netPnl = computeRowPnl(c, isOpen);"),
            "web/static/app.js recentCells row must delegate to computeRowPnl(c, isOpen) (R12-QA)"
        );
    }

    #[tokio::test]
    async fn test_r12_n1_api_health_staleness_guard_and_pond_mark_dedup() {
        let root = resolve_project_root();
        let db = DatabaseManager::memory();
        db.initialize_schema(false).expect("init schema");
        let now_ms = db.now_ms();

        let app = build_router(db.clone(), root.join("web/static"));

        // 1. In PAPER mode with no bars yet, /health returns 200 OK (status: "ok", data_fresh: false)
        let req_paper_empty = axum::http::Request::builder()
            .method("GET")
            .uri("/health")
            .header(header::HOST, "localhost")
            .body(Body::empty())
            .unwrap();
        let resp_paper_empty = app.clone().oneshot(req_paper_empty).await.unwrap();
        assert_eq!(resp_paper_empty.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp_paper_empty.into_body(), usize::MAX)
            .await
            .unwrap();
        let json_paper_empty: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json_paper_empty["status"], "ok");
        assert_eq!(json_paper_empty["data_fresh"], false);

        // 2. Insert a bar older than max_bar_staleness_ms (10h old > 8h). In PAPER mode, data_fresh only requires last_ms > 0 -> true, 200 OK.
        let stale_bar_ts = now_ms - 10 * 3_600_000;
        db.upsert_ohlcv_batch(&[aegis_core::OhlcvBar {
            symbol: "BTC/USDC:USDC".to_string(),
            timeframe: "1h".to_string(),
            timestamp_ms: stale_bar_ts,
            open: 90_000.0,
            high: 90_500.0,
            low: 89_500.0,
            close: 90_100.0,
            volume: 100.0,
            funding_rate_hourly: 0.0001,
            is_synthetic: 0,
        }])
        .unwrap();

        let req_paper_stale = axum::http::Request::builder()
            .method("GET")
            .uri("/api/v1/health")
            .header(header::HOST, "localhost")
            .body(Body::empty())
            .unwrap();
        let resp_paper_stale = app.clone().oneshot(req_paper_stale).await.unwrap();
        assert_eq!(resp_paper_stale.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp_paper_stale.into_body(), usize::MAX)
            .await
            .unwrap();
        let json_paper_stale: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json_paper_stale["status"], "ok");
        assert_eq!(json_paper_stale["data_fresh"], true);

        // 3. Switch to TESTNET mode -> 10h-old bar exceeds max_bar_staleness_ms (8h) -> 503 Service Unavailable, status: "DEGRADED"
        db.configure_hyperliquid_credentials(
            Some("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"),
            Some("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"),
            Some(true),
            Some("TESTNET"),
            None,
            None,
            false,
        )
        .unwrap();
        let req_testnet_stale = axum::http::Request::builder()
            .method("GET")
            .uri("/health")
            .header(header::HOST, "localhost")
            .body(Body::empty())
            .unwrap();
        let resp_testnet_stale = app.clone().oneshot(req_testnet_stale).await.unwrap();
        assert_eq!(resp_testnet_stale.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body_bytes = axum::body::to_bytes(resp_testnet_stale.into_body(), usize::MAX)
            .await
            .unwrap();
        let json_testnet_stale: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json_testnet_stale["status"], "DEGRADED");
        assert_eq!(json_testnet_stale["data_fresh"], false);

        // 4. Insert a fresh 1h bar (10m old) -> TESTNET /health returns 200 OK, status: "ok", data_fresh: true
        let fresh_bar_ts = now_ms - 600_000;
        db.upsert_ohlcv_batch(&[aegis_core::OhlcvBar {
            symbol: "BTC/USDC:USDC".to_string(),
            timeframe: "1h".to_string(),
            timestamp_ms: fresh_bar_ts,
            open: 90_100.0,
            high: 90_800.0,
            low: 90_000.0,
            close: 90_600.0,
            volume: 120.0,
            funding_rate_hourly: 0.0001,
            is_synthetic: 0,
        }])
        .unwrap();

        let req_testnet_fresh = axum::http::Request::builder()
            .method("GET")
            .uri("/api/v1/health")
            .header(header::HOST, "localhost")
            .body(Body::empty())
            .unwrap();
        let resp_testnet_fresh = app.clone().oneshot(req_testnet_fresh).await.unwrap();
        assert_eq!(resp_testnet_fresh.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp_testnet_fresh.into_body(), usize::MAX)
            .await
            .unwrap();
        let json_testnet_fresh: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json_testnet_fresh["status"], "ok");
        assert_eq!(json_testnet_fresh["data_fresh"], true);

        // 5. Verify R12-N3: compute_pond_pnl_components_with_marks populates mark_cache and enrich_pond_cells_with_marks reuses it
        let _ = db
            .upsert_pond_cell(&aegis_core::PondCell {
                cell_id: 0,
                symbol: "BTC/USDC:USDC".to_string(),
                champion_id: 1,
                state: "OPEN".to_string(),
                cycle_opened_id: None,
                side: "LONG".to_string(),
                option_analogue: "SHORT_PUT".to_string(),
                entry_price: Some(90_000.0),
                quantity: Some(0.01),
                notional_usdc: Some(900.0),
                risk_budget_usdc: Some(25.0),
                conviction_multiplier: Some(1.0),
                funding_z_entry: Some(-2.0),
                funding_rate_entry: Some(-0.0001),
                initial_stop: Some(87_500.0),
                chandelier_stop: Some(87_500.0),
                highest_price_seen: Some(90_600.0),
                lowest_price_seen: Some(90_000.0),
                funding_accrued_usdc: 0.0,
                realized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.0,
                exit_price: None,
                exit_reason: None,
                resting_order_id: None,
                opened_at: Some("2026-11-11T00:00:00Z".to_string()),
                closed_at: None,
                cooldown_until: None,
                updated_at: "2026-11-11T00:00:00Z".to_string(),
            })
            .unwrap();

        let (pond_pnl, mut mark_cache) = db.compute_pond_pnl_components_with_marks().unwrap();
        assert_eq!(mark_cache.get("BTC/USDC:USDC").copied(), Some(90_600.0));
        assert!((pond_pnl.unrealized_pnl_usdc - 6.0).abs() < 1e-6);

        // Override mark_cache value to prove enrich_pond_cells_with_marks reads from mark_cache without re-querying SQLite
        mark_cache.insert("BTC/USDC:USDC".to_string(), 91_000.0);
        let open_cells = db.get_open_pond_cells().unwrap();
        let enriched =
            crate::payloads::enrich_pond_cells_with_marks(&db, open_cells, &mut mark_cache);
        assert_eq!(enriched.len(), 1);
        assert_eq!(enriched[0].current_price, 91_000.0);
        assert!((enriched[0].unrealized_pnl_usdc - 10.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_db_backup_endpoint_and_login_rate_limiting_me13_me14_me19() {
        let root = resolve_project_root();
        let tmp_dir = std::env::temp_dir().join(format!(
            "aegis_web_backup_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&tmp_dir).unwrap();
        let db_path = tmp_dir.join("aegis_web_test.db");
        let db = DatabaseManager::new(&db_path);
        db.initialize_schema(true).expect("seed schema");
        db.set_config_value("web_api_token", &json!("test-backup-token"))
            .expect("set token");

        let app = build_router(db.clone(), root.join("web/static"));

        // 1. Trigger DB backup via POST /api/v1/db/backup
        let backup_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/db/backup")
            .header(header::AUTHORIZATION, "Bearer test-backup-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"reason":"dashboard_button_test"}"#))
            .unwrap();
        let backup_resp = app.clone().oneshot(backup_req).await.unwrap();
        assert_eq!(backup_resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(backup_resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let backup_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(backup_json["status"], "ok");
        assert!(backup_json["backup"]["size_bytes"].as_u64().unwrap_or(0) > 0);
        assert_eq!(backup_json["backup"]["retained_count"], 1);

        // 2. Verify session login sets Max-Age=86400 and logout revokes session cookie (ME-13)
        let login_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/auth/login")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"token":"test-backup-token"}"#))
            .unwrap();
        let login_resp = app.clone().oneshot(login_req).await.unwrap();
        assert_eq!(login_resp.status(), StatusCode::OK);
        let set_cookie = login_resp
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(set_cookie.contains("Max-Age=86400"));
        let cookie_pair = set_cookie.split(';').next().unwrap().to_string();

        // Logout with that session cookie revokes the server-side session nonce
        let logout_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/auth/logout")
            .header(header::COOKIE, &cookie_pair)
            .header("x-requested-with", "XMLHttpRequest")
            .body(Body::empty())
            .unwrap();
        let logout_resp = app.clone().oneshot(logout_req).await.unwrap();
        assert_eq!(logout_resp.status(), StatusCode::OK);

        // Subsequent request with the revoked session cookie must return 401
        let revoked_req = axum::http::Request::builder()
            .method("GET")
            .uri("/api/v1/status")
            .header(header::COOKIE, &cookie_pair)
            .body(Body::empty())
            .unwrap();
        let revoked_resp = app.clone().oneshot(revoked_req).await.unwrap();
        assert_eq!(revoked_resp.status(), StatusCode::UNAUTHORIZED);

        // 3. Verify 5 consecutive failed logins trigger 429 Too Many Requests on the 6th attempt (ME-14)
        for _ in 0..5 {
            let bad_req = axum::http::Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"token":"wrong-token"}"#))
                .unwrap();
            let bad_resp = app.clone().oneshot(bad_req).await.unwrap();
            assert_eq!(bad_resp.status(), StatusCode::UNAUTHORIZED);
        }
        let rate_limited_req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/auth/login")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"token":"wrong-token"}"#))
            .unwrap();
        let rate_limited_resp = app.oneshot(rate_limited_req).await.unwrap();
        assert_eq!(rate_limited_resp.status(), StatusCode::TOO_MANY_REQUESTS);

        // 4. Verify index.html and app.js contain the DB backup button and handler, and validate_config_update enforces immutable web_api_token + 16-char minimum token length (ME-14)
        assert!(crate::payloads::validate_config_update("web_api_token", "short").is_err());
        assert!(crate::payloads::validate_config_update("dashboard_api_token", "short").is_err());
        assert!(
            crate::payloads::validate_config_update("dashboard_api_token", "0123456789abcdef")
                .is_ok()
        );
        assert!(EMBEDDED_INDEX_HTML.contains("btn-backup-db"));
        assert!(EMBEDDED_INDEX_HTML.contains("btn-backup-db-settings"));
        assert!(EMBEDDED_APP_JS.contains("triggerDbBackup"));
        assert!(EMBEDDED_APP_JS.contains("/api/v1/db/backup"));
        assert!(EMBEDDED_APP_JS.contains("CONFIRM_LIVE_SETTLE"));

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[tokio::test]
    async fn test_me13_r15_n7_cookie_rejects_raw_master_token_and_table3_web_security() {
        let root = resolve_project_root();
        let db = DatabaseManager::memory();
        db.initialize_schema(false).expect("init schema");
        db.set_config_value("web_api_token", &json!("master-web-secret-123456"))
            .unwrap();
        let app = build_router(db.clone(), root.join("web/static"));

        // ME-13 / R15-N7: Raw master token inside `aegis_session` cookie MUST be rejected (401)
        let raw_cookie_req = axum::http::Request::builder()
            .method("GET")
            .uri("/api/v1/status")
            .header(header::HOST, "localhost")
            .header(header::COOKIE, "aegis_session=master-web-secret-123456")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(raw_cookie_req).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        // Table 3 #7: 192.168.122.254 removed from hardcoded CORS allowlist, allowed only when configured
        assert!(!is_allowed_cors_origin("http://192.168.122.254:8080", None));
        assert!(is_allowed_cors_origin(
            "http://192.168.122.254:8080",
            Some("http://192.168.122.254:8080")
        ));

        // Table 3 #6: ?token= on /api/v1/stream is disabled by default and gated behind web_allow_query_token_sse
        let sse_state = AppState {
            db: db.clone(),
            static_dir: root.join("web/static"),
            cycle_lock: Arc::new(Mutex::new(())),
            session_token: "master-web-secret-123456".to_string(),
            auth_cache: Arc::new(parking_lot::Mutex::new(None)),
            active_sessions: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
            login_failures: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
            sse_payload_cache: Arc::new(parking_lot::Mutex::new(None)),
            last_manual_backup_at: Arc::new(parking_lot::Mutex::new(None)),
            restore_in_progress: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            pending_restore_tokens: Arc::new(parking_lot::Mutex::new(
                std::collections::HashMap::new(),
            )),
        };
        let sse_req_denied = axum::http::Request::builder()
            .method("GET")
            .uri("/api/v1/stream?token=master-web-secret-123456")
            .body(axum::body::Body::empty())
            .unwrap();
        assert!(!is_request_authorized(&sse_state, &sse_req_denied));

        db.set_config_value("web_allow_query_token_sse", &json!(true))
            .unwrap();
        sse_state.invalidate_auth_cache();
        let sse_req_allowed = axum::http::Request::builder()
            .method("GET")
            .uri("/api/v1/stream?token=master-web-secret-123456")
            .body(axum::body::Body::empty())
            .unwrap();
        assert!(is_request_authorized(&sse_state, &sse_req_allowed));

        // Table 3 #8: web_auth_enabled=false is ignored while execution_mode or pond_execution_mode is LIVE/TESTNET
        db.set_config_value("web_auth_enabled", &json!(false))
            .unwrap();
        db.set_config_value("execution_mode", &json!("TESTNET"))
            .unwrap();
        let unauth_testnet_req = axum::http::Request::builder()
            .method("GET")
            .uri("/api/v1/status")
            .header(header::HOST, "localhost")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(unauth_testnet_req).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn test_me14_me17_r15_n1_n6_n12_table3_web_handlers() {
        // ME-14 / R15-N6: Proxy header trust gate + failure map pruning & 4096 cap
        let mut hdrs = axum::http::HeaderMap::new();
        hdrs.insert("x-forwarded-for", "203.0.113.5, 10.0.0.1".parse().unwrap());
        assert_eq!(extract_client_ip_key(&hdrs, false), "local-direct");
        assert_eq!(extract_client_ip_key(&hdrs, true), "203.0.113.5");

        let mut map = std::collections::HashMap::new();
        let now = std::time::Instant::now();
        for i in 0..4100 {
            map.insert(format!("ip-{i}"), (1u32, now));
        }
        prune_and_cap_login_failures(&mut map, now, std::time::Duration::from_secs(60), 4096);
        assert!(map.len() < 4096);

        // R15-N1, R15-N12, Table 3 #9, ME-17
        let root = resolve_project_root();
        let tmp_dir = std::env::temp_dir().join(format!(
            "aegis_web_r15_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&tmp_dir).unwrap();
        let db = DatabaseManager::new(tmp_dir.join("web_r15.db"));
        db.initialize_schema(false).unwrap();
        db.set_config_value("web_api_token", &json!("r15-web-test-token-123"))
            .unwrap();
        // Delete pond_execution_mode to test R15-N1 fallback to execution_mode
        db.with_conn(|c| {
            c.execute(
                "DELETE FROM system_config WHERE config_key = 'pond_execution_mode';",
                [],
            )
            .map_err(|e| aegis_core::AegisError::Database(e.to_string()))?;
            Ok(())
        })
        .unwrap();
        db.set_config_value("execution_mode", &json!("TESTNET"))
            .unwrap();

        let app = build_router(db.clone(), root.join("web/static"));

        let health_resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("GET")
                    .uri("/health")
                    .header(header::HOST, "localhost")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let health_bytes = axum::body::to_bytes(health_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let health_json: serde_json::Value = serde_json::from_slice(&health_bytes).unwrap();
        assert_eq!(health_json["max_bar_staleness_ms"], 28_800_000);

        let status_resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("GET")
                    .uri("/api/v1/status")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r15-web-test-token-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_resp.status(), StatusCode::OK);
        let status_bytes = axum::body::to_bytes(status_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let status_json: serde_json::Value = serde_json::from_slice(&status_bytes).unwrap();
        assert_eq!(status_json["pond_execution_mode"], "TESTNET");

        // Table 3 #9: GET /api/v1/config succeeds via readonly query
        let cfg_resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("GET")
                    .uri("/api/v1/config")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r15-web-test-token-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cfg_resp.status(), StatusCode::OK);

        // R15-N12: 1st POST /api/v1/db/backup returns lightweight payload; 2nd within 60s returns 429
        let b1 = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/db/backup")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r15-web-test-token-123")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"reason":"r15_test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(b1.status(), StatusCode::OK);
        let b1_bytes = axum::body::to_bytes(b1.into_body(), usize::MAX)
            .await
            .unwrap();
        let b1_json: serde_json::Value = serde_json::from_slice(&b1_bytes).unwrap();
        assert!(b1_json.get("dashboard").is_none());

        let b2 = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/db/backup")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r15-web-test-token-123")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"reason":"r15_test_2"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(b2.status(), StatusCode::TOO_MANY_REQUESTS);

        // ME-17: Core SSE payload hash stability
        let mut p1 = build_dashboard_payload(&db).unwrap();
        let h1 = compute_sse_core_hash(&p1);
        assert_eq!(h1, compute_sse_core_hash(&p1));
        p1.portfolio.total_equity_usdc += 123.45;
        assert_ne!(h1, compute_sse_core_hash(&p1));

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[tokio::test]
    async fn test_r16_u1_web_backups_list_and_two_step_guarded_restore() {
        let root = resolve_project_root();
        let tmp_dir = std::env::temp_dir().join(format!(
            "aegis_web_r16_u1_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&tmp_dir).unwrap();
        let db = DatabaseManager::new(tmp_dir.join("web_r16_u1.db"));
        db.initialize_schema(false).unwrap();
        db.set_config_value("web_api_token", &json!("r16-u1-web-test-token-1234"))
            .unwrap();
        db.set_config_value("settling_deadband_bps", &json!(65))
            .unwrap();

        let app = build_router(db.clone(), root.join("web/static"));

        // 1. Create a backup via POST /api/v1/db/backup
        let create_resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/db/backup")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r16-u1-web-test-token-1234")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"reason":"r16_u1_snapshot"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_resp.status(), StatusCode::OK);

        // 2. List backups via GET /api/v1/db/backups
        let list_resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("GET")
                    .uri("/api/v1/db/backups")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r16-u1-web-test-token-1234")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_resp.status(), StatusCode::OK);
        let list_bytes = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let list_json: serde_json::Value = serde_json::from_slice(&list_bytes).unwrap();
        assert_eq!(list_json["status"], "ok");
        assert_eq!(list_json["restore_allowed"], true);
        assert!(list_json["disabled_reasons"].as_array().unwrap().is_empty());
        let backups = list_json["backups"].as_array().unwrap();
        assert_eq!(backups.len(), 1);
        let backup_filename = backups[0]["filename"].as_str().unwrap().to_string();
        assert!(backups[0]["size_bytes"].as_u64().unwrap() > 0);
        assert!(backups[0]["restore_cli_command"]
            .as_str()
            .unwrap()
            .contains(&backup_filename));

        // 3. Path traversal rejection (400) & missing file (404)
        let trav_resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/db/restore")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r16-u1-web-test-token-1234")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"filename":"../evil.db","confirm":false}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(trav_resp.status(), StatusCode::BAD_REQUEST);

        let missing_resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/db/restore")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r16-u1-web-test-token-1234")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"filename":"aegis_quant_99999999_000000_000.db","confirm":false}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_resp.status(), StatusCode::NOT_FOUND);

        // 4. LIVE-mode refusal (409 Conflict)
        db.set_config_value("execution_mode", &json!("LIVE"))
            .unwrap();
        let live_refusal = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/db/restore")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r16-u1-web-test-token-1234")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(
                        r#"{{"filename":"{backup_filename}","confirm":false}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(live_refusal.status(), StatusCode::CONFLICT);
        db.set_config_value("execution_mode", &json!("PAPER"))
            .unwrap();

        // 5. Open-trade refusal (409 Conflict)
        db.reconcile_on_chain_orphan_position(
            "SOL/USDC:USDC",
            "SOL",
            "LONG",
            1.0,
            100.0,
            95.0,
            5.0,
        )
        .unwrap();
        let open_trade_refusal = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/db/restore")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r16-u1-web-test-token-1234")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(
                        r#"{{"filename":"{backup_filename}","confirm":false}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(open_trade_refusal.status(), StatusCode::CONFLICT);
        db.with_conn(|c| {
            c.execute("DELETE FROM trades;", [])
                .map_err(|e| aegis_core::AegisError::Database(e.to_string()))?;
            Ok(())
        })
        .unwrap();

        // 6. Mutate live config so Step 2 restore can be verified
        db.set_config_value("settling_deadband_bps", &json!(95))
            .unwrap();
        assert_eq!(
            db.load_config()
                .unwrap()
                .get_i64("settling_deadband_bps", 0),
            95
        );

        // 7. Step 1 preview mints single-use restore_token
        let step1_resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/db/restore")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r16-u1-web-test-token-1234")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(
                        r#"{{"filename":"{backup_filename}","confirm":false}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(step1_resp.status(), StatusCode::OK);
        let step1_bytes = axum::body::to_bytes(step1_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let step1_json: serde_json::Value = serde_json::from_slice(&step1_bytes).unwrap();
        assert_eq!(step1_json["status"], "preview");
        assert_eq!(step1_json["expires_in_seconds"], 60);
        assert_eq!(step1_json["preview"]["integrity_check"], "ok");
        let restore_token = step1_json["restore_token"].as_str().unwrap().to_string();
        assert_eq!(restore_token.len(), 64);

        // 8. Step 2 without typed "RESTORE" is rejected with 400 without burning the token
        let bad_confirm_resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/db/restore")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r16-u1-web-test-token-1234")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(
                        r#"{{"filename":"{backup_filename}","confirm":true,"restore_token":"{restore_token}","confirm_text":"wrong"}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_confirm_resp.status(), StatusCode::BAD_REQUEST);

        // 9. Step 2 with valid token + confirm_text == "RESTORE" restores DB & writes DB_RESTORE_COMPLETED audit log
        let step2_resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/db/restore")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r16-u1-web-test-token-1234")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(
                        r#"{{"filename":"{backup_filename}","confirm":true,"restore_token":"{restore_token}","confirm_text":"RESTORE"}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(step2_resp.status(), StatusCode::OK);
        let step2_bytes = axum::body::to_bytes(step2_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let step2_json: serde_json::Value = serde_json::from_slice(&step2_bytes).unwrap();
        assert_eq!(step2_json["status"], "restored");
        assert_eq!(
            step2_json["schema_version"],
            aegis_db::CANONICAL_SCHEMA_VERSION
        );
        assert!(step2_json["pre_restore_snapshot"]
            .as_str()
            .unwrap()
            .contains("aegis_quant_pre_restore_"));

        // Verify config value was restored to 65
        assert_eq!(
            db.load_config()
                .unwrap()
                .get_i64("settling_deadband_bps", 0),
            65
        );

        // Verify single-use token replay is rejected (400)
        let replay_resp = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/db/restore")
                    .header(header::HOST, "localhost")
                    .header(header::AUTHORIZATION, "Bearer r16-u1-web-test-token-1234")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(
                        r#"{{"filename":"{backup_filename}","confirm":true,"restore_token":"{restore_token}","confirm_text":"RESTORE"}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replay_resp.status(), StatusCode::BAD_REQUEST);

        // Verify DB_RESTORE_COMPLETED audit log entry
        let logs = db.get_recent_logs(20).unwrap();
        assert!(
            logs.iter().any(|l| {
                l.get("component").and_then(|v| v.as_str()) == Some("web.restore")
                    && l.get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .contains("DB_RESTORE_COMPLETED")
            }),
            "telemetry_logs must contain DB_RESTORE_COMPLETED entry from web.restore"
        );

        assert!(EMBEDDED_INDEX_HTML.contains("Database Restore Confirmation Modal"));
        assert!(EMBEDDED_INDEX_HTML.contains("Refresh Backups"));
        assert!(EMBEDDED_APP_JS.contains("es.addEventListener('heartbeat'"));
        assert!(EMBEDDED_APP_JS.contains("/api/v1/db/backups"));
        assert!(EMBEDDED_APP_JS.contains("/api/v1/db/restore"));

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
