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
    pub db_token: Option<String>,
    pub web_cors_allowed_origins: Option<String>,
    pub web_allowed_hosts: String,
}

#[derive(Clone)]
pub struct AppState {
    pub db: DatabaseManager,
    pub static_dir: PathBuf,
    pub cycle_lock: Arc<Mutex<()>>,
    pub session_token: String,
    pub auth_cache: Arc<parking_lot::Mutex<Option<CachedWebAuthConfig>>>,
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
        let db_cfg = self.db.load_config_logged("aegis-web.auth_cache");
        let ttl_ms = db_cfg
            .get_i64("web_auth_cache_ttl_ms", 2000)
            .clamp(0, 60_000);
        let web_auth_enabled = db_cfg.get_bool("web_auth_enabled", true);
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

fn extract_cookie_token(headers: &axum::http::HeaderMap, cookie_name: &str) -> Option<String> {
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
        "http://192.168.122.254",
        "https://192.168.122.254",
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
        valid_tokens
            .iter()
            .any(|expected| constant_time_eq(cand, expected))
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

    if path == "/api/v1/stream" || path == "/api/stream" {
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
    };

    Router::new()
        .route("/", get(serve_index))
        .route("/static/app.js", get(serve_app_js))
        .route("/app.js", get(serve_app_js))
        .route("/api/v1/auth/login", post(api_auth_login))
        .route("/api/auth/login", post(api_auth_login))
        .route("/api/v1/auth/logout", post(api_auth_logout))
        .route("/api/auth/logout", post(api_auth_logout))
        .route("/api/v1/health", get(api_health))
        .route("/api/v1/status", get(api_status))
        .route("/api/status", get(api_status))
        .route("/api/v1/dashboard", get(api_dashboard))
        .route("/api/v1/portfolio", get(api_portfolio))
        .route("/api/v1/pond", get(api_pond))
        .route("/api/v1/pond/universe", get(api_pond_universe))
        .route("/api/v1/pond/cells", get(api_pond_cells))
        .route("/api/v1/pond/cycles", get(api_pond_cycles))
        .route("/api/v1/pond/scan", post(api_trigger_pond_scan))
        .route("/api/v1/settle", get(api_settle))
        .route("/api/v1/settle/run", post(api_settle_run))
        .route("/api/v1/signals", get(api_signals))
        .route("/api/v1/trades", get(api_trades))
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
        .route("/api/v1/stream", get(api_stream))
        // Legacy aliases for backwards compatibility
        .route("/api/dashboard", get(api_dashboard))
        .route("/api/portfolio", get(api_portfolio))
        .route("/api/pond", get(api_pond))
        .route("/api/pond/scan", post(api_trigger_pond_scan))
        .route("/api/settle", get(api_settle))
        .route("/api/settle/run", post(api_settle_run))
        .route("/api/signals", get(api_signals))
        .route("/api/trades", get(api_trades))
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
    let app = build_router(db, static_dir);
    let addr = format!("{host}:{port}");
    tracing::info!("Aegis-Quant PP-50 Rust Web Server listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// Entrypoint invoked by `aegis-cli` (`aegis-quant`) to serve the embedded/static WebUI & REST/SSE API.
pub async fn serve_web(db: DatabaseManager, host: &str, port: u16) -> Result<()> {
    let static_dir = resolve_project_root().join("web").join("static");
    run_web_server(db, static_dir, host, port).await
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
            assert!(html.contains(id), "index.html missing expected marker: {id}");
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
        assert_eq!(payload.schema_version, 8);
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
        assert_eq!(payload.schema_version, 8);

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
        assert_eq!(
            idx_resp.headers().get("x-frame-options").unwrap(),
            "DENY"
        );
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

        let secret_tg = "123456789:AAH_super_secret_telegram_bot_token_value";
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
                !m.contains(secret_pk) && !m.contains(secret_tg),
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
    }
}
