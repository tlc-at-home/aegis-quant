//! Web API authentication, bearer token verification, and security header middleware (`aegis-web::auth`).

use crate::payloads::ApiErrorBody;
use crate::server::AppState;
use axum::{
    extract::{Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

pub const DEFAULT_CSP_POLICY: &str = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthLoginPayload {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthLoginResponse {
    pub status: &'static str,
    pub authenticated: bool,
}

/// Constant-time string comparison for bearer tokens.
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    if a_bytes.len() != b_bytes.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a_bytes.iter().zip(b_bytes.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Extracts `aegis_session` token from HTTP `Cookie` header if present.
pub fn extract_cookie_token(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    for part in raw.split(';') {
        let trimmed = part.trim();
        if let Some(val) = trimmed.strip_prefix("aegis_session=") {
            let v = val.trim();
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Extracts bearer token from `Authorization: Bearer <token>`, `X-Aegis-Token`, or `aegis_session` cookie.
pub fn extract_request_token(headers: &HeaderMap) -> Option<String> {
    if let Some(auth_val) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(token) = auth_val
            .trim()
            .strip_prefix("Bearer ")
            .or_else(|| auth_val.trim().strip_prefix("bearer "))
        {
            let t = token.trim();
            if !t.is_empty() {
                return Some(t.to_string());
            }
        }
    }
    if let Some(x_tok) = headers.get("x-aegis-token").and_then(|v| v.to_str().ok()) {
        let t = x_tok.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    extract_cookie_token(headers)
}

/// Security headers & optional bearer authentication middleware.
/// When `web_auth_enabled` is explicitly `"true"` AND a non-empty `web_api_token` is configured
/// AND `web_require_auth` is `"true"`, enforces token authentication on mutating `/api/v1/` endpoints.
pub async fn security_and_auth_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    if state.require_bearer_auth
        && path.starts_with("/api/v1/")
        && path != "/api/v1/health"
        && path != "/api/v1/auth/login"
    {
        if let Some(ref expected) = state.configured_api_token {
            if !expected.is_empty() && matches!(method, Method::POST | Method::PUT | Method::DELETE)
            {
                let provided = extract_request_token(req.headers());
                let valid = provided
                    .as_deref()
                    .map(|p| {
                        constant_time_eq(p, expected) || constant_time_eq(p, &state.session_token)
                    })
                    .unwrap_or(false);
                if !valid {
                    return (
                        StatusCode::UNAUTHORIZED,
                        Json(ApiErrorBody {
                            error: "Unauthorized".to_string(),
                            detail: "Valid Bearer token or X-Aegis-Token required".to_string(),
                        }),
                    )
                        .into_response();
                }
            }
        }
    }

    let mut resp = next.run(req).await;
    let headers = resp.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(DEFAULT_CSP_POLICY),
    );
    resp
}
