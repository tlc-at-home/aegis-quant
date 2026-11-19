//! Web API authentication, CSRF origin checks, bearer token verification, and security header middleware (`aegis-web::auth`).

use crate::payloads::ApiErrorBody;
use crate::server::AppState;
use axum::{
    extract::{Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

pub const DEFAULT_CSP_POLICY: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthLoginPayload {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthLoginResponse {
    pub status: &'static str,
    pub authenticated: bool,
}

/// Constant-time string comparison for bearer tokens without early length leak.
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    let max_len = a_bytes.len().max(b_bytes.len());
    let mut diff: u8 = if a_bytes.len() == b_bytes.len() { 0 } else { 1 };
    for i in 0..max_len {
        let x = *a_bytes.get(i).unwrap_or(&0);
        let y = *b_bytes.get(i).unwrap_or(&0);
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

/// Extracts bearer token from `Authorization: Bearer <token>` or `X-Aegis-Token` header.
pub fn extract_header_token(headers: &HeaderMap) -> Option<String> {
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
    None
}

/// Extracts bearer token from `Authorization: Bearer <token>`, `X-Aegis-Token`, or `aegis_session` cookie.
pub fn extract_request_token(headers: &HeaderMap) -> Option<String> {
    extract_header_token(headers).or_else(|| extract_cookie_token(headers))
}

fn extract_origin_authority(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("null")
        || trimmed.contains('@')
        || trimmed.chars().any(char::is_whitespace)
    {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    let rest = lower
        .strip_prefix("http://")
        .or_else(|| lower.strip_prefix("https://"))?;
    let auth = rest.split('/').next()?.trim();
    if auth.is_empty() {
        return None;
    }
    Some(auth.to_string())
}

/// Validates CSRF protections on mutating API routes (`POST`/`PUT`/`DELETE`/`PATCH`):
/// - If `Origin` or `Referer` is present, its authority must exactly match `Host`.
/// - If cookie auth is used without `Origin`/`Referer`, requires `X-Requested-With` or header Bearer token.
pub fn validate_csrf_for_mutation(
    headers: &HeaderMap,
    uri: &Uri,
) -> std::result::Result<(), &'static str> {
    let host_opt = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .or_else(|| uri.authority().map(|a| a.as_str().to_ascii_lowercase()));

    if let Some(origin_hv) = headers.get(header::ORIGIN) {
        let origin_str = origin_hv
            .to_str()
            .map_err(|_| "CSRF rejected: invalid Origin header")?;
        let origin_auth = extract_origin_authority(origin_str)
            .ok_or("CSRF rejected: malformed or null Origin header")?;
        return match host_opt {
            Some(ref host) if origin_auth == *host => Ok(()),
            _ => Err("CSRF rejected: Origin does not match Host"),
        };
    }

    if let Some(referer_hv) = headers.get(header::REFERER) {
        let referer_str = referer_hv
            .to_str()
            .map_err(|_| "CSRF rejected: invalid Referer header")?;
        let ref_auth = extract_origin_authority(referer_str)
            .ok_or("CSRF rejected: malformed Referer header")?;
        return match host_opt {
            Some(ref host) if ref_auth == *host => Ok(()),
            _ => Err("CSRF rejected: Referer does not match Host"),
        };
    }

    if extract_cookie_token(headers).is_some() {
        let has_xhr = headers
            .get("x-requested-with")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|s| !s.trim().is_empty());
        if !has_xhr && extract_header_token(headers).is_none() {
            return Err("CSRF rejected: cookie-authenticated mutation requires Origin/Host match or X-Requested-With");
        }
    }

    Ok(())
}

/// Security headers, CSRF validation, and unified bearer/cookie authentication middleware.
/// When `require_bearer_auth` is enabled and `web_api_token` is configured, requires valid auth
/// on ALL `/api/v1/*` routes (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`) except `/api/v1/health` and `/api/v1/auth/login`.
pub async fn security_and_auth_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();
    let is_mutation = matches!(
        method,
        Method::POST | Method::PUT | Method::DELETE | Method::PATCH
    );

    if path.starts_with("/api/v1/") && is_mutation {
        if let Err(csrf_reason) = validate_csrf_for_mutation(req.headers(), req.uri()) {
            return (
                StatusCode::FORBIDDEN,
                Json(ApiErrorBody {
                    error: "Forbidden".to_string(),
                    detail: csrf_reason.to_string(),
                }),
            )
                .into_response();
        }
    }

    if state.require_bearer_auth
        && path.starts_with("/api/v1/")
        && path != "/api/v1/health"
        && path != "/api/v1/auth/login"
    {
        if let Some(ref expected) = state.configured_api_token {
            if !expected.is_empty() {
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
                            detail: "Valid Bearer token, X-Aegis-Token, or session cookie required"
                                .to_string(),
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
