//! Web API authentication, CSRF origin checks, trusted-proxy extraction, and strict CSP middleware (`aegis-web::auth`).

use crate::payloads::ApiErrorBody;
use crate::server::{hash_session_token, normalize_host_without_port, AppState, ClientIpKey};
use axum::{
    extract::{ConnectInfo, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};

/// Strict Content Security Policy removing Google Fonts and all `'unsafe-inline'` directives (`R14-L17`).
pub const DEFAULT_CSP_POLICY: &str = "default-src 'self'; script-src 'self'; style-src 'self'; style-src-attr 'none'; font-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'; object-src 'none'";

/// Request extension recording whether the direct TCP peer belongs to `web_trusted_proxies` (`R14-M27`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerTrustedProxy(pub bool);

/// Request extension capturing how the caller authenticated for continuous SSE session checks (`R14-L15`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthenticatedPrincipal {
    UnauthenticatedAllowed,
    BearerToken,
    SessionCookie { token_hash: [u8; 32] },
}

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

fn with_security_headers(mut resp: Response) -> Response {
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

pub(crate) fn auth_error_response(status: StatusCode, detail: impl Into<String>) -> Response {
    (
        status,
        Json(ApiErrorBody {
            error: status.canonical_reason().unwrap_or("Error").to_string(),
            detail: detail.into(),
            correlation_id: None,
        }),
    )
        .into_response()
}

pub(crate) fn too_many_requests_response(retry_after_secs: u64) -> Response {
    let mut resp = auth_error_response(
        StatusCode::TOO_MANY_REQUESTS,
        format!("Too many failed authentication attempts. Retry after {retry_after_secs}s"),
    );
    if let Ok(hv) = HeaderValue::from_str(&retry_after_secs.to_string()) {
        resp.headers_mut().insert(header::RETRY_AFTER, hv);
    }
    resp
}

/// Extracts client IP using right-to-left `X-Forwarded-For` traversal ONLY when the direct TCP
/// peer IP matches `web_trusted_proxies` (`R14-M27`).
fn extract_client_ip(state: &AppState, req: &Request) -> (ClientIpKey, PeerTrustedProxy) {
    let peer_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip());
    let peer_is_trusted_proxy = peer_ip.is_some_and(|ip| state.is_trusted_proxy(ip));

    if peer_is_trusted_proxy {
        if let Some(xff) = req
            .headers()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
        {
            for raw_hop in xff.rsplit(',') {
                let hop_str = raw_hop.trim();
                if hop_str.is_empty() {
                    continue;
                }
                match hop_str.parse::<IpAddr>() {
                    Ok(hop_ip) => {
                        if !state.is_trusted_proxy(hop_ip) {
                            return (ClientIpKey::Ip(hop_ip), PeerTrustedProxy(true));
                        }
                    }
                    Err(_) => {
                        // Malformed client hop -> fail closed to peer_ip
                        break;
                    }
                }
            }
        }
        if let Some(xri) = req.headers().get("x-real-ip").and_then(|v| v.to_str().ok()) {
            if let Ok(ip) = xri.trim().parse::<IpAddr>() {
                if !state.is_trusted_proxy(ip) {
                    return (ClientIpKey::Ip(ip), PeerTrustedProxy(true));
                }
            }
        }
    }

    (
        peer_ip.map(ClientIpKey::Ip).unwrap_or(ClientIpKey::Local),
        PeerTrustedProxy(peer_is_trusted_proxy),
    )
}

fn validate_api_host_header(
    state: &AppState,
    req: &Request,
) -> std::result::Result<(), &'static str> {
    let host_count = req.headers().get_all(header::HOST).iter().count();
    if host_count > 1 {
        return Err("Host rejected: multiple Host headers are not permitted");
    }
    if host_count == 1 {
        let host_raw = req
            .headers()
            .get(header::HOST)
            .and_then(|v| v.to_str().ok())
            .ok_or("Host rejected: invalid Host header encoding")?;
        let host_no_port =
            normalize_host_without_port(host_raw).ok_or("Host rejected: malformed Host header")?;
        if !state.is_trusted_host(&host_no_port) {
            return Err("Host rejected: untrusted Host header");
        }
        return Ok(());
    }

    // Host absent: allow ONLY when neither Origin nor Referer is present (preserves header-less oneshot tests).
    if req.headers().contains_key(header::ORIGIN) || req.headers().contains_key(header::REFERER) {
        return Err("Host rejected: Origin or Referer requires a valid Host header");
    }
    if let Some(auth) = req.uri().authority() {
        let valid_auth =
            normalize_host_without_port(auth.as_str()).is_some_and(|h| state.is_trusted_host(&h));
        if !valid_auth {
            return Err("Host rejected: untrusted request URI authority");
        }
    }
    Ok(())
}

/// Security headers, Host allowlist, CSRF validation, rate limiting, and separated bearer/cookie authentication middleware.
/// When `require_bearer_auth` is enabled, requires valid auth on ALL `/api/v1/*` routes except `/api/v1/health`, `/api/v1/auth/login`, and `/api/v1/auth/logout`.
pub async fn security_and_auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    let (client_ip, peer_trusted_proxy) = extract_client_ip(&state, &req);
    req.extensions_mut().insert(client_ip);
    req.extensions_mut().insert(peer_trusted_proxy);

    let path = req.uri().path().to_string();
    let method = req.method().clone();
    let is_mutation = matches!(
        method,
        Method::POST | Method::PUT | Method::DELETE | Method::PATCH
    );

    if path.starts_with("/api/v1/") {
        if let Err(host_reason) = validate_api_host_header(&state, &req) {
            return with_security_headers(auth_error_response(StatusCode::FORBIDDEN, host_reason));
        }

        if is_mutation {
            if let Err(csrf_reason) = validate_csrf_for_mutation(req.headers(), req.uri()) {
                return with_security_headers(auth_error_response(
                    StatusCode::FORBIDDEN,
                    csrf_reason,
                ));
            }
        }
    }

    if state.require_bearer_auth
        && path.starts_with("/api/v1/")
        && path != "/api/v1/health"
        && path != "/api/v1/auth/login"
        && path != "/api/v1/auth/logout"
    {
        let Some(expected) = state
            .configured_api_token
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            return with_security_headers(auth_error_response(
                StatusCode::UNAUTHORIZED,
                "Authentication required but web_api_token is missing or undecryptable",
            ));
        };

        let header_tok = extract_header_token(req.headers());
        let cookie_tok = extract_cookie_token(req.headers());
        let supplied_credentials = header_tok.is_some() || cookie_tok.is_some();

        if supplied_credentials {
            if let Err(retry_after) = state.auth_rate_limiter.check_allowed(client_ip) {
                return with_security_headers(too_many_requests_response(retry_after));
            }
        }

        let authenticated_principal = if let Some(ref h) = header_tok {
            constant_time_eq(h, expected).then_some(AuthenticatedPrincipal::BearerToken)
        } else if let Some(ref c) = cookie_tok {
            state
                .validate_session_cookie(c)
                .then(|| AuthenticatedPrincipal::SessionCookie {
                    token_hash: hash_session_token(c),
                })
        } else {
            None
        };

        let Some(principal) = authenticated_principal else {
            if supplied_credentials {
                state.auth_rate_limiter.record_failure(client_ip);
            }
            return with_security_headers(auth_error_response(
                StatusCode::UNAUTHORIZED,
                "Valid Bearer token, X-Aegis-Token, or session cookie required",
            ));
        };

        state.auth_rate_limiter.reset(client_ip);
        req.extensions_mut().insert(principal);
    } else {
        req.extensions_mut()
            .insert(AuthenticatedPrincipal::UnauthenticatedAllowed);
    }

    with_security_headers(next.run(req).await)
}
