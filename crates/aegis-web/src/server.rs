//! Axum HTTP server lifecycle and `AppState` initialization (`aegis-web::server`).

use crate::payloads::build_dashboard_payload;
use crate::routes::build_router;
use aegis_core::{resolve_project_root, AegisError, ConfigMap, Result};
use aegis_db::{is_encrypted_value, DatabaseManager};
use axum::Router;
use parking_lot::Mutex;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::{watch, Notify};

pub const SESSION_TTL_SECS: u64 = 43_200;
pub const MAX_SESSIONS: usize = 128;
pub const AUTH_RATE_LIMIT_MAX_KEYS: usize = 4096;
pub const AUTH_RATE_LIMIT_MAX_FAILURES: u32 = 5;
pub const AUTH_RATE_LIMIT_WINDOW_SECS: u64 = 60;

/// Client IP identifier extracted by `security_and_auth_middleware` for rate limiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientIpKey {
    Ip(IpAddr),
    Local,
}

#[derive(Debug, Clone, Copy)]
struct AuthFailureBucket {
    failures: u32,
    window_start: Instant,
}

/// Bounded in-memory per-client-IP authentication failure rate limiter.
#[derive(Debug, Clone, Default)]
pub struct AuthRateLimiter {
    buckets: Arc<Mutex<HashMap<ClientIpKey, AuthFailureBucket>>>,
}

impl AuthRateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Checks whether `key` is currently locked out; returns `Err(retry_after_secs)` if blocked.
    pub fn check_allowed(&self, key: ClientIpKey) -> std::result::Result<(), u64> {
        let window = Duration::from_secs(AUTH_RATE_LIMIT_WINDOW_SECS);
        let now = Instant::now();
        let mut guard = self.buckets.lock();
        let Some(bucket) = guard.get(&key).copied() else {
            return Ok(());
        };
        let elapsed = now.saturating_duration_since(bucket.window_start);
        if elapsed >= window {
            guard.remove(&key);
            return Ok(());
        }
        if bucket.failures >= AUTH_RATE_LIMIT_MAX_FAILURES {
            let rem_ms = window.saturating_sub(elapsed).as_millis();
            let retry_after = rem_ms.div_ceil(1000).max(1) as u64;
            return Err(retry_after);
        }
        Ok(())
    }

    /// Records a failed authentication attempt for `key`, evicting expired/oldest buckets beyond capacity.
    pub fn record_failure(&self, key: ClientIpKey) {
        let window = Duration::from_secs(AUTH_RATE_LIMIT_WINDOW_SECS);
        let now = Instant::now();
        let mut guard = self.buckets.lock();
        if !guard.contains_key(&key) && guard.len() >= AUTH_RATE_LIMIT_MAX_KEYS {
            guard.retain(|_, b| now.saturating_duration_since(b.window_start) < window);
            while guard.len() >= AUTH_RATE_LIMIT_MAX_KEYS {
                if let Some(oldest_key) = guard
                    .iter()
                    .min_by_key(|(_, b)| b.window_start)
                    .map(|(k, _)| *k)
                {
                    guard.remove(&oldest_key);
                } else {
                    break;
                }
            }
        }
        match guard.get_mut(&key) {
            Some(bucket) if now.saturating_duration_since(bucket.window_start) < window => {
                bucket.failures = bucket.failures.saturating_add(1);
            }
            Some(bucket) => {
                bucket.failures = 1;
                bucket.window_start = now;
            }
            None => {
                guard.insert(
                    key,
                    AuthFailureBucket {
                        failures: 1,
                        window_start: now,
                    },
                );
            }
        }
    }

    /// Resets failure count for `key` after a successful authentication.
    pub fn reset(&self, key: ClientIpKey) {
        self.buckets.lock().remove(&key);
    }
}

struct BroadcasterDropGuard {
    shutdown_tx: watch::Sender<bool>,
}

impl BroadcasterDropGuard {
    fn new(shutdown_tx: watch::Sender<bool>) -> Self {
        Self { shutdown_tx }
    }

    fn subscribe(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }
}

impl Drop for BroadcasterDropGuard {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

/// Shared application state for the Aegis Quant v2.0 Web server.
#[derive(Clone)]
pub struct AppState {
    pub db: DatabaseManager,
    pub static_dir: PathBuf,
    pub sessions: Arc<Mutex<HashMap<[u8; 32], Instant>>>,
    pub configured_api_token: Option<String>,
    pub require_bearer_auth: bool,
    pub allowed_hosts: HashSet<String>,
    pub allow_ip_literal_hosts: bool,
    pub trust_proxy_headers: bool,
    pub cookie_secure: bool,
    pub auth_rate_limiter: AuthRateLimiter,
    pub dashboard_rx: watch::Receiver<Arc<String>>,
    pub dashboard_tx: Arc<watch::Sender<Arc<String>>>,
    pub refresh_notify: Arc<Notify>,
    pub sse_client_count: Arc<AtomicUsize>,
    broadcaster_started: Arc<AtomicBool>,
    broadcaster_guard: Arc<BroadcasterDropGuard>,
}

struct LoadedWebAuthConfig {
    configured_api_token: Option<String>,
    explicit_auth_flag: Option<bool>,
    bind_host: String,
    allowed_hosts_csv: String,
    trust_proxy_headers: bool,
    cookie_secure: bool,
}

fn parse_optional_bool(raw: Option<&String>) -> Option<bool> {
    let v = raw?.trim().to_ascii_lowercase();
    match v.as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Normalizes an HTTP `Host` header or bind host by stripping a valid port, unbracketing IPv6, and lowercasing.
pub fn normalize_host_without_port(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.contains('@')
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains('?')
        || trimmed.contains('#')
        || trimmed.chars().any(char::is_whitespace)
    {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    if let Some(after_open) = lower.strip_prefix('[') {
        let close_idx = after_open.find(']')?;
        let ipv6_inner = &after_open[..close_idx];
        let rest = &after_open[close_idx + 1..];
        if !rest.is_empty() {
            let port_str = rest.strip_prefix(':')?;
            if port_str.is_empty() || !port_str.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let _port: u16 = port_str.parse().ok()?;
        }
        if ipv6_inner.parse::<std::net::Ipv6Addr>().is_err() {
            return None;
        }
        return Some(ipv6_inner.to_string());
    }

    if lower.parse::<IpAddr>().is_ok() {
        return Some(lower);
    }

    if let Some((host_part, port_part)) = lower.split_once(':') {
        if host_part.is_empty()
            || port_part.is_empty()
            || port_part.contains(':')
            || !port_part.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let _port: u16 = port_part.parse().ok()?;
        return Some(host_part.to_string());
    }

    Some(lower)
}

fn is_wildcard_host(host: &str) -> bool {
    matches!(
        normalize_host_without_port(host).as_deref(),
        Some("0.0.0.0" | "::")
    )
}

fn hash_session_token(raw_token: &str) -> [u8; 32] {
    let digest = Sha256::digest(raw_token.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

fn load_auth_config_fail_closed(db: &DatabaseManager) -> LoadedWebAuthConfig {
    let mut tok: Option<String> = None;
    let mut explicit_enabled: Option<bool> = None;
    let mut explicit_require: Option<bool> = None;
    let mut bind_host = ConfigMap::default().get_str("web_bind_host", "127.0.0.1");
    let mut allowed_hosts_csv = String::new();
    let mut trust_proxy_headers = false;
    let mut cookie_secure = false;
    let mut any_auth_error = false;

    if let Ok(cfg) = db.load_config() {
        tok = cfg
            .get("web_api_token")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        explicit_enabled = parse_optional_bool(cfg.get("web_auth_enabled"));
        explicit_require = parse_optional_bool(cfg.get("web_require_auth"));
        bind_host = cfg.get_str("web_bind_host", "127.0.0.1");
        allowed_hosts_csv = cfg.get_str("web_allowed_hosts", "");
        trust_proxy_headers = cfg.get_bool("web_trust_proxy_headers", false);
        cookie_secure = cfg.get_bool("web_cookie_secure", false);
    }

    // ALWAYS inspect raw system_config rows even when load_config() returns Ok,
    // so corrupt web_api_token ciphertexts or DB query errors fail closed.
    match db.list_system_config_rows() {
        Ok(rows) => {
            for r in rows {
                let key = r.config_key.trim().to_ascii_lowercase();
                let raw = r.config_value.trim();
                match key.as_str() {
                    "web_api_token" => {
                        if raw.is_empty() {
                            tok = None;
                        } else if is_encrypted_value(raw) || raw.starts_with("ENC:") {
                            match db.fernet().decrypt(raw) {
                                Ok(plain) if !plain.trim().is_empty() => {
                                    tok = Some(plain.trim().to_string());
                                }
                                _ => {
                                    any_auth_error = true;
                                    tok = None;
                                }
                            }
                        } else {
                            tok = Some(raw.to_string());
                        }
                    }
                    "web_auth_enabled" => {
                        if let Some(parsed) = parse_optional_bool(Some(&r.config_value)) {
                            explicit_enabled = Some(parsed);
                        }
                    }
                    "web_require_auth" => {
                        if let Some(parsed) = parse_optional_bool(Some(&r.config_value)) {
                            explicit_require = Some(parsed);
                        }
                    }
                    "web_bind_host" if !raw.is_empty() => {
                        bind_host = raw.to_string();
                    }
                    "web_allowed_hosts" => {
                        allowed_hosts_csv = raw.to_string();
                    }
                    "web_trust_proxy_headers" => {
                        if let Some(parsed) = parse_optional_bool(Some(&r.config_value)) {
                            trust_proxy_headers = parsed;
                        }
                    }
                    "web_cookie_secure" => {
                        if let Some(parsed) = parse_optional_bool(Some(&r.config_value)) {
                            cookie_secure = parsed;
                        }
                    }
                    _ => {}
                }
            }
        }
        Err(_) => {
            any_auth_error = true;
            tok = None;
        }
    }

    let mut explicit_auth_flag = match (explicit_enabled, explicit_require) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), _) | (_, Some(false)) => Some(false),
        (None, None) => None,
    };

    if any_auth_error && explicit_auth_flag != Some(false) {
        explicit_auth_flag = Some(true);
        tok = None;
    }

    LoadedWebAuthConfig {
        configured_api_token: tok,
        explicit_auth_flag,
        bind_host,
        allowed_hosts_csv,
        trust_proxy_headers,
        cookie_secure,
    }
}

fn is_loopback_host(host: &str) -> bool {
    let trimmed = host.trim();
    if trimmed.is_empty() {
        return true;
    }
    let Some(norm) = normalize_host_without_port(trimmed) else {
        return false;
    };
    if norm.eq_ignore_ascii_case("localhost") {
        return true;
    }
    norm.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

impl AppState {
    pub fn new(db: DatabaseManager, static_dir: Option<PathBuf>) -> Self {
        Self::new_with_bind_host(db, static_dir, None)
    }

    pub fn new_with_bind_host(
        db: DatabaseManager,
        static_dir: Option<PathBuf>,
        bind_host: Option<&str>,
    ) -> Self {
        let dir = static_dir.unwrap_or_else(|| resolve_project_root().join("web").join("static"));
        let auth_cfg = load_auth_config_fail_closed(&db);
        let effective_host = bind_host
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(auth_cfg.bind_host.as_str());

        let require_bearer_auth = match auth_cfg.explicit_auth_flag {
            Some(false) => {
                if !is_loopback_host(effective_host) {
                    tracing::warn!(
                        bind_host = %effective_host,
                        "web_auth_enabled=false explicitly set for non-loopback bind; preserving unauthenticated access per operator opt-out"
                    );
                }
                false
            }
            Some(true) => true,
            None => !is_loopback_host(effective_host) || auth_cfg.configured_api_token.is_some(),
        };

        if require_bearer_auth && !is_loopback_host(effective_host) && !auth_cfg.cookie_secure {
            tracing::warn!(
                bind_host = %effective_host,
                "Web authentication enabled on non-loopback bind without web_cookie_secure=true; use HTTPS or a TLS proxy in production"
            );
        }

        let mut allowed_hosts = HashSet::from([
            "127.0.0.1".to_string(),
            "localhost".to_string(),
            "::1".to_string(),
        ]);
        if let Some(norm_eff) = normalize_host_without_port(effective_host) {
            if !is_wildcard_host(&norm_eff) {
                allowed_hosts.insert(norm_eff);
            }
        }
        let mut csv_hosts = Vec::new();
        for raw_entry in auth_cfg.allowed_hosts_csv.split(',') {
            if let Some(norm) = normalize_host_without_port(raw_entry) {
                if !is_wildcard_host(&norm) {
                    csv_hosts.push(norm);
                }
            }
        }
        let allow_ip_literal_hosts = is_wildcard_host(effective_host) && csv_hosts.is_empty();
        for host_entry in csv_hosts {
            allowed_hosts.insert(host_entry);
        }

        let (dashboard_tx, dashboard_rx) = watch::channel(Arc::new(String::new()));
        let (sd_tx, _) = watch::channel(false);

        let state = Self {
            db,
            static_dir: dir,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            configured_api_token: auth_cfg.configured_api_token,
            require_bearer_auth,
            allowed_hosts,
            allow_ip_literal_hosts,
            trust_proxy_headers: auth_cfg.trust_proxy_headers,
            cookie_secure: auth_cfg.cookie_secure,
            auth_rate_limiter: AuthRateLimiter::new(),
            dashboard_rx,
            dashboard_tx: Arc::new(dashboard_tx),
            refresh_notify: Arc::new(Notify::new()),
            sse_client_count: Arc::new(AtomicUsize::new(0)),
            broadcaster_started: Arc::new(AtomicBool::new(false)),
            broadcaster_guard: Arc::new(BroadcasterDropGuard::new(sd_tx)),
        };
        state.ensure_broadcaster_running();
        state
    }

    /// Returns `true` if `host_no_port` (lowercase, port-stripped) is allowed by `allowed_hosts`
    /// or `allow_ip_literal_hosts` for wildcard binds.
    pub fn is_trusted_host(&self, host_no_port: &str) -> bool {
        let lower = host_no_port.trim().to_ascii_lowercase();
        if self.allowed_hosts.contains(&lower) {
            return true;
        }
        self.allow_ip_literal_hosts && lower.parse::<IpAddr>().is_ok()
    }

    /// Mints a new random 256-bit session cookie token (valid for 12h), optionally revoking a prior cookie.
    pub fn mint_session(&self, revoke_prior_cookie: Option<&str>) -> String {
        let token = generate_session_token();
        let token_hash = hash_session_token(&token);
        let now = Instant::now();
        let expires_at = now + Duration::from_secs(SESSION_TTL_SECS);

        let mut guard = self.sessions.lock();
        if let Some(prior) = revoke_prior_cookie {
            let trimmed = prior.trim();
            if !trimmed.is_empty() {
                guard.remove(&hash_session_token(trimmed));
            }
        }
        guard.retain(|_, exp| *exp > now);
        while guard.len() >= MAX_SESSIONS {
            if let Some(oldest_key) = guard.iter().min_by_key(|(_, exp)| *exp).map(|(k, _)| *k) {
                guard.remove(&oldest_key);
            } else {
                break;
            }
        }
        guard.insert(token_hash, expires_at);
        token
    }

    /// Validates an `aegis_session` cookie against the active in-memory session store, evicting expired sessions.
    pub fn validate_session_cookie(&self, raw_cookie: &str) -> bool {
        let trimmed = raw_cookie.trim();
        if trimmed.is_empty() {
            return false;
        }
        let key = hash_session_token(trimmed);
        let now = Instant::now();
        let mut guard = self.sessions.lock();
        match guard.get(&key).copied() {
            Some(exp) if exp > now => true,
            Some(_) => {
                guard.remove(&key);
                false
            }
            None => false,
        }
    }

    /// Triggers an immediate refresh on the shared 1.5s SSE watch broadcaster.
    pub fn notify_dashboard_refresh(&self) {
        self.refresh_notify.notify_one();
    }

    /// Signals the SSE broadcaster and all active SSE client streams to terminate cleanly.
    pub fn shutdown(&self) {
        let _ = self.broadcaster_guard.shutdown_tx.send(true);
    }

    /// Subscribes to the server shutdown signal.
    pub fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.broadcaster_guard.subscribe()
    }

    /// Spawns the single shared background task that refreshes the serialized `DashboardPayload`
    /// every 1.5s into the `tokio::sync::watch` channel for all connected SSE clients.
    pub fn ensure_broadcaster_running(&self) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        if self
            .broadcaster_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let db = self.db.clone();
        let tx = Arc::clone(&self.dashboard_tx);
        let notify = Arc::clone(&self.refresh_notify);
        let mut shutdown_rx = self.broadcaster_guard.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(1500));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut first = true;

            loop {
                if !first {
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = notify.notified() => {}
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                }
                if *shutdown_rx.borrow() {
                    break;
                }

                // Refresh on startup or whenever at least one SSE client is subscribed (`receiver_count() > 1`)
                if first || tx.receiver_count() > 1 {
                    first = false;
                    let db_cl = db.clone();
                    if let Ok(Ok(json_str)) = tokio::task::spawn_blocking(move || {
                        build_dashboard_payload(&db_cl)
                            .and_then(|p| serde_json::to_string(&p).map_err(Into::into))
                    })
                    .await
                    {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                        let _ = tx.send(Arc::new(json_str));
                    }
                }
            }
        });
    }
}

/// Generates a random 32-byte hex session token.
pub fn generate_session_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Constructs an Axum `Router` for `db` and optional `static_dir`.
pub fn create_web_router(db: DatabaseManager, static_dir: Option<&Path>) -> Router {
    create_web_router_for_host(db, static_dir, None)
}

/// Constructs an Axum `Router` with explicit bind host awareness for fail-closed auth checks.
pub fn create_web_router_for_host(
    db: DatabaseManager,
    static_dir: Option<&Path>,
    bind_host: Option<&str>,
) -> Router {
    let state = AppState::new_with_bind_host(db, static_dir.map(|p| p.to_path_buf()), bind_host);
    build_router(state)
}

/// Starts the Aegis Quant v2.0 Web UI and REST/SSE server on `host:port` until `shutdown_rx` signals true.
pub async fn serve_web_with_shutdown(
    db: DatabaseManager,
    host: &str,
    port: u16,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let state = AppState::new_with_bind_host(db, None, Some(host));
    if state.require_bearer_auth
        && state
            .configured_api_token
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .is_empty()
    {
        return Err(AegisError::SecurityViolation(
            "Refusing to start WebUI server: authentication is required but web_api_token is missing, empty, or undecryptable".to_string(),
        ));
    }

    let trimmed_host = host.trim();
    let addr = if trimmed_host.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{trimmed_host}]:{port}")
    } else {
        format!("{trimmed_host}:{port}")
    };
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(AegisError::Io)?;

    tracing::info!(
        bind_addr = %addr,
        "Aegis Quant v2.0 Web Dashboard & Real-Time SSE Server listening on http://{addr}"
    );

    let shutdown_state = state.clone();
    let app = build_router(state);

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        while !*shutdown_rx.borrow() {
            if shutdown_rx.changed().await.is_err() {
                break;
            }
        }
        shutdown_state.shutdown();
    })
    .await
    .map_err(AegisError::Io)?;

    Ok(())
}
