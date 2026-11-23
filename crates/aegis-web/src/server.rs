//! Axum HTTP server lifecycle, trusted-proxy CIDR parsing, and `AppState` initialization (`aegis-web::server`).

use crate::assets::is_dev_static_override_enabled;
use crate::payloads::{
    build_dashboard_payload, build_degraded_sse_event_json, has_critical_degradation,
    next_error_correlation_id, next_snapshot_seq, server_boot_id,
};
use crate::routes::build_router;
use crate::stream::{SSE_AUTH_RECHECK_INTERVAL, SSE_CLIENT_SEND_TIMEOUT};
use aegis_core::{now_ms, resolve_project_root, AegisError, ConfigMap, Result};
use aegis_db::{is_encrypted_value, DatabaseManager};
use axum::Router;
use parking_lot::Mutex;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv6Addr};
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

/// Zero-dependency IPv4/IPv6 CIDR matcher supporting IPv4-mapped IPv6 normalization (`R14-M27`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpCidr {
    V4 { net: u32, mask: u32 },
    V6 { net: u128, mask: u128 },
}

impl IpCidr {
    /// Parses a single IP address or `<ip>/<prefix>` CIDR block, returning `None` on malformed input.
    pub fn parse(raw: &str) -> Option<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }
        if let Some((ip_str, prefix_str)) = trimmed.split_once('/') {
            let ip: IpAddr = ip_str.trim().parse().ok()?;
            let prefix: u8 = prefix_str.trim().parse().ok()?;
            match ip {
                IpAddr::V4(v4) => {
                    if prefix > 32 {
                        return None;
                    }
                    let mask = if prefix == 0 {
                        0u32
                    } else {
                        !0u32 << (32 - u32::from(prefix))
                    };
                    Some(Self::V4 {
                        net: u32::from(v4) & mask,
                        mask,
                    })
                }
                IpAddr::V6(v6) => {
                    if prefix > 128 {
                        return None;
                    }
                    let mask = if prefix == 0 {
                        0u128
                    } else {
                        !0u128 << (128 - u32::from(prefix))
                    };
                    Some(Self::V6 {
                        net: u128::from(v6) & mask,
                        mask,
                    })
                }
            }
        } else {
            let ip: IpAddr = trimmed.parse().ok()?;
            match ip {
                IpAddr::V4(v4) => Some(Self::V4 {
                    net: u32::from(v4),
                    mask: u32::MAX,
                }),
                IpAddr::V6(v6) => Some(Self::V6 {
                    net: u128::from(v6),
                    mask: u128::MAX,
                }),
            }
        }
    }

    /// Returns `true` if `ip` falls inside this CIDR range (normalizing `::ffff:a.b.c.d`).
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self, ip) {
            (Self::V4 { net, mask }, IpAddr::V4(v4)) => (u32::from(v4) & *mask) == *net,
            (Self::V4 { net, mask }, IpAddr::V6(v6)) => v6
                .to_ipv4_mapped()
                .is_some_and(|v4| (u32::from(v4) & *mask) == *net),
            (Self::V6 { net, mask }, IpAddr::V6(v6)) => (u128::from(v6) & *mask) == *net,
            (Self::V6 { net, mask }, IpAddr::V4(v4)) => {
                (u128::from(v4.to_ipv6_mapped()) & *mask) == *net
            }
        }
    }
}

/// Parses a CSV of trusted proxy IPs/CIDRs, warning and skipping malformed entries (fail-closed).
pub fn parse_trusted_proxies(csv: &str) -> Vec<IpCidr> {
    let mut out = Vec::new();
    for raw in csv.split(',') {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        match IpCidr::parse(trimmed) {
            Some(cidr) => out.push(cidr),
            None => {
                tracing::warn!(
                    entry = %trimmed,
                    "Ignoring malformed web_trusted_proxies CIDR entry"
                );
            }
        }
    }
    out
}

/// Cached serialized dashboard snapshot distributed over the shared SSE watch channel (`R14-L13`).
#[derive(Debug, Clone)]
pub struct BroadcastSnapshot {
    pub seq: u64,
    pub generated_at: Instant,
    pub generated_at_ms: i64,
    pub json: Arc<String>,
    pub degraded: bool,
}

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
    pub dev_static_override: bool,
    pub sessions: Arc<Mutex<HashMap<[u8; 32], Instant>>>,
    pub session_revocation_tx: Arc<watch::Sender<u64>>,
    pub configured_api_token: Option<String>,
    pub require_bearer_auth: bool,
    pub allowed_hosts: HashSet<String>,
    pub allow_ip_literal_hosts: bool,
    pub trust_proxy_headers: bool,
    pub trusted_proxies: Vec<IpCidr>,
    pub cookie_secure: bool,
    pub auth_rate_limiter: AuthRateLimiter,
    pub dashboard_rx: watch::Receiver<Option<BroadcastSnapshot>>,
    pub dashboard_tx: Arc<watch::Sender<Option<BroadcastSnapshot>>>,
    pub refresh_notify: Arc<Notify>,
    pub sse_client_count: Arc<AtomicUsize>,
    pub sse_client_send_timeout: Duration,
    pub sse_auth_recheck_interval: Duration,
    broadcaster_started: Arc<AtomicBool>,
    broadcaster_guard: Arc<BroadcasterDropGuard>,
}

struct LoadedWebAuthConfig {
    configured_api_token: Option<String>,
    explicit_auth_flag: Option<bool>,
    bind_host: String,
    allowed_hosts_csv: String,
    trust_proxy_headers: bool,
    trusted_proxies_csv: String,
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
        if ipv6_inner.parse::<Ipv6Addr>().is_err() {
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

pub(crate) fn hash_session_token(raw_token: &str) -> [u8; 32] {
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
    let mut trusted_proxies_csv = String::new();
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
        trusted_proxies_csv = cfg.get_str("web_trusted_proxies", "");
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
                    "web_trusted_proxies" => {
                        trusted_proxies_csv = raw.to_string();
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
        trusted_proxies_csv,
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
        let dev_static_override = static_dir.is_some() || is_dev_static_override_enabled();
        let dir = static_dir.unwrap_or_else(|| resolve_project_root().join("web").join("static"));
        let auth_cfg = load_auth_config_fail_closed(&db);
        let effective_host = bind_host
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(auth_cfg.bind_host.as_str());

        // R14-M27: Safe auth-by-default even on loopback unless explicitly disabled via DB-immutable web_auth_enabled=false
        let require_bearer_auth = match auth_cfg.explicit_auth_flag {
            Some(false) => {
                tracing::warn!(
                    bind_host = %effective_host,
                    is_loopback = is_loopback_host(effective_host),
                    "SECURITY WARNING: WebUI authentication disabled via DB-immutable web_auth_enabled=false"
                );
                false
            }
            Some(true) | None => true,
        };

        if require_bearer_auth && !is_loopback_host(effective_host) && !auth_cfg.cookie_secure {
            tracing::warn!(
                bind_host = %effective_host,
                "Web authentication enabled on non-loopback bind without web_cookie_secure=true; use HTTPS or a TLS proxy in production"
            );
        }

        let trusted_proxies = parse_trusted_proxies(&auth_cfg.trusted_proxies_csv);
        if auth_cfg.trust_proxy_headers && trusted_proxies.is_empty() {
            tracing::warn!(
                "web_trust_proxy_headers=true ignored: web_trusted_proxies CIDR allowlist is empty"
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

        let (dashboard_tx, dashboard_rx) = watch::channel(None);
        let (sd_tx, _) = watch::channel(false);
        let (rev_tx, _) = watch::channel(0u64);

        let state = Self {
            db,
            static_dir: dir,
            dev_static_override,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            session_revocation_tx: Arc::new(rev_tx),
            configured_api_token: auth_cfg.configured_api_token,
            require_bearer_auth,
            allowed_hosts,
            allow_ip_literal_hosts,
            trust_proxy_headers: auth_cfg.trust_proxy_headers,
            trusted_proxies,
            cookie_secure: auth_cfg.cookie_secure,
            auth_rate_limiter: AuthRateLimiter::new(),
            dashboard_rx,
            dashboard_tx: Arc::new(dashboard_tx),
            refresh_notify: Arc::new(Notify::new()),
            sse_client_count: Arc::new(AtomicUsize::new(0)),
            sse_client_send_timeout: SSE_CLIENT_SEND_TIMEOUT,
            sse_auth_recheck_interval: SSE_AUTH_RECHECK_INTERVAL,
            broadcaster_started: Arc::new(AtomicBool::new(false)),
            broadcaster_guard: Arc::new(BroadcasterDropGuard::new(sd_tx)),
        };
        state.ensure_broadcaster_running();
        state
    }

    /// Overrides SSE send and auth re-check timers (primarily for deterministic integration tests).
    pub fn with_sse_timeouts(
        mut self,
        send_timeout: Duration,
        auth_recheck_interval: Duration,
    ) -> Self {
        self.sse_client_send_timeout = send_timeout;
        self.sse_auth_recheck_interval = auth_recheck_interval;
        self
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

    /// Returns `true` when `trust_proxy_headers` is enabled AND `ip` matches `trusted_proxies`.
    pub fn is_trusted_proxy(&self, ip: IpAddr) -> bool {
        self.trust_proxy_headers && self.trusted_proxies.iter().any(|cidr| cidr.contains(ip))
    }

    fn bump_session_revocation_watch(&self) {
        let next = self.session_revocation_tx.borrow().wrapping_add(1);
        let _ = self.session_revocation_tx.send(next);
    }

    /// Mints a new random 256-bit session cookie token (valid for 12h), optionally revoking a prior cookie.
    /// Emits a revocation watch bump whenever a prior cookie is rotated or an older session is evicted.
    pub fn mint_session(&self, revoke_prior_cookie: Option<&str>) -> String {
        let token = generate_session_token();
        let token_hash = hash_session_token(&token);
        let now = Instant::now();
        let expires_at = now + Duration::from_secs(SESSION_TTL_SECS);
        let mut revoked_or_evicted = false;

        let mut guard = self.sessions.lock();
        if let Some(prior) = revoke_prior_cookie {
            let trimmed = prior.trim();
            if !trimmed.is_empty() && guard.remove(&hash_session_token(trimmed)).is_some() {
                revoked_or_evicted = true;
            }
        }
        let before_retain = guard.len();
        guard.retain(|_, exp| *exp > now);
        if guard.len() < before_retain {
            revoked_or_evicted = true;
        }
        while guard.len() >= MAX_SESSIONS {
            if let Some(oldest_key) = guard.iter().min_by_key(|(_, exp)| *exp).map(|(k, _)| *k) {
                guard.remove(&oldest_key);
                revoked_or_evicted = true;
            } else {
                break;
            }
        }
        guard.insert(token_hash, expires_at);
        drop(guard);

        if revoked_or_evicted {
            self.bump_session_revocation_watch();
        }
        token
    }

    /// Revokes a session by raw cookie value and wakes SSE streams for immediate termination (`R14-L15`).
    pub fn revoke_session(&self, raw_cookie: &str) -> bool {
        let trimmed = raw_cookie.trim();
        if trimmed.is_empty() {
            return false;
        }
        let removed = self
            .sessions
            .lock()
            .remove(&hash_session_token(trimmed))
            .is_some();
        if removed {
            self.bump_session_revocation_watch();
        }
        removed
    }

    /// Re-validates a pre-hashed session cookie token (used by continuous SSE session checks, `R14-L15`).
    pub fn is_session_hash_valid(&self, token_hash: &[u8; 32]) -> bool {
        let now = Instant::now();
        let mut guard = self.sessions.lock();
        match guard.get(token_hash).copied() {
            Some(exp) if exp > now => true,
            Some(_) => {
                guard.remove(token_hash);
                false
            }
            None => false,
        }
    }

    /// Validates an `aegis_session` cookie against the active in-memory session store, evicting expired sessions.
    pub fn validate_session_cookie(&self, raw_cookie: &str) -> bool {
        let trimmed = raw_cookie.trim();
        if trimmed.is_empty() {
            return false;
        }
        let key = hash_session_token(trimmed);
        self.is_session_hash_valid(&key)
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
        let sse_clients = Arc::clone(&self.sse_client_count);
        let mut shutdown_rx = self.broadcaster_guard.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(1500));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut first = true;

            loop {
                if !first {
                    tokio::select! {
                        biased;
                        _ = shutdown_rx.wait_for(|v| *v) => break,
                        _ = ticker.tick() => {}
                        _ = notify.notified() => {}
                    }
                }
                if *shutdown_rx.borrow() {
                    break;
                }

                // Refresh on startup or whenever at least one SSE client or watch subscriber is active
                if first || sse_clients.load(Ordering::SeqCst) > 0 || tx.receiver_count() > 1 {
                    first = false;
                    let db_cl = db.clone();
                    let snap_res =
                        tokio::task::spawn_blocking(move || {
                            match build_dashboard_payload(&db_cl) {
                                Ok(payload) => {
                                    if has_critical_degradation(&payload) {
                                        let json_str = build_degraded_sse_event_json(
                                            payload.database_reachable,
                                            payload.degraded_sections,
                                            payload.degraded_errors,
                                            payload.snapshot_seq,
                                            payload.generated_at_ms,
                                            payload.server_boot_id,
                                        );
                                        BroadcastSnapshot {
                                            seq: payload.snapshot_seq,
                                            generated_at: Instant::now(),
                                            generated_at_ms: payload.generated_at_ms,
                                            json: Arc::new(json_str),
                                            degraded: true,
                                        }
                                    } else {
                                        match serde_json::to_string(&payload) {
                                            Ok(json_str) => BroadcastSnapshot {
                                                seq: payload.snapshot_seq,
                                                generated_at: Instant::now(),
                                                generated_at_ms: payload.generated_at_ms,
                                                json: Arc::new(json_str),
                                                degraded: payload.degraded,
                                            },
                                            Err(ser_err) => {
                                                let cid = next_error_correlation_id();
                                                tracing::error!(
                                                    correlation_id = %cid,
                                                    error = %ser_err,
                                                    "SSE dashboard serialization failed"
                                                );
                                                let ts = now_ms();
                                                let seq = next_snapshot_seq();
                                                let boot_id = server_boot_id();
                                                let json_str = build_degraded_sse_event_json(
                                                    true,
                                                    vec!["serialization".to_string()],
                                                    vec![format!(
                                                        "Payload serialization failed (ref: {cid})"
                                                    )],
                                                    seq,
                                                    ts,
                                                    boot_id,
                                                );
                                                BroadcastSnapshot {
                                                    seq,
                                                    generated_at: Instant::now(),
                                                    generated_at_ms: ts,
                                                    json: Arc::new(json_str),
                                                    degraded: true,
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(err) => {
                                    let cid = next_error_correlation_id();
                                    tracing::error!(
                                        correlation_id = %cid,
                                        error = %err,
                                        "SSE dashboard broadcast failed"
                                    );
                                    let ts = now_ms();
                                    let seq = next_snapshot_seq();
                                    let boot_id = server_boot_id();
                                    let json_str = build_degraded_sse_event_json(
                                        false,
                                        vec!["database".to_string()],
                                        vec![format!("Database unavailable (ref: {cid})")],
                                        seq,
                                        ts,
                                        boot_id,
                                    );
                                    BroadcastSnapshot {
                                        seq,
                                        generated_at: Instant::now(),
                                        generated_at_ms: ts,
                                        json: Arc::new(json_str),
                                        degraded: true,
                                    }
                                }
                            }
                        })
                        .await;

                    if *shutdown_rx.borrow() {
                        break;
                    }
                    if let Ok(snap) = snap_res {
                        let _ = tx.send(Some(snap));
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
            "Refusing to start WebUI server: authentication is required by default, but web_api_token is missing, empty, or undecryptable. Run 'aegis-quant db --init' to provision a token, or explicitly opt out via DB-immutable web_auth_enabled=false."
                .to_string(),
        ));
    }

    let trimmed_host = host.trim();
    let addr = if trimmed_host.parse::<Ipv6Addr>().is_ok() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_ip_cidr_v4_v6_and_mapped() {
        let c4 = IpCidr::parse("10.20.0.0/16").unwrap();
        assert!(c4.contains(IpAddr::V4(Ipv4Addr::new(10, 20, 5, 9))));
        assert!(!c4.contains(IpAddr::V4(Ipv4Addr::new(10, 21, 0, 1))));
        let mapped: Ipv6Addr = "::ffff:10.20.1.2".parse().unwrap();
        assert!(c4.contains(IpAddr::V6(mapped)));

        let c_single = IpCidr::parse("127.0.0.1").unwrap();
        assert!(c_single.contains(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(!c_single.contains(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))));

        let c6 = IpCidr::parse("fd00::/8").unwrap();
        assert!(c6.contains("fd12:3456::1".parse().unwrap()));
        assert!(!c6.contains("fe80::1".parse().unwrap()));

        assert!(IpCidr::parse("10.0.0.1/33").is_none());
        assert!(IpCidr::parse("not-an-ip").is_none());
        assert_eq!(parse_trusted_proxies("127.0.0.1/32, bad, ::1").len(), 2);
    }
}
