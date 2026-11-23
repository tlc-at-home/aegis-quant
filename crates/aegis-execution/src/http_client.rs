//! Shared Hyperliquid `/info` & `/exchange` HTTP client, host-keyed REST weight governor,
//! tiered request options, and SSRF/MITM endpoint validator (`aegis-execution::http_client`).

use crate::signing::{
    bump_nonce_high_water, next_monotonic_nonce, sign_l1_action, CancelAction, CancelWire,
    OrderAction, OrderWire, HL_EXCHANGE_OPTIMAL_BATCH_CHUNK,
};
use aegis_core::{
    parse_hl_f64_opt, parse_hl_i64_opt, parse_hl_u64_opt, round_dp, AegisError, ConfigMap,
    ExecutionMode, Result, L2_DEPTH_BAND_PCT,
};
use aegis_db::DatabaseManager;
use parking_lot::Mutex;
use rand::Rng;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

pub const HYPERLIQUID_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
pub const HYPERLIQUID_EXCHANGE_URL: &str = "https://api.hyperliquid.xyz/exchange";
pub const HYPERLIQUID_TESTNET_INFO_URL: &str = "https://api.hyperliquid-testnet.xyz/info";
pub const HYPERLIQUID_TESTNET_EXCHANGE_URL: &str = "https://api.hyperliquid-testnet.xyz/exchange";

pub const DEFAULT_HL_REST_WEIGHT_LIMIT_PER_MIN: u32 = 900;
pub const MIN_HL_REST_WEIGHT_LIMIT_PER_MIN: u32 = 300;
pub const MAX_HL_REST_WEIGHT_LIMIT_PER_MIN: u32 = 1_150;
pub const HL_REST_BURST_ALLOWANCE: f64 = 150.0;
pub const HL_GOVERNOR_FLOOR_CRITICAL: f64 = 0.0;
pub const HL_GOVERNOR_FLOOR_HIGH: f64 = 150.0;
pub const HL_GOVERNOR_FLOOR_NORMAL: f64 = 300.0;
pub const HL_GOVERNOR_FLOOR_LOW: f64 = 450.0;

static SHARED_BLOCKING_CLIENTS: OnceLock<Mutex<HashMap<(u64, bool), Client>>> = OnceLock::new();
static HOST_GOVERNORS: OnceLock<Mutex<HashMap<String, Arc<RestWeightGovernor>>>> = OnceLock::new();

fn fallback_client(timeout_sec: u64, no_proxy: bool) -> Client {
    let mut builder = Client::builder().timeout(Duration::from_secs(timeout_sec.max(1)));
    if no_proxy {
        builder = builder.no_proxy();
    }
    builder.build().unwrap_or_else(|_| Client::new())
}

/// Returns a process-wide cached `reqwest::blocking::Client` built on a dedicated OS thread
/// so dropping inside an async runtime (`#[tokio::test]` / Axum) never panics (`R14-H2`).
pub fn shared_blocking_client(timeout_sec: u64) -> Client {
    shared_blocking_client_opts(timeout_sec, false)
}

pub fn shared_blocking_client_opts(timeout_sec: u64, no_proxy: bool) -> Client {
    let t_sec = timeout_sec.max(1);
    let cache = SHARED_BLOCKING_CLIENTS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(existing) = cache.lock().get(&(t_sec, no_proxy)).cloned() {
        return existing;
    }
    let client = std::thread::Builder::new()
        .name("hl-http-init".into())
        .spawn(move || fallback_client(t_sec, no_proxy))
        .ok()
        .and_then(|h| h.join().ok())
        .unwrap_or_else(|| fallback_client(t_sec, no_proxy));
    let mut guard = cache.lock();
    guard
        .entry((t_sec, no_proxy))
        .or_insert_with(|| client.clone());
    client
}

/// Priority lane for Hyperliquid REST weight budgeting (`R14-H4`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RequestPriority {
    Critical,
    High,
    Normal,
    Low,
}

impl RequestPriority {
    /// Minimum remaining token budget required before permitting a request in this lane.
    #[inline]
    pub const fn floor(self) -> f64 {
        match self {
            Self::Critical => HL_GOVERNOR_FLOOR_CRITICAL,
            Self::High => HL_GOVERNOR_FLOOR_HIGH,
            Self::Normal => HL_GOVERNOR_FLOOR_NORMAL,
            Self::Low => HL_GOVERNOR_FLOOR_LOW,
        }
    }
}

/// Tiered HTTP execution parameters for Hyperliquid endpoints (`R14-H2`, `R14-H4`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestOptions {
    pub timeout_sec: u64,
    pub max_retries: usize,
    pub max_retry_sleep_ms: u64,
    pub priority: RequestPriority,
}

impl RequestOptions {
    /// Fast surveillance lane (`<= 2s` timeout, 1 retry, `<= 300ms` backoff, `High` priority).
    pub const fn surveillance() -> Self {
        Self {
            timeout_sec: 2,
            max_retries: 1,
            max_retry_sleep_ms: 300,
            priority: RequestPriority::High,
        }
    }

    /// Critical surveillance lane (stop-exit reads: `<= 2s` timeout, `Critical` priority).
    pub const fn surveillance_critical() -> Self {
        Self {
            timeout_sec: 2,
            max_retries: 1,
            max_retry_sleep_ms: 300,
            priority: RequestPriority::Critical,
        }
    }

    /// Exchange mutation lane (`/exchange`: `5s` timeout, `0` HTTP retries, `Critical` priority).
    pub const fn exchange_action() -> Self {
        Self {
            timeout_sec: 5,
            max_retries: 0,
            max_retry_sleep_ms: 0,
            priority: RequestPriority::Critical,
        }
    }

    /// Background reconciler / sync lane (`8s` timeout, up to 2 retries, `<= 2000ms` backoff, `Normal` priority).
    pub const fn background_sync() -> Self {
        Self {
            timeout_sec: 8,
            max_retries: 2,
            max_retry_sleep_ms: 2_000,
            priority: RequestPriority::Normal,
        }
    }

    /// Sheddable background lane (`8s` timeout, `Low` priority; shed immediately when below budget).
    pub const fn low_priority_sync() -> Self {
        Self {
            timeout_sec: 8,
            max_retries: 2,
            max_retry_sleep_ms: 2_000,
            priority: RequestPriority::Low,
        }
    }

    pub const fn with_priority(mut self, priority: RequestPriority) -> Self {
        self.priority = priority;
        self
    }

    pub const fn with_timeout_sec(mut self, timeout_sec: u64) -> Self {
        self.timeout_sec = if timeout_sec == 0 { 1 } else { timeout_sec };
        self
    }

    pub const fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self::background_sync()
    }
}

#[derive(Debug)]
struct GovernorBucket {
    limit_per_min: u32,
    capacity: f64,
    tokens: f64,
    last_refill: Instant,
    rate_limited_until: Option<Instant>,
}

impl GovernorBucket {
    fn new(limit_per_min: u32, now: Instant) -> Self {
        let clamped = limit_per_min.clamp(
            MIN_HL_REST_WEIGHT_LIMIT_PER_MIN,
            MAX_HL_REST_WEIGHT_LIMIT_PER_MIN,
        );
        let cap = Self::compute_capacity(clamped);
        Self {
            limit_per_min: clamped,
            capacity: cap,
            tokens: cap,
            last_refill: now,
            rate_limited_until: None,
        }
    }

    #[inline]
    fn compute_capacity(limit_per_min: u32) -> f64 {
        (HL_GOVERNOR_FLOOR_LOW + HL_REST_BURST_ALLOWANCE).min(limit_per_min as f64)
    }

    #[inline]
    fn refill_rate_per_sec(&self) -> f64 {
        (self.limit_per_min as f64 / 60.0).max(1.0)
    }

    fn refill(&mut self, now: Instant) {
        let elapsed_secs = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        if elapsed_secs > 0.0 {
            self.tokens =
                (self.tokens + elapsed_secs * self.refill_rate_per_sec()).min(self.capacity);
            self.last_refill = now;
        }
        if let Some(until) = self.rate_limited_until {
            if now >= until {
                self.rate_limited_until = None;
            }
        }
    }

    fn update_limit(&mut self, limit_per_min: u32) {
        let clamped = limit_per_min.clamp(
            MIN_HL_REST_WEIGHT_LIMIT_PER_MIN,
            MAX_HL_REST_WEIGHT_LIMIT_PER_MIN,
        );
        if clamped != self.limit_per_min {
            self.limit_per_min = clamped;
            self.capacity = Self::compute_capacity(clamped);
            self.tokens = self.tokens.min(self.capacity);
        }
    }
}

/// Host-keyed Hyperliquid REST weight token-bucket governor with 4 priority lanes (`R14-H4`).
///
/// Auto-bypassed on loopback hosts (`127.0.0.1`, `localhost`, `[::1]`) unless explicitly
/// constructed via [`RestWeightGovernor::new_enforced`].
#[derive(Debug)]
pub struct RestWeightGovernor {
    bucket: Mutex<GovernorBucket>,
}

impl RestWeightGovernor {
    /// Creates an actively enforced governor for unit tests or remote hosts.
    pub fn new_enforced(limit_per_min: u32) -> Self {
        Self {
            bucket: Mutex::new(GovernorBucket::new(limit_per_min, Instant::now())),
        }
    }

    /// Returns `true` when `host` is a local loopback hostname/address.
    pub fn is_loopback_host(host: &str) -> bool {
        matches!(
            host.trim().to_ascii_lowercase().as_str(),
            "127.0.0.1" | "localhost" | "[::1]" | "::1"
        )
    }

    /// Extracts the normalized authority host from an HTTP(S) URL.
    pub fn extract_url_host(url: &str) -> String {
        let trimmed = url.trim();
        let rest = trimmed
            .strip_prefix("https://")
            .or_else(|| trimmed.strip_prefix("http://"))
            .unwrap_or(trimmed);
        let authority = rest.split('/').next().unwrap_or("");
        if authority.starts_with('[') {
            authority
                .split(']')
                .next()
                .map(|h| format!("{h}]"))
                .unwrap_or_default()
                .to_ascii_lowercase()
        } else {
            authority
                .split(':')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
        }
    }

    /// Returns the shared host-keyed governor for `url`, or `None` for loopback hosts.
    pub fn for_url(url: &str, limit_per_min: u32) -> Option<Arc<Self>> {
        let host = Self::extract_url_host(url);
        if host.is_empty() || Self::is_loopback_host(&host) {
            return None;
        }
        let registry = HOST_GOVERNORS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = registry.lock();
        if let Some(existing) = guard.get(&host) {
            existing.set_limit_per_min(limit_per_min);
            return Some(Arc::clone(existing));
        }
        let gov = Arc::new(Self::new_enforced(limit_per_min));
        guard.insert(host, Arc::clone(&gov));
        Some(gov)
    }

    pub fn set_limit_per_min(&self, limit_per_min: u32) {
        self.bucket.lock().update_limit(limit_per_min);
    }

    /// Returns Hyperliquid's official base REST weight for the given URL and JSON body (`R14-H4`).
    pub fn base_weight_for_request(url: &str, payload: &Value) -> u32 {
        if !is_idempotent_info_url(url) {
            let batch_len = match payload.pointer("/action/type").and_then(Value::as_str) {
                Some("order") => payload
                    .pointer("/action/orders")
                    .and_then(Value::as_array)
                    .map_or(1, Vec::len),
                Some("cancel") => payload
                    .pointer("/action/cancels")
                    .and_then(Value::as_array)
                    .map_or(1, Vec::len),
                Some("batchModify") => payload
                    .pointer("/action/modifies")
                    .and_then(Value::as_array)
                    .map_or(1, Vec::len),
                _ => 1,
            };
            return 1 + (batch_len as u32 / 40);
        }

        match payload.get("type").and_then(Value::as_str).unwrap_or("") {
            "allMids"
            | "l2Book"
            | "clearinghouseState"
            | "spotClearinghouseState"
            | "orderStatus"
            | "exchangeStatus" => 2,
            "metaAndAssetCtxs" | "meta" | "frontendOpenOrders" | "openOrders" | "userFills"
            | "userFillsByTime" | "userFunding" | "userAbstraction" | "portfolio"
            | "userVaultEquities" | "vaultDetails" | "candleSnapshot" | "userFees"
            | "extraAgents" => 20,
            _ => 20,
        }
    }

    /// Returns Hyperliquid's post-response variable item weight debit (`R14-H4`).
    pub fn variable_item_debit(payload: &Value, resp: &Value) -> u32 {
        let Some(items) = resp.as_array() else {
            return 0;
        };
        if items.is_empty() {
            return 0;
        }
        match payload.get("type").and_then(Value::as_str).unwrap_or("") {
            "candleSnapshot" => items.len().div_ceil(60) as u32,
            "userFills" | "userFillsByTime" | "userFunding" => items.len().div_ceil(20) as u32,
            _ => 0,
        }
    }

    /// Attempts non-blocking weight acquisition; on insufficiency returns `Err(estimated_wait_ms)`.
    pub fn try_acquire(
        &self,
        priority: RequestPriority,
        weight: u32,
    ) -> std::result::Result<(), u64> {
        let now = Instant::now();
        let mut guard = self.bucket.lock();
        guard.refill(now);

        if priority != RequestPriority::Critical {
            if let Some(until) = guard.rate_limited_until {
                if until > now {
                    let rem_ms = until.saturating_duration_since(now).as_millis() as u64;
                    return Err(rem_ms.max(1));
                }
            }
        }

        let floor = priority.floor();
        let cost = weight as f64;
        if guard.tokens - cost >= floor {
            guard.tokens -= cost;
            Ok(())
        } else {
            let deficit = (floor + cost - guard.tokens).max(0.0);
            let wait_ms = ((deficit / guard.refill_rate_per_sec()) * 1_000.0)
                .ceil()
                .max(1.0) as u64;
            Err(wait_ms)
        }
    }

    /// Acquires weight for a request, immediately shedding `Low` requests when constrained
    /// and allowing bounded waits for higher-priority lanes.
    pub fn acquire_for_request(&self, priority: RequestPriority, weight: u32) -> bool {
        match self.try_acquire(priority, weight) {
            Ok(()) => true,
            Err(wait_ms) => {
                let max_wait = match priority {
                    RequestPriority::Low => return false,
                    RequestPriority::Normal => 300,
                    RequestPriority::High => 150,
                    RequestPriority::Critical => 300,
                };
                if wait_ms > max_wait {
                    return false;
                }
                std::thread::sleep(Duration::from_millis(wait_ms));
                self.try_acquire(priority, weight).is_ok()
            }
        }
    }

    /// Debits additional post-response variable item weight (clamping floor to avoid starving Critical).
    pub fn record_post_response_debit(&self, extra_weight: u32) {
        if extra_weight == 0 {
            return;
        }
        let now = Instant::now();
        let mut guard = self.bucket.lock();
        guard.refill(now);
        guard.tokens = (guard.tokens - (extra_weight as f64)).max(-300.0);
    }

    /// Opens a host-wide cooldown gate for non-critical traffic upon HTTP 429 (`R14-H4`).
    pub fn on_rate_limited(&self, retry_after_ms: u64) {
        let now = Instant::now();
        let mut guard = self.bucket.lock();
        guard.refill(now);
        let gate_ms = retry_after_ms.clamp(250, 10_000);
        let target_until = now + Duration::from_millis(gate_ms);
        guard.rate_limited_until = Some(match guard.rate_limited_until {
            Some(prev) if prev > target_until => prev,
            _ => target_until,
        });
        guard.tokens = guard.tokens.min(HL_GOVERNOR_FLOOR_HIGH);
    }

    pub fn snapshot_tokens(&self) -> f64 {
        let now = Instant::now();
        let mut guard = self.bucket.lock();
        guard.refill(now);
        guard.tokens
    }

    pub fn rate_limited_remaining_ms(&self) -> u64 {
        let now = Instant::now();
        let mut guard = self.bucket.lock();
        guard.refill(now);
        guard
            .rate_limited_until
            .map(|until| until.saturating_duration_since(now).as_millis() as u64)
            .unwrap_or(0)
    }
}

/// Validates that a Hyperliquid endpoint URL uses `https://` and an allowlisted host.
/// Loopback hosts (`127.0.0.1`, `localhost`, `[::1]`) are permitted over `http://` or `https://`
/// for offline and local mock test servers.
pub fn validate_hyperliquid_endpoint_url(url: &str, cfg: &ConfigMap) -> bool {
    let trimmed = url.trim();
    let (is_https, rest) = if let Some(r) = trimmed.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = trimmed.strip_prefix("http://") {
        (false, r)
    } else {
        tracing::warn!(
            url = trimmed,
            "Rejected Hyperliquid URL without http/https scheme"
        );
        return false;
    };

    let authority = rest.split('/').next().unwrap_or("");
    if authority.is_empty() || authority.contains('@') {
        tracing::warn!(
            url = trimmed,
            "Rejected Hyperliquid URL with invalid authority"
        );
        return false;
    }

    let host = if authority.starts_with('[') {
        authority
            .split(']')
            .next()
            .map(|h| format!("{h}]"))
            .unwrap_or_default()
            .to_ascii_lowercase()
    } else {
        authority
            .split(':')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase()
    };

    if host == "127.0.0.1" || host == "localhost" || host == "[::1]" {
        return true;
    }

    if !is_https {
        tracing::warn!(
            url = trimmed,
            host = %host,
            "Rejected non-HTTPS remote Hyperliquid endpoint URL"
        );
        return false;
    }

    let allowed_raw = cfg.get_str(
        "hyperliquid_allowed_hosts",
        "api.hyperliquid.xyz,api.hyperliquid-testnet.xyz",
    );
    let is_allowed = allowed_raw
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .any(|allowed| allowed == host);

    if !is_allowed {
        tracing::warn!(
            url = trimmed,
            host = %host,
            "Rejected untrusted Hyperliquid endpoint host not in hyperliquid_allowed_hosts"
        );
    }
    is_allowed
}

/// Returns `true` when Hyperliquid requests should route to Testnet (strictly `execution_mode != LIVE`).
pub fn is_testnet_configured(cfg: &ConfigMap) -> bool {
    cfg.execution_mode() != ExecutionMode::Live
}

/// Returns `true` when both resolved `/info` and `/exchange` endpoints point to loopback (`127.0.0.1`, `localhost`, `[::1]`).
pub(crate) fn endpoints_are_loopback(cfg: &ConfigMap) -> bool {
    let is_lb = |u: &str| {
        let lower = u.trim().to_ascii_lowercase();
        lower.starts_with("http://127.0.0.1")
            || lower.starts_with("https://127.0.0.1")
            || lower.starts_with("http://localhost")
            || lower.starts_with("https://localhost")
            || lower.starts_with("http://[::1]")
            || lower.starts_with("https://[::1]")
    };
    is_lb(&resolve_info_url(cfg)) && is_lb(&resolve_exchange_url(cfg))
}

/// Resolves and validates the Hyperliquid `/info` endpoint URL from `system_config`.
pub fn resolve_info_url(cfg: &ConfigMap) -> String {
    resolve_info_url_for_network(cfg, is_testnet_configured(cfg))
}

pub fn resolve_info_url_for_network(cfg: &ConfigMap, is_testnet: bool) -> String {
    let (candidate, canonical) = if is_testnet {
        let base = cfg.get_str(
            "hyperliquid_testnet_url",
            "https://api.hyperliquid-testnet.xyz",
        );
        let fallback = format!("{}/info", base.trim_end_matches('/'));
        let custom = cfg.get_str("hyperliquid_testnet_info_url", "");
        let cand = if custom.trim().is_empty() {
            fallback
        } else {
            custom.trim().to_string()
        };
        (cand, HYPERLIQUID_TESTNET_INFO_URL.to_string())
    } else {
        let base = cfg.get_str("hyperliquid_mainnet_url", "https://api.hyperliquid.xyz");
        let fallback = format!("{}/info", base.trim_end_matches('/'));
        let custom = cfg.get_str("hyperliquid_info_url", "");
        let cand = if custom.trim().is_empty() {
            fallback
        } else {
            custom.trim().to_string()
        };
        (cand, HYPERLIQUID_INFO_URL.to_string())
    };

    if validate_hyperliquid_endpoint_url(&candidate, cfg) {
        candidate
    } else {
        canonical
    }
}

/// Resolves and validates the Hyperliquid `/exchange` endpoint URL from `system_config`.
pub fn resolve_exchange_url(cfg: &ConfigMap) -> String {
    resolve_exchange_url_for_network(cfg, is_testnet_configured(cfg))
}

pub fn resolve_exchange_url_for_network(cfg: &ConfigMap, is_testnet: bool) -> String {
    let (candidate, canonical) = if is_testnet {
        let base = cfg.get_str(
            "hyperliquid_testnet_url",
            "https://api.hyperliquid-testnet.xyz",
        );
        let fallback = format!("{}/exchange", base.trim_end_matches('/'));
        let custom = cfg.get_str("hyperliquid_testnet_exchange_url", "");
        let cand = if custom.trim().is_empty() {
            fallback
        } else {
            custom.trim().to_string()
        };
        (cand, HYPERLIQUID_TESTNET_EXCHANGE_URL.to_string())
    } else {
        let base = cfg.get_str("hyperliquid_mainnet_url", "https://api.hyperliquid.xyz");
        let fallback = format!("{}/exchange", base.trim_end_matches('/'));
        let custom = cfg.get_str("hyperliquid_exchange_url", "");
        let cand = if custom.trim().is_empty() {
            fallback
        } else {
            custom.trim().to_string()
        };
        (cand, HYPERLIQUID_EXCHANGE_URL.to_string())
    };

    if validate_hyperliquid_endpoint_url(&candidate, cfg) {
        candidate
    } else {
        canonical
    }
}

const HL_INFO_MAX_RETRIES: usize = 2;
const HL_INFO_RETRY_BACKOFF_MS: [u64; HL_INFO_MAX_RETRIES] = [250, 650];
const HL_INFO_MAX_RETRY_AFTER_MS: u64 = 2_000;

fn is_idempotent_info_url(url: &str) -> bool {
    url.trim().trim_end_matches('/').ends_with("/info")
}

fn is_retryable_info_status(status_code: u16) -> bool {
    matches!(status_code, 429 | 502 | 503 | 504)
}

fn parse_retry_after_ms(retry_after_header: Option<&str>) -> Option<u64> {
    let raw = retry_after_header?.trim();
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(secs.saturating_mul(1_000));
    }
    if let Ok(secs_f) = raw.parse::<f64>() {
        if secs_f.is_finite() && secs_f >= 0.0 {
            return Some((secs_f * 1_000.0).round() as u64);
        }
    }
    None
}

fn compute_info_retry_backoff_ms_with_opts(
    attempt: usize,
    status_code: u16,
    retry_after_header: Option<&str>,
    max_retry_sleep_ms: u64,
    with_jitter: bool,
) -> (u64, u64) {
    let default_ms = HL_INFO_RETRY_BACKOFF_MS
        .get(attempt)
        .copied()
        .unwrap_or(650);
    let raw_ms = parse_retry_after_ms(retry_after_header).unwrap_or(default_ms);
    let jittered_ms =
        if with_jitter && raw_ms > 0 && (status_code == 429 || retry_after_header.is_some()) {
            let factor: f64 = rand::thread_rng().gen_range(0.75_f64..=1.25_f64);
            ((raw_ms as f64) * factor).round().max(0.0) as u64
        } else {
            raw_ms
        };
    (jittered_ms.min(max_retry_sleep_ms), raw_ms)
}

#[cfg(test)]
fn compute_info_retry_backoff_ms(attempt: usize, retry_after_header: Option<&str>) -> u64 {
    compute_info_retry_backoff_ms_with_opts(
        attempt,
        503,
        retry_after_header,
        HL_INFO_MAX_RETRY_AFTER_MS,
        false,
    )
    .0
}

/// Shared blocking JSON POST helper with endpoint validation, lock-discipline assertion (`R14-H2`),
/// host-keyed REST weight governor (`R14-H4`), and bounded `/info` jittered backoff.
#[allow(clippy::too_many_arguments)]
pub fn http_post_json_with_options(
    http_client: &Client,
    client_timeout_sec: u64,
    url: &str,
    payload: &Value,
    cfg: &ConfigMap,
    opts: RequestOptions,
    governor_override: Option<&RestWeightGovernor>,
) -> Option<Value> {
    if crate::lock_guard::is_execution_lock_held_on_current_thread() {
        tracing::error!(
            url = url,
            "HTTP IO attempted while execution_lock is held on current thread (R14-H2 violation)"
        );
        debug_assert!(
            !crate::lock_guard::is_execution_lock_held_on_current_thread(),
            "HTTP IO forbidden under execution_lock"
        );
    }

    if !validate_hyperliquid_endpoint_url(url, cfg) {
        return None;
    }

    let eff_timeout_sec = opts.timeout_sec.max(1);
    let is_loopback =
        url.contains("127.0.0.1") || url.contains("localhost") || url.contains("[::1]");
    let owned_client;
    let client_ref = if eff_timeout_sec == client_timeout_sec && !is_loopback {
        http_client
    } else {
        owned_client = shared_blocking_client_opts(eff_timeout_sec, is_loopback);
        &owned_client
    };

    let limit_per_min = cfg
        .get_i64(
            "hl_rest_weight_limit_per_min",
            DEFAULT_HL_REST_WEIGHT_LIMIT_PER_MIN as i64,
        )
        .clamp(
            MIN_HL_REST_WEIGHT_LIMIT_PER_MIN as i64,
            MAX_HL_REST_WEIGHT_LIMIT_PER_MIN as i64,
        ) as u32;
    let host_governor = if governor_override.is_some() {
        None
    } else {
        RestWeightGovernor::for_url(url, limit_per_min)
    };
    let active_governor: Option<&RestWeightGovernor> =
        governor_override.or(host_governor.as_deref());
    let base_weight = RestWeightGovernor::base_weight_for_request(url, payload);

    let allow_info_retry = is_idempotent_info_url(url);
    let max_attempts = if allow_info_retry {
        1 + opts.max_retries
    } else {
        1
    };

    for attempt in 0..max_attempts {
        if let Some(gov) = active_governor {
            if !gov.acquire_for_request(opts.priority, base_weight) {
                tracing::debug!(
                    url = url,
                    priority = ?opts.priority,
                    base_weight,
                    "Hyperliquid REST request shed by RestWeightGovernor"
                );
                return None;
            }
        }

        let resp = match client_ref.post(url).json(payload).send() {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(url = url, error = %e, "Hyperliquid HTTP POST request failed");
                return None;
            }
        };
        let status = resp.status();
        if status.is_success() {
            return match resp.json::<Value>() {
                Ok(val) => {
                    if let Some(gov) = active_governor {
                        let extra = RestWeightGovernor::variable_item_debit(payload, &val);
                        gov.record_post_response_debit(extra);
                    }
                    Some(val)
                }
                Err(e) => {
                    tracing::warn!(url = url, error = %e, "Failed to parse JSON from Hyperliquid endpoint");
                    None
                }
            };
        }

        let status_code = status.as_u16();
        let retry_after_hdr = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        let (backoff_ms, raw_retry_ms) = compute_info_retry_backoff_ms_with_opts(
            attempt,
            status_code,
            retry_after_hdr.as_deref(),
            opts.max_retry_sleep_ms,
            true,
        );

        if status_code == 429 {
            if let Some(gov) = active_governor {
                gov.on_rate_limited(raw_retry_ms.max(250));
            }
        }

        if allow_info_retry && attempt + 1 < max_attempts && is_retryable_info_status(status_code) {
            tracing::debug!(
                url = url,
                status = %status,
                attempt = attempt + 1,
                max_attempts = max_attempts,
                backoff_ms = backoff_ms,
                "Hyperliquid /info returned transient HTTP status; backing off before retry"
            );
            if backoff_ms > 0 {
                std::thread::sleep(Duration::from_millis(backoff_ms));
            }
            continue;
        }

        tracing::warn!(
            url = url,
            status = %status,
            attempts = attempt + 1,
            "Hyperliquid endpoint returned non-2xx HTTP status"
        );
        return None;
    }

    None
}

/// Compatible default wrapper around [`http_post_json_with_options`].
pub fn http_post_json(
    http_client: &Client,
    timeout_sec: u64,
    client_timeout_sec: u64,
    url: &str,
    payload: &Value,
    cfg: &ConfigMap,
) -> Option<Value> {
    let opts = if is_idempotent_info_url(url) {
        RequestOptions {
            timeout_sec: timeout_sec.max(1),
            max_retries: HL_INFO_MAX_RETRIES,
            max_retry_sleep_ms: HL_INFO_MAX_RETRY_AFTER_MS,
            priority: RequestPriority::Normal,
        }
    } else {
        RequestOptions {
            timeout_sec: timeout_sec.max(1),
            max_retries: 0,
            max_retry_sleep_ms: 0,
            priority: RequestPriority::Critical,
        }
    };
    http_post_json_with_options(
        http_client,
        client_timeout_sec,
        url,
        payload,
        cfg,
        opts,
        None,
    )
}

/// Parsed acknowledgment from a Hyperliquid `/exchange` order submission.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeOrderAck {
    pub accepted: bool,
    pub status: String, // "RESTING", "FILLED", "WAITING_FOR_TRIGGER", "REJECTED"
    pub exchange_order_id: Option<String>,
    pub filled_quantity: f64,
    pub avg_fill_price: f64,
    pub fee_usdc: f64,
    pub raw_error: Option<String>,
}

/// Snapshot of resting exchange open orders preserving whether full trigger metadata is present (`R14-M7`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct OpenOrdersSnapshot {
    pub orders: Vec<Value>,
    /// `true` when sourced from `frontendOpenOrders`, `false` when sourced from `openOrders` fallback.
    pub metadata_complete: bool,
}

/// Parsed Hyperliquid `orderStatus` lookup result (`R14-M1`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeOrderStatus {
    pub oid: u64,
    pub status: String,
    pub orig_sz: f64,
    pub remaining_sz: f64,
    pub filled_sz: f64,
    pub limit_px: f64,
    pub avg_px: f64,
    pub is_trigger: bool,
    pub cloid: Option<String>,
}

fn parse_exchange_order_status_entry(
    entry_opt: Option<&Value>,
    requested_qty: f64,
    ref_price: f64,
    fee_bps: f64,
) -> ExchangeOrderAck {
    let Some(entry) = entry_opt else {
        return ExchangeOrderAck {
            accepted: true,
            status: "RESTING".to_string(),
            exchange_order_id: None,
            filled_quantity: 0.0,
            avg_fill_price: ref_price,
            fee_usdc: 0.0,
            raw_error: None,
        };
    };

    if let Some(err_msg) = entry.get("error").and_then(Value::as_str) {
        return ExchangeOrderAck {
            accepted: false,
            status: "REJECTED".to_string(),
            exchange_order_id: None,
            filled_quantity: 0.0,
            avg_fill_price: 0.0,
            fee_usdc: 0.0,
            raw_error: Some(err_msg.to_string()),
        };
    }

    if entry.as_str() == Some("waitingForTrigger") || entry.as_str() == Some("waitingForFill") {
        return ExchangeOrderAck {
            accepted: true,
            status: "WAITING_FOR_TRIGGER".to_string(),
            exchange_order_id: None,
            filled_quantity: 0.0,
            avg_fill_price: ref_price,
            fee_usdc: 0.0,
            raw_error: None,
        };
    }

    if let Some(waiting) = entry
        .get("waitingForTrigger")
        .or_else(|| entry.get("waitingForFill"))
    {
        let oid = parse_hl_u64_opt(waiting.get("oid")).unwrap_or(0);
        let eoid = (oid > 0).then(|| format!("hl-{oid}"));
        return ExchangeOrderAck {
            accepted: true,
            status: "WAITING_FOR_TRIGGER".to_string(),
            exchange_order_id: eoid,
            filled_quantity: 0.0,
            avg_fill_price: ref_price,
            fee_usdc: 0.0,
            raw_error: None,
        };
    }

    if let Some(resting) = entry.get("resting") {
        let oid = parse_hl_u64_opt(resting.get("oid")).unwrap_or(0);
        let eoid = (oid > 0).then(|| format!("hl-{oid}"));
        return ExchangeOrderAck {
            accepted: true,
            status: "RESTING".to_string(),
            exchange_order_id: eoid,
            filled_quantity: 0.0,
            avg_fill_price: ref_price,
            fee_usdc: 0.0,
            raw_error: None,
        };
    }

    if let Some(filled) = entry.get("filled") {
        let oid = parse_hl_u64_opt(filled.get("oid")).unwrap_or(0);
        let eoid = (oid > 0).then(|| format!("hl-{oid}"));
        let avg_px = parse_hl_f64_opt(filled.get("avgPx"))
            .filter(|&v| v > 0.0)
            .unwrap_or(ref_price);
        let total_sz = parse_hl_f64_opt(filled.get("totalSz"))
            .filter(|&v| v > 0.0)
            .unwrap_or(0.0);
        let fee = parse_hl_f64_opt(filled.get("fee"))
            .unwrap_or_else(|| round_dp(total_sz * avg_px * (fee_bps.max(0.0) / 10_000.0), 4));

        return ExchangeOrderAck {
            accepted: true,
            status: if total_sz > 0.0 {
                "FILLED".to_string()
            } else {
                "RESTING".to_string()
            },
            exchange_order_id: eoid,
            filled_quantity: total_sz,
            avg_fill_price: avg_px,
            fee_usdc: fee,
            raw_error: None,
        };
    }

    let _ = requested_qty;
    ExchangeOrderAck {
        accepted: true,
        status: "RESTING".to_string(),
        exchange_order_id: None,
        filled_quantity: 0.0,
        avg_fill_price: ref_price,
        fee_usdc: 0.0,
        raw_error: None,
    }
}

/// Parses a single Hyperliquid `/exchange` JSON response into an [`ExchangeOrderAck`].
pub fn parse_exchange_order_ack(
    resp: &Value,
    requested_qty: f64,
    ref_price: f64,
    fee_bps: f64,
) -> ExchangeOrderAck {
    let status_str = resp.get("status").and_then(Value::as_str).unwrap_or("err");
    if status_str != "ok" {
        let err_msg = resp
            .get("response")
            .map(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| v.to_string())
            })
            .unwrap_or_else(|| "HL_EXCHANGE_STATUS_ERR".to_string());
        return ExchangeOrderAck {
            accepted: false,
            status: "REJECTED".to_string(),
            exchange_order_id: None,
            filled_quantity: 0.0,
            avg_fill_price: 0.0,
            fee_usdc: 0.0,
            raw_error: Some(err_msg),
        };
    }

    let first_opt = resp
        .pointer("/response/data/statuses")
        .and_then(Value::as_array)
        .and_then(|arr| arr.first());
    parse_exchange_order_status_entry(first_opt, requested_qty, ref_price, fee_bps)
}

/// Parses a batched Hyperliquid `/exchange` order response into one [`ExchangeOrderAck`] per submitted order (`Sec. 11`).
pub fn parse_exchange_order_acks_batch(
    resp: &Value,
    orders_meta: &[(f64, f64, f64)],
) -> Vec<ExchangeOrderAck> {
    let status_str = resp.get("status").and_then(Value::as_str).unwrap_or("err");
    if status_str != "ok" {
        let err_msg = resp
            .get("response")
            .map(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| v.to_string())
            })
            .unwrap_or_else(|| "HL_EXCHANGE_STATUS_ERR".to_string());
        let count = orders_meta.len().max(1);
        return vec![
            ExchangeOrderAck {
                accepted: false,
                status: "REJECTED".to_string(),
                exchange_order_id: None,
                filled_quantity: 0.0,
                avg_fill_price: 0.0,
                fee_usdc: 0.0,
                raw_error: Some(err_msg),
            };
            count
        ];
    }

    let statuses = resp
        .pointer("/response/data/statuses")
        .and_then(Value::as_array);
    orders_meta
        .iter()
        .enumerate()
        .map(|(idx, &(req_qty, ref_px, fee_bps))| {
            let entry = statuses.and_then(|arr| arr.get(idx));
            parse_exchange_order_status_entry(entry, req_qty, ref_px, fee_bps)
        })
        .collect()
}

/// Parses `(maker_fee_bps, taker_fee_bps)` from a Hyperliquid `userFees` `/info` JSON payload (`Sec. 11`).
pub fn parse_user_fee_rates_bps(raw: &Value) -> Option<(f64, f64)> {
    let taker_rate = parse_hl_f64_opt(raw.get("userCrossRate"))?;
    let maker_rate = parse_hl_f64_opt(raw.get("userAddRate"))?;
    let taker_bps = round_dp((taker_rate * 10_000.0).max(0.0), 4);
    let maker_bps = round_dp((maker_rate * 10_000.0).max(0.0), 4);
    if taker_bps.is_finite() && maker_bps.is_finite() {
        Some((maker_bps, taker_bps))
    } else {
        None
    }
}

const HL_USER_FUNDING_PAGE_LIMIT: usize = 500;
const HL_USER_FUNDING_MAX_PAGES: usize = 50;

fn funding_event_dedup_key(ev: &Value) -> (i64, String, String, String, String) {
    let time_ms = parse_hl_i64_opt(ev.get("time")).unwrap_or(0);
    let delta = ev.get("delta");
    let coin = delta
        .and_then(|d| d.get("coin"))
        .or_else(|| ev.get("coin"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase();
    let usdc = delta
        .and_then(|d| d.get("usdc"))
        .or_else(|| ev.get("usdc"))
        .map(|v| {
            v.as_str()
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|| v.to_string())
        })
        .unwrap_or_default();
    let hash = ev
        .get("hash")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let n_val = ev
        .get("n")
        .or_else(|| delta.and_then(|d| d.get("nSamples")))
        .map(|v| {
            v.as_str()
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|| v.to_string())
        })
        .unwrap_or_default();
    (time_ms, coin, usdc, hash, n_val)
}

/// Parsed top-of-book spread and 15 bps two-sided L2 depth snapshot from Hyperliquid `l2Book`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct L2BookDepthSnapshot {
    pub best_bid: f64,
    pub best_ask: f64,
    pub spread_bps: f64,
    pub bid_depth_15bps_usdc: f64,
    pub ask_depth_15bps_usdc: f64,
}

/// Parses a Hyperliquid `l2Book` JSON response into [`L2BookDepthSnapshot`].
///
/// Sums `px * sz` over bid levels within [`L2_DEPTH_BAND_PCT`] (`15 bps`) of `best_bid`
/// and ask levels within [`L2_DEPTH_BAND_PCT`] (`15 bps`) of `best_ask`.
pub fn parse_l2_book_depth(raw: &Value) -> Option<L2BookDepthSnapshot> {
    let levels = raw.get("levels").and_then(Value::as_array)?;
    if levels.len() < 2 {
        return None;
    }
    let bids = levels[0].as_array()?;
    let asks = levels[1].as_array()?;

    let best_bid = bids
        .first()
        .and_then(|lvl| parse_hl_f64_opt(lvl.get("px")))
        .filter(|&px| px > 0.0)?;
    let best_ask = asks
        .first()
        .and_then(|lvl| parse_hl_f64_opt(lvl.get("px")))
        .filter(|&px| px > 0.0)?;

    if best_ask < best_bid {
        return None;
    }
    let mid = (best_bid + best_ask) * 0.5;
    if !mid.is_finite() || mid <= 0.0 {
        return None;
    }
    let spread_bps = ((best_ask - best_bid) / mid) * 10_000.0;
    if !spread_bps.is_finite() {
        return None;
    }

    let bid_min_px = best_bid * (1.0 - L2_DEPTH_BAND_PCT - 1e-12);
    let ask_max_px = best_ask * (1.0 + L2_DEPTH_BAND_PCT + 1e-12);

    let mut bid_depth_15bps_usdc = 0.0;
    for lvl in bids {
        if let (Some(px), Some(sz)) = (
            parse_hl_f64_opt(lvl.get("px")),
            parse_hl_f64_opt(lvl.get("sz")),
        ) {
            if px > 0.0 && sz > 0.0 && px >= bid_min_px {
                bid_depth_15bps_usdc += px * sz;
            }
        }
    }

    let mut ask_depth_15bps_usdc = 0.0;
    for lvl in asks {
        if let (Some(px), Some(sz)) = (
            parse_hl_f64_opt(lvl.get("px")),
            parse_hl_f64_opt(lvl.get("sz")),
        ) {
            if px > 0.0 && sz > 0.0 && px <= ask_max_px {
                ask_depth_15bps_usdc += px * sz;
            }
        }
    }

    Some(L2BookDepthSnapshot {
        best_bid,
        best_ask,
        spread_bps,
        bid_depth_15bps_usdc,
        ask_depth_15bps_usdc,
    })
}

/// High-level Hyperliquid `/info` and `/exchange` client for Aegis Quant v2.0.
#[derive(Clone)]
pub struct HyperliquidHttpClient {
    pub timeout_sec: u64,
    client: Client,
    custom_governor: Option<Arc<RestWeightGovernor>>,
    ws_cache: Option<Arc<crate::ws_client::WsStateCache>>,
}

impl HyperliquidHttpClient {
    pub fn new(timeout_sec: u64) -> Self {
        let t = timeout_sec.max(1);
        Self {
            timeout_sec: t,
            client: shared_blocking_client(t),
            custom_governor: None,
            ws_cache: None,
        }
    }

    /// Attaches an explicit [`RestWeightGovernor`] (primarily for testing rate limits against loopback mocks).
    pub fn with_governor(mut self, governor: Arc<RestWeightGovernor>) -> Self {
        self.custom_governor = Some(governor);
        self
    }

    /// Attaches a shared [`crate::ws_client::WsStateCache`] so `/exchange` order mutations
    /// automatically trigger the `1_500 ms` post-submit user-cache bypass (`R14-H4`).
    pub fn with_ws_cache(mut self, ws_cache: Arc<crate::ws_client::WsStateCache>) -> Self {
        self.ws_cache = Some(ws_cache);
        self
    }

    pub fn post_info_with_options(
        &self,
        payload: &Value,
        cfg: &ConfigMap,
        opts: RequestOptions,
    ) -> Result<Value> {
        let url = resolve_info_url(cfg);
        http_post_json_with_options(
            &self.client,
            self.timeout_sec,
            &url,
            payload,
            cfg,
            opts,
            self.custom_governor.as_deref(),
        )
        .ok_or_else(|| {
            AegisError::Execution(format!("Hyperliquid /info request failed at '{url}'"))
        })
    }

    pub fn post_info(&self, payload: &Value, cfg: &ConfigMap) -> Result<Value> {
        let opts = RequestOptions::background_sync().with_timeout_sec(self.timeout_sec);
        self.post_info_with_options(payload, cfg, opts)
    }

    /// Polls `metaAndAssetCtxs` returning universe metadata and live contexts across all 150+ Hyperliquid perps.
    pub fn fetch_meta_and_asset_ctxs(&self, cfg: &ConfigMap) -> Result<Value> {
        let opts = RequestOptions::background_sync().with_timeout_sec(self.timeout_sec.min(8));
        self.post_info_with_options(&json!({ "type": "metaAndAssetCtxs" }), cfg, opts)
    }

    /// Polls `allMids` returning `coin -> mid_price` across all Hyperliquid markets (`Surveillance <= 2s`).
    pub fn fetch_all_mids(&self, cfg: &ConfigMap) -> Result<HashMap<String, f64>> {
        let opts = RequestOptions::surveillance().with_timeout_sec(self.timeout_sec.min(2));
        let raw = self.post_info_with_options(&json!({ "type": "allMids" }), cfg, opts)?;
        let mut out = HashMap::new();
        if let Some(obj) = raw.as_object() {
            for (coin, val) in obj {
                if let Some(px) = parse_hl_f64_opt(Some(val)).filter(|&p| p > 0.0) {
                    out.insert(coin.trim().to_string(), px);
                }
            }
        }
        Ok(out)
    }

    /// Polls `l2Book` for `coin` with explicit [`RequestOptions`].
    pub fn fetch_l2_book_depth_with_options(
        &self,
        coin: &str,
        cfg: &ConfigMap,
        opts: RequestOptions,
    ) -> Result<Option<L2BookDepthSnapshot>> {
        let raw =
            self.post_info_with_options(&json!({ "type": "l2Book", "coin": coin }), cfg, opts)?;
        Ok(parse_l2_book_depth(&raw))
    }

    /// Polls `l2Book` for `coin` and returns top-of-book spread and 15 bps depth snapshot if available (`Surveillance <= 2s`).
    pub fn fetch_l2_book_depth(
        &self,
        coin: &str,
        cfg: &ConfigMap,
    ) -> Result<Option<L2BookDepthSnapshot>> {
        let opts = RequestOptions::surveillance().with_timeout_sec(self.timeout_sec.min(2));
        self.fetch_l2_book_depth_with_options(coin, cfg, opts)
    }

    /// Polls `l2Book` for `coin` using `Critical` priority (for stop/exit paths).
    pub fn fetch_l2_book_critical(
        &self,
        coin: &str,
        cfg: &ConfigMap,
    ) -> Result<Option<(f64, f64)>> {
        let opts =
            RequestOptions::surveillance_critical().with_timeout_sec(self.timeout_sec.min(2));
        Ok(self
            .fetch_l2_book_depth_with_options(coin, cfg, opts)?
            .map(|snap| (snap.best_bid, snap.best_ask)))
    }

    /// Polls `l2Book` for `coin` and returns `(best_bid, best_ask)` if available (`Surveillance <= 2s`).
    pub fn fetch_l2_book(&self, coin: &str, cfg: &ConfigMap) -> Result<Option<(f64, f64)>> {
        Ok(self
            .fetch_l2_book_depth(coin, cfg)?
            .map(|snap| (snap.best_bid, snap.best_ask)))
    }

    /// Polls `candleSnapshot` for `(coin, interval)` between `start_ms` and `end_ms` (`Low` priority).
    pub fn fetch_candle_snapshot(
        &self,
        coin: &str,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
        cfg: &ConfigMap,
    ) -> Result<Vec<Value>> {
        let payload = json!({
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": start_ms,
                "endTime": end_ms,
            }
        });
        let opts = RequestOptions::low_priority_sync().with_timeout_sec(self.timeout_sec.min(8));
        let raw = self.post_info_with_options(&payload, cfg, opts)?;
        Ok(raw.as_array().cloned().unwrap_or_default())
    }

    /// Polls `clearinghouseState` for `user_wallet` with explicit [`RequestOptions`].
    pub fn fetch_clearinghouse_state_with_options(
        &self,
        user_wallet: &str,
        cfg: &ConfigMap,
        opts: RequestOptions,
    ) -> Result<Value> {
        self.post_info_with_options(
            &json!({
                "type": "clearinghouseState",
                "user": user_wallet.trim(),
            }),
            cfg,
            opts,
        )
    }

    /// Polls `clearinghouseState` for `user_wallet` (`Surveillance <= 2s`).
    pub fn fetch_clearinghouse_state(&self, user_wallet: &str, cfg: &ConfigMap) -> Result<Value> {
        let opts = RequestOptions::surveillance().with_timeout_sec(self.timeout_sec.min(2));
        self.fetch_clearinghouse_state_with_options(user_wallet, cfg, opts)
    }

    /// Polls `spotClearinghouseState` for `user_wallet` (used for unified/spot USDC balance checks).
    pub fn fetch_spot_clearinghouse_state(
        &self,
        user_wallet: &str,
        cfg: &ConfigMap,
    ) -> Result<Value> {
        let opts = RequestOptions::background_sync().with_timeout_sec(self.timeout_sec.min(8));
        self.post_info_with_options(
            &json!({
                "type": "spotClearinghouseState",
                "user": user_wallet.trim(),
            }),
            cfg,
            opts,
        )
    }

    /// Polls `userAbstraction` for `user_wallet` (e.g. `"default"`, `"unifiedAccount"`, `"portfolioMargin"`).
    /// Returns `Ok(None)` when the exchange payload is not a non-empty JSON string.
    pub fn fetch_user_abstraction(
        &self,
        user_wallet: &str,
        cfg: &ConfigMap,
    ) -> Result<Option<String>> {
        let opts = RequestOptions::background_sync().with_timeout_sec(self.timeout_sec.min(8));
        let raw = self.post_info_with_options(
            &json!({
                "type": "userAbstraction",
                "user": user_wallet.trim(),
            }),
            cfg,
            opts,
        )?;
        Ok(raw
            .as_str()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()))
    }

    /// Polls `portfolio` for `user_wallet` (`Low` priority).
    pub fn fetch_user_portfolio(&self, user_wallet: &str, cfg: &ConfigMap) -> Result<Value> {
        let opts = RequestOptions::low_priority_sync().with_timeout_sec(self.timeout_sec.min(8));
        self.post_info_with_options(
            &json!({
                "type": "portfolio",
                "user": user_wallet.trim(),
            }),
            cfg,
            opts,
        )
    }

    /// Polls `userVaultEquities` for `user_wallet` returning Hyperliquid vault equity records (`Low` priority).
    pub fn fetch_user_vault_equities(
        &self,
        user_wallet: &str,
        cfg: &ConfigMap,
    ) -> Result<Vec<Value>> {
        let opts = RequestOptions::low_priority_sync().with_timeout_sec(self.timeout_sec.min(8));
        let raw = self.post_info_with_options(
            &json!({
                "type": "userVaultEquities",
                "user": user_wallet.trim(),
            }),
            cfg,
            opts,
        )?;
        raw.as_array().cloned().ok_or_else(|| {
            AegisError::Execution(
                "Hyperliquid userVaultEquities response was not a JSON array".to_string(),
            )
        })
    }

    /// Polls `vaultDetails` for `(vault_address, user_wallet)` (`Low` priority, 4s bounded timeout).
    /// Returns `Ok(None)` when Hyperliquid responds with JSON `null` (unknown vault).
    pub fn fetch_vault_details(
        &self,
        vault_address: &str,
        user_wallet: &str,
        cfg: &ConfigMap,
    ) -> Result<Option<Value>> {
        let mut payload = json!({
            "type": "vaultDetails",
            "vaultAddress": vault_address.trim(),
        });
        if !user_wallet.trim().is_empty() {
            payload["user"] = json!(user_wallet.trim());
        }
        let opts = RequestOptions::low_priority_sync().with_timeout_sec(self.timeout_sec.min(4));
        let raw = self.post_info_with_options(&payload, cfg, opts)?;
        if raw.is_null() {
            return Ok(None);
        }
        if raw.is_object() {
            return Ok(Some(raw));
        }
        Err(AegisError::Execution(format!(
            "Hyperliquid vaultDetails response for '{}' was not a JSON object",
            vault_address.trim()
        )))
    }

    /// Polls `openOrders` for `user_wallet`.
    pub fn fetch_open_orders(&self, user_wallet: &str, cfg: &ConfigMap) -> Result<Vec<Value>> {
        let opts = RequestOptions::surveillance().with_timeout_sec(self.timeout_sec.min(2));
        self.fetch_open_orders_with_options(user_wallet, cfg, opts)
    }

    fn fetch_open_orders_with_options(
        &self,
        user_wallet: &str,
        cfg: &ConfigMap,
        opts: RequestOptions,
    ) -> Result<Vec<Value>> {
        let raw = self.post_info_with_options(
            &json!({
                "type": "openOrders",
                "user": user_wallet.trim(),
            }),
            cfg,
            opts,
        )?;
        Ok(raw.as_array().cloned().unwrap_or_default())
    }

    /// Polls `frontendOpenOrders` for `user_wallet` (includes trigger/SL/TP metadata and `oid`/`cloid`).
    pub fn fetch_frontend_open_orders(
        &self,
        user_wallet: &str,
        cfg: &ConfigMap,
    ) -> Result<Vec<Value>> {
        let opts = RequestOptions::surveillance().with_timeout_sec(self.timeout_sec.min(2));
        self.fetch_frontend_open_orders_with_options(user_wallet, cfg, opts)
    }

    fn fetch_frontend_open_orders_with_options(
        &self,
        user_wallet: &str,
        cfg: &ConfigMap,
        opts: RequestOptions,
    ) -> Result<Vec<Value>> {
        let raw = self.post_info_with_options(
            &json!({
                "type": "frontendOpenOrders",
                "user": user_wallet.trim(),
            }),
            cfg,
            opts,
        )?;
        Ok(raw.as_array().cloned().unwrap_or_default())
    }

    /// Polls `frontendOpenOrders` first (`metadata_complete = true`), falling back to `openOrders`
    /// with `metadata_complete = false` so callers never mistake trigger-stripped responses for complete metadata (`R14-M7`).
    pub fn fetch_open_orders_with_metadata_opts(
        &self,
        user_wallet: &str,
        cfg: &ConfigMap,
        opts: RequestOptions,
    ) -> Result<OpenOrdersSnapshot> {
        match self.fetch_frontend_open_orders_with_options(user_wallet, cfg, opts) {
            Ok(orders) => Ok(OpenOrdersSnapshot {
                orders,
                metadata_complete: true,
            }),
            Err(front_err) => {
                tracing::warn!(
                    error = %front_err,
                    "frontendOpenOrders failed; falling back to openOrders with metadata_complete=false"
                );
                let orders = self.fetch_open_orders_with_options(user_wallet, cfg, opts)?;
                Ok(OpenOrdersSnapshot {
                    orders,
                    metadata_complete: false,
                })
            }
        }
    }

    pub fn fetch_open_orders_with_metadata(
        &self,
        user_wallet: &str,
        cfg: &ConfigMap,
    ) -> Result<OpenOrdersSnapshot> {
        let opts = RequestOptions::surveillance().with_timeout_sec(self.timeout_sec.min(2));
        self.fetch_open_orders_with_metadata_opts(user_wallet, cfg, opts)
    }

    /// Queries Hyperliquid `orderStatus` by `0x...` `cloid` hex (weight `2`) (`R14-M1`).
    /// Returns `Ok(None)` when the exchange responds with `status = "unknownOid"`.
    pub fn fetch_order_status_by_cloid(
        &self,
        user_wallet: &str,
        cloid_hex: &str,
        cfg: &ConfigMap,
    ) -> Result<Option<ExchangeOrderStatus>> {
        let opts =
            RequestOptions::surveillance_critical().with_timeout_sec(self.timeout_sec.min(2));
        let raw = self.post_info_with_options(
            &json!({
                "type": "orderStatus",
                "user": user_wallet.trim(),
                "oid": cloid_hex.trim(),
            }),
            cfg,
            opts,
        )?;

        let top_status = raw.get("status").and_then(Value::as_str).unwrap_or("");
        if top_status.eq_ignore_ascii_case("unknownOid") || raw.is_null() {
            return Ok(None);
        }

        let order_container = raw.get("order").unwrap_or(&raw);
        let inner = order_container.get("order").unwrap_or(order_container);
        let status_label = order_container
            .get("status")
            .and_then(Value::as_str)
            .or_else(|| inner.get("status").and_then(Value::as_str))
            .unwrap_or(top_status)
            .trim()
            .to_string();

        if status_label.is_empty() || status_label.eq_ignore_ascii_case("unknownOid") {
            return Ok(None);
        }

        let oid = parse_hl_u64_opt(inner.get("oid"))
            .or_else(|| parse_hl_u64_opt(order_container.get("oid")))
            .unwrap_or(0);
        let orig_sz = parse_hl_f64_opt(inner.get("origSz"))
            .or_else(|| parse_hl_f64_opt(inner.get("sz")))
            .unwrap_or(0.0);
        let remaining_sz = parse_hl_f64_opt(inner.get("sz")).unwrap_or(0.0);
        let limit_px = parse_hl_f64_opt(inner.get("limitPx")).unwrap_or(0.0);
        let avg_px = parse_hl_f64_opt(inner.get("avgPx"))
            .or_else(|| parse_hl_f64_opt(order_container.get("avgPx")))
            .filter(|&p| p > 0.0)
            .unwrap_or(limit_px);
        let filled_sz = if status_label.eq_ignore_ascii_case("filled") && remaining_sz <= 1e-12 {
            orig_sz
        } else {
            (orig_sz - remaining_sz).max(0.0)
        };
        let is_trigger = aegis_core::is_hl_trigger_order(inner);
        let cloid = inner
            .get("cloid")
            .and_then(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        Ok(Some(ExchangeOrderStatus {
            oid,
            status: status_label,
            orig_sz,
            remaining_sz,
            filled_sz,
            limit_px,
            avg_px,
            is_trigger,
            cloid,
        }))
    }

    /// Polls `userFills` for `user_wallet` returning historical trade fills from Hyperliquid L1.
    pub fn fetch_user_fills(&self, user_wallet: &str, cfg: &ConfigMap) -> Result<Vec<Value>> {
        let opts = RequestOptions::background_sync().with_timeout_sec(self.timeout_sec.min(8));
        let raw = self.post_info_with_options(
            &json!({
                "type": "userFills",
                "user": user_wallet.trim(),
            }),
            cfg,
            opts,
        )?;
        Ok(raw.as_array().cloned().unwrap_or_default())
    }

    /// Polls `userFillsByTime` for `user_wallet` starting at `start_time_ms` (`R14-H4`, `R14-M4`).
    pub fn fetch_user_fills_by_time(
        &self,
        user_wallet: &str,
        start_time_ms: i64,
        end_time_ms: Option<i64>,
        aggregate_by_time: bool,
        cfg: &ConfigMap,
    ) -> Result<Vec<Value>> {
        let mut payload = json!({
            "type": "userFillsByTime",
            "user": user_wallet.trim(),
            "startTime": start_time_ms.max(0),
            "aggregateByTime": aggregate_by_time,
        });
        if let Some(end_ms) = end_time_ms {
            payload["endTime"] = json!(end_ms.max(0));
        }
        let opts = RequestOptions::background_sync().with_timeout_sec(self.timeout_sec.min(8));
        let raw = self.post_info_with_options(&payload, cfg, opts)?;
        Ok(raw.as_array().cloned().unwrap_or_default())
    }

    /// Polls `userFees` for `user_wallet` returning account maker/taker fee tiers (`Sec. 11`).
    pub fn fetch_user_fees(&self, user_wallet: &str, cfg: &ConfigMap) -> Result<Value> {
        let opts = RequestOptions::background_sync().with_timeout_sec(self.timeout_sec.min(8));
        self.post_info_with_options(
            &json!({
                "type": "userFees",
                "user": user_wallet.trim(),
            }),
            cfg,
            opts,
        )
    }

    /// Polls `extraAgents` for `user_wallet` returning registered API agent wallets and expiry timestamps (`Sec. 11`).
    pub fn fetch_extra_agents(&self, user_wallet: &str, cfg: &ConfigMap) -> Result<Vec<Value>> {
        let opts = RequestOptions::background_sync().with_timeout_sec(self.timeout_sec.min(8));
        let raw = self.post_info_with_options(
            &json!({
                "type": "extraAgents",
                "user": user_wallet.trim(),
            }),
            cfg,
            opts,
        )?;
        Ok(raw.as_array().cloned().unwrap_or_default())
    }

    /// Polls `userFunding` for `user_wallet` returning paginated historical funding ledger entries from Hyperliquid L1 (`Low` priority).
    pub fn fetch_user_funding(
        &self,
        user_wallet: &str,
        start_time_ms: Option<i64>,
        cfg: &ConfigMap,
    ) -> Result<Vec<Value>> {
        self.fetch_user_funding_paginated(
            user_wallet,
            start_time_ms,
            HL_USER_FUNDING_PAGE_LIMIT,
            HL_USER_FUNDING_MAX_PAGES,
            cfg,
        )
    }

    fn fetch_user_funding_paginated(
        &self,
        user_wallet: &str,
        start_time_ms: Option<i64>,
        page_limit: usize,
        max_pages: usize,
        cfg: &ConfigMap,
    ) -> Result<Vec<Value>> {
        let mut cursor_ms =
            start_time_ms.unwrap_or_else(|| (aegis_core::now_ms() - 90 * 86_400_000).max(0));
        let trimmed_user = user_wallet.trim();
        let page_limit = page_limit.max(1);
        let max_pages = max_pages.max(1);

        let mut out: Vec<Value> = Vec::new();
        let mut seen: HashSet<(i64, String, String, String, String)> = HashSet::new();
        let opts = RequestOptions::low_priority_sync().with_timeout_sec(self.timeout_sec.min(8));

        for page_idx in 0..max_pages {
            let raw = match self.post_info_with_options(
                &json!({
                    "type": "userFunding",
                    "user": trimmed_user,
                    "startTime": cursor_ms,
                }),
                cfg,
                opts,
            ) {
                Ok(v) => v,
                Err(e) => {
                    if page_idx == 0 {
                        return Err(e);
                    }
                    tracing::warn!(
                        page = page_idx,
                        cursor_ms,
                        error = %e,
                        "Hyperliquid userFunding pagination interrupted; returning partial ledger"
                    );
                    break;
                }
            };
            let page = raw.as_array().cloned().unwrap_or_default();
            let page_len = page.len();
            if page_len == 0 {
                break;
            }

            let mut max_time_ms: Option<i64> = None;
            let mut added_on_page = 0usize;
            for ev in page {
                if let Some(t) = parse_hl_i64_opt(ev.get("time")) {
                    max_time_ms = Some(max_time_ms.map_or(t, |m| m.max(t)));
                }
                if seen.insert(funding_event_dedup_key(&ev)) {
                    out.push(ev);
                    added_on_page += 1;
                }
            }

            if page_len < page_limit {
                break;
            }
            let Some(page_max) = max_time_ms else {
                break;
            };
            if page_max > cursor_ms {
                // Re-query inclusively at `page_max` so multiple coins sharing the boundary
                // timestamp past the 500-record cutoff are never skipped; `seen` deduplicates overlap.
                cursor_ms = page_max;
            } else if page_max == cursor_ms {
                // Saturated single millisecond (`>= page_limit` events at `cursor_ms`): advance past it.
                cursor_ms = page_max.saturating_add(1);
            } else if added_on_page > 0 {
                cursor_ms = cursor_ms.saturating_add(1);
            } else {
                // `page_max < cursor_ms` and zero new items: server ignored `startTime`.
                break;
            }
        }
        Ok(out)
    }

    /// Signs and posts an EIP-712 L1 action to `/exchange` with automatic single-retry on nonce collision (`5s` timeout, `0` HTTP retries).
    pub fn post_exchange_action<T: Serialize>(
        &self,
        action: &T,
        private_key: &str,
        db: Option<&DatabaseManager>,
        cfg: &ConfigMap,
    ) -> Result<Value> {
        let is_testnet = is_testnet_configured(cfg);
        let url = resolve_exchange_url_for_network(cfg, is_testnet);
        let opts = RequestOptions::exchange_action().with_timeout_sec(self.timeout_sec.min(5));
        if let Some(ref cache) = self.ws_cache {
            cache.mark_order_submit();
        }
        for attempt in 0..2 {
            let nonce = next_monotonic_nonce(db);
            let sig = sign_l1_action(private_key, action, None, nonce, None, !is_testnet)?;
            let payload = json!({
                "action": serde_json::to_value(action).unwrap_or_default(),
                "nonce": nonce,
                "signature": {
                    "r": sig["r"],
                    "s": sig["s"],
                    "v": sig["v"],
                },
                "vaultAddress": Value::Null,
            });
            let Some(resp) = http_post_json_with_options(
                &self.client,
                self.timeout_sec,
                &url,
                &payload,
                cfg,
                opts,
                self.custom_governor.as_deref(),
            ) else {
                return Err(AegisError::Execution(format!(
                    "Hyperliquid /exchange request failed at '{url}'"
                )));
            };
            if attempt == 0 {
                let resp_lower = resp.to_string().to_ascii_lowercase();
                let is_err = resp.get("status").and_then(Value::as_str) != Some("ok");
                if is_err && resp_lower.contains("nonce") {
                    tracing::warn!(
                        nonce = nonce,
                        "Hyperliquid rejected nonce; bumping high-water mark and retrying once"
                    );
                    let _ = bump_nonce_high_water(1_000, db);
                    continue;
                }
            }
            return Ok(resp);
        }
        Err(AegisError::Execution(
            "Hyperliquid /exchange nonce retry exhausted".to_string(),
        ))
    }

    /// Submits a single [`OrderWire`] to `/exchange` and parses its [`ExchangeOrderAck`].
    /// Trigger orders (`t.trigger.is_some()`) first try `grouping = "positionTpsl"` and fall back once to `"na"`.
    #[allow(clippy::too_many_arguments)]
    pub fn submit_order_wire(
        &self,
        order: OrderWire,
        requested_qty: f64,
        ref_price: f64,
        fee_bps: f64,
        private_key: &str,
        db: Option<&DatabaseManager>,
        cfg: &ConfigMap,
    ) -> Result<ExchangeOrderAck> {
        let is_trigger = order.t.trigger.is_some();
        let primary_grouping = if is_trigger { "positionTpsl" } else { "na" };
        let action = OrderAction {
            action_type: "order".to_string(),
            orders: vec![order.clone()],
            grouping: primary_grouping.to_string(),
        };
        let resp = self.post_exchange_action(&action, private_key, db, cfg)?;
        let ack = parse_exchange_order_ack(&resp, requested_qty, ref_price, fee_bps);
        if is_trigger && !ack.accepted {
            let fallback_action = OrderAction {
                action_type: "order".to_string(),
                orders: vec![order],
                grouping: "na".to_string(),
            };
            if let Ok(fb_resp) = self.post_exchange_action(&fallback_action, private_key, db, cfg) {
                return Ok(parse_exchange_order_ack(
                    &fb_resp,
                    requested_qty,
                    ref_price,
                    fee_bps,
                ));
            }
        }
        Ok(ack)
    }

    /// Submits a batch of [`OrderWire`]s to `/exchange` chunked at `<= 39` actions per signed payload (`Sec. 11`).
    pub fn submit_orders_batch(
        &self,
        orders: Vec<OrderWire>,
        orders_meta: &[(f64, f64, f64)],
        grouping: &str,
        private_key: &str,
        db: Option<&DatabaseManager>,
        cfg: &ConfigMap,
    ) -> Result<Vec<ExchangeOrderAck>> {
        if orders.is_empty() {
            return Ok(Vec::new());
        }
        let mut all_acks = Vec::with_capacity(orders.len());
        let chunk_sz = HL_EXCHANGE_OPTIMAL_BATCH_CHUNK.max(1);
        for (chunk_idx, order_chunk) in orders.chunks(chunk_sz).enumerate() {
            let start = chunk_idx * chunk_sz;
            let end = (start + order_chunk.len()).min(orders_meta.len());
            let mut chunk_meta = Vec::with_capacity(order_chunk.len());
            for idx in 0..order_chunk.len() {
                if start + idx < end {
                    chunk_meta.push(orders_meta[start + idx]);
                } else {
                    chunk_meta.push((0.0, 0.0, 4.5));
                }
            }
            let action = OrderAction {
                action_type: "order".to_string(),
                orders: order_chunk.to_vec(),
                grouping: grouping.to_string(),
            };
            let resp = self.post_exchange_action(&action, private_key, db, cfg)?;
            let acks = parse_exchange_order_acks_batch(&resp, &chunk_meta);
            all_acks.extend(acks);
        }
        Ok(all_acks)
    }

    /// Cancels a resting order on Hyperliquid by `(asset_index, oid)`.
    pub fn cancel_order_wire(
        &self,
        asset_index: u32,
        oid: u64,
        private_key: &str,
        db: Option<&DatabaseManager>,
        cfg: &ConfigMap,
    ) -> Result<Value> {
        self.cancel_orders_batch(
            vec![CancelWire {
                a: asset_index,
                o: oid,
            }],
            private_key,
            db,
            cfg,
        )
    }

    /// Cancels a batch of resting orders on Hyperliquid, chunked at `<= 39` actions per signed `/exchange` request (`Sec. 11`).
    pub fn cancel_orders_batch(
        &self,
        cancels: Vec<CancelWire>,
        private_key: &str,
        db: Option<&DatabaseManager>,
        cfg: &ConfigMap,
    ) -> Result<Value> {
        if cancels.is_empty() {
            return Ok(json!({ "status": "ok" }));
        }
        let chunk_sz = HL_EXCHANGE_OPTIMAL_BATCH_CHUNK.max(1);
        if cancels.len() <= chunk_sz {
            let action = CancelAction {
                action_type: "cancel".to_string(),
                cancels,
            };
            return self.post_exchange_action(&action, private_key, db, cfg);
        }

        let mut merged_statuses: Vec<Value> = Vec::with_capacity(cancels.len());
        let mut last_resp = json!({ "status": "ok" });
        for chunk in cancels.chunks(chunk_sz) {
            let action = CancelAction {
                action_type: "cancel".to_string(),
                cancels: chunk.to_vec(),
            };
            let resp = self.post_exchange_action(&action, private_key, db, cfg)?;
            if resp.get("status").and_then(Value::as_str) != Some("ok") {
                return Ok(resp);
            }
            if let Some(statuses) = resp
                .pointer("/response/data/statuses")
                .and_then(Value::as_array)
            {
                merged_statuses.extend(statuses.iter().cloned());
            }
            last_resp = resp;
        }
        if !merged_statuses.is_empty() {
            Ok(json!({
                "status": "ok",
                "response": {
                    "type": "cancel",
                    "data": { "statuses": merged_statuses }
                }
            }))
        } else {
            Ok(last_resp)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_test_support::mock_http::{MockHttpResponse, MockHttpServer};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_url_validation_and_ack_parsing() {
        let cfg = ConfigMap::new();
        assert!(validate_hyperliquid_endpoint_url(
            "https://api.hyperliquid-testnet.xyz/info",
            &cfg
        ));
        assert!(validate_hyperliquid_endpoint_url(
            "http://127.0.0.1:18080/info",
            &cfg
        ));
        assert!(!validate_hyperliquid_endpoint_url(
            "http://api.hyperliquid.xyz/info",
            &cfg
        ));
        assert!(!validate_hyperliquid_endpoint_url(
            "https://evil.example.com/info",
            &cfg
        ));

        let resting_json = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        { "resting": { "oid": 987654321u64 } }
                    ]
                }
            }
        });
        let ack = parse_exchange_order_ack(&resting_json, 1.5, 184.25, 1.5);
        assert!(ack.accepted);
        assert_eq!(ack.status, "RESTING");
        assert_eq!(ack.exchange_order_id.as_deref(), Some("hl-987654321"));

        let filled_json = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        { "filled": { "oid": 123456u64, "totalSz": "14.5", "avgPx": "184.25" } }
                    ]
                }
            }
        });
        let f_ack = parse_exchange_order_ack(&filled_json, 14.5, 184.0, 1.5);
        assert!(f_ack.accepted);
        assert_eq!(f_ack.status, "FILLED");
        assert_eq!(f_ack.exchange_order_id.as_deref(), Some("hl-123456"));
        assert!((f_ack.filled_quantity - 14.5).abs() < 1e-9);
        assert!((f_ack.avg_fill_price - 184.25).abs() < 1e-9);

        // Batch ack parser handles mixed resting + rejected + waitingForTrigger statuses
        let batch_resp = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        { "waitingForTrigger": { "oid": 5001u64 } },
                        { "error": "Insufficient margin" }
                    ]
                }
            }
        });
        let batch_acks =
            parse_exchange_order_acks_batch(&batch_resp, &[(1.0, 100.0, 4.5), (2.0, 200.0, 4.5)]);
        assert_eq!(batch_acks.len(), 2);
        assert!(batch_acks[0].accepted);
        assert_eq!(batch_acks[0].exchange_order_id.as_deref(), Some("hl-5001"));
        assert!(!batch_acks[1].accepted);
        assert_eq!(
            batch_acks[1].raw_error.as_deref(),
            Some("Insufficient margin")
        );
    }

    #[test]
    fn test_live_mode_routes_to_mainnet_even_with_stale_testnet_flag() {
        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "LIVE");
        cfg.insert("hyperliquid_testnet", "true");
        assert!(!is_testnet_configured(&cfg));
        assert_eq!(resolve_info_url(&cfg), HYPERLIQUID_INFO_URL);
        assert_eq!(resolve_exchange_url(&cfg), HYPERLIQUID_EXCHANGE_URL);

        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet", "false");
        assert!(is_testnet_configured(&cfg));
        assert_eq!(resolve_info_url(&cfg), HYPERLIQUID_TESTNET_INFO_URL);
    }

    #[test]
    fn test_fetch_user_funding_paginates_and_deduplicates_boundary() {
        let mut ledger: Vec<Value> = Vec::new();
        let base_ts = 1_795_000_000_000i64;
        for i in 0..498 {
            ledger.push(json!({
                "time": base_ts + i,
                "hash": format!("0x{:04x}", i),
                "delta": { "type": "funding", "coin": "BTC", "usdc": "0.10" }
            }));
        }
        let boundary1 = base_ts + 500;
        for (idx, coin) in ["ETH", "SOL", "ARB", "AVAX", "DOGE"].iter().enumerate() {
            ledger.push(json!({
                "time": boundary1,
                "hash": "0x0000",
                "delta": { "type": "funding", "coin": coin, "usdc": format!("0.2{}", idx) }
            }));
        }
        for i in 0..493 {
            ledger.push(json!({
                "time": boundary1 + 100 + i,
                "hash": format!("0x1{:04x}", i),
                "delta": { "type": "funding", "coin": "OP", "usdc": "-0.05" }
            }));
        }
        let boundary2 = boundary1 + 1_000;
        for coin in ["SUI", "TAO", "PYTH"] {
            ledger.push(json!({
                "time": boundary2,
                "hash": "0x0000",
                "delta": { "type": "funding", "coin": coin, "usdc": "0.75" }
            }));
        }
        for i in 0..4 {
            ledger.push(json!({
                "time": boundary2 + 100 + i,
                "hash": format!("0x2{:04x}", i),
                "delta": { "type": "funding", "coin": "MET", "usdc": "1.25" }
            }));
        }
        let expected_total = ledger.len();

        let requested_cursors = Arc::new(Mutex::new(Vec::<i64>::new()));
        let cursors_clone = Arc::clone(&requested_cursors);
        let ledger_arc = Arc::new(ledger);
        let ledger_clone = Arc::clone(&ledger_arc);

        let srv = MockHttpServer::start_json(move |_path, body| {
            let start_time = body.get("startTime").and_then(Value::as_i64).unwrap_or(0);
            cursors_clone.lock().push(start_time);
            let slice: Vec<Value> = ledger_clone
                .iter()
                .filter(|ev| ev["time"].as_i64().unwrap() >= start_time)
                .take(HL_USER_FUNDING_PAGE_LIMIT)
                .cloned()
                .collect();
            (200, Value::Array(slice))
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");

        let client = HyperliquidHttpClient::new(5);
        let fetched = client
            .fetch_user_funding(
                "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
                Some(base_ts),
                &cfg,
            )
            .expect("paginated userFunding fetch succeeds");

        assert_eq!(fetched.len(), expected_total);
        let keys: HashSet<_> = fetched.iter().map(funding_event_dedup_key).collect();
        assert_eq!(keys.len(), expected_total);
        for coin in ["ETH", "SOL", "ARB", "AVAX", "DOGE"] {
            assert!(keys
                .iter()
                .any(|(t, c, _, _, _)| *t == boundary1 && c == coin));
        }
        for coin in ["SUI", "TAO", "PYTH"] {
            assert!(keys
                .iter()
                .any(|(t, c, _, _, _)| *t == boundary2 && c == coin));
        }
        let cursors = requested_cursors.lock().clone();
        assert_eq!(cursors, vec![base_ts, boundary1, boundary2]);
    }

    #[test]
    fn test_fetch_user_funding_saturated_ms_and_n_dedup() {
        let ledger = Arc::new(vec![
            json!({ "time": 1000, "hash": "0x0", "n": 1, "delta": { "type": "funding", "coin": "BTC", "usdc": "0.1" } }),
            json!({ "time": 1000, "hash": "0x0", "n": 2, "delta": { "type": "funding", "coin": "BTC", "usdc": "0.1" } }),
            json!({ "time": 1000, "hash": "0x0", "n": 3, "delta": { "type": "funding", "coin": "ETH", "usdc": "0.2" } }),
            json!({ "time": 2000, "hash": "0x1", "n": 4, "delta": { "type": "funding", "coin": "SOL", "usdc": "0.3" } }),
        ]);
        let ledger_clone = Arc::clone(&ledger);
        let srv = MockHttpServer::start_json(move |_path, body| {
            let start_time = body.get("startTime").and_then(Value::as_i64).unwrap_or(0);
            let slice: Vec<Value> = ledger_clone
                .iter()
                .filter(|ev| ev["time"].as_i64().unwrap() >= start_time)
                .take(3)
                .cloned()
                .collect();
            (200, Value::Array(slice))
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");

        let client = HyperliquidHttpClient::new(5);
        let fetched = client
            .fetch_user_funding_paginated("0xabc", Some(1000), 3, 10, &cfg)
            .expect("saturated ms pagination succeeds");
        assert_eq!(fetched.len(), 4);
    }

    #[test]
    fn test_info_retry_backoff_helpers() {
        assert!(is_idempotent_info_url(
            "https://api.hyperliquid-testnet.xyz/info"
        ));
        assert!(is_idempotent_info_url("http://127.0.0.1:18080/info/"));
        assert!(!is_idempotent_info_url(
            "https://api.hyperliquid-testnet.xyz/exchange"
        ));
        assert!(!is_idempotent_info_url("http://127.0.0.1:18080/exchange/"));
        assert!(!is_idempotent_info_url("http://127.0.0.1:18080/info/other"));

        for code in [429u16, 502, 503, 504] {
            assert!(is_retryable_info_status(code));
        }
        for code in [200u16, 400, 401, 404, 500] {
            assert!(!is_retryable_info_status(code));
        }

        assert_eq!(compute_info_retry_backoff_ms(0, None), 250);
        assert_eq!(compute_info_retry_backoff_ms(1, None), 650);
        assert_eq!(
            compute_info_retry_backoff_ms(0, Some("Wed, 21 Oct 2015 07:28:00 GMT")),
            250
        );
        assert_eq!(compute_info_retry_backoff_ms(0, Some("0")), 0);
        assert_eq!(compute_info_retry_backoff_ms(0, Some("1")), 1_000);
        assert_eq!(compute_info_retry_backoff_ms(1, Some("10")), 2_000);

        // Verify jitter stays bounded in [0.75, 1.25] * 1000ms before cap
        for _ in 0..20 {
            let (ms, raw) = compute_info_retry_backoff_ms_with_opts(0, 429, Some("1"), 2_000, true);
            assert_eq!(raw, 1_000);
            assert!((750..=1_250).contains(&ms));
        }
    }

    #[test]
    fn test_http_post_json_retries_info_only_and_never_retries_exchange() {
        let info_calls = Arc::new(AtomicUsize::new(0));
        let exchange_calls = Arc::new(AtomicUsize::new(0));
        let scenario = Arc::new(Mutex::new("info_429_once"));

        let info_calls_srv = Arc::clone(&info_calls);
        let exchange_calls_srv = Arc::clone(&exchange_calls);
        let scenario_srv = Arc::clone(&scenario);

        let srv = MockHttpServer::start(move |req| {
            let mode = *scenario_srv.lock();
            if req.path == "/info" {
                let n = info_calls_srv.fetch_add(1, Ordering::SeqCst) + 1;
                return match mode {
                    "info_429_once" => {
                        if n == 1 {
                            MockHttpResponse::json(429, &json!({ "error": "rate_limited" }))
                                .with_header("Retry-After", "0")
                        } else {
                            MockHttpResponse::ok_json(&json!({ "ok": true, "attempt": n }))
                        }
                    }
                    "info_429_then_502_then_ok" => {
                        if n == 1 {
                            MockHttpResponse::json(429, &json!({ "error": "rate_limited" }))
                        } else if n == 2 {
                            MockHttpResponse::json(502, &json!({ "error": "bad_gateway" }))
                                .with_header("Retry-After", "0")
                        } else {
                            MockHttpResponse::ok_json(&json!({ "ok": true, "attempt": n }))
                        }
                    }
                    "info_retry_after_clamped" => {
                        if n == 1 {
                            MockHttpResponse::json(429, &json!({ "error": "rate_limited" }))
                                .with_header("Retry-After", "10")
                        } else {
                            MockHttpResponse::ok_json(&json!({ "clamped_ok": true, "attempt": n }))
                        }
                    }
                    "info_exhaust_503" => {
                        MockHttpResponse::json(503, &json!({ "error": "unavailable" }))
                    }
                    _ => MockHttpResponse::ok_json(&json!({ "ok": true })),
                };
            }
            if req.path == "/exchange" {
                exchange_calls_srv.fetch_add(1, Ordering::SeqCst);
                return match mode {
                    "exchange_429" => {
                        MockHttpResponse::json(429, &json!({ "error": "rate_limited" }))
                            .with_header("Retry-After", "0")
                    }
                    "exchange_502" => {
                        MockHttpResponse::json(502, &json!({ "error": "bad_gateway" }))
                    }
                    _ => MockHttpResponse::ok_json(&json!({ "status": "ok" })),
                };
            }
            MockHttpResponse::json(404, &json!({ "error": "not_found" }))
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");

        let client = shared_blocking_client_opts(5, true);
        let info_url = srv.url("/info");
        let exchange_url = srv.url("/exchange");
        let payload = json!({ "type": "allMids" });

        // 1. /info retries 429 once and succeeds with JSON
        *scenario.lock() = "info_429_once";
        info_calls.store(0, Ordering::SeqCst);
        let res1 = http_post_json(&client, 5, 5, &info_url, &payload, &cfg)
            .expect("/info should succeed after 1 retry on 429");
        assert_eq!(res1, json!({ "ok": true, "attempt": 2 }));
        assert_eq!(info_calls.load(Ordering::SeqCst), 2);

        // 2. /info retries twice (429 -> 502 -> 200) and succeeds with JSON
        *scenario.lock() = "info_429_then_502_then_ok";
        info_calls.store(0, Ordering::SeqCst);
        let res2 = http_post_json(&client, 5, 5, &info_url, &payload, &cfg)
            .expect("/info should succeed after 2 retries across 429 and 502");
        assert_eq!(res2, json!({ "ok": true, "attempt": 3 }));
        assert_eq!(info_calls.load(Ordering::SeqCst), 3);

        // 3. /info honors integer Retry-After jittered & clamped to <= 2000ms (10s header does NOT sleep 10s)
        *scenario.lock() = "info_retry_after_clamped";
        info_calls.store(0, Ordering::SeqCst);
        let started = Instant::now();
        let res3 = http_post_json(&client, 5, 5, &info_url, &payload, &cfg)
            .expect("/info should succeed after clamped Retry-After");
        let elapsed = started.elapsed();
        assert_eq!(res3, json!({ "clamped_ok": true, "attempt": 2 }));
        assert_eq!(info_calls.load(Ordering::SeqCst), 2);
        assert!(elapsed >= Duration::from_millis(1_800));
        assert!(elapsed < Duration::from_secs(5));

        // 4. /info bounds retries at max 2 (3 total attempts) on persistent 503
        *scenario.lock() = "info_exhaust_503";
        info_calls.store(0, Ordering::SeqCst);
        let res4 = http_post_json(
            &client,
            5,
            5,
            &info_url,
            &json!({ "type": "frontendOpenOrders" }),
            &cfg,
        );
        assert!(res4.is_none());
        assert_eq!(info_calls.load(Ordering::SeqCst), 3);

        // 5. /exchange NEVER retries on 429 or 502
        *scenario.lock() = "exchange_429";
        exchange_calls.store(0, Ordering::SeqCst);
        let ex_429 = http_post_json(
            &client,
            5,
            5,
            &exchange_url,
            &json!({ "action": { "type": "order" } }),
            &cfg,
        );
        assert!(ex_429.is_none());
        assert_eq!(exchange_calls.load(Ordering::SeqCst), 1);

        *scenario.lock() = "exchange_502";
        exchange_calls.store(0, Ordering::SeqCst);
        let ex_502 = http_post_json(
            &client,
            5,
            5,
            &exchange_url,
            &json!({ "action": { "type": "cancel" } }),
            &cfg,
        );
        assert!(ex_502.is_none());
        assert_eq!(exchange_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_parse_l2_book_depth_multi_level_boundaries_and_malformed_books() {
        let book = json!({
            "coin": "SOL",
            "levels": [
                [
                    { "px": "100.00", "sz": "2.0", "n": 1 },
                    { "px": "99.90", "sz": "3.0", "n": 2 },
                    { "px": 99.85, "sz": 1.0, "n": 1 },
                    { "px": "99.84", "sz": "50.0", "n": 4 }
                ],
                [
                    { "px": "100.10", "sz": "1.5", "n": 1 },
                    { "px": "100.25", "sz": "2.0", "n": 2 },
                    { "px": "100.26", "sz": "100.0", "n": 5 }
                ]
            ]
        });
        let snap = parse_l2_book_depth(&book).expect("valid multi-level l2Book should parse");
        assert!((snap.best_bid - 100.0).abs() < 1e-9);
        assert!((snap.best_ask - 100.10).abs() < 1e-9);
        let expected_spread_bps = ((100.10 - 100.0) / 100.05) * 10_000.0;
        assert!((snap.spread_bps - expected_spread_bps).abs() < 1e-6);
        let expected_bid_depth = 100.0 * 2.0 + 99.90 * 3.0 + 99.85 * 1.0;
        let expected_ask_depth = 100.10 * 1.5 + 100.25 * 2.0;
        assert!((snap.bid_depth_15bps_usdc - expected_bid_depth).abs() < 1e-6);
        assert!((snap.ask_depth_15bps_usdc - expected_ask_depth).abs() < 1e-6);

        let locked = json!({
            "levels": [
                [{ "px": "10.0", "sz": "5.0" }],
                [{ "px": "10.0", "sz": "4.0" }]
            ]
        });
        let locked_snap = parse_l2_book_depth(&locked).expect("locked book should parse");
        assert!((locked_snap.spread_bps - 0.0).abs() < 1e-9);
        assert!((locked_snap.bid_depth_15bps_usdc - 50.0).abs() < 1e-6);
        assert!((locked_snap.ask_depth_15bps_usdc - 40.0).abs() < 1e-6);

        let wide = json!({
            "levels": [
                [{ "px": "0.009973", "sz": "50000" }],
                [{ "px": "0.010027", "sz": "50000" }]
            ]
        });
        let wide_snap = parse_l2_book_depth(&wide).expect("wide book should parse");
        assert!((wide_snap.spread_bps - 54.0).abs() < 1e-6);

        let no_sz = json!({
            "levels": [
                [{ "px": "100.0" }],
                [{ "px": "100.1" }]
            ]
        });
        let no_sz_snap = parse_l2_book_depth(&no_sz).expect("px-only book should parse");
        assert!((no_sz_snap.best_bid - 100.0).abs() < 1e-9);
        assert!((no_sz_snap.best_ask - 100.1).abs() < 1e-9);
        assert_eq!(no_sz_snap.bid_depth_15bps_usdc, 0.0);
        assert_eq!(no_sz_snap.ask_depth_15bps_usdc, 0.0);

        for bad in [
            json!({}),
            json!({ "levels": [] }),
            json!({ "levels": [[{"px": "100.0", "sz": "1.0"}]] }),
            json!({ "levels": [[], [{"px": "100.0", "sz": "1.0"}]] }),
            json!({ "levels": [[{"px": "100.0", "sz": "1.0"}], []] }),
            json!({ "levels": [[{"px": "101.0", "sz": "1.0"}], [{"px": "100.0", "sz": "1.0"}]] }),
            json!({ "levels": [[{"px": "0.0", "sz": "1.0"}], [{"px": "100.0", "sz": "1.0"}]] }),
            json!({ "levels": [[{"px": "bad", "sz": "1.0"}], [{"px": "100.0", "sz": "1.0"}]] }),
        ] {
            assert!(
                parse_l2_book_depth(&bad).is_none(),
                "Expected None for malformed/empty book: {bad}"
            );
        }
    }

    #[test]
    fn test_fetch_l2_book_depth_and_bbo_delegation() {
        let srv = MockHttpServer::start_json(|_path, body| {
            let coin = body.get("coin").and_then(Value::as_str).unwrap_or("");
            if coin == "SOL" {
                (
                    200,
                    json!({
                        "levels": [
                            [{"px": "184.20", "sz": "2.0"}, {"px": "183.50", "sz": "50.0"}],
                            [{"px": "184.29", "sz": "3.0"}, {"px": "185.00", "sz": "50.0"}]
                        ]
                    }),
                )
            } else {
                (200, json!({ "levels": [[], []] }))
            }
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");

        let client = HyperliquidHttpClient::new(5);
        let depth = client
            .fetch_l2_book_depth("SOL", &cfg)
            .unwrap()
            .expect("SOL depth snapshot");
        assert!((depth.best_bid - 184.20).abs() < 1e-9);
        assert!((depth.best_ask - 184.29).abs() < 1e-9);
        assert!((depth.bid_depth_15bps_usdc - 368.40).abs() < 1e-6);
        assert!((depth.ask_depth_15bps_usdc - 552.87).abs() < 1e-6);

        let bbo = client
            .fetch_l2_book("SOL", &cfg)
            .unwrap()
            .expect("SOL BBO tuple");
        assert_eq!(bbo, (depth.best_bid, depth.best_ask));
        assert!(client.fetch_l2_book_depth("EMPTY", &cfg).unwrap().is_none());
    }

    #[test]
    fn test_fetch_user_vault_equities_and_vault_details() {
        let srv = MockHttpServer::start_json(|_path, body| {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            match typ {
                "userVaultEquities" => {
                    let user = body.get("user").and_then(Value::as_str).unwrap_or("");
                    if user == "0xbad" {
                        (200, json!({ "error": "unexpected object" }))
                    } else {
                        (
                            200,
                            json!([{
                                "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
                                "equity": "202.400584",
                                "lockedUntilTimestamp": 1_793_779_590_878i64
                            }]),
                        )
                    }
                }
                "vaultDetails" => {
                    let addr = body
                        .get("vaultAddress")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if addr == "0x0000000000000000000000000000000000000000" {
                        (200, Value::Null)
                    } else {
                        (
                            200,
                            json!({
                                "name": "Hyperliquidity Provider (HLP)",
                                "vaultAddress": addr,
                                "leader": "0x677d831aef5328190852e24f13c46cac05f984e7",
                                "isClosed": false,
                                "apr": 0.0814,
                                "followerState": {
                                    "vaultEquity": "202.400584",
                                    "pnl": "0.92",
                                    "allTimePnl": "1.34",
                                    "daysFollowing": 25,
                                    "lockupUntil": 1_793_779_590_878i64
                                }
                            }),
                        )
                    }
                }
                _ => (404, json!({ "error": "unknown" })),
            }
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");

        let client = HyperliquidHttpClient::new(5);
        let eqs = client
            .fetch_user_vault_equities("0x2c7536e3605d9c16a7a3d7b1898e529396a65c23", &cfg)
            .expect("userVaultEquities succeeds");
        assert_eq!(eqs.len(), 1);
        assert_eq!(
            eqs[0]["vaultAddress"],
            "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303"
        );

        assert!(client.fetch_user_vault_equities("0xbad", &cfg).is_err());

        let det = client
            .fetch_vault_details(
                "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
                "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
                &cfg,
            )
            .expect("vaultDetails succeeds")
            .expect("vaultDetails object present");
        assert_eq!(det["name"], "Hyperliquidity Provider (HLP)");

        let null_det = client
            .fetch_vault_details(
                "0x0000000000000000000000000000000000000000",
                "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
                &cfg,
            )
            .expect("null vaultDetails returns Ok(None)");
        assert!(null_det.is_none());
    }

    #[test]
    fn test_r14_h4_rest_weight_governor_priority_lanes_variable_debits_and_loopback_bypass() {
        // Loopback hosts auto-bypass
        assert!(RestWeightGovernor::for_url("http://127.0.0.1:8080/info", 900).is_none());
        assert!(RestWeightGovernor::for_url("http://localhost:8080/info", 900).is_none());
        assert!(RestWeightGovernor::for_url("http://[::1]:8080/info", 900).is_none());
        assert!(RestWeightGovernor::for_url("https://api.hyperliquid.xyz/info", 900).is_some());

        let gov = Arc::new(RestWeightGovernor::new_enforced(900));
        assert!((gov.snapshot_tokens() - 600.0).abs() < 1e-6);

        // Drain Low priority (floor 450): 7 requests * 20 = 140 -> 460 remaining, 8th (20 -> 440 < 450) sheds!
        for _ in 0..7 {
            assert!(gov.try_acquire(RequestPriority::Low, 20).is_ok());
        }
        assert!(gov.try_acquire(RequestPriority::Low, 20).is_err());

        // Variable item debit (e.g. 121 candles -> ceil(121/60) = 3)
        let candle_req = json!({ "type": "candleSnapshot" });
        let candle_resp = Value::Array(vec![json!({}); 121]);
        assert_eq!(
            RestWeightGovernor::variable_item_debit(&candle_req, &candle_resp),
            3
        );
        let fills_req = json!({ "type": "userFillsByTime" });
        let fills_resp = Value::Array(vec![json!({}); 41]);
        assert_eq!(
            RestWeightGovernor::variable_item_debit(&fills_req, &fills_resp),
            3
        );

        // Normal (floor 300) and High (floor 150) remain available; Critical (floor 0) retains 150 exclusive reserve!
        assert!(gov.try_acquire(RequestPriority::Normal, 20).is_ok());
        for _ in 0..14 {
            let _ = gov.try_acquire(RequestPriority::High, 20);
        }
        assert!(gov.try_acquire(RequestPriority::High, 20).is_err());
        assert!(gov.try_acquire(RequestPriority::Critical, 20).is_ok());
        assert!(gov.try_acquire(RequestPriority::Critical, 2).is_ok());

        // Wire governor to MockHttpServer and verify Low candleSnapshot is shed while Critical passes
        let srv = MockHttpServer::start_json(|path, _body| {
            if path == "/exchange" {
                (200, json!({ "status": "ok" }))
            } else {
                (200, json!([]))
            }
        });
        let gov2 = Arc::new(RestWeightGovernor::new_enforced(900));
        // Exhaust tokens down to ~455 (above Low floor by only 5)
        for _ in 0..7 {
            let _ = gov2.try_acquire(RequestPriority::Low, 20);
        }
        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");
        let client = HyperliquidHttpClient::new(5).with_governor(Arc::clone(&gov2));
        // CandleSnapshot (weight 20, Low) sheds immediately without hitting server!
        assert!(client
            .fetch_candle_snapshot("BTC", "1h", 0, 1000, &cfg)
            .is_err());
        assert_eq!(srv.request_count(), 0);
        // Critical l2Book (weight 2) still hits server!
        let _ = client.fetch_l2_book_critical("BTC", &cfg);
        assert_eq!(srv.request_count(), 1);
    }

    #[test]
    fn test_r14_m7_m1_sec11_open_orders_metadata_order_status_fees_agents_and_39_chunking() {
        let srv = MockHttpServer::start(move |req| {
            if req.path == "/info" {
                match req
                    .json_body
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                {
                    "frontendOpenOrders" => {
                        if req.json_body.get("user").and_then(Value::as_str) == Some("0xfailfront")
                        {
                            MockHttpResponse::json(500, &json!({ "error": "boom" }))
                        } else {
                            MockHttpResponse::ok_json(&json!([{ "coin": "ETH", "oid": 11 }]))
                        }
                    }
                    "openOrders" => {
                        MockHttpResponse::ok_json(&json!([{ "coin": "ETH", "oid": 22 }]))
                    }
                    "orderStatus" => {
                        match req
                            .json_body
                            .get("oid")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                        {
                            "0xunknown" => {
                                MockHttpResponse::ok_json(&json!({ "status": "unknownOid" }))
                            }
                            _ => MockHttpResponse::ok_json(&json!({
                                "status": "order",
                                "order": {
                                    "status": "filled",
                                    "order": {
                                        "oid": 77777u64,
                                        "origSz": "10.0",
                                        "sz": "0.0",
                                        "limitPx": "180.0",
                                        "avgPx": "179.95",
                                        "cloid": "0xfilled"
                                    }
                                }
                            })),
                        }
                    }
                    "userFillsByTime" => {
                        MockHttpResponse::ok_json(&json!([{ "coin": "ETH", "px": "180.0" }]))
                    }
                    "userFees" => MockHttpResponse::ok_json(&json!({
                        "userCrossRate": "0.00042",
                        "userAddRate": "0.00012"
                    })),
                    "extraAgents" => MockHttpResponse::ok_json(
                        &json!([{ "name": "aegis", "address": "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23", "validUntil": 9999999999999i64 }]),
                    ),
                    _ => MockHttpResponse::ok_json(&json!([])),
                }
            } else {
                let n = req
                    .json_body
                    .pointer("/action/orders")
                    .and_then(Value::as_array)
                    .map(Vec::len)
                    .or_else(|| {
                        req.json_body
                            .pointer("/action/cancels")
                            .and_then(Value::as_array)
                            .map(Vec::len)
                    })
                    .unwrap_or(1);
                let statuses: Vec<Value> = (0..n)
                    .map(|i| json!({ "resting": { "oid": (1000 + i) as u64 } }))
                    .collect();
                MockHttpResponse::ok_json(&json!({
                    "status": "ok",
                    "response": { "type": "order", "data": { "statuses": statuses } }
                }))
            }
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");

        let client = HyperliquidHttpClient::new(5);

        let front = client
            .fetch_open_orders_with_metadata("0xok", &cfg)
            .unwrap();
        assert!(front.metadata_complete);
        assert_eq!(front.orders.len(), 1);

        let fallback = client
            .fetch_open_orders_with_metadata("0xfailfront", &cfg)
            .unwrap();
        assert!(!fallback.metadata_complete);
        assert_eq!(fallback.orders.len(), 1);

        assert!(client
            .fetch_order_status_by_cloid("0xok", "0xunknown", &cfg)
            .unwrap()
            .is_none());
        let filled = client
            .fetch_order_status_by_cloid("0xok", "0xfilled", &cfg)
            .unwrap()
            .unwrap();
        assert_eq!(filled.oid, 77777);
        assert_eq!(filled.status, "filled");
        assert!((filled.filled_sz - 10.0).abs() < 1e-6);
        assert!((filled.avg_px - 179.95).abs() < 1e-6);

        assert_eq!(
            client
                .fetch_user_fills_by_time("0xok", 1000, None, true, &cfg)
                .unwrap()
                .len(),
            1
        );
        let fees = client.fetch_user_fees("0xok", &cfg).unwrap();
        assert_eq!(parse_user_fee_rates_bps(&fees), Some((1.2, 4.2)));
        assert_eq!(client.fetch_extra_agents("0xok", &cfg).unwrap().len(), 1);

        // 45 cancels chunked at 39 -> 2 /exchange calls (39 + 6)
        srv.clear_requests();
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let cancels: Vec<CancelWire> = (1..=45).map(|o| CancelWire { a: 0, o }).collect();
        let cancel_res = client.cancel_orders_batch(cancels, pk, None, &cfg).unwrap();
        assert_eq!(
            cancel_res
                .pointer("/response/data/statuses")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(45)
        );
        let ex_reqs: Vec<_> = srv
            .requests()
            .into_iter()
            .filter(|r| r.path == "/exchange")
            .collect();
        assert_eq!(ex_reqs.len(), 2);
        assert_eq!(
            ex_reqs[0]
                .json_body
                .pointer("/action/cancels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(39)
        );
        assert_eq!(
            ex_reqs[1]
                .json_body
                .pointer("/action/cancels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(6)
        );
    }

    #[test]
    #[should_panic(expected = "HTTP IO forbidden under execution_lock")]
    fn test_r14_h2_debug_assert_catches_http_under_execution_lock() {
        let srv = MockHttpServer::start_static(200, &json!({}));
        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_testnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");

        let client = HyperliquidHttpClient::new(2);
        let _guard = crate::lock_guard::acquire_execution_lock();
        let _ = client.fetch_all_mids(&cfg);
    }
}
