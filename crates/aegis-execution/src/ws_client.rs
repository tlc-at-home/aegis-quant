//! 2-Connection Hyperliquid WebSocket Market+User Stream Manager & Lock-Free/RwLock Cache
//! (`aegis-execution::ws_client`, `R14-H4` Phase 2, Section 11).
//!
//! Manages two independent WebSocket connections with SSRF URL validation, paced subscription
//! replay (`<= 20 msgs/sec`), `25s` JSON ping / `40s` stale-frame watchdog, jittered exponential
//! backoff (`500ms..=15s`, reset after `30s` healthy), and automatic REST gap-fill triggering
//! (`needs_rest_gap_reconcile`).
//!
//! Invariant: Network loops NEVER acquire `execution_lock()`.

use crate::http_client::{
    is_testnet_configured, parse_l2_book_depth, L2BookDepthSnapshot, OpenOrdersSnapshot,
};
use aegis_core::{
    normalize_hyperliquid_symbol, now_ms, parse_hl_f64, parse_hl_f64_opt, parse_hl_i64_opt,
    parse_hl_u64_opt, validate_evm_address, AegisError, ConfigMap, ExchangeAsset, OhlcvBar, Result,
};
use aegis_db::DatabaseManager;
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

pub const HYPERLIQUID_MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const HYPERLIQUID_TESTNET_WS_URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";

pub const WS_MIDS_FRESH_MS: i64 = 5_000;
pub const WS_L2_BOOK_FRESH_MS: i64 = 3_000;
pub const WS_CLEARINGHOUSE_FRESH_MS: i64 = 5_000;
pub const WS_OPEN_ORDERS_FRESH_MS: i64 = 5_000;
pub const WS_ORDER_SUBMIT_BYPASS_MS: i64 = 1_500;
pub const WS_ACTIVE_CTX_FRESH_MS: i64 = 15_000;
pub const WS_CANDLE_FRESH_MS: i64 = 90_000;

pub const MAX_MARKET_WS_SUBSCRIPTIONS: usize = 110;
pub const MAX_L2_BOOK_WS_SUBSCRIPTIONS: usize = 16;
pub const WS_CANDLES_RING_CAP: usize = 200;
pub const WS_USER_FILLS_RING_CAP: usize = 500;
pub const WS_USER_FUNDINGS_RING_CAP: usize = 500;
pub const WS_ORDER_UPDATES_RING_CAP: usize = 500;
pub const DEFAULT_CANDLE_REFRESH_MAX_ASSETS_WS: usize = 30;

type CandleKey = (String, String);
type FillDedupKey = (i64, u64, u64, String, String, u64, u64);
type FundingDedupKey = (i64, String, u64, u64, String);

/// Timing and pacing configuration for [`HyperliquidWsManager`].
#[derive(Debug, Clone, Copy)]
pub struct WsTimingConfig {
    pub ping_interval: Duration,
    pub stale_timeout: Duration,
    pub connect_timeout: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    pub healthy_reset_duration: Duration,
    pub subscribe_pace_interval: Duration,
    pub status_persist_interval: Duration,
}

impl Default for WsTimingConfig {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(25),
            stale_timeout: Duration::from_secs(40),
            connect_timeout: Duration::from_secs(10),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(15),
            healthy_reset_duration: Duration::from_secs(30),
            // 50 ms spacing = 20 msgs/sec (safely under Hyperliquid 2,000 msgs/min limit).
            subscribe_pace_interval: Duration::from_millis(50),
            status_persist_interval: Duration::from_secs(15),
        }
    }
}

/// Generic timestamped cache entry stored in [`WsStateCache`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Stamped<T> {
    pub value: T,
    pub updated_at_ms: i64,
}

/// Live `activeAssetCtx` market context snapshot streamed over Hyperliquid WS.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct WsActiveAssetCtx {
    pub coin: String,
    pub mark_px: f64,
    pub oracle_px: f64,
    pub mid_px: f64,
    pub prev_day_px: f64,
    pub funding_rate_hourly: f64,
    /// Open interest in base coins (multiply by `mark_px` for USDC notional).
    pub open_interest: f64,
    pub day_ntl_vlm: f64,
}

/// Individual Hyperliquid `orderUpdates` event normalized for fast order status lookup.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WsOrderUpdate {
    pub oid: u64,
    pub cloid: Option<String>,
    pub coin: String,
    pub side: String,
    pub limit_px: f64,
    pub sz: f64,
    pub orig_sz: f64,
    pub status: String,
    pub status_timestamp_ms: i64,
    pub raw: Value,
}

/// Persisted stream health telemetry written to `system_config.ws_stream_status_json`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WsStreamStatus {
    pub enabled: bool,
    pub ws_url_redacted: String,
    pub market_connected: bool,
    pub user_connected: bool,
    pub market_reconnect_count: u64,
    pub user_reconnect_count: u64,
    pub market_last_frame_ms: i64,
    pub user_last_frame_ms: i64,
    pub market_last_pong_ms: i64,
    pub user_last_pong_ms: i64,
    pub market_subscription_count: usize,
    pub user_subscription_count: usize,
    pub needs_rest_gap_reconcile: bool,
    pub mids_age_ms: Option<i64>,
    pub clearinghouse_age_ms: Option<i64>,
    pub updated_at_ms: i64,
    pub status: String,
}

/// Dynamic Market WS subscription set watched via `tokio::sync::watch::Receiver<WsSubscriptionSet>`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WsSubscriptionSet {
    /// Prioritized coins for `activeAssetCtx` + `candle` (`1h`, `4h`) streams.
    pub ctx_and_candle_coins: Vec<String>,
    /// Prioritized coins for `l2Book` streams (held/ordered + top-10 candidates).
    pub l2_book_coins: Vec<String>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum MarketSubKey {
    AllMids,
    ActiveAssetCtx(String),
    Candle {
        coin: String,
        interval: &'static str,
    },
    L2Book(String),
}

impl MarketSubKey {
    fn to_subscription_json(&self) -> Value {
        match self {
            Self::AllMids => json!({ "type": "allMids" }),
            Self::ActiveAssetCtx(coin) => json!({ "type": "activeAssetCtx", "coin": coin }),
            Self::Candle { coin, interval } => json!({
                "type": "candle",
                "coin": coin,
                "interval": interval,
            }),
            Self::L2Book(coin) => json!({ "type": "l2Book", "coin": coin }),
        }
    }
}

fn push_market_key(
    out: &mut Vec<MarketSubKey>,
    seen: &mut HashSet<MarketSubKey>,
    key: MarketSubKey,
) {
    if out.len() < MAX_MARKET_WS_SUBSCRIPTIONS && seen.insert(key.clone()) {
        out.push(key);
    }
}

impl WsSubscriptionSet {
    /// Expands this set into deduplicated Market WS subscription keys capped at
    /// [`MAX_MARKET_WS_SUBSCRIPTIONS`] (`110`, comfortably below HL's 1,000 limit).
    fn to_market_keys(&self) -> Vec<MarketSubKey> {
        let mut out = Vec::with_capacity(MAX_MARKET_WS_SUBSCRIPTIONS);
        let mut seen = HashSet::with_capacity(MAX_MARKET_WS_SUBSCRIPTIONS);

        push_market_key(&mut out, &mut seen, MarketSubKey::AllMids);

        // Subscribe L2 books first for held/ordered/top-10 entry candidates
        for raw_coin in self.l2_book_coins.iter().take(MAX_L2_BOOK_WS_SUBSCRIPTIONS) {
            let coin = normalize_ws_coin_name(raw_coin);
            if !coin.is_empty() {
                push_market_key(&mut out, &mut seen, MarketSubKey::L2Book(coin));
            }
        }

        // Subscribe activeAssetCtx + 1h/4h candles in priority order up to 110 total subs
        for raw_coin in &self.ctx_and_candle_coins {
            if out.len() + 3 > MAX_MARKET_WS_SUBSCRIPTIONS {
                break;
            }
            let coin = normalize_ws_coin_name(raw_coin);
            if coin.is_empty() {
                continue;
            }
            push_market_key(
                &mut out,
                &mut seen,
                MarketSubKey::ActiveAssetCtx(coin.clone()),
            );
            push_market_key(
                &mut out,
                &mut seen,
                MarketSubKey::Candle {
                    coin: coin.clone(),
                    interval: "1h",
                },
            );
            push_market_key(
                &mut out,
                &mut seen,
                MarketSubKey::Candle {
                    coin,
                    interval: "4h",
                },
            );
        }

        out
    }

    /// Total bounded Market WS subscription count that will be requested on the wire.
    pub fn total_subscription_count(&self) -> usize {
        self.to_market_keys().len()
    }
}

fn normalize_ws_coin_name(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    normalize_hyperliquid_symbol(trimmed)
        .map(|(_, c)| c)
        .unwrap_or_else(|_| trimmed.to_ascii_uppercase())
}

/// Builds a prioritized [`WsSubscriptionSet`] from current exchange assets and active position IDs.
pub fn build_ws_subscription_set_from_assets(
    assets: &[ExchangeAsset],
    active_asset_ids: &HashSet<i64>,
    candle_refresh_max_assets: usize,
) -> WsSubscriptionSet {
    let cap = candle_refresh_max_assets.clamp(5, 150);

    let mut active_coins: Vec<String> = Vec::new();
    let mut priority_coins: Vec<String> = Vec::new();
    let mut eligible_assets: Vec<&ExchangeAsset> = Vec::new();

    for a in assets {
        if active_asset_ids.contains(&a.id) {
            active_coins.push(a.coin.clone());
        } else if a.is_priority_watched {
            priority_coins.push(a.coin.clone());
        } else if a.is_core_eligible || a.is_event_eligible {
            eligible_assets.push(a);
        }
    }
    eligible_assets.sort_by(|a, b| b.day_volume_usdc.total_cmp(&a.day_volume_usdc));

    let mut ctx_and_candle_coins = Vec::new();
    let mut seen_ctx = HashSet::new();
    let mut push_ctx = |c: &str| {
        let norm = normalize_ws_coin_name(c);
        if !norm.is_empty() && seen_ctx.insert(norm.to_ascii_uppercase()) {
            ctx_and_candle_coins.push(norm);
        }
    };

    for c in &active_coins {
        push_ctx(c);
    }
    for c in &priority_coins {
        push_ctx(c);
    }
    for a in eligible_assets.iter().take(cap) {
        push_ctx(&a.coin);
    }

    let mut l2_book_coins = Vec::new();
    let mut seen_l2 = HashSet::new();
    let push_l2 = |out: &mut Vec<String>, seen: &mut HashSet<String>, c: &str| {
        if out.len() >= MAX_L2_BOOK_WS_SUBSCRIPTIONS {
            return;
        }
        let norm = normalize_ws_coin_name(c);
        if !norm.is_empty() && seen.insert(norm.to_ascii_uppercase()) {
            out.push(norm);
        }
    };

    for c in &active_coins {
        push_l2(&mut l2_book_coins, &mut seen_l2, c);
    }
    for c in &priority_coins {
        if l2_book_coins.len() >= 10 + active_coins.len() {
            break;
        }
        push_l2(&mut l2_book_coins, &mut seen_l2, c);
    }
    let mut top_candidates: Vec<&ExchangeAsset> = assets
        .iter()
        .filter(|a| a.is_core_eligible || a.is_event_eligible || a.is_priority_watched)
        .collect();
    top_candidates.sort_by(|a, b| b.day_volume_usdc.total_cmp(&a.day_volume_usdc));
    for a in top_candidates.into_iter().take(10) {
        push_l2(&mut l2_book_coins, &mut seen_l2, &a.coin);
    }

    WsSubscriptionSet {
        ctx_and_candle_coins,
        l2_book_coins,
    }
}

/// Convenience helper loading active held/ordered IDs and `exchange_assets` from `db`
/// without holding `execution_lock()`.
pub fn build_ws_subscription_set_from_db(
    db: &DatabaseManager,
    cfg: &ConfigMap,
) -> WsSubscriptionSet {
    let mut active_ids = HashSet::new();
    for h in db.get_assets_held().unwrap_or_default() {
        active_ids.insert(h.asset_id);
    }
    for o in db.get_assets_ordered().unwrap_or_default() {
        active_ids.insert(o.asset_id);
    }
    let assets = db.list_exchange_assets().unwrap_or_default();
    let max_assets = cfg.get_usize(
        "candle_refresh_max_assets",
        DEFAULT_CANDLE_REFRESH_MAX_ASSETS_WS,
    );
    build_ws_subscription_set_from_assets(&assets, &active_ids, max_assets)
}

fn extract_ws_host_from_authority(authority: &str) -> Option<String> {
    if authority.is_empty() || authority.contains('@') || authority.chars().any(char::is_whitespace)
    {
        return None;
    }
    if authority.starts_with('[') {
        let end_idx = authority.find(']')?;
        let host = authority[..=end_idx].to_ascii_lowercase();
        let rest = &authority[end_idx + 1..];
        if !rest.is_empty() {
            let port_str = rest.strip_prefix(':')?;
            if port_str.is_empty() || port_str.parse::<u16>().is_err() {
                return None;
            }
        }
        return Some(host);
    }
    if let Some((host_part, port_part)) = authority.split_once(':') {
        if host_part.is_empty()
            || port_part.is_empty()
            || port_part.contains(':')
            || port_part.parse::<u16>().is_err()
        {
            return None;
        }
        Some(host_part.trim().to_ascii_lowercase())
    } else {
        Some(authority.trim().to_ascii_lowercase())
    }
}

/// Returns `true` if `url` is a valid `ws://` or `wss://` loopback endpoint
/// (`127.0.0.1`, `localhost`, or `[::1]`).
pub fn is_loopback_ws_url(url: &str) -> bool {
    let trimmed = url.trim();
    let rest = if let Some(r) = trimmed.strip_prefix("wss://") {
        r
    } else if let Some(r) = trimmed.strip_prefix("ws://") {
        r
    } else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let Some(host) = extract_ws_host_from_authority(authority) else {
        return false;
    };
    host == "127.0.0.1" || host == "localhost" || host == "[::1]"
}

/// SSRF-safe Hyperliquid WebSocket URL validator (`R14-H4` Phase 2).
///
/// Requires `wss://` and a host present in `system_config.hyperliquid_allowed_hosts`
/// (default `api.hyperliquid.xyz,api.hyperliquid-testnet.xyz`), while permitting
/// `ws://` (and `wss://`) on loopback hosts (`127.0.0.1`, `localhost`, `[::1]`) for
/// local `MockWsServer` integration tests.
pub fn validate_hyperliquid_ws_url(url: &str, cfg: &ConfigMap) -> bool {
    let trimmed = url.trim();
    if trimmed.is_empty() || trimmed.chars().any(char::is_control) {
        return false;
    }

    let (is_wss, rest) = if let Some(r) = trimmed.strip_prefix("wss://") {
        (true, r)
    } else if let Some(r) = trimmed.strip_prefix("ws://") {
        (false, r)
    } else {
        tracing::warn!(
            url = trimmed,
            "Rejected Hyperliquid WS URL without ws:// or wss:// scheme"
        );
        return false;
    };

    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let Some(host) = extract_ws_host_from_authority(authority) else {
        tracing::warn!(
            url = trimmed,
            "Rejected Hyperliquid WS URL with malformed authority"
        );
        return false;
    };

    if host == "127.0.0.1" || host == "localhost" || host == "[::1]" {
        return true;
    }

    if !is_wss {
        tracing::warn!(
            url = trimmed,
            host = %host,
            "Rejected insecure non-loopback ws:// Hyperliquid URL (wss:// required)"
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
            "Rejected untrusted Hyperliquid WS host not in hyperliquid_allowed_hosts"
        );
    }
    is_allowed
}

/// Derives and validates a Hyperliquid WebSocket URL from either an explicit `ws://`/`wss://` URL
/// or an HTTP/HTTPS API base URL (`https://api.hyperliquid.xyz` -> `wss://api.hyperliquid.xyz/ws`).
pub fn derive_hyperliquid_ws_url(raw_url: &str, cfg: &ConfigMap) -> Option<String> {
    let trimmed = raw_url.trim();
    if trimmed.is_empty() || trimmed.chars().any(char::is_control) {
        return None;
    }

    let candidate = if trimmed.starts_with("wss://") || trimmed.starts_with("ws://") {
        let scheme_len = if trimmed.starts_with("wss://") { 6 } else { 5 };
        let after_scheme = &trimmed[scheme_len..];
        if let Some(slash_idx) = after_scheme.find('/') {
            let path = &after_scheme[slash_idx..];
            if path == "/" {
                format!("{}/ws", &trimmed[..scheme_len + slash_idx])
            } else {
                trimmed.to_string()
            }
        } else if is_loopback_ws_url(trimmed) {
            trimmed.to_string()
        } else {
            format!("{trimmed}/ws")
        }
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        if authority.is_empty() {
            return None;
        }
        format!("wss://{authority}/ws")
    } else {
        let rest = trimmed.strip_prefix("http://")?;
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        if authority.is_empty() {
            return None;
        }
        format!("ws://{authority}/ws")
    };

    validate_hyperliquid_ws_url(&candidate, cfg).then_some(candidate)
}

/// Canonical Hyperliquid WS endpoint for Testnet or Mainnet.
pub fn canonical_hyperliquid_ws_url(is_testnet: bool) -> &'static str {
    if is_testnet {
        HYPERLIQUID_TESTNET_WS_URL
    } else {
        HYPERLIQUID_MAINNET_WS_URL
    }
}

/// Resolves and SSRF-validates the active Hyperliquid WebSocket URL from `system_config`:
/// 1. Explicit `hyperliquid_ws_url` override
/// 2. `hyperliquid_api_url` derivation
/// 3. Network HTTP base (`hyperliquid_testnet_url` / `hyperliquid_mainnet_url`) derivation
/// 4. Canonical Mainnet/Testnet WS fallback
pub fn resolve_hyperliquid_ws_url(cfg: &ConfigMap) -> String {
    let is_testnet = is_testnet_configured(cfg);
    let explicit_ws = cfg.get_str("hyperliquid_ws_url", "");
    if !explicit_ws.trim().is_empty() {
        if let Some(valid) = derive_hyperliquid_ws_url(&explicit_ws, cfg) {
            return valid;
        }
    }

    let api_url = cfg.get_str("hyperliquid_api_url", "");
    if !api_url.trim().is_empty() {
        if let Some(valid) = derive_hyperliquid_ws_url(&api_url, cfg) {
            return valid;
        }
    }

    let base_url = if is_testnet {
        cfg.get_str(
            "hyperliquid_testnet_url",
            "https://api.hyperliquid-testnet.xyz",
        )
    } else {
        cfg.get_str("hyperliquid_mainnet_url", "https://api.hyperliquid.xyz")
    };
    if let Some(valid) = derive_hyperliquid_ws_url(&base_url, cfg) {
        return valid;
    }

    canonical_hyperliquid_ws_url(is_testnet).to_string()
}

/// Returns `true` when `system_config` explicitly specifies a loopback WS endpoint via
/// `hyperliquid_ws_url` or `hyperliquid_api_url`.
pub fn has_explicit_loopback_ws_url(cfg: &ConfigMap) -> bool {
    for key in ["hyperliquid_ws_url", "hyperliquid_api_url"] {
        let raw = cfg.get_str(key, "");
        if !raw.trim().is_empty() {
            if let Some(derived) = derive_hyperliquid_ws_url(&raw, cfg) {
                if is_loopback_ws_url(&derived) {
                    return true;
                }
            }
        }
    }
    false
}

/// Determines whether `ExecutionEngine::spawn_background_runtime` should start `HyperliquidWsManager`.
///
/// Disabled cleanly in HTTP-only ephemeral test DBs unless an explicit loopback WS URL is configured.
pub fn should_enable_ws_manager(db: &DatabaseManager, cfg: &ConfigMap) -> bool {
    if !cfg.get_bool("hl_websocket_enabled", true) {
        return false;
    }
    if !cfg.execution_mode().is_live_or_testnet() {
        return false;
    }
    let ws_is_loopback = has_explicit_loopback_ws_url(cfg);
    if db.is_ephemeral_test_db() && !ws_is_loopback {
        return false;
    }
    let ws_url = resolve_hyperliquid_ws_url(cfg);
    validate_hyperliquid_ws_url(&ws_url, cfg)
}

/// Computes reconnect backoff in milliseconds (`min_ms..=max_ms`) with optional `0.75..=1.25` jitter.
pub fn compute_ws_reconnect_backoff(
    attempt: u32,
    min_ms: u64,
    max_ms: u64,
    with_jitter: bool,
) -> Duration {
    let eff_min = min_ms.max(1);
    let eff_max = max_ms.max(eff_min);
    let mult = 1u64.checked_shl(attempt.min(16)).unwrap_or(u64::MAX);
    let raw_ms = eff_min.saturating_mul(mult).clamp(eff_min, eff_max);
    let jittered_ms = if with_jitter {
        let factor = rand::thread_rng().gen_range(0.75_f64..=1.25_f64);
        ((raw_ms as f64) * factor)
            .round()
            .clamp(eff_min as f64, eff_max as f64) as u64
    } else {
        raw_ms
    };
    Duration::from_millis(jittered_ms.clamp(eff_min, eff_max))
}

#[derive(Default)]
struct WsUserRingState {
    user_fills_ring: VecDeque<Value>,
    seen_fill_keys: HashSet<FillDedupKey>,
    user_fills_updated_at_ms: i64,
    user_fundings_ring: VecDeque<Value>,
    seen_funding_keys: HashSet<FundingDedupKey>,
    user_fundings_updated_at_ms: i64,
    order_updates_ring: VecDeque<WsOrderUpdate>,
    order_updates_by_oid: HashMap<u64, WsOrderUpdate>,
    order_updates_by_cloid: HashMap<String, WsOrderUpdate>,
}

/// Shared, thread-safe in-memory WebSocket state cache (`Arc<WsStateCache>`) consumed by
/// `market_watcher`, `priority_monitor`, `order_chaser`, `reconcile`, and `engine`.
pub struct WsStateCache {
    all_mids: RwLock<Option<Stamped<HashMap<String, f64>>>>,
    active_asset_ctxs: RwLock<HashMap<String, Stamped<WsActiveAssetCtx>>>,
    candles: RwLock<HashMap<CandleKey, Stamped<VecDeque<OhlcvBar>>>>,
    l2_books: RwLock<HashMap<String, Stamped<L2BookDepthSnapshot>>>,
    clearinghouse_state: RwLock<Option<Stamped<Value>>>,
    open_orders: RwLock<Option<Stamped<OpenOrdersSnapshot>>>,
    user_rings: RwLock<WsUserRingState>,
    pub market_ws_connected: AtomicBool,
    pub user_ws_connected: AtomicBool,
    pub needs_rest_gap_reconcile: AtomicBool,
    pub ws_enabled: AtomicBool,
    pub market_last_frame_ms: AtomicI64,
    pub user_last_frame_ms: AtomicI64,
    pub market_last_pong_ms: AtomicI64,
    pub user_last_pong_ms: AtomicI64,
    pub market_reconnect_count: AtomicU64,
    pub user_reconnect_count: AtomicU64,
    pub market_subscription_count: AtomicUsize,
    pub user_subscription_count: AtomicUsize,
    pub last_order_submit_ms: AtomicI64,
    pub market_disconnected_since_ms: AtomicI64,
    pub user_disconnected_since_ms: AtomicI64,
}

impl Default for WsStateCache {
    fn default() -> Self {
        Self::new()
    }
}

impl WsStateCache {
    pub fn new() -> Self {
        Self {
            all_mids: RwLock::new(None),
            active_asset_ctxs: RwLock::new(HashMap::new()),
            candles: RwLock::new(HashMap::new()),
            l2_books: RwLock::new(HashMap::new()),
            clearinghouse_state: RwLock::new(None),
            open_orders: RwLock::new(None),
            user_rings: RwLock::new(WsUserRingState::default()),
            market_ws_connected: AtomicBool::new(false),
            user_ws_connected: AtomicBool::new(false),
            needs_rest_gap_reconcile: AtomicBool::new(false),
            ws_enabled: AtomicBool::new(false),
            market_last_frame_ms: AtomicI64::new(0),
            user_last_frame_ms: AtomicI64::new(0),
            market_last_pong_ms: AtomicI64::new(0),
            user_last_pong_ms: AtomicI64::new(0),
            market_reconnect_count: AtomicU64::new(0),
            user_reconnect_count: AtomicU64::new(0),
            market_subscription_count: AtomicUsize::new(0),
            user_subscription_count: AtomicUsize::new(0),
            last_order_submit_ms: AtomicI64::new(0),
            market_disconnected_since_ms: AtomicI64::new(0),
            user_disconnected_since_ms: AtomicI64::new(0),
        }
    }

    pub fn set_market_connected(&self, connected: bool) {
        self.market_ws_connected.store(connected, Ordering::Release);
    }

    pub fn set_user_connected(&self, connected: bool) {
        self.user_ws_connected.store(connected, Ordering::Release);
    }

    pub fn is_market_ws_connected(&self) -> bool {
        self.market_ws_connected.load(Ordering::Acquire)
    }

    pub fn is_user_ws_connected(&self) -> bool {
        self.user_ws_connected.load(Ordering::Acquire)
    }

    pub fn needs_rest_gap_reconcile(&self) -> bool {
        self.needs_rest_gap_reconcile.load(Ordering::Acquire)
    }

    pub fn set_needs_rest_gap_reconcile(&self, val: bool) {
        self.needs_rest_gap_reconcile.store(val, Ordering::Release);
    }

    /// Clears `needs_rest_gap_reconcile` ONLY if `user_reconnect_count` has not changed since
    /// `expected_user_reconnect_count`, preventing a mid-flight reconnect gap from being lost.
    pub fn try_clear_rest_gap_reconcile(&self, expected_user_reconnect_count: u64) -> bool {
        if self.user_reconnect_count.load(Ordering::Acquire) != expected_user_reconnect_count {
            return false;
        }
        self.needs_rest_gap_reconcile
            .store(false, Ordering::Release);
        true
    }

    pub fn is_user_ws_ready_for_reconcile(&self) -> bool {
        self.is_user_ws_connected() && !self.needs_rest_gap_reconcile()
    }

    pub fn user_reconnect_count(&self) -> u64 {
        self.user_reconnect_count.load(Ordering::Acquire)
    }

    pub fn market_reconnect_count(&self) -> u64 {
        self.market_reconnect_count.load(Ordering::Acquire)
    }

    pub fn user_disconnected_since_ms(&self) -> i64 {
        self.user_disconnected_since_ms.load(Ordering::Acquire)
    }

    /// Marks an `/exchange` order submission/cancellation so `clearinghouseState` and `openOrders`
    /// WS cache reads are bypassed for `1_500 ms` in favor of authoritative REST.
    pub fn mark_order_submit(&self) {
        self.mark_order_submit_at(now_ms());
    }

    pub fn mark_order_submit_at(&self, ts_ms: i64) {
        self.last_order_submit_ms.store(ts_ms, Ordering::Release);
    }

    pub fn is_clearinghouse_bypassed_at(&self, now: i64) -> bool {
        let submit_ts = self.last_order_submit_ms.load(Ordering::Acquire);
        submit_ts > 0
            && now >= submit_ts
            && now.saturating_sub(submit_ts) < WS_ORDER_SUBMIT_BYPASS_MS
    }

    /// Clears cached user state when switching wallet addresses.
    pub fn reset_user_state(&self) {
        *self.clearinghouse_state.write() = None;
        *self.open_orders.write() = None;
        let mut rings = self.user_rings.write();
        *rings = WsUserRingState::default();
    }

    pub fn get_fresh_all_mids(&self) -> Option<HashMap<String, f64>> {
        self.get_fresh_all_mids_at(now_ms(), WS_MIDS_FRESH_MS)
    }

    pub fn get_fresh_all_mids_at(&self, now: i64, max_age_ms: i64) -> Option<HashMap<String, f64>> {
        if !self.is_market_ws_connected() {
            return None;
        }
        let guard = self.all_mids.read();
        let stamped = guard.as_ref()?;
        if now < stamped.updated_at_ms
            || now.saturating_sub(stamped.updated_at_ms) > max_age_ms
            || stamped.value.is_empty()
        {
            return None;
        }
        Some(stamped.value.clone())
    }

    pub fn get_fresh_active_asset_ctx(&self, coin: &str) -> Option<WsActiveAssetCtx> {
        self.get_fresh_active_asset_ctx_at(coin, now_ms(), WS_ACTIVE_CTX_FRESH_MS)
    }

    pub fn get_fresh_active_asset_ctx_at(
        &self,
        coin: &str,
        now: i64,
        max_age_ms: i64,
    ) -> Option<WsActiveAssetCtx> {
        if !self.is_market_ws_connected() {
            return None;
        }
        let key = normalize_ws_coin_name(coin).to_ascii_uppercase();
        let guard = self.active_asset_ctxs.read();
        let stamped = guard.get(&key)?;
        if now < stamped.updated_at_ms || now.saturating_sub(stamped.updated_at_ms) > max_age_ms {
            return None;
        }
        Some(stamped.value.clone())
    }

    pub fn get_fresh_active_asset_ctxs_map(&self) -> Option<HashMap<String, WsActiveAssetCtx>> {
        self.get_fresh_active_asset_ctxs_map_at(now_ms(), WS_ACTIVE_CTX_FRESH_MS)
    }

    pub fn get_fresh_active_asset_ctxs_map_at(
        &self,
        now: i64,
        max_age_ms: i64,
    ) -> Option<HashMap<String, WsActiveAssetCtx>> {
        if !self.is_market_ws_connected() {
            return None;
        }
        let guard = self.active_asset_ctxs.read();
        let mut out = HashMap::new();
        for (k, stamped) in guard.iter() {
            if now >= stamped.updated_at_ms
                && now.saturating_sub(stamped.updated_at_ms) <= max_age_ms
            {
                out.insert(k.clone(), stamped.value.clone());
            }
        }
        (!out.is_empty()).then_some(out)
    }

    pub fn get_fresh_candles(&self, coin: &str, interval: &str) -> Option<(Vec<OhlcvBar>, i64)> {
        self.get_fresh_candles_at(coin, interval, now_ms(), WS_CANDLE_FRESH_MS)
    }

    pub fn get_fresh_candles_at(
        &self,
        coin: &str,
        interval: &str,
        now: i64,
        max_age_ms: i64,
    ) -> Option<(Vec<OhlcvBar>, i64)> {
        if !self.is_market_ws_connected() {
            return None;
        }
        let coin_upper = normalize_ws_coin_name(coin).to_ascii_uppercase();
        let key = (coin_upper, interval.trim().to_string());
        let guard = self.candles.read();
        let stamped = guard.get(&key)?;
        if now < stamped.updated_at_ms
            || now.saturating_sub(stamped.updated_at_ms) > max_age_ms
            || stamped.value.is_empty()
        {
            return None;
        }
        Some((
            stamped.value.iter().cloned().collect(),
            stamped.updated_at_ms,
        ))
    }

    pub fn get_fresh_l2_book_depth(&self, coin: &str) -> Option<L2BookDepthSnapshot> {
        self.get_fresh_l2_book_depth_at(coin, now_ms(), WS_L2_BOOK_FRESH_MS)
    }

    pub fn get_fresh_l2_book_depth_at(
        &self,
        coin: &str,
        now: i64,
        max_age_ms: i64,
    ) -> Option<L2BookDepthSnapshot> {
        if !self.is_market_ws_connected() {
            return None;
        }
        let coin_upper = normalize_ws_coin_name(coin).to_ascii_uppercase();
        let guard = self.l2_books.read();
        let stamped = guard.get(&coin_upper)?;
        if now < stamped.updated_at_ms || now.saturating_sub(stamped.updated_at_ms) > max_age_ms {
            return None;
        }
        Some(stamped.value)
    }

    pub fn get_fresh_l2_bbo(&self, coin: &str) -> Option<(f64, f64)> {
        self.get_fresh_l2_book_depth(coin)
            .map(|b| (b.best_bid, b.best_ask))
    }

    pub fn get_fresh_clearinghouse_state(&self) -> Option<Value> {
        self.get_fresh_clearinghouse_state_at(now_ms(), WS_CLEARINGHOUSE_FRESH_MS)
    }

    pub fn get_fresh_clearinghouse_state_at(&self, now: i64, max_age_ms: i64) -> Option<Value> {
        if !self.is_user_ws_connected() || self.is_clearinghouse_bypassed_at(now) {
            return None;
        }
        let guard = self.clearinghouse_state.read();
        let stamped = guard.as_ref()?;
        if now < stamped.updated_at_ms || now.saturating_sub(stamped.updated_at_ms) > max_age_ms {
            return None;
        }
        Some(stamped.value.clone())
    }

    pub fn get_fresh_clearinghouse_state_if_gap_filled(&self) -> Option<Value> {
        if !self.is_user_ws_ready_for_reconcile() {
            return None;
        }
        self.get_fresh_clearinghouse_state()
    }

    pub fn get_fresh_open_orders(&self) -> Option<OpenOrdersSnapshot> {
        self.get_fresh_open_orders_at(now_ms(), WS_OPEN_ORDERS_FRESH_MS)
    }

    pub fn get_fresh_open_orders_at(
        &self,
        now: i64,
        max_age_ms: i64,
    ) -> Option<OpenOrdersSnapshot> {
        if !self.is_user_ws_connected() || self.is_clearinghouse_bypassed_at(now) {
            return None;
        }
        let guard = self.open_orders.read();
        let stamped = guard.as_ref()?;
        if now < stamped.updated_at_ms || now.saturating_sub(stamped.updated_at_ms) > max_age_ms {
            return None;
        }
        Some(stamped.value.clone())
    }

    pub fn get_order_update_by_cloid(&self, cloid: &str) -> Option<WsOrderUpdate> {
        let key = cloid.trim().to_ascii_lowercase();
        if key.is_empty() {
            return None;
        }
        self.user_rings
            .read()
            .order_updates_by_cloid
            .get(&key)
            .cloned()
    }

    pub fn get_order_update_by_oid(&self, oid: u64) -> Option<WsOrderUpdate> {
        if oid == 0 {
            return None;
        }
        self.user_rings
            .read()
            .order_updates_by_oid
            .get(&oid)
            .cloned()
    }

    pub fn get_recent_user_fills(&self) -> (Vec<Value>, i64) {
        let guard = self.user_rings.read();
        (
            guard.user_fills_ring.iter().cloned().collect(),
            guard.user_fills_updated_at_ms,
        )
    }

    pub fn get_recent_user_fundings(&self) -> (Vec<Value>, i64) {
        let guard = self.user_rings.read();
        (
            guard.user_fundings_ring.iter().cloned().collect(),
            guard.user_fundings_updated_at_ms,
        )
    }

    /// Builds a [`WsStreamStatus`] telemetry snapshot for `system_config.ws_stream_status_json`.
    pub fn status_snapshot(&self, ws_url: &str, enabled: bool) -> WsStreamStatus {
        let now = now_ms();
        let market_connected = self.is_market_ws_connected();
        let user_connected = self.is_user_ws_connected();
        let mids_age_ms = self
            .all_mids
            .read()
            .as_ref()
            .map(|s| now.saturating_sub(s.updated_at_ms).max(0));
        let clearinghouse_age_ms = self
            .clearinghouse_state
            .read()
            .as_ref()
            .map(|s| now.saturating_sub(s.updated_at_ms).max(0));
        let redacted_url = ws_url
            .split(['?', '#'])
            .next()
            .unwrap_or(ws_url)
            .to_string();

        let status = if !enabled {
            "DISABLED"
        } else if market_connected && user_connected {
            "CONNECTED"
        } else if market_connected || user_connected {
            "DEGRADED"
        } else {
            "DISCONNECTED"
        };

        WsStreamStatus {
            enabled,
            ws_url_redacted: redacted_url,
            market_connected,
            user_connected,
            market_reconnect_count: self.market_reconnect_count.load(Ordering::Acquire),
            user_reconnect_count: self.user_reconnect_count.load(Ordering::Acquire),
            market_last_frame_ms: self.market_last_frame_ms.load(Ordering::Acquire),
            user_last_frame_ms: self.user_last_frame_ms.load(Ordering::Acquire),
            market_last_pong_ms: self.market_last_pong_ms.load(Ordering::Acquire),
            user_last_pong_ms: self.user_last_pong_ms.load(Ordering::Acquire),
            market_subscription_count: self.market_subscription_count.load(Ordering::Acquire),
            user_subscription_count: self.user_subscription_count.load(Ordering::Acquire),
            needs_rest_gap_reconcile: self.needs_rest_gap_reconcile.load(Ordering::Acquire),
            mids_age_ms,
            clearinghouse_age_ms,
            updated_at_ms: now,
            status: status.to_string(),
        }
    }

    pub fn ingest_frame_str(&self, raw: &str, is_user_conn: bool) -> Option<String> {
        self.ingest_frame_str_at(raw, is_user_conn, now_ms())
    }

    pub fn ingest_frame_str_at(&self, raw: &str, is_user_conn: bool, ts_ms: i64) -> Option<String> {
        let val = serde_json::from_str::<Value>(raw).ok()?;
        self.ingest_frame_value_at(&val, is_user_conn, ts_ms)
    }

    /// Parses and ingests any Hyperliquid WS JSON frame into [`WsStateCache`], returning the channel name.
    pub fn ingest_frame_value_at(
        &self,
        val: &Value,
        is_user_conn: bool,
        ts_ms: i64,
    ) -> Option<String> {
        if is_user_conn {
            self.user_last_frame_ms.store(ts_ms, Ordering::Release);
        } else {
            self.market_last_frame_ms.store(ts_ms, Ordering::Release);
        }

        let channel = val
            .get("channel")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if channel.is_empty() {
            return None;
        }
        let data = val.get("data").unwrap_or(&Value::Null);

        match channel {
            "pong" => {
                if is_user_conn {
                    self.user_last_pong_ms.store(ts_ms, Ordering::Release);
                } else {
                    self.market_last_pong_ms.store(ts_ms, Ordering::Release);
                }
            }
            "allMids" => self.ingest_all_mids(data, ts_ms),
            "activeAssetCtx" => self.ingest_active_asset_ctx(data, ts_ms),
            "candle" => self.ingest_candles(data, ts_ms),
            "l2Book" => self.ingest_l2_book(data, ts_ms),
            "webData3" | "webData2" => self.ingest_web_data(data, ts_ms),
            "clearinghouseState" => {
                if data.is_object() {
                    *self.clearinghouse_state.write() = Some(Stamped {
                        value: data.clone(),
                        updated_at_ms: ts_ms,
                    });
                }
            }
            "openOrders" => self.ingest_open_orders_channel(data, ts_ms),
            "orderUpdates" => self.ingest_order_updates(data, ts_ms),
            "userFills" => self.ingest_user_fills_payload(data, ts_ms),
            "userFundings" => self.ingest_user_fundings_payload(data, ts_ms),
            "userEvents" => self.ingest_user_events(data, ts_ms),
            "error" => {
                tracing::warn!(error_frame = %data, is_user_conn, "Hyperliquid WebSocket error frame received");
            }
            _ => {}
        }

        Some(channel.to_string())
    }

    fn ingest_all_mids(&self, data: &Value, ts_ms: i64) {
        let Some(mids_obj) = data
            .get("mids")
            .and_then(Value::as_object)
            .or_else(|| data.as_object())
        else {
            return;
        };
        let mut parsed_updates: Vec<(String, String, f64)> = Vec::with_capacity(mids_obj.len());
        for (raw_coin, v) in mids_obj {
            if let Some(px) = parse_hl_f64_opt(Some(v)).filter(|&p| p > 0.0) {
                let raw_trimmed = raw_coin.trim();
                let norm = normalize_ws_coin_name(raw_trimmed);
                if !norm.is_empty() {
                    parsed_updates.push((raw_trimmed.to_string(), norm, px));
                }
            }
        }
        if parsed_updates.is_empty() {
            return;
        }

        let mut guard = self.all_mids.write();
        let entry = guard.get_or_insert_with(|| Stamped {
            value: HashMap::with_capacity(parsed_updates.len()),
            updated_at_ms: ts_ms,
        });
        for (raw_key, norm_key, px) in parsed_updates {
            if !raw_key.is_empty() && raw_key != norm_key {
                entry.value.insert(raw_key, px);
            }
            entry.value.insert(norm_key, px);
        }
        entry.updated_at_ms = ts_ms;
    }

    fn ingest_active_asset_ctx(&self, data: &Value, ts_ms: i64) {
        let raw_coin = data
            .get("coin")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        let norm = normalize_ws_coin_name(raw_coin);
        if norm.is_empty() {
            return;
        }
        let ctx_obj = data.get("ctx").filter(|v| v.is_object()).unwrap_or(data);
        let mark_px = parse_hl_f64(ctx_obj.get("markPx"));
        let mid_px = parse_hl_f64(ctx_obj.get("midPx"));
        let oracle_px = parse_hl_f64(ctx_obj.get("oraclePx"));
        if mark_px <= 0.0 && mid_px <= 0.0 && oracle_px <= 0.0 {
            return;
        }
        let ctx = WsActiveAssetCtx {
            coin: norm.clone(),
            mark_px,
            oracle_px,
            mid_px,
            prev_day_px: parse_hl_f64(ctx_obj.get("prevDayPx")),
            funding_rate_hourly: parse_hl_f64(ctx_obj.get("funding")),
            open_interest: parse_hl_f64(ctx_obj.get("openInterest")).max(0.0),
            day_ntl_vlm: parse_hl_f64(ctx_obj.get("dayNtlVlm")).max(0.0),
        };
        let coin_upper = norm.to_ascii_uppercase();
        self.active_asset_ctxs.write().insert(
            coin_upper,
            Stamped {
                value: ctx,
                updated_at_ms: ts_ms,
            },
        );
    }

    fn ingest_candles(&self, data: &Value, ts_ms: i64) {
        let mut items: Vec<&Value> = Vec::new();
        if let Some(arr) = data.as_array() {
            items.extend(arr.iter());
        } else if data.is_object() {
            items.push(data);
        }
        if items.is_empty() {
            return;
        }

        let mut guard = self.candles.write();
        for item in items {
            let raw_coin = item
                .get("s")
                .or_else(|| item.get("coin"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            let norm = normalize_ws_coin_name(raw_coin);
            let interval = item
                .get("i")
                .or_else(|| item.get("interval"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            let t = parse_hl_i64_opt(item.get("t")).unwrap_or(0);
            let c = parse_hl_f64(item.get("c"));
            if norm.is_empty() || interval.is_empty() || t <= 0 || c <= 0.0 {
                continue;
            }
            let o = parse_hl_f64(item.get("o"));
            let h = parse_hl_f64(item.get("h"));
            let l = parse_hl_f64(item.get("l"));
            let v = parse_hl_f64(item.get("v")).max(0.0);
            let f = parse_hl_f64(item.get("f"));
            let bar = OhlcvBar {
                id: 0,
                asset_id: 0,
                timeframe: interval.clone(),
                timestamp_ms: t,
                open: if o > 0.0 { o } else { c },
                high: if h > 0.0 { h } else { c },
                low: if l > 0.0 { l } else { c },
                close: c,
                volume: v,
                funding_rate: f,
                created_at_ms: ts_ms,
                updated_at_ms: ts_ms,
            };
            let key = (norm.to_ascii_uppercase(), interval);
            let stamped = guard.entry(key).or_insert_with(|| Stamped {
                value: VecDeque::with_capacity(WS_CANDLES_RING_CAP),
                updated_at_ms: ts_ms,
            });
            upsert_candle_ring(&mut stamped.value, bar, WS_CANDLES_RING_CAP);
            stamped.updated_at_ms = ts_ms;
        }
    }

    fn ingest_l2_book(&self, data: &Value, ts_ms: i64) {
        let raw_coin = data
            .get("coin")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        let norm = normalize_ws_coin_name(raw_coin);
        if norm.is_empty() {
            return;
        }
        let Some(depth) = parse_l2_book_depth(data) else {
            return;
        };
        let coin_upper = norm.to_ascii_uppercase();
        self.l2_books.write().insert(
            coin_upper,
            Stamped {
                value: depth,
                updated_at_ms: ts_ms,
            },
        );
    }

    fn ingest_web_data(&self, data: &Value, ts_ms: i64) {
        if !data.is_object() {
            return;
        }
        let first_perp_dex = data
            .get("perpDexStates")
            .and_then(Value::as_array)
            .and_then(|arr| arr.first());

        let ch_state_opt = data
            .get("clearinghouseState")
            .filter(|v| v.is_object())
            .or_else(|| {
                first_perp_dex
                    .and_then(|p| p.get("clearinghouseState"))
                    .filter(|v| v.is_object())
            })
            .or_else(|| {
                (data.get("marginSummary").is_some() || data.get("assetPositions").is_some())
                    .then_some(data)
            });

        if let Some(ch_state) = ch_state_opt {
            *self.clearinghouse_state.write() = Some(Stamped {
                value: ch_state.clone(),
                updated_at_ms: ts_ms,
            });
        }

        let open_orders_opt = data
            .get("openOrders")
            .and_then(Value::as_array)
            .or_else(|| {
                first_perp_dex
                    .and_then(|p| p.get("openOrders"))
                    .and_then(Value::as_array)
            });

        if let Some(orders) = open_orders_opt {
            let explicit_meta = data.get("metadataComplete").and_then(Value::as_bool);
            let metadata_complete = explicit_meta.unwrap_or_else(|| {
                orders.is_empty() || orders.iter().all(order_has_frontend_metadata)
            });
            *self.open_orders.write() = Some(Stamped {
                value: OpenOrdersSnapshot {
                    orders: orders.clone(),
                    metadata_complete,
                },
                updated_at_ms: ts_ms,
            });
        }
    }

    fn ingest_open_orders_channel(&self, data: &Value, ts_ms: i64) {
        let orders_opt = data
            .as_array()
            .or_else(|| data.get("orders").and_then(Value::as_array));
        let Some(orders) = orders_opt else {
            return;
        };
        let explicit_meta = data.get("metadataComplete").and_then(Value::as_bool);
        let metadata_complete = explicit_meta.unwrap_or_else(|| {
            !orders.is_empty() && orders.iter().all(order_has_frontend_metadata)
        });
        *self.open_orders.write() = Some(Stamped {
            value: OpenOrdersSnapshot {
                orders: orders.clone(),
                metadata_complete,
            },
            updated_at_ms: ts_ms,
        });
    }

    fn ingest_order_updates(&self, data: &Value, ts_ms: i64) {
        let mut items: Vec<&Value> = Vec::new();
        if let Some(arr) = data.as_array() {
            items.extend(arr.iter());
        } else if data.is_object() {
            items.push(data);
        }
        if items.is_empty() {
            return;
        }

        let mut rings = self.user_rings.write();
        for item in items {
            let order_obj = item.get("order").unwrap_or(item);
            let oid = parse_hl_u64_opt(order_obj.get("oid")).unwrap_or(0);
            if oid == 0 {
                continue;
            }
            let raw_coin = order_obj
                .get("coin")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            let coin = normalize_ws_coin_name(raw_coin);
            let cloid = order_obj
                .get("cloid")
                .and_then(Value::as_str)
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty());
            let side = order_obj
                .get("side")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            let status = item
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            let status_timestamp_ms = parse_hl_i64_opt(item.get("statusTimestamp"))
                .filter(|&t| t > 0)
                .unwrap_or(ts_ms);
            let update = WsOrderUpdate {
                oid,
                cloid: cloid.clone(),
                coin,
                side,
                limit_px: parse_hl_f64(order_obj.get("limitPx")),
                sz: parse_hl_f64(order_obj.get("sz")),
                orig_sz: parse_hl_f64(order_obj.get("origSz")),
                status,
                status_timestamp_ms,
                raw: item.clone(),
            };
            rings.order_updates_by_oid.insert(oid, update.clone());
            if let Some(ref c) = cloid {
                rings
                    .order_updates_by_cloid
                    .insert(c.clone(), update.clone());
            }
            rings.order_updates_ring.push_back(update);
            while rings.order_updates_ring.len() > WS_ORDER_UPDATES_RING_CAP {
                if let Some(evicted) = rings.order_updates_ring.pop_front() {
                    if rings
                        .order_updates_by_oid
                        .get(&evicted.oid)
                        .is_some_and(|cur| cur.status_timestamp_ms == evicted.status_timestamp_ms)
                    {
                        rings.order_updates_by_oid.remove(&evicted.oid);
                    }
                    if let Some(ref c) = evicted.cloid {
                        if rings.order_updates_by_cloid.get(c).is_some_and(|cur| {
                            cur.status_timestamp_ms == evicted.status_timestamp_ms
                        }) {
                            rings.order_updates_by_cloid.remove(c);
                        }
                    }
                }
            }
        }
    }

    fn ingest_user_fills_payload(&self, data: &Value, ts_ms: i64) {
        let fills_arr = data
            .get("fills")
            .and_then(Value::as_array)
            .or_else(|| data.as_array());
        let Some(fills) = fills_arr else {
            return;
        };
        self.push_user_fills_slice(fills, ts_ms);
    }

    fn push_user_fills_slice(&self, fills: &[Value], ts_ms: i64) {
        if fills.is_empty() {
            return;
        }
        let mut rings = self.user_rings.write();
        for fill in fills {
            let Some(key) = fill_dedup_key(fill, ts_ms) else {
                continue;
            };
            if rings.seen_fill_keys.insert(key) {
                rings.user_fills_ring.push_back(fill.clone());
                while rings.user_fills_ring.len() > WS_USER_FILLS_RING_CAP {
                    if let Some(evicted) = rings.user_fills_ring.pop_front() {
                        if let Some(ev_key) = fill_dedup_key(&evicted, ts_ms) {
                            rings.seen_fill_keys.remove(&ev_key);
                        }
                    }
                }
            }
        }
        rings.user_fills_updated_at_ms = ts_ms;
    }

    fn ingest_user_fundings_payload(&self, data: &Value, ts_ms: i64) {
        let fundings_arr = data
            .get("fundings")
            .and_then(Value::as_array)
            .or_else(|| data.as_array());
        let Some(fundings) = fundings_arr else {
            return;
        };
        self.push_user_fundings_slice(fundings, ts_ms);
    }

    fn push_user_fundings_slice(&self, fundings: &[Value], ts_ms: i64) {
        if fundings.is_empty() {
            return;
        }
        let mut rings = self.user_rings.write();
        for raw_ev in fundings {
            let normalized = normalize_ws_funding_event(raw_ev);
            let Some(key) = ws_funding_dedup_key(&normalized) else {
                continue;
            };
            if rings.seen_funding_keys.insert(key) {
                rings.user_fundings_ring.push_back(normalized);
                while rings.user_fundings_ring.len() > WS_USER_FUNDINGS_RING_CAP {
                    if let Some(evicted) = rings.user_fundings_ring.pop_front() {
                        if let Some(ev_key) = ws_funding_dedup_key(&evicted) {
                            rings.seen_funding_keys.remove(&ev_key);
                        }
                    }
                }
            }
        }
        rings.user_fundings_updated_at_ms = ts_ms;
    }

    fn ingest_user_events(&self, data: &Value, ts_ms: i64) {
        if let Some(fills) = data.get("fills").and_then(Value::as_array) {
            self.push_user_fills_slice(fills, ts_ms);
        }
        if let Some(funding_val) = data.get("funding") {
            if let Some(arr) = funding_val.as_array() {
                self.push_user_fundings_slice(arr, ts_ms);
            } else if funding_val.is_object() {
                self.push_user_fundings_slice(std::slice::from_ref(funding_val), ts_ms);
            }
        }
    }
}

fn order_has_frontend_metadata(ord: &Value) -> bool {
    ord.get("orderType").is_some()
        || ord.get("isTrigger").is_some()
        || ord.get("reduceOnly").is_some()
        || ord.get("isPositionTpsl").is_some()
}

fn upsert_candle_ring(ring: &mut VecDeque<OhlcvBar>, bar: OhlcvBar, cap: usize) {
    if let Some(last) = ring.back_mut() {
        if last.timestamp_ms == bar.timestamp_ms {
            *last = bar;
            return;
        }
        if bar.timestamp_ms > last.timestamp_ms {
            ring.push_back(bar);
            while ring.len() > cap {
                ring.pop_front();
            }
            return;
        }
    } else {
        ring.push_back(bar);
        return;
    }

    if let Some(pos) = ring.iter().position(|b| b.timestamp_ms == bar.timestamp_ms) {
        ring[pos] = bar;
        return;
    }

    let insert_idx = ring
        .iter()
        .position(|b| b.timestamp_ms > bar.timestamp_ms)
        .unwrap_or(ring.len());
    ring.insert(insert_idx, bar);
    while ring.len() > cap {
        ring.pop_front();
    }
}

fn fill_dedup_key(fill: &Value, fallback_ts: i64) -> Option<FillDedupKey> {
    let raw_coin = fill.get("coin").and_then(Value::as_str)?.trim();
    if raw_coin.is_empty() {
        return None;
    }
    let px = parse_hl_f64(fill.get("px"));
    let sz = parse_hl_f64(fill.get("sz"));
    if px <= 0.0 || sz <= 0.0 {
        return None;
    }
    let fill_time = parse_hl_i64_opt(fill.get("time"))
        .filter(|&t| t > 0)
        .unwrap_or(fallback_ts);
    let tid = parse_hl_u64_opt(fill.get("tid")).unwrap_or(0);
    let oid = parse_hl_u64_opt(fill.get("oid"))
        .filter(|&id| id > 0)
        .or((tid > 0).then_some(tid))
        .unwrap_or(fill_time as u64);
    let side = fill
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let coin_upper = normalize_ws_coin_name(raw_coin).to_ascii_uppercase();
    Some((
        fill_time,
        oid,
        tid,
        coin_upper,
        side,
        sz.to_bits(),
        px.to_bits(),
    ))
}

fn normalize_ws_funding_event(ev: &Value) -> Value {
    if ev.get("delta").is_some_and(Value::is_object) {
        return ev.clone();
    }
    let time_ms = parse_hl_i64_opt(ev.get("time")).unwrap_or(0);
    let coin = ev.get("coin").and_then(Value::as_str).unwrap_or("").trim();
    let usdc = ev.get("usdc").cloned().unwrap_or(Value::Null);
    let szi = ev.get("szi").cloned().unwrap_or(Value::Null);
    let funding_rate = ev.get("fundingRate").cloned().unwrap_or(Value::Null);
    let n_samples = ev
        .get("nSamples")
        .or_else(|| ev.get("n"))
        .cloned()
        .unwrap_or(Value::Null);
    let hash = ev.get("hash").cloned().unwrap_or(Value::Null);

    json!({
        "time": time_ms,
        "hash": hash,
        "n": n_samples,
        "delta": {
            "type": "funding",
            "coin": coin,
            "usdc": usdc,
            "szi": szi,
            "fundingRate": funding_rate,
            "nSamples": n_samples,
        }
    })
}

fn ws_funding_dedup_key(ev: &Value) -> Option<FundingDedupKey> {
    let time_ms = parse_hl_i64_opt(ev.get("time")).filter(|&t| t > 0)?;
    let delta = ev.get("delta")?;
    let coin = delta
        .get("coin")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase();
    if coin.is_empty() {
        return None;
    }
    let usdc = parse_hl_f64(delta.get("usdc"));
    if usdc.abs() <= 1e-12 {
        return None;
    }
    let szi = parse_hl_f64(delta.get("szi"));
    let hash = ev
        .get("hash")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    Some((time_ms, coin, usdc.to_bits(), szi.to_bits(), hash))
}

/// Manages the 2 dedicated Hyperliquid WebSocket connections (`Market` + `User`) and periodic
/// `ws_stream_status_json` persistence inside an isolated background Tokio runtime.
pub struct HyperliquidWsManager {
    pub cache: Arc<WsStateCache>,
    shutdown_tx: watch::Sender<bool>,
    thread: Option<JoinHandle<()>>,
}

impl HyperliquidWsManager {
    /// Starts a standalone [`HyperliquidWsManager`] with RAII shutdown on [`Drop`].
    pub fn start(
        db: DatabaseManager,
        cache: Arc<WsStateCache>,
        sub_rx: watch::Receiver<WsSubscriptionSet>,
        cfg: &ConfigMap,
        timing: WsTimingConfig,
    ) -> Result<Self> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let thread =
            Self::spawn_background(db, Arc::clone(&cache), sub_rx, cfg, timing, shutdown_rx)?;
        Ok(Self {
            cache,
            shutdown_tx,
            thread: Some(thread),
        })
    }

    /// Spawns the background WS manager OS thread wired to an existing `shutdown_rx`.
    /// Never touches `execution_lock()`.
    pub fn spawn_background(
        db: DatabaseManager,
        cache: Arc<WsStateCache>,
        sub_rx: watch::Receiver<WsSubscriptionSet>,
        cfg: &ConfigMap,
        timing: WsTimingConfig,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Result<JoinHandle<()>> {
        let ws_url = resolve_hyperliquid_ws_url(cfg);
        if !validate_hyperliquid_ws_url(&ws_url, cfg) {
            return Err(AegisError::SecurityViolation(format!(
                "Refusing HyperliquidWsManager start with invalid WS URL '{ws_url}'"
            )));
        }

        let initial_wallet = normalize_wallet_for_ws(&cfg.get_str("wallet_address", ""));
        cache.ws_enabled.store(true, Ordering::Release);

        std::thread::Builder::new()
            .name("hl-ws-manager".to_string())
            .spawn(move || {
                let Ok(rt) = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                else {
                    tracing::error!("Failed building Tokio runtime for HyperliquidWsManager");
                    cache.ws_enabled.store(false, Ordering::Release);
                    return;
                };

                rt.block_on(async move {
                    let status_notify = Arc::new(Notify::new());
                    let (wallet_tx, wallet_rx) = watch::channel(initial_wallet);

                    let market_handle = tokio::spawn(run_market_ws_loop(
                        ws_url.clone(),
                        Arc::clone(&cache),
                        sub_rx,
                        timing,
                        Arc::clone(&status_notify),
                        shutdown_rx.clone(),
                    ));

                    let user_handle = tokio::spawn(run_user_ws_loop(
                        ws_url.clone(),
                        Arc::clone(&cache),
                        wallet_rx,
                        timing,
                        Arc::clone(&status_notify),
                        shutdown_rx.clone(),
                    ));

                    let status_handle = tokio::spawn(run_status_persister_loop(
                        db,
                        ws_url,
                        Arc::clone(&cache),
                        wallet_tx,
                        timing,
                        status_notify,
                        shutdown_rx,
                    ));

                    let _ = tokio::join!(market_handle, user_handle, status_handle);
                    cache.set_market_connected(false);
                    cache.set_user_connected(false);
                });
            })
            .map_err(|e| {
                AegisError::Execution(format!("Failed spawning hl-ws-manager thread: {e}"))
            })
    }

    pub fn shutdown(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for HyperliquidWsManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn normalize_wallet_for_ws(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    validate_evm_address(trimmed).unwrap_or_default()
}

fn shutdown_requested(rx: &watch::Receiver<bool>) -> bool {
    *rx.borrow()
}

async fn wait_for_shutdown_or_duration(
    duration: Duration,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    if shutdown_requested(shutdown_rx) {
        return true;
    }
    tokio::select! {
        biased;
        res = shutdown_rx.changed() => {
            res.is_err() || *shutdown_rx.borrow()
        }
        _ = tokio::time::sleep(duration) => false,
    }
}

async fn run_market_ws_loop(
    ws_url: String,
    cache: Arc<WsStateCache>,
    mut sub_rx: watch::Receiver<WsSubscriptionSet>,
    timing: WsTimingConfig,
    status_notify: Arc<Notify>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut attempt: u32 = 0;
    let mut has_connected_once = false;

    loop {
        if shutdown_requested(&shutdown_rx) {
            break;
        }

        let conn_res = tokio::select! {
            biased;
            res = shutdown_rx.changed() => {
                if res.is_err() || *shutdown_rx.borrow() {
                    break;
                }
                continue;
            }
            res = tokio::time::timeout(
                timing.connect_timeout,
                tokio_tungstenite::connect_async(ws_url.as_str()),
            ) => res,
        };

        match conn_res {
            Ok(Ok((ws_stream, _))) => {
                let connected_at = Instant::now();
                if has_connected_once {
                    cache.market_reconnect_count.fetch_add(1, Ordering::AcqRel);
                    cache.set_needs_rest_gap_reconcile(true);
                }
                has_connected_once = true;
                cache.set_market_connected(true);
                status_notify.notify_one();

                let session_clean = drive_market_session(
                    ws_stream,
                    &cache,
                    &mut sub_rx,
                    timing,
                    &status_notify,
                    &mut shutdown_rx,
                )
                .await;

                cache.set_market_connected(false);
                cache
                    .market_disconnected_since_ms
                    .store(now_ms(), Ordering::Release);
                cache.market_subscription_count.store(0, Ordering::Release);
                status_notify.notify_one();

                if session_clean || shutdown_requested(&shutdown_rx) {
                    break;
                }

                if connected_at.elapsed() >= timing.healthy_reset_duration {
                    attempt = 0;
                }
            }
            Ok(Err(err)) => {
                tracing::debug!(ws_url = %ws_url, error = %err, attempt, "Market WS connect failed");
            }
            Err(_) => {
                tracing::debug!(ws_url = %ws_url, attempt, "Market WS connect timed out");
            }
        }

        let backoff = compute_ws_reconnect_backoff(
            attempt,
            timing.backoff_min.as_millis() as u64,
            timing.backoff_max.as_millis() as u64,
            true,
        );
        attempt = attempt.saturating_add(1);
        if wait_for_shutdown_or_duration(backoff, &mut shutdown_rx).await {
            break;
        }
    }
}

#[derive(Debug, Clone)]
enum PendingMarketMsg {
    Subscribe(MarketSubKey),
    Unsubscribe(MarketSubKey),
}

impl PendingMarketMsg {
    fn key(&self) -> &MarketSubKey {
        match self {
            Self::Subscribe(k) | Self::Unsubscribe(k) => k,
        }
    }

    fn to_text(&self) -> String {
        match self {
            Self::Subscribe(k) => json!({
                "method": "subscribe",
                "subscription": k.to_subscription_json(),
            })
            .to_string(),
            Self::Unsubscribe(k) => json!({
                "method": "unsubscribe",
                "subscription": k.to_subscription_json(),
            })
            .to_string(),
        }
    }
}

async fn drive_market_session(
    ws_stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    cache: &WsStateCache,
    sub_rx: &mut watch::Receiver<WsSubscriptionSet>,
    timing: WsTimingConfig,
    status_notify: &Notify,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    let (mut ws_write, mut ws_read) = ws_stream.split();
    let mut active_subs: HashSet<MarketSubKey> = HashSet::new();
    let mut pending_queue: VecDeque<PendingMarketMsg> = VecDeque::new();

    for key in sub_rx.borrow().to_market_keys() {
        pending_queue.push_back(PendingMarketMsg::Subscribe(key));
    }
    cache
        .market_subscription_count
        .store(pending_queue.len(), Ordering::Release);

    let mut next_pace_at = Instant::now();
    let mut ping_interval =
        tokio::time::interval_at(Instant::now() + timing.ping_interval, timing.ping_interval);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_rx_at = Instant::now();

    loop {
        if shutdown_requested(shutdown_rx) {
            let _ = ws_write.send(Message::Close(None)).await;
            return true;
        }

        let stale_deadline = last_rx_at + timing.stale_timeout;
        let send_sleep = tokio::time::sleep_until(next_pace_at);
        let stale_sleep = tokio::time::sleep_until(stale_deadline);

        tokio::select! {
            biased;
            res = shutdown_rx.changed() => {
                if res.is_err() || *shutdown_rx.borrow() {
                    let _ = ws_write.send(Message::Close(None)).await;
                    return true;
                }
            }
            _ = stale_sleep => {
                tracing::warn!(
                    stale_secs = timing.stale_timeout.as_secs_f64(),
                    "Market WS exceeded stale-frame watchdog; forcing reconnect"
                );
                let _ = ws_write.send(Message::Close(None)).await;
                return false;
            }
            _ = ping_interval.tick() => {
                if ws_write
                    .send(Message::Text(r#"{"method":"ping"}"#.to_string()))
                    .await
                    .is_err()
                {
                    return false;
                }
            }
            _ = send_sleep, if !pending_queue.is_empty() => {
                if let Some(msg) = pending_queue.pop_front() {
                    let payload = msg.to_text();
                    if ws_write.send(Message::Text(payload)).await.is_err() {
                        return false;
                    }
                    match msg {
                        PendingMarketMsg::Subscribe(k) => {
                            active_subs.insert(k);
                        }
                        PendingMarketMsg::Unsubscribe(k) => {
                            active_subs.remove(&k);
                        }
                    }
                    cache
                        .market_subscription_count
                        .store(active_subs.len(), Ordering::Release);
                    next_pace_at = Instant::now() + timing.subscribe_pace_interval;
                }
            }
            sub_res = sub_rx.changed() => {
                if sub_res.is_ok() {
                    let desired: Vec<MarketSubKey> = sub_rx.borrow().to_market_keys();
                    let desired_set: HashSet<MarketSubKey> = desired.iter().cloned().collect();

                    pending_queue.retain(|p| match p {
                        PendingMarketMsg::Subscribe(k) => desired_set.contains(k),
                        PendingMarketMsg::Unsubscribe(k) => !desired_set.contains(k),
                    });

                    for k in active_subs.iter() {
                        if !desired_set.contains(k)
                            && !pending_queue.iter().any(|p| p.key() == k)
                        {
                            pending_queue.push_back(PendingMarketMsg::Unsubscribe(k.clone()));
                        }
                    }
                    for k in desired {
                        if !active_subs.contains(&k)
                            && !pending_queue.iter().any(|p| p.key() == &k)
                        {
                            pending_queue.push_back(PendingMarketMsg::Subscribe(k));
                        }
                    }
                    status_notify.notify_one();
                }
            }
            frame_opt = ws_read.next() => {
                match frame_opt {
                    Some(Ok(Message::Text(txt))) => {
                        last_rx_at = Instant::now();
                        cache.ingest_frame_str(&txt, false);
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        last_rx_at = Instant::now();
                        if let Ok(txt) = std::str::from_utf8(&bytes) {
                            cache.ingest_frame_str(txt, false);
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        last_rx_at = Instant::now();
                        let now = now_ms();
                        cache.market_last_frame_ms.store(now, Ordering::Release);
                        cache.market_last_pong_ms.store(now, Ordering::Release);
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        last_rx_at = Instant::now();
                        cache.market_last_frame_ms.store(now_ms(), Ordering::Release);
                        if ws_write.send(Message::Pong(payload)).await.is_err() {
                            return false;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                        return false;
                    }
                    Some(Ok(_)) => {
                        last_rx_at = Instant::now();
                    }
                }
            }
        }
    }
}

async fn run_user_ws_loop(
    ws_url: String,
    cache: Arc<WsStateCache>,
    mut wallet_rx: watch::Receiver<String>,
    timing: WsTimingConfig,
    status_notify: Arc<Notify>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut attempt: u32 = 0;
    let mut has_connected_once = false;

    loop {
        if shutdown_requested(&shutdown_rx) {
            break;
        }

        let wallet = wallet_rx.borrow().clone();
        if wallet.is_empty() {
            cache.set_user_connected(false);
            cache.user_subscription_count.store(0, Ordering::Release);
            tokio::select! {
                biased;
                res = shutdown_rx.changed() => {
                    if res.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                w_res = wallet_rx.changed() => {
                    if w_res.is_err() {
                        break;
                    }
                }
            }
            continue;
        }

        let conn_res = tokio::select! {
            biased;
            res = shutdown_rx.changed() => {
                if res.is_err() || *shutdown_rx.borrow() {
                    break;
                }
                continue;
            }
            w_res = wallet_rx.changed() => {
                if w_res.is_err() {
                    break;
                }
                continue;
            }
            res = tokio::time::timeout(
                timing.connect_timeout,
                tokio_tungstenite::connect_async(ws_url.as_str()),
            ) => res,
        };

        match conn_res {
            Ok(Ok((ws_stream, _))) => {
                let connected_at = Instant::now();
                if has_connected_once {
                    cache.user_reconnect_count.fetch_add(1, Ordering::AcqRel);
                }
                has_connected_once = true;
                cache.set_user_connected(true);
                // Require immediate REST gap-fill on initial connect and every reconnect.
                cache.set_needs_rest_gap_reconcile(true);
                status_notify.notify_one();

                let (session_clean, wallet_changed) = drive_user_session(
                    ws_stream,
                    &wallet,
                    &cache,
                    &mut wallet_rx,
                    timing,
                    &mut shutdown_rx,
                )
                .await;

                cache.set_user_connected(false);
                cache
                    .user_disconnected_since_ms
                    .store(now_ms(), Ordering::Release);
                cache.user_subscription_count.store(0, Ordering::Release);
                status_notify.notify_one();

                if session_clean || shutdown_requested(&shutdown_rx) {
                    break;
                }
                if wallet_changed {
                    cache.reset_user_state();
                    attempt = 0;
                    continue;
                }
                if connected_at.elapsed() >= timing.healthy_reset_duration {
                    attempt = 0;
                }
            }
            Ok(Err(err)) => {
                tracing::debug!(ws_url = %ws_url, error = %err, attempt, "User WS connect failed");
            }
            Err(_) => {
                tracing::debug!(ws_url = %ws_url, attempt, "User WS connect timed out");
            }
        }

        let backoff = compute_ws_reconnect_backoff(
            attempt,
            timing.backoff_min.as_millis() as u64,
            timing.backoff_max.as_millis() as u64,
            true,
        );
        attempt = attempt.saturating_add(1);
        tokio::select! {
            biased;
            res = shutdown_rx.changed() => {
                if res.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            w_res = wallet_rx.changed() => {
                if w_res.is_err() {
                    break;
                }
                cache.reset_user_state();
                attempt = 0;
            }
            _ = tokio::time::sleep(backoff) => {}
        }
    }
}

async fn drive_user_session(
    ws_stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    wallet: &str,
    cache: &WsStateCache,
    wallet_rx: &mut watch::Receiver<String>,
    timing: WsTimingConfig,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> (bool, bool) {
    let (mut ws_write, mut ws_read) = ws_stream.split();
    let user_channels = [
        "orderUpdates",
        "userFills",
        "userFundings",
        "userEvents",
        "webData3",
    ];
    let mut pending_subs: VecDeque<String> = user_channels
        .iter()
        .map(|ch| {
            json!({
                "method": "subscribe",
                "subscription": {
                    "type": ch,
                    "user": wallet,
                }
            })
            .to_string()
        })
        .collect();
    let mut sent_count = 0usize;
    let mut next_pace_at = Instant::now();

    let mut ping_interval =
        tokio::time::interval_at(Instant::now() + timing.ping_interval, timing.ping_interval);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_rx_at = Instant::now();

    loop {
        if shutdown_requested(shutdown_rx) {
            let _ = ws_write.send(Message::Close(None)).await;
            return (true, false);
        }

        let stale_deadline = last_rx_at + timing.stale_timeout;
        let send_sleep = tokio::time::sleep_until(next_pace_at);
        let stale_sleep = tokio::time::sleep_until(stale_deadline);

        tokio::select! {
            biased;
            res = shutdown_rx.changed() => {
                if res.is_err() || *shutdown_rx.borrow() {
                    let _ = ws_write.send(Message::Close(None)).await;
                    return (true, false);
                }
            }
            w_res = wallet_rx.changed() => {
                if w_res.is_ok() && *wallet_rx.borrow() != wallet {
                    let _ = ws_write.send(Message::Close(None)).await;
                    return (false, true);
                }
            }
            _ = stale_sleep => {
                tracing::warn!(
                    stale_secs = timing.stale_timeout.as_secs_f64(),
                    "User WS exceeded stale-frame watchdog; forcing reconnect"
                );
                let _ = ws_write.send(Message::Close(None)).await;
                return (false, false);
            }
            _ = ping_interval.tick() => {
                if ws_write
                    .send(Message::Text(r#"{"method":"ping"}"#.to_string()))
                    .await
                    .is_err()
                {
                    return (false, false);
                }
            }
            _ = send_sleep, if !pending_subs.is_empty() => {
                if let Some(payload) = pending_subs.pop_front() {
                    if ws_write.send(Message::Text(payload)).await.is_err() {
                        return (false, false);
                    }
                    sent_count += 1;
                    cache
                        .user_subscription_count
                        .store(sent_count, Ordering::Release);
                    next_pace_at = Instant::now() + timing.subscribe_pace_interval;
                }
            }
            frame_opt = ws_read.next() => {
                match frame_opt {
                    Some(Ok(Message::Text(txt))) => {
                        last_rx_at = Instant::now();
                        cache.ingest_frame_str(&txt, true);
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        last_rx_at = Instant::now();
                        if let Ok(txt) = std::str::from_utf8(&bytes) {
                            cache.ingest_frame_str(txt, true);
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        last_rx_at = Instant::now();
                        let now = now_ms();
                        cache.user_last_frame_ms.store(now, Ordering::Release);
                        cache.user_last_pong_ms.store(now, Ordering::Release);
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        last_rx_at = Instant::now();
                        cache.user_last_frame_ms.store(now_ms(), Ordering::Release);
                        if ws_write.send(Message::Pong(payload)).await.is_err() {
                            return (false, false);
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                        return (false, false);
                    }
                    Some(Ok(_)) => {
                        last_rx_at = Instant::now();
                    }
                }
            }
        }
    }
}

async fn run_status_persister_loop(
    db: DatabaseManager,
    ws_url: String,
    cache: Arc<WsStateCache>,
    wallet_tx: watch::Sender<String>,
    timing: WsTimingConfig,
    status_notify: Arc<Notify>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(timing.status_persist_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            res = shutdown_rx.changed() => {
                if res.is_err() || *shutdown_rx.borrow() {
                    cache.set_market_connected(false);
                    cache.set_user_connected(false);
                    persist_ws_status_blocking(&db, &cache, &ws_url, false).await;
                    break;
                }
            }
            _ = status_notify.notified() => {
                persist_ws_status_blocking(&db, &cache, &ws_url, true).await;
            }
            _ = interval.tick() => {
                persist_ws_status_blocking(&db, &cache, &ws_url, true).await;
                let db_cfg = db.clone();
                if let Ok(cfg) = tokio::task::spawn_blocking(move || db_cfg.load_config_or_default()).await {
                    let next_wallet = normalize_wallet_for_ws(&cfg.get_str("wallet_address", ""));
                    wallet_tx.send_if_modified(|cur| {
                        if *cur != next_wallet {
                            *cur = next_wallet;
                            true
                        } else {
                            false
                        }
                    });
                }
            }
        }
    }
}

async fn persist_ws_status_blocking(
    db: &DatabaseManager,
    cache: &WsStateCache,
    ws_url: &str,
    enabled: bool,
) {
    let snapshot = cache.status_snapshot(ws_url, enabled);
    let Ok(json_str) = serde_json::to_string(&snapshot) else {
        return;
    };
    let db_c = db.clone();
    let _ = tokio::task::spawn_blocking(move || {
        let _ = db_c.set_config_value(
            "ws_stream_status_json",
            &json_str,
            Some(
                "Cached Hyperliquid WebSocket Market+User stream health snapshot JSON (read-only)",
            ),
        );
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_test_support::mock_ws::MockWsServer;
    use serde_json::json;

    #[test]
    fn test_validate_hyperliquid_ws_url_ssrf_guards() {
        let cfg = ConfigMap::new();

        // Allowed WSS endpoints
        assert!(validate_hyperliquid_ws_url(
            "wss://api.hyperliquid.xyz/ws",
            &cfg
        ));
        assert!(validate_hyperliquid_ws_url(
            "wss://api.hyperliquid-testnet.xyz/ws",
            &cfg
        ));

        // Permitted loopback for tests (ws:// and wss://)
        assert!(validate_hyperliquid_ws_url("ws://127.0.0.1:18080", &cfg));
        assert!(validate_hyperliquid_ws_url("ws://127.0.0.1:18080/ws", &cfg));
        assert!(validate_hyperliquid_ws_url("ws://localhost:8080/ws", &cfg));
        assert!(validate_hyperliquid_ws_url("ws://[::1]:8080/ws", &cfg));
        assert!(validate_hyperliquid_ws_url("wss://127.0.0.1:8443/ws", &cfg));

        // Rejected: non-loopback ws://, untrusted hosts, invalid schemes, userinfo
        assert!(!validate_hyperliquid_ws_url(
            "ws://api.hyperliquid.xyz/ws",
            &cfg
        ));
        assert!(!validate_hyperliquid_ws_url(
            "wss://evil.example.com/ws",
            &cfg
        ));
        assert!(!validate_hyperliquid_ws_url(
            "ws://169.254.169.254/latest",
            &cfg
        ));
        assert!(!validate_hyperliquid_ws_url(
            "https://api.hyperliquid.xyz/ws",
            &cfg
        ));
        assert!(!validate_hyperliquid_ws_url("http://127.0.0.1:8080", &cfg));
        assert!(!validate_hyperliquid_ws_url(
            "wss://user@api.hyperliquid.xyz/ws",
            &cfg
        ));
        assert!(!validate_hyperliquid_ws_url("wss://[::1/ws", &cfg));

        // Derivation from hyperliquid_api_url and hyperliquid_ws_url override
        assert_eq!(
            derive_hyperliquid_ws_url("https://api.hyperliquid.xyz/info", &cfg).as_deref(),
            Some("wss://api.hyperliquid.xyz/ws")
        );
        assert_eq!(
            derive_hyperliquid_ws_url("http://127.0.0.1:19999", &cfg).as_deref(),
            Some("ws://127.0.0.1:19999/ws")
        );
        assert_eq!(
            derive_hyperliquid_ws_url("https://evil.attacker.invalid/info", &cfg),
            None
        );

        let mut override_cfg = ConfigMap::new();
        override_cfg.insert("execution_mode", "LIVE");
        override_cfg.insert("hyperliquid_api_url", "https://api.hyperliquid.xyz/info");
        assert_eq!(
            resolve_hyperliquid_ws_url(&override_cfg),
            "wss://api.hyperliquid.xyz/ws"
        );
        override_cfg.insert("hyperliquid_ws_url", "ws://127.0.0.1:17777");
        assert_eq!(
            resolve_hyperliquid_ws_url(&override_cfg),
            "ws://127.0.0.1:17777"
        );
        // Untrusted explicit override falls back safely to canonical HL host
        override_cfg.insert("hyperliquid_ws_url", "wss://evil.example.com/ws");
        assert_eq!(
            resolve_hyperliquid_ws_url(&override_cfg),
            "wss://api.hyperliquid.xyz/ws"
        );
    }

    #[test]
    fn test_ws_state_cache_ingestion_freshness_and_submit_bypass() {
        let cache = WsStateCache::new();
        cache.set_market_connected(true);
        cache.set_user_connected(true);

        let t0 = 1_750_000_000_000i64;
        cache.ingest_frame_value_at(
            &json!({
                "channel": "allMids",
                "data": { "mids": { "BTC": "65432.5", "kPEPE": "0.0123" } }
            }),
            false,
            t0,
        );
        cache.ingest_frame_value_at(
            &json!({
                "channel": "activeAssetCtx",
                "data": {
                    "coin": "BTC",
                    "ctx": {
                        "markPx": "65430.0",
                        "oraclePx": "65428.0",
                        "midPx": "65432.5",
                        "prevDayPx": "64000.0",
                        "funding": "0.0000125",
                        "openInterest": "1000.0",
                        "dayNtlVlm": "500000000.0"
                    }
                }
            }),
            false,
            t0,
        );
        cache.ingest_frame_value_at(
            &json!({
                "channel": "candle",
                "data": {
                    "t": 1_749_996_000_000i64,
                    "s": "BTC",
                    "i": "1h",
                    "o": "65000.0",
                    "h": "65500.0",
                    "l": "64900.0",
                    "c": "65430.0",
                    "v": "250.0"
                }
            }),
            false,
            t0,
        );
        cache.ingest_frame_value_at(
            &json!({
                "channel": "l2Book",
                "data": {
                    "coin": "BTC",
                    "levels": [
                        [{"px": "65430.0", "sz": "2.0"}],
                        [{"px": "65435.0", "sz": "2.0"}]
                    ]
                }
            }),
            false,
            t0,
        );
        cache.ingest_frame_value_at(
            &json!({
                "channel": "webData3",
                "data": {
                    "clearinghouseState": {
                        "marginSummary": { "accountValue": "1234.56" },
                        "assetPositions": []
                    },
                    "openOrders": [
                        { "coin": "BTC", "oid": 777u64, "orderType": "Stop Limit", "isTrigger": true, "triggerPx": "62000.0" }
                    ]
                }
            }),
            true,
            t0,
        );
        cache.ingest_frame_value_at(
            &json!({
                "channel": "orderUpdates",
                "data": [{
                    "order": {
                        "coin": "BTC",
                        "side": "B",
                        "limitPx": "65400.0",
                        "sz": "0.0",
                        "origSz": "0.05",
                        "oid": 888u64,
                        "cloid": "0xabc123"
                    },
                    "status": "filled",
                    "statusTimestamp": t0
                }]
            }),
            true,
            t0,
        );

        assert_eq!(
            cache
                .get_fresh_all_mids_at(t0 + 4_000, WS_MIDS_FRESH_MS)
                .and_then(|m| m.get("BTC").copied()),
            Some(65_432.5)
        );
        assert!(cache
            .get_fresh_all_mids_at(t0 + 5_001, WS_MIDS_FRESH_MS)
            .is_none());

        assert!(cache
            .get_fresh_l2_book_depth_at("BTC", t0 + 2_500, WS_L2_BOOK_FRESH_MS)
            .is_some());
        assert!(cache
            .get_fresh_l2_book_depth_at("BTC", t0 + 3_001, WS_L2_BOOK_FRESH_MS)
            .is_none());

        assert!(cache
            .get_fresh_clearinghouse_state_at(t0 + 1_000, WS_CLEARINGHOUSE_FRESH_MS)
            .is_some());
        assert_eq!(
            cache
                .get_fresh_open_orders_at(t0 + 1_000, WS_OPEN_ORDERS_FRESH_MS)
                .map(|s| (s.orders.len(), s.metadata_complete)),
            Some((1, true))
        );
        assert_eq!(
            cache
                .get_order_update_by_cloid("0xABC123")
                .map(|u| (u.oid, u.status)),
            Some((888, "filled".to_string()))
        );

        // Post-order-submit bypass for 1_500 ms
        cache.mark_order_submit_at(t0 + 1_000);
        assert!(cache
            .get_fresh_clearinghouse_state_at(t0 + 2_000, WS_CLEARINGHOUSE_FRESH_MS)
            .is_none());
        assert!(cache
            .get_fresh_open_orders_at(t0 + 2_000, WS_OPEN_ORDERS_FRESH_MS)
            .is_none());
        // Restores once 1_500 ms passes (within 5_000 ms frame window)
        assert!(cache
            .get_fresh_clearinghouse_state_at(t0 + 2_600, WS_CLEARINGHOUSE_FRESH_MS)
            .is_some());

        // Subscription set cap <= 110
        let many_coins: Vec<String> = (0..60).map(|i| format!("COIN{i}")).collect();
        let sub_set = WsSubscriptionSet {
            ctx_and_candle_coins: many_coins.clone(),
            l2_book_coins: many_coins,
        };
        assert!(sub_set.total_subscription_count() <= MAX_MARKET_WS_SUBSCRIPTIONS);
    }

    #[test]
    fn test_ws_manager_2_connection_ping_stale_reconnect_and_gap_fill() {
        let server = MockWsServer::start();
        let db = DatabaseManager::new_isolated_test(None).expect("isolated db");
        db.init_schema().expect("init schema");
        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_ws_url", &server.ws_url(), None)
            .unwrap();
        db.set_config_value(
            "wallet_address",
            "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
            None,
        )
        .unwrap();
        let cfg = db.load_config_or_default();

        let cache = Arc::new(WsStateCache::new());
        let (sub_tx, sub_rx) = watch::channel(WsSubscriptionSet {
            ctx_and_candle_coins: vec!["BTC".to_string()],
            l2_book_coins: vec!["BTC".to_string()],
        });

        let timing = WsTimingConfig {
            ping_interval: Duration::from_millis(80),
            stale_timeout: Duration::from_millis(260),
            connect_timeout: Duration::from_secs(2),
            backoff_min: Duration::from_millis(25),
            backoff_max: Duration::from_millis(60),
            healthy_reset_duration: Duration::from_secs(5),
            subscribe_pace_interval: Duration::from_millis(10),
            status_persist_interval: Duration::from_millis(80),
        };

        let mut mgr =
            HyperliquidWsManager::start(db.clone(), Arc::clone(&cache), sub_rx, &cfg, timing)
                .expect("start ws manager");

        // Wait for both Market + User connections + initial subscriptions + pings
        assert!(
            server.wait_for_connections(2, Duration::from_secs(3)),
            "expected 2 WS connections (Market + User)"
        );
        assert!(
            server.wait_for_received(8, Duration::from_secs(3)),
            "expected market + user subscription messages"
        );
        assert!(cache.is_market_ws_connected());
        assert!(cache.is_user_ws_connected());
        assert!(cache.needs_rest_gap_reconcile());
        assert!(cache.try_clear_rest_gap_reconcile(cache.user_reconnect_count()));
        assert!(!cache.needs_rest_gap_reconcile());

        // Dynamic l2Book watch update without reconnect
        let _ = sub_tx.send(WsSubscriptionSet {
            ctx_and_candle_coins: vec!["BTC".to_string()],
            l2_book_coins: vec!["BTC".to_string(), "ETH".to_string()],
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut saw_eth_l2 = false;
        while std::time::Instant::now() < deadline {
            if server.received_json().iter().any(|v| {
                v.get("method").and_then(Value::as_str) == Some("subscribe")
                    && v.pointer("/subscription/type").and_then(Value::as_str) == Some("l2Book")
                    && v.pointer("/subscription/coin").and_then(Value::as_str) == Some("ETH")
            }) {
                saw_eth_l2 = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(15));
        }
        assert!(saw_eth_l2, "expected dynamic ETH l2Book subscription");

        // Push market + user frames and verify ingestion
        server.push_json(&json!({
            "channel": "allMids",
            "data": { "mids": { "BTC": "71234.5" } }
        }));
        server.push_json(&json!({
            "channel": "webData3",
            "data": {
                "clearinghouseState": {
                    "marginSummary": { "accountValue": "4321.0" },
                    "assetPositions": []
                },
                "openOrders": []
            }
        }));
        let ingest_deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < ingest_deadline {
            if cache.get_fresh_all_mids().is_some()
                && cache.get_fresh_clearinghouse_state().is_some()
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(15));
        }
        assert_eq!(
            cache
                .get_fresh_all_mids()
                .and_then(|m| m.get("BTC").copied()),
            Some(71_234.5)
        );
        assert!(cache.get_fresh_clearinghouse_state().is_some());

        // Disable auto_pong to trigger 260ms stale watchdog forced reconnect + gap-fill flag
        let conns_before = server.connection_count();
        server.set_auto_pong(false);
        assert!(
            server.wait_for_connections(conns_before + 2, Duration::from_secs(3)),
            "stale watchdog must force reconnect of Market & User WS"
        );
        server.set_auto_pong(true);
        assert!(
            cache.needs_rest_gap_reconcile(),
            "reconnect must set needs_rest_gap_reconcile=true"
        );

        // Verify status persisted to system_config.ws_stream_status_json
        let status_deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut status_ok = false;
        while std::time::Instant::now() < status_deadline {
            let fresh_cfg = db.load_config_or_default();
            let raw_json = fresh_cfg.get_str("ws_stream_status_json", "");
            if let Ok(parsed) = serde_json::from_str::<WsStreamStatus>(&raw_json) {
                if parsed.enabled
                    && (parsed.market_reconnect_count > 0 || parsed.user_reconnect_count > 0)
                {
                    status_ok = true;
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(status_ok, "expected ws_stream_status_json persisted to DB");

        mgr.shutdown();
    }

    #[test]
    fn test_ws_state_cache_rings_candle_upsert_and_enable_gate() {
        let cache = WsStateCache::new();
        cache.set_market_connected(true);
        cache.set_user_connected(true);
        let t0 = 1_750_000_000_000i64;

        // Candle same-timestamp in-place update + new bar append + ring cap at 200
        for i in 0..205i64 {
            cache.ingest_frame_value_at(
                &json!({
                    "channel": "candle",
                    "data": {
                        "t": t0 + i * 3_600_000,
                        "s": "ETH",
                        "i": "1h",
                        "o": "3400.0",
                        "h": "3420.0",
                        "l": "3390.0",
                        "c": format!("{:.1}", 3400.0 + i as f64),
                        "v": "100.0"
                    }
                }),
                false,
                t0,
            );
        }
        cache.ingest_frame_value_at(
            &json!({
                "channel": "candle",
                "data": {
                    "t": t0 + 204 * 3_600_000,
                    "s": "ETH",
                    "i": "1h",
                    "o": "3400.0",
                    "h": "3700.0",
                    "l": "3390.0",
                    "c": "3699.5",
                    "v": "999.0"
                }
            }),
            false,
            t0 + 100,
        );
        let (bars, ts) = cache
            .get_fresh_candles_at("ETH", "1h", t0 + 100, WS_CANDLE_FRESH_MS)
            .expect("candles present");
        assert_eq!(ts, t0 + 100);
        assert_eq!(bars.len(), WS_CANDLES_RING_CAP);
        assert!((bars.last().unwrap().close - 3699.5).abs() < 1e-6);

        // openOrders channel without trigger metadata marks metadata_complete = false
        cache.ingest_frame_value_at(
            &json!({
                "channel": "openOrders",
                "data": [{ "coin": "ETH", "oid": 1001u64, "limitPx": "3400.0", "sz": "1.0" }]
            }),
            true,
            t0,
        );
        assert_eq!(
            cache
                .get_fresh_open_orders_at(t0, WS_OPEN_ORDERS_FRESH_MS)
                .map(|s| s.metadata_complete),
            Some(false)
        );

        // userFills, userFundings (flat WS format), and userEvents deduplication
        cache.ingest_frame_value_at(
            &json!({
                "channel": "userFills",
                "data": {
                    "isSnapshot": true,
                    "fills": [
                        { "coin": "ETH", "side": "B", "px": "3400.0", "sz": "0.5", "fee": "0.25", "closedPnl": "0.0", "time": t0, "oid": 1u64, "tid": 11u64 },
                        { "coin": "ETH", "side": "B", "px": "3400.0", "sz": "0.5", "fee": "0.25", "closedPnl": "0.0", "time": t0, "oid": 1u64, "tid": 11u64 }
                    ]
                }
            }),
            true,
            t0,
        );
        cache.ingest_frame_value_at(
            &json!({
                "channel": "userFundings",
                "data": {
                    "isSnapshot": true,
                    "fundings": [
                        { "time": t0, "coin": "ETH", "usdc": "-1.25", "szi": "0.5", "fundingRate": "0.0001", "nSamples": 1 }
                    ]
                }
            }),
            true,
            t0,
        );
        cache.ingest_frame_value_at(
            &json!({
                "channel": "userEvents",
                "data": {
                    "fills": [
                        { "coin": "BTC", "side": "A", "px": "65000.0", "sz": "0.01", "fee": "0.1", "closedPnl": "5.0", "time": t0 + 1, "oid": 2u64, "tid": 12u64 }
                    ],
                    "funding": { "time": t0 + 2, "coin": "BTC", "usdc": "0.42", "szi": "-0.01", "fundingRate": "0.0001", "nSamples": 1 }
                }
            }),
            true,
            t0 + 2,
        );
        assert_eq!(cache.get_recent_user_fills().0.len(), 2);
        assert_eq!(cache.get_recent_user_fundings().0.len(), 2);

        // Enable gate checks
        let db = DatabaseManager::new_isolated_test(None).expect("isolated db");
        db.init_schema().expect("init schema");
        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_testnet_url", "http://127.0.0.1:18080", None)
            .unwrap();
        let http_only_cfg = db.load_config_or_default();
        assert!(
            !should_enable_ws_manager(&db, &http_only_cfg),
            "HTTP-only ephemeral test DB without explicit WS URL must not spawn WS manager"
        );
        db.set_config_value("hyperliquid_ws_url", "ws://127.0.0.1:18081", None)
            .unwrap();
        let ws_cfg = db.load_config_or_default();
        assert!(should_enable_ws_manager(&db, &ws_cfg));
        db.set_config_value("hl_websocket_enabled", "false", None)
            .unwrap();
        let disabled_cfg = db.load_config_or_default();
        assert!(!should_enable_ws_manager(&db, &disabled_cfg));
    }

    #[test]
    fn test_ws_manager_abrupt_drop_and_paced_resubscription() {
        let server = MockWsServer::start();
        let db = DatabaseManager::new_isolated_test(None).expect("isolated db");
        db.init_schema().expect("init schema");
        db.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        db.set_config_value("hyperliquid_ws_url", &server.ws_url(), None)
            .unwrap();
        let cfg = db.load_config_or_default();

        let cache = Arc::new(WsStateCache::new());
        let (_sub_tx, sub_rx) = watch::channel(WsSubscriptionSet {
            ctx_and_candle_coins: vec!["BTC".to_string(), "ETH".to_string()],
            l2_book_coins: vec!["BTC".to_string(), "ETH".to_string()],
        });

        let timing = WsTimingConfig {
            ping_interval: Duration::from_secs(5),
            stale_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(2),
            backoff_min: Duration::from_millis(20),
            backoff_max: Duration::from_millis(50),
            healthy_reset_duration: Duration::from_secs(5),
            subscribe_pace_interval: Duration::from_millis(25),
            status_persist_interval: Duration::from_secs(5),
        };

        let mut mgr =
            HyperliquidWsManager::start(db, Arc::clone(&cache), sub_rx, &cfg, timing).unwrap();
        // Empty wallet -> 1 Market connection with 1 (allMids) + 2 (l2Book) + 6 (ctx+candles) = 9 subs
        assert!(server.wait_for_connections(1, Duration::from_secs(2)));
        let t_sub_start = std::time::Instant::now();
        assert!(server.wait_for_received(9, Duration::from_secs(3)));
        let sub_elapsed = t_sub_start.elapsed();
        assert!(
            sub_elapsed >= Duration::from_millis(120),
            "9 subscriptions paced at 25ms must take >= ~200ms (got {:?})",
            sub_elapsed
        );

        server.clear_received();
        let conns_before = server.connection_count();
        server.abrupt_drop_all();
        assert!(server.wait_for_connections(conns_before + 1, Duration::from_secs(3)));
        assert!(server.wait_for_received(9, Duration::from_secs(3)));
        assert!(cache.market_reconnect_count() >= 1);
        assert!(cache.needs_rest_gap_reconcile());
        mgr.shutdown();
    }
}
