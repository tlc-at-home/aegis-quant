//! Shared Hyperliquid `/info` & `/exchange` HTTP client and SSRF/MITM endpoint validator (`aegis-execution::http_client`).

use crate::signing::{
    next_monotonic_nonce, sign_l1_action, CancelAction, CancelWire, OrderAction, OrderWire,
};
use aegis_core::{round_dp, AegisError, ConfigMap, ExecutionMode, Result};
use aegis_db::DatabaseManager;
use parking_lot::Mutex;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

pub const HYPERLIQUID_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
pub const HYPERLIQUID_EXCHANGE_URL: &str = "https://api.hyperliquid.xyz/exchange";
pub const HYPERLIQUID_TESTNET_INFO_URL: &str = "https://api.hyperliquid-testnet.xyz/info";
pub const HYPERLIQUID_TESTNET_EXCHANGE_URL: &str = "https://api.hyperliquid-testnet.xyz/exchange";

static SHARED_BLOCKING_CLIENTS: OnceLock<Mutex<HashMap<(u64, bool), Client>>> = OnceLock::new();

/// Returns a process-wide cached `reqwest::blocking::Client` built on a dedicated OS thread
/// so dropping inside an async runtime (`#[tokio::test]` / Axum) never panics.
pub fn shared_blocking_client(timeout_sec: u64) -> Client {
    shared_blocking_client_opts(timeout_sec, false)
}

pub fn shared_blocking_client_opts(timeout_sec: u64, no_proxy: bool) -> Client {
    let t_sec = timeout_sec.max(1);
    let cache = SHARED_BLOCKING_CLIENTS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(existing) = cache.lock().get(&(t_sec, no_proxy)).cloned() {
        return existing;
    }
    let client = std::thread::spawn(move || {
        let mut builder = Client::builder().timeout(Duration::from_secs(t_sec));
        if no_proxy {
            builder = builder.no_proxy();
        }
        builder.build().unwrap_or_else(|_| Client::new())
    })
    .join()
    .expect("Failed to initialize shared Hyperliquid blocking HTTP client");
    let mut guard = cache.lock();
    guard
        .entry((t_sec, no_proxy))
        .or_insert_with(|| client.clone());
    client
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

fn compute_info_retry_backoff_ms(attempt: usize, retry_after_header: Option<&str>) -> u64 {
    let default_ms = HL_INFO_RETRY_BACKOFF_MS
        .get(attempt)
        .copied()
        .unwrap_or(650);
    if let Some(raw) = retry_after_header {
        if let Ok(secs) = raw.trim().parse::<u64>() {
            return secs.saturating_mul(1_000).min(HL_INFO_MAX_RETRY_AFTER_MS);
        }
    }
    default_ms
}

/// Shared blocking JSON POST helper with endpoint validation and bounded `/info` backoff.
pub fn http_post_json(
    http_client: &Client,
    timeout_sec: u64,
    client_timeout_sec: u64,
    url: &str,
    payload: &Value,
    cfg: &ConfigMap,
) -> Option<Value> {
    if !validate_hyperliquid_endpoint_url(url, cfg) {
        return None;
    }
    let is_loopback =
        url.contains("127.0.0.1") || url.contains("localhost") || url.contains("[::1]");
    let owned_client;
    let client_ref = if timeout_sec == client_timeout_sec && !is_loopback {
        http_client
    } else {
        owned_client = shared_blocking_client_opts(timeout_sec, is_loopback);
        &owned_client
    };

    let allow_info_retry = is_idempotent_info_url(url);
    let max_attempts = if allow_info_retry {
        1 + HL_INFO_MAX_RETRIES
    } else {
        1
    };

    for attempt in 0..max_attempts {
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
                Ok(val) => Some(val),
                Err(e) => {
                    tracing::warn!(url = url, error = %e, "Failed to parse JSON from Hyperliquid endpoint");
                    None
                }
            };
        }

        if allow_info_retry
            && attempt + 1 < max_attempts
            && is_retryable_info_status(status.as_u16())
        {
            let retry_after = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok());
            let backoff_ms = compute_info_retry_backoff_ms(attempt, retry_after);
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

/// Parsed acknowledgment from a Hyperliquid `/exchange` order submission.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeOrderAck {
    pub accepted: bool,
    pub status: String, // "RESTING", "FILLED", "REJECTED"
    pub exchange_order_id: Option<String>,
    pub filled_quantity: f64,
    pub avg_fill_price: f64,
    pub fee_usdc: f64,
    pub raw_error: Option<String>,
}

/// Parses a Hyperliquid `/exchange` JSON response into an [`ExchangeOrderAck`].
pub fn parse_exchange_order_ack(
    resp: &Value,
    _requested_qty: f64,
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

    let Some(first) = first_opt else {
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

    if let Some(err_msg) = first.get("error").and_then(Value::as_str) {
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

    if first.as_str() == Some("waitingForTrigger") || first.as_str() == Some("waitingForFill") {
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

    if let Some(waiting) = first
        .get("waitingForTrigger")
        .or_else(|| first.get("waitingForFill"))
    {
        let oid = waiting
            .get("oid")
            .and_then(Value::as_u64)
            .or_else(|| {
                waiting
                    .get("oid")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .unwrap_or(0);
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

    if let Some(resting) = first.get("resting") {
        let oid = resting
            .get("oid")
            .and_then(Value::as_u64)
            .or_else(|| {
                resting
                    .get("oid")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .unwrap_or(0);
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

    if let Some(filled) = first.get("filled") {
        let oid = filled
            .get("oid")
            .and_then(Value::as_u64)
            .or_else(|| {
                filled
                    .get("oid")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .unwrap_or(0);
        let eoid = (oid > 0).then(|| format!("hl-{oid}"));
        let avg_px = filled
            .get("avgPx")
            .and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(ref_price);
        let total_sz = filled
            .get("totalSz")
            .and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(0.0);
        let fee = filled
            .get("fee")
            .and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
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

const HL_USER_FUNDING_PAGE_LIMIT: usize = 500;
const HL_USER_FUNDING_MAX_PAGES: usize = 50;

fn extract_funding_time_ms(ev: &Value) -> Option<i64> {
    ev.get("time").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
    })
}

fn funding_event_dedup_key(ev: &Value) -> (i64, String, String, String, String) {
    let time_ms = extract_funding_time_ms(ev).unwrap_or(0);
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

/// High-level Hyperliquid `/info` and `/exchange` client for Aegis Quant v2.0.
#[derive(Clone)]
pub struct HyperliquidHttpClient {
    pub timeout_sec: u64,
    client: Client,
}

impl HyperliquidHttpClient {
    pub fn new(timeout_sec: u64) -> Self {
        let t = timeout_sec.max(1);
        Self {
            timeout_sec: t,
            client: shared_blocking_client(t),
        }
    }

    pub fn post_info(&self, payload: &Value, cfg: &ConfigMap) -> Result<Value> {
        let url = resolve_info_url(cfg);
        http_post_json(
            &self.client,
            self.timeout_sec,
            self.timeout_sec,
            &url,
            payload,
            cfg,
        )
        .ok_or_else(|| {
            AegisError::Execution(format!("Hyperliquid /info request failed at '{url}'"))
        })
    }

    /// Polls `metaAndAssetCtxs` returning universe metadata and live contexts across all 150+ Hyperliquid perps.
    pub fn fetch_meta_and_asset_ctxs(&self, cfg: &ConfigMap) -> Result<Value> {
        self.post_info(&json!({ "type": "metaAndAssetCtxs" }), cfg)
    }

    /// Polls `allMids` returning `coin -> mid_price` across all Hyperliquid markets.
    pub fn fetch_all_mids(&self, cfg: &ConfigMap) -> Result<HashMap<String, f64>> {
        let raw = self.post_info(&json!({ "type": "allMids" }), cfg)?;
        let mut out = HashMap::new();
        if let Some(obj) = raw.as_object() {
            for (coin, val) in obj {
                let px = val
                    .as_f64()
                    .or_else(|| val.as_str().and_then(|s| s.parse::<f64>().ok()))
                    .unwrap_or(0.0);
                if px.is_finite() && px > 0.0 {
                    out.insert(coin.trim().to_string(), px);
                }
            }
        }
        Ok(out)
    }

    /// Polls `l2Book` for `coin` and returns `(best_bid, best_ask)` if available.
    pub fn fetch_l2_book(&self, coin: &str, cfg: &ConfigMap) -> Result<Option<(f64, f64)>> {
        let raw = self.post_info(&json!({ "type": "l2Book", "coin": coin }), cfg)?;
        let levels = raw.get("levels").and_then(Value::as_array);
        let Some(levels) = levels else {
            return Ok(None);
        };
        if levels.len() < 2 {
            return Ok(None);
        }
        let parse_top = |side_arr: &Value| -> Option<f64> {
            let first = side_arr.as_array()?.first()?;
            first
                .get("px")
                .and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                })
                .filter(|v| v.is_finite() && *v > 0.0)
        };
        match (parse_top(&levels[0]), parse_top(&levels[1])) {
            (Some(bid), Some(ask)) if bid <= ask => Ok(Some((bid, ask))),
            _ => Ok(None),
        }
    }

    /// Polls `candleSnapshot` for `(coin, interval)` between `start_ms` and `end_ms`.
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
        let raw = self.post_info(&payload, cfg)?;
        Ok(raw.as_array().cloned().unwrap_or_default())
    }

    /// Polls `clearinghouseState` for `user_wallet`.
    pub fn fetch_clearinghouse_state(&self, user_wallet: &str, cfg: &ConfigMap) -> Result<Value> {
        self.post_info(
            &json!({
                "type": "clearinghouseState",
                "user": user_wallet.trim(),
            }),
            cfg,
        )
    }

    /// Polls `spotClearinghouseState` for `user_wallet` (used for unified/spot USDC balance checks).
    pub fn fetch_spot_clearinghouse_state(
        &self,
        user_wallet: &str,
        cfg: &ConfigMap,
    ) -> Result<Value> {
        self.post_info(
            &json!({
                "type": "spotClearinghouseState",
                "user": user_wallet.trim(),
            }),
            cfg,
        )
    }

    /// Polls `openOrders` for `user_wallet`.
    pub fn fetch_open_orders(&self, user_wallet: &str, cfg: &ConfigMap) -> Result<Vec<Value>> {
        let raw = self.post_info(
            &json!({
                "type": "openOrders",
                "user": user_wallet.trim(),
            }),
            cfg,
        )?;
        Ok(raw.as_array().cloned().unwrap_or_default())
    }

    /// Polls `frontendOpenOrders` for `user_wallet` (includes trigger/SL/TP metadata and `oid`/`cloid`).
    pub fn fetch_frontend_open_orders(
        &self,
        user_wallet: &str,
        cfg: &ConfigMap,
    ) -> Result<Vec<Value>> {
        let raw = self.post_info(
            &json!({
                "type": "frontendOpenOrders",
                "user": user_wallet.trim(),
            }),
            cfg,
        )?;
        Ok(raw.as_array().cloned().unwrap_or_default())
    }

    /// Polls `userFills` for `user_wallet` returning historical trade fills from Hyperliquid L1.
    pub fn fetch_user_fills(&self, user_wallet: &str, cfg: &ConfigMap) -> Result<Vec<Value>> {
        let raw = self.post_info(
            &json!({
                "type": "userFills",
                "user": user_wallet.trim(),
            }),
            cfg,
        )?;
        Ok(raw.as_array().cloned().unwrap_or_default())
    }

    /// Polls `userFunding` for `user_wallet` returning paginated historical funding ledger entries from Hyperliquid L1.
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

        for page_idx in 0..max_pages {
            let raw = match self.post_info(
                &json!({
                    "type": "userFunding",
                    "user": trimmed_user,
                    "startTime": cursor_ms,
                }),
                cfg,
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
                if let Some(t) = extract_funding_time_ms(&ev) {
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

    /// Signs and posts an EIP-712 L1 action to `/exchange` with automatic single-retry on nonce collision.
    pub fn post_exchange_action<T: Serialize>(
        &self,
        action: &T,
        private_key: &str,
        db: Option<&DatabaseManager>,
        cfg: &ConfigMap,
    ) -> Result<Value> {
        let is_testnet = is_testnet_configured(cfg);
        let url = resolve_exchange_url_for_network(cfg, is_testnet);
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
            let Some(resp) = http_post_json(
                &self.client,
                self.timeout_sec,
                self.timeout_sec,
                &url,
                &payload,
                cfg,
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
                        "Hyperliquid rejected nonce; retrying with incremented monotonic nonce"
                    );
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

    /// Cancels a batch of resting orders on Hyperliquid in a single signed `/exchange` request.
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
        let action = CancelAction {
            action_type: "cancel".to_string(),
            cancels,
        };
        self.post_exchange_action(&action, private_key, db, cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn spawn_mock_info_server<F>(handler: F) -> (u16, std::sync::mpsc::Sender<()>)
    where
        F: Fn(&Value) -> (u16, Value) + Send + Sync + 'static,
    {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::Arc;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback mock server");
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        let handler = Arc::new(handler);

        std::thread::spawn(move || loop {
            if stop_rx.try_recv().is_ok() {
                break;
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let _ = stream.set_nonblocking(false);
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    let mut header_end = None;
                    let mut content_len = 0usize;
                    while let Ok(n) = stream.read(&mut tmp) {
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if header_end.is_none() {
                            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                header_end = Some(pos + 4);
                                let headers = String::from_utf8_lossy(&buf[..pos]);
                                for line in headers.lines() {
                                    if let Some((k, v)) = line.split_once(':') {
                                        if k.trim().eq_ignore_ascii_case("content-length") {
                                            content_len = v.trim().parse::<usize>().unwrap_or(0);
                                        }
                                    }
                                }
                            }
                        }
                        if let Some(hend) = header_end {
                            if buf.len() >= hend + content_len {
                                break;
                            }
                        }
                    }
                    let body_slice = header_end
                        .map(|h| &buf[h..buf.len().min(h + content_len)])
                        .unwrap_or(&[]);
                    let req_json: Value = serde_json::from_slice(body_slice).unwrap_or(Value::Null);
                    let (status, resp_json) = handler(&req_json);
                    let resp_body = resp_json.to_string();
                    let resp = format!(
                        "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        status,
                        resp_body.len(),
                        resp_body
                    );
                    let _ = stream.write_all(resp.as_bytes());
                    let _ = stream.flush();
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        });

        (port, stop_tx)
    }

    #[test]
    fn test_fetch_user_funding_paginates_and_deduplicates_boundary() {
        let mut ledger: Vec<Value> = Vec::new();
        let base_ts = 1_795_000_000_000i64;
        // 498 single-coin entries before boundary 1
        for i in 0..498 {
            ledger.push(json!({
                "time": base_ts + i,
                "hash": format!("0x{:04x}", i),
                "delta": { "type": "funding", "coin": "BTC", "usdc": "0.10" }
            }));
        }
        // 5 coins sharing boundary 1 timestamp across the 500-record cutoff (2 on page 1, 3 overflow to page 2)
        let boundary1 = base_ts + 500;
        for (idx, coin) in ["ETH", "SOL", "ARB", "AVAX", "DOGE"].iter().enumerate() {
            ledger.push(json!({
                "time": boundary1,
                "hash": "0x0000",
                "delta": { "type": "funding", "coin": coin, "usdc": format!("0.2{}", idx) }
            }));
        }
        // 493 entries before boundary 2 (so 5 boundary1 + 493 + 2 of 3 boundary2 = 500 on page 2)
        for i in 0..493 {
            ledger.push(json!({
                "time": boundary1 + 100 + i,
                "hash": format!("0x1{:04x}", i),
                "delta": { "type": "funding", "coin": "OP", "usdc": "-0.05" }
            }));
        }
        // 3 coins sharing boundary 2 across page 2 -> page 3, plus 4 newer entries
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
        let expected_total = ledger.len(); // 498 + 5 + 493 + 3 + 4 = 1003

        let requested_cursors = std::sync::Arc::new(Mutex::new(Vec::<i64>::new()));
        let cursors_clone = requested_cursors.clone();
        let ledger_arc = std::sync::Arc::new(ledger);
        let ledger_clone = ledger_arc.clone();

        let (port, stop_tx) = spawn_mock_info_server(move |body| {
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
        cfg.insert(
            "hyperliquid_testnet_url",
            format!("http://127.0.0.1:{port}"),
        );
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");

        let client = HyperliquidHttpClient::new(5);
        let fetched = client
            .fetch_user_funding(
                "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
                Some(base_ts),
                &cfg,
            )
            .expect("paginated userFunding fetch succeeds");

        let _ = stop_tx.send(());

        assert_eq!(fetched.len(), expected_total);
        // Ensure zero duplicates and zero skipped boundary coins across both 500-entry boundaries
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
        // Test saturated equal-millisecond page advancement and `n` field dedup
        let ledger = std::sync::Arc::new(vec![
            json!({ "time": 1000, "hash": "0x0", "n": 1, "delta": { "type": "funding", "coin": "BTC", "usdc": "0.1" } }),
            json!({ "time": 1000, "hash": "0x0", "n": 2, "delta": { "type": "funding", "coin": "BTC", "usdc": "0.1" } }),
            json!({ "time": 1000, "hash": "0x0", "n": 3, "delta": { "type": "funding", "coin": "ETH", "usdc": "0.2" } }),
            json!({ "time": 2000, "hash": "0x1", "n": 4, "delta": { "type": "funding", "coin": "SOL", "usdc": "0.3" } }),
        ]);
        let ledger_clone = ledger.clone();
        let (port, stop_tx) = spawn_mock_info_server(move |body| {
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
        cfg.insert(
            "hyperliquid_testnet_url",
            format!("http://127.0.0.1:{port}"),
        );
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");

        let client = HyperliquidHttpClient::new(5);
        let fetched = client
            .fetch_user_funding_paginated("0xabc", Some(1000), 3, 10, &cfg)
            .expect("saturated ms pagination succeeds");

        let _ = stop_tx.send(());
        assert_eq!(fetched.len(), 4);
    }

    fn spawn_mock_path_server<F>(handler: F) -> (u16, std::sync::mpsc::Sender<()>)
    where
        F: Fn(&str, &Value) -> (u16, Vec<(&'static str, String)>, Value) + Send + Sync + 'static,
    {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::Arc;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback mock path server");
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        let handler = Arc::new(handler);

        std::thread::spawn(move || loop {
            if stop_rx.try_recv().is_ok() {
                break;
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let _ = stream.set_nonblocking(false);
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    let mut header_end = None;
                    let mut content_len = 0usize;
                    while let Ok(n) = stream.read(&mut tmp) {
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if header_end.is_none() {
                            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                header_end = Some(pos + 4);
                                let headers = String::from_utf8_lossy(&buf[..pos]);
                                for line in headers.lines() {
                                    if let Some((k, v)) = line.split_once(':') {
                                        if k.trim().eq_ignore_ascii_case("content-length") {
                                            content_len = v.trim().parse::<usize>().unwrap_or(0);
                                        }
                                    }
                                }
                            }
                        }
                        if let Some(hend) = header_end {
                            if buf.len() >= hend + content_len {
                                break;
                            }
                        }
                    }
                    let req_path = header_end
                        .and_then(|hend| {
                            let head = String::from_utf8_lossy(&buf[..hend]);
                            head.lines()
                                .next()
                                .and_then(|line| line.split_whitespace().nth(1))
                                .map(str::to_string)
                        })
                        .unwrap_or_default();
                    let body_slice = header_end
                        .map(|h| &buf[h..buf.len().min(h + content_len)])
                        .unwrap_or(&[]);
                    let req_json: Value = serde_json::from_slice(body_slice).unwrap_or(Value::Null);
                    let (status, extra_headers, resp_json) = handler(&req_path, &req_json);
                    let reason = match status {
                        200 => "OK",
                        429 => "Too Many Requests",
                        502 => "Bad Gateway",
                        503 => "Service Unavailable",
                        504 => "Gateway Timeout",
                        _ => "Error",
                    };
                    let mut header_block = String::new();
                    for (k, v) in extra_headers {
                        header_block.push_str(&format!("{k}: {v}\r\n"));
                    }
                    let resp_body = resp_json.to_string();
                    let resp = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n{header_block}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                        resp_body.len(),
                        resp_body
                    );
                    let _ = stream.write_all(resp.as_bytes());
                    let _ = stream.flush();
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        });

        (port, stop_tx)
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
    }

    #[test]
    fn test_http_post_json_retries_info_only_and_never_retries_exchange() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Instant;

        let info_calls = Arc::new(AtomicUsize::new(0));
        let exchange_calls = Arc::new(AtomicUsize::new(0));
        let scenario = Arc::new(Mutex::new("info_429_once"));

        let info_calls_srv = info_calls.clone();
        let exchange_calls_srv = exchange_calls.clone();
        let scenario_srv = scenario.clone();

        let (port, stop_tx) = spawn_mock_path_server(move |path, _body| {
            let mode = *scenario_srv.lock();
            if path == "/info" {
                let n = info_calls_srv.fetch_add(1, Ordering::SeqCst) + 1;
                return match mode {
                    "info_429_once" => {
                        if n == 1 {
                            (
                                429,
                                vec![("Retry-After", "0".to_string())],
                                json!({ "error": "rate_limited" }),
                            )
                        } else {
                            (200, vec![], json!({ "ok": true, "attempt": n }))
                        }
                    }
                    "info_429_then_502_then_ok" => {
                        if n == 1 {
                            (429, vec![], json!({ "error": "rate_limited" }))
                        } else if n == 2 {
                            (
                                502,
                                vec![("Retry-After", "0".to_string())],
                                json!({ "error": "bad_gateway" }),
                            )
                        } else {
                            (200, vec![], json!({ "ok": true, "attempt": n }))
                        }
                    }
                    "info_retry_after_clamped" => {
                        if n == 1 {
                            (
                                429,
                                vec![("Retry-After", "10".to_string())],
                                json!({ "error": "rate_limited" }),
                            )
                        } else {
                            (200, vec![], json!({ "clamped_ok": true, "attempt": n }))
                        }
                    }
                    "info_exhaust_503" => (503, vec![], json!({ "error": "unavailable" })),
                    _ => (200, vec![], json!({ "ok": true })),
                };
            }
            if path == "/exchange" {
                exchange_calls_srv.fetch_add(1, Ordering::SeqCst);
                return match mode {
                    "exchange_429" => (
                        429,
                        vec![("Retry-After", "0".to_string())],
                        json!({ "error": "rate_limited" }),
                    ),
                    "exchange_502" => (502, vec![], json!({ "error": "bad_gateway" })),
                    _ => (200, vec![], json!({ "status": "ok" })),
                };
            }
            (404, vec![], json!({ "error": "not_found" }))
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");

        let client = shared_blocking_client_opts(5, true);
        let info_url = format!("http://127.0.0.1:{port}/info");
        let exchange_url = format!("http://127.0.0.1:{port}/exchange");
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

        // 3. /info honors integer Retry-After clamped to <= 2000ms (10s header does NOT sleep 10s)
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

        let _ = stop_tx.send(());
    }
}
