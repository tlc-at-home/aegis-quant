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
use std::collections::HashMap;
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

/// Returns `true` when Hyperliquid requests should route to Testnet.
pub fn is_testnet_configured(cfg: &ConfigMap) -> bool {
    cfg.get_bool("hyperliquid_testnet", true) || cfg.execution_mode() == ExecutionMode::Testnet
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

/// Shared blocking JSON POST helper with endpoint validation.
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
    std::thread::scope(|s| {
        s.spawn(|| {
            let resp = match client_ref.post(url).json(payload).send() {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!(url = url, error = %e, "Hyperliquid HTTP POST request failed");
                    return None;
                }
            };
            if !resp.status().is_success() {
                tracing::warn!(
                    url = url,
                    status = %resp.status(),
                    "Hyperliquid endpoint returned non-2xx HTTP status"
                );
                return None;
            }
            match resp.json::<Value>() {
                Ok(val) => Some(val),
                Err(e) => {
                    tracing::warn!(url = url, error = %e, "Failed to parse JSON from Hyperliquid endpoint");
                    None
                }
            }
        })
        .join()
        .ok()
        .flatten()
    })
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
        let action = OrderAction {
            action_type: "order".to_string(),
            orders: vec![order],
            grouping: "na".to_string(),
        };
        let resp = self.post_exchange_action(&action, private_key, db, cfg)?;
        Ok(parse_exchange_order_ack(
            &resp,
            requested_qty,
            ref_price,
            fee_bps,
        ))
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
}
