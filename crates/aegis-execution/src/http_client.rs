//! Shared Hyperliquid HTTP client & SSRF/MITM endpoint validator (`aegis-execution::http_client`, `NH-2`, `NQ-2`).

use aegis_db::ConfigMap;
use parking_lot::Mutex;
use reqwest::blocking::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

pub const HYPERLIQUID_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
pub const HYPERLIQUID_EXCHANGE_URL: &str = "https://api.hyperliquid.xyz/exchange";
pub const HYPERLIQUID_TESTNET_INFO_URL: &str = "https://api.hyperliquid-testnet.xyz/info";
pub const HYPERLIQUID_TESTNET_EXCHANGE_URL: &str = "https://api.hyperliquid-testnet.xyz/exchange";

static SHARED_BLOCKING_CLIENTS: OnceLock<Mutex<HashMap<(u64, bool), Client>>> = OnceLock::new();

/// Returns a process-wide cached `reqwest::blocking::Client` for `timeout_sec` built on a dedicated OS thread
/// so dropping `ExecutionEngine` or `MarketIngestionEngine` inside an async runtime (`#[tokio::test]`) never panics.
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

/// Validates that a Hyperliquid endpoint URL uses `https://` and an allowlisted host (`NH-2`).
/// Loopback hosts (`127.0.0.1`, `localhost`, `[::1]`) are permitted over `http://` or `https://`
/// for offline and local mock test servers.
pub fn validate_hyperliquid_endpoint_url(url: &str, cfg: &ConfigMap) -> bool {
    let trimmed = url.trim();
    let (is_https, rest) = if let Some(r) = trimmed.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = trimmed.strip_prefix("http://") {
        (false, r)
    } else {
        tracing::warn!(url = trimmed, "Rejected Hyperliquid URL without http/https scheme");
        return false;
    };

    let authority = rest.split('/').next().unwrap_or("");
    if authority.is_empty() || authority.contains('@') {
        tracing::warn!(url = trimmed, "Rejected Hyperliquid URL with invalid authority");
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
            "Rejected non-HTTPS remote Hyperliquid endpoint URL (NH-2)"
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
            "Rejected untrusted Hyperliquid endpoint host not in hyperliquid_allowed_hosts (NH-2)"
        );
    }
    is_allowed
}

/// Resolves and validates the Hyperliquid `/info` endpoint URL from `system_config` (`NH-2`, `NQ-2`).
pub fn resolve_info_url(cfg: &ConfigMap) -> String {
    let is_testnet = cfg.get_bool("hyperliquid_testnet", false)
        || cfg
            .get_str("execution_mode", "PAPER")
            .eq_ignore_ascii_case("TESTNET");
    resolve_info_url_for_network(cfg, is_testnet)
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

/// Resolves and validates the Hyperliquid `/exchange` endpoint URL from `system_config` (`NH-2`, `NQ-2`).
pub fn resolve_exchange_url(cfg: &ConfigMap) -> String {
    let is_testnet = cfg.get_bool("hyperliquid_testnet", false)
        || cfg
            .get_str("execution_mode", "PAPER")
            .eq_ignore_ascii_case("TESTNET");
    resolve_exchange_url_for_network(cfg, is_testnet)
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

/// Shared blocking JSON POST helper with endpoint validation (`NH-2`, `NQ-2`).
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

impl crate::ExecutionEngine {
    pub(crate) fn info_url(&self, cfg: &ConfigMap, is_testnet: bool) -> String {
        if is_testnet
            && cfg
                .get_str("execution_mode", "PAPER")
                .eq_ignore_ascii_case("LIVE")
            && cfg.get_bool("hyperliquid_testnet", false)
        {
            tracing::warn!(
                "execution_mode is LIVE while hyperliquid_testnet=true; routing /info request to Testnet endpoint (ME-5)"
            );
        }
        resolve_info_url_for_network(cfg, is_testnet)
    }

    pub(crate) fn exchange_url(&self, cfg: &ConfigMap, is_testnet: bool) -> String {
        if is_testnet
            && cfg
                .get_str("execution_mode", "PAPER")
                .eq_ignore_ascii_case("LIVE")
            && cfg.get_bool("hyperliquid_testnet", false)
        {
            tracing::warn!(
                "execution_mode is LIVE while hyperliquid_testnet=true; routing /exchange action to Testnet endpoint (ME-5)"
            );
        }
        resolve_exchange_url_for_network(cfg, is_testnet)
    }

    pub(crate) fn http_post_json(&self, url: &str, payload: &Value) -> Option<Value> {
        let cfg = self.load_config_logged("ExecutionEngine.http_post_json");
        http_post_json(
            &self.http_client,
            self.timeout_sec,
            self.client_timeout_sec,
            url,
            payload,
            &cfg,
        )
    }
}
