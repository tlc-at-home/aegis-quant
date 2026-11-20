//! Config-switchable Discord Webhook dispatcher for Aegis Quant v2.0 (`aegis-alerts::discord`).

use crate::formatter::{
    html_to_discord_markdown, truncate_discord_markdown, DISCORD_MAX_MESSAGE_CHARS,
};
#[cfg(test)]
use crate::telegram::is_loopback_url;
use aegis_core::{sanitize_telemetry_message, AegisError, Result};
#[cfg(test)]
use parking_lot::Mutex;
use reqwest::blocking::Client as BlockingClient;
use serde::{Deserialize, Serialize};
use serde_json::json;
#[cfg(test)]
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
#[cfg(test)]
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

const ALLOWED_DISCORD_WEBHOOK_HOSTS: &[&str] = &[
    "discord.com",
    "discordapp.com",
    "ptb.discord.com",
    "canary.discord.com",
];

#[cfg(test)]
static DISCORD_CLIENTS: OnceLock<Mutex<HashMap<(u64, bool), BlockingClient>>> = OnceLock::new();

#[cfg(test)]
fn get_discord_client(timeout_sec: u64, is_loopback: bool) -> BlockingClient {
    let key = (timeout_sec.max(2), is_loopback);
    let cache = DISCORD_CLIENTS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(client) = cache.lock().get(&key) {
        return client.clone();
    }
    let mut builder = BlockingClient::builder()
        .timeout(Duration::from_secs(key.0))
        .redirect(reqwest::redirect::Policy::none());
    if is_loopback {
        builder = builder.no_proxy();
    }
    let client = builder.build().unwrap_or_else(|_| BlockingClient::new());
    cache.lock().insert(key, client.clone());
    client
}

fn is_globally_routable_ipv4(v4: Ipv4Addr) -> bool {
    let oct = v4.octets();
    let is_unspecified = oct[0] == 0; // 0.0.0.0/8
    let is_rfc1918 = oct[0] == 10
        || (oct[0] == 172 && (16..=31).contains(&oct[1]))
        || (oct[0] == 192 && oct[1] == 168);
    let is_cgnat = oct[0] == 100 && (64..=127).contains(&oct[1]); // 100.64.0.0/10
    let is_loopback = oct[0] == 127; // 127.0.0.0/8
    let is_link_local = oct[0] == 169 && oct[1] == 254; // 169.254.0.0/16
    let is_ietf_protocol = oct[0] == 192 && oct[1] == 0 && oct[2] == 0; // 192.0.0.0/24
    let is_doc = (oct[0] == 192 && oct[1] == 0 && oct[2] == 2)
        || (oct[0] == 198 && oct[1] == 51 && oct[2] == 100)
        || (oct[0] == 203 && oct[1] == 0 && oct[2] == 113);
    let is_benchmarking = oct[0] == 198 && (oct[1] == 18 || oct[1] == 19); // 198.18.0.0/15
    let is_multicast_or_reserved = oct[0] >= 224; // 224.0.0.0/4 + 240.0.0.0/4 + 255.255.255.255

    !(is_unspecified
        || is_rfc1918
        || is_cgnat
        || is_loopback
        || is_link_local
        || is_ietf_protocol
        || is_doc
        || is_benchmarking
        || is_multicast_or_reserved)
}

fn is_globally_routable_ipv6(v6: Ipv6Addr) -> bool {
    if let Some(v4) = v6.to_ipv4_mapped() {
        return is_globally_routable_ipv4(v4);
    }
    let seg = v6.segments();
    // Reject ::/96 (unspecified ::, loopback ::1, deprecated IPv4-compatible ::a.b.c.d)
    if seg[..6] == [0, 0, 0, 0, 0, 0] {
        return false;
    }
    let is_ula = (seg[0] & 0xfe00) == 0xfc00; // fc00::/7
    let is_link_local = (seg[0] & 0xffc0) == 0xfe80; // fe80::/10
    let is_multicast = (seg[0] & 0xff00) == 0xff00; // ff00::/8
    let is_doc = seg[0] == 0x2001 && seg[1] == 0x0db8; // 2001:db8::/32
    let is_discard = seg[0] == 0x0100 && seg[1] == 0 && seg[2] == 0 && seg[3] == 0; // 100::/64

    !(v6.is_loopback()
        || v6.is_unspecified()
        || is_ula
        || is_link_local
        || is_multicast
        || is_doc
        || is_discard)
}

/// Returns `true` when `ip` is a globally routable public address (unwrapping IPv4-mapped IPv6).
pub fn is_globally_routable_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_globally_routable_ipv4(v4),
        IpAddr::V6(v6) => is_globally_routable_ipv6(v6),
    }
}

/// Purely syntactic validation for Discord Webhook URLs (safe for offline config checks).
/// Enforces `https://`, no credentials/fragments/whitespace/control chars, port 443 only,
/// official Discord webhook host allowlist (no IP literals), and `/api/webhooks/<digits>/<token>`.
pub fn validate_discord_webhook_url(raw: &str) -> Result<reqwest::Url> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(AegisError::Validation(
            "Discord webhook URL must be non-empty without whitespace or control characters"
                .to_string(),
        ));
    }

    // Reject dot-segments, backslashes, or percent-encoded dot/slash obfuscation before
    // WHATWG URL normalization collapses `/foo/../` or `\`.
    let pre_query = trimmed.split(['?', '#']).next().unwrap_or(trimmed);
    let pre_query_lower = pre_query.to_ascii_lowercase();
    if pre_query.contains("..")
        || pre_query.contains('\\')
        || pre_query_lower.contains("%2e")
        || pre_query_lower.contains("%2f")
    {
        return Err(AegisError::Validation(
            "Discord webhook URL path must not contain traversal or encoded path segments"
                .to_string(),
        ));
    }

    let parsed = reqwest::Url::parse(trimmed)
        .map_err(|_| AegisError::Validation("Invalid Discord webhook URL syntax".to_string()))?;

    if parsed.scheme() != "https" {
        return Err(AegisError::Validation(
            "Discord webhook URL must use the https:// scheme".to_string(),
        ));
    }
    if !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.authority().contains('@')
    {
        return Err(AegisError::Validation(
            "Discord webhook URL must not contain userinfo credentials".to_string(),
        ));
    }
    if parsed.fragment().is_some() {
        return Err(AegisError::Validation(
            "Discord webhook URL must not contain a fragment".to_string(),
        ));
    }
    if let Some(port) = parsed.port() {
        if port != 443 {
            return Err(AegisError::Validation(
                "Discord webhook URL must use the default HTTPS port (443)".to_string(),
            ));
        }
    }

    let host = parsed.host_str().ok_or_else(|| {
        AegisError::Validation("Discord webhook URL must include a hostname".to_string())
    })?;
    if host.starts_with('[') || host.ends_with(']') || host.parse::<IpAddr>().is_ok() {
        return Err(AegisError::Validation(
            "Discord webhook URL must not use an IP literal host".to_string(),
        ));
    }
    if !ALLOWED_DISCORD_WEBHOOK_HOSTS.contains(&host) {
        return Err(AegisError::Validation(
            "Discord webhook URL host must be an official Discord webhook domain".to_string(),
        ));
    }

    let raw_path = parsed.path();
    let trimmed_path = raw_path.strip_suffix('/').unwrap_or(raw_path);
    let rel_path = trimmed_path.strip_prefix('/').unwrap_or(trimmed_path);
    let segments: Vec<&str> = rel_path.split('/').collect();
    if segments.len() != 4 {
        return Err(AegisError::Validation(
            "Discord webhook URL path must match /api/webhooks/<id>/<token>".to_string(),
        ));
    }
    let [s_api, s_webhooks, webhook_id, webhook_token] =
        [segments[0], segments[1], segments[2], segments[3]];
    if segments
        .iter()
        .any(|s| s.is_empty() || *s == "." || *s == "..")
        || s_api != "api"
        || s_webhooks != "webhooks"
        || webhook_id.len() > 32
        || !webhook_id.bytes().all(|b| b.is_ascii_digit())
        || webhook_token.len() > 256
        || !webhook_token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(AegisError::Validation(
            "Discord webhook URL path must be /api/webhooks/<digits>/<token>".to_string(),
        ));
    }

    Ok(parsed)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscordSendOutcome {
    pub delivered: bool,
    pub http_status: u16,
    pub retriable: bool,
    pub error_detail: Option<String>,
}

fn dispatch_discord_post(client: &BlockingClient, url: &str, markdown: &str) -> DiscordSendOutcome {
    let payload = json!({
        "username": "Aegis Quant v2.0",
        "content": markdown,
    });

    match client.post(url).json(&payload).send() {
        Ok(resp) if resp.status().is_success() => DiscordSendOutcome {
            delivered: true,
            http_status: resp.status().as_u16(),
            retriable: false,
            error_detail: None,
        },
        Ok(resp) => {
            let status = resp.status();
            let retriable = status.as_u16() == 429 || status.is_server_error();
            DiscordSendOutcome {
                delivered: false,
                http_status: status.as_u16(),
                retriable,
                error_detail: Some(format!("Discord HTTP {status}")),
            }
        }
        Err(e) => {
            let err_str = sanitize_telemetry_message(&e.without_url().to_string());
            DiscordSendOutcome {
                delivered: false,
                http_status: 0,
                retriable: true,
                error_detail: Some(err_str),
            }
        }
    }
}

fn run_discord_task<F>(run_blocking: F) -> DiscordSendOutcome
where
    F: FnOnce() -> DiscordSendOutcome + Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        thread::spawn(run_blocking)
            .join()
            .unwrap_or_else(|_| DiscordSendOutcome {
                delivered: false,
                http_status: 0,
                retriable: false,
                error_detail: Some("Discord dispatch worker thread panicked".to_string()),
            })
    } else {
        run_blocking()
    }
}

/// Dispatches a formatted alert message to a Discord Webhook URL (`alert_provider == "discord"` or `"both"`).
/// Automatically converts HTML Telegram tags (`<b>`, `<code>`, `<pre>`) into Discord markdown.
pub fn send_discord_webhook(
    webhook_url: &str,
    html_or_markdown_message: &str,
    timeout_sec: u64,
) -> DiscordSendOutcome {
    let url = webhook_url.trim().to_string();
    if url.is_empty() {
        return DiscordSendOutcome {
            delivered: false,
            http_status: 0,
            retriable: false,
            error_detail: Some("Discord webhook_url is empty".to_string()),
        };
    }

    let markdown = truncate_discord_markdown(
        &html_to_discord_markdown(html_or_markdown_message),
        DISCORD_MAX_MESSAGE_CHARS,
    );

    #[cfg(test)]
    if is_loopback_url(&url) {
        let run_loopback = move || -> DiscordSendOutcome {
            let client = get_discord_client(timeout_sec, true);
            dispatch_discord_post(&client, &url, &markdown)
        };
        return run_discord_task(run_loopback);
    }

    let parsed = match validate_discord_webhook_url(&url) {
        Ok(u) => u,
        Err(e) => {
            return DiscordSendOutcome {
                delivered: false,
                http_status: 0,
                retriable: false,
                error_detail: Some(sanitize_telemetry_message(&e.to_string())),
            };
        }
    };
    let host = parsed.host_str().unwrap_or_default().to_string();
    let port = parsed.port_or_known_default().unwrap_or(443);
    let target_url = parsed.to_string();

    let run_blocking = move || -> DiscordSendOutcome {
        let resolved_addrs: Vec<SocketAddr> = match (host.as_str(), port).to_socket_addrs() {
            Ok(addrs) => addrs.collect(),
            Err(e) => {
                return DiscordSendOutcome {
                    delivered: false,
                    http_status: 0,
                    retriable: true,
                    error_detail: Some(sanitize_telemetry_message(&format!(
                        "Discord webhook DNS resolution failed: {e}"
                    ))),
                };
            }
        };
        if resolved_addrs.is_empty() {
            return DiscordSendOutcome {
                delivered: false,
                http_status: 0,
                retriable: true,
                error_detail: Some(
                    "Discord webhook DNS resolution returned no addresses".to_string(),
                ),
            };
        }
        if let Some(bad_addr) = resolved_addrs
            .iter()
            .find(|addr| !is_globally_routable_ip(addr.ip()))
        {
            return DiscordSendOutcome {
                delivered: false,
                http_status: 0,
                retriable: true,
                error_detail: Some(format!(
                    "Discord webhook resolved to non-globally-routable IP {}",
                    bad_addr.ip()
                )),
            };
        }

        let client = match BlockingClient::builder()
            .timeout(Duration::from_secs(timeout_sec.max(2)))
            .redirect(reqwest::redirect::Policy::none())
            .resolve_to_addrs(&host, &resolved_addrs)
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                return DiscordSendOutcome {
                    delivered: false,
                    http_status: 0,
                    retriable: true,
                    error_detail: Some(sanitize_telemetry_message(&format!(
                        "Failed to build Discord HTTP client: {e}"
                    ))),
                };
            }
        };

        dispatch_discord_post(&client, &target_url, &markdown)
    };

    run_discord_task(run_blocking)
}

/// Async version of `send_discord_webhook`.
pub async fn send_discord_webhook_async(
    webhook_url: &str,
    html_or_markdown_message: &str,
    timeout_sec: u64,
) -> Result<DiscordSendOutcome> {
    let url = webhook_url.to_string();
    let msg = html_or_markdown_message.to_string();
    tokio::task::spawn_blocking(move || send_discord_webhook(&url, &msg, timeout_sec))
        .await
        .map_err(|e| AegisError::Alert(format!("Async Discord dispatch join error: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_discord_webhook_url_accepts_official_domains() {
        let valid_urls = [
            "https://discord.com/api/webhooks/123456789012345678/AbCdEf_12345-xyz",
            "https://discordapp.com/api/webhooks/987654321/token_ABC-123/",
            "https://ptb.discord.com/api/webhooks/42/secretToken_99",
            "https://canary.discord.com/api/webhooks/111222333/token-xyz",
            "  https://discord.com/api/webhooks/12345/abcDEF  ",
            "https://discord.com:443/api/webhooks/12345/abcDEF",
        ];
        for raw in valid_urls {
            let parsed = validate_discord_webhook_url(raw)
                .unwrap_or_else(|e| panic!("expected valid Discord webhook URL '{raw}': {e}"));
            assert_eq!(parsed.scheme(), "https");
        }
    }

    #[test]
    fn test_validate_discord_webhook_url_rejects_ssrf_and_malformed_inputs() {
        let invalid_urls = [
            "",
            "   ",
            "http://discord.com/api/webhooks/123/abc",
            "https://127.0.0.1/api/webhooks/123/abc",
            "https://169.254.169.254/api/webhooks/123/abc",
            "https://[::1]/api/webhooks/123/abc",
            "https://discord.com@evil.com/api/webhooks/123/abc",
            "https://user:pass@discord.com/api/webhooks/123/abc",
            "https://discord.com:8443/api/webhooks/123/abc",
            "https://discord.com:80/api/webhooks/123/abc",
            "https://evil.com/api/webhooks/123/abc",
            "https://evil.discord.com/api/webhooks/123/abc",
            "https://discord.com.evil.com/api/webhooks/123/abc",
            "https://discord.com/api/webhooks/123/abc#frag",
            "https://discord.com/api/webhooks/123/abc#",
            "https://discord.com/api/webhooks/123/abc def",
            "https://discord.com/api/webhooks/123/ab\tc",
            "https://discord.com/api/webhooks/123/ab\x00c",
            "https://discord.com/api/webhooks/123/abc/../xyz",
            "https://discord.com/api/webhooks/123/..",
            "https://discord.com/api/webhooks/123/%2e%2e/abc",
            "https://discord.com/api/webhooks/123/abc/extra",
            "https://discord.com/api/webhooks/123/abc//",
            "https://discord.com/api/webhooks/123",
            "https://discord.com/api/webhooks/notdigits/abc",
            "https://discord.com/api/v10/webhooks/123/abc",
            "https://discord.com/api/webhooks/123/bad$token",
        ];
        for raw in invalid_urls {
            assert!(
                validate_discord_webhook_url(raw).is_err(),
                "expected invalid Discord webhook URL to be rejected: {raw:?}"
            );
        }
    }

    #[test]
    fn test_is_globally_routable_ip_filters_non_global_and_allows_public() {
        let non_global = [
            "0.0.0.0",
            "0.10.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "172.31.255.254",
            "192.168.1.1",
            "127.0.0.1",
            "127.1.2.3",
            "169.254.169.254",
            "100.64.0.1",
            "100.127.255.254",
            "192.0.2.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "ff02::1",
            "2001:db8::1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::ffff:169.254.169.254",
            "::ffff:100.64.0.1",
            "::ffff:192.168.1.10",
            "::127.0.0.1",
        ];
        for s in non_global {
            let ip: IpAddr = s.parse().unwrap();
            assert!(
                !is_globally_routable_ip(ip),
                "expected non-globally-routable IP to be rejected: {s}"
            );
        }

        let public_ips = [
            "162.159.135.232",
            "1.1.1.1",
            "8.8.8.8",
            "100.63.255.254",
            "100.128.0.1",
            "172.15.255.254",
            "172.32.0.1",
            "2606:4700:4700::1111",
            "::ffff:162.159.135.232",
        ];
        for s in public_ips {
            let ip: IpAddr = s.parse().unwrap();
            assert!(
                is_globally_routable_ip(ip),
                "expected public IP to be accepted: {s}"
            );
        }
    }

    #[test]
    fn test_send_discord_webhook_rejects_invalid_url_non_retriable() {
        let outcome = send_discord_webhook("https://evil.example/api/webhooks/1/abc", "hi", 5);
        assert!(!outcome.delivered);
        assert!(!outcome.retriable);
        assert_eq!(outcome.http_status, 0);
    }
}
