//! Telegram Bot API (`parse_mode = "HTML"`) client for Aegis Quant v2.0 (`aegis-alerts::telegram`).

use crate::formatter::{
    html_to_discord_markdown, truncate_discord_markdown, truncate_telegram_html,
    TELEGRAM_MAX_MESSAGE_CHARS,
};
use aegis_core::{mask_identifier, sanitize_telemetry_message, AegisError, Result};
use parking_lot::Mutex;
use reqwest::blocking::Client as BlockingClient;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

static TG_CLIENTS: OnceLock<Mutex<HashMap<(u64, bool), BlockingClient>>> = OnceLock::new();

fn get_telegram_client(timeout_sec: u64, is_loopback: bool) -> BlockingClient {
    let key = (timeout_sec.max(2), is_loopback);
    let cache = TG_CLIENTS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(client) = cache.lock().get(&key) {
        return client.clone();
    }
    let mut builder = BlockingClient::builder().timeout(Duration::from_secs(key.0));
    if is_loopback {
        builder = builder.no_proxy();
    }
    let client = builder.build().unwrap_or_else(|_| BlockingClient::new());
    cache.lock().insert(key, client.clone());
    client
}

pub(crate) fn is_loopback_url(raw_url: &str) -> bool {
    let trimmed = raw_url.trim().to_ascii_lowercase();
    let rest = trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("https://"))
        .unwrap_or(&trimmed);
    let host_port = rest.split('/').next().unwrap_or("");
    let host = if let Some(bracketed) = host_port.strip_prefix('[') {
        bracketed.split(']').next().unwrap_or("")
    } else {
        host_port.split(':').next().unwrap_or("")
    };
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramSendOutcome {
    pub delivered: bool,
    pub http_status: u16,
    pub masked_chat_id: String,
    pub retriable: bool,
    pub error_detail: Option<String>,
}

/// Dispatches an HTML-formatted message (`parse_mode: "HTML"`, `disable_web_page_preview: true`)
/// via the Telegram Bot API (`/bot<token>/sendMessage`).
/// Safe to invoke from both synchronous threads and active Tokio async runtime tasks.
pub fn send_telegram_html(
    api_base_url: &str,
    bot_token: &str,
    chat_id: &str,
    html_message: &str,
    timeout_sec: u64,
) -> TelegramSendOutcome {
    let base = api_base_url.trim().trim_end_matches('/').to_string();
    let token = bot_token.trim().to_string();
    let cid = chat_id.trim().to_string();
    let html = truncate_telegram_html(html_message, TELEGRAM_MAX_MESSAGE_CHARS);
    let masked_cid = mask_identifier(&cid);

    if token.is_empty() {
        return TelegramSendOutcome {
            delivered: false,
            http_status: 0,
            masked_chat_id: masked_cid,
            retriable: false,
            error_detail: Some("Telegram bot_token is empty".to_string()),
        };
    }

    if cid.is_empty() {
        return TelegramSendOutcome {
            delivered: false,
            http_status: 0,
            masked_chat_id: masked_cid,
            retriable: false,
            error_detail: Some(
                "Telegram chat_id is empty. Send '/start' to your bot and retry.".to_string(),
            ),
        };
    }

    let is_loopback = is_loopback_url(&base);
    let bot_id_prefix = token.split(':').next().unwrap_or("").trim();
    if !bot_id_prefix.is_empty() && cid == bot_id_prefix && !is_loopback {
        return TelegramSendOutcome {
            delivered: false,
            http_status: 400,
            masked_chat_id: masked_cid.clone(),
            retriable: false,
            error_detail: Some(format!(
                "Configured chat_id '{masked_cid}' matches the bot's own ID prefix before ':' instead of a recipient chat_id."
            )),
        };
    }

    // Run blocking reqwest inside a dedicated OS thread if called from an active Tokio runtime
    // so reqwest::blocking never panics on nested runtime drop.
    let run_blocking = move || -> TelegramSendOutcome {
        let client = get_telegram_client(timeout_sec, is_loopback);
        let url = format!("{base}/bot{token}/sendMessage");
        let payload = json!({
            "chat_id": cid,
            "text": html,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        });

        match client.post(&url).json(&payload).send() {
            Ok(resp) if resp.status().is_success() => {
                let status_code = resp.status().as_u16();
                let body: Value = resp.json().unwrap_or_else(|_| json!({}));
                let ok = body.get("ok").and_then(Value::as_bool).unwrap_or(true);
                TelegramSendOutcome {
                    delivered: ok,
                    http_status: status_code,
                    masked_chat_id: masked_cid,
                    retriable: false,
                    error_detail: if ok {
                        None
                    } else {
                        Some("Telegram response returned ok=false".to_string())
                    },
                }
            }
            Ok(resp) => {
                let status = resp.status();
                let body: Value = resp.json().unwrap_or_else(|_| json!({}));
                let desc = body
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("");

                // Fallback to plain text if HTML entity parsing failed
                if status.as_u16() == 400 && desc.to_ascii_lowercase().contains("parse entities") {
                    let plain_text = truncate_discord_markdown(
                        &html_to_discord_markdown(&html),
                        TELEGRAM_MAX_MESSAGE_CHARS,
                    );
                    let plain_payload = json!({
                        "chat_id": cid,
                        "text": plain_text,
                        "disable_web_page_preview": true,
                    });
                    if let Ok(fb_resp) = client.post(&url).json(&plain_payload).send() {
                        if fb_resp.status().is_success() {
                            return TelegramSendOutcome {
                                delivered: true,
                                http_status: fb_resp.status().as_u16(),
                                masked_chat_id: masked_cid,
                                retriable: false,
                                error_detail: None,
                            };
                        }
                    }
                }

                let retriable = status.as_u16() == 429 || status.is_server_error();
                let raw_err = if desc.is_empty() {
                    format!("Telegram HTTP {status}")
                } else {
                    format!("Telegram HTTP {status}: {desc}")
                };
                TelegramSendOutcome {
                    delivered: false,
                    http_status: status.as_u16(),
                    masked_chat_id: masked_cid,
                    retriable,
                    error_detail: Some(sanitize_telemetry_message(&raw_err)),
                }
            }
            Err(e) => {
                let err_str = sanitize_telemetry_message(&e.without_url().to_string());
                TelegramSendOutcome {
                    delivered: false,
                    http_status: 0,
                    masked_chat_id: masked_cid,
                    retriable: true,
                    error_detail: Some(err_str),
                }
            }
        }
    };

    if tokio::runtime::Handle::try_current().is_ok() {
        thread::spawn(run_blocking)
            .join()
            .unwrap_or_else(|_| TelegramSendOutcome {
                delivered: false,
                http_status: 0,
                masked_chat_id: mask_identifier(chat_id),
                retriable: false,
                error_detail: Some("Telegram dispatch worker thread panicked".to_string()),
            })
    } else {
        run_blocking()
    }
}

/// Async version of `send_telegram_html` for non-blocking Tokio callers.
pub async fn send_telegram_html_async(
    api_base_url: &str,
    bot_token: &str,
    chat_id: &str,
    html_message: &str,
    timeout_sec: u64,
) -> Result<TelegramSendOutcome> {
    let base = api_base_url.to_string();
    let token = bot_token.to_string();
    let cid = chat_id.to_string();
    let html = html_message.to_string();
    tokio::task::spawn_blocking(move || send_telegram_html(&base, &token, &cid, &html, timeout_sec))
        .await
        .map_err(|e| AegisError::Alert(format!("Async Telegram dispatch join error: {e}")))
}

/// Queries Telegram `/bot<token>/getUpdates` to auto-discover the most recent user or group `chat.id`
/// ONLY when `configured_chat_id` is empty. Refuses to overwrite an already configured non-empty `telegram_chat_id`.
pub fn discover_telegram_chat_id_guarded(
    api_base_url: &str,
    bot_token: &str,
    configured_chat_id: &str,
    timeout_sec: u64,
) -> Option<String> {
    if !configured_chat_id.trim().is_empty() {
        return None;
    }
    discover_telegram_chat_id_from_updates(api_base_url, bot_token, timeout_sec)
}

/// Queries Telegram `/bot<token>/getUpdates` to auto-discover the most recent user or group `chat.id`
/// when `telegram_chat_id` is unconfigured.
pub fn discover_telegram_chat_id_from_updates(
    api_base_url: &str,
    bot_token: &str,
    timeout_sec: u64,
) -> Option<String> {
    let base = api_base_url.trim().trim_end_matches('/').to_string();
    let token = bot_token.trim().to_string();
    if token.is_empty() {
        return None;
    }
    let is_loopback = is_loopback_url(&base);
    let bot_id_prefix = token.split(':').next().unwrap_or("").trim().to_string();

    let run_discovery = move || -> Option<String> {
        let client = get_telegram_client(timeout_sec, is_loopback);
        let url = format!("{base}/bot{token}/getUpdates");
        let resp = client.get(&url).send().ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let body: Value = resp.json().ok()?;
        let updates = body.get("result")?.as_array()?;
        for upd in updates.iter().rev() {
            let chat_id_val = upd
                .pointer("/message/chat/id")
                .or_else(|| upd.pointer("/edited_message/chat/id"))
                .or_else(|| upd.pointer("/channel_post/chat/id"))
                .or_else(|| upd.pointer("/my_chat_member/chat/id"))
                .or_else(|| upd.pointer("/callback_query/message/chat/id"));
            if let Some(id_val) = chat_id_val {
                let cid = if let Some(n) = id_val.as_i64() {
                    n.to_string()
                } else if let Some(s) = id_val.as_str() {
                    s.trim().to_string()
                } else {
                    continue;
                };
                if !cid.is_empty() && cid != bot_id_prefix {
                    return Some(cid);
                }
            }
        }
        None
    };

    if tokio::runtime::Handle::try_current().is_ok() {
        thread::spawn(run_discovery).join().ok().flatten()
    } else {
        run_discovery()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    #[test]
    fn test_send_telegram_html_loopback_verifies_html_parse_mode() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req_str = String::from_utf8_lossy(&buf[..n]).to_string();
            let resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 21\r\nConnection: close\r\n\r\n{\"ok\":true,\"result\":{}}";
            let _ = stream.write_all(resp.as_bytes());
            req_str
        });

        let base_url = format!("http://{addr}");
        let html = "🟢 <b>AEGIS v2.0 // ORDER FILLED (OPEN)</b>\n<pre>Fill Price : $184.2500</pre>";
        let outcome = send_telegram_html(&base_url, "123456:ABCDEF", "987654321", html, 5);
        assert!(outcome.delivered);
        assert_eq!(outcome.http_status, 200);

        let captured = handle.join().unwrap();
        assert!(captured.contains("POST /bot123456:ABCDEF/sendMessage"));
        assert!(captured.contains("\"parse_mode\":\"HTML\""));
        assert!(captured.contains("\"disable_web_page_preview\":true"));
    }
}
