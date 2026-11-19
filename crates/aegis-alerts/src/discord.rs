//! Config-switchable Discord Webhook dispatcher for Aegis Quant v2.0 (`aegis-alerts::discord`).

use crate::formatter::{
    html_to_discord_markdown, truncate_discord_markdown, DISCORD_MAX_MESSAGE_CHARS,
};
use crate::telegram::is_loopback_url;
use aegis_core::{sanitize_telemetry_message, AegisError, Result};
use parking_lot::Mutex;
use reqwest::blocking::Client as BlockingClient;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

static DISCORD_CLIENTS: OnceLock<Mutex<HashMap<(u64, bool), BlockingClient>>> = OnceLock::new();

fn get_discord_client(timeout_sec: u64, is_loopback: bool) -> BlockingClient {
    let key = (timeout_sec.max(2), is_loopback);
    let cache = DISCORD_CLIENTS.get_or_init(|| Mutex::new(HashMap::new()));
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscordSendOutcome {
    pub delivered: bool,
    pub http_status: u16,
    pub retriable: bool,
    pub error_detail: Option<String>,
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
    let is_loopback = is_loopback_url(&url);

    let run_blocking = move || -> DiscordSendOutcome {
        let client = get_discord_client(timeout_sec, is_loopback);
        let payload = json!({
            "username": "Aegis Quant v2.0",
            "content": markdown,
        });

        match client.post(&url).json(&payload).send() {
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
    };

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
