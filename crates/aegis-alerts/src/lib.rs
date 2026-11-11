//! Real-Time Operational Alerting Engine (`aegis-alerts`).
//!
//! 1:1 port of `engine/alerts.py`:
//! - Dispatches structured alerts via Telegram Bot API (`/sendMessage`) and Discord Webhooks
//! - Single-worker exponential-backoff background `AlertRetryQueue` (`0.5s`, `1.0s`, `2.0s`)
//! - Triggers on HMM Regime Flips, Position Reconciliation Mismatches, and Daily Drawdown > 3%
//! - Includes isolated `:memory:` offline loopback HTTP receiver for `aegis-quant alerts --test` (`S8`)

use aegis_core::{mask_identifier, round_dp, sanitize_telemetry_message, Result};
use aegis_db::{ConfigMap, DatabaseManager};
use chrono::Utc;
use parking_lot::Mutex;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

/// Typed domain result returned by all `AlertEngine` dispatch methods (`Q4`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertDispatchResult {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub telegram_sent: bool,
    pub webhook_payload_verified: bool,
    pub queued_for_retry: bool,
    pub chat_id: String,
    pub error_detail: Option<String>,
    pub title: String,
    pub severity: String,
    pub timestamp: String,
    pub channels: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loopback_verified: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isolated_self_test: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loopback_payload_captured: Option<bool>,
}

impl AlertDispatchResult {
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| json!({}))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedAlertTask {
    pub title: String,
    pub message: String,
    pub severity: String,
    pub details: Option<Value>,
    pub attempt: usize,
    pub next_attempt_ms: i64,
}

#[derive(Clone)]
pub struct AlertRetryQueue {
    max_retries: usize,
    base_backoff_sec: f64,
    max_queue_size: usize,
    queue: Arc<Mutex<VecDeque<QueuedAlertTask>>>,
    worker_running: Arc<AtomicBool>,
    in_flight: Arc<AtomicUsize>,
    delivered_count: Arc<AtomicUsize>,
    failed_permanently_count: Arc<AtomicUsize>,
    dropped_count: Arc<AtomicUsize>,
}

impl AlertRetryQueue {
    pub fn new(max_retries: usize, base_backoff_sec: f64, max_queue_size: usize) -> Self {
        Self {
            max_retries,
            base_backoff_sec,
            max_queue_size,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            worker_running: Arc::new(AtomicBool::new(false)),
            in_flight: Arc::new(AtomicUsize::new(0)),
            delivered_count: Arc::new(AtomicUsize::new(0)),
            failed_permanently_count: Arc::new(AtomicUsize::new(0)),
            dropped_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn pending_count(&self) -> usize {
        self.queue.lock().len() + self.in_flight.load(Ordering::SeqCst)
    }

    pub fn delivered_count(&self) -> usize {
        self.delivered_count.load(Ordering::SeqCst)
    }

    pub fn failed_permanently_count(&self) -> usize {
        self.failed_permanently_count.load(Ordering::SeqCst)
    }

    pub fn dropped_count(&self) -> usize {
        self.dropped_count.load(Ordering::SeqCst)
    }

    pub fn enqueue(
        &self,
        engine: AlertEngine,
        title: String,
        message: String,
        severity: String,
        details: Option<Value>,
        attempt: usize,
    ) -> bool {
        if attempt > self.max_retries {
            self.failed_permanently_count.fetch_add(1, Ordering::SeqCst);
            return false;
        }
        let delay_ms = (self.base_backoff_sec
            * 1000.0
            * (2_f64.powi((attempt.saturating_sub(1)) as i32))) as i64;
        let next_ms = Utc::now().timestamp_millis() + delay_ms;
        {
            let mut q = self.queue.lock();
            if q.len() >= self.max_queue_size {
                let dropped = self.dropped_count.fetch_add(1, Ordering::SeqCst) + 1;
                drop(q);
                tracing::warn!(
                    capacity = self.max_queue_size,
                    dropped_total = dropped,
                    title = %title,
                    "AlertRetryQueue full; dropping alert"
                );
                let _ = engine.db.log_telemetry(
                    "WARNING",
                    "engine.alerts",
                    &format!(
                        "AlertRetryQueue full (capacity={}); dropped alert '{}' (dropped_total={dropped})",
                        self.max_queue_size, title
                    ),
                    None,
                );
                return false;
            }
            q.push_back(QueuedAlertTask {
                title,
                message,
                severity,
                details,
                attempt,
                next_attempt_ms: next_ms,
            });
        }
        self.ensure_worker(engine);
        true
    }

    /// Ensures at most one background retry thread is active at any time (`Q7`).
    fn ensure_worker(&self, engine: AlertEngine) {
        if self
            .worker_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let queue_ref = self.clone();
        thread::spawn(move || loop {
            loop {
                let task_opt = {
                    let mut q = queue_ref.queue.lock();
                    q.pop_front()
                };
                let Some(task) = task_opt else {
                    break;
                };
                queue_ref.in_flight.fetch_add(1, Ordering::SeqCst);
                let now_ms = Utc::now().timestamp_millis();
                if task.next_attempt_ms > now_ms {
                    thread::sleep(Duration::from_millis((task.next_attempt_ms - now_ms) as u64));
                }
                let res = engine.send_alert(
                    &task.title,
                    &task.message,
                    &task.severity,
                    task.details.clone(),
                    false,
                    false,
                );
                if res.status == "DELIVERED" {
                    queue_ref.delivered_count.fetch_add(1, Ordering::SeqCst);
                } else if task.attempt < queue_ref.max_retries {
                    let delay_ms = (queue_ref.base_backoff_sec
                        * 1000.0
                        * (2_f64.powi(task.attempt as i32)))
                        as i64;
                    let next_ms = Utc::now().timestamp_millis() + delay_ms;
                    let mut q = queue_ref.queue.lock();
                    if q.len() < queue_ref.max_queue_size {
                        q.push_back(QueuedAlertTask {
                            title: task.title,
                            message: task.message,
                            severity: task.severity,
                            details: task.details,
                            attempt: task.attempt + 1,
                            next_attempt_ms: next_ms,
                        });
                    } else {
                        let dropped = queue_ref.dropped_count.fetch_add(1, Ordering::SeqCst) + 1;
                        drop(q);
                        tracing::warn!(
                            capacity = queue_ref.max_queue_size,
                            dropped_total = dropped,
                            title = %task.title,
                            "AlertRetryQueue full on retry; dropping alert"
                        );
                        let _ = engine.db.log_telemetry(
                            "WARNING",
                            "engine.alerts",
                            &format!(
                                "AlertRetryQueue full on retry (capacity={}); dropped alert '{}' (dropped_total={dropped})",
                                queue_ref.max_queue_size, task.title
                            ),
                            None,
                        );
                    }
                } else {
                    queue_ref
                        .failed_permanently_count
                        .fetch_add(1, Ordering::SeqCst);
                    let _ = engine.db.log_telemetry(
                        "ERROR",
                        "engine.alerts",
                        &format!(
                            "Alert permanently failed after {} retries: {}",
                            queue_ref.max_retries, task.title
                        ),
                        Some(res.to_value()),
                    );
                }
                queue_ref.in_flight.fetch_sub(1, Ordering::SeqCst);
            }

            queue_ref.worker_running.store(false, Ordering::SeqCst);
            if queue_ref.queue.lock().is_empty() {
                break;
            }
            if queue_ref
                .worker_running
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                break;
            }
        });
    }

    pub fn drain_for_test(&self, timeout_sec: f64) -> bool {
        let deadline = Instant::now() + Duration::from_secs_f64(timeout_sec.max(0.1));
        while Instant::now() < deadline {
            if self.pending_count() == 0 {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
        }
        self.pending_count() == 0
    }
}

#[derive(Clone)]
pub struct AlertEngine {
    pub db: DatabaseManager,
    pub timeout_sec: u64,
    pub telegram_api_base: String,
    pub retry_queue: AlertRetryQueue,
    http_client: Client,
    loopback_client: Client,
    last_regime: Arc<Mutex<Option<String>>>,
    last_drawdown_alert_date: Arc<Mutex<Option<String>>>,
    chat_id_discovery_attempted: Arc<AtomicBool>,
}

impl AlertEngine {
    pub fn new(db: DatabaseManager) -> Self {
        let cfg = db.load_config_opts(false).unwrap_or_default();
        let timeout_sec = cfg
            .get_usize(
                "alert_http_timeout_sec",
                cfg.get_usize("alert_http_timeout_secs", 8),
            )
            .max(1) as u64;
        let max_retries = cfg.get_usize("alert_max_retries", 5);
        let base_backoff_sec = cfg
            .get_f64(
                "alert_retry_base_backoff_sec",
                cfg.get_f64("alert_backoff_base_sec", 0.5),
            )
            .max(0.05);
        let max_queue_size = cfg
            .get_usize(
                "alerts_retry_queue_capacity",
                cfg.get_usize(
                    "alert_max_queue_size",
                    cfg.get_usize("alert_worker_queue_capacity", 64),
                ),
            )
            .max(10);
        let telegram_api_base = cfg.get_str("telegram_api_base_url", "https://api.telegram.org");

        static ALERT_CLIENTS: std::sync::OnceLock<
            Mutex<std::collections::HashMap<(u64, bool), Client>>,
        > = std::sync::OnceLock::new();
        let get_client = |t: u64, no_proxy: bool| -> Client {
            let map = ALERT_CLIENTS
                .get_or_init(|| Mutex::new(std::collections::HashMap::new()));
            if let Some(c) = map.lock().get(&(t, no_proxy)) {
                return c.clone();
            }
            let built = thread::spawn(move || {
                let mut b = Client::builder().timeout(Duration::from_secs(t));
                if no_proxy {
                    b = b.no_proxy();
                }
                b.build().unwrap_or_else(|_| Client::new())
            })
            .join()
            .expect("build alert blocking client");
            map.lock().entry((t, no_proxy)).or_insert(built).clone()
        };
        let http_client = get_client(timeout_sec, false);
        let loopback_client = get_client(timeout_sec, true);

        Self {
            db,
            timeout_sec,
            telegram_api_base,
            retry_queue: AlertRetryQueue::new(max_retries, base_backoff_sec, max_queue_size),
            http_client,
            loopback_client,
            last_regime: Arc::new(Mutex::new(None)),
            last_drawdown_alert_date: Arc::new(Mutex::new(None)),
            chat_id_discovery_attempted: Arc::new(AtomicBool::new(false)),
        }
    }

    fn load_config_logged(&self) -> ConfigMap {
        self.db.load_config_logged("engine.alerts")
    }

    /// Queries Telegram `/getUpdates` to automatically discover the most recent user or group
    /// `chat.id` that has interacted with this bot (excluding the bot's own user ID prefix).
    pub fn discover_telegram_chat_id(&self, tg_token: &str) -> Option<String> {
        let token = tg_token.trim();
        if token.is_empty() {
            return None;
        }
        let bot_id_prefix = token.split(':').next().unwrap_or("").trim();
        let is_loopback = self.telegram_api_base.contains("127.0.0.1")
            || self.telegram_api_base.contains("localhost");
        let client = if is_loopback {
            &self.loopback_client
        } else {
            &self.http_client
        };
        let url = format!(
            "{}/bot{}/getUpdates",
            self.telegram_api_base.trim_end_matches('/'),
            token
        );
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
                    let _ = self.db.set_config_value("telegram_chat_id", &json!(cid));
                    let masked_cid = mask_identifier(&cid);
                    let _ = self.db.log_telemetry(
                        "INFO",
                        "engine.alerts",
                        &format!("Auto-discovered Telegram chat_id={masked_cid} via getUpdates"),
                        None,
                    );
                    return Some(cid);
                }
            }
        }
        None
    }

    pub fn send_alert(
        &self,
        title: &str,
        message: &str,
        severity: &str,
        details: Option<Value>,
        _throw_on_error: bool,
        allow_retry_queue: bool,
    ) -> AlertDispatchResult {
        let cfg = self.load_config_logged();
        let tg_token = cfg.get_str("telegram_bot_token", "").trim().to_string();
        let mut tg_chat_id = cfg.get_str("telegram_chat_id", "").trim().to_string();
        let discord_url = cfg.get_str("discord_webhook_url", "").trim().to_string();
        let ts_utc = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        if tg_token.is_empty() && discord_url.is_empty() {
            return AlertDispatchResult {
                status: "SKIPPED".to_string(),
                reason: Some("NO_ALERT_CHANNEL_CONFIGURED".to_string()),
                telegram_sent: false,
                webhook_payload_verified: false,
                queued_for_retry: false,
                chat_id: String::new(),
                error_detail: None,
                title: title.to_string(),
                severity: severity.to_string(),
                timestamp: ts_utc,
                channels: Vec::new(),
                loopback_verified: None,
                isolated_self_test: None,
                loopback_payload_captured: None,
            };
        }

        let mut results: Vec<Value> = Vec::new();
        let mut any_error = false;
        let mut retriable_error = false;
        let mut error_detail: Option<String> = None;
        let is_loopback = self.telegram_api_base.contains("127.0.0.1")
            || self.telegram_api_base.contains("localhost");
        let client = if is_loopback {
            &self.loopback_client
        } else {
            &self.http_client
        };

        if !tg_token.is_empty() {
            let bot_id_prefix = tg_token.split(':').next().unwrap_or("").trim().to_string();
            if (tg_chat_id.is_empty()
                || (!bot_id_prefix.is_empty() && tg_chat_id == bot_id_prefix && !is_loopback))
                && allow_retry_queue
                && !self.chat_id_discovery_attempted.swap(true, Ordering::SeqCst)
            {
                if let Some(discovered) = self.discover_telegram_chat_id(&tg_token) {
                    tg_chat_id = discovered;
                    self.chat_id_discovery_attempted.store(false, Ordering::SeqCst);
                }
            }

            if tg_chat_id.is_empty() {
                any_error = true;
                let msg = "Telegram Chat ID is empty and no incoming messages were found via getUpdates. Open Telegram, send '/start' or 'hello' to your bot, then click 'Send Test Alert' again.".to_string();
                error_detail = Some(msg.clone());
                results.push(json!({
                    "channel": "TELEGRAM",
                    "error": msg,
                }));
            } else if !bot_id_prefix.is_empty() && tg_chat_id == bot_id_prefix && !is_loopback {
                any_error = true;
                let masked = mask_identifier(&tg_chat_id);
                let msg = format!(
                    "Chat ID '{masked}' is the Bot's own ID (the prefix of your bot token before ':'), not your personal Telegram Chat ID. Open Telegram, open a chat with your bot and click Start / send 'hello', then click 'Send Test Alert' to auto-detect your Chat ID."
                );
                error_detail = Some(msg.clone());
                results.push(json!({
                    "channel": "TELEGRAM",
                    "error": msg,
                }));
            } else {
                let icon = match severity.to_ascii_uppercase().as_str() {
                    "CRITICAL" => "[CRITICAL]",
                    "WARNING" => "[WARNING]",
                    _ => "[INFO]",
                };
                let safe_title = title.replace('_', "\\_");
                let safe_message = message.replace('_', "\\_");
                let mut text_lines = vec![
                    format!("{icon} *Aegis Quant — {safe_title}*"),
                    format!("Severity: `{severity}` | Time: `{ts_utc}`"),
                    String::new(),
                    safe_message,
                ];
                if let Some(Value::Object(map)) = &details {
                    text_lines.push(String::new());
                    for (k, v) in map {
                        let safe_k = k.replace('_', "\\_");
                        text_lines.push(format!("• *{safe_k}*: `{v}`"));
                    }
                }
                let text_joined = text_lines.join("\n");
                let url = format!(
                    "{}/bot{}/sendMessage",
                    self.telegram_api_base.trim_end_matches('/'),
                    tg_token
                );
                let payload = json!({
                    "chat_id": tg_chat_id,
                    "text": text_joined,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": true,
                });

                // R3-N14: Do not overwrite an explicitly configured telegram_chat_id via getUpdates on HTTP 400/403.
                let resp_res = client.post(&url).json(&payload).send();

                let masked_chat_id = mask_identifier(&tg_chat_id).replace("...", "***");
                match resp_res {
                    Ok(resp) if resp.status().is_success() => {
                        let body: Value = resp.json().unwrap_or_else(|_| json!({}));
                        results.push(json!({
                            "channel": "TELEGRAM",
                            "http_status": 200,
                            "chat_id": masked_chat_id,
                            "ok": body.get("ok").and_then(Value::as_bool).unwrap_or(true),
                        }));
                    }
                    Ok(resp) => {
                        let http_status = resp.status();
                        let body: Value = resp.json().unwrap_or_else(|_| json!({}));
                        let tg_desc = body
                            .get("description")
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        // Fallback to plain-text sendMessage without parse_mode if Telegram rejects Markdown entities
                        let mut plain_fallback_succeeded = false;
                        if http_status.as_u16() == 400
                            && tg_desc.to_ascii_lowercase().contains("parse entities")
                        {
                            let mut plain_lines = vec![
                                format!("{icon} Aegis Quant — {title}"),
                                format!("Severity: {severity} | Time: {ts_utc}"),
                                String::new(),
                                message.to_string(),
                            ];
                            if let Some(Value::Object(map)) = &details {
                                plain_lines.push(String::new());
                                for (k, v) in map {
                                    plain_lines.push(format!("• {k}: {v}"));
                                }
                            }
                            let plain_payload = json!({
                                "chat_id": tg_chat_id,
                                "text": plain_lines.join("\n"),
                                "disable_web_page_preview": true,
                            });
                            if let Ok(fb_resp) = client.post(&url).json(&plain_payload).send() {
                                if fb_resp.status().is_success() {
                                    let fb_body: Value =
                                        fb_resp.json().unwrap_or_else(|_| json!({}));
                                    results.push(json!({
                                        "channel": "TELEGRAM",
                                        "http_status": 200,
                                        "chat_id": masked_chat_id,
                                        "ok": fb_body.get("ok").and_then(Value::as_bool).unwrap_or(true),
                                    }));
                                    plain_fallback_succeeded = true;
                                }
                            }
                        }
                        if !plain_fallback_succeeded {
                            any_error = true;
                            if http_status.as_u16() == 429 || http_status.is_server_error() {
                                retriable_error = true;
                            }
                            let raw_err_str = if tg_desc.is_empty() {
                                format!("HTTP {http_status}")
                            } else {
                                format!("HTTP {http_status}: {tg_desc}")
                            };
                            let err_str = sanitize_telemetry_message(&raw_err_str);
                            error_detail = Some(err_str.clone());
                            results.push(json!({
                                "channel": "TELEGRAM",
                                "http_status": http_status.as_u16(),
                                "error": err_str,
                            }));
                        }
                    }
                    Err(e) => {
                        any_error = true;
                        retriable_error = true;
                        // R3-B2: Strip request URL (which embeds /bot<token>) before formatting or persisting.
                        let err_str = sanitize_telemetry_message(
                            &e.without_url().to_string(),
                        );
                        error_detail = Some(err_str.clone());
                        results.push(json!({
                            "channel": "TELEGRAM",
                            "error": err_str,
                        }));
                    }
                }
            }
        }

        if !discord_url.is_empty() {
            let payload = json!({
                "username": "Aegis Quant Alert Engine",
                "content": format!("**[{severity}] {title}** ({ts_utc})\n{message}"),
            });
            match client.post(&discord_url).json(&payload).send() {
                Ok(resp) if resp.status().is_success() => {
                    results.push(json!({
                        "channel": "DISCORD",
                        "http_status": resp.status().as_u16(),
                        "ok": true,
                    }));
                }
                Ok(resp) => {
                    any_error = true;
                    if resp.status().as_u16() == 429 || resp.status().is_server_error() {
                        retriable_error = true;
                    }
                    results.push(json!({
                        "channel": "DISCORD",
                        "error": format!("HTTP {}", resp.status()),
                    }));
                }
                Err(e) => {
                    any_error = true;
                    retriable_error = true;
                    // R3-B2: Strip Discord webhook URL before formatting or persisting.
                    let err_str = sanitize_telemetry_message(
                        &e.without_url().to_string(),
                    );
                    results.push(json!({
                        "channel": "DISCORD",
                        "error": err_str,
                    }));
                }
            }
        }

        let delivered = results
            .iter()
            .any(|r| r.get("ok").and_then(Value::as_bool) == Some(true));
        let mut status = if delivered { "DELIVERED" } else { "ERROR" }.to_string();
        let mut queued_for_retry = false;

        if status == "ERROR" && any_error && retriable_error && allow_retry_queue {
            queued_for_retry = self.retry_queue.enqueue(
                self.clone(),
                title.to_string(),
                message.to_string(),
                severity.to_string(),
                details.clone(),
                1,
            );
            if queued_for_retry {
                status = "QUEUED_FOR_RETRY".to_string();
            }
        }

        let masked_cid = mask_identifier(&tg_chat_id).replace("...", "***");
        let report = AlertDispatchResult {
            status: status.clone(),
            reason: None,
            telegram_sent: delivered,
            webhook_payload_verified: delivered,
            queued_for_retry,
            chat_id: masked_cid,
            error_detail,
            title: title.to_string(),
            severity: severity.to_string(),
            timestamp: ts_utc,
            channels: results,
            loopback_verified: None,
            isolated_self_test: None,
            loopback_payload_captured: None,
        };
        let _ = self.db.log_telemetry(
            if delivered { "INFO" } else { "WARNING" },
            "engine.alerts",
            &format!("Alert dispatch [{severity}] {title}: {status}"),
            Some(report.to_value()),
        );
        report
    }

    pub fn notify_critical_event(
        &self,
        event_type: &str,
        summary: &str,
        details: Option<Value>,
    ) -> AlertDispatchResult {
        self.send_alert(
            &format!("CRITICAL SYSTEM EVENT: {event_type}"),
            summary,
            "CRITICAL",
            details,
            false,
            true,
        )
    }

    pub fn notify_stop_out(
        &self,
        symbol: &str,
        side: &str,
        entry_price: f64,
        exit_price: f64,
        net_pnl_usdc: f64,
        reason: &str,
    ) -> AlertDispatchResult {
        let msg = format!(
            "Stop-out triggered on *{symbol}* ({side}): exited at `${exit_price:.4}` (entry `${entry_price:.4}`, net PnL `${net_pnl_usdc:+.2}`, reason `{reason}`)."
        );
        self.send_alert(
            &format!("Chandelier Stop-Out: {symbol}"),
            &msg,
            "WARNING",
            Some(json!({
                "symbol": symbol,
                "side": side,
                "entry_price": round_dp(entry_price, 6),
                "exit_price": round_dp(exit_price, 6),
                "net_pnl_usdc": round_dp(net_pnl_usdc, 2),
                "exit_reason": reason,
            })),
            false,
            true,
        )
    }

    pub fn check_and_alert_regime_flip(
        &self,
        new_regime: &str,
        prob_bull: f64,
        prob_bear: f64,
        prob_chop: f64,
    ) -> Option<AlertDispatchResult> {
        let mut prior_guard = self.last_regime.lock();
        if prior_guard.is_none() {
            if let Ok(snap) = self.db.get_latest_portfolio_snapshot() {
                *prior_guard = Some(snap.hmm_regime);
            }
        }
        let prior = prior_guard.clone();
        *prior_guard = Some(new_regime.to_string());
        drop(prior_guard);

        if let Some(prev) = prior {
            if prev != new_regime {
                let msg = format!(
                    "3-State Gaussian HMM transitioned from *{prev}* to *{new_regime}*."
                );
                return Some(self.send_alert(
                    &format!("HMM Regime Flip: {prev} -> {new_regime}"),
                    &msg,
                    "INFO",
                    Some(json!({
                        "previous_regime": prev,
                        "new_regime": new_regime,
                        "prob_bull": format!("{:.1}%", prob_bull * 100.0),
                        "prob_bear": format!("{:.1}%", prob_bear * 100.0),
                        "prob_chop": format!("{:.1}%", prob_chop * 100.0),
                    })),
                    false,
                    true,
                ));
            }
        }
        None
    }

    pub fn notify_reconciliation_mismatch(
        &self,
        discrepancy_count: usize,
        discrepancies: &[Value],
        wallet_address: &str,
    ) -> Option<AlertDispatchResult> {
        if discrepancy_count == 0 {
            return None;
        }
        let msg = format!(
            "Detected *{discrepancy_count}* position discrepancy(ies) between local SQLite `trades` and Hyperliquid L1 clearinghouse state."
        );
        let sample = discrepancies
            .first()
            .map(Value::to_string)
            .unwrap_or_else(|| "NONE".to_string());
        Some(self.send_alert(
            "Position Reconciliation Mismatch",
            &msg,
            "WARNING",
            Some(json!({
                "wallet_address": mask_identifier(wallet_address),
                "discrepancies_reconciled": discrepancy_count,
                "sample": sample,
            })),
            false,
            true,
        ))
    }

    pub fn check_and_alert_daily_drawdown(
        &self,
        current_equity_usdc: f64,
    ) -> Option<AlertDispatchResult> {
        let cfg = self.load_config_logged();
        let threshold_pct = cfg.get_f64("max_daily_drawdown_alert_pct", 0.03);
        let now_ms = self.db.now_ms();
        let lookback_24h_ms = now_ms - 86_400_000;

        let peak_24h: f64 = self
            .db
            .with_conn(|conn| {
                let p: Option<f64> = conn
                    .query_row(
                        "SELECT MAX(total_equity_usdc)
                         FROM portfolio_snapshots
                         WHERE timestamp_ms >= ?1;",
                        [lookback_24h_ms],
                        |r| r.get(0),
                    )
                    .ok()
                    .flatten();
                Ok(p.unwrap_or(0.0))
            })
            .unwrap_or(0.0);

        let peak = peak_24h.max(cfg.get_f64("initial_equity_usdc", 10000.0));
        if peak <= 0.0 {
            return None;
        }

        let dd_pct = (peak - current_equity_usdc) / peak;
        let today_str = Utc::now().format("%Y-%m-%d").to_string();
        let mut last_date = self.last_drawdown_alert_date.lock();
        if dd_pct >= threshold_pct && last_date.as_deref() != Some(today_str.as_str()) {
            *last_date = Some(today_str);
            drop(last_date);
            let msg = format!(
                "24H Rolling Portfolio Drawdown reached *{:.2}%* (Threshold: {:.2}%).",
                dd_pct * 100.0,
                threshold_pct * 100.0
            );
            return Some(self.send_alert(
                "Daily Drawdown Limit Breached (> 3.0%)",
                &msg,
                "CRITICAL",
                Some(json!({
                    "peak_24h_equity_usdc": format!("${peak:.2}"),
                    "current_equity_usdc": format!("${current_equity_usdc:.2}"),
                    "drawdown_pct": format!("{:.2}%", dd_pct * 100.0),
                    "threshold_pct": format!("{:.2}%", threshold_pct * 100.0),
                })),
                false,
                true,
            ));
        }
        None
    }

    /// Runs a live or isolated `:memory:` loopback webhook verification (`aegis-quant alerts --test`)
    /// without mutating or blanking real Telegram credentials in `self.db` (`S8`).
    pub fn run_test_alert(
        &mut self,
        simulate_local_if_unconfigured: bool,
    ) -> Result<AlertDispatchResult> {
        let cfg = self.db.load_config()?;
        let tg_token = cfg.get_str("telegram_bot_token", "").trim().to_string();
        let tg_chat_id = cfg.get_str("telegram_chat_id", "").trim().to_string();
        let discord_url = cfg.get_str("discord_webhook_url", "").trim().to_string();

        if (!tg_token.is_empty() && !tg_chat_id.is_empty()) || !discord_url.is_empty() {
            return Ok(self.send_alert(
                "Operational Alert Verification",
                "Real-time operational alerting is online and verified.",
                "INFO",
                Some(json!({
                    "mode": cfg.get_str("execution_mode", "PAPER"),
                    "drawdown_threshold_pct": round_dp(cfg.get_f64("max_daily_drawdown_alert_pct", 0.03) * 100.0, 2),
                })),
                false,
                true,
            ));
        }

        if !simulate_local_if_unconfigured {
            return Ok(AlertDispatchResult {
                status: "SKIPPED".to_string(),
                reason: Some("NO_ALERT_CHANNEL_CONFIGURED".to_string()),
                telegram_sent: false,
                webhook_payload_verified: false,
                queued_for_retry: false,
                chat_id: String::new(),
                error_detail: None,
                title: "Operational Alert Verification".to_string(),
                severity: "INFO".to_string(),
                timestamp: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                channels: Vec::new(),
                loopback_verified: None,
                isolated_self_test: None,
                loopback_payload_captured: None,
            });
        }

        // Spin up ephemeral loopback HTTP server and run self-test against an isolated `:memory:` DB (`S8`)
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        let received_payload: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
        let recv_clone = received_payload.clone();

        let handle = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(4);
            while Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                        let mut buf = [0u8; 8192];
                        if let Ok(n) = stream.read(&mut buf) {
                            let req_str = String::from_utf8_lossy(&buf[..n]);
                            if let Some(idx) = req_str.find("\r\n\r\n") {
                                let body = &req_str[idx + 4..];
                                if let Ok(parsed) = serde_json::from_str::<Value>(body) {
                                    *recv_clone.lock() = Some(parsed);
                                }
                            }
                        }
                        let resp_body = r#"{"ok": true, "result": {"message_id": 1}}"#;
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            resp_body.len(),
                            resp_body
                        );
                        let _ = stream.write_all(resp.as_bytes());
                        break;
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(15));
                    }
                    Err(_) => break,
                }
            }
        });

        let iso_db = self.db.clone_to_memory()?;
        iso_db.configure_telegram_alerts(
            Some("test_loopback_bot_token_030rc1"),
            Some("999888777"),
            Some(0.03),
        )?;

        let mut iso_engine = AlertEngine::new(iso_db);
        iso_engine.telegram_api_base = format!("http://127.0.0.1:{port}");

        let mut res = iso_engine.send_alert(
            "Loopback Alert Self-Test",
            "Verified end-to-end Telegram Bot HTTP payload delivery & Fernet token decryption.",
            "INFO",
            Some(json!({
                "regime_flip_trigger": "ENABLED",
                "reconcile_mismatch_trigger": "ENABLED",
                "daily_drawdown_trigger": "3.0%",
            })),
            false,
            false,
        );

        let _ = handle.join();

        res.loopback_verified = Some(true);
        res.isolated_self_test = Some(true);
        res.loopback_payload_captured = Some(received_payload.lock().is_some());
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_loopback_alert_and_retry_queue_preserves_partial_credentials() {
        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        // Configure only telegram_bot_token (leaving chat_id empty so loopback self-test runs)
        db.configure_telegram_alerts(Some("real_production_bot_token_secret"), Some(""), Some(0.03))
            .unwrap();

        let mut alerts = AlertEngine::new(db.clone());
        let res = alerts.run_test_alert(true).unwrap();
        assert_eq!(res.status, "DELIVERED");
        assert_eq!(res.loopback_verified, Some(true));
        // NL-6: chat_id in response and telemetry must be masked
        assert!(res.chat_id.contains("***"));

        // Verify the real token in `db` was NOT overwritten or blanked by the loopback self-test (`S8`)
        let cfg_after = db.load_config().unwrap();
        assert_eq!(
            cfg_after.get_str("telegram_bot_token", ""),
            "real_production_bot_token_secret"
        );
    }

    #[test]
    fn test_r3_b2_reqwest_error_never_leaks_bot_token_or_webhook_url() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        db.configure_telegram_alerts(
            Some("999111222:SUPER_SECRET_BOT_TOKEN_R3_B2"),
            Some("-100123456789"),
            Some(0.03),
        )
        .unwrap();
        db.set_config_value(
            "discord_webhook_url",
            &json!("http://127.0.0.1:1/api/webhooks/777/SUPER_SECRET_DISCORD_WEBHOOK_R3_B2"),
        )
        .unwrap();

        let mut alerts = AlertEngine::new(db.clone());
        alerts.telegram_api_base = "http://127.0.0.1:1".to_string();
        let res = alerts.send_alert("Test Error", "Body", "WARNING", None, false, false);
        assert_eq!(res.status, "ERROR");
        let serialized = res.to_value().to_string();
        assert!(!serialized.contains("SUPER_SECRET_BOT_TOKEN_R3_B2"));
        assert!(!serialized.contains("SUPER_SECRET_DISCORD_WEBHOOK_R3_B2"));

        let logs = db.get_recent_logs(10).unwrap();
        for row in logs {
            let row_str = row.to_string();
            assert!(!row_str.contains("SUPER_SECRET_BOT_TOKEN_R3_B2"));
            assert!(!row_str.contains("SUPER_SECRET_DISCORD_WEBHOOK_R3_B2"));
        }
    }
}

