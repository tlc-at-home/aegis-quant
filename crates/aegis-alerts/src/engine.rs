//! Primary Alert Engine (`aegis-alerts::engine`) orchestrating Rich HTML Telegram Cards,
//! Config-Switchable Discord Webhooks, MariaDB `alert_queue` persistence, and 5-Minute Deduplication Guard.

use crate::discord::send_discord_webhook;
use crate::formatter::{
    escape_html, format_cutover_reconcile_html, format_order_filled_open_html,
    format_position_closed_html, format_system_test_alert_html, format_working_order_placed_html,
    CutoverReconcileAlert, OrderFilledOpenAlert, PositionClosedAlert, SystemTestAlert,
    WorkingOrderPlacedAlert,
};
use crate::queue::{
    enqueue_pending_alert, poll_and_dispatch_pending_alerts, update_alert_queue_status,
    AlertRetryQueue,
};
use crate::telegram::{discover_telegram_chat_id_guarded, send_telegram_html};
use aegis_core::{
    format_ms_human_utc, mask_identifier, now_iso8601_utc, now_ms, AegisError, AlertProvider,
    ConfigMap, Result,
};
use aegis_db::DatabaseManager;
use mysql::params;
use mysql::prelude::*;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::OnceLock;

/// Default 5-minute (300,000 ms) deduplication cooldown window preventing repeated Telegram alerts
/// for the same `(event_type, asset_symbol / message_prefix)`.
pub const DEFAULT_ALERT_DEDUP_WINDOW_MS: i64 = 300_000;

/// Maximum number of entries retained in the process-wide in-memory deduplication cache.
pub const MAX_PROCESS_ALERT_DEDUP_ENTRIES: usize = 2048;

static PROCESS_ALERT_DEDUP_CACHE: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();

fn process_dedup_cache() -> &'static Mutex<HashMap<String, i64>> {
    PROCESS_ALERT_DEDUP_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Inserts a dedup timestamp into `cache`, pruning expired entries and bounding size to `MAX_PROCESS_ALERT_DEDUP_ENTRIES`.
fn cache_insert_bounded(cache: &mut HashMap<String, i64>, key: String, ts_ms: i64, cutoff_ms: i64) {
    cache.retain(|_, ts| *ts >= cutoff_ms);
    while cache.len() >= MAX_PROCESS_ALERT_DEDUP_ENTRIES && !cache.contains_key(&key) {
        if let Some(oldest_key) = cache
            .iter()
            .min_by_key(|(_, ts)| **ts)
            .map(|(k, _)| k.clone())
        {
            cache.remove(&oldest_key);
        } else {
            break;
        }
    }
    cache.insert(key, ts_ms);
}

/// Normalizes synonymous close event types (`POSITION_CLOSED_COOLDOWN` -> `POSITION_CLOSED`).
pub fn normalize_alert_event_type(event_type: &str) -> String {
    let ev = event_type.trim().to_ascii_uppercase();
    if ev == "POSITION_CLOSED_COOLDOWN" {
        "POSITION_CLOSED".to_string()
    } else {
        ev
    }
}

/// Extracts the canonical asset symbol from `payload_json` (`"symbol"`) or from the HTML card tag
/// (`<b>Asset:</b> <code>SYMBOL</code>`).
pub fn extract_asset_symbol_from_alert(
    html_message: &str,
    payload_json: Option<&Value>,
) -> Option<String> {
    if let Some(sym) = payload_json
        .and_then(|v| v.get("symbol"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return Some(sym.to_ascii_uppercase());
    }
    let marker = "<b>Asset:</b> <code>";
    if let Some(start_idx) = html_message.find(marker) {
        let rest = &html_message[start_idx + marker.len()..];
        if let Some(end_idx) = rest.find("</code>") {
            let sym = rest[..end_idx].trim();
            if !sym.is_empty() {
                return Some(sym.to_ascii_uppercase());
            }
        }
    }
    None
}

/// Returns the first `max_chars` characters of `s` safely on UTF-8 char boundaries.
pub fn message_prefix_chars(s: &str, max_chars: usize) -> String {
    s.chars().take(max_chars).collect()
}

/// Computes the deterministic in-memory deduplication key for an alert.
/// Normalizes `POSITION_CLOSED` and `POSITION_CLOSED_COOLDOWN` and includes `position_id`
/// (or distinct trade signature) so distinct closes on the same symbol are not suppressed.
pub fn compute_alert_dedup_key(
    event_type: &str,
    html_message: &str,
    payload_json: Option<&Value>,
) -> String {
    let ev = normalize_alert_event_type(event_type);
    let sym_opt = extract_asset_symbol_from_alert(html_message, payload_json);
    if ev == "POSITION_CLOSED" {
        let sym = sym_opt.unwrap_or_else(|| "UNKNOWN".to_string());
        if let Some(pid) = payload_json
            .and_then(|v| v.get("position_id"))
            .and_then(|p| {
                p.as_i64()
                    .map(|n| n.to_string())
                    .or_else(|| p.as_str().map(|s| s.trim().to_string()))
                    .filter(|s| !s.is_empty())
            })
        {
            return format!("{ev}::ASSET::{sym}::POS::{pid}");
        }
        if let Some(payload) = payload_json {
            let side = payload
                .get("side")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_ascii_uppercase();
            let entry_px = payload.get("entry_price").and_then(Value::as_f64);
            let exit_px = payload.get("exit_price").and_then(Value::as_f64);
            let qty = payload.get("filled_qty").and_then(Value::as_f64);
            if let (Some(en), Some(ex), Some(q)) = (entry_px, exit_px, qty) {
                return format!("{ev}::ASSET::{sym}::{side}::{en:.6}::{ex:.6}::{q:.6}");
            }
        }
        let prefix = message_prefix_chars(html_message.trim(), 160);
        return format!("{ev}::ASSET::{sym}::MSG::{prefix}");
    }
    if let Some(sym) = sym_opt {
        format!("{ev}::ASSET::{sym}")
    } else {
        let prefix = message_prefix_chars(html_message.trim(), 160);
        format!("{ev}::MSG::{prefix}")
    }
}

/// Checks both the process-wide in-memory deduplication cache and MariaDB `alert_queue`
/// to determine whether an alert with the same canonical dedup key was already queued/delivered
/// within `window_ms` (300,000 ms). Excludes `SYSTEM_TEST_ALERT`, `'SKIPPED'`, and `'FAILED'`.
pub fn is_duplicate_alert_within_window(
    db: &DatabaseManager,
    event_type: &str,
    html_message: &str,
    payload_json: Option<&Value>,
    window_ms: i64,
) -> bool {
    let ev_norm = normalize_alert_event_type(event_type);
    if ev_norm == "SYSTEM_TEST_ALERT" || window_ms <= 0 {
        return false;
    }

    let now = now_ms();
    let cutoff_ms = now.saturating_sub(window_ms);
    let dedup_key = compute_alert_dedup_key(&ev_norm, html_message, payload_json);

    // 1. Check process-wide in-memory cache first
    {
        let mut cache = process_dedup_cache().lock();
        cache.retain(|_, ts| *ts >= cutoff_ms);
        if let Some(&last_ts) = cache.get(&dedup_key) {
            if last_ts >= cutoff_ms {
                return true;
            }
        }
    }

    // 2. Check MariaDB `alert_queue` for any recent DELIVERED / PENDING / PARTIAL / DISPATCHING row
    //    (explicitly excluding SKIPPED and FAILED) and warm cache using original `created_at_ms`.
    let (ev_match_a, ev_match_b) = if ev_norm == "POSITION_CLOSED" {
        (
            "POSITION_CLOSED".to_string(),
            "POSITION_CLOSED_COOLDOWN".to_string(),
        )
    } else {
        let trimmed = event_type.trim().to_string();
        (trimmed.clone(), trimmed)
    };

    if let Ok(mut conn) = db.get_conn() {
        type AlertDedupCandidateRow = (i64, String, String, Option<String>);
        let rows: std::result::Result<Vec<AlertDedupCandidateRow>, mysql::Error> = conn.exec(
            r#"SELECT created_at_ms, event_type, message, CAST(payload_json AS CHAR)
                   FROM alert_queue
                   WHERE (event_type = :eva OR event_type = :evb)
                     AND created_at_ms >= :cutoff
                     AND status IN ('DELIVERED', 'PENDING', 'PARTIAL', 'DISPATCHING')
                   ORDER BY created_at_ms DESC, id DESC
                   LIMIT 64"#,
            params! {
                "eva" => &ev_match_a,
                "evb" => &ev_match_b,
                "cutoff" => cutoff_ms,
            },
        );

        if let Ok(candidates) = rows {
            for (orig_created_ms, row_ev, row_msg, raw_payload) in candidates {
                let row_payload = raw_payload.and_then(|s| serde_json::from_str::<Value>(&s).ok());
                let row_key = compute_alert_dedup_key(&row_ev, &row_msg, row_payload.as_ref());
                if row_key == dedup_key && orig_created_ms >= cutoff_ms {
                    let mut cache = process_dedup_cache().lock();
                    cache_insert_bounded(&mut cache, dedup_key, orig_created_ms, cutoff_ms);
                    return true;
                }
            }
        }
    }

    false
}

/// Records an alert dispatch in the process-wide in-memory deduplication cache with explicit timestamp & window.
pub fn record_alert_dedup_timestamp_with_ts(
    event_type: &str,
    html_message: &str,
    payload_json: Option<&Value>,
    ts_ms: i64,
    window_ms: i64,
) {
    let ev_norm = normalize_alert_event_type(event_type);
    if ev_norm == "SYSTEM_TEST_ALERT" || window_ms <= 0 {
        return;
    }
    let key = compute_alert_dedup_key(&ev_norm, html_message, payload_json);
    let cutoff_ms = now_ms().saturating_sub(window_ms);
    let mut cache = process_dedup_cache().lock();
    cache_insert_bounded(&mut cache, key, ts_ms, cutoff_ms);
}

/// Records an alert dispatch in the process-wide in-memory deduplication cache.
pub fn record_alert_dedup_timestamp(
    event_type: &str,
    html_message: &str,
    payload_json: Option<&Value>,
) {
    record_alert_dedup_timestamp_with_ts(
        event_type,
        html_message,
        payload_json,
        now_ms(),
        DEFAULT_ALERT_DEDUP_WINDOW_MS,
    );
}

/// Returns the current number of entries in the process-wide deduplication cache (for unit tests).
pub fn alert_dedup_cache_len_for_tests() -> usize {
    process_dedup_cache().lock().len()
}

/// Clears the in-memory deduplication cache (for unit tests).
pub fn clear_alert_dedup_cache_for_tests() {
    process_dedup_cache().lock().clear();
}

/// Structured dispatch outcome returned by all `AlertEngine` methods.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertDispatchResult {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub provider: String,
    pub telegram_sent: bool,
    pub discord_sent: bool,
    pub webhook_payload_verified: bool,
    pub queued_for_retry: bool,
    pub alert_queue_id: Option<i64>,
    pub chat_id: String,
    pub error_detail: Option<String>,
    pub title: String,
    pub severity: String,
    pub timestamp: String,
    pub html_message: String,
    pub channels: Vec<Value>,
}

impl AlertDispatchResult {
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| json!({}))
    }

    pub fn is_delivered(&self) -> bool {
        self.status == "DELIVERED"
    }
}

/// Primary v2.0 Alerting Engine backed by MariaDB `system_config` and `alert_queue`.
#[derive(Clone)]
pub struct AlertEngine {
    pub db: DatabaseManager,
    pub timeout_sec: u64,
    pub dedup_window_ms: i64,
    pub retry_queue: AlertRetryQueue,
}

impl AlertEngine {
    pub fn new(db: DatabaseManager) -> Self {
        let cfg = db.load_config_or_default();
        let timeout_sec = cfg.get_usize("alert_http_timeout_sec", 10).max(2) as u64;
        let dedup_window_ms = cfg
            .get_i64("alert_dedup_window_ms", DEFAULT_ALERT_DEDUP_WINDOW_MS)
            .max(0);
        Self {
            db,
            timeout_sec,
            dedup_window_ms,
            retry_queue: AlertRetryQueue::new(),
        }
    }

    /// Reads and decrypts a secret or string configuration key from a pre-loaded `ConfigMap`.
    fn decrypt_from_config(&self, cfg: &ConfigMap, key: &str, default: &str) -> String {
        let val = cfg.get_str(key, default);
        if val.starts_with("gAAAAAB") || val.starts_with("ENC:gAAAAAB") {
            self.db
                .fernet()
                .decrypt(&val)
                .unwrap_or_else(|_| default.to_string())
        } else {
            val
        }
    }

    /// Reads and decrypts a secret or string configuration key from MariaDB `system_config`.
    pub fn get_decrypted_config_value(&self, key: &str, default: &str) -> String {
        let cfg = self.db.load_config_or_default();
        self.decrypt_from_config(&cfg, key, default)
    }

    /// Internal raw HTML dispatcher allowing channel filtering so `PARTIAL` retries
    /// do not re-send to providers that already succeeded.
    pub(crate) fn dispatch_raw_html_for_channels(
        &self,
        event_type: &str,
        severity: &str,
        html_message: &str,
        allow_telegram: bool,
        allow_discord: bool,
    ) -> AlertDispatchResult {
        let cfg = self.db.load_config_or_default();
        let active_timeout_sec = cfg
            .get_usize("alert_http_timeout_sec", self.timeout_sec as usize)
            .max(2) as u64;
        let provider = cfg.alert_provider();
        let tg_base = cfg.get_str("telegram_api_base_url", "https://api.telegram.org");
        let tg_token = self.decrypt_from_config(&cfg, "telegram_bot_token", "");
        let mut tg_chat_id = self.decrypt_from_config(&cfg, "telegram_chat_id", "");
        let discord_url = self.decrypt_from_config(&cfg, "discord_webhook_url", "");
        let ts_utc = now_iso8601_utc();

        let provider_wants_tg = matches!(provider, AlertProvider::Telegram | AlertProvider::Both)
            || (provider == AlertProvider::Discord
                && discord_url.is_empty()
                && !tg_token.is_empty());
        let provider_wants_dc = matches!(provider, AlertProvider::Discord | AlertProvider::Both)
            && !discord_url.is_empty();

        let use_telegram = allow_telegram && provider_wants_tg;
        let use_discord = allow_discord && provider_wants_dc;

        if (!use_telegram || tg_token.is_empty()) && (!use_discord || discord_url.is_empty()) {
            return AlertDispatchResult {
                status: "SKIPPED".to_string(),
                reason: Some("NO_ALERT_CHANNEL_CONFIGURED".to_string()),
                provider: provider.as_str().to_string(),
                telegram_sent: false,
                discord_sent: false,
                webhook_payload_verified: false,
                queued_for_retry: false,
                alert_queue_id: None,
                chat_id: String::new(),
                error_detail: None,
                title: event_type.to_string(),
                severity: severity.to_string(),
                timestamp: ts_utc,
                html_message: html_message.to_string(),
                channels: Vec::new(),
            };
        }

        let mut channels = Vec::new();
        let mut tg_sent = false;
        let mut discord_sent = false;
        let mut any_error = false;
        let mut retriable = false;
        let mut error_detail: Option<String> = None;

        if use_telegram && !tg_token.is_empty() {
            // Never overwrite an already-configured non-empty `telegram_chat_id` (even if raw in DB)
            if tg_chat_id.trim().is_empty() {
                let raw_has_chat_id = self
                    .db
                    .list_system_config_rows()
                    .map(|rows| {
                        rows.iter().any(|r| {
                            r.config_key == "telegram_chat_id" && !r.config_value.trim().is_empty()
                        })
                    })
                    .unwrap_or(false);
                if !raw_has_chat_id {
                    if let Some(discovered) = discover_telegram_chat_id_guarded(
                        &tg_base,
                        &tg_token,
                        &tg_chat_id,
                        active_timeout_sec,
                    ) {
                        let _ = self.db.set_config_value(
                            "telegram_chat_id",
                            &discovered,
                            Some("Auto-discovered Telegram chat_id"),
                        );
                        tg_chat_id = discovered;
                    }
                }
            }

            let tg_outcome = send_telegram_html(
                &tg_base,
                &tg_token,
                &tg_chat_id,
                html_message,
                active_timeout_sec,
            );
            tg_sent = tg_outcome.delivered;
            if !tg_outcome.delivered {
                any_error = true;
                retriable = retriable || tg_outcome.retriable;
                error_detail = tg_outcome.error_detail.clone();
            }
            channels.push(json!({
                "channel": "TELEGRAM",
                "parse_mode": "HTML",
                "http_status": tg_outcome.http_status,
                "chat_id": tg_outcome.masked_chat_id,
                "ok": tg_outcome.delivered,
                "retriable": tg_outcome.retriable,
                "error": tg_outcome.error_detail,
            }));
        }

        if use_discord && !discord_url.is_empty() {
            let dc_outcome = send_discord_webhook(&discord_url, html_message, active_timeout_sec);
            discord_sent = dc_outcome.delivered;
            if !dc_outcome.delivered {
                any_error = true;
                retriable = retriable || dc_outcome.retriable;
                if error_detail.is_none() {
                    error_detail = dc_outcome.error_detail.clone();
                }
            }
            channels.push(json!({
                "channel": "DISCORD",
                "http_status": dc_outcome.http_status,
                "ok": dc_outcome.delivered,
                "retriable": dc_outcome.retriable,
                "error": dc_outcome.error_detail,
            }));
        }

        let status = if (tg_sent || discord_sent) && !any_error {
            self.retry_queue.record_delivered();
            "DELIVERED".to_string()
        } else if tg_sent || discord_sent {
            "PARTIAL".to_string()
        } else {
            if !retriable {
                self.retry_queue.record_failed();
            }
            "ERROR".to_string()
        };

        AlertDispatchResult {
            status,
            reason: None,
            provider: provider.as_str().to_string(),
            telegram_sent: tg_sent,
            discord_sent,
            webhook_payload_verified: tg_sent || discord_sent,
            queued_for_retry: retriable && any_error,
            alert_queue_id: None,
            chat_id: mask_identifier(&tg_chat_id),
            error_detail,
            title: event_type.to_string(),
            severity: severity.to_string(),
            timestamp: ts_utc,
            html_message: html_message.to_string(),
            channels,
        }
    }

    /// Internal raw HTML dispatcher that sends directly to Telegram and/or Discord according to `alert_provider`
    /// without inserting a new row into `alert_queue` (used by both immediate dispatch and `queue.rs` retries).
    pub(crate) fn dispatch_raw_html_without_requeue(
        &self,
        event_type: &str,
        severity: &str,
        html_message: &str,
    ) -> AlertDispatchResult {
        self.dispatch_raw_html_for_channels(event_type, severity, html_message, true, true)
    }

    /// Dispatches a formatted HTML alert immediately and records its lifecycle in MariaDB `alert_queue` and `telemetry_logs`.
    /// Enforces a 5-minute (300,000 ms) in-memory and DB-backed deduplication cooldown guard (excluding `SYSTEM_TEST_ALERT`),
    /// recording the dedup timestamp *after* enqueue/dispatch succeeds.
    pub fn dispatch_html_alert(
        &self,
        event_type: &str,
        severity: &str,
        html_message: &str,
        payload_json: Option<Value>,
    ) -> AlertDispatchResult {
        let cfg = self.db.load_config_or_default();
        let active_window_ms = cfg
            .get_i64("alert_dedup_window_ms", self.dedup_window_ms)
            .max(0);

        if is_duplicate_alert_within_window(
            &self.db,
            event_type,
            html_message,
            payload_json.as_ref(),
            active_window_ms,
        ) {
            return AlertDispatchResult {
                status: "SUPPRESSED_DUPLICATE".to_string(),
                reason: Some("DUPLICATE_WITHIN_5M_COOLDOWN".to_string()),
                provider: cfg.alert_provider().as_str().to_string(),
                telegram_sent: false,
                discord_sent: false,
                webhook_payload_verified: false,
                queued_for_retry: false,
                alert_queue_id: None,
                chat_id: String::new(),
                error_detail: None,
                title: event_type.to_string(),
                severity: severity.to_string(),
                timestamp: now_iso8601_utc(),
                html_message: html_message.to_string(),
                channels: Vec::new(),
            };
        }

        let mut res = self.dispatch_raw_html_without_requeue(event_type, severity, html_message);
        let db_status = if res.status == "DELIVERED" {
            "DELIVERED"
        } else if res.status == "SKIPPED" {
            "SKIPPED"
        } else if res.status == "PARTIAL" && res.queued_for_retry {
            "PARTIAL"
        } else if res.queued_for_retry {
            "PENDING"
        } else {
            "FAILED"
        };

        let mut stored_payload = payload_json.clone();
        if db_status == "PARTIAL" {
            let mut obj = match stored_payload.take() {
                Some(Value::Object(map)) => map,
                Some(other) => {
                    let mut m = serde_json::Map::new();
                    m.insert("data".to_string(), other);
                    m
                }
                None => serde_json::Map::new(),
            };
            obj.insert(
                "_delivery".to_string(),
                json!({
                    "telegram_sent": res.telegram_sent,
                    "discord_sent": res.discord_sent,
                }),
            );
            stored_payload = Some(Value::Object(obj));
        }

        let mut db_inserted = false;
        if let Ok(qid) = self.db.insert_alert_queue_record(
            &res.provider,
            event_type,
            severity,
            html_message,
            stored_payload.as_ref(),
            db_status,
        ) {
            db_inserted = true;
            res.alert_queue_id = Some(qid);
            if matches!(db_status, "PENDING" | "PARTIAL") {
                let next_retry = now_ms() + 1000;
                let _ = update_alert_queue_status(&self.db, qid, db_status, 1, next_retry);
            }
        }

        // Record dedup key ONLY AFTER dispatch/enqueue succeeded (never on SKIPPED or FAILED)
        if db_status == "DELIVERED" || (db_inserted && matches!(db_status, "PENDING" | "PARTIAL")) {
            record_alert_dedup_timestamp_with_ts(
                event_type,
                html_message,
                payload_json.as_ref(),
                now_ms(),
                active_window_ms,
            );
        }

        let log_level = if res.status == "DELIVERED" || res.status == "SKIPPED" {
            "INFO"
        } else {
            "WARNING"
        };
        let _ = self.db.log_telemetry(
            log_level,
            "aegis-alerts",
            &format!("Alert [{severity}] {event_type} -> {}", res.status),
            Some(json!({
                "provider": res.provider,
                "status": res.status,
                "telegram_sent": res.telegram_sent,
                "discord_sent": res.discord_sent,
                "chat_id": res.chat_id,
                "error_detail": res.error_detail,
            })),
        );

        res
    }

    /// Async wrapper for `dispatch_html_alert`.
    pub async fn dispatch_html_alert_async(
        &self,
        event_type: &str,
        severity: &str,
        html_message: &str,
        payload_json: Option<Value>,
    ) -> Result<AlertDispatchResult> {
        let engine = self.clone();
        let ev = event_type.to_string();
        let sev = severity.to_string();
        let msg = html_message.to_string();
        tokio::task::spawn_blocking(move || {
            engine.dispatch_html_alert(&ev, &sev, &msg, payload_json)
        })
        .await
        .map_err(|e| AegisError::Alert(format!("dispatch_html_alert_async join failed: {e}")))
    }

    /// Enqueues an HTML alert into MariaDB `alert_queue` (`status = 'PENDING'`) for background polling,
    /// enforcing the 5-minute deduplication guard.
    pub fn enqueue_html_alert(
        &self,
        event_type: &str,
        severity: &str,
        html_message: &str,
        payload_json: Option<&Value>,
    ) -> Result<i64> {
        enqueue_pending_alert(&self.db, event_type, severity, html_message, payload_json)
    }

    /// Polls and dispatches due `PENDING` alerts from MariaDB `alert_queue`.
    pub fn poll_pending_queue(&self, batch_limit: usize) -> Result<usize> {
        poll_and_dispatch_pending_alerts(self, batch_limit)
    }

    /// Formats and dispatches a `WORKING ORDER (RESERVED)` rich HTML card.
    pub fn notify_working_order_placed(
        &self,
        alert: &WorkingOrderPlacedAlert,
    ) -> AlertDispatchResult {
        let html = format_working_order_placed_html(alert);
        let payload = serde_json::to_value(alert).ok();
        self.dispatch_html_alert("WORKING_ORDER_RESERVED", "INFO", &html, payload)
    }

    /// Formats and dispatches an `ORDER FILLED (OPEN)` rich HTML card matching Component 5 of `PLAN_V2.md`.
    pub fn notify_order_filled_open(&self, alert: &OrderFilledOpenAlert) -> AlertDispatchResult {
        let html = format_order_filled_open_html(alert);
        let payload = serde_json::to_value(alert).ok();
        self.dispatch_html_alert("ORDER_FILLED_OPEN", "INFO", &html, payload)
    }

    /// Formats and dispatches a `POSITION CLOSED & COOLDOWN` rich HTML card.
    pub fn notify_position_closed(&self, alert: &PositionClosedAlert) -> AlertDispatchResult {
        let html = format_position_closed_html(alert);
        let payload = serde_json::to_value(alert).ok();
        self.dispatch_html_alert("POSITION_CLOSED_COOLDOWN", "WARNING", &html, payload)
    }

    /// Formats and dispatches a `LIVE TELEMETRY & CUTOVER VERIFIED` rich HTML card after cutover reconciliation.
    pub fn notify_cutover_reconciled(&self, alert: &CutoverReconcileAlert) -> AlertDispatchResult {
        let html = format_cutover_reconcile_html(alert);
        let payload = serde_json::to_value(alert).ok();
        self.dispatch_html_alert("CUTOVER_RECONCILED", "INFO", &html, payload)
    }

    /// Builds and dispatches a live `LIVE TELEMETRY & CUTOVER VERIFIED` HTML test card using current MariaDB state.
    pub fn send_test_alert(&self) -> AlertDispatchResult {
        send_test_alert(&self.db)
    }

    /// General-purpose alert helper formatting arbitrary title/message/details as a clean v2.0 HTML card.
    /// Always HTML-escapes dynamic title, severity, and message fields.
    pub fn send_alert(
        &self,
        title: &str,
        message: &str,
        severity: &str,
        details: Option<Value>,
        _throw_on_error: bool,
        _allow_retry_queue: bool,
    ) -> AlertDispatchResult {
        let icon = match severity.to_ascii_uppercase().as_str() {
            "CRITICAL" | "ERROR" => "🔴",
            "WARNING" => "🟡",
            _ => "⚡",
        };
        let safe_title = escape_html(title);
        let safe_msg = escape_html(message);
        let ts_str = format_ms_human_utc(now_ms());
        let html = format!(
            "{icon} <b>AEGIS v2.0 // {safe_title}</b>\n\
━━━━━━━━━━━━━━━━━━━━━━\n\
<b>Severity:</b> <code>{}</code>\n\n\
{safe_msg}\n\
⏱ <i>{ts_str}</i>",
            escape_html(severity)
        );
        self.dispatch_html_alert(title, severity, &html, details)
    }
}

/// Standalone helper that loads and decrypts `telegram_bot_token` and `telegram_chat_id` from MariaDB `system_config`,
/// snapshots live portfolio/view telemetry, and dispatches a rich HTML `LIVE TELEMETRY & CUTOVER VERIFIED` card.
pub fn send_test_alert(db: &DatabaseManager) -> AlertDispatchResult {
    let cfg = db.load_config_or_default();
    let snap =
        db.get_latest_portfolio_snapshot()
            .unwrap_or_else(|_| aegis_core::PortfolioSnapshot {
                id: 0,
                total_equity_usdc: cfg.get_f64("initial_paper_equity_usdc", 10_000.0),
                available_cash_usdc: cfg.get_f64("initial_paper_equity_usdc", 10_000.0),
                reserved_order_cash_usdc: 0.0,
                committed_notional_usdc: 0.0,
                net_notional_usdc: 0.0,
                gross_leverage_x: 0.0,
                net_leverage_x: 0.0,
                market_beta_exposure: 0.0,
                market_breadth_bull: 0.5,
                market_regime_label: "NEUTRAL".to_string(),
                risk_tier: cfg.risk_tier().as_str().to_string(),
                leverage_multiplier: cfg.target_leverage_multiplier(),
                s1_gross_weight: 0.0,
                s2_gross_weight: 0.0,
                s3_gross_weight: 0.0,
                working_order_count: 0,
                open_long_count: 0,
                open_short_count: 0,
                eligible_coin_count: 0,
                created_at_ms: now_ms(),
                updated_at_ms: now_ms(),
            });

    let mariadb_version = db
        .server_version()
        .unwrap_or_else(|_| "13.0.2-MariaDB".to_string());
    let total_assets = db
        .list_exchange_assets()
        .map(|v| v.len())
        .unwrap_or(snap.eligible_coin_count.max(0) as usize);

    let test_card = SystemTestAlert {
        execution_mode: cfg.execution_mode().as_str().to_string(),
        mariadb_version,
        risk_tier: cfg.risk_tier(),
        leverage_multiplier: Some(cfg.target_leverage_multiplier()),
        total_equity_usdc: snap.total_equity_usdc,
        available_cash_usdc: snap.available_cash_usdc,
        reserved_order_cash_usdc: snap.reserved_order_cash_usdc,
        committed_notional_usdc: snap.committed_notional_usdc,
        working_orders: snap.working_order_count.max(0) as usize,
        open_positions: (snap.open_long_count.max(0) + snap.open_short_count.max(0)) as usize,
        eligible_perps: total_assets.max(snap.eligible_coin_count.max(0) as usize),
        timestamp_ms: now_ms(),
    };

    let html = format_system_test_alert_html(&test_card);
    let payload = serde_json::to_value(&test_card).ok();
    let engine = AlertEngine::new(db.clone());
    engine.dispatch_html_alert("SYSTEM_TEST_ALERT", "INFO", &html, payload)
}

/// Async version of `send_test_alert(&DatabaseManager)` for Axum web handlers (`POST /api/v1/alerts/test`).
pub async fn send_test_alert_async(db: &DatabaseManager) -> Result<AlertDispatchResult> {
    let db_clone = db.clone();
    tokio::task::spawn_blocking(move || send_test_alert(&db_clone))
        .await
        .map_err(|e| AegisError::Alert(format!("send_test_alert_async join failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_core::OrderSide;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    static DEDUP_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn spawn_mock_telegram_server(
        expected_requests: usize,
        response_status: &'static str,
        response_body: &'static str,
    ) -> (String, Arc<AtomicUsize>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let count_cl = Arc::clone(&count);
        let handle = thread::spawn(move || {
            for _ in 0..expected_requests {
                if let Ok((mut stream, _)) = listener.accept() {
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf);
                    count_cl.fetch_add(1, Ordering::SeqCst);
                    let resp = format!(
                        "HTTP/1.1 {response_status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                        response_body.len()
                    );
                    let _ = stream.write_all(resp.as_bytes());
                }
            }
        });
        (format!("http://{addr}"), count, handle)
    }

    #[test]
    fn test_extract_asset_symbol_and_dedup_key() {
        let closed = PositionClosedAlert {
            symbol: "MET-USDC".to_string(),
            side: OrderSide::Short,
            exit_reason: "PROTECTIVE_STOP_EXIT (mark=0.1753, stop=0.1726)".to_string(),
            entry_price: 0.1660,
            exit_price: 0.1753,
            filled_qty: 799.0,
            realized_pnl_usdc: -7.43,
            available_cash_usdc: 910.42,
            cooldown_type: Some("STOP_OUT".to_string()),
            cooldown_seconds: 1800,
            timestamp_ms: 1795053912000,
        };
        let html = format_position_closed_html(&closed);
        let payload = serde_json::to_value(&closed).ok();

        assert_eq!(
            extract_asset_symbol_from_alert(&html, None),
            Some("MET-USDC".to_string())
        );
        assert_eq!(
            extract_asset_symbol_from_alert("plain message", payload.as_ref()),
            Some("MET-USDC".to_string())
        );
        let k_cd = compute_alert_dedup_key("POSITION_CLOSED_COOLDOWN", &html, payload.as_ref());
        let k_cl = compute_alert_dedup_key("POSITION_CLOSED", &html, payload.as_ref());
        assert_eq!(k_cd, k_cl);
        assert!(k_cd.starts_with("POSITION_CLOSED::ASSET::MET-USDC::SHORT::"));

        // Distinct close (different entry/qty or position_id) produces a distinct dedup key
        let mut closed_other = closed.clone();
        closed_other.entry_price = 0.2100;
        let other_payload = serde_json::to_value(&closed_other).ok();
        let k_other = compute_alert_dedup_key("POSITION_CLOSED", &html, other_payload.as_ref());
        assert_ne!(k_cd, k_other);
    }

    #[test]
    fn test_in_memory_and_db_alert_deduplication_post_send_and_skip_exclusion() {
        let _guard = DEDUP_TEST_LOCK.lock();
        clear_alert_dedup_cache_for_tests();
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let engine = AlertEngine::new(db.clone());

        let closed = PositionClosedAlert {
            symbol: "HYPE-USDC".to_string(),
            side: OrderSide::Short,
            exit_reason: "PROTECTIVE_STOP_EXIT".to_string(),
            entry_price: 39.20,
            exit_price: 41.32,
            filled_qty: 3.31,
            realized_pnl_usdc: -7.02,
            available_cash_usdc: 910.42,
            cooldown_type: Some("STOP_OUT".to_string()),
            cooldown_seconds: 1800,
            timestamp_ms: now_ms(),
        };

        // SKIPPED dispatches (unconfigured credentials) must NOT record in dedup cache or suppress future alerts
        let skipped1 = engine.notify_position_closed(&closed);
        assert_eq!(skipped1.status, "SKIPPED");
        let skipped2 = engine.notify_position_closed(&closed);
        assert_eq!(skipped2.status, "SKIPPED");

        // Now configure mock Telegram server and dispatch -> DELIVERED records dedup AFTER send
        let (base_url, calls, srv_handle) =
            spawn_mock_telegram_server(3, "200 OK", "{\"ok\":true,\"result\":{}}");
        db.set_config_value("telegram_api_base_url", &base_url, None)
            .unwrap();
        db.set_config_value("telegram_bot_token", "123456:TOKEN", None)
            .unwrap();
        db.set_config_value("telegram_chat_id", "999888777", None)
            .unwrap();

        let delivered = engine.notify_position_closed(&closed);
        assert_eq!(delivered.status, "DELIVERED");

        // Immediate duplicate within 5m is suppressed in-memory (including synonymous POSITION_CLOSED)
        let dup_in_mem = engine.notify_position_closed(&closed);
        assert_eq!(dup_in_mem.status, "SUPPRESSED_DUPLICATE");
        let dup_synonym = engine.dispatch_html_alert(
            "POSITION_CLOSED",
            "WARNING",
            &format_position_closed_html(&closed),
            serde_json::to_value(&closed).ok(),
        );
        assert_eq!(dup_synonym.status, "SUPPRESSED_DUPLICATE");

        // Distinct position close on the same symbol is NOT suppressed
        let mut distinct = closed.clone();
        distinct.entry_price = 42.00;
        let distinct_res = engine.notify_position_closed(&distinct);
        assert_eq!(distinct_res.status, "DELIVERED");

        // Clear in-memory cache (simulating restart) -> DB DELIVERED row suppresses duplicate
        clear_alert_dedup_cache_for_tests();
        let dup_from_db = engine.notify_position_closed(&closed);
        assert_eq!(dup_from_db.status, "SUPPRESSED_DUPLICATE");

        // SYSTEM_TEST_ALERT is never suppressed by deduplication
        let test1 = engine.send_test_alert();
        assert_eq!(test1.status, "DELIVERED");
        let _ = srv_handle.join();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_db_cache_warming_preserves_original_created_at_ms_and_caps_2048() {
        let _guard = DEDUP_TEST_LOCK.lock();
        clear_alert_dedup_cache_for_tests();
        let db = DatabaseManager::new_isolated_test(None).unwrap();

        let msg = "<b>Asset:</b> <code>SOL-USDC</code>";
        let payload = json!({ "symbol": "SOL-USDC" });
        let old_ts = now_ms() - 295_000; // 295s old
        let mut conn = db.get_conn().unwrap();
        conn.exec_drop(
            r#"INSERT INTO alert_queue
               (provider, event_type, severity, message, payload_json, status, attempts, next_retry_ms, created_at_ms, updated_at_ms)
               VALUES ('telegram', 'ORDER_FILLED_OPEN', 'INFO', :msg, :pay, 'DELIVERED', 1, :ts, :ts, :ts)"#,
            params! {
                "msg" => msg,
                "pay" => payload.to_string(),
                "ts" => old_ts,
            },
        )
        .unwrap();

        // Within 300s window -> true, and warms cache with `old_ts` (NOT `now_ms()`)
        assert!(is_duplicate_alert_within_window(
            &db,
            "ORDER_FILLED_OPEN",
            msg,
            Some(&payload),
            300_000
        ));
        // Within 290s window -> false (proves cache retained `old_ts` rather than extending to `now_ms()`)
        assert!(!is_duplicate_alert_within_window(
            &db,
            "ORDER_FILLED_OPEN",
            msg,
            Some(&payload),
            290_000
        ));

        // Cache never exceeds MAX_PROCESS_ALERT_DEDUP_ENTRIES (2048)
        for i in 0..2150 {
            let sym = format!("CAP{i}-USDC");
            record_alert_dedup_timestamp_with_ts(
                "ORDER_FILLED_OPEN",
                &format!("<b>Asset:</b> <code>{sym}</code>"),
                Some(&json!({ "symbol": sym })),
                now_ms(),
                300_000,
            );
        }
        assert!(alert_dedup_cache_len_for_tests() <= MAX_PROCESS_ALERT_DEDUP_ENTRIES);
        clear_alert_dedup_cache_for_tests();
    }

    #[test]
    fn test_send_alert_always_escapes_html_and_never_overwrites_chat_id() {
        let _guard = DEDUP_TEST_LOCK.lock();
        clear_alert_dedup_cache_for_tests();
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let (base_url, calls, srv_handle) =
            spawn_mock_telegram_server(1, "200 OK", "{\"ok\":true,\"result\":{}}");

        db.set_config_value("telegram_api_base_url", &base_url, None)
            .unwrap();
        db.set_config_value("telegram_bot_token", "123456:TOKEN", None)
            .unwrap();
        db.set_config_value("telegram_chat_id", "555444333", None)
            .unwrap();

        let engine = AlertEngine::new(db.clone());
        let res = engine.send_alert(
            "TEST <ALERT>",
            "<b>AEGIS v2.0</b> <script>alert('xss')</script>",
            "WARNING",
            None,
            false,
            false,
        );
        assert_eq!(res.status, "DELIVERED");
        assert!(!res.html_message.contains("<script>"));
        assert!(res.html_message.contains("&lt;script&gt;"));
        assert_eq!(
            db.load_config().unwrap().get_str("telegram_chat_id", ""),
            "555444333"
        );
        let _ = srv_handle.join();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
