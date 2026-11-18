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
use crate::telegram::{discover_telegram_chat_id_from_updates, send_telegram_html};
use aegis_core::{
    format_ms_human_utc, mask_identifier, now_iso8601_utc, now_ms, AegisError, AlertProvider,
    Result,
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

static PROCESS_ALERT_DEDUP_CACHE: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();

fn process_dedup_cache() -> &'static Mutex<HashMap<String, i64>> {
    PROCESS_ALERT_DEDUP_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
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
pub fn compute_alert_dedup_key(
    event_type: &str,
    html_message: &str,
    payload_json: Option<&Value>,
) -> String {
    let ev = event_type.trim().to_ascii_uppercase();
    if let Some(sym) = extract_asset_symbol_from_alert(html_message, payload_json) {
        format!("{ev}::ASSET::{sym}")
    } else {
        let prefix = message_prefix_chars(html_message.trim(), 160);
        format!("{ev}::MSG::{prefix}")
    }
}

/// Checks both the process-wide in-memory deduplication cache and MariaDB `alert_queue`
/// to determine whether an alert with the same `event_type` and same asset symbol
/// (or identical first 160 chars of `message`) was already sent within `window_ms` (300,000 ms).
/// Excludes `SYSTEM_TEST_ALERT`.
pub fn is_duplicate_alert_within_window(
    db: &DatabaseManager,
    event_type: &str,
    html_message: &str,
    payload_json: Option<&Value>,
    window_ms: i64,
) -> bool {
    let ev_upper = event_type.trim().to_ascii_uppercase();
    if ev_upper == "SYSTEM_TEST_ALERT" || window_ms <= 0 {
        return false;
    }

    let now = now_ms();
    let cutoff_ms = now.saturating_sub(window_ms);
    let dedup_key = compute_alert_dedup_key(&ev_upper, html_message, payload_json);

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

    // 2. Check MariaDB `alert_queue` for any recent row within `cutoff_ms`
    let prefix = message_prefix_chars(html_message, 160);
    let sym_opt = extract_asset_symbol_from_alert(html_message, payload_json);

    if let Ok(mut conn) = db.get_conn() {
        let count_res: std::result::Result<Option<i64>, mysql::Error> =
            if let Some(ref sym) = sym_opt {
                let sym_tag_pattern = format!("%<code>{sym}</code>%");
                let sym_json_pattern = format!("%\"symbol\":\"{sym}\"%");
                conn.exec_first(
                    r#"SELECT COUNT(*)
                       FROM alert_queue
                       WHERE event_type = :ev
                         AND created_at_ms >= :cutoff
                         AND status IN ('DELIVERED', 'PENDING', 'SKIPPED')
                         AND (
                             message LIKE :sym_tag
                             OR CAST(payload_json AS CHAR) LIKE :sym_json
                             OR LEFT(message, 160) = :prefix
                         )"#,
                    params! {
                        "ev" => event_type.trim(),
                        "cutoff" => cutoff_ms,
                        "sym_tag" => sym_tag_pattern,
                        "sym_json" => sym_json_pattern,
                        "prefix" => &prefix,
                    },
                )
            } else {
                conn.exec_first(
                    r#"SELECT COUNT(*)
                       FROM alert_queue
                       WHERE event_type = :ev
                         AND created_at_ms >= :cutoff
                         AND status IN ('DELIVERED', 'PENDING', 'SKIPPED')
                         AND LEFT(message, 160) = :prefix"#,
                    params! {
                        "ev" => event_type.trim(),
                        "cutoff" => cutoff_ms,
                        "prefix" => &prefix,
                    },
                )
            };

        if let Ok(Some(cnt)) = count_res {
            if cnt > 0 {
                process_dedup_cache().lock().insert(dedup_key, now);
                return true;
            }
        }
    }

    false
}

/// Records an alert dispatch in the process-wide in-memory deduplication cache.
pub fn record_alert_dedup_timestamp(
    event_type: &str,
    html_message: &str,
    payload_json: Option<&Value>,
) {
    let ev_upper = event_type.trim().to_ascii_uppercase();
    if ev_upper == "SYSTEM_TEST_ALERT" {
        return;
    }
    let key = compute_alert_dedup_key(&ev_upper, html_message, payload_json);
    process_dedup_cache().lock().insert(key, now_ms());
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

    /// Reads and decrypts a secret or string configuration key from MariaDB `system_config`.
    pub fn get_decrypted_config_value(&self, key: &str, default: &str) -> String {
        let cfg = self.db.load_config_or_default();
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

    /// Internal raw HTML dispatcher that sends directly to Telegram and/or Discord according to `alert_provider`
    /// without inserting a new row into `alert_queue` (used by both immediate dispatch and `queue.rs` retries).
    pub(crate) fn dispatch_raw_html_without_requeue(
        &self,
        event_type: &str,
        severity: &str,
        html_message: &str,
    ) -> AlertDispatchResult {
        let cfg = self.db.load_config_or_default();
        let provider = cfg.alert_provider();
        let tg_base = cfg.get_str("telegram_api_base_url", "https://api.telegram.org");
        let tg_token = self.get_decrypted_config_value("telegram_bot_token", "");
        let mut tg_chat_id = self.get_decrypted_config_value("telegram_chat_id", "");
        let discord_url = self.get_decrypted_config_value("discord_webhook_url", "");
        let ts_utc = now_iso8601_utc();

        let use_telegram = matches!(provider, AlertProvider::Telegram | AlertProvider::Both)
            || (provider == AlertProvider::Discord
                && discord_url.is_empty()
                && !tg_token.is_empty());
        let use_discord = matches!(provider, AlertProvider::Discord | AlertProvider::Both)
            && !discord_url.is_empty();

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
            if tg_chat_id.is_empty() {
                if let Some(discovered) =
                    discover_telegram_chat_id_from_updates(&tg_base, &tg_token, self.timeout_sec)
                {
                    let _ = self.db.set_config_value(
                        "telegram_chat_id",
                        &discovered,
                        Some("Auto-discovered Telegram chat_id"),
                    );
                    tg_chat_id = discovered;
                }
            }

            let tg_outcome = send_telegram_html(
                &tg_base,
                &tg_token,
                &tg_chat_id,
                html_message,
                self.timeout_sec,
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
                "error": tg_outcome.error_detail,
            }));
        }

        if use_discord && !discord_url.is_empty() {
            let dc_outcome = send_discord_webhook(&discord_url, html_message, self.timeout_sec);
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

    /// Dispatches a formatted HTML alert immediately and records its lifecycle in MariaDB `alert_queue` and `telemetry_logs`.
    /// Enforces a 5-minute (300,000 ms) in-memory and DB-backed deduplication cooldown guard (excluding `SYSTEM_TEST_ALERT`)
    /// so duplicate alerts for the same `event_type` and asset symbol cannot spam Telegram.
    pub fn dispatch_html_alert(
        &self,
        event_type: &str,
        severity: &str,
        html_message: &str,
        payload_json: Option<Value>,
    ) -> AlertDispatchResult {
        if is_duplicate_alert_within_window(
            &self.db,
            event_type,
            html_message,
            payload_json.as_ref(),
            self.dedup_window_ms,
        ) {
            let cfg = self.db.load_config_or_default();
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

        // Record in-memory dedup timestamp prior to dispatch to block concurrent duplicate bursts
        record_alert_dedup_timestamp(event_type, html_message, payload_json.as_ref());

        let mut res = self.dispatch_raw_html_without_requeue(event_type, severity, html_message);
        let db_status = if res.status == "DELIVERED" {
            "DELIVERED"
        } else if res.status == "SKIPPED" {
            "SKIPPED"
        } else if res.queued_for_retry {
            "PENDING"
        } else {
            "FAILED"
        };

        if let Ok(qid) = self.db.insert_alert_queue_record(
            &res.provider,
            event_type,
            severity,
            html_message,
            payload_json.as_ref(),
            db_status,
        ) {
            res.alert_queue_id = Some(qid);
            if db_status == "PENDING" {
                let next_retry = now_ms() + 1000;
                let _ = update_alert_queue_status(&self.db, qid, "PENDING", 1, next_retry);
            }
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
    pub fn send_alert(
        &self,
        title: &str,
        message: &str,
        severity: &str,
        details: Option<Value>,
        _throw_on_error: bool,
        _allow_retry_queue: bool,
    ) -> AlertDispatchResult {
        let html = if message.contains("<b>") && message.contains("AEGIS v2.0") {
            message.to_string()
        } else {
            let icon = match severity.to_ascii_uppercase().as_str() {
                "CRITICAL" | "ERROR" => "🔴",
                "WARNING" => "🟡",
                _ => "⚡",
            };
            let safe_title = escape_html(title);
            let safe_msg = escape_html(message);
            let ts_str = format_ms_human_utc(now_ms());
            format!(
                "{icon} <b>AEGIS v2.0 // {safe_title}</b>\n\
━━━━━━━━━━━━━━━━━━━━━━\n\
<b>Severity:</b> <code>{}</code>\n\n\
{safe_msg}\n\
⏱ <i>{ts_str}</i>",
                escape_html(severity)
            )
        };
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
        assert_eq!(
            compute_alert_dedup_key("POSITION_CLOSED_COOLDOWN", &html, payload.as_ref()),
            "POSITION_CLOSED_COOLDOWN::ASSET::MET-USDC"
        );
    }

    #[test]
    fn test_in_memory_and_db_alert_deduplication_5m_window() {
        clear_alert_dedup_cache_for_tests();
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let engine = AlertEngine::new(db);

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

        // First dispatch proceeds (SKIPPED because no token is configured in isolated test DB)
        let first = engine.notify_position_closed(&closed);
        assert_eq!(first.status, "SKIPPED");

        // Second dispatch within 5 minutes for the same (POSITION_CLOSED_COOLDOWN, HYPE-USDC) is suppressed in-memory
        let second = engine.notify_position_closed(&closed);
        assert_eq!(second.status, "SUPPRESSED_DUPLICATE");
        assert_eq!(
            second.reason.as_deref(),
            Some("DUPLICATE_WITHIN_5M_COOLDOWN")
        );

        // Even if the in-memory cache is cleared (simulating daemon restart), MariaDB `alert_queue` 5-minute check suppresses the duplicate
        clear_alert_dedup_cache_for_tests();
        let third = engine.notify_position_closed(&closed);
        assert_eq!(third.status, "SUPPRESSED_DUPLICATE");

        // SYSTEM_TEST_ALERT is never suppressed by deduplication
        let test1 = engine.send_test_alert();
        let test2 = engine.send_test_alert();
        assert_ne!(test1.status, "SUPPRESSED_DUPLICATE");
        assert_ne!(test2.status, "SUPPRESSED_DUPLICATE");
    }
}
