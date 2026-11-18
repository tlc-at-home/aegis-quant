//! MariaDB `alert_queue` poller, persistent retry state machine, and in-memory retry queue (`aegis-alerts::queue`).

use crate::engine::{
    is_duplicate_alert_within_window, record_alert_dedup_timestamp, AlertEngine,
    DEFAULT_ALERT_DEDUP_WINDOW_MS,
};
use aegis_core::{now_ms, AegisError, AlertQueueRecord, Result};
use aegis_db::DatabaseManager;
use mysql::params;
use mysql::prelude::*;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// Enqueues a rich HTML alert card into MariaDB `alert_queue` with `status = "PENDING"`,
/// enforcing the 5-minute (300,000 ms) deduplication cooldown guard (excluding `SYSTEM_TEST_ALERT`).
pub fn enqueue_pending_alert(
    db: &DatabaseManager,
    event_type: &str,
    severity: &str,
    html_message: &str,
    payload_json: Option<&Value>,
) -> Result<i64> {
    let cfg = db.load_config_or_default();
    let window_ms = cfg
        .get_i64("alert_dedup_window_ms", DEFAULT_ALERT_DEDUP_WINDOW_MS)
        .max(0);
    if is_duplicate_alert_within_window(db, event_type, html_message, payload_json, window_ms) {
        return Ok(0);
    }
    record_alert_dedup_timestamp(event_type, html_message, payload_json);
    let provider = cfg.alert_provider().as_str();
    let ts = now_ms();
    let payload_str = payload_json.map(|v| v.to_string());
    let mut conn = db.get_conn()?;
    conn.exec_drop(
        r#"INSERT INTO alert_queue
           (provider, event_type, severity, message, payload_json, status, attempts, next_retry_ms, created_at_ms, updated_at_ms)
           VALUES (:prov, :ev, :sev, :msg, :pay, 'PENDING', 0, :ts, :ts, :ts)"#,
        params! {
            "prov" => provider,
            "ev" => event_type,
            "sev" => severity,
            "msg" => html_message,
            "pay" => payload_str,
            "ts" => ts,
        },
    )
    .map_err(|e| AegisError::Database(format!("enqueue_pending_alert failed: {e}")))?;
    Ok(conn.last_insert_id() as i64)
}

/// Fetches up to `limit` `PENDING` alerts from `alert_queue` whose `next_retry_ms <= now_ms()`.
pub fn fetch_due_pending_alerts(
    db: &DatabaseManager,
    limit: usize,
) -> Result<Vec<AlertQueueRecord>> {
    let ts = now_ms();
    let mut conn = db.get_conn()?;
    let rows: Vec<mysql::Row> = conn
        .exec(
            r#"SELECT id, provider, event_type, severity, message, CAST(payload_json AS CHAR),
                      status, attempts, next_retry_ms, created_at_ms, updated_at_ms
               FROM alert_queue
               WHERE status = 'PENDING' AND next_retry_ms <= :now
               ORDER BY created_at_ms ASC, id ASC
               LIMIT :lim"#,
            params! {
                "now" => ts,
                "lim" => limit.max(1) as u64,
            },
        )
        .map_err(|e| AegisError::Database(format!("fetch_due_pending_alerts failed: {e}")))?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let raw_json: Option<String> = r.get::<Option<String>, _>(5).unwrap_or(None);
            let parsed_json = raw_json.and_then(|s| serde_json::from_str::<Value>(&s).ok());
            AlertQueueRecord {
                id: r.get(0).unwrap_or_default(),
                provider: r.get(1).unwrap_or_else(|| "telegram".to_string()),
                event_type: r.get(2).unwrap_or_default(),
                severity: r.get(3).unwrap_or_else(|| "INFO".to_string()),
                message: r.get(4).unwrap_or_default(),
                payload_json: parsed_json,
                status: r.get(6).unwrap_or_else(|| "PENDING".to_string()),
                attempts: r.get(7).unwrap_or(0),
                next_retry_ms: r.get(8).unwrap_or_default(),
                created_at_ms: r.get(9).unwrap_or_default(),
                updated_at_ms: r.get(10).unwrap_or_default(),
            }
        })
        .collect())
}

/// Updates an `alert_queue` row's delivery status, attempts count, and `next_retry_ms`.
pub fn update_alert_queue_status(
    db: &DatabaseManager,
    alert_id: i64,
    status: &str,
    attempts: i32,
    next_retry_ms: i64,
) -> Result<()> {
    let ts = now_ms();
    let mut conn = db.get_conn()?;
    conn.exec_drop(
        r#"UPDATE alert_queue
           SET status = :st,
               attempts = :att,
               next_retry_ms = :next,
               updated_at_ms = :ts
           WHERE id = :id"#,
        params! {
            "st" => status,
            "att" => attempts,
            "next" => next_retry_ms,
            "ts" => ts,
            "id" => alert_id,
        },
    )
    .map_err(|e| AegisError::Database(format!("update_alert_queue_status failed: {e}")))?;
    Ok(())
}

/// Polls due `PENDING` alerts from MariaDB `alert_queue`, dispatches each via `AlertEngine`,
/// and transitions rows to `DELIVERED`, `SKIPPED`, or exponential-backoff `PENDING` / `FAILED`.
pub fn poll_and_dispatch_pending_alerts(engine: &AlertEngine, batch_limit: usize) -> Result<usize> {
    let pending = fetch_due_pending_alerts(&engine.db, batch_limit)?;
    if pending.is_empty() {
        return Ok(0);
    }

    let max_attempts = 5i32;
    let mut delivered_count = 0usize;

    for record in pending {
        let next_attempt_num = record.attempts.saturating_add(1);
        let outcome = engine.dispatch_raw_html_without_requeue(
            &record.event_type,
            &record.severity,
            &record.message,
        );

        if outcome.status == "DELIVERED" {
            update_alert_queue_status(
                &engine.db,
                record.id,
                "DELIVERED",
                next_attempt_num,
                now_ms(),
            )?;
            delivered_count += 1;
        } else if outcome.status == "SKIPPED" {
            update_alert_queue_status(
                &engine.db,
                record.id,
                "SKIPPED",
                next_attempt_num,
                now_ms(),
            )?;
        } else if next_attempt_num < max_attempts {
            let backoff_ms = 500_i64 * (1_i64 << (next_attempt_num.saturating_sub(1).min(6)));
            let next_retry = now_ms() + backoff_ms;
            update_alert_queue_status(
                &engine.db,
                record.id,
                "PENDING",
                next_attempt_num,
                next_retry,
            )?;
        } else {
            update_alert_queue_status(&engine.db, record.id, "FAILED", next_attempt_num, now_ms())?;
        }
    }

    Ok(delivered_count)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedAlertTask {
    pub event_type: String,
    pub message: String,
    pub severity: String,
    pub details: Option<Value>,
    pub attempt: usize,
    pub next_attempt_ms: i64,
}

/// Lightweight thread-safe retry queue wrapper backed by MariaDB `alert_queue`.
#[derive(Clone, Default)]
pub struct AlertRetryQueue {
    in_memory: Arc<Mutex<VecDeque<QueuedAlertTask>>>,
    delivered_count: Arc<AtomicUsize>,
    failed_count: Arc<AtomicUsize>,
}

impl AlertRetryQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pending_count(&self) -> usize {
        self.in_memory.lock().len()
    }

    pub fn delivered_count(&self) -> usize {
        self.delivered_count.load(Ordering::SeqCst)
    }

    pub fn failed_permanently_count(&self) -> usize {
        self.failed_count.load(Ordering::SeqCst)
    }

    pub fn record_delivered(&self) {
        self.delivered_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn record_failed(&self) {
        self.failed_count.fetch_add(1, Ordering::SeqCst);
    }
}
