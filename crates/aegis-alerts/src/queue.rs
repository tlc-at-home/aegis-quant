//! MariaDB `alert_queue` poller, persistent retry state machine, and in-memory retry queue (`aegis-alerts::queue`).

use crate::engine::{
    is_duplicate_alert_within_window, record_alert_dedup_timestamp_with_ts, AlertEngine,
    DEFAULT_ALERT_DEDUP_WINDOW_MS,
};
use aegis_core::{now_ms, AegisError, AlertQueueRecord, Result};
use aegis_db::DatabaseManager;
use mysql::params;
use mysql::prelude::*;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn row_to_alert_queue_record(r: mysql::Row) -> AlertQueueRecord {
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
}

/// Enqueues a rich HTML alert card into MariaDB `alert_queue` with `status = "PENDING"`,
/// enforcing the 5-minute (300,000 ms) deduplication cooldown guard (excluding `SYSTEM_TEST_ALERT`).
/// Records the dedup timestamp *after* the database insert succeeds.
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
    let inserted_id = conn.last_insert_id() as i64;
    record_alert_dedup_timestamp_with_ts(event_type, html_message, payload_json, ts, window_ms);
    Ok(inserted_id)
}

/// Fetches up to `limit` due `PENDING` / `PARTIAL` alerts from `alert_queue` whose `next_retry_ms <= now_ms()`.
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
               WHERE status IN ('PENDING', 'PARTIAL') AND next_retry_ms <= :now
               ORDER BY created_at_ms ASC, id ASC
               LIMIT :lim"#,
            params! {
                "now" => ts,
                "lim" => limit.max(1) as u64,
            },
        )
        .map_err(|e| AegisError::Database(format!("fetch_due_pending_alerts failed: {e}")))?;

    Ok(rows.into_iter().map(row_to_alert_queue_record).collect())
}

/// Atomically claims up to `limit` due `PENDING` / `PARTIAL` alerts using `FOR UPDATE SKIP LOCKED`
/// and transitions them to `DISPATCHING` inside a transaction before any network dispatch.
pub fn claim_due_pending_alerts(
    db: &DatabaseManager,
    limit: usize,
) -> Result<Vec<AlertQueueRecord>> {
    let ts = now_ms();
    let stale_dispatch_cutoff = ts.saturating_sub(60_000);
    let mut conn = db.get_conn()?;
    let mut tx = conn
        .start_transaction(mysql::TxOpts::default())
        .map_err(|e| AegisError::Database(format!("claim_due_pending_alerts tx failed: {e}")))?;

    // Recover any orphaned DISPATCHING rows left behind by a crashed worker >60s ago
    let _ = tx.exec_drop(
        r#"UPDATE alert_queue
           SET status = 'PENDING', updated_at_ms = :now
           WHERE status = 'DISPATCHING' AND updated_at_ms <= :stale"#,
        params! {
            "now" => ts,
            "stale" => stale_dispatch_cutoff,
        },
    );

    let rows: Vec<mysql::Row> = tx
        .exec(
            r#"SELECT id, provider, event_type, severity, message, CAST(payload_json AS CHAR),
                      status, attempts, next_retry_ms, created_at_ms, updated_at_ms
               FROM alert_queue
               WHERE status IN ('PENDING', 'PARTIAL') AND next_retry_ms <= :now
               ORDER BY created_at_ms ASC, id ASC
               LIMIT :lim
               FOR UPDATE SKIP LOCKED"#,
            params! {
                "now" => ts,
                "lim" => limit.max(1) as u64,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("claim_due_pending_alerts select failed: {e}"))
        })?;

    let mut claimed = Vec::with_capacity(rows.len());
    for r in rows {
        let mut rec = row_to_alert_queue_record(r);
        tx.exec_drop(
            r#"UPDATE alert_queue
               SET status = 'DISPATCHING', updated_at_ms = :now
               WHERE id = :id AND status IN ('PENDING', 'PARTIAL')"#,
            params! {
                "now" => ts,
                "id" => rec.id,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("claim_due_pending_alerts update failed: {e}"))
        })?;
        if tx.affected_rows() == 1 {
            rec.status = "DISPATCHING".to_string();
            rec.updated_at_ms = ts;
            claimed.push(rec);
        }
    }

    tx.commit().map_err(|e| {
        AegisError::Database(format!("claim_due_pending_alerts commit failed: {e}"))
    })?;

    Ok(claimed)
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

/// Updates an `alert_queue` row's status, attempts, `next_retry_ms`, and `payload_json`.
pub fn update_alert_queue_state_and_payload(
    db: &DatabaseManager,
    alert_id: i64,
    status: &str,
    attempts: i32,
    next_retry_ms: i64,
    payload_json: Option<&Value>,
) -> Result<()> {
    let ts = now_ms();
    let payload_str = payload_json.map(|v| v.to_string());
    let mut conn = db.get_conn()?;
    conn.exec_drop(
        r#"UPDATE alert_queue
           SET status = :st,
               attempts = :att,
               next_retry_ms = :next,
               payload_json = COALESCE(:pay, payload_json),
               updated_at_ms = :ts
           WHERE id = :id"#,
        params! {
            "st" => status,
            "att" => attempts,
            "next" => next_retry_ms,
            "pay" => payload_str,
            "ts" => ts,
            "id" => alert_id,
        },
    )
    .map_err(|e| {
        AegisError::Database(format!("update_alert_queue_state_and_payload failed: {e}"))
    })?;
    Ok(())
}

fn extract_delivery_state(payload: Option<&Value>) -> (bool, bool) {
    let tg = payload
        .and_then(|v| v.get("_delivery"))
        .and_then(|d| d.get("telegram_sent"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let dc = payload
        .and_then(|v| v.get("_delivery"))
        .and_then(|d| d.get("discord_sent"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    (tg, dc)
}

fn with_delivery_state(payload: Option<&Value>, tg_sent: bool, dc_sent: bool) -> Value {
    let mut obj = match payload.cloned() {
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
            "telegram_sent": tg_sent,
            "discord_sent": dc_sent,
        }),
    );
    Value::Object(obj)
}

/// Atomically claims due `PENDING` / `PARTIAL` alerts from MariaDB `alert_queue`, dispatches only
/// unsent provider channels via `AlertEngine`, respects `retriable`, and transitions rows to
/// `DELIVERED`, `SKIPPED`, exponential-backoff `PENDING`/`PARTIAL`, or `FAILED`.
pub fn poll_and_dispatch_pending_alerts(engine: &AlertEngine, batch_limit: usize) -> Result<usize> {
    let claimed = claim_due_pending_alerts(&engine.db, batch_limit)?;
    if claimed.is_empty() {
        return Ok(0);
    }

    let max_attempts = 5i32;
    let mut delivered_count = 0usize;

    for record in claimed {
        let next_attempt_num = record.attempts.saturating_add(1);
        let (prior_tg, prior_dc) = extract_delivery_state(record.payload_json.as_ref());

        let outcome = engine.dispatch_raw_html_for_channels(
            &record.event_type,
            &record.severity,
            &record.message,
            !prior_tg,
            !prior_dc,
        );

        let combined_tg = prior_tg || outcome.telegram_sent;
        let combined_dc = prior_dc || outcome.discord_sent;
        let updated_payload =
            with_delivery_state(record.payload_json.as_ref(), combined_tg, combined_dc);

        if outcome.status == "DELIVERED" {
            update_alert_queue_state_and_payload(
                &engine.db,
                record.id,
                "DELIVERED",
                next_attempt_num,
                now_ms(),
                Some(&updated_payload),
            )?;
            delivered_count += 1;
        } else if outcome.status == "SKIPPED" {
            let final_st = if prior_tg || prior_dc {
                delivered_count += 1;
                "DELIVERED"
            } else {
                "SKIPPED"
            };
            update_alert_queue_state_and_payload(
                &engine.db,
                record.id,
                final_st,
                next_attempt_num,
                now_ms(),
                Some(&updated_payload),
            )?;
        } else if outcome.queued_for_retry && next_attempt_num < max_attempts {
            let backoff_ms = 500_i64 * (1_i64 << (next_attempt_num.saturating_sub(1).min(6)));
            let next_retry = now_ms() + backoff_ms;
            let retry_status = if combined_tg || combined_dc {
                "PARTIAL"
            } else {
                "PENDING"
            };
            update_alert_queue_state_and_payload(
                &engine.db,
                record.id,
                retry_status,
                next_attempt_num,
                next_retry,
                Some(&updated_payload),
            )?;
        } else {
            update_alert_queue_state_and_payload(
                &engine.db,
                record.id,
                "FAILED",
                next_attempt_num,
                now_ms(),
                Some(&updated_payload),
            )?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn test_atomic_claim_due_pending_alerts_single_winner() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let msg = "<b>Asset:</b> <code>QCLAIM-USDC</code>";
        let payload = json!({ "symbol": "QCLAIM-USDC" });
        let qid =
            enqueue_pending_alert(&db, "ORDER_FILLED_OPEN", "INFO", msg, Some(&payload)).unwrap();
        assert!(qid > 0);

        let barrier = Arc::new(Barrier::new(2));
        let db1 = db.clone();
        let b1 = Arc::clone(&barrier);
        let h1 = thread::spawn(move || {
            b1.wait();
            claim_due_pending_alerts(&db1, 10).unwrap()
        });

        let db2 = db.clone();
        let b2 = Arc::clone(&barrier);
        let h2 = thread::spawn(move || {
            b2.wait();
            claim_due_pending_alerts(&db2, 10).unwrap()
        });

        let c1 = h1.join().unwrap();
        let c2 = h2.join().unwrap();
        assert_eq!(c1.len() + c2.len(), 1);
        let claimed = if !c1.is_empty() { &c1[0] } else { &c2[0] };
        assert_eq!(claimed.id, qid);
        assert_eq!(claimed.status, "DISPATCHING");

        let rows = db.list_recent_alerts(10).unwrap();
        let row = rows.into_iter().find(|r| r.id == qid).unwrap();
        assert_eq!(row.status, "DISPATCHING");
    }

    #[test]
    fn test_non_retriable_4xx_transitions_directly_to_failed() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_cl = Arc::clone(&calls);

        let srv = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                calls_cl.fetch_add(1, Ordering::SeqCst);
                let body = "{\"ok\":false,\"description\":\"Bad Request: chat not found\"}";
                let resp = format!(
                    "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });

        db.set_config_value("telegram_api_base_url", &format!("http://{addr}"), None)
            .unwrap();
        db.set_config_value("telegram_bot_token", "123456:TOKEN", None)
            .unwrap();
        db.set_config_value("telegram_chat_id", "999888777", None)
            .unwrap();

        let msg = "<b>Asset:</b> <code>QFAIL400-USDC</code>";
        let qid = enqueue_pending_alert(
            &db,
            "ORDER_FILLED_OPEN",
            "INFO",
            msg,
            Some(&json!({ "symbol": "QFAIL400-USDC" })),
        )
        .unwrap();
        assert!(qid > 0);

        let engine = AlertEngine::new(db.clone());
        let delivered = poll_and_dispatch_pending_alerts(&engine, 10).unwrap();
        assert_eq!(delivered, 0);
        let _ = srv.join();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let rows = db.list_recent_alerts(10).unwrap();
        let row = rows.into_iter().find(|r| r.id == qid).unwrap();
        assert_eq!(row.status, "FAILED");
        assert_eq!(row.attempts, 1);
    }

    #[test]
    fn test_partial_provider_only_retries_failed_channel() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();

        // Mock Telegram: always returns 200 OK; count requests to assert it is called ONLY ONCE
        let tg_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let tg_addr = tg_listener.local_addr().unwrap();
        tg_listener.set_nonblocking(false).unwrap();
        let tg_calls = Arc::new(AtomicUsize::new(0));
        let tg_calls_cl = Arc::clone(&tg_calls);
        let tg_srv = thread::spawn(move || {
            if let Ok((mut stream, _)) = tg_listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                tg_calls_cl.fetch_add(1, Ordering::SeqCst);
                let body = "{\"ok\":true,\"result\":{}}";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });

        // Mock Discord: 1st call returns 503 (retriable), 2nd call returns 204 No Content
        let dc_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let dc_addr = dc_listener.local_addr().unwrap();
        let dc_calls = Arc::new(AtomicUsize::new(0));
        let dc_calls_cl = Arc::clone(&dc_calls);
        let dc_srv = thread::spawn(move || {
            for attempt in 0..2 {
                if let Ok((mut stream, _)) = dc_listener.accept() {
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf);
                    dc_calls_cl.fetch_add(1, Ordering::SeqCst);
                    let resp = if attempt == 0 {
                        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    } else {
                        "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    };
                    let _ = stream.write_all(resp.as_bytes());
                }
            }
        });

        db.set_config_value("alert_provider", "both", None).unwrap();
        db.set_config_value("telegram_api_base_url", &format!("http://{tg_addr}"), None)
            .unwrap();
        db.set_config_value("telegram_bot_token", "123456:TOKEN", None)
            .unwrap();
        db.set_config_value("telegram_chat_id", "999888777", None)
            .unwrap();
        db.set_config_value(
            "discord_webhook_url",
            &format!("http://{dc_addr}/api/webhooks/1/abc"),
            None,
        )
        .unwrap();

        let msg = "<b>Asset:</b> <code>QPARTIAL-USDC</code>";
        let qid = enqueue_pending_alert(
            &db,
            "ORDER_FILLED_OPEN",
            "INFO",
            msg,
            Some(&json!({ "symbol": "QPARTIAL-USDC" })),
        )
        .unwrap();
        assert!(qid > 0);

        let engine = AlertEngine::new(db.clone());
        // Poll 1: Telegram succeeds, Discord 503 -> row moves to PARTIAL
        let delivered1 = poll_and_dispatch_pending_alerts(&engine, 10).unwrap();
        assert_eq!(delivered1, 0);

        let rows1 = db.list_recent_alerts(10).unwrap();
        let row1 = rows1.into_iter().find(|r| r.id == qid).unwrap();
        assert_eq!(row1.status, "PARTIAL");
        assert_eq!(row1.attempts, 1);

        // Force next_retry_ms due immediately and Poll 2: ONLY Discord is retried!
        update_alert_queue_status(&db, qid, "PARTIAL", 1, 0).unwrap();
        let delivered2 = poll_and_dispatch_pending_alerts(&engine, 10).unwrap();
        assert_eq!(delivered2, 1);

        let _ = tg_srv.join();
        let _ = dc_srv.join();
        assert_eq!(tg_calls.load(Ordering::SeqCst), 1);
        assert_eq!(dc_calls.load(Ordering::SeqCst), 2);

        let rows2 = db.list_recent_alerts(10).unwrap();
        let row2 = rows2.into_iter().find(|r| r.id == qid).unwrap();
        assert_eq!(row2.status, "DELIVERED");
        assert_eq!(row2.attempts, 2);
    }
}
