//! MariaDB `alert_queue` poller, persistent retry state machine, and delivery receipt tracking (`aegis-alerts::queue`).

use crate::engine::{
    is_duplicate_alert_within_window, record_alert_dedup_timestamp_with_ts, AlertEngine,
    DEFAULT_ALERT_DEDUP_WINDOW_MS,
};
use aegis_core::{now_ms, AegisError, AlertQueueRecord, Result};
use aegis_db::DatabaseManager;
use mysql::params;
use mysql::prelude::*;
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, OnceLock,
};
use std::thread;
use std::time::Duration;

const MAX_ALERT_RETRY_ATTEMPTS: i32 = 5;
const STALE_DISPATCH_TIMEOUT_MS: i64 = 60_000;
const STATE_UPDATE_RETRY_DELAYS_MS: [u64; 3] = [50, 150, 300];
const MAX_DELIVERY_RECEIPTS: usize = 2048;

#[derive(Debug, Clone, Copy)]
struct DeliveryReceipt {
    telegram_sent: bool,
    discord_sent: bool,
    all_delivered: bool,
}

#[derive(Default)]
struct DeliveryReceiptCache {
    order: VecDeque<(String, i64)>,
    entries: HashMap<(String, i64), DeliveryReceipt>,
}

impl DeliveryReceiptCache {
    fn get(&self, db_name: &str, alert_id: i64) -> Option<DeliveryReceipt> {
        self.entries.get(&(db_name.to_string(), alert_id)).copied()
    }

    fn insert(&mut self, db_name: &str, alert_id: i64, receipt: DeliveryReceipt) {
        let key = (db_name.to_string(), alert_id);
        if let Some(existing) = self.entries.get_mut(&key) {
            existing.telegram_sent |= receipt.telegram_sent;
            existing.discord_sent |= receipt.discord_sent;
            existing.all_delivered |= receipt.all_delivered;
            return;
        }
        while self.entries.len() >= MAX_DELIVERY_RECEIPTS {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
        self.order.push_back(key.clone());
        self.entries.insert(key, receipt);
    }

    #[cfg(test)]
    fn clear_db(&mut self, db_name: &str) {
        self.entries.retain(|(db, _), _| db != db_name);
        self.order.retain(|(db, _)| db != db_name);
    }
}

static DELIVERY_RECEIPTS: OnceLock<Mutex<DeliveryReceiptCache>> = OnceLock::new();

fn delivery_receipts() -> &'static Mutex<DeliveryReceiptCache> {
    DELIVERY_RECEIPTS.get_or_init(|| Mutex::new(DeliveryReceiptCache::default()))
}

pub(crate) fn record_delivery_receipt(
    db_name: &str,
    alert_id: i64,
    telegram_sent: bool,
    discord_sent: bool,
    all_delivered: bool,
) {
    if alert_id <= 0 {
        return;
    }
    delivery_receipts().lock().insert(
        db_name,
        alert_id,
        DeliveryReceipt {
            telegram_sent,
            discord_sent,
            all_delivered,
        },
    );
}

fn get_delivery_receipt(db_name: &str, alert_id: i64) -> Option<DeliveryReceipt> {
    if alert_id <= 0 {
        return None;
    }
    delivery_receipts().lock().get(db_name, alert_id)
}

#[cfg(test)]
pub(crate) fn clear_delivery_receipts_for_db(db_name: &str) {
    delivery_receipts().lock().clear_db(db_name);
}

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

fn recover_stale_dispatching_rows(tx: &mut mysql::Transaction<'_>, ts: i64) -> Result<()> {
    let stale_dispatch_cutoff = ts.saturating_sub(STALE_DISPATCH_TIMEOUT_MS);
    type StaleDispatchRow = (i64, String, Option<String>, i32, Option<i64>);
    let stale_rows: Vec<StaleDispatchRow> = tx
        .exec(
            r#"SELECT id, provider, CAST(payload_json AS CHAR), attempts, dispatched_at_ms
               FROM alert_queue
               WHERE status = 'DISPATCHING' AND updated_at_ms <= :stale
               ORDER BY id ASC
               FOR UPDATE"#,
            params! {
                "stale" => stale_dispatch_cutoff,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("claim_due_pending_alerts stale scan failed: {e}"))
        })?;

    for (alert_id, provider, raw_payload, attempts, dispatched_at_ms) in stale_rows {
        let next_attempts = attempts.saturating_add(1);
        let parsed_payload = raw_payload.and_then(|s| serde_json::from_str::<Value>(&s).ok());
        let (tg_sent, dc_sent, has_delivery_obj) =
            extract_delivery_state_with_presence(parsed_payload.as_ref());

        let provider_complete = match provider.as_str() {
            "telegram" => tg_sent,
            "discord" => dc_sent,
            _ => tg_sent && dc_sent,
        };

        let recovered_status = if next_attempts >= MAX_ALERT_RETRY_ATTEMPTS {
            "FAILED"
        } else if provider_complete {
            "DELIVERED"
        } else if dispatched_at_ms.is_some() && !has_delivery_obj {
            // Prior worker already persisted a dispatch timestamp with no partial breakdown;
            // finalize to DELIVERED rather than risking duplicate external notifications.
            "DELIVERED"
        } else if tg_sent || dc_sent {
            "PARTIAL"
        } else if dispatched_at_ms.is_some() {
            "DELIVERED"
        } else {
            "PENDING"
        };

        tx.exec_drop(
            r#"UPDATE alert_queue
               SET status = :st,
                   attempts = :att,
                   next_retry_ms = CASE WHEN :st IN ('PENDING', 'PARTIAL') THEN :now ELSE next_retry_ms END,
                   updated_at_ms = :now
               WHERE id = :id AND status = 'DISPATCHING'"#,
            params! {
                "st" => recovered_status,
                "att" => next_attempts,
                "now" => ts,
                "id" => alert_id,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!(
                "claim_due_pending_alerts stale recovery update failed for id={alert_id}: {e}"
            ))
        })?;
    }
    Ok(())
}

/// Atomically claims up to `limit` due `PENDING` / `PARTIAL` alerts using `FOR UPDATE SKIP LOCKED`
/// and transitions them to `DISPATCHING` inside a transaction before any network dispatch.
pub fn claim_due_pending_alerts(
    db: &DatabaseManager,
    limit: usize,
) -> Result<Vec<AlertQueueRecord>> {
    let ts = now_ms();
    let mut conn = db.get_conn()?;
    let mut tx = conn
        .start_transaction(mysql::TxOpts::default())
        .map_err(|e| AegisError::Database(format!("claim_due_pending_alerts tx failed: {e}")))?;

    // Recover orphaned DISPATCHING rows left behind by a crashed worker >60s ago
    recover_stale_dispatching_rows(&mut tx, ts)?;

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

/// Updates a claimed (`status = 'DISPATCHING'`) `alert_queue` row's status, attempts, `next_retry_ms`, and `payload_json`.
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
           WHERE id = :id AND status = 'DISPATCHING'"#,
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
    if conn.affected_rows() == 0 {
        tracing::warn!(
            alert_id = alert_id,
            target_status = status,
            "update_alert_queue_state_and_payload matched 0 DISPATCHING rows"
        );
    }
    Ok(())
}

fn update_alert_queue_state_with_retry<F>(
    db: &DatabaseManager,
    alert_id: i64,
    status: &str,
    attempts: i32,
    next_retry_ms: i64,
    payload_json: Option<&Value>,
    updater: &mut F,
) -> Result<()>
where
    F: FnMut(&DatabaseManager, i64, &str, i32, i64, Option<&Value>) -> Result<()>,
{
    let mut last_err = match updater(db, alert_id, status, attempts, next_retry_ms, payload_json) {
        Ok(()) => return Ok(()),
        Err(e) => e,
    };

    for delay_ms in STATE_UPDATE_RETRY_DELAYS_MS {
        thread::sleep(Duration::from_millis(delay_ms));
        match updater(db, alert_id, status, attempts, next_retry_ms, payload_json) {
            Ok(()) => return Ok(()),
            Err(e) => last_err = e,
        }
    }

    Err(last_err)
}

fn extract_delivery_state_with_presence(payload: Option<&Value>) -> (bool, bool, bool) {
    let delivery_obj = payload.and_then(|v| v.get("_delivery"));
    let has_delivery_obj = delivery_obj.is_some_and(Value::is_object);
    let tg = delivery_obj
        .and_then(|d| d.get("telegram_sent"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let dc = delivery_obj
        .and_then(|d| d.get("discord_sent"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    (tg, dc, has_delivery_obj)
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
    poll_and_dispatch_pending_alerts_with_updater(
        engine,
        batch_limit,
        update_alert_queue_state_and_payload,
    )
}

fn poll_and_dispatch_pending_alerts_with_updater<F>(
    engine: &AlertEngine,
    batch_limit: usize,
    mut updater: F,
) -> Result<usize>
where
    F: FnMut(&DatabaseManager, i64, &str, i32, i64, Option<&Value>) -> Result<()>,
{
    let claimed = claim_due_pending_alerts(&engine.db, batch_limit)?;
    if claimed.is_empty() {
        return Ok(0);
    }

    let db_name = engine.db.db_name();
    let mut delivered_count = 0usize;

    for record in claimed {
        let next_attempt_num = record.attempts.saturating_add(1);
        let (payload_tg, payload_dc, has_delivery_obj) =
            extract_delivery_state_with_presence(record.payload_json.as_ref());
        let receipt_opt = get_delivery_receipt(db_name, record.id);
        let prior_tg = payload_tg || receipt_opt.is_some_and(|r| r.telegram_sent);
        let prior_dc = payload_dc || receipt_opt.is_some_and(|r| r.discord_sent);
        let already_all_delivered = receipt_opt.is_some_and(|r| r.all_delivered);

        if already_all_delivered {
            let updated_payload =
                with_delivery_state(record.payload_json.as_ref(), prior_tg, prior_dc);
            if let Err(err) = engine.db.mark_alert_queue_dispatched(record.id, now_ms()) {
                tracing::warn!(
                    alert_id = record.id,
                    error = %err,
                    "Failed to mark alert_queue dispatched_at_ms from in-memory receipt"
                );
            }
            match update_alert_queue_state_with_retry(
                &engine.db,
                record.id,
                "DELIVERED",
                next_attempt_num,
                now_ms(),
                Some(&updated_payload),
                &mut updater,
            ) {
                Ok(()) => delivered_count += 1,
                Err(err) => {
                    tracing::error!(
                        alert_id = record.id,
                        error = %err,
                        "Failed to finalize DELIVERED state from in-memory receipt; continuing batch"
                    );
                }
            }
            continue;
        }

        if !has_delivery_obj
            && !prior_tg
            && !prior_dc
            && engine
                .db
                .get_alert_queue_dispatched_at_ms(record.id)
                .ok()
                .flatten()
                .is_some()
        {
            match update_alert_queue_state_with_retry(
                &engine.db,
                record.id,
                "DELIVERED",
                next_attempt_num,
                now_ms(),
                record.payload_json.as_ref(),
                &mut updater,
            ) {
                Ok(()) => delivered_count += 1,
                Err(err) => {
                    tracing::error!(
                        alert_id = record.id,
                        error = %err,
                        "Failed to finalize DELIVERED state from DB dispatch receipt; continuing batch"
                    );
                }
            }
            continue;
        }

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

        let (target_status, target_next_retry, counts_as_delivered) =
            if outcome.status == "DELIVERED" {
                ("DELIVERED", now_ms(), true)
            } else if outcome.status == "SKIPPED" {
                if prior_tg || prior_dc {
                    ("DELIVERED", now_ms(), true)
                } else {
                    ("SKIPPED", now_ms(), false)
                }
            } else if outcome.queued_for_retry && next_attempt_num < MAX_ALERT_RETRY_ATTEMPTS {
                let backoff_ms = 500_i64 * (1_i64 << (next_attempt_num.saturating_sub(1).min(6)));
                let retry_status = if combined_tg || combined_dc {
                    "PARTIAL"
                } else {
                    "PENDING"
                };
                (retry_status, now_ms() + backoff_ms, false)
            } else {
                ("FAILED", now_ms(), false)
            };

        if combined_tg || combined_dc || target_status == "DELIVERED" {
            record_delivery_receipt(
                db_name,
                record.id,
                combined_tg,
                combined_dc,
                target_status == "DELIVERED",
            );
            if let Err(err) = engine.db.mark_alert_queue_dispatched(record.id, now_ms()) {
                tracing::warn!(
                    alert_id = record.id,
                    error = %err,
                    "Failed to record alert_queue dispatched_at_ms; retaining in-memory receipt"
                );
            }
        }

        match update_alert_queue_state_with_retry(
            &engine.db,
            record.id,
            target_status,
            next_attempt_num,
            target_next_retry,
            Some(&updated_payload),
            &mut updater,
        ) {
            Ok(()) => {
                if counts_as_delivered {
                    delivered_count += 1;
                }
            }
            Err(err) => {
                tracing::error!(
                    alert_id = record.id,
                    target_status = target_status,
                    error = %err,
                    "Failed to update alert_queue state after retries; preserving receipt and continuing batch"
                );
            }
        }
    }

    Ok(delivered_count)
}

/// Thread-safe atomic counters tracking alert delivery outcomes for an [`AlertEngine`] instance.
#[derive(Clone, Default, Debug)]
pub struct AlertDeliveryCounters {
    delivered_count: Arc<AtomicUsize>,
    failed_count: Arc<AtomicUsize>,
}

impl AlertDeliveryCounters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn delivered_count(&self) -> usize {
        self.delivered_count.load(Ordering::SeqCst)
    }

    pub fn failed_count(&self) -> usize {
        self.failed_count.load(Ordering::SeqCst)
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
    use aegis_db::AlertQueueInsertInput;
    use aegis_test_support::mock_http::{MockHttpResponse, MockHttpServer};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Barrier;
    use std::time::Instant;

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

    #[test]
    fn test_r14_l14_stale_dispatching_recovery_respects_dispatch_receipt_and_fails_exhausted() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let tg_srv = MockHttpServer::start_static(200, &json!({ "ok": true, "result": {} }));
        let dc_srv = MockHttpServer::start(|_| MockHttpResponse::text(204, "text/plain", ""));

        db.set_config_value("alert_provider", "telegram", None)
            .unwrap();
        db.set_config_value("telegram_api_base_url", &tg_srv.base_url(), None)
            .unwrap();
        db.set_config_value("telegram_bot_token", "123456:TOKEN", None)
            .unwrap();
        db.set_config_value("telegram_chat_id", "999888777", None)
            .unwrap();
        db.set_config_value(
            "discord_webhook_url",
            &dc_srv.url("/api/webhooks/1/abc"),
            None,
        )
        .unwrap();

        let stale_ts = now_ms() - 120_000;

        // 1. Stale DISPATCHING row with dispatched_at_ms set (simulating crash after HTTP send,
        //    followed by process restart where in-memory receipts are empty).
        let q_dispatched = db
            .insert_alert_queue_record_full(&AlertQueueInsertInput {
                provider: "telegram",
                event_type: "ORDER_FILLED_OPEN",
                severity: "INFO",
                message: "<b>Asset:</b> <code>R14L14A-USDC</code>",
                payload_json: Some(&json!({ "symbol": "R14L14A-USDC" })),
                status: "DISPATCHING",
                attempts: 1,
                next_retry_ms: stale_ts,
                dispatched_at_ms: Some(stale_ts),
            })
            .unwrap();

        // 2. Stale DISPATCHING row already at attempts=4 with NO dispatched_at_ms -> must transition
        //    directly to FAILED (attempts=5 >= max_attempts) without sending HTTP.
        let q_exhausted = db
            .insert_alert_queue_record_full(&AlertQueueInsertInput {
                provider: "telegram",
                event_type: "ORDER_FILLED_OPEN",
                severity: "INFO",
                message: "<b>Asset:</b> <code>R14L14B-USDC</code>",
                payload_json: Some(&json!({ "symbol": "R14L14B-USDC" })),
                status: "DISPATCHING",
                attempts: 4,
                next_retry_ms: stale_ts,
                dispatched_at_ms: None,
            })
            .unwrap();

        // 3. Stale DISPATCHING row from a PARTIAL retry (Telegram already sent, Discord pending).
        db.set_config_value("alert_provider", "both", None).unwrap();
        let q_partial_stale = db
            .insert_alert_queue_record_full(&AlertQueueInsertInput {
                provider: "both",
                event_type: "ORDER_FILLED_OPEN",
                severity: "INFO",
                message: "<b>Asset:</b> <code>R14L14C-USDC</code>",
                payload_json: Some(&json!({
                    "symbol": "R14L14C-USDC",
                    "_delivery": { "telegram_sent": true, "discord_sent": false }
                })),
                status: "DISPATCHING",
                attempts: 1,
                next_retry_ms: stale_ts,
                dispatched_at_ms: Some(stale_ts),
            })
            .unwrap();

        {
            let mut conn = db.get_conn().unwrap();
            conn.exec_drop(
                "UPDATE alert_queue SET updated_at_ms = :stale WHERE id IN (:a, :b, :c)",
                params! {
                    "stale" => stale_ts,
                    "a" => q_dispatched,
                    "b" => q_exhausted,
                    "c" => q_partial_stale,
                },
            )
            .unwrap();
        }
        clear_delivery_receipts_for_db(db.db_name());

        let engine = AlertEngine::new(db.clone());
        let delivered = poll_and_dispatch_pending_alerts(&engine, 10).unwrap();
        assert_eq!(delivered, 1);

        // Zero duplicate Telegram requests (q_dispatched had DB receipt, q_exhausted failed,
        // q_partial_stale skipped Telegram via _delivery).
        assert_eq!(tg_srv.request_count(), 0);
        // Only Discord leg of q_partial_stale was dispatched.
        assert_eq!(dc_srv.request_count(), 1);

        let rows = db.list_recent_alerts(10).unwrap();
        let row_dispatched = rows.iter().find(|r| r.id == q_dispatched).unwrap();
        assert_eq!(row_dispatched.status, "DELIVERED");
        assert_eq!(row_dispatched.attempts, 2);

        let row_exhausted = rows.iter().find(|r| r.id == q_exhausted).unwrap();
        assert_eq!(row_exhausted.status, "FAILED");
        assert_eq!(row_exhausted.attempts, 5);

        let row_partial = rows.iter().find(|r| r.id == q_partial_stale).unwrap();
        assert_eq!(row_partial.status, "DELIVERED");
        assert_eq!(row_partial.attempts, 3);
    }

    #[test]
    fn test_r14_l14_batch_continues_after_db_update_failure_and_memory_receipt_prevents_dup() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let tg_srv = MockHttpServer::start_static(200, &json!({ "ok": true, "result": {} }));

        db.set_config_value("alert_provider", "telegram", None)
            .unwrap();
        db.set_config_value("telegram_api_base_url", &tg_srv.base_url(), None)
            .unwrap();
        db.set_config_value("telegram_bot_token", "123456:TOKEN", None)
            .unwrap();
        db.set_config_value("telegram_chat_id", "999888777", None)
            .unwrap();

        let q1 = db
            .insert_alert_queue_record_full(&AlertQueueInsertInput {
                provider: "telegram",
                event_type: "ORDER_FILLED_OPEN",
                severity: "INFO",
                message: "<b>Asset:</b> <code>BATCH1-USDC</code>",
                payload_json: Some(&json!({ "symbol": "BATCH1-USDC" })),
                status: "PENDING",
                attempts: 0,
                next_retry_ms: 0,
                dispatched_at_ms: None,
            })
            .unwrap();
        let q2 = db
            .insert_alert_queue_record_full(&AlertQueueInsertInput {
                provider: "telegram",
                event_type: "ORDER_FILLED_OPEN",
                severity: "INFO",
                message: "<b>Asset:</b> <code>BATCH2-USDC</code>",
                payload_json: Some(&json!({ "symbol": "BATCH2-USDC" })),
                status: "PENDING",
                attempts: 0,
                next_retry_ms: 0,
                dispatched_at_ms: None,
            })
            .unwrap();

        let engine = AlertEngine::new(db.clone());
        let mut q1_update_attempts = 0usize;
        let started = Instant::now();
        let delivered = poll_and_dispatch_pending_alerts_with_updater(
            &engine,
            10,
            |d, id, st, att, next, pay| {
                if id == q1 {
                    q1_update_attempts += 1;
                    Err(AegisError::Database(
                        "simulated transient MariaDB lock timeout".to_string(),
                    ))
                } else {
                    update_alert_queue_state_and_payload(d, id, st, att, next, pay)
                }
            },
        )
        .unwrap();
        let elapsed = started.elapsed();

        // q1 tried initial + 3 retries (50+150+300ms = 500ms) and failed, while q2 STILL completed.
        assert_eq!(q1_update_attempts, 4);
        assert!(elapsed >= Duration::from_millis(450));
        assert_eq!(delivered, 1);
        assert_eq!(tg_srv.request_count(), 2);

        let rows1 = db.list_recent_alerts(10).unwrap();
        assert_eq!(
            rows1.iter().find(|r| r.id == q1).unwrap().status,
            "DISPATCHING"
        );
        assert_eq!(
            rows1.iter().find(|r| r.id == q2).unwrap().status,
            "DELIVERED"
        );

        // Simulate stale recovery where even dispatched_at_ms is NULL in DB, relying solely on the
        // bounded in-memory delivery receipt set to prevent duplicate HTTP send of q1.
        {
            let stale_ts = now_ms() - 120_000;
            let mut conn = db.get_conn().unwrap();
            conn.exec_drop(
                "UPDATE alert_queue SET dispatched_at_ms = NULL, updated_at_ms = :stale WHERE id = :id",
                params! { "stale" => stale_ts, "id" => q1 },
            )
            .unwrap();
        }

        let recovered_delivered = poll_and_dispatch_pending_alerts(&engine, 10).unwrap();
        assert_eq!(recovered_delivered, 1);
        assert_eq!(
            tg_srv.request_count(),
            2,
            "in-memory receipt must prevent duplicate Telegram send on recovery"
        );
        let rows2 = db.list_recent_alerts(10).unwrap();
        assert_eq!(
            rows2.iter().find(|r| r.id == q1).unwrap().status,
            "DELIVERED"
        );
    }

    #[test]
    fn test_r14_l14_update_state_requires_dispatching_status() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let qid = enqueue_pending_alert(
            &db,
            "ORDER_FILLED_OPEN",
            "INFO",
            "<b>Asset:</b> <code>GUARD-USDC</code>",
            Some(&json!({ "symbol": "GUARD-USDC" })),
        )
        .unwrap();

        // Row is PENDING, not DISPATCHING -> guarded UPDATE must leave status untouched.
        update_alert_queue_state_and_payload(&db, qid, "FAILED", 1, 0, None).unwrap();
        let row_pending = db
            .list_recent_alerts(10)
            .unwrap()
            .into_iter()
            .find(|r| r.id == qid)
            .unwrap();
        assert_eq!(row_pending.status, "PENDING");

        let claimed = claim_due_pending_alerts(&db, 10).unwrap();
        assert_eq!(claimed.len(), 1);
        update_alert_queue_state_and_payload(&db, qid, "DELIVERED", 1, 0, None).unwrap();
        let row_delivered = db
            .list_recent_alerts(10)
            .unwrap()
            .into_iter()
            .find(|r| r.id == qid)
            .unwrap();
        assert_eq!(row_delivered.status, "DELIVERED");
    }

    #[test]
    fn test_r14_l1_alert_delivery_counters() {
        let counters = AlertDeliveryCounters::new();
        assert_eq!(counters.delivered_count(), 0);
        assert_eq!(counters.failed_count(), 0);
        assert_eq!(counters.failed_permanently_count(), 0);
        counters.record_delivered();
        counters.record_delivered();
        counters.record_failed();
        assert_eq!(counters.delivered_count(), 2);
        assert_eq!(counters.failed_count(), 1);
        assert_eq!(counters.failed_permanently_count(), 1);
    }
}
