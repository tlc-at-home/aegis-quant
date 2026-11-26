//! Tracing subscriber initialization, Tokio inter-task telemetry channels, and MariaDB `telemetry_logs` engine (`aegis-telemetry::engine`).

use aegis_core::{now_ms, ConfigMap, Result, TelemetryLogRecord};
use aegis_db::{DatabaseManager, TelemetryInsertInput};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Condvar, Mutex,
};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};

/// Maximum number of queued telemetry events persisted per single MariaDB transaction.
const MAX_TELEMETRY_BATCH_SIZE: usize = 32;

/// Default 5-minute (`300_000 ms`) summary window for repeated identical `WARN` deduplication (`R14-W4`).
pub const DEFAULT_WARN_DEDUP_WINDOW_MS: i64 = 300_000;

/// Default maximum tracked keys in [`RepeatedWarnDeduplicator`] (`R14-W4`).
pub const DEFAULT_WARN_DEDUP_MAX_KEYS: usize = 256;

/// Outcome of recording an occurrence in [`RepeatedWarnDeduplicator`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarnDedupDecision {
    /// First occurrence for this key (or first occurrence after a clear / signature transition).
    EmitFirst,
    /// Identical repeat within the active suppression window; suppress duplicate `WARN` emission.
    Suppress { suppressed_repeats: u64 },
    /// Summary window elapsed while condition remains active; emit summary with `(suppressed_repeats = N)`.
    EmitSummary { suppressed_repeats: u64 },
}

impl WarnDedupDecision {
    /// Returns `true` when the caller should emit a `WARN` log (`EmitFirst` or `EmitSummary`).
    pub fn should_emit(self) -> bool {
        matches!(self, Self::EmitFirst | Self::EmitSummary { .. })
    }

    /// Returns the repeat counter associated with this decision (`0` on `EmitFirst`).
    pub fn suppressed_repeats(self) -> u64 {
        match self {
            Self::EmitFirst => 0,
            Self::Suppress { suppressed_repeats } | Self::EmitSummary { suppressed_repeats } => {
                suppressed_repeats
            }
        }
    }
}

#[derive(Debug, Clone)]
struct WarnDedupEntry {
    signature: String,
    window_start_ms: i64,
    last_seen_ms: i64,
    suppressed_repeats: u64,
}

/// Bounded per-key repeated-`WARN` deduplicator (`R14-W4`).
///
/// Emits on the first occurrence of a key, suppresses identical repeats within `window_ms`
/// (`300_000 ms` / 5 minutes by default) while incrementing a counter, and emits
/// `(suppressed_repeats = N)` when the summary interval elapses or when the condition clears.
pub struct RepeatedWarnDeduplicator {
    window_ms: i64,
    max_keys: usize,
    entries: Mutex<HashMap<String, WarnDedupEntry>>,
}

impl Default for RepeatedWarnDeduplicator {
    fn default() -> Self {
        Self::new(DEFAULT_WARN_DEDUP_WINDOW_MS, DEFAULT_WARN_DEDUP_MAX_KEYS)
    }
}

impl RepeatedWarnDeduplicator {
    pub fn new(window_ms: i64, max_keys: usize) -> Self {
        Self {
            window_ms: window_ms.max(1),
            max_keys: max_keys.max(1),
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Observes an occurrence of `key` at `now_ms_val`.
    pub fn observe(&self, key: &str, now_ms_val: i64) -> WarnDedupDecision {
        self.observe_with_signature(key, "", now_ms_val).0
    }

    /// Observes an occurrence of `key` with sub-fingerprint `signature` at `now_ms_val`.
    /// If `key` was active under a different `signature` with suppressed repeats, returns the
    /// flushed repeat count from the prior signature as the second tuple element.
    pub fn observe_with_signature(
        &self,
        key: &str,
        signature: &str,
        now_ms_val: i64,
    ) -> (WarnDedupDecision, Option<u64>) {
        let mut guard = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = guard.get_mut(key) {
            if entry.signature != signature {
                let flushed = (entry.suppressed_repeats > 0).then_some(entry.suppressed_repeats);
                entry.signature = signature.to_string();
                entry.window_start_ms = now_ms_val;
                entry.last_seen_ms = now_ms_val;
                entry.suppressed_repeats = 0;
                return (WarnDedupDecision::EmitFirst, flushed);
            }
            entry.last_seen_ms = now_ms_val;
            if now_ms_val < entry.window_start_ms {
                entry.window_start_ms = now_ms_val;
            }
            if now_ms_val.saturating_sub(entry.window_start_ms) >= self.window_ms {
                let suppressed = entry.suppressed_repeats;
                entry.window_start_ms = now_ms_val;
                entry.suppressed_repeats = 0;
                return (
                    WarnDedupDecision::EmitSummary {
                        suppressed_repeats: suppressed,
                    },
                    None,
                );
            }
            entry.suppressed_repeats = entry.suppressed_repeats.saturating_add(1);
            return (
                WarnDedupDecision::Suppress {
                    suppressed_repeats: entry.suppressed_repeats,
                },
                None,
            );
        }

        if guard.len() >= self.max_keys {
            if let Some(oldest_key) = guard
                .iter()
                .min_by_key(|(_, v)| v.last_seen_ms)
                .map(|(k, _)| k.clone())
            {
                guard.remove(&oldest_key);
            }
        }
        guard.insert(
            key.to_string(),
            WarnDedupEntry {
                signature: signature.to_string(),
                window_start_ms: now_ms_val,
                last_seen_ms: now_ms_val,
                suppressed_repeats: 0,
            },
        );
        (WarnDedupDecision::EmitFirst, None)
    }

    /// Clears `key` when the underlying condition resolves. Returns `Some(suppressed_repeats)` if
    /// one or more identical repeats had been suppressed since the last emission.
    pub fn clear(&self, key: &str) -> Option<u64> {
        let mut guard = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let removed = guard.remove(key)?;
        (removed.suppressed_repeats > 0).then_some(removed.suppressed_repeats)
    }

    /// Returns the current number of tracked deduplication keys (`<= max_keys`).
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Returns `true` when no active deduplication keys are tracked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Structured telemetry event sent over `tokio::sync::mpsc` to the MariaDB `telemetry_logs` writer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryEvent {
    pub level: String,
    pub component: String,
    pub message: String,
    pub details: Option<Value>,
}

struct QueuedTelemetryEvent {
    event: TelemetryEvent,
    created_at_ms: i64,
}

impl QueuedTelemetryEvent {
    fn into_insert_input(self) -> TelemetryInsertInput {
        TelemetryInsertInput {
            level: self.event.level,
            component: self.event.component,
            message: self.event.message,
            details: self.event.details,
            created_at_ms: Some(self.created_at_ms),
        }
    }
}

struct WriterThreadState {
    done_pair: (Mutex<bool>, Condvar),
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl WriterThreadState {
    fn new() -> Self {
        Self {
            done_pair: (Mutex::new(false), Condvar::new()),
            handle: Mutex::new(None),
        }
    }

    fn mark_done(&self) {
        let (lock, cvar) = &self.done_pair;
        let mut done = lock.lock().unwrap_or_else(|e| e.into_inner());
        *done = true;
        cvar.notify_all();
    }
}

#[cfg(test)]
type BeforeDrainHook = Arc<dyn Fn() + Send + Sync>;

/// Unified v2.0 Telemetry Engine & Tokio Broadcast/MPSC Bus backed by MariaDB `telemetry_logs`.
#[derive(Clone)]
pub struct TelemetryBus {
    pub db: DatabaseManager,
    tx_slot: Arc<Mutex<Option<mpsc::Sender<QueuedTelemetryEvent>>>>,
    pub snapshot_tx: broadcast::Sender<Value>,
    pub config_tx: watch::Sender<ConfigMap>,
    pub config_rx: watch::Receiver<ConfigMap>,
    dropped_events_counter: Arc<AtomicU64>,
    writer_state: Arc<WriterThreadState>,
}

pub type TelemetryEngine = TelemetryBus;

impl TelemetryBus {
    pub fn new(db: DatabaseManager) -> Self {
        let initial_cfg = db.load_config_or_default();
        let queue_cap = initial_cfg
            .get_usize("telemetry_queue_capacity", 2048)
            .max(64);
        Self::with_capacity_internal(
            db,
            initial_cfg,
            queue_cap,
            #[cfg(test)]
            None,
        )
    }

    fn with_capacity_internal(
        db: DatabaseManager,
        initial_cfg: ConfigMap,
        queue_cap: usize,
        #[cfg(test)] before_drain_hook: Option<BeforeDrainHook>,
    ) -> Self {
        let (event_tx, mut event_rx) = mpsc::channel::<QueuedTelemetryEvent>(queue_cap.max(1));
        let (snapshot_tx, _) = broadcast::channel::<Value>(128);
        let (config_tx, config_rx) = watch::channel(initial_cfg);

        let writer_db = db.clone();
        let writer_state = Arc::new(WriterThreadState::new());
        let writer_state_bg = Arc::clone(&writer_state);

        let spawn_res = std::thread::Builder::new()
            .name("aegis-telemetry-writer".into())
            .spawn(move || {
                while let Some(first) = event_rx.blocking_recv() {
                    #[cfg(test)]
                    if let Some(ref hook) = before_drain_hook {
                        hook();
                    }
                    let mut batch = Vec::with_capacity(MAX_TELEMETRY_BATCH_SIZE);
                    batch.push(first.into_insert_input());
                    while batch.len() < MAX_TELEMETRY_BATCH_SIZE {
                        match event_rx.try_recv() {
                            Ok(ev) => batch.push(ev.into_insert_input()),
                            Err(_) => break,
                        }
                    }
                    if let Err(err) = writer_db.log_telemetry_batch(&batch) {
                        tracing::warn!(
                            batch_len = batch.len(),
                            error = %err,
                            "TelemetryBus writer thread failed to persist telemetry batch"
                        );
                    }
                }
                writer_state_bg.mark_done();
            });

        let tx_slot = match spawn_res {
            Ok(handle) => {
                *writer_state
                    .handle
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(handle);
                Arc::new(Mutex::new(Some(event_tx)))
            }
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "Failed to spawn dedicated aegis-telemetry-writer OS thread; telemetry events will be dropped"
                );
                writer_state.mark_done();
                Arc::new(Mutex::new(None))
            }
        };

        Self {
            db,
            tx_slot,
            snapshot_tx,
            config_tx,
            config_rx,
            dropped_events_counter: Arc::new(AtomicU64::new(0)),
            writer_state,
        }
    }

    #[cfg(test)]
    fn new_with_capacity_and_hook_for_tests(
        db: DatabaseManager,
        queue_cap: usize,
        before_drain_hook: Option<BeforeDrainHook>,
    ) -> Self {
        let initial_cfg = db.load_config_or_default();
        Self::with_capacity_internal(db, initial_cfg, queue_cap, before_drain_hook)
    }

    /// Returns the total number of telemetry events dropped due to a full or closed queue.
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events_counter.load(Ordering::SeqCst)
    }

    /// Compatibility alias returning [`Self::dropped_events`] as `usize`.
    pub fn dropped_event_count(&self) -> usize {
        self.dropped_events() as usize
    }

    /// Signals the background OS writer thread to drain any buffered events, flush them to MariaDB,
    /// and join cleanly within `timeout`. Idempotent across repeated calls or cloned handles.
    pub fn shutdown_and_flush(&self, timeout: Duration) {
        {
            let mut guard = self.tx_slot.lock().unwrap_or_else(|e| e.into_inner());
            drop(guard.take());
        }

        let (lock, cvar) = &self.writer_state.done_pair;
        let mut done = lock.lock().unwrap_or_else(|e| e.into_inner());
        if !*done {
            let wait_res = cvar
                .wait_timeout_while(done, timeout, |is_done| !*is_done)
                .unwrap_or_else(|e| e.into_inner());
            done = wait_res.0;
        }
        let finished = *done;
        drop(done);

        if finished {
            let handle_opt = self
                .writer_state
                .handle
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            if let Some(handle) = handle_opt {
                let _ = handle.join();
            }
        } else {
            tracing::warn!(
                timeout_ms = timeout.as_millis() as u64,
                "TelemetryBus writer thread did not finish flushing within shutdown timeout"
            );
        }
    }

    pub fn reload_config(&self) {
        if let Ok(cfg) = self.db.load_config() {
            let _ = self.config_tx.send(cfg);
        }
    }

    /// Publishes an asynchronous telemetry event to the background MariaDB OS writer thread
    /// and SSE broadcast stream. Never performs blocking MariaDB I/O on the caller thread.
    pub fn publish_event(
        &self,
        level: &str,
        component: &str,
        message: &str,
        details: Option<Value>,
    ) {
        let item = QueuedTelemetryEvent {
            event: TelemetryEvent {
                level: level.to_string(),
                component: component.to_string(),
                message: message.to_string(),
                details: details.clone(),
            },
            created_at_ms: now_ms(),
        };

        let send_res = {
            let guard = self.tx_slot.lock().unwrap_or_else(|e| e.into_inner());
            match guard.as_ref() {
                Some(tx) => tx.try_send(item),
                None => Err(mpsc::error::TrySendError::Closed(item)),
            }
        };

        if let Err(err) = send_res {
            let dropped = self.dropped_events_counter.fetch_add(1, Ordering::SeqCst) + 1;
            if dropped == 1 || dropped.is_power_of_two() || dropped.is_multiple_of(100) {
                tracing::warn!(
                    component = component,
                    dropped_total = dropped,
                    reason = %err,
                    "TelemetryBus event queue full or closed; dropping telemetry event"
                );
            }
        }

        if let Some(d) = details {
            let _ = self.snapshot_tx.send(d);
        }
    }

    /// Writes a telemetry record synchronously to MariaDB `telemetry_logs` and broadcasts to SSE subscribers.
    pub fn log_sync(
        &self,
        level: &str,
        component: &str,
        message: &str,
        details: Option<Value>,
    ) -> Result<()> {
        if let Some(ref d) = details {
            let _ = self.snapshot_tx.send(d.clone());
        }
        self.db.log_telemetry(level, component, message, details)
    }

    /// Fetches the most recent `limit` operational telemetry rows from MariaDB `telemetry_logs`.
    pub fn list_recent_logs(&self, limit: usize) -> Result<Vec<TelemetryLogRecord>> {
        self.db.list_recent_telemetry(limit)
    }

    /// Publishes a deduplicated `WARN` event via `dedup` (`R14-W4`).
    /// Emits immediately on the first occurrence, suppresses identical repeats within the window,
    /// and appends `(suppressed_repeats = N)` when the summary window elapses.
    pub fn publish_warn_dedup(
        &self,
        dedup: &RepeatedWarnDeduplicator,
        dedup_key: &str,
        component: &str,
        message: &str,
        details: Option<Value>,
        now_ms_val: i64,
    ) -> WarnDedupDecision {
        let decision = dedup.observe(dedup_key, now_ms_val);
        match decision {
            WarnDedupDecision::EmitFirst => {
                self.publish_event("WARN", component, message, details);
            }
            WarnDedupDecision::Suppress { .. } => {}
            WarnDedupDecision::EmitSummary { suppressed_repeats } => {
                let summary_msg = format!("{message} (suppressed_repeats = {suppressed_repeats})");
                self.publish_event("WARN", component, &summary_msg, details);
            }
        }
        decision
    }

    /// Clears `dedup_key` in `dedup` and, if any repeats had been suppressed, emits a summary
    /// `INFO` clearance event including `(suppressed_repeats = N)` (`R14-W4`).
    pub fn clear_warn_dedup(
        &self,
        dedup: &RepeatedWarnDeduplicator,
        dedup_key: &str,
        component: &str,
        clear_message: &str,
        details: Option<Value>,
    ) -> Option<u64> {
        let suppressed = dedup.clear(dedup_key)?;
        let msg = format!("{clear_message} (suppressed_repeats = {suppressed})");
        self.publish_event("INFO", component, &msg, details);
        Some(suppressed)
    }
}

/// Initializes the global `tracing_subscriber` with `RUST_LOG` environment filter (default `"info"`).
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

/// Direct helper to record a sanitized telemetry event into MariaDB `telemetry_logs`.
pub fn log_telemetry_event(
    db: &DatabaseManager,
    level: &str,
    component: &str,
    message: &str,
    details: Option<Value>,
) -> Result<()> {
    db.log_telemetry(level, component, message, details)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Instant;

    #[tokio::test(flavor = "current_thread")]
    async fn test_publish_event_never_blocks_current_thread_reactor_and_persists_batched_rows() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let bus = TelemetryBus::new(db.clone());
        let mut rx = bus.snapshot_tx.subscribe();

        // Burst 40 events (> 32 max single batch) directly from a single-threaded Tokio reactor.
        let started = Instant::now();
        for i in 0..40usize {
            let msg = if i == 0 {
                "startup token gAAAAABsecret_token_value".to_string()
            } else {
                format!("telemetry-event-{i:02}")
            };
            bus.publish_event("INFO", "telemetry.test", &msg, Some(json!({ "seq": i })));
        }
        let publish_elapsed = started.elapsed();
        assert!(
            publish_elapsed < Duration::from_millis(250),
            "publish_event must remain non-blocking on single-threaded reactor, took {publish_elapsed:?}"
        );

        // Verify reactor makes forward progress immediately.
        tokio::time::timeout(Duration::from_millis(250), tokio::task::yield_now())
            .await
            .expect("current_thread reactor should yield cleanly");

        let first_broadcast = rx.try_recv().expect("SSE broadcast should receive event");
        assert_eq!(first_broadcast["seq"], 0);

        let bus_flush = bus.clone();
        tokio::task::spawn_blocking(move || bus_flush.shutdown_and_flush(Duration::from_secs(10)))
            .await
            .unwrap();

        assert_eq!(bus.dropped_events(), 0);
        assert_eq!(bus.dropped_event_count(), 0);

        let logs = bus.list_recent_logs(100).unwrap();
        assert_eq!(logs.len(), 40);
        assert!(
            logs.iter()
                .all(|l| !l.message.contains("gAAAAABsecret_token_value")),
            "batch persistence must preserve secret redaction"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_publish_event_overflow_increments_dropped_events_without_sync_db_write() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();

        // Deterministic gate pausing the OS writer thread right after popping item #1.
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let gate_hook = Arc::clone(&gate);
        let hook_once = Arc::new(Mutex::new(true));
        let hook: BeforeDrainHook = Arc::new(move || {
            let mut run = hook_once.lock().unwrap();
            if !*run {
                return;
            }
            *run = false;
            drop(run);

            let (lock, cvar) = &*gate_hook;
            let mut state = lock.lock().unwrap();
            state.0 = true; // entered hook
            cvar.notify_all();
            while !state.1 {
                state = cvar.wait(state).unwrap();
            }
        });

        let bus = TelemetryBus::new_with_capacity_and_hook_for_tests(db.clone(), 2, Some(hook));

        // 1. Send first event and wait until writer pops it and pauses before DB write.
        bus.publish_event("INFO", "overflow.test", "item-1", None);
        {
            let (lock, cvar) = &*gate;
            let mut state = lock.lock().unwrap();
            let res = cvar
                .wait_timeout_while(state, Duration::from_secs(5), |s| !s.0)
                .unwrap();
            state = res.0;
            assert!(state.0, "writer thread did not reach before-drain hook");
        }

        // 2. Fill bounded channel capacity (2) + overflow by 2 extra events.
        bus.publish_event("INFO", "overflow.test", "item-2", None);
        bus.publish_event("INFO", "overflow.test", "item-3", None);
        bus.publish_event("WARN", "overflow.test", "drop-4", None);
        bus.publish_event("WARN", "overflow.test", "drop-5", None);

        assert_eq!(bus.dropped_events(), 2);
        // Must NEVER have written synchronously to MariaDB on the caller thread.
        assert_eq!(db.list_recent_telemetry(20).unwrap().len(), 0);

        // 3. Unblock writer thread and flush cleanly.
        {
            let (lock, cvar) = &*gate;
            let mut state = lock.lock().unwrap();
            state.1 = true;
            cvar.notify_all();
        }

        let bus_flush = bus.clone();
        tokio::task::spawn_blocking(move || bus_flush.shutdown_and_flush(Duration::from_secs(10)))
            .await
            .unwrap();

        assert_eq!(bus.dropped_events(), 2);
        let persisted = db.list_recent_telemetry(20).unwrap();
        assert_eq!(persisted.len(), 3);
        let msgs: Vec<&str> = persisted.iter().map(|r| r.message.as_str()).collect();
        assert!(msgs.contains(&"item-1"));
        assert!(msgs.contains(&"item-2"));
        assert!(msgs.contains(&"item-3"));
        assert!(!msgs.contains(&"drop-4"));
        assert!(!msgs.contains(&"drop-5"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_publish_event_after_shutdown_drops_without_blocking_or_writing() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let bus = TelemetryBus::new(db.clone());

        let bus_flush = bus.clone();
        tokio::task::spawn_blocking(move || bus_flush.shutdown_and_flush(Duration::from_secs(5)))
            .await
            .unwrap();
        // Calling shutdown_and_flush a second time must be a safe no-op.
        bus.shutdown_and_flush(Duration::from_secs(5));

        bus.publish_event("INFO", "after.shutdown", "dropped-1", None);
        bus.publish_event("INFO", "after.shutdown", "dropped-2", None);

        assert_eq!(bus.dropped_events(), 2);
        assert_eq!(db.list_recent_telemetry(10).unwrap().len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_repeated_warn_deduplicator_window_summary_clear_and_bounded_keys() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let bus = TelemetryBus::new(db.clone());
        let dedup = RepeatedWarnDeduplicator::new(DEFAULT_WARN_DEDUP_WINDOW_MS, 4);

        let t0 = 1_700_000_000_000_i64;
        // 1. First occurrence emits immediately with 0 suppressed repeats
        let d0 = bus.publish_warn_dedup(
            &dedup,
            "equity_cliff:wallet1",
            "execution.reconcile.equity",
            "Equity cliff active",
            None,
            t0,
        );
        assert_eq!(d0, WarnDedupDecision::EmitFirst);
        assert!(d0.should_emit());
        assert_eq!(d0.suppressed_repeats(), 0);

        // 2. 19 repeats across 15s..285s inside 300_000 ms window are suppressed
        for step in 1..=19_i64 {
            let d = bus.publish_warn_dedup(
                &dedup,
                "equity_cliff:wallet1",
                "execution.reconcile.equity",
                "Equity cliff active",
                None,
                t0 + step * 15_000,
            );
            assert_eq!(
                d,
                WarnDedupDecision::Suppress {
                    suppressed_repeats: step as u64
                }
            );
            assert!(!d.should_emit());
        }

        // 3. At t0 + 300_000 ms, summary emits with (suppressed_repeats = 19)
        let d_sum = bus.publish_warn_dedup(
            &dedup,
            "equity_cliff:wallet1",
            "execution.reconcile.equity",
            "Equity cliff active",
            None,
            t0 + 300_000,
        );
        assert_eq!(
            d_sum,
            WarnDedupDecision::EmitSummary {
                suppressed_repeats: 19
            }
        );
        assert!(d_sum.should_emit());

        // 4. Two more repeats at +315s, +330s, then clear emits (suppressed_repeats = 2)
        assert_eq!(
            bus.publish_warn_dedup(
                &dedup,
                "equity_cliff:wallet1",
                "execution.reconcile.equity",
                "Equity cliff active",
                None,
                t0 + 315_000,
            ),
            WarnDedupDecision::Suppress {
                suppressed_repeats: 1
            }
        );
        assert_eq!(
            bus.publish_warn_dedup(
                &dedup,
                "equity_cliff:wallet1",
                "execution.reconcile.equity",
                "Equity cliff active",
                None,
                t0 + 330_000,
            ),
            WarnDedupDecision::Suppress {
                suppressed_repeats: 2
            }
        );
        let cleared = bus.clear_warn_dedup(
            &dedup,
            "equity_cliff:wallet1",
            "execution.reconcile.equity",
            "Equity cliff cleared",
            None,
        );
        assert_eq!(cleared, Some(2));
        // Clearing again when absent is a no-op
        assert_eq!(dedup.clear("equity_cliff:wallet1"), None);

        // 5. Verify max_keys = 4 eviction bound
        for idx in 0..10_i64 {
            let _ = dedup.observe(&format!("k-{idx}"), t0 + idx * 1_000);
            assert!(dedup.len() <= 4);
        }
        assert_eq!(dedup.len(), 4);

        let bus_flush = bus.clone();
        tokio::task::spawn_blocking(move || bus_flush.shutdown_and_flush(Duration::from_secs(10)))
            .await
            .unwrap();

        let logs = db.list_recent_telemetry(20).unwrap();
        assert_eq!(logs.len(), 3);
        assert!(logs
            .iter()
            .any(|l| l.level == "WARN" && l.message == "Equity cliff active"));
        assert!(logs
            .iter()
            .any(|l| l.level == "WARN"
                && l.message == "Equity cliff active (suppressed_repeats = 19)"));
        assert!(logs
            .iter()
            .any(|l| l.level == "INFO"
                && l.message == "Equity cliff cleared (suppressed_repeats = 2)"));
    }
}
