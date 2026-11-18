//! Tracing subscriber initialization, Tokio inter-task telemetry channels, and MariaDB `telemetry_logs` engine (`aegis-telemetry::engine`).

use aegis_core::{ConfigMap, Result, TelemetryLogRecord};
use aegis_db::DatabaseManager;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{broadcast, mpsc, watch};

/// Structured telemetry event sent over `tokio::sync::mpsc` to the MariaDB `telemetry_logs` writer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryEvent {
    pub level: String,
    pub component: String,
    pub message: String,
    pub details: Option<Value>,
}

/// Unified v2.0 Telemetry Engine & Tokio Broadcast/MPSC Bus backed by MariaDB `telemetry_logs`.
#[derive(Clone)]
pub struct TelemetryBus {
    pub db: DatabaseManager,
    pub event_tx: mpsc::Sender<TelemetryEvent>,
    pub snapshot_tx: broadcast::Sender<Value>,
    pub config_tx: watch::Sender<ConfigMap>,
    pub config_rx: watch::Receiver<ConfigMap>,
    pub dropped_events: Arc<AtomicUsize>,
}

pub type TelemetryEngine = TelemetryBus;

impl TelemetryBus {
    pub fn new(db: DatabaseManager) -> Self {
        let initial_cfg = db.load_config_or_default();
        let queue_cap = initial_cfg
            .get_usize("telemetry_queue_capacity", 2048)
            .max(64);
        let (event_tx, mut event_rx) = mpsc::channel::<TelemetryEvent>(queue_cap);
        let (snapshot_tx, _) = broadcast::channel::<Value>(128);
        let (config_tx, config_rx) = watch::channel(initial_cfg);

        let writer_db = db.clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                while let Some(ev) = event_rx.recv().await {
                    let _ =
                        writer_db.log_telemetry(&ev.level, &ev.component, &ev.message, ev.details);
                }
            });
        } else {
            let _ = std::thread::Builder::new()
                .name("aegis-telemetry-writer".to_string())
                .spawn(move || {
                    while let Some(ev) = event_rx.blocking_recv() {
                        let _ = writer_db.log_telemetry(
                            &ev.level,
                            &ev.component,
                            &ev.message,
                            ev.details,
                        );
                    }
                });
        }

        Self {
            db,
            event_tx,
            snapshot_tx,
            config_tx,
            config_rx,
            dropped_events: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn dropped_event_count(&self) -> usize {
        self.dropped_events.load(Ordering::SeqCst)
    }

    pub fn reload_config(&self) {
        if let Ok(cfg) = self.db.load_config() {
            let _ = self.config_tx.send(cfg);
        }
    }

    /// Publishes an asynchronous telemetry event to the background MariaDB writer and SSE broadcast stream.
    pub fn publish_event(
        &self,
        level: &str,
        component: &str,
        message: &str,
        details: Option<Value>,
    ) {
        let ev = TelemetryEvent {
            level: level.to_string(),
            component: component.to_string(),
            message: message.to_string(),
            details: details.clone(),
        };
        if let Err(err) = self.event_tx.try_send(ev) {
            let dropped = self.dropped_events.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::warn!(
                component = component,
                dropped_total = dropped,
                reason = %err,
                "TelemetryBus event_tx queue full or closed; writing directly to MariaDB"
            );
            let _ = self
                .db
                .log_telemetry(level, component, message, details.clone());
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
