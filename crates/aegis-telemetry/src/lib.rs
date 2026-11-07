//! Tracing subscriber, Tokio inter-task communication bus (`mpsc`, `broadcast`, `watch`),
//! and operational alerting (`AlertEngine`, `AlertRetryQueue`).

pub use aegis_alerts::{AlertEngine, AlertRetryQueue, QueuedAlertTask};

use aegis_db::{ConfigMap, DatabaseManager};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, watch};

/// Structured telemetry event sent over `tokio::sync::mpsc` to the database writer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryEvent {
    pub level: String,
    pub component: String,
    pub message: String,
    pub details: Option<Value>,
}

/// Tokio inter-task communication bus for the PP-50 Two-Book Platform.
#[derive(Clone)]
pub struct TelemetryBus {
    pub event_tx: mpsc::Sender<TelemetryEvent>,
    pub snapshot_tx: broadcast::Sender<Value>,
    pub config_tx: watch::Sender<ConfigMap>,
    pub config_rx: watch::Receiver<ConfigMap>,
    pub settling_tx: watch::Sender<bool>,
    pub settling_rx: watch::Receiver<bool>,
}

impl TelemetryBus {
    pub fn new(db: DatabaseManager) -> Self {
        let (event_tx, mut event_rx) = mpsc::channel::<TelemetryEvent>(256);
        let (snapshot_tx, _) = broadcast::channel::<Value>(64);
        let initial_cfg = db.load_config_logged("aegis-telemetry.bus");
        let (config_tx, config_rx) = watch::channel(initial_cfg);
        let (settling_tx, settling_rx) = watch::channel(false);

        let writer_db = db.clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                while let Some(ev) = event_rx.recv().await {
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
            event_tx,
            snapshot_tx,
            config_tx,
            config_rx,
            settling_tx,
            settling_rx,
        }
    }

    pub fn set_settling_in_progress(&self, in_progress: bool) {
        let _ = self.settling_tx.send(in_progress);
    }

    pub fn is_settling_in_progress(&self) -> bool {
        *self.settling_rx.borrow()
    }

    pub fn reload_config(&self, db: &DatabaseManager) {
        if let Ok(cfg) = db.load_config() {
            let _ = self.config_tx.send(cfg);
        }
    }

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
        let _ = self.event_tx.try_send(ev);
        if let Some(d) = details {
            let _ = self.snapshot_tx.send(d);
        }
    }
}

/// Initializes the global `tracing_subscriber` if not already initialized.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_telemetry_bus_settling_watch_and_config_reload() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let bus = TelemetryBus::new(db.clone());

        assert!(!bus.is_settling_in_progress());
        bus.set_settling_in_progress(true);
        assert!(bus.is_settling_in_progress());
        bus.set_settling_in_progress(false);
        assert!(!bus.is_settling_in_progress());

        db.set_config_value("polling_interval_seconds", &json!(30))
            .unwrap();
        bus.reload_config(&db);
        let cfg = bus.config_rx.borrow().clone();
        assert_eq!(cfg.get_i64("polling_interval_seconds", 15), 30);
    }

    #[test]
    fn test_telemetry_bus_publish_event_and_snapshot_broadcast() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let bus = TelemetryBus::new(db);
        let mut rx = bus.snapshot_tx.subscribe();

        bus.publish_event(
            "INFO",
            "test.component",
            "unit test event",
            Some(json!({"equity": 10250.5})),
        );

        let received = rx.try_recv().unwrap();
        assert_eq!(received["equity"], json!(10250.5));
    }
}
