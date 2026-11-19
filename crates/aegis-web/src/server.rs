//! Axum HTTP server lifecycle and `AppState` initialization (`aegis-web::server`).

use crate::payloads::build_dashboard_payload;
use crate::routes::build_router;
use aegis_core::{resolve_project_root, AegisError, ConfigMap, Result};
use aegis_db::DatabaseManager;
use axum::Router;
use rand::RngCore;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{watch, Notify};

struct BroadcasterDropGuard {
    shutdown_tx: watch::Sender<bool>,
}

impl BroadcasterDropGuard {
    fn new(shutdown_tx: watch::Sender<bool>) -> Self {
        Self { shutdown_tx }
    }

    fn subscribe(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }
}

impl Drop for BroadcasterDropGuard {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

/// Shared application state for the Aegis Quant v2.0 Web server.
#[derive(Clone)]
pub struct AppState {
    pub db: DatabaseManager,
    pub static_dir: PathBuf,
    pub session_token: String,
    pub configured_api_token: Option<String>,
    pub require_bearer_auth: bool,
    pub dashboard_rx: watch::Receiver<Arc<String>>,
    pub dashboard_tx: Arc<watch::Sender<Arc<String>>>,
    pub refresh_notify: Arc<Notify>,
    pub sse_client_count: Arc<AtomicUsize>,
    broadcaster_started: Arc<AtomicBool>,
    broadcaster_guard: Arc<BroadcasterDropGuard>,
}

fn load_auth_config_fail_closed(db: &DatabaseManager) -> (Option<String>, bool, String) {
    match db.load_config() {
        Ok(cfg) => {
            let tok = cfg
                .get("web_api_token")
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            let auth_flag =
                cfg.get_bool("web_auth_enabled", false) || cfg.get_bool("web_require_auth", false);
            let bind_host = cfg.get_str("web_bind_host", "127.0.0.1");
            (tok, auth_flag, bind_host)
        }
        Err(_) => {
            // Fallback inspection of raw system_config rows so one corrupt secret cannot silently disable auth
            let mut tok: Option<String> = None;
            let mut auth_flag = false;
            let mut bind_host = "127.0.0.1".to_string();
            let mut any_auth_error = false;
            if let Ok(rows) = db.list_system_config_rows() {
                for r in rows {
                    match r.config_key.as_str() {
                        "web_api_token" => {
                            let raw = r.config_value.trim();
                            if !raw.is_empty() {
                                if raw.starts_with("gAAAAAB") || raw.starts_with("ENC:gAAAAAB") {
                                    match db.fernet().decrypt(raw) {
                                        Ok(plain) if !plain.trim().is_empty() => {
                                            tok = Some(plain.trim().to_string());
                                        }
                                        _ => {
                                            any_auth_error = true;
                                        }
                                    }
                                } else {
                                    tok = Some(raw.to_string());
                                }
                            }
                        }
                        "web_auth_enabled" | "web_require_auth" => {
                            let v = r.config_value.trim().to_ascii_lowercase();
                            if matches!(v.as_str(), "true" | "1" | "yes" | "on") {
                                auth_flag = true;
                            }
                        }
                        "web_bind_host" if !r.config_value.trim().is_empty() => {
                            bind_host = r.config_value.trim().to_string();
                        }
                        _ => {}
                    }
                }
            } else {
                let cfg = ConfigMap::default();
                bind_host = cfg.get_str("web_bind_host", "127.0.0.1");
            }
            if any_auth_error {
                auth_flag = true;
                if tok.is_none() {
                    tok = Some(generate_session_token());
                }
            }
            (tok, auth_flag, bind_host)
        }
    }
}

fn is_loopback_host(host: &str) -> bool {
    let h = host.trim().to_ascii_lowercase();
    matches!(h.as_str(), "127.0.0.1" | "::1" | "localhost" | "")
}

impl AppState {
    pub fn new(db: DatabaseManager, static_dir: Option<PathBuf>) -> Self {
        Self::new_with_bind_host(db, static_dir, None)
    }

    pub fn new_with_bind_host(
        db: DatabaseManager,
        static_dir: Option<PathBuf>,
        bind_host: Option<&str>,
    ) -> Self {
        let dir = static_dir.unwrap_or_else(|| resolve_project_root().join("web").join("static"));
        let (configured_api_token, auth_flag, cfg_bind_host) = load_auth_config_fail_closed(&db);
        let effective_host = bind_host
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(cfg_bind_host.as_str());
        // Fail closed to true when bound non-loopback and a token exists
        let require_bearer_auth =
            auth_flag || (!is_loopback_host(effective_host) && configured_api_token.is_some());

        let (dashboard_tx, dashboard_rx) = watch::channel(Arc::new(String::new()));
        let (sd_tx, _) = watch::channel(false);

        let state = Self {
            db,
            static_dir: dir,
            session_token: generate_session_token(),
            configured_api_token,
            require_bearer_auth,
            dashboard_rx,
            dashboard_tx: Arc::new(dashboard_tx),
            refresh_notify: Arc::new(Notify::new()),
            sse_client_count: Arc::new(AtomicUsize::new(0)),
            broadcaster_started: Arc::new(AtomicBool::new(false)),
            broadcaster_guard: Arc::new(BroadcasterDropGuard::new(sd_tx)),
        };
        state.ensure_broadcaster_running();
        state
    }

    /// Triggers an immediate refresh on the shared 1.5s SSE watch broadcaster.
    pub fn notify_dashboard_refresh(&self) {
        self.refresh_notify.notify_one();
    }

    /// Signals the SSE broadcaster and all active SSE client streams to terminate cleanly.
    pub fn shutdown(&self) {
        let _ = self.broadcaster_guard.shutdown_tx.send(true);
    }

    /// Subscribes to the server shutdown signal.
    pub fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.broadcaster_guard.subscribe()
    }

    /// Spawns the single shared background task that refreshes the serialized `DashboardPayload`
    /// every 1.5s into the `tokio::sync::watch` channel for all connected SSE clients.
    pub fn ensure_broadcaster_running(&self) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        if self
            .broadcaster_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let db = self.db.clone();
        let tx = Arc::clone(&self.dashboard_tx);
        let notify = Arc::clone(&self.refresh_notify);
        let mut shutdown_rx = self.broadcaster_guard.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(1500));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut first = true;

            loop {
                if !first {
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = notify.notified() => {}
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                }
                if *shutdown_rx.borrow() {
                    break;
                }

                // Refresh on startup or whenever at least one SSE client is subscribed (`receiver_count() > 1`)
                if first || tx.receiver_count() > 1 {
                    first = false;
                    let db_cl = db.clone();
                    if let Ok(Ok(json_str)) = tokio::task::spawn_blocking(move || {
                        build_dashboard_payload(&db_cl)
                            .and_then(|p| serde_json::to_string(&p).map_err(Into::into))
                    })
                    .await
                    {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                        let _ = tx.send(Arc::new(json_str));
                    }
                }
            }
        });
    }
}

/// Generates a random 32-byte hex session token.
pub fn generate_session_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Constructs an Axum `Router` for `db` and optional `static_dir`.
pub fn create_web_router(db: DatabaseManager, static_dir: Option<&Path>) -> Router {
    create_web_router_for_host(db, static_dir, None)
}

/// Constructs an Axum `Router` with explicit bind host awareness for fail-closed auth checks.
pub fn create_web_router_for_host(
    db: DatabaseManager,
    static_dir: Option<&Path>,
    bind_host: Option<&str>,
) -> Router {
    let state = AppState::new_with_bind_host(db, static_dir.map(|p| p.to_path_buf()), bind_host);
    build_router(state)
}

/// Starts the Aegis Quant v2.0 Web UI and REST/SSE server on `host:port` until `shutdown_rx` signals true.
pub async fn serve_web_with_shutdown(
    db: DatabaseManager,
    host: &str,
    port: u16,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let addr = format!("{}:{}", host.trim(), port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(AegisError::Io)?;

    tracing::info!(
        bind_addr = %addr,
        "Aegis Quant v2.0 Web Dashboard & Real-Time SSE Server listening on http://{addr}"
    );

    let state = AppState::new_with_bind_host(db, None, Some(host));
    let shutdown_state = state.clone();
    let app = build_router(state);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            while !*shutdown_rx.borrow() {
                if shutdown_rx.changed().await.is_err() {
                    break;
                }
            }
            shutdown_state.shutdown();
        })
        .await
        .map_err(AegisError::Io)?;

    Ok(())
}
