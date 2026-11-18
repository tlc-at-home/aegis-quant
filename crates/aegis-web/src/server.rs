//! Axum HTTP server lifecycle and `AppState` initialization (`aegis-web::server`).

use crate::routes::build_router;
use aegis_core::{resolve_project_root, AegisError, Result};
use aegis_db::DatabaseManager;
use axum::Router;
use rand::RngCore;
use std::path::{Path, PathBuf};

/// Shared application state for the Aegis Quant v2.0 Web server.
#[derive(Clone)]
pub struct AppState {
    pub db: DatabaseManager,
    pub static_dir: PathBuf,
    pub session_token: String,
    pub configured_api_token: Option<String>,
    pub require_bearer_auth: bool,
}

impl AppState {
    pub fn new(db: DatabaseManager, static_dir: Option<PathBuf>) -> Self {
        let dir = static_dir.unwrap_or_else(|| resolve_project_root().join("web").join("static"));
        let cfg = db.load_config_or_default();
        let configured_api_token = cfg
            .get("web_api_token")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let require_bearer_auth = cfg.get_bool("web_require_auth", false);

        Self {
            db,
            static_dir: dir,
            session_token: generate_session_token(),
            configured_api_token,
            require_bearer_auth,
        }
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
    let state = AppState::new(db, static_dir.map(|p| p.to_path_buf()));
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

    let app = create_web_router(db, None);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            while !*shutdown_rx.borrow() {
                if shutdown_rx.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .map_err(AegisError::Io)?;

    Ok(())
}
