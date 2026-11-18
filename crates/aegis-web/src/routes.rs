//! Axum router composition wiring static assets, REST endpoints, and the 1s/2s SSE stream (`aegis-web::routes`).

use crate::auth::security_and_auth_middleware;
use crate::handlers::{
    get_dashboard_handler, get_health_handler, post_alerts_test_handler, post_auth_login_handler,
    post_config_handler, post_positions_close_handler, post_risk_tier_handler, serve_app_js,
    serve_index, serve_styles_css,
};
use crate::server::AppState;
use crate::stream::sse_stream_handler;
use axum::{
    middleware,
    routing::{get, post},
    Router,
};

/// Builds the complete Aegis Quant v2.0 Axum `Router` backed by MariaDB 13.0 and the 7 SQL Views.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        // Static Showroom UI Assets (web/static/)
        .route("/", get(serve_index))
        .route("/index.html", get(serve_index))
        .route("/styles.css", get(serve_styles_css))
        .route("/static/styles.css", get(serve_styles_css))
        .route("/app.js", get(serve_app_js))
        .route("/static/app.js", get(serve_app_js))
        // v2.0 REST API & Real-Time SSE Stream
        .route("/api/v1/health", get(get_health_handler))
        .route("/api/v1/dashboard", get(get_dashboard_handler))
        .route("/api/v1/state", get(get_dashboard_handler))
        .route("/api/v1/stream", get(sse_stream_handler))
        .route("/api/v1/risk-tier", post(post_risk_tier_handler))
        .route("/api/v1/config", post(post_config_handler))
        .route("/api/v1/alerts/test", post(post_alerts_test_handler))
        .route(
            "/api/v1/positions/close",
            post(post_positions_close_handler),
        )
        .route("/api/v1/auth/login", post(post_auth_login_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            security_and_auth_middleware,
        ))
        .with_state(state)
}
