//! Real-time 1.5s Server-Sent Events (SSE) stream backed by a shared `tokio::sync::watch` broadcaster (`aegis-web::stream`).

use crate::payloads::{build_dashboard_payload, ApiErrorBody};
use crate::server::AppState;
use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Json,
};
use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio_stream::wrappers::ReceiverStream;

pub const MAX_SSE_CLIENTS: usize = 64;

struct SseClientGuard(Arc<AtomicUsize>);

impl Drop for SseClientGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// `GET /api/v1/stream` — Real-time Server-Sent Events (SSE) stream subscribing to the shared
/// 1.5s `tokio::sync::watch` `DashboardPayload` broadcaster (1 DB refresh per 1.5s across all tabs).
pub async fn sse_stream_handler(State(state): State<AppState>) -> Response {
    // Atomically acquire a client slot under MAX_SSE_CLIENTS
    let mut prev = state.sse_client_count.load(Ordering::SeqCst);
    loop {
        if prev >= MAX_SSE_CLIENTS {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiErrorBody {
                    error: "Service Unavailable".to_string(),
                    detail: format!(
                        "Maximum concurrent SSE connections ({MAX_SSE_CLIENTS}) reached"
                    ),
                }),
            )
                .into_response();
        }
        match state.sse_client_count.compare_exchange_weak(
            prev,
            prev + 1,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => break,
            Err(actual) => prev = actual,
        }
    }

    let guard = SseClientGuard(Arc::clone(&state.sse_client_count));
    state.ensure_broadcaster_running();

    let (ev_tx, ev_rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(2);
    let mut rx = state.dashboard_rx.clone();
    let mut shutdown_rx = state.subscribe_shutdown();
    let notify = Arc::clone(&state.refresh_notify);
    let db_fallback = state.db.clone();

    tokio::spawn(async move {
        let _client_guard = guard;

        let initial_payload = {
            let current = rx.borrow_and_update().clone();
            if !current.is_empty() {
                Some(current)
            } else {
                notify.notify_one();
                tokio::task::spawn_blocking(move || {
                    build_dashboard_payload(&db_fallback)
                        .ok()
                        .and_then(|p| serde_json::to_string(&p).ok())
                        .map(Arc::new)
                })
                .await
                .ok()
                .flatten()
            }
        };

        if let Some(payload) = initial_payload {
            if ev_tx
                .send(Ok(Event::default().data(payload.as_str())))
                .await
                .is_err()
            {
                return;
            }
        }

        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            tokio::select! {
                _ = ev_tx.closed() => break,
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                res = rx.changed() => {
                    if res.is_err() {
                        break;
                    }
                    let payload = rx.borrow_and_update().clone();
                    if !payload.is_empty()
                        && ev_tx
                            .send(Ok(Event::default().data(payload.as_str())))
                            .await
                            .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });

    let stream = ReceiverStream::new(ev_rx);
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(10))
                .text("aegis-v2-heartbeat"),
        )
        .into_response()
}
