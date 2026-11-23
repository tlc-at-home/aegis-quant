//! Real-time 1.5s Server-Sent Events (SSE) stream backed by a shared `tokio::sync::watch` broadcaster (`aegis-web::stream`).

use crate::auth::{auth_error_response, AuthenticatedPrincipal};
use crate::payloads::{
    build_dashboard_payload, build_degraded_sse_event_json, has_critical_degradation,
    next_error_correlation_id, next_snapshot_seq, server_boot_id,
};
use crate::server::{AppState, BroadcastSnapshot};
use aegis_core::now_ms;
use axum::{
    extract::{Extension, State},
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
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

pub const MAX_SSE_CLIENTS: usize = 64;
pub const SSE_CLIENT_SEND_TIMEOUT: Duration = Duration::from_secs(5);
pub const SSE_SNAPSHOT_STALE_THRESHOLD: Duration = Duration::from_millis(2_000);
pub const SSE_AUTH_RECHECK_INTERVAL: Duration = Duration::from_secs(30);

struct SseClientGuard(Arc<AtomicUsize>);

impl Drop for SseClientGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Returns once `shutdown_rx` becomes `true` (or its sender drops), without holding a non-`Send`
/// `watch::Ref` guard across `.await` points inside `tokio::select!`.
async fn wait_for_shutdown(rx: &mut watch::Receiver<bool>) {
    loop {
        if *rx.borrow_and_update() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Interruptible bounded SSE event delivery (`R14-M21`).
/// Fast path uses `try_send`; when the 2-slot buffer is full (backpressured/stalled client),
/// `tokio::select!` races shutdown, receiver close, and `timeout` so stalled readers drop cleanly.
async fn deliver_sse_event(
    ev_tx: &mpsc::Sender<std::result::Result<Event, Infallible>>,
    shutdown_rx: &mut watch::Receiver<bool>,
    event: Event,
    timeout: Duration,
) -> bool {
    if *shutdown_rx.borrow() {
        return false;
    }
    match ev_tx.try_send(Ok(event)) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Closed(_)) => false,
        Err(mpsc::error::TrySendError::Full(ev)) => {
            tokio::select! {
                biased;
                _ = wait_for_shutdown(shutdown_rx) => false,
                _ = ev_tx.closed() => false,
                res = tokio::time::timeout(timeout, ev_tx.send(ev)) => match res {
                    Ok(Ok(())) => true,
                    Ok(Err(_)) => false,
                    Err(_) => {
                        tracing::warn!(
                            timeout_ms = timeout.as_millis() as u64,
                            "Dropping stalled SSE client after send timeout"
                        );
                        false
                    }
                }
            }
        }
    }
}

fn is_principal_still_valid(state: &AppState, principal: &AuthenticatedPrincipal) -> bool {
    if !state.require_bearer_auth {
        return true;
    }
    match principal {
        AuthenticatedPrincipal::UnauthenticatedAllowed => false,
        AuthenticatedPrincipal::BearerToken => state
            .configured_api_token
            .as_deref()
            .map(str::trim)
            .is_some_and(|s| !s.is_empty()),
        AuthenticatedPrincipal::SessionCookie { token_hash } => {
            state.is_session_hash_valid(token_hash)
        }
    }
}

fn emit_auth_expired_and_close(ev_tx: &mpsc::Sender<std::result::Result<Event, Infallible>>) {
    let _ = ev_tx.try_send(Ok(Event::default()
        .event("auth_expired")
        .data(r#"{"reason":"session_invalid"}"#)));
}

fn serialize_fresh_snapshot_fallback(db: &aegis_db::DatabaseManager) -> BroadcastSnapshot {
    match build_dashboard_payload(db) {
        Ok(payload) => {
            if has_critical_degradation(&payload) {
                let json_str = build_degraded_sse_event_json(
                    payload.database_reachable,
                    payload.degraded_sections,
                    payload.degraded_errors,
                    payload.snapshot_seq,
                    payload.generated_at_ms,
                    payload.server_boot_id,
                );
                BroadcastSnapshot {
                    seq: payload.snapshot_seq,
                    generated_at: Instant::now(),
                    generated_at_ms: payload.generated_at_ms,
                    json: Arc::new(json_str),
                    degraded: true,
                }
            } else {
                match serde_json::to_string(&payload) {
                    Ok(json_str) => BroadcastSnapshot {
                        seq: payload.snapshot_seq,
                        generated_at: Instant::now(),
                        generated_at_ms: payload.generated_at_ms,
                        json: Arc::new(json_str),
                        degraded: payload.degraded,
                    },
                    Err(e) => {
                        let cid = next_error_correlation_id();
                        tracing::error!(correlation_id = %cid, error = %e, "SSE fallback serialization failed");
                        let ts = now_ms();
                        let seq = next_snapshot_seq();
                        let boot_id = server_boot_id();
                        let json_str = build_degraded_sse_event_json(
                            true,
                            vec!["serialization".to_string()],
                            vec![format!("Payload serialization failed (ref: {cid})")],
                            seq,
                            ts,
                            boot_id,
                        );
                        BroadcastSnapshot {
                            seq,
                            generated_at: Instant::now(),
                            generated_at_ms: ts,
                            json: Arc::new(json_str),
                            degraded: true,
                        }
                    }
                }
            }
        }
        Err(e) => {
            let cid = next_error_correlation_id();
            tracing::error!(correlation_id = %cid, error = %e, "SSE initial snapshot query failed");
            let ts = now_ms();
            let seq = next_snapshot_seq();
            let boot_id = server_boot_id();
            let json_str = build_degraded_sse_event_json(
                false,
                vec!["database".to_string()],
                vec![format!("Database unavailable (ref: {cid})")],
                seq,
                ts,
                boot_id,
            );
            BroadcastSnapshot {
                seq,
                generated_at: Instant::now(),
                generated_at_ms: ts,
                json: Arc::new(json_str),
                degraded: true,
            }
        }
    }
}

/// `GET /api/v1/stream` — Real-time Server-Sent Events (SSE) stream subscribing to the shared
/// 1.5s `tokio::sync::watch` `DashboardPayload` broadcaster (1 DB refresh per 1.5s across all tabs).
pub async fn sse_stream_handler(
    State(state): State<AppState>,
    principal_ext: Option<Extension<AuthenticatedPrincipal>>,
) -> Response {
    let principal = match principal_ext {
        Some(Extension(p)) => p,
        None if state.require_bearer_auth => {
            return auth_error_response(
                StatusCode::UNAUTHORIZED,
                "Missing authenticated principal for SSE stream",
            );
        }
        None => AuthenticatedPrincipal::UnauthenticatedAllowed,
    };

    // Atomically acquire a client slot under MAX_SSE_CLIENTS, tracking idle->active transitions
    let mut prev = state.sse_client_count.load(Ordering::SeqCst);
    let was_idle: bool;
    loop {
        if prev >= MAX_SSE_CLIENTS {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(crate::payloads::ApiErrorBody {
                    error: "Service Unavailable".to_string(),
                    detail: format!(
                        "Maximum concurrent SSE connections ({MAX_SSE_CLIENTS}) reached"
                    ),
                    correlation_id: None,
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
            Ok(_) => {
                was_idle = prev == 0;
                break;
            }
            Err(actual) => prev = actual,
        }
    }

    let guard = SseClientGuard(Arc::clone(&state.sse_client_count));
    // Clone watch receiver BEFORE notifying broadcaster so receiver_count() > 1 is guaranteed (`R14-L13`)
    let mut rx = state.dashboard_rx.clone();
    state.ensure_broadcaster_running();

    let (ev_tx, ev_rx) = mpsc::channel::<std::result::Result<Event, Infallible>>(2);
    let mut shutdown_rx = state.subscribe_shutdown();
    let mut revocation_rx = state.session_revocation_tx.subscribe();
    let notify = Arc::clone(&state.refresh_notify);
    let db_fallback = state.db.clone();
    let send_timeout = state.sse_client_send_timeout;
    let recheck_interval = state.sse_auth_recheck_interval;
    let auth_state = state.clone();

    tokio::spawn(async move {
        let _client_guard = guard;

        let initial_snapshot: Option<BroadcastSnapshot> = {
            let cached_opt = rx.borrow_and_update().clone();
            let cached_seq = cached_opt.as_ref().map(|s| s.seq).unwrap_or(0);
            let needs_fresh = was_idle
                || match &cached_opt {
                    Some(snap) => snap.generated_at.elapsed() > SSE_SNAPSHOT_STALE_THRESHOLD,
                    None => true,
                };

            if !needs_fresh {
                cached_opt
            } else {
                notify.notify_one();
                let wait_deadline = tokio::time::sleep(Duration::from_secs(3));
                tokio::pin!(wait_deadline);
                loop {
                    tokio::select! {
                        biased;
                        _ = wait_for_shutdown(&mut shutdown_rx) => break None,
                        _ = ev_tx.closed() => break None,
                        _ = &mut wait_deadline => {
                            let db_cl = db_fallback.clone();
                            break tokio::task::spawn_blocking(move || serialize_fresh_snapshot_fallback(&db_cl))
                                .await
                                .ok();
                        }
                        res = rx.changed() => {
                            if res.is_err() {
                                let db_cl = db_fallback.clone();
                                break tokio::task::spawn_blocking(move || serialize_fresh_snapshot_fallback(&db_cl))
                                    .await
                                    .ok();
                            }
                            let next_snap = rx.borrow_and_update().clone();
                            if let Some(snap) = next_snap {
                                if snap.seq > cached_seq {
                                    break Some(snap);
                                }
                            }
                        }
                    }
                }
            }
        };

        if !is_principal_still_valid(&auth_state, &principal) {
            emit_auth_expired_and_close(&ev_tx);
            return;
        }

        if let Some(snap) = initial_snapshot {
            if !snap.json.is_empty()
                && !deliver_sse_event(
                    &ev_tx,
                    &mut shutdown_rx,
                    Event::default().data(snap.json.as_str()),
                    send_timeout,
                )
                .await
            {
                return;
            }
        }

        let require_auth = auth_state.require_bearer_auth;
        let mut auth_recheck_ticker = tokio::time::interval(recheck_interval);
        auth_recheck_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let _ = auth_recheck_ticker.tick().await;
        let _ = revocation_rx.borrow_and_update();

        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown_rx) => break,
                _ = ev_tx.closed() => break,
                rev_res = revocation_rx.changed(), if require_auth => {
                    if rev_res.is_err() {
                        break;
                    }
                    let _ = revocation_rx.borrow_and_update();
                    if !is_principal_still_valid(&auth_state, &principal) {
                        emit_auth_expired_and_close(&ev_tx);
                        break;
                    }
                }
                _ = auth_recheck_ticker.tick(), if require_auth => {
                    if !is_principal_still_valid(&auth_state, &principal) {
                        emit_auth_expired_and_close(&ev_tx);
                        break;
                    }
                }
                res = rx.changed() => {
                    if res.is_err() {
                        break;
                    }
                    let next_snap = rx.borrow_and_update().clone();
                    if let Some(snap) = next_snap {
                        if !snap.json.is_empty()
                            && !deliver_sse_event(
                                &ev_tx,
                                &mut shutdown_rx,
                                Event::default().data(snap.json.as_str()),
                                send_timeout,
                            )
                            .await
                        {
                            break;
                        }
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
