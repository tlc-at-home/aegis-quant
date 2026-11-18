//! Real-time 1s/2s Server-Sent Events (SSE) stream backed by MariaDB 13.0 and the 7 SQL Views (`aegis-web::stream`).

use crate::payloads::build_dashboard_payload;
use crate::server::AppState;
use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
};
use std::{convert::Infallible, time::Duration};
use tokio_stream::{wrappers::IntervalStream, StreamExt};

/// `GET /api/v1/stream` — Real-time Server-Sent Events (SSE) stream pushing unified MariaDB 13.0 + 7 SQL Views state every 1.5 seconds.
pub async fn sse_stream_handler(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let interval = tokio::time::interval(Duration::from_millis(1500));
    let stream = IntervalStream::new(interval).then(move |_| {
        let st = state.clone();
        async move {
            let db = st.db.clone();
            let payload_json = tokio::task::spawn_blocking(move || {
                build_dashboard_payload(&db)
                    .ok()
                    .and_then(|p| serde_json::to_string(&p).ok())
                    .unwrap_or_else(|| "{}".to_string())
            })
            .await
            .unwrap_or_else(|_| "{}".to_string());

            Ok::<Event, Infallible>(Event::default().data(payload_json))
        }
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(10))
            .text("aegis-v2-heartbeat"),
    )
}
