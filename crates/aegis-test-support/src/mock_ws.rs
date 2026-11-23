//! Reusable loopback WebSocket mock server (`aegis-test-support::mock_ws`).
//!
//! Built on `tokio-tungstenite` for Hyperliquid WebSocket client tests:
//! accepts WS connections, records incoming subscribe/ping frames, replies with
//! protocol/JSON pongs, pushes scripted JSON frames, and supports controlled
//! or abrupt disconnects for reconnect/resubscribe tests.

use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

enum WsServerCommand {
    SendText(String),
    CloseGraceful,
    DropAbrupt,
}

/// Reusable RAII loopback WebSocket mock server (`ws://127.0.0.1:<port>`).
///
/// Runs a lightweight single-threaded Tokio reactor on a dedicated background
/// OS thread so it can be constructed and dropped safely inside both sync and
/// async tests.
pub struct MockWsServer {
    port: u16,
    connection_count: Arc<AtomicUsize>,
    active_connection_count: Arc<AtomicUsize>,
    auto_pong: Arc<AtomicBool>,
    reject_next_handshakes: Arc<AtomicUsize>,
    received_messages: Arc<Mutex<Vec<String>>>,
    on_connect_frames: Arc<Mutex<Vec<String>>>,
    connections: Arc<Mutex<HashMap<usize, mpsc::UnboundedSender<WsServerCommand>>>>,
    shutdown_tx: watch::Sender<bool>,
    thread: Option<JoinHandle<()>>,
}

impl MockWsServer {
    /// Starts a loopback WebSocket mock server bound to `127.0.0.1:0`.
    pub fn try_start() -> std::io::Result<Self> {
        let std_listener = TcpListener::bind("127.0.0.1:0")?;
        let port = std_listener.local_addr()?.port();
        std_listener.set_nonblocking(true)?;

        let connection_count = Arc::new(AtomicUsize::new(0));
        let active_connection_count = Arc::new(AtomicUsize::new(0));
        let auto_pong = Arc::new(AtomicBool::new(true));
        let reject_next_handshakes = Arc::new(AtomicUsize::new(0));
        let received_messages = Arc::new(Mutex::new(Vec::new()));
        let on_connect_frames = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(Mutex::new(HashMap::new()));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let conn_count_bg = Arc::clone(&connection_count);
        let active_count_bg = Arc::clone(&active_connection_count);
        let auto_pong_bg = Arc::clone(&auto_pong);
        let reject_bg = Arc::clone(&reject_next_handshakes);
        let received_bg = Arc::clone(&received_messages);
        let on_connect_bg = Arc::clone(&on_connect_frames);
        let conns_bg = Arc::clone(&connections);

        let thread = std::thread::Builder::new()
            .name(format!("aegis-mock-ws-{port}"))
            .spawn(move || {
                let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                rt.block_on(async move {
                    let Ok(listener) = tokio::net::TcpListener::from_std(std_listener) else {
                        return;
                    };
                    let mut next_conn_id = 1usize;

                    loop {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => {
                                if *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            accept_res = listener.accept() => {
                                let Ok((stream, _)) = accept_res else {
                                    break;
                                };
                                let mut should_reject = false;
                                let mut cur = reject_bg.load(Ordering::SeqCst);
                                while cur > 0 {
                                    match reject_bg.compare_exchange_weak(
                                        cur,
                                        cur - 1,
                                        Ordering::SeqCst,
                                        Ordering::SeqCst,
                                    ) {
                                        Ok(_) => {
                                            should_reject = true;
                                            break;
                                        }
                                        Err(next) => cur = next,
                                    }
                                }
                                if should_reject {
                                    drop(stream);
                                    continue;
                                }

                                let conn_id = next_conn_id;
                                next_conn_id = next_conn_id.saturating_add(1);
                                let conn_count_c = Arc::clone(&conn_count_bg);
                                let active_count_c = Arc::clone(&active_count_bg);
                                let auto_pong_c = Arc::clone(&auto_pong_bg);
                                let received_c = Arc::clone(&received_bg);
                                let on_connect_c = Arc::clone(&on_connect_bg);
                                let conns_c = Arc::clone(&conns_bg);
                                let mut conn_shutdown = shutdown_rx.clone();

                                tokio::spawn(async move {
                                    let Ok(ws_stream) = tokio_tungstenite::accept_async(stream).await else {
                                        return;
                                    };
                                    conn_count_c.fetch_add(1, Ordering::SeqCst);
                                    active_count_c.fetch_add(1, Ordering::SeqCst);

                                    let (mut write, mut read) = ws_stream.split();
                                    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
                                    conns_c.lock().insert(conn_id, cmd_tx);

                                    let initial_frames = on_connect_c.lock().clone();
                                    for frame in initial_frames {
                                        if write.send(Message::Text(frame)).await.is_err() {
                                            conns_c.lock().remove(&conn_id);
                                            active_count_c.fetch_sub(1, Ordering::SeqCst);
                                            return;
                                        }
                                    }

                                    loop {
                                        if *conn_shutdown.borrow() {
                                            let _ = write.send(Message::Close(None)).await;
                                            break;
                                        }
                                        tokio::select! {
                                            biased;
                                            _ = conn_shutdown.changed() => {
                                                if *conn_shutdown.borrow() {
                                                    let _ = write.send(Message::Close(None)).await;
                                                    break;
                                                }
                                            }
                                            cmd_opt = cmd_rx.recv() => {
                                                match cmd_opt {
                                                    Some(WsServerCommand::SendText(txt)) => {
                                                        if write.send(Message::Text(txt)).await.is_err() {
                                                            break;
                                                        }
                                                    }
                                                    Some(WsServerCommand::CloseGraceful) => {
                                                        let _ = write.send(Message::Close(None)).await;
                                                        break;
                                                    }
                                                    Some(WsServerCommand::DropAbrupt) | None => break,
                                                }
                                            }
                                            msg_opt = read.next() => {
                                                match msg_opt {
                                                    Some(Ok(Message::Text(txt))) => {
                                                        received_c.lock().push(txt.clone());
                                                        if auto_pong_c.load(Ordering::Acquire) {
                                                            if let Ok(val) = serde_json::from_str::<Value>(&txt) {
                                                                if val.get("method").and_then(Value::as_str) == Some("ping")
                                                                    && write.send(Message::Text(r#"{"channel":"pong"}"#.to_string())).await.is_err()
                                                                {
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                    }
                                                    Some(Ok(Message::Ping(payload))) => {
                                                        if auto_pong_c.load(Ordering::Acquire)
                                                            && write.send(Message::Pong(payload)).await.is_err()
                                                        {
                                                            break;
                                                        }
                                                    }
                                                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                                                    Some(Ok(_)) => {}
                                                }
                                            }
                                        }
                                    }

                                    conns_c.lock().remove(&conn_id);
                                    active_count_c.fetch_sub(1, Ordering::SeqCst);
                                });
                            }
                        }
                    }
                });
            })?;

        Ok(Self {
            port,
            connection_count,
            active_connection_count,
            auto_pong,
            reject_next_handshakes,
            received_messages,
            on_connect_frames,
            connections,
            shutdown_tx,
            thread: Some(thread),
        })
    }

    /// Panicking convenience wrapper around [`Self::try_start`] for tests.
    pub fn start() -> Self {
        Self::try_start().expect("bind loopback MockWsServer")
    }

    /// Ephemeral loopback port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Loopback WebSocket URL (`ws://127.0.0.1:<port>`).
    pub fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }

    /// Total accepted WebSocket connections since server start.
    pub fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::SeqCst)
    }

    /// Currently connected WebSocket clients.
    pub fn active_connection_count(&self) -> usize {
        self.active_connection_count.load(Ordering::SeqCst)
    }

    /// Snapshot of all raw text frames received from clients.
    pub fn received_messages(&self) -> Vec<String> {
        self.received_messages.lock().clone()
    }

    /// Snapshot of client text frames successfully parsed as JSON.
    pub fn received_json(&self) -> Vec<Value> {
        self.received_messages
            .lock()
            .iter()
            .filter_map(|raw| serde_json::from_str::<Value>(raw).ok())
            .collect()
    }

    /// Clears recorded client messages.
    pub fn clear_received(&self) {
        self.received_messages.lock().clear();
    }

    /// Controls automatic replies to WS `Ping` frames and JSON `{"method":"ping"}` frames.
    pub fn set_auto_pong(&self, enabled: bool) {
        self.auto_pong.store(enabled, Ordering::Release);
    }

    /// Configures JSON frames pushed automatically upon each new WS connection.
    pub fn set_on_connect_frames(&self, frames: Vec<Value>) {
        let mut guard = self.on_connect_frames.lock();
        *guard = frames.into_iter().map(|v| v.to_string()).collect();
    }

    /// Drops the next `count` TCP connections before WS handshake (simulates transient failures).
    pub fn set_reject_next_handshakes(&self, count: usize) {
        self.reject_next_handshakes.store(count, Ordering::SeqCst);
    }

    /// Pushes a raw text frame to all currently connected clients.
    /// Returns the number of connections the frame was queued to.
    pub fn push_text(&self, text: impl Into<String>) -> usize {
        let txt = text.into();
        let senders: Vec<mpsc::UnboundedSender<WsServerCommand>> =
            self.connections.lock().values().cloned().collect();
        let mut sent = 0usize;
        for tx in senders {
            if tx.send(WsServerCommand::SendText(txt.clone())).is_ok() {
                sent += 1;
            }
        }
        sent
    }

    /// Serializes `value` as JSON and pushes it to all currently connected clients.
    pub fn push_json(&self, value: &Value) -> usize {
        self.push_text(value.to_string())
    }

    /// Gracefully closes all active WS connections (sends WS Close frame).
    pub fn disconnect_all(&self) {
        let senders: Vec<mpsc::UnboundedSender<WsServerCommand>> =
            self.connections.lock().values().cloned().collect();
        for tx in senders {
            let _ = tx.send(WsServerCommand::CloseGraceful);
        }
    }

    /// Abruptly terminates all active WS connections without sending a WS Close frame.
    pub fn abrupt_drop_all(&self) {
        let senders: Vec<mpsc::UnboundedSender<WsServerCommand>> =
            self.connections.lock().values().cloned().collect();
        for tx in senders {
            let _ = tx.send(WsServerCommand::DropAbrupt);
        }
    }

    /// Polls asynchronously until `connection_count() >= min_count` or `timeout` elapses.
    pub async fn wait_for_connections_async(&self, min_count: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.connection_count() >= min_count {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.connection_count() >= min_count
    }

    /// Polls asynchronously until at least `min_count` client text frames are recorded.
    pub async fn wait_for_received_async(&self, min_count: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.received_messages.lock().len() >= min_count {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.received_messages.lock().len() >= min_count
    }

    /// Blocking wait until `connection_count() >= min_count` or `timeout` elapses.
    pub fn wait_for_connections(&self, min_count: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.connection_count() >= min_count {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        self.connection_count() >= min_count
    }

    /// Blocking wait until at least `min_count` client text frames are recorded.
    pub fn wait_for_received(&self, min_count: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.received_messages.lock().len() >= min_count {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        self.received_messages.lock().len() >= min_count
    }
}

impl Drop for MockWsServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_mock_ws_subscribe_ping_push_and_reconnect() {
        let srv = MockWsServer::start();
        srv.set_on_connect_frames(vec![json!({
            "channel": "subscriptionResponse",
            "data": { "status": "ready" }
        })]);

        let (mut ws1, _) = tokio_tungstenite::connect_async(srv.ws_url())
            .await
            .expect("connect first ws client");

        // Receives scripted on_connect frame
        let first = ws1.next().await.expect("frame").expect("ok");
        assert!(first
            .to_text()
            .expect("text")
            .contains("subscriptionResponse"));

        // Send Hyperliquid subscribe + JSON ping
        ws1.send(Message::Text(
            json!({ "method": "subscribe", "subscription": { "type": "allMids" } }).to_string(),
        ))
        .await
        .expect("send sub");
        ws1.send(Message::Text(json!({ "method": "ping" }).to_string()))
            .await
            .expect("send ping");

        let pong_frame = ws1.next().await.expect("pong frame").expect("ok");
        assert_eq!(pong_frame.to_text().expect("text"), r#"{"channel":"pong"}"#);

        assert!(
            srv.wait_for_received_async(2, Duration::from_secs(1)).await,
            "server should record subscribe + ping"
        );
        let rec = srv.received_json();
        assert_eq!(rec[0]["method"], "subscribe");
        assert_eq!(rec[1]["method"], "ping");

        // Push live market JSON frame
        let pushed = srv.push_json(&json!({
            "channel": "allMids",
            "data": { "mids": { "BTC": "95000.0" } }
        }));
        assert_eq!(pushed, 1);
        let mids_frame = ws1.next().await.expect("mids frame").expect("ok");
        assert!(mids_frame.to_text().expect("text").contains("95000.0"));

        // Scripted disconnect + reconnect verification
        srv.disconnect_all();
        let closed = ws1.next().await;
        assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));

        let (mut ws2, _) = tokio_tungstenite::connect_async(srv.ws_url())
            .await
            .expect("reconnect ws client");
        let replay = ws2.next().await.expect("replayed frame").expect("ok");
        assert!(replay
            .to_text()
            .expect("text")
            .contains("subscriptionResponse"));
        assert!(
            srv.wait_for_connections_async(2, Duration::from_secs(1))
                .await
        );
        assert_eq!(srv.connection_count(), 2);
    }
}
