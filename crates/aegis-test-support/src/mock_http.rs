//! Reusable loopback HTTP mock server (`aegis-test-support::mock_http`).
//!
//! Consolidates loopback `TcpListener` mocks used across workspace unit and integration tests
//! with request recording, header/status control, scripted responses, and RAII `Drop` teardown.

use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// Captured HTTP request recorded by [`MockHttpServer`].
#[derive(Debug, Clone)]
pub struct RecordedHttpRequest {
    pub method: String,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub body_text: String,
    pub json_body: Value,
}

/// HTTP response returned by a [`MockHttpServer`] handler.
#[derive(Debug, Clone)]
pub struct MockHttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl MockHttpResponse {
    /// Builds a JSON response with `Content-Type: application/json`.
    pub fn json(status: u16, value: &Value) -> Self {
        Self::json_with_headers(status, Vec::new(), value)
    }

    /// Builds a `200 OK` JSON response.
    pub fn ok_json(value: &Value) -> Self {
        Self::json(200, value)
    }

    /// Builds a JSON response with additional custom response headers.
    pub fn json_with_headers(status: u16, headers: Vec<(String, String)>, value: &Value) -> Self {
        Self {
            status,
            headers: merge_content_type(headers, "application/json"),
            body: value.to_string().into_bytes(),
        }
    }

    /// Builds a plain or custom `Content-Type` text response.
    pub fn text(status: u16, content_type: &str, body: impl Into<String>) -> Self {
        Self {
            status,
            headers: vec![("Content-Type".to_string(), content_type.to_string())],
            body: body.into().into_bytes(),
        }
    }

    /// Appends or replaces a response header.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        let value = value.into();
        self.headers.retain(|(k, _)| !k.eq_ignore_ascii_case(&name));
        self.headers.push((name, value));
        self
    }
}

fn merge_content_type(
    mut headers: Vec<(String, String)>,
    default_ct: &str,
) -> Vec<(String, String)> {
    if !headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
    {
        headers.insert(0, ("Content-Type".to_string(), default_ct.to_string()));
    }
    headers
}

fn status_reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        408 => "Request Timeout",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "OK",
    }
}

/// Reusable RAII loopback HTTP mock server bound to `127.0.0.1:0`.
///
/// Automatically signals shutdown and joins its background thread when dropped.
pub struct MockHttpServer {
    port: u16,
    stop: Arc<AtomicBool>,
    recorded: Arc<Mutex<Vec<RecordedHttpRequest>>>,
    thread: Option<JoinHandle<()>>,
}

impl MockHttpServer {
    /// Starts a mock server backed by a general request handler.
    pub fn try_start<F>(handler: F) -> std::io::Result<Self>
    where
        F: FnMut(&RecordedHttpRequest) -> MockHttpResponse + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        listener.set_nonblocking(true)?;

        let stop = Arc::new(AtomicBool::new(false));
        let stop_bg = Arc::clone(&stop);
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let recorded_bg = Arc::clone(&recorded);
        let handler = Arc::new(Mutex::new(handler));

        let thread = std::thread::Builder::new()
            .name(format!("aegis-mock-http-{port}"))
            .spawn(move || {
                while !stop_bg.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            if stop_bg.load(Ordering::Acquire) {
                                break;
                            }
                            if let Some(req) = read_http_request(&mut stream) {
                                recorded_bg.lock().push(req.clone());
                                let resp = (handler.lock())(&req);
                                write_http_response(&mut stream, &resp);
                            }
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            })?;

        Ok(Self {
            port,
            stop,
            recorded,
            thread: Some(thread),
        })
    }

    /// Panicking convenience wrapper around [`Self::try_start`] for tests.
    pub fn start<F>(handler: F) -> Self
    where
        F: FnMut(&RecordedHttpRequest) -> MockHttpResponse + Send + 'static,
    {
        Self::try_start(handler).expect("bind loopback MockHttpServer")
    }

    /// Starts a mock server using a `(path, &json_body) -> (status, Value)` closure.
    pub fn start_json<F>(mut handler: F) -> Self
    where
        F: FnMut(&str, &Value) -> (u16, Value) + Send + 'static,
    {
        Self::start(move |req| {
            let (status, val) = handler(&req.path, &req.json_body);
            MockHttpResponse::json(status, &val)
        })
    }

    /// Starts a mock server always returning the same static JSON status and body.
    pub fn start_static(status: u16, body: &Value) -> Self {
        let fixed = MockHttpResponse::json(status, body);
        Self::start(move |_| fixed.clone())
    }

    /// Starts a mock server returning scripted responses in FIFO order.
    /// Once exhausted, repeats the final scripted response (or `500` if empty).
    pub fn start_scripted<I>(responses: I) -> Self
    where
        I: IntoIterator<Item = MockHttpResponse>,
    {
        let mut queue: VecDeque<MockHttpResponse> = responses.into_iter().collect();
        let last_fallback = queue
            .back()
            .cloned()
            .unwrap_or_else(|| MockHttpResponse::text(500, "text/plain", "Mock queue empty"));
        Self::start(move |_| queue.pop_front().unwrap_or_else(|| last_fallback.clone()))
    }

    /// Ephemeral loopback port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Base HTTP URL (`http://127.0.0.1:<port>`).
    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Full HTTP URL for a path (`http://127.0.0.1:<port><path>`).
    pub fn url(&self, path: &str) -> String {
        if path.starts_with('/') {
            format!("http://127.0.0.1:{}{}", self.port, path)
        } else {
            format!("http://127.0.0.1:{}/{}", self.port, path)
        }
    }

    /// Snapshot of all recorded requests received so far.
    pub fn requests(&self) -> Vec<RecordedHttpRequest> {
        self.recorded.lock().clone()
    }

    /// Total number of requests received so far.
    pub fn request_count(&self) -> usize {
        self.recorded.lock().len()
    }

    /// Clears recorded requests.
    pub fn clear_requests(&self) {
        self.recorded.lock().clear();
    }
}

impl Drop for MockHttpServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let addr = SocketAddr::from(([127, 0, 0, 1], self.port));
        let _ = TcpStream::connect_timeout(&addr, Duration::from_millis(50));
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

fn read_http_request(stream: &mut TcpStream) -> Option<RecordedHttpRequest> {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));

    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 4096];
    let mut header_end = None;
    let mut content_len = 0usize;
    let mut is_chunked = false;

    loop {
        if let Some(hend) = header_end {
            if is_chunked {
                if decode_chunked(&buf[hend..]).is_some() {
                    break;
                }
            } else if buf.len() >= hend + content_len {
                break;
            }
        }
        match stream.read(&mut tmp) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                if header_end.is_none() {
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        header_end = Some(pos + 4);
                        let hdr_str = String::from_utf8_lossy(&buf[..pos]);
                        for line in hdr_str.lines().skip(1) {
                            if let Some((k, v)) = line.split_once(':') {
                                let kl = k.trim().to_ascii_lowercase();
                                let vv = v.trim();
                                if kl == "content-length" {
                                    content_len = vv.parse::<usize>().unwrap_or(0);
                                } else if kl == "transfer-encoding"
                                    && vv.to_ascii_lowercase().contains("chunked")
                                {
                                    is_chunked = true;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let hdr_end = header_end?;
    let hdr_str = String::from_utf8_lossy(&buf[..hdr_end]);
    let mut lines = hdr_str.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("GET").to_ascii_uppercase();
    let path = parts.next().unwrap_or("/").to_string();

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }

    let raw_body = &buf[hdr_end..];
    let body = if is_chunked {
        decode_chunked(raw_body).unwrap_or_else(|| raw_body.to_vec())
    } else {
        raw_body[..raw_body.len().min(content_len)].to_vec()
    };
    let body_text = String::from_utf8_lossy(&body).to_string();
    let json_body = serde_json::from_slice(&body).unwrap_or(Value::Null);

    Some(RecordedHttpRequest {
        method,
        path,
        headers,
        body,
        body_text,
        json_body,
    })
}

fn decode_chunked(raw: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut idx = 0usize;
    loop {
        let rest = raw.get(idx..)?;
        let line_end = rest.windows(2).position(|w| w == b"\r\n")?;
        let size_line = std::str::from_utf8(&rest[..line_end]).ok()?.trim();
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let chunk_len = usize::from_str_radix(size_hex, 16).ok()?;
        idx += line_end + 2;
        if chunk_len == 0 {
            if raw.get(idx..).is_some_and(|s| s.starts_with(b"\r\n")) {
                return Some(out);
            }
            return None;
        }
        let data_end = idx + chunk_len;
        if raw.len() < data_end + 2 {
            return None;
        }
        out.extend_from_slice(&raw[idx..data_end]);
        idx = data_end + 2;
    }
}

fn write_http_response(stream: &mut TcpStream, resp: &MockHttpResponse) {
    let reason = status_reason(resp.status);
    let mut head = format!("HTTP/1.1 {} {}\r\n", resp.status, reason);
    let mut has_content_length = false;
    let mut has_connection = false;
    for (k, v) in &resp.headers {
        if k.eq_ignore_ascii_case("content-length") {
            has_content_length = true;
        } else if k.eq_ignore_ascii_case("connection") {
            has_connection = true;
        }
        head.push_str(k);
        head.push_str(": ");
        head.push_str(v);
        head.push_str("\r\n");
    }
    if !has_content_length {
        head.push_str(&format!("Content-Length: {}\r\n", resp.body.len()));
    }
    if !has_connection {
        head.push_str("Connection: close\r\n");
    }
    head.push_str("\r\n");

    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&resp.body);
    let _ = stream.flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn send_raw_http(port: u16, raw_request: &str) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to mock http");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set timeout");
        stream
            .write_all(raw_request.as_bytes())
            .expect("write request");
        stream.flush().expect("flush");
        let mut resp = String::new();
        stream.read_to_string(&mut resp).expect("read response");
        resp
    }

    #[test]
    fn test_mock_http_scripted_recording_headers_and_drop() {
        let srv = MockHttpServer::start_scripted([
            MockHttpResponse::json(429, &json!({ "error": "rate_limited" }))
                .with_header("Retry-After", "1"),
            MockHttpResponse::ok_json(&json!({ "ok": true })),
        ]);
        let port = srv.port();

        let body1 = r#"{"type":"allMids"}"#;
        let req1 = format!(
            "POST /info HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nX-Custom: abc\r\nContent-Length: {}\r\n\r\n{}",
            body1.len(),
            body1
        );
        let resp1 = send_raw_http(port, &req1);
        assert!(resp1.starts_with("HTTP/1.1 429 Too Many Requests\r\n"));
        assert!(resp1.contains("Retry-After: 1\r\n"));
        assert!(resp1.contains(r#"{"error":"rate_limited"}"#));

        let req2 = format!(
            "POST /info HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nTransfer-Encoding: chunked\r\n\r\n11\r\n{{\"type\":\"l2Book\"}}\r\n0\r\n\r\n"
        );
        let resp2 = send_raw_http(port, &req2);
        assert!(resp2.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(resp2.contains(r#"{"ok":true}"#));

        let captured = srv.requests();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].method, "POST");
        assert_eq!(captured[0].path, "/info");
        assert_eq!(
            captured[0].headers.get("x-custom").map(String::as_str),
            Some("abc")
        );
        assert_eq!(captured[0].json_body["type"], "allMids");
        assert_eq!(captured[1].json_body["type"], "l2Book");

        drop(srv);
        assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
    }

    #[test]
    fn test_mock_http_json_closure() {
        let srv = MockHttpServer::start_json(|path, body| {
            if path == "/exchange" && body["action"]["type"] == "order" {
                (200, json!({ "status": "ok" }))
            } else {
                (400, json!({ "status": "err" }))
            }
        });
        let payload = r#"{"action":{"type":"order"}}"#;
        let req = format!(
            "POST /exchange HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        let resp = send_raw_http(srv.port(), &req);
        assert!(resp.contains(r#"{"status":"ok"}"#));
        assert_eq!(srv.request_count(), 1);
    }
}
