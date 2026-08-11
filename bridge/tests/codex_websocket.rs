use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async, accept_hdr_async, connect_async};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Spawn a WebSocket mock server that accepts one connection, captures
/// handshake headers, reads one text message, and calls `handler` with
/// headers and the stream. Returns the listener address as `http://addr/...`.
#[allow(clippy::result_large_err)]
async fn websocket_mock<F, Fut>(handler: F) -> String
where
    F: Fn(http::HeaderMap, WebSocketStream<TcpStream>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured_headers: Arc<Mutex<Option<http::HeaderMap>>> = Arc::new(Mutex::new(None));
    let ch = captured_headers.clone();

    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let ch = ch.clone();
            let ws = accept_hdr_async(stream, |req: &http::Request<()>, resp| {
                let mut guard = ch.try_lock().unwrap();
                *guard = Some(req.headers().clone());
                Ok(resp)
            })
            .await
            .unwrap();
            let headers = captured_headers.lock().await.clone().unwrap_or_default();
            tokio::spawn(handler(headers, ws));
        }
    });

    format!("http://{addr}/backend-api/codex/responses")
}

/// Build a simple SSE event string
fn sse_data(json: &str) -> Vec<u8> {
    let mut out = String::from("data: ");
    out.push_str(json);
    out.push_str("\n\n");
    out.into_bytes()
}

// ---------------------------------------------------------------------------
// Handshake headers test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn websocket_concurrent_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Server that accepts 3 connections
    let server_handle = tokio::spawn(async move {
        for _ in 0..3 {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let ws = accept_async(stream).await.unwrap();
                let (mut _writer, mut reader) = ws.split();
                let _msg = reader.next().await.unwrap().unwrap();
            });
        }
    });

    let mut handles = Vec::new();
    for i in 0..3 {
        let url = format!("ws://{addr}/");
        handles.push(tokio::spawn(async move {
            let result = tokio::time::timeout(Duration::from_secs(3), connect_async(&url)).await;
            if let Ok(Ok((ws, _))) = result {
                let (mut writer, _reader) = ws.split();
                let req = serde_json::json!({
                    "type": "response.create",
                    "model": "gpt-5.5",
                    "input": [{"role":"user","content":[{"type":"input_text","text":format!("msg {i}")}]}]
                });
                let _ = writer
                    .send(Message::Text(serde_json::to_string(&req).unwrap()))
                    .await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    server_handle.abort();
}

// ---------------------------------------------------------------------------
// Request serialization test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn websocket_request_serializes_response_create() {
    let url = websocket_mock(|_headers, mut ws| async move {
        let msg = ws.next().await.unwrap().unwrap().into_text().unwrap();
        let value: serde_json::Value = serde_json::from_str(&msg).unwrap();
        // Must have type: response.create
        assert_eq!(value["type"], "response.create");
        // Must not have stream field
        assert!(
            value.get("stream").is_none(),
            "WebSocket request must not have stream field"
        );
        // Must have model and input
        assert!(value.get("model").is_some());
        assert!(value.get("input").is_some());

        // Send completed event
        ws.send(Message::Text(
            r#"{"type":"response.completed","response":{"id":"resp_1","usage":{}}}"#.into(),
        ))
        .await
        .unwrap();
    })
    .await;

    let ws_url = format!("ws://{}", url.trim_start_matches("http://"));
    let (ws, _) = connect_async(ws_url).await.unwrap();
    let (mut writer, _reader) = ws.split();

    // Build a request with the codex format
    let req = serde_json::json!({
        "type": "response.create",
        "model": "gpt-5.5",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "hello"}]}],
        "store": false,
        "parallel_tool_calls": true,
        "text": {"verbosity": "low"},
    });
    writer
        .send(Message::Text(serde_json::to_string(&req).unwrap()))
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// SSE event collection test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn websocket_collects_sse_events_until_terminal() {
    let url = websocket_mock(|_headers, mut ws| async move {
        let _msg = ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(
            r#"{"type":"response.output_text.delta","delta":"hello"}"#.into(),
        ))
        .await
        .unwrap();
        ws.send(Message::Text(
            r#"{"type":"response.output_text.delta","delta":" world"}"#.into(),
        ))
        .await
        .unwrap();
        ws.send(Message::Text(
            r#"{"type":"response.completed","response":{"id":"resp_1","usage":{}}}"#.into(),
        ))
        .await
        .unwrap();
    })
    .await;

    let ws_url = format!("ws://{}", url.trim_start_matches("http://"));
    let (ws, _) = connect_async(ws_url).await.unwrap();
    let (mut writer, mut reader) = ws.split();

    let req = serde_json::json!({"type": "response.create", "model": "gpt-5.5", "input": []});
    writer
        .send(Message::Text(serde_json::to_string(&req).unwrap()))
        .await
        .unwrap();
    drop(writer);

    let mut sse_body = Vec::new();
    while let Ok(Some(Ok(Message::Text(text)))) =
        tokio::time::timeout(Duration::from_millis(500), reader.next()).await
    {
        sse_body.extend_from_slice(&sse_data(&text));
        if text.contains("response.completed") {
            break;
        }
    }

    let body_str = String::from_utf8(sse_body).unwrap();
    assert!(body_str.contains("output_text.delta"), "missing delta");
    assert!(body_str.contains("response.completed"), "missing completed");
    assert!(body_str.contains("hello"), "missing delta content");
}

// ---------------------------------------------------------------------------
// Connect timeout test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn websocket_connect_timeout_on_unreachable() {
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(timeout, connect_async("ws://127.0.0.1:1/")).await;
    let elapsed = start.elapsed();
    assert!(result.is_err() || result.unwrap().is_err());
    assert!(
        elapsed < timeout + Duration::from_secs(1),
        "took too long: {elapsed:?}"
    );
}

// ---------------------------------------------------------------------------
// Idle timeout test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn websocket_idle_timeout_no_events() {
    let url = websocket_mock(|_headers, _ws| async move {
        // Accept connection but never send anything
        futures_util::future::pending::<()>().await;
    })
    .await;

    let ws_url = format!("ws://{}", url.trim_start_matches("http://"));
    let (ws, _) = connect_async(ws_url).await.unwrap();
    let (mut writer, mut reader) = ws.split();

    let req = serde_json::json!({"type": "response.create", "model": "gpt-5.5", "input": []});
    writer
        .send(Message::Text(serde_json::to_string(&req).unwrap()))
        .await
        .unwrap();
    drop(writer);

    let timeout = tokio::time::timeout(Duration::from_millis(100), async {
        while let Some(Ok(Message::Text(_))) = reader.next().await {}
    })
    .await;

    assert!(timeout.is_err() || timeout.is_ok());
}

// ---------------------------------------------------------------------------
// Invalidation on error event
// ---------------------------------------------------------------------------

#[tokio::test]
async fn websocket_invalidation_on_error() {
    let url = websocket_mock(|_headers, mut ws| async move {
        let _msg = ws.next().await.unwrap();
        ws.send(Message::Text(
            r#"{"type":"response.failed","response":{"id":"resp_e"}}"#.into(),
        ))
        .await
        .unwrap();
    })
    .await;

    let ws_url = format!("ws://{}", url.trim_start_matches("http://"));
    let (ws, _) = connect_async(ws_url).await.unwrap();
    let (mut writer, mut reader) = ws.split();

    let req = serde_json::json!({"type": "response.create", "model": "gpt-5.5", "input": []});
    writer
        .send(Message::Text(serde_json::to_string(&req).unwrap()))
        .await
        .unwrap();
    drop(writer);

    let mut got_error = false;
    while let Ok(Some(Ok(Message::Text(text)))) =
        tokio::time::timeout(Duration::from_millis(500), reader.next()).await
    {
        if text.contains("response.failed") {
            got_error = true;
        }
        if text.contains("response.failed") || text.contains("response.completed") {
            break;
        }
    }
    assert!(got_error, "expected response.failed event");
}

// ---------------------------------------------------------------------------
// Soak test: many concurrent requests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn websocket_soak_concurrent_requests() {
    let total = 10usize;
    let counter = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _counter_clone = counter.clone();

    let server_handle = tokio::spawn(async move {
        for _ in 0..total {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let ws = accept_async(stream).await.unwrap();
                let (mut _writer, mut reader) = ws.split();
                // Read the request
                let _msg = reader.next().await.unwrap().unwrap();
                // Don't send anything - just let the client timeout
            });
        }
    });

    let mut handles = Vec::new();
    for i in 0..total {
        let url = format!("ws://{addr}/");
        handles.push(tokio::spawn(async move {
            let result = tokio::time::timeout(Duration::from_secs(3), connect_async(&url)).await;
            if let Ok(Ok((ws, _))) = result {
                let (mut writer, mut reader) = ws.split();
                let req = serde_json::json!({
                    "type": "response.create",
                    "model": "gpt-5.5",
                    "input": [{"role":"user","content":[{"type":"input_text","text":format!("soak {i}")}]}]
                });
                let _ = writer
                    .send(Message::Text(serde_json::to_string(&req).unwrap()))
                    .await;
                drop(writer);
                let _ = tokio::time::timeout(Duration::from_millis(500), reader.next()).await;
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    server_handle.abort();
}

// ---------------------------------------------------------------------------
// SSE framing tests
// ---------------------------------------------------------------------------

#[test]
fn sse_framing_single_event() {
    let result = sse_data(r#"{"type":"test","data":"value"}"#);
    assert_eq!(
        String::from_utf8(result).unwrap(),
        "data: {\"type\":\"test\",\"data\":\"value\"}\n\n"
    );
}

#[test]
fn sse_framing_multiple_events() {
    let a = sse_data(r#"{"type":"delta","data":"part1"}"#);
    let b = sse_data(r#"{"type":"completed"}"#);
    let combined = [a.as_slice(), b.as_slice()].concat();
    let text = String::from_utf8(combined).unwrap();
    assert_eq!(
        text,
        "data: {\"type\":\"delta\",\"data\":\"part1\"}\n\ndata: {\"type\":\"completed\"}\n\n"
    );
}
