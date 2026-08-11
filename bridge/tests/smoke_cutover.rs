// End-to-end tests for local server health, provider routing, Kimi, Codex HTTP,
// and Codex WebSocket through in-process mock upstreams with isolated auth.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::response::Response;
use claude_code_proxy::providers::codex::compaction::clear_all_compactions_for_tests;
use claude_code_proxy::providers::codex::continuation::clear_all_continuations_for_tests;
use claude_code_proxy::providers::codex::websocket::clear_codex_websocket_pool_for_tests;
use claude_code_proxy::{
    registry::Registry,
    server::{app, app_with_options},
};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tower::util::ServiceExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Serialize all env-var-mutating tests so they never run concurrently.
fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    // Recover from a poisoned mutex so a failing test doesn't cascade
    let m = ENV_LOCK.get_or_init(|| Mutex::new(()));
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Write a valid auth.json for `provider` under `config_dir`.
fn write_auth(config_dir: &std::path::Path, provider: &str) {
    let dir = config_dir.join(provider);
    std::fs::create_dir_all(&dir).unwrap();
    let expires: i64 = 4102444800000;
    let auth = if provider == "codex" {
        json!({"access":"test-access","refresh":"test-refresh","expires":expires,"account_id":"acct_test"})
    } else {
        json!({"access":"test-access","refresh":"test-refresh","expires":expires,"scope":"openid","userId":"user_test"})
    };
    std::fs::write(dir.join("auth.json"), serde_json::to_vec(&auth).unwrap()).unwrap();
}

struct EnvGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvGuard {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        unsafe {
            match self.previous.take() {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

/// Send a minimal `POST /v1/messages` through the in-process app.
async fn call_messages(model: &str) -> Response {
    call_messages_body(json!({
        "model": model,
        "max_tokens": 64,
        "messages": [{"role":"user","content":"hello"}]
    }))
    .await
}

async fn call_messages_body(body: Value) -> Response {
    let _no_proxy_env = EnvGuard::set("NO_PROXY", "127.0.0.1,localhost");
    app(Arc::new(Registry::with_default_alias()))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .header("x-claude-code-session-id", "smoke-session")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn call_responses_body(body: Value) -> Response {
    let _no_proxy_env = EnvGuard::set("NO_PROXY", "127.0.0.1,localhost");
    app_with_options(Arc::new(Registry::with_default_alias()), None, true)
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .header("x-claude-code-session-id", "smoke-session")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

fn collect_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(collect_files(&path));
        } else {
            out.push(path);
        }
    }
    out
}

fn traffic_files(state_dir: &Path) -> Vec<PathBuf> {
    collect_files(
        &state_dir
            .join("claude-code-proxy")
            .join("traffic")
            .join("smoke-session"),
    )
}

fn traffic_file<'a>(files: &'a [PathBuf], suffix: &str) -> &'a Path {
    files
        .iter()
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(suffix))
        })
        .map(PathBuf::as_path)
        .unwrap_or_else(|| panic!("missing traffic artifact ending in {suffix}; files={files:?}"))
}

fn traffic_json(files: &[PathBuf], suffix: &str) -> Value {
    serde_json::from_slice(&std::fs::read(traffic_file(files, suffix)).unwrap()).unwrap()
}

/// Spawn a mock axum HTTP server that accepts requests at any path, calls
/// `handler(request_json)` and returns the handler's response body as a 200
/// with `content-type: text/event-stream`.
async fn spawn_http_upstream<F>(handler: F) -> String
where
    F: Fn(Value) -> Vec<u8> + Send + Sync + 'static,
{
    let handler = Arc::new(handler);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{addr}");

    let app = axum::Router::new().fallback({
        let handler = handler.clone();
        move |body: String| {
            let handler = handler.clone();
            async move {
                let json: Value = serde_json::from_str(&body).unwrap_or_default();
                let response_bytes = handler(json);
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(response_bytes))
                    .unwrap()
            }
        }
    });

    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    addr_str
}

#[allow(clippy::await_holding_lock)]
async fn assert_codex_http_presemantic_retry(first_response: Vec<u8>) {
    let _guard = env_lock();
    clear_all_continuations_for_tests();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let attempts = Arc::new(AtomicUsize::new(0));
    let request_bodies = Arc::new(Mutex::new(Vec::new()));
    let first_response = Arc::new(first_response);
    let upstream = spawn_http_upstream({
        let attempts = attempts.clone();
        let request_bodies = request_bodies.clone();
        let first_response = first_response.clone();
        move |body: Value| {
            request_bodies.lock().unwrap().push(body);
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                first_response.as_ref().clone()
            } else {
                concat!(
                    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_retry\"}}\n\n",
                    "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_retry\"}}\n\n",
                    "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"retry succeeded\"}\n\n",
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_retry\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n"
                )
                .as_bytes()
                .to_vec()
            }
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let response = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"hello"}]
    }))
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = tokio::time::timeout(
        Duration::from_secs(2),
        axum::body::to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("retried stream must finish")
    .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert_eq!(attempts.load(Ordering::SeqCst), 2, "stream body: {text}");
    let request_bodies = request_bodies.lock().unwrap();
    assert_eq!(request_bodies.len(), 2);
    assert_eq!(
        request_bodies[0], request_bodies[1],
        "HTTP startup retry must reuse the exact prepared semantic body"
    );
    assert!(text.contains("retry succeeded"), "stream body: {text}");
    assert!(!text.contains("event: error"), "stream body: {text}");
    assert_eq!(text.matches("event: message_start").count(), 1);
    assert_eq!(text.matches("event: message_stop").count(), 1);
}

/// Spawn a mock WebSocket server that accepts one connection, captures the
/// first text message, and responds with Codex WebSocket events that
/// accumulate to `"codex websocket ok"`.
async fn spawn_websocket_upstream(captured: Arc<Mutex<Option<Value>>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{addr}");

    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await
            && let Ok(ws) = tokio_tungstenite::accept_async(stream).await
        {
            let (mut sender, mut receiver) = ws.split();

            // Read the incoming response.create message
            if let Some(Ok(Message::Text(text))) = receiver.next().await
                && let Ok(json) = serde_json::from_str::<Value>(&text)
            {
                let _ = captured.lock().map(|mut g| *g = Some(json));
            }

            // Send Codex Responses events as WebSocket text messages
            let events = [
                r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_up"}}"#,
                r#"{"type":"response.output_text.delta","output_index":0,"delta":"codex websocket ok"}"#,
                r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"message"}}"#,
                r#"{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":5,"output_tokens":2}}}"#,
            ];

            for event in &events {
                let _ = sender.send(Message::Text(event.to_string())).await;
            }
        }
    });

    addr_str
}

/// Reproduce the Codex subscription-credit response observed in production:
/// the included window is exhausted, but usable credits remain and the model
/// still completes the response after the rate-limit snapshot.
async fn spawn_websocket_credited_rate_limit_upstream() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{addr}");

    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await
            && let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await
        {
            let _ = ws.next().await;
            let events = [
                r#"{"type":"codex.rate_limits","rate_limits":{"allowed":false,"limit_reached":true,"primary":{"used_percent":100,"window_minutes":10080,"reset_after_seconds":509821}},"credits":{"has_credits":true,"unlimited":false,"balance":null}}"#,
                r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_up"}}"#,
                r#"{"type":"response.output_text.delta","output_index":0,"delta":"credited codex ok"}"#,
                r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"message"}}"#,
                r#"{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":5,"output_tokens":2}}}"#,
            ];

            for event in &events {
                let _ = ws.send(Message::Text(event.to_string())).await;
            }
        }
    });

    addr_str
}

async fn spawn_websocket_delayed_terminal_upstream() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{addr}");

    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await
            && let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await
        {
            let _ = ws.next().await;
            let early_events = [
                r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_up"}}"#,
                r#"{"type":"response.output_text.delta","output_index":0,"delta":"early chunk"}"#,
            ];
            for event in &early_events {
                let _ = ws.send(Message::Text(event.to_string())).await;
            }

            tokio::time::sleep(Duration::from_secs(2)).await;

            let terminal_events = [
                r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"message"}}"#,
                r#"{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":5,"output_tokens":2}}}"#,
            ];
            for event in &terminal_events {
                let _ = ws.send(Message::Text(event.to_string())).await;
            }
        }
    });

    addr_str
}

async fn spawn_websocket_error_upstream(message: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{addr}");

    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await
            && let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await
        {
            let _ = ws.next().await;
            let event = json!({
                "type": "error",
                "status": 400,
                "error": {
                    "type": "invalid_request_error",
                    "param": "input",
                    "message": message
                }
            });
            let _ = ws.send(Message::Text(event.to_string())).await;
        }
    });

    addr_str
}

async fn spawn_websocket_sequence_upstream(captured: Arc<Mutex<Vec<Value>>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{addr}");

    tokio::spawn(async move {
        let texts = ["first", "second", "third"];
        let mut handled = 0usize;
        while handled < texts.len() {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                return;
            };
            let (mut sender, mut receiver) = ws.split();

            while handled < texts.len() {
                let Some(text) = (loop {
                    match receiver.next().await {
                        Some(Ok(Message::Text(text))) => break Some(text),
                        Some(Ok(Message::Ping(data))) => {
                            let _ = sender.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(_)) => {}
                        Some(Err(_)) | None => break None,
                    }
                }) else {
                    break;
                };
                if let Ok(json) = serde_json::from_str::<Value>(&text) {
                    let _ = captured.lock().map(|mut g| g.push(json));
                }

                let idx = handled;
                let response_text = texts[idx];
                let response_id = format!("resp_{}", idx + 1);
                let events = [
                    json!({
                        "type":"response.output_item.added",
                        "output_index":0,
                        "item":{"type":"message","id":format!("msg_up_{idx}")}
                    }),
                    json!({
                        "type":"response.output_text.delta",
                        "output_index":0,
                        "delta":response_text
                    }),
                    json!({
                        "type":"response.output_item.done",
                        "output_index":0,
                        "item":{"type":"message"}
                    }),
                    json!({
                        "type":"response.completed",
                        "response":{"id":response_id,"usage":{"input_tokens":5,"output_tokens":2}}
                    }),
                ];

                for event in &events {
                    let _ = sender.send(Message::Text(event.to_string())).await;
                }
                handled += 1;
            }
        }
    });

    addr_str
}

async fn spawn_websocket_previous_missing_then_retry_upstream(
    captured: Arc<Mutex<Vec<Value>>>,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{addr}");

    tokio::spawn(async move {
        let mut handled = 0usize;
        while handled < 3 {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                return;
            };
            let (mut sender, mut receiver) = ws.split();

            while handled < 3 {
                let Some(text) = (loop {
                    match receiver.next().await {
                        Some(Ok(Message::Text(text))) => break Some(text),
                        Some(Ok(Message::Ping(data))) => {
                            let _ = sender.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(_)) => {}
                        Some(Err(_)) | None => break None,
                    }
                }) else {
                    break;
                };
                if let Ok(json) = serde_json::from_str::<Value>(&text) {
                    let _ = captured.lock().map(|mut g| g.push(json));
                }

                if handled == 1 {
                    let rate_limits = json!({
                        "type": "codex.rate_limits",
                        "rate_limits": {"limit_reached": false}
                    });
                    let _ = sender.send(Message::Text(rate_limits.to_string())).await;
                    let event = json!({
                        "type": "error",
                        "error": {
                            "code": "previous_response_not_found",
                            "message": "previous response not found",
                            "status": 400
                        }
                    });
                    let _ = sender.send(Message::Text(event.to_string())).await;
                    handled += 1;
                    break;
                }

                let response_text = if handled == 0 { "first" } else { "retry" };
                let response_id = if handled == 0 { "resp_1" } else { "resp_retry" };
                let events = [
                    json!({
                        "type":"response.output_item.added",
                        "output_index":0,
                        "item":{"type":"message","id":format!("msg_up_{handled}")}
                    }),
                    json!({
                        "type":"response.output_text.delta",
                        "output_index":0,
                        "delta":response_text
                    }),
                    json!({
                        "type":"response.output_item.done",
                        "output_index":0,
                        "item":{"type":"message"}
                    }),
                    json!({
                        "type":"response.completed",
                        "response":{"id":response_id,"usage":{"input_tokens":5,"output_tokens":2}}
                    }),
                ];

                for event in &events {
                    let _ = sender.send(Message::Text(event.to_string())).await;
                }
                handled += 1;
            }
        }
    });

    addr_str
}

async fn spawn_websocket_close_then_retry_upstream(captured: Arc<Mutex<Vec<Value>>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{addr}");

    tokio::spawn(async move {
        let mut handled = 0usize;
        while handled < 3 {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                return;
            };
            let (mut sender, mut receiver) = ws.split();

            while handled < 3 {
                let Some(text) = (loop {
                    match receiver.next().await {
                        Some(Ok(Message::Text(text))) => break Some(text),
                        Some(Ok(Message::Ping(data))) => {
                            let _ = sender.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(_)) => {}
                        Some(Err(_)) | None => break None,
                    }
                }) else {
                    break;
                };
                if let Ok(json) = serde_json::from_str::<Value>(&text) {
                    let _ = captured.lock().map(|mut g| g.push(json));
                }

                if handled == 1 {
                    handled += 1;
                    let _ = sender.close().await;
                    break;
                }

                let response_text = if handled == 0 { "first" } else { "retry" };
                let response_id = if handled == 0 { "resp_1" } else { "resp_retry" };
                let events = [
                    json!({
                        "type":"response.output_item.added",
                        "output_index":0,
                        "item":{"type":"message","id":format!("msg_close_{handled}")}
                    }),
                    json!({
                        "type":"response.output_text.delta",
                        "output_index":0,
                        "delta":response_text
                    }),
                    json!({
                        "type":"response.output_item.done",
                        "output_index":0,
                        "item":{"type":"message"}
                    }),
                    json!({
                        "type":"response.completed",
                        "response":{"id":response_id,"usage":{"input_tokens":5,"output_tokens":2}}
                    }),
                ];

                for event in &events {
                    let _ = sender.send(Message::Text(event.to_string())).await;
                }
                handled += 1;
            }
        }
    });

    addr_str
}

async fn spawn_websocket_empty_completion_then_retry_upstream(
    captured: Arc<Mutex<Vec<Value>>>,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{addr}");

    tokio::spawn(async move {
        let mut handled = 0usize;
        while handled < 3 {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                return;
            };
            let (mut sender, mut receiver) = ws.split();

            while handled < 3 {
                let Some(text) = (loop {
                    match receiver.next().await {
                        Some(Ok(Message::Text(text))) => break Some(text),
                        Some(Ok(Message::Ping(data))) => {
                            let _ = sender.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(_)) => {}
                        Some(Err(_)) | None => break None,
                    }
                }) else {
                    break;
                };
                if let Ok(json) = serde_json::from_str::<Value>(&text) {
                    let _ = captured.lock().map(|mut g| g.push(json));
                }

                if handled == 1 {
                    let event = json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp_empty",
                            "status": "completed",
                            "incomplete_details": null,
                            "usage": {"input_tokens": 5, "output_tokens": 0}
                        }
                    });
                    let _ = sender.send(Message::Text(event.to_string())).await;
                    handled += 1;
                    continue;
                }

                let response_text = if handled == 0 { "first" } else { "retry" };
                let response_id = if handled == 0 { "resp_1" } else { "resp_retry" };
                let events = [
                    json!({
                        "type":"response.output_item.added",
                        "output_index":0,
                        "item":{"type":"message","id":format!("msg_empty_{handled}")}
                    }),
                    json!({
                        "type":"response.output_text.delta",
                        "output_index":0,
                        "delta":response_text
                    }),
                    json!({
                        "type":"response.output_item.done",
                        "output_index":0,
                        "item":{"type":"message"}
                    }),
                    json!({
                        "type":"response.completed",
                        "response":{"id":response_id,"usage":{"input_tokens":5,"output_tokens":2}}
                    }),
                ];

                for event in &events {
                    let _ = sender.send(Message::Text(event.to_string())).await;
                }
                handled += 1;
            }
        }
    });

    addr_str
}

/// Upstream that answers every request with a terminal-only completion,
/// so the proxy's bounded retry loop always exhausts.
async fn spawn_websocket_always_empty_completion_upstream(
    request_count: Arc<std::sync::atomic::AtomicUsize>,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{addr}");

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                return;
            };
            let (mut sender, mut receiver) = ws.split();
            let request_count = request_count.clone();

            tokio::spawn(async move {
                while let Some(message) = receiver.next().await {
                    match message {
                        Ok(Message::Text(_)) => {
                            request_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            let event = json!({
                                "type": "response.completed",
                                "response": {
                                    "id": "resp_empty",
                                    "status": "completed",
                                    "incomplete_details": null,
                                    "usage": {"input_tokens": 5, "output_tokens": 0}
                                }
                            });
                            if sender.send(Message::Text(event.to_string())).await.is_err() {
                                return;
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = sender.send(Message::Pong(data)).await;
                        }
                        Ok(_) => {}
                        Err(_) => return,
                    }
                }
            });
        }
    });

    addr_str
}

// ---------------------------------------------------------------------------
// Health and routing smoke tests (no env var mutation needed)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn smoke_healthz_returns_ok() {
    let app = app(Arc::new(Registry::with_default_alias()));
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap();
    assert_eq!(body, json!({"ok": true}));
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn smoke_codex_model_routes_to_real_provider() {
    let _guard = env_lock();
    let response = call_messages("gpt-5.5").await;
    // Should attempt auth (not return 501 placeholder)
    assert!(
        response.status() != StatusCode::NOT_IMPLEMENTED,
        "codex models must resolve to the real provider, not a placeholder"
    );
}

#[test]
fn smoke_kimi_model_is_registered() {
    // Kimi uses reqwest::blocking::Client internally, which panics when
    // dropped from an async context (it joins a dedicated runtime thread).
    // Test routing at the Registry level instead of through the HTTP stack.
    let registry = Registry::with_default_alias();
    let provider = registry.provider_for_model("kimi-for-coding", None);
    assert!(
        provider.is_some(),
        "kimi-for-coding must resolve to a registered provider"
    );
    assert_eq!(
        provider.unwrap().name(),
        "kimi",
        "kimi-for-coding must route to the kimi provider"
    );
}

// ---------------------------------------------------------------------------
// Kimi smoke: mock upstream verifies request shape and returns a valid
// streaming response. Uses multi-thread runtime because KimiHttpClient uses
// reqwest::blocking::Client internally.
// ---------------------------------------------------------------------------

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_kimi_messages_uses_mock_upstream() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "kimi");

    let captured = Arc::new(Mutex::new(None));
    let upstream = spawn_http_upstream({
        let captured = captured.clone();
        move |body: Value| {
            let _ = captured.lock().map(|mut g| *g = Some(body));
            concat!(
                "data: {\"choices\":[{\"delta\":{\"content\":\"kimi ok\"}}]}\n\n",
                "data: {\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
                "data: [DONE]\n\n"
            )
            .as_bytes()
            .to_vec()
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_KIMI_BASE_URL", &upstream);
    let _compaction_env = EnvGuard::set("CCP_CODEX_SERVER_COMPACTION", "1");
    let response = call_messages("kimi-for-coding").await;

    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value["content"][0]["text"], "kimi ok");

    let sent = captured.lock().unwrap().clone().unwrap();
    assert_eq!(sent["model"], "kimi-for-coding");
    assert_eq!(sent["stream"], true);
    assert!(sent.get("input").is_none());
    assert!(!sent.to_string().contains("compaction_trigger"));
}

// ---------------------------------------------------------------------------
// Codex HTTP smoke: mock upstream verifies request shape and returns
// Responses SSE events.
// ---------------------------------------------------------------------------

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_messages_uses_mock_upstream() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let captured = Arc::new(Mutex::new(None));
    let upstream = spawn_http_upstream({
        let captured = captured.clone();
        move |body: Value| {
            let _ = captured.lock().map(|mut g| *g = Some(body));
            concat!(
                "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_up\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"codex http ok\"}\n\n",
                "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n"
            )
            .as_bytes()
            .to_vec()
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let response = call_messages("gpt-5.5").await;

    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value["content"][0]["text"], "codex http ok");

    let sent = captured.lock().unwrap().clone().unwrap();
    assert_eq!(sent["model"], "gpt-5.5");
    assert_eq!(sent["stream"], true);
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_inline_compaction_replays_tool_state_across_provider_restart() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    let state_dir = config.path().join("inline-contexts");

    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = spawn_http_upstream({
        let captured = captured.clone();
        move |body: Value| {
            let request_number = {
                let mut requests = captured.lock().unwrap();
                requests.push(body);
                requests.len()
            };
            match request_number {
                1 => concat!(
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"opaque-inline-obsolete\"}}\n\n",
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"future_output_item\",\"opaque\":{\"drop\":\"subsumed-by-latest\"}}}\n\n",
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":2,\"item\":{\"type\":\"compaction\", \"encrypted_content\":\"opaque-inline-latest\",\"future_field\":{\"kept\":true}}}\n\n",
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":3,\"item\":{\"type\":\"future_output_item\",\"opaque\":{\"kept\":\"exactly\"}}}\n\n",
                    "data: {\"type\":\"response.output_item.added\",\"output_index\":4,\"item\":{\"type\":\"function_call\",\"call_id\":\"call_inline_1\",\"name\":\"lookup\"}}\n\n",
                    "data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":4,\"arguments\":\"{\\\"key\\\":\\\"project\\\"}\"}\n\n",
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":4,\"item\":{\"type\":\"function_call\",\"call_id\":\"call_inline_1\",\"name\":\"lookup\",\"arguments\":\"{\\\"key\\\":\\\"project\\\"}\"}}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_inline_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":40000,\"output_tokens\":8}}}\n\n"
                )
                .as_bytes()
                .to_vec(),
                2 => concat!(
                    "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_inline_2\"}}\n\n",
                    "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"inline continuity ok\"}\n\n",
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"inline continuity ok\"}]}}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_inline_2\",\"status\":\"completed\",\"usage\":{\"input_tokens\":700,\"output_tokens\":4}}}\n\n"
                )
                .as_bytes()
                .to_vec(),
                3 => concat!(
                    "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_inline_3\"}}\n\n",
                    "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"third continuity ok\"}\n\n",
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"third continuity ok\"}]}}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_inline_3\",\"status\":\"completed\",\"usage\":{\"input_tokens\":800,\"output_tokens\":4}}}\n\n"
                )
                .as_bytes()
                .to_vec(),
                _ => panic!("inline compaction smoke made an unexpected upstream request"),
            }
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _threshold_env = EnvGuard::set("CCP_CODEX_INLINE_COMPACTION_THRESHOLD", "32768");
    let _state_dir_env = EnvGuard::set("CCP_CODEX_COMPACTION_STATE_DIR", &state_dir);
    let _legacy_compaction_env = EnvGuard::set("CCP_CODEX_SERVER_COMPACTION", "false");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "false");
    let tools = json!([{
        "name": "lookup",
        "description": "Return a remembered value",
        "input_schema": {
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"]
        }
    }]);
    let first = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "system": "Keep the project sentinel and use tools exactly.",
        "messages": [{"role":"user","content":"project sentinel: alpha"}],
        "tools": tools,
        "tool_choice": {"type":"tool","name":"lookup"}
    }))
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_value: Value = serde_json::from_slice(
        &axum::body::to_bytes(first.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first_value["stop_reason"], "tool_use");
    assert_eq!(first_value["content"][0]["id"], "call_inline_1");

    // call_messages_body constructs a fresh app/provider on every invocation,
    // so this second request proves that the persisted sidecar survives a
    // bridge process-equivalent restart.
    let second = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "system": "Keep the project sentinel and use tools exactly.",
        "messages": [
            {"role":"user","content":"project sentinel: alpha"},
            {"role":"assistant","content":[{
                "type":"tool_use",
                "id":"call_inline_1",
                "name":"lookup",
                "input":{"key":"project"}
            }]},
            {"role":"user","content":[{
                "type":"tool_result",
                "tool_use_id":"call_inline_1",
                "content":"tool result: alpha"
            }]}
        ],
        "tools": tools,
        "tool_choice": {"type":"auto"}
    }))
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_value: Value = serde_json::from_slice(
        &axum::body::to_bytes(second.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(second_value["content"][0]["text"], "inline continuity ok");

    // A third provider-equivalent restart must retain the tool result and
    // assistant output that were added after the compaction boundary.
    let third = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "system": "Keep the project sentinel and use tools exactly.",
        "messages": [
            {"role":"user","content":"project sentinel: alpha"},
            {"role":"assistant","content":[{
                "type":"tool_use",
                "id":"call_inline_1",
                "name":"lookup",
                "input":{"key":"project"}
            }]},
            {"role":"user","content":[{
                "type":"tool_result",
                "tool_use_id":"call_inline_1",
                "content":"tool result: alpha"
            }]},
            {"role":"assistant","content":"inline continuity ok"},
            {"role":"user","content":"verify the third turn"}
        ],
        "tools": tools,
        "tool_choice": {"type":"auto"}
    }))
    .await;
    assert_eq!(third.status(), StatusCode::OK);
    let third_value: Value = serde_json::from_slice(
        &axum::body::to_bytes(third.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(third_value["content"][0]["text"], "third continuity ok");

    let requests = captured.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[0]["context_management"],
        json!([{"type":"compaction","compact_threshold":32768}])
    );
    assert_eq!(requests[0]["model"], "gpt-5.6-sol");
    assert!(
        requests[0].get("client_metadata").is_none(),
        "inline compaction must use the full Responses lane"
    );
    assert!(
        requests[0]["include"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "reasoning.encrypted_content")
    );
    assert!(requests[0].get("previous_response_id").is_none());
    let replay = requests[1]["input"].as_array().unwrap();
    assert_eq!(
        replay
            .iter()
            .map(|item| item["type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "compaction",
            "future_output_item",
            "function_call",
            "function_call_output"
        ]
    );
    assert_eq!(replay[0]["future_field"]["kept"], true);
    assert_eq!(replay[0]["encrypted_content"], "opaque-inline-latest");
    assert_eq!(replay[1]["opaque"]["kept"], "exactly");
    assert!(replay.iter().all(|item| {
        item["encrypted_content"] != "opaque-inline-obsolete"
            && item["opaque"]["drop"] != "subsumed-by-latest"
    }));
    assert_eq!(replay[2]["call_id"], "call_inline_1");
    assert_eq!(replay[3]["call_id"], "call_inline_1");
    assert!(requests[1].get("previous_response_id").is_none());
    let third_replay = requests[2]["input"].as_array().unwrap();
    assert_eq!(
        third_replay
            .iter()
            .map(|item| item["type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "compaction",
            "future_output_item",
            "function_call",
            "function_call_output",
            "message",
            "message"
        ]
    );
    assert_eq!(third_replay[2]["call_id"], "call_inline_1");
    assert_eq!(third_replay[3]["call_id"], "call_inline_1");
    assert_eq!(third_replay[4]["role"], "assistant");
    assert_eq!(third_replay[5]["role"], "user");
    assert!(requests[2].get("previous_response_id").is_none());
    assert_eq!(collect_files(&state_dir).len(), 1);
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_inline_compaction_uses_old_opaque_only_to_build_portable_summary() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    let state_dir = config.path().join("inline-portable-contexts");

    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = spawn_http_upstream({
        let captured = captured.clone();
        move |body: Value| {
            let request_number = {
                let mut requests = captured.lock().unwrap();
                requests.push(body);
                requests.len()
            };
            match request_number {
                1 => format!(
                    "{}\n\n",
                    [
                        r#"data: {"type":"response.output_item.done","output_index":0,"item":{"type":"compaction","encrypted_content":"opaque-before-portable"}}"#,
                        r#"data: {"type":"response.output_item.added","output_index":1,"item":{"type":"message","id":"msg_portable_seed"}}"#,
                        r#"data: {"type":"response.output_text.delta","output_index":1,"delta":"seed ok"}"#,
                        r#"data: {"type":"response.output_item.done","output_index":1,"item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"seed ok"}]}}"#,
                        r#"data: {"type":"response.completed","response":{"id":"resp_portable_1","status":"completed","usage":{"input_tokens":40000,"output_tokens":4}}}"#,
                    ]
                    .join("\n\n")
                )
                .into_bytes(),
                2 => format!(
                    "{}\n\n",
                    [
                        r#"data: {"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_portable_summary"}}"#,
                        r#"data: {"type":"response.output_text.delta","output_index":0,"delta":"portable summary"}"#,
                        r#"data: {"type":"response.output_item.done","output_index":0,"item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"portable summary"}]}}"#,
                        r#"data: {"type":"response.completed","response":{"id":"resp_portable_2","status":"completed","usage":{"input_tokens":900,"output_tokens":4}}}"#,
                    ]
                    .join("\n\n")
                )
                .into_bytes(),
                3 => format!(
                    "{}\n\n",
                    [
                        r#"data: {"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_portable_continue"}}"#,
                        r#"data: {"type":"response.output_text.delta","output_index":0,"delta":"portable continue ok"}"#,
                        r#"data: {"type":"response.output_item.done","output_index":0,"item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"portable continue ok"}]}}"#,
                        r#"data: {"type":"response.completed","response":{"id":"resp_portable_3","status":"completed","usage":{"input_tokens":700,"output_tokens":4}}}"#,
                    ]
                    .join("\n\n")
                )
                .into_bytes(),
                _ => panic!("portable inline smoke made an unexpected upstream request"),
            }
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _threshold_env = EnvGuard::set("CCP_CODEX_INLINE_COMPACTION_THRESHOLD", "32768");
    let _state_dir_env = EnvGuard::set("CCP_CODEX_COMPACTION_STATE_DIR", &state_dir);
    let _legacy_compaction_env = EnvGuard::set("CCP_CODEX_SERVER_COMPACTION", "false");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "false");

    let first = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "system": "Keep the original seed.",
        "messages": [{"role":"user","content":"original seed"}]
    }))
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_value: Value = serde_json::from_slice(
        &axum::body::to_bytes(first.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first_value["content"][0]["text"], "seed ok");

    let portable = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "system": "You are a helpful AI assistant tasked with summarizing conversations.",
        "messages": [
            {"role":"user","content":"original seed"},
            {"role":"assistant","content":"seed ok"},
            {"role":"user","content":concat!(
                "CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.\n\n",
                "Your task is to create a detailed summary of the conversation so far."
            )}
        ]
    }))
    .await;
    assert_eq!(portable.status(), StatusCode::OK);
    let portable_value: Value = serde_json::from_slice(
        &axum::body::to_bytes(portable.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(portable_value["content"][0]["text"], "portable summary");

    let next = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "system": "Continue from the portable transcript.",
        "messages": [
            {"role":"user","content":"<summary>portable summary</summary>"},
            {"role":"user","content":"continue"}
        ]
    }))
    .await;
    assert_eq!(next.status(), StatusCode::OK);
    let next_value: Value = serde_json::from_slice(
        &axum::body::to_bytes(next.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(next_value["content"][0]["text"], "portable continue ok");

    let requests = captured.lock().unwrap();
    assert_eq!(requests.len(), 3);
    let portable_input = requests[1]["input"].as_array().unwrap();
    assert_eq!(
        portable_input
            .iter()
            .map(|item| item["type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["compaction", "message", "message"]
    );
    assert_eq!(
        portable_input[0]["encrypted_content"],
        "opaque-before-portable"
    );
    assert!(!requests[1].to_string().contains("original seed"));
    assert!(
        requests[1]
            .to_string()
            .contains("CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.")
    );
    assert!(
        requests[1]
            .to_string()
            .contains("Your task is to create a detailed summary of the conversation so far.")
    );
    assert!(!requests[2].to_string().contains("opaque-before-portable"));
    assert!(requests[2].to_string().contains("portable summary"));
    assert_eq!(
        collect_files(&state_dir)
            .iter()
            .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
            .count(),
        0
    );
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_inline_compaction_accepts_claude_edit_false_default_without_rewriting_raw_call() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    let state_dir = config.path().join("inline-edit-contexts");

    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = spawn_http_upstream({
        let captured = captured.clone();
        move |body: Value| {
            let request_number = {
                let mut requests = captured.lock().unwrap();
                requests.push(body);
                requests.len()
            };
            match request_number {
                1 => concat!(
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"opaque-edit-default\"}}\n\n",
                    "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"call_id\":\"call_edit_default\",\"name\":\"Edit\"}}\n\n",
                    "data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":1,\"arguments\":\"{\\\"file_path\\\":\\\"pipeline.py\\\",\\\"old_string\\\":\\\"before\\\",\\\"new_string\\\":\\\"after\\\"}\"}\n\n",
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"call_id\":\"call_edit_default\",\"name\":\"Edit\",\"arguments\":\"{\\\"file_path\\\":\\\"pipeline.py\\\",\\\"old_string\\\":\\\"before\\\",\\\"new_string\\\":\\\"after\\\"}\"}}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_edit_default_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":40000,\"output_tokens\":8}}}\n\n"
                )
                .as_bytes()
                .to_vec(),
                2 => concat!(
                    "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_edit_default_2\"}}\n\n",
                    "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"edit continuity ok\"}\n\n",
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"edit continuity ok\"}]}}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_edit_default_2\",\"status\":\"completed\",\"usage\":{\"input_tokens\":700,\"output_tokens\":4}}}\n\n"
                )
                .as_bytes()
                .to_vec(),
                _ => panic!("Edit default smoke made an unexpected upstream request"),
            }
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _threshold_env = EnvGuard::set("CCP_CODEX_INLINE_COMPACTION_THRESHOLD", "32768");
    let _state_dir_env = EnvGuard::set("CCP_CODEX_COMPACTION_STATE_DIR", &state_dir);
    let _legacy_compaction_env = EnvGuard::set("CCP_CODEX_SERVER_COMPACTION", "false");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "false");

    let tools = json!([{
        "name": "Edit",
        "description": "Replace exact text in a file",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string"},
                "old_string": {"type": "string"},
                "new_string": {"type": "string"},
                "replace_all": {"type": "boolean", "default": false}
            },
            "required": ["file_path", "old_string", "new_string"]
        }
    }]);
    let first = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "system": "Use the Edit tool exactly.",
        "messages": [{"role":"user","content":"update the synthetic file"}],
        "tools": tools,
        "tool_choice": {"type":"tool","name":"Edit"}
    }))
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_value: Value = serde_json::from_slice(
        &axum::body::to_bytes(first.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first_value["stop_reason"], "tool_use");
    assert!(
        first_value["content"][0]["input"]
            .get("replace_all")
            .is_none()
    );

    let second = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "system": "Use the Edit tool exactly.",
        "messages": [
            {"role":"user","content":"update the synthetic file"},
            {"role":"assistant","content":[{
                "type":"tool_use",
                "id":"call_edit_default",
                "name":"Edit",
                "input":{
                    "file_path":"pipeline.py",
                    "old_string":"before",
                    "new_string":"after",
                    "replace_all":false
                }
            }]},
            {"role":"user","content":[{
                "type":"tool_result",
                "tool_use_id":"call_edit_default",
                "content":"edited"
            }]}
        ],
        "tools": tools,
        "tool_choice": {"type":"auto"}
    }))
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_value: Value = serde_json::from_slice(
        &axum::body::to_bytes(second.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(second_value["content"][0]["text"], "edit continuity ok");

    let requests = captured.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let replay = requests[1]["input"].as_array().unwrap();
    assert_eq!(
        replay
            .iter()
            .map(|item| item["type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["compaction", "function_call", "function_call_output"]
    );
    let replay_arguments: Value =
        serde_json::from_str(replay[1]["arguments"].as_str().unwrap()).unwrap();
    assert!(replay_arguments.get("replace_all").is_none());
    assert_eq!(replay[1]["call_id"], "call_edit_default");
    assert_eq!(replay[2]["call_id"], "call_edit_default");
    assert_eq!(collect_files(&state_dir).len(), 1);
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_inline_compaction_state_directory_rejects_a_second_server_writer() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    let state_dir = config.path().join("writer-lock-state");
    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _threshold_env = EnvGuard::set("CCP_CODEX_INLINE_COMPACTION_THRESHOLD", "32768");
    let _state_dir_env = EnvGuard::set("CCP_CODEX_COMPACTION_STATE_DIR", &state_dir);
    let _legacy_compaction_env = EnvGuard::set("CCP_CODEX_SERVER_COMPACTION", "false");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "false");

    let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let first_server = tokio::spawn(async move {
        claude_code_proxy::server::serve_listener(first_listener, None, async move {
            let _ = shutdown_rx.await;
        })
        .await
    });
    for _ in 0..100 {
        if state_dir.join(".writer.lock").is_file() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(state_dir.join(".writer.lock").is_file());
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let error = claude_code_proxy::server::serve_listener(
        second_listener,
        None,
        std::future::pending::<()>(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("already owned"));

    let _ = shutdown_tx.send(());
    first_server.await.unwrap().unwrap();

    let third_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    claude_code_proxy::server::serve_listener(third_listener, None, async {})
        .await
        .unwrap();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_inline_compaction_queues_one_lane_but_allows_agent_lanes() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    let state_dir = config.path().join("lane-lock-state");
    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _threshold_env = EnvGuard::set("CCP_CODEX_INLINE_COMPACTION_THRESHOLD", "32768");
    let _state_dir_env = EnvGuard::set("CCP_CODEX_COMPACTION_STATE_DIR", &state_dir);
    let _legacy_compaction_env = EnvGuard::set("CCP_CODEX_SERVER_COMPACTION", "false");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "false");
    let _session_concurrency_env = EnvGuard::set("CCP_CODEX_SESSION_MAX_CONCURRENT_REQUESTS", "4");
    let _session_queue_timeout_env = EnvGuard::set("CCP_CODEX_SESSION_QUEUE_TIMEOUT_SECS", "30");
    let _no_proxy_env = EnvGuard::set("NO_PROXY", "127.0.0.1,localhost");

    let successful_sse = || {
        concat!(
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_lane\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"ok\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_lane\",\"status\":\"completed\",\"usage\":{\"input_tokens\":100,\"output_tokens\":1}}}\n\n"
        )
        .as_bytes()
        .to_vec()
    };
    let request = |model: &str, agent: Option<&str>| {
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri("/v1/messages")
            .header("content-type", "application/json")
            .header("x-claude-code-session-id", "lane-session");
        if let Some(agent) = agent {
            builder = builder.header("x-claude-code-agent-id", agent);
        }
        builder
            .body(Body::from(
                json!({
                    "model": model,
                    "max_tokens": 64,
                    "messages": [{"role":"user","content":"lane test"}]
                })
                .to_string(),
            ))
            .unwrap()
    };

    let entered = Arc::new(tokio::sync::Barrier::new(2));
    let release = Arc::new(tokio::sync::Notify::new());
    let upstream_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let first_upstream = {
        let entered = entered.clone();
        let release = release.clone();
        let upstream_calls = upstream_calls.clone();
        let successful_sse = successful_sse();
        axum::Router::new().fallback(move |_body: String| {
            let entered = entered.clone();
            let release = release.clone();
            let upstream_calls = upstream_calls.clone();
            let successful_sse = successful_sse.clone();
            async move {
                let call_index = upstream_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if call_index == 0 {
                    entered.wait().await;
                    release.notified().await;
                }
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(successful_sse))
                    .unwrap()
            }
        })
    };
    tokio::spawn(async move {
        axum::serve(listener, first_upstream).await.ok();
    });
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let shared_app = app(Arc::new(Registry::with_default_alias()));
    let first_app = shared_app.clone();
    let first_request = tokio::spawn(async move {
        first_app
            .oneshot(request("gpt-5.6-luna", None))
            .await
            .unwrap()
    });
    entered.wait().await;
    let second_app = shared_app.clone();
    let second_request = tokio::spawn(async move {
        second_app
            .oneshot(request("gpt-5.6-sol", None))
            .await
            .unwrap()
    });
    tokio::task::yield_now().await;
    assert!(!second_request.is_finished());
    assert_eq!(upstream_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    release.notify_waiters();
    assert_eq!(first_request.await.unwrap().status(), StatusCode::OK);
    assert_eq!(second_request.await.unwrap().status(), StatusCode::OK);
    assert_eq!(upstream_calls.load(std::sync::atomic::Ordering::SeqCst), 2);

    let parallel_barrier = Arc::new(tokio::sync::Barrier::new(5));
    let parallel_release = Arc::new(tokio::sync::Notify::new());
    let parallel_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let max_active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let parallel_upstream = format!("http://{}", listener.local_addr().unwrap());
    let parallel_app = {
        let barrier = parallel_barrier.clone();
        let release = parallel_release.clone();
        let calls = parallel_calls.clone();
        let active = active.clone();
        let max_active = max_active.clone();
        let successful_sse = successful_sse();
        axum::Router::new().fallback(move |_body: String| {
            let barrier = barrier.clone();
            let release = release.clone();
            let calls = calls.clone();
            let active = active.clone();
            let max_active = max_active.clone();
            let successful_sse = successful_sse.clone();
            async move {
                let call_index = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let now = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                max_active.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                if call_index < 4 {
                    barrier.wait().await;
                    release.notified().await;
                }
                active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(successful_sse))
                    .unwrap()
            }
        })
    };
    tokio::spawn(async move {
        axum::serve(listener, parallel_app).await.ok();
    });
    drop(_base_url_env);
    let _parallel_base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &parallel_upstream);
    let agent_app = app(Arc::new(Registry::with_default_alias()));
    let mut agents = Vec::new();
    for agent_id in ["agent-a", "agent-b", "agent-c", "agent-d", "agent-e"] {
        let request_app = agent_app.clone();
        agents.push(tokio::spawn(async move {
            request_app
                .oneshot(request("gpt-5.6-sol", Some(agent_id)))
                .await
                .unwrap()
        }));
    }
    parallel_barrier.wait().await;
    tokio::task::yield_now().await;
    assert_eq!(parallel_calls.load(std::sync::atomic::Ordering::SeqCst), 4);
    assert_eq!(max_active.load(std::sync::atomic::Ordering::SeqCst), 4);
    parallel_release.notify_waiters();
    for agent in agents {
        assert_eq!(agent.await.unwrap().status(), StatusCode::OK);
    }
    assert_eq!(parallel_calls.load(std::sync::atomic::Ordering::SeqCst), 5);
    assert_eq!(max_active.load(std::sync::atomic::Ordering::SeqCst), 4);
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_upstream_capacity_reserves_one_control_slot() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _global_capacity_env =
        EnvGuard::set("CCP_CODEX_UPSTREAM_GLOBAL_MAX_CONCURRENT_REQUESTS", "3");
    let _data_capacity_env = EnvGuard::set("CCP_CODEX_UPSTREAM_DATA_MAX_CONCURRENT_REQUESTS", "2");
    let _control_capacity_env =
        EnvGuard::set("CCP_CODEX_UPSTREAM_CONTROL_MAX_CONCURRENT_REQUESTS", "1");
    let _queue_timeout_env = EnvGuard::set("CCP_CODEX_UPSTREAM_QUEUE_TIMEOUT_SECS", "5");

    let data_calls = Arc::new(AtomicUsize::new(0));
    let control_calls = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let release_data = Arc::new(AtomicBool::new(false));
    let release_notify = Arc::new(tokio::sync::Notify::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let upstream_app = {
        let data_calls = data_calls.clone();
        let control_calls = control_calls.clone();
        let active = active.clone();
        let max_active = max_active.clone();
        let release_data = release_data.clone();
        let release_notify = release_notify.clone();
        axum::Router::new().fallback(move |body: String| {
            let data_calls = data_calls.clone();
            let control_calls = control_calls.clone();
            let active = active.clone();
            let max_active = max_active.clone();
            let release_data = release_data.clone();
            let release_notify = release_notify.clone();
            async move {
                let body: Value = serde_json::from_str(&body).unwrap();
                let is_control = body["model"] == "codex-auto-review";
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_active.fetch_max(now, Ordering::SeqCst);
                if is_control {
                    control_calls.fetch_add(1, Ordering::SeqCst);
                } else {
                    data_calls.fetch_add(1, Ordering::SeqCst);
                    while !release_data.load(Ordering::SeqCst) {
                        release_notify.notified().await;
                    }
                }
                active.fetch_sub(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(concat!(
                        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_capacity\"}}\n\n",
                        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"ok\"}\n\n",
                        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
                        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_capacity\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n"
                    )))
                    .unwrap()
            }
        })
    };
    tokio::spawn(async move {
        axum::serve(listener, upstream_app).await.ok();
    });
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);

    let request = |session: &'static str, agent: &'static str| {
        Request::builder()
            .method(Method::POST)
            .uri("/v1/messages")
            .header("content-type", "application/json")
            .header("x-claude-code-session-id", session)
            .header("x-claude-code-agent-id", agent)
            .body(Body::from(
                json!({
                    "model": "gpt-5.6-terra",
                    "max_tokens": 64,
                    "stream": false,
                    "messages": [{"role":"user","content":"capacity test"}]
                })
                .to_string(),
            ))
            .unwrap()
    };
    let classifier = || {
        Request::builder()
            .method(Method::POST)
            .uri("/v1/messages")
            .header("content-type", "application/json")
            .header("x-claude-code-session-id", "capacity-review")
            .body(Body::from(
                json!({
                    "model": "gpt-5.6-sol",
                    "max_tokens": 64,
                    "stream": false,
                    "system": [{
                        "type": "text",
                        "text": "You are a security monitor for autonomous AI coding agents.\n\n## Context"
                    }],
                    "messages": [{"role":"user","content":"review this Bash command"}],
                    "tools": []
                })
                .to_string(),
            ))
            .unwrap()
    };

    let shared_app = app(Arc::new(Registry::with_default_alias()));
    let first = {
        let app = shared_app.clone();
        tokio::spawn(async move { app.oneshot(request("capacity-a", "agent-a")).await.unwrap() })
    };
    let second = {
        let app = shared_app.clone();
        tokio::spawn(async move { app.oneshot(request("capacity-b", "agent-b")).await.unwrap() })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while data_calls.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let third = {
        let app = shared_app.clone();
        tokio::spawn(async move { app.oneshot(request("capacity-c", "agent-c")).await.unwrap() })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(data_calls.load(Ordering::SeqCst), 2);
    assert!(!third.is_finished());

    let reviewer = {
        let app = shared_app.clone();
        tokio::spawn(async move { app.oneshot(classifier()).await.unwrap() })
    };
    let reviewer = tokio::time::timeout(Duration::from_secs(1), reviewer)
        .await
        .expect("control request must not wait behind the queued data request")
        .unwrap();
    assert_eq!(reviewer.status(), StatusCode::OK);
    assert_eq!(control_calls.load(Ordering::SeqCst), 1);
    assert_eq!(max_active.load(Ordering::SeqCst), 3);

    release_data.store(true, Ordering::SeqCst);
    release_notify.notify_waiters();
    for response in [
        first.await.unwrap(),
        second.await.unwrap(),
        third.await.unwrap(),
    ] {
        assert_eq!(response.status(), StatusCode::OK);
    }
    assert_eq!(data_calls.load(Ordering::SeqCst), 3);
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_native_responses_preserves_parallel_tool_calls() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let captured = Arc::new(Mutex::new(None));
    let upstream = spawn_http_upstream({
        let captured = captured.clone();
        move |body: Value| {
            let _ = captured.lock().map(|mut guard| *guard = Some(body));
            br#"{"id":"resp_1","object":"response","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}"#.to_vec()
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let response = call_responses_body(json!({
        "model":"gpt-5.4",
        "input":"hello",
        "parallel_tool_calls":false
    }))
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let sent = captured.lock().unwrap().clone().unwrap();
    assert_eq!(sent["parallel_tool_calls"], false);
}

/// Resets the retry-delay override even when the test panics, so later tests
/// in this process keep real backoff behavior.
struct ZeroRetryDelayGuard;

impl ZeroRetryDelayGuard {
    fn enable() -> Self {
        claude_code_proxy::retry::set_zero_retry_delay_for_tests(true);
        ZeroRetryDelayGuard
    }
}

impl Drop for ZeroRetryDelayGuard {
    fn drop(&mut self) {
        claude_code_proxy::retry::set_zero_retry_delay_for_tests(false);
    }
}

fn empty_completion_sse() -> Vec<u8> {
    concat!(
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_empty\",",
        "\"status\":\"completed\",\"incomplete_details\":null,",
        "\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n"
    )
    .as_bytes()
    .to_vec()
}

fn empty_message_completion_sse() -> Vec<u8> {
    concat!(
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,",
        "\"item\":{\"type\":\"message\",\"id\":\"msg_empty\"}}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,",
        "\"item\":{\"type\":\"message\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_empty\",",
        "\"status\":\"completed\",\"incomplete_details\":null,",
        "\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n"
    )
    .as_bytes()
    .to_vec()
}

fn buffered_success_sse(text: &str) -> Vec<u8> {
    format!(
        concat!(
            "data: {{\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{{\"type\":\"message\",\"id\":\"msg_up\"}}}}\n\n",
            "data: {{\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"{text}\"}}\n\n",
            "data: {{\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{{\"type\":\"message\"}}}}\n\n",
            "data: {{\"type\":\"response.completed\",\"response\":{{\"id\":\"resp_1\",\"usage\":{{\"input_tokens\":5,\"output_tokens\":2}}}}}}\n\n"
        ),
        text = text
    )
    .into_bytes()
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_retries_empty_completion() {
    let _guard = env_lock();
    let _delay_guard = ZeroRetryDelayGuard::enable();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let upstream = spawn_http_upstream({
        let attempts = attempts.clone();
        move |_body: Value| {
            let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt == 0 {
                empty_completion_sse()
            } else {
                buffered_success_sse("buffered retry ok")
            }
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");

    let response = call_messages("gpt-5.5").await;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_text = String::from_utf8_lossy(&body);

    assert_eq!(status, StatusCode::OK, "body: {body_text}");
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["content"][0]["text"], "buffered retry ok");
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "empty completion must trigger one retry"
    );
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_retries_empty_message_completion() {
    let _guard = env_lock();
    let _delay_guard = ZeroRetryDelayGuard::enable();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let upstream = spawn_http_upstream({
        let attempts = attempts.clone();
        move |_body: Value| {
            let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt == 0 {
                empty_message_completion_sse()
            } else {
                buffered_success_sse("empty message retry ok")
            }
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");

    let response = call_messages("gpt-5.5").await;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_text = String::from_utf8_lossy(&body);

    assert_eq!(status, StatusCode::OK, "body: {body_text}");
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["content"][0]["text"], "empty message retry ok");
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_stream_retries_empty_completion() {
    let _guard = env_lock();
    let _delay_guard = ZeroRetryDelayGuard::enable();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let upstream = spawn_http_upstream({
        let attempts = attempts.clone();
        move |_body: Value| {
            let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt == 0 {
                empty_completion_sse()
            } else {
                buffered_success_sse("buffered stream retry ok")
            }
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");

    let response = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"one"}]
    }))
    .await;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_text = String::from_utf8_lossy(&body);

    assert_eq!(status, StatusCode::OK, "body: {body_text}");
    assert!(
        body_text.contains("buffered stream retry ok"),
        "expected retried text in SSE body: {body_text}"
    );
    assert!(
        !body_text.contains(r#""input_tokens":0"#),
        "message_start should expose the request token estimate: {body_text}"
    );
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_empty_completions_exhaust_to_service_unavailable() {
    let _guard = env_lock();
    let _delay_guard = ZeroRetryDelayGuard::enable();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let upstream = spawn_http_upstream({
        let attempts = attempts.clone();
        move |_body: Value| {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            empty_completion_sse()
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");

    let response = call_messages("gpt-5.5").await;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_text = String::from_utf8_lossy(&body);

    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "exhausted empty completions must surface an explicit error: {body_text}"
    );
    assert!(
        body_text.contains("Codex completed without producing output"),
        "unexpected exhaustion body: {body_text}"
    );
    // Initial attempt plus MAX_EMPTY_COMPLETION_RETRIES retries.
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::SeqCst),
        11,
        "retry loop must stay bounded"
    );
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_auto_review_uses_codex_default_and_configured_override() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = spawn_http_upstream({
        let captured = captured.clone();
        move |body: Value| {
            captured.lock().unwrap().push(body);
            concat!(
                "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_up\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"review ok\"}\n\n",
                "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n"
            )
            .as_bytes()
            .to_vec()
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _codex_model_env = EnvGuard::set("CCP_CODEX_MODEL", "gpt-5.6-sol");
    let classifier_body = || {
        json!({
            "model": "gpt-5.6-sol",
            "max_tokens": 64,
            "stream": false,
            "system": [{
                "type": "text",
                "text": "You are a security monitor for autonomous AI coding agents.\n\n## Context"
            }],
            "messages": [{"role":"user","content":"review this Bash command"}],
            "tools": []
        })
    };

    let classifier = call_messages_body(classifier_body()).await;
    assert_eq!(classifier.status(), StatusCode::OK);
    let _ = axum::body::to_bytes(classifier.into_body(), usize::MAX)
        .await
        .unwrap();

    {
        let _review_model_env = EnvGuard::set("CCP_AUTO_REVIEW_MODEL", "gpt-5.6-terra");
        let classifier = call_messages_body(classifier_body()).await;
        assert_eq!(classifier.status(), StatusCode::OK);
        let _ = axum::body::to_bytes(classifier.into_body(), usize::MAX)
            .await
            .unwrap();
    }

    let normal = call_messages("gpt-5.6-sol").await;
    assert_eq!(normal.status(), StatusCode::OK);
    let _ = axum::body::to_bytes(normal.into_body(), usize::MAX)
        .await
        .unwrap();

    let sent = captured.lock().unwrap();
    assert_eq!(sent.len(), 3);
    assert_eq!(sent[0]["model"], "codex-auto-review");
    assert_eq!(sent[1]["model"], "gpt-5.6-terra");
    assert_eq!(sent[2]["model"], "gpt-5.6-sol");
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_native_auto_review_bypasses_inline_compaction_without_lane() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    let state_dir = config.path().join("inline-auto-review-contexts");

    let captured = Arc::new(Mutex::new(None));
    let upstream = spawn_http_upstream({
        let captured = captured.clone();
        move |body: Value| {
            let _ = captured.lock().map(|mut guard| *guard = Some(body));
            concat!(
                "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_review\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"review ok\"}\n\n",
                "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_review\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n"
            )
            .as_bytes()
            .to_vec()
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _review_model_env = EnvGuard::set("CCP_AUTO_REVIEW_MODEL", "codex-auto-review");
    let _threshold_env = EnvGuard::set("CCP_CODEX_INLINE_COMPACTION_THRESHOLD", "32768");
    let _state_dir_env = EnvGuard::set("CCP_CODEX_COMPACTION_STATE_DIR", &state_dir);

    let response = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "stream": false,
        "system": [{
            "type": "text",
            "text": "You are a security monitor for autonomous AI coding agents.\n\n## Context"
        }],
        "messages": [{"role":"user","content":"review this Bash command"}],
        "tools": []
    }))
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let sent = captured.lock().unwrap().clone().unwrap();
    assert_eq!(sent["model"], "codex-auto-review");
    assert!(sent.get("context_management").is_none());
    assert_eq!(collect_files(&state_dir).len(), 0);
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_server_compaction_replays_native_history() {
    let _guard = env_lock();
    clear_all_compactions_for_tests();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = spawn_http_upstream({
        let captured = captured.clone();
        move |body: Value| {
            let is_compaction = body["input"].as_array().is_some_and(|input| {
                input.last().and_then(|item| item["type"].as_str())
                    == Some("compaction_trigger")
            });
            let request_number = {
                let mut requests = captured.lock().unwrap();
                requests.push(body);
                requests.len()
            };
            if is_compaction {
                concat!(
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"opaque-history\"}}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_compact\",\"usage\":{\"input_tokens\":100,\"output_tokens\":1}}}\n\n"
                )
                .as_bytes()
                .to_vec()
            } else {
                let text = if request_number == 2 {
                    "portable summary with enough detail to anchor the compacted conversation"
                } else {
                    "compacted ok"
                };
                format!(
                    "data: {{\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{{\"type\":\"message\",\"id\":\"msg_up\"}}}}\n\n\
                     data: {{\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"{text}\"}}\n\n\
                     data: {{\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{{\"type\":\"message\"}}}}\n\n\
                     data: {{\"type\":\"response.completed\",\"response\":{{\"id\":\"resp_{request_number}\",\"usage\":{{\"input_tokens\":5,\"output_tokens\":2}}}}}}\n\n"
                )
                .into_bytes()
            }
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _compaction_env = EnvGuard::set("CCP_CODEX_SERVER_COMPACTION", "1");
    let compact_response = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "system": "You are Claude Code.",
        "messages": [
            {"role":"user","content":"old conversation"},
            {"role":"assistant","content":[{"type":"tool_use","id":"tool-1","name":"Read","input":{}}]},
            {"role":"user","content":[
                {"type":"tool_result","tool_use_id":"tool-1","content":"result"},
                {"type":"text","text":"CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.\n\nYour task is to create a detailed summary of the conversation so far, paying close attention to the user's explicit requests."}
            ]}
        ]
    }))
    .await;
    assert_eq!(compact_response.status(), StatusCode::OK);

    let response = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "system": "current instructions",
        "messages": [
            {"role":"user","content":"<summary>portable summary with enough detail to anchor the compacted conversation</summary>"},
            {"role":"user","content":"continue"}
        ]
    }))
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value["content"][0]["text"], "compacted ok");

    let requests = captured.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0]["model"], "gpt-5.6-sol");
    assert!(requests[0].get("client_metadata").is_some());
    assert_eq!(
        requests[0]["input"].as_array().unwrap().last().unwrap()["type"],
        "compaction_trigger"
    );
    assert!(
        !requests[0]
            .to_string()
            .contains("Your task is to create a detailed summary")
    );
    assert!(
        requests[1]
            .to_string()
            .contains("Your task is to create a detailed summary")
    );
    assert!(!requests[1].to_string().contains("opaque-history"));
    let replay = requests[2]["input"].as_array().unwrap();
    assert!(requests[2].get("client_metadata").is_some());
    assert!(requests[2].to_string().contains("current instructions"));
    let compaction = replay
        .iter()
        .find(|item| item["type"] == "compaction")
        .unwrap();
    assert_eq!(compaction["encrypted_content"], "opaque-history");
    assert!(replay.iter().any(|item| item["role"] == "user"));
    clear_all_compactions_for_tests();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_compaction_failure_preserves_portable_summary() {
    let _guard = env_lock();
    clear_all_compactions_for_tests();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = spawn_http_upstream({
        let captured = captured.clone();
        move |body: Value| {
            let is_compaction = body["input"].as_array().is_some_and(|input| {
                input.last().and_then(|item| item["type"].as_str())
                    == Some("compaction_trigger")
            });
            captured.lock().unwrap().push(body);
            if is_compaction {
                b"data: {\"type\":\"response.completed\",\"response\":{}}\n\n".to_vec()
            } else {
                concat!(
                    "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_up\"}}\n\n",
                    "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"portable fallback\"}\n\n",
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n"
                )
                .as_bytes()
                .to_vec()
            }
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _compaction_env = EnvGuard::set("CCP_CODEX_SERVER_COMPACTION", "1");
    let response = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "system": "You are a helpful AI assistant tasked with summarizing conversations.",
        "messages": [{"role":"user","content":"old conversation"}]
    }))
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value["content"][0]["text"], "portable fallback");

    let requests = captured.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].to_string().contains("old conversation"));
    assert!(!requests[1].to_string().contains("opaque-history"));
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_context_window_error_requests_compaction() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let upstream = spawn_http_upstream(|_body: Value| {
        "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"input exceeds context window\"}}}\n\n"
            .as_bytes()
            .to_vec()
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let response = call_messages("gpt-5.5").await;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["type"], "error");
    assert_eq!(value["error"]["type"], "request_too_large");
    assert!(
        value["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("input exceeds context window")),
        "response body: {}",
        String::from_utf8_lossy(&body)
    );
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_traffic_capture_writes_upstream_artifacts() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    let state = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let upstream = spawn_http_upstream(|_body: Value| {
        concat!(
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_up\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"codex http ok\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n"
        )
        .as_bytes()
        .to_vec()
    })
    .await;

    let _traffic_env = EnvGuard::set("CCP_TRAFFIC_LOG", "1");
    let _state_env = EnvGuard::set("XDG_STATE_HOME", state.path());
    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let response = call_messages("gpt-5.5").await;

    assert_eq!(response.status(), StatusCode::OK);
    let files = traffic_files(state.path());
    let request = traffic_json(&files, "020-upstream-request.json");
    assert_eq!(request["model"], "gpt-5.5");

    let metadata = traffic_json(&files, "021-upstream-request-metadata.json");
    assert_eq!(metadata["transport"], "http");
    assert!(
        metadata["headers"]["authorization"]
            .as_str()
            .unwrap()
            .contains("redacted")
    );
    assert_eq!(
        traffic_json(&files, "030-upstream-response-headers.json")["status"],
        200
    );
    traffic_file(&files, "032-upstream-response-body.sse");
    traffic_file(&files, "040-upstream-event.json");
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_stream_traffic_captures_downstream_events() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    let state = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let upstream = spawn_http_upstream(|_body: Value| {
        concat!(
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_up\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"codex stream ok\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n"
        )
        .as_bytes()
        .to_vec()
    })
    .await;

    let _traffic_env = EnvGuard::set("CCP_TRAFFIC_LOG", "1");
    let _state_env = EnvGuard::set("XDG_STATE_HOME", state.path());
    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let response = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"hello"}]
    }))
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("message_stop"), "stream body: {text}");

    let files = traffic_files(state.path());
    let downstream = traffic_json(&files, "050-downstream-event.json");
    assert!(downstream.get("event").is_some());
    assert!(downstream.get("data").is_some());
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_stream_returns_before_upstream_completion() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let release = Arc::new(tokio::sync::Notify::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let upstream = format!("http://{addr}");
    let mock = axum::Router::new().fallback({
        let release = release.clone();
        move |_body: String| {
            let release = release.clone();
            async move {
                let stream = futures_util::stream::unfold(0_u8, move |state| {
                    let release = release.clone();
                    async move {
                        match state {
                            0 => Some((
                                Ok::<_, std::convert::Infallible>(bytes::Bytes::from_static(
                                    concat!(
                                        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_up\"}}\n\n",
                                        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"incremental ok\"}\n\n"
                                    )
                                    .as_bytes(),
                                )),
                                1,
                            )),
                            1 => {
                                release.notified().await;
                                Some((
                                    Ok(bytes::Bytes::from_static(concat!(
                                        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
                                        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n"
                                    ).as_bytes())),
                                    2,
                                ))
                            }
                            _ => None,
                        }
                    }
                });
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from_stream(stream))
                    .unwrap()
            }
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, mock).await.ok();
    });

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let response = tokio::time::timeout(
        Duration::from_millis(500),
        call_messages_body(json!({
            "model": "gpt-5.5",
            "max_tokens": 64,
            "stream": true,
            "messages": [{"role":"user","content":"hello"}]
        })),
    )
    .await
    .expect("CCP must return streaming headers before upstream completion");
    assert_eq!(response.status(), StatusCode::OK);

    let mut body = response.into_body();
    let first = tokio::time::timeout(Duration::from_millis(200), body.frame())
        .await
        .expect("initial Anthropic heartbeat must arrive before upstream completion")
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    let first = String::from_utf8(first.to_vec()).unwrap();
    assert!(first.contains("event: message_start"));
    assert!(first.contains("event: ping"));
    assert!(first.contains("incremental ok"));

    release.notify_one();
    let rest = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let rest = String::from_utf8(rest.to_vec()).unwrap();
    assert!(rest.contains("event: message_stop"), "stream body: {rest}");
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_live_inline_compaction_commits_before_stop_and_releases_lane() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    let state_dir = config.path().join("live-inline-state");

    let release = Arc::new(tokio::sync::Notify::new());
    let upstream_calls = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let mock = axum::Router::new().fallback({
        let release = release.clone();
        let upstream_calls = upstream_calls.clone();
        move |_body: String| {
            let release = release.clone();
            let upstream_calls = upstream_calls.clone();
            async move {
                let call = upstream_calls.fetch_add(1, Ordering::SeqCst);
                let body = if call == 0 {
                    let stream = futures_util::stream::unfold(0_u8, move |state| {
                        let release = release.clone();
                        async move {
                            match state {
                                0 => Some((
                                    Ok::<_, std::convert::Infallible>(bytes::Bytes::from_static(
                                        concat!(
                                            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"opaque-live\"}}\n\n",
                                            "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"message\",\"id\":\"msg_live\"}}\n\n",
                                            "data: {\"type\":\"response.output_text.delta\",\"output_index\":1,\"delta\":\"live compact ok\"}\n\n"
                                        )
                                        .as_bytes(),
                                    )),
                                    1,
                                )),
                                1 => {
                                    release.notified().await;
                                    Some((
                                        Ok(bytes::Bytes::from_static(
                                            concat!(
                                                "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"live compact ok\"}]}}\n\n",
                                                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_live_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":40000,\"output_tokens\":3}}}\n\n"
                                            )
                                            .as_bytes(),
                                        )),
                                        2,
                                    ))
                                }
                                _ => None,
                            }
                        }
                    });
                    Body::from_stream(stream)
                } else {
                    Body::from(
                        concat!(
                            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_live_2\"}}\n\n",
                            "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"second ok\"}\n\n",
                            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"second ok\"}]}}\n\n",
                            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_live_2\",\"status\":\"completed\",\"usage\":{\"input_tokens\":100,\"output_tokens\":2}}}\n\n"
                        )
                        .as_bytes(),
                    )
                };
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(body)
                    .unwrap()
            }
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, mock).await.ok();
    });

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _threshold_env = EnvGuard::set("CCP_CODEX_INLINE_COMPACTION_THRESHOLD", "32768");
    let _state_dir_env = EnvGuard::set("CCP_CODEX_COMPACTION_STATE_DIR", &state_dir);
    let _legacy_compaction_env = EnvGuard::set("CCP_CODEX_SERVER_COMPACTION", "false");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "false");
    let _session_concurrency_env = EnvGuard::set("CCP_CODEX_SESSION_MAX_CONCURRENT_REQUESTS", "4");
    let _no_proxy_env = EnvGuard::set("NO_PROXY", "127.0.0.1,localhost");

    let shared_app = app(Arc::new(Registry::with_default_alias()));
    let first_request = Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("content-type", "application/json")
        .header("x-claude-code-session-id", "smoke-session")
        .body(Body::from(
            json!({
                "model": "gpt-5.6-sol",
                "max_tokens": 64,
                "stream": true,
                "messages": [{"role":"user","content":"seed"}]
            })
            .to_string(),
        ))
        .unwrap();
    let response = tokio::time::timeout(
        Duration::from_millis(500),
        shared_app.clone().oneshot(first_request),
    )
    .await
    .expect("live inline response must publish text before completion")
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut first_body = response.into_body();
    let first = tokio::time::timeout(Duration::from_millis(200), first_body.frame())
        .await
        .expect("first live inline frame timed out")
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    let first_text = String::from_utf8_lossy(&first);
    assert!(first_text.contains("live compact ok"));
    assert!(!first_text.contains("message_stop"));
    assert_eq!(
        collect_files(&state_dir)
            .iter()
            .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
            .count(),
        0,
        "sidecar must not commit before response.completed"
    );

    let second_request = Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("content-type", "application/json")
        .header("x-claude-code-session-id", "smoke-session")
        .body(Body::from(
            json!({
                "model": "gpt-5.6-sol",
                "max_tokens": 64,
                "stream": true,
                "messages": [
                    {"role":"user","content":"seed"},
                    {"role":"assistant","content":"live compact ok"},
                    {"role":"user","content":"next"}
                ]
            })
            .to_string(),
        ))
        .unwrap();
    let second_app = shared_app.clone();
    let second = tokio::spawn(async move { second_app.oneshot(second_request).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !second.is_finished(),
        "same lane escaped before producer terminal"
    );
    assert_eq!(upstream_calls.load(Ordering::SeqCst), 1);

    release.notify_one();
    for _ in 0..50 {
        if collect_files(&state_dir)
            .iter()
            .any(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        upstream_calls.load(Ordering::SeqCst),
        1,
        "same lane must remain blocked while committed terminal is queued"
    );
    assert!(
        !second.is_finished(),
        "lease released before Body consumed stop"
    );
    let rest = axum::body::to_bytes(first_body, usize::MAX).await.unwrap();
    let rest = String::from_utf8(rest.to_vec()).unwrap();
    assert!(rest.contains("event: message_stop"), "stream body: {rest}");
    assert_eq!(
        collect_files(&state_dir)
            .iter()
            .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
            .count(),
        1,
        "sidecar must exist before message_stop is observable"
    );

    let second_response = tokio::time::timeout(Duration::from_secs(1), second)
        .await
        .expect("same lane was not released after producer terminal")
        .unwrap();
    assert_eq!(second_response.status(), StatusCode::OK);
    let second_body = axum::body::to_bytes(second_response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&second_body).contains("message_stop"));
    assert_eq!(upstream_calls.load(Ordering::SeqCst), 2);
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_live_inline_commit_failure_emits_error_without_message_stop() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    let state_dir = config.path().join("live-inline-failure-state");

    let release = Arc::new(tokio::sync::Notify::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let mock = axum::Router::new().fallback({
        let release = release.clone();
        move |_body: String| {
            let release = release.clone();
            async move {
                let stream = futures_util::stream::unfold(0_u8, move |state| {
                    let release = release.clone();
                    async move {
                        match state {
                            0 => Some((
                                Ok::<_, std::convert::Infallible>(bytes::Bytes::from_static(
                                    concat!(
                                        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"opaque-fail\"}}\n\n",
                                        "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"message\",\"id\":\"msg_fail\"}}\n\n",
                                        "data: {\"type\":\"response.output_text.delta\",\"output_index\":1,\"delta\":\"generated text\"}\n\n"
                                    )
                                    .as_bytes(),
                                )),
                                1,
                            )),
                            1 => {
                                release.notified().await;
                                Some((
                                    Ok(bytes::Bytes::from_static(
                                        concat!(
                                            "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"generated text\"}]}}\n\n",
                                            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fail\",\"status\":\"completed\",\"usage\":{\"input_tokens\":40000,\"output_tokens\":2}}}\n\n"
                                        )
                                        .as_bytes(),
                                    )),
                                    2,
                                ))
                            }
                            _ => None,
                        }
                    }
                });
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from_stream(stream))
                    .unwrap()
            }
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, mock).await.ok();
    });

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _threshold_env = EnvGuard::set("CCP_CODEX_INLINE_COMPACTION_THRESHOLD", "32768");
    let _state_dir_env = EnvGuard::set("CCP_CODEX_COMPACTION_STATE_DIR", &state_dir);
    let _legacy_compaction_env = EnvGuard::set("CCP_CODEX_SERVER_COMPACTION", "false");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "false");

    let response = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"force commit failure"}]
    }))
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert!(String::from_utf8_lossy(&first).contains("generated text"));

    let lane_hash = claude_code_proxy::providers::codex::inline_compaction::telemetry_lane_hash(
        "smoke-session",
    );
    let state_path = state_dir.join(format!("{lane_hash}.json"));
    std::fs::create_dir(&state_path).unwrap();
    release.notify_one();

    let rest = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let mut rendered = first.to_vec();
    rendered.extend_from_slice(&rest);
    let rendered = String::from_utf8(rendered).unwrap();
    assert!(rendered.contains("event: error"), "stream body: {rendered}");
    assert!(
        !rendered.contains("event: message_stop"),
        "commit failure must not publish success: {rendered}"
    );
    assert!(state_path.is_dir());
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_live_inline_duplicate_terminal_never_commits() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    let state_dir = config.path().join("live-inline-duplicate-terminal-state");

    let upstream = spawn_http_upstream(move |_body: Value| {
        concat!(
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"must-not-commit\"}}\n\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"message\",\"id\":\"msg_duplicate\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"output_index\":1,\"delta\":\"partial duplicate\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"partial duplicate\"}]}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_duplicate_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":40000,\"output_tokens\":2}}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_duplicate_2\",\"status\":\"completed\",\"usage\":{\"input_tokens\":40000,\"output_tokens\":2}}}\n\n"
        )
        .as_bytes()
        .to_vec()
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _threshold_env = EnvGuard::set("CCP_CODEX_INLINE_COMPACTION_THRESHOLD", "32768");
    let _state_dir_env = EnvGuard::set("CCP_CODEX_COMPACTION_STATE_DIR", &state_dir);
    let _legacy_compaction_env = EnvGuard::set("CCP_CODEX_SERVER_COMPACTION", "false");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "false");

    let response = call_messages_body(json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"duplicate terminal"}]
    }))
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let rendered = String::from_utf8(body.to_vec()).unwrap();
    assert!(rendered.contains("partial duplicate"));
    assert!(rendered.contains("event: error"), "stream body: {rendered}");
    assert!(
        !rendered.contains("event: message_stop"),
        "duplicate terminal must not publish success: {rendered}"
    );
    assert_eq!(
        collect_files(&state_dir)
            .iter()
            .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
            .count(),
        0,
        "duplicate terminal must not commit a sidecar"
    );
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_live_inline_disconnect_cancels_upstream_and_preserves_sidecar() {
    struct UpstreamBodyDropSignal(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for UpstreamBodyDropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    let state_dir = config.path().join("live-inline-disconnect-state");

    let upstream_calls = Arc::new(AtomicUsize::new(0));
    let (upstream_dropped_tx, upstream_dropped_rx) = tokio::sync::oneshot::channel();
    let upstream_dropped_tx = Arc::new(Mutex::new(Some(upstream_dropped_tx)));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let mock = axum::Router::new().fallback({
        let upstream_calls = upstream_calls.clone();
        let upstream_dropped_tx = upstream_dropped_tx.clone();
        move |_body: String| {
            let upstream_calls = upstream_calls.clone();
            let upstream_dropped_tx = upstream_dropped_tx.clone();
            async move {
                let call = upstream_calls.fetch_add(1, Ordering::SeqCst);
                let body = if call == 0 {
                    Body::from(
                        concat!(
                            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"opaque-before-disconnect\"}}\n\n",
                            "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"message\",\"id\":\"msg_seeded\"}}\n\n",
                            "data: {\"type\":\"response.output_text.delta\",\"output_index\":1,\"delta\":\"seeded\"}\n\n",
                            "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"seeded\"}]}}\n\n",
                            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_seeded\",\"status\":\"completed\",\"usage\":{\"input_tokens\":40000,\"output_tokens\":2}}}\n\n"
                        )
                        .as_bytes(),
                    )
                } else {
                    let sender = upstream_dropped_tx
                        .lock()
                        .unwrap()
                        .take()
                        .expect("disconnect probe may be used once");
                    let stream = futures_util::stream::unfold(
                        (0_u8, UpstreamBodyDropSignal(Some(sender))),
                        |(state, signal)| async move {
                            match state {
                                0 => Some((
                                    Ok::<_, std::convert::Infallible>(
                                        bytes::Bytes::from_static(
                                            concat!(
                                                "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_partial\"}}\n\n",
                                                "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"partial before disconnect\"}\n\n"
                                            )
                                            .as_bytes(),
                                        ),
                                    ),
                                    (1, signal),
                                )),
                                _ => {
                                    std::future::pending::<()>().await;
                                    let _ = signal;
                                    None
                                }
                            }
                        },
                    );
                    Body::from_stream(stream)
                };
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(body)
                    .unwrap()
            }
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, mock).await.ok();
    });

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let _threshold_env = EnvGuard::set("CCP_CODEX_INLINE_COMPACTION_THRESHOLD", "32768");
    let _state_dir_env = EnvGuard::set("CCP_CODEX_COMPACTION_STATE_DIR", &state_dir);
    let _legacy_compaction_env = EnvGuard::set("CCP_CODEX_SERVER_COMPACTION", "false");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "false");
    let _no_proxy_env = EnvGuard::set("NO_PROXY", "127.0.0.1,localhost");

    let shared_app = app(Arc::new(Registry::with_default_alias()));
    let request = |messages: Value| {
        Request::builder()
            .method(Method::POST)
            .uri("/v1/messages")
            .header("content-type", "application/json")
            .header("x-claude-code-session-id", "disconnect-session")
            .body(Body::from(
                json!({
                    "model": "gpt-5.6-sol",
                    "max_tokens": 64,
                    "stream": true,
                    "messages": messages
                })
                .to_string(),
            ))
            .unwrap()
    };

    let seeded = shared_app
        .clone()
        .oneshot(request(json!([{"role":"user","content":"seed"}])))
        .await
        .unwrap();
    assert_eq!(seeded.status(), StatusCode::OK);
    let seeded_body = axum::body::to_bytes(seeded.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&seeded_body).contains("message_stop"));
    let state_path = collect_files(&state_dir)
        .into_iter()
        .find(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .expect("seed response must commit one sidecar");
    let old_state = std::fs::read(&state_path).unwrap();

    let response = shared_app
        .oneshot(request(json!([
            {"role":"user","content":"seed"},
            {"role":"assistant","content":"seeded"},
            {"role":"user","content":"continue"}
        ])))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let first = tokio::time::timeout(Duration::from_secs(1), body.frame())
        .await
        .expect("partial downstream frame timed out")
        .expect("partial downstream stream ended")
        .expect("partial downstream frame failed")
        .into_data()
        .unwrap();
    assert!(String::from_utf8_lossy(&first).contains("partial before disconnect"));
    drop(body);

    tokio::time::timeout(Duration::from_secs(1), upstream_dropped_rx)
        .await
        .expect("downstream disconnect did not cancel the upstream body")
        .expect("upstream body drop signal disappeared");
    assert_eq!(upstream_calls.load(Ordering::SeqCst), 2);
    assert_eq!(std::fs::read(&state_path).unwrap(), old_state);
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_retries_overload_after_control_events() {
    assert_codex_http_presemantic_retry(
        concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_failed\"}}\n\n",
            "data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_failed\"}}\n\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"overloaded_error\",\"message\":\"Our servers are currently overloaded. Please try again later.\",\"retry_after\":0}}}\n\n"
        )
        .as_bytes()
        .to_vec(),
    )
    .await;
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_retries_rate_limit_after_control_events() {
    assert_codex_http_presemantic_retry(
        concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_limited\"}}\n\n",
            "data: {\"type\":\"codex.rate_limits\",\"rate_limits\":{\"limit_reached\":true,\"primary\":{\"reset_after_seconds\":0}}}\n\n"
        )
        .as_bytes()
        .to_vec(),
    )
    .await;
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_retries_transient_failure_after_control_events() {
    assert_codex_http_presemantic_retry(
        concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_transient\"}}\n\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"server_error\",\"status\":503,\"message\":\"temporarily unavailable\",\"retry_after\":0}}}\n\n"
        )
        .as_bytes()
        .to_vec(),
    )
    .await;
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_retries_presemantic_eof() {
    assert_codex_http_presemantic_retry(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_truncated\"}}\n\n"
            .as_bytes()
            .to_vec(),
    )
    .await;
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_bounds_initial_status_retries() {
    let _guard = env_lock();
    clear_all_continuations_for_tests();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let attempts = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let upstream = format!("http://{addr}");
    let mock = axum::Router::new().fallback({
        let attempts = attempts.clone();
        move || {
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header("retry-after", "0")
                    .body(Body::empty())
                    .unwrap()
            }
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, mock).await.ok();
    });

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let response = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"hello"}]
    }))
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_retries_presemantic_invalid_json() {
    assert_codex_http_presemantic_retry(b"data: not-json\n\n".to_vec()).await;
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_retries_presemantic_invalid_utf8() {
    assert_codex_http_presemantic_retry(b"data: \xff\n\n".to_vec()).await;
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_does_not_retry_overload_after_semantic_output() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let attempts = Arc::new(AtomicUsize::new(0));
    let upstream = spawn_http_upstream({
        let attempts = attempts.clone();
        move |_body: Value| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                concat!(
                    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_partial\"}}\n\n",
                    "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_partial\"}}\n\n",
                    "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"partial output\"}\n\n",
                    "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"overloaded_error\",\"message\":\"overloaded after output\",\"retry_after\":0}}}\n\n"
                )
                .as_bytes()
                .to_vec()
            } else {
                panic!("semantic output must close the full-request retry window");
            }
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let response = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"hello"}]
    }))
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = tokio::time::timeout(
        Duration::from_secs(2),
        axum::body::to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("failed semantic stream must terminate")
    .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "stream body: {text}");
    assert!(text.contains("partial output"), "stream body: {text}");
    assert!(
        text.contains("overloaded after output"),
        "stream body: {text}"
    );
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_stops_after_retry_limit() {
    let _guard = env_lock();
    clear_all_continuations_for_tests();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let attempts = Arc::new(AtomicUsize::new(0));
    let upstream = spawn_http_upstream({
        let attempts = attempts.clone();
        move |_body: Value| {
            attempts.fetch_add(1, Ordering::SeqCst);
            concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_exhausted\"}}\n\n",
                "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"overloaded_error\",\"message\":\"overloaded until retry limit\",\"retry_after\":0}}}\n\n"
            )
            .as_bytes()
            .to_vec()
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let response = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"hello"}]
    }))
    .await;

    assert_eq!(response.status().as_u16(), 529);
    let body = tokio::time::timeout(
        Duration::from_secs(2),
        axum::body::to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("exhausted stream must terminate")
    .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert_eq!(attempts.load(Ordering::SeqCst), 4, "stream body: {text}");
    assert!(
        text.contains("overloaded until retry limit"),
        "stream body: {text}"
    );
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_cancels_retry_backoff_when_request_drops() {
    let _guard = env_lock();
    clear_all_continuations_for_tests();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let attempts = Arc::new(AtomicUsize::new(0));
    let upstream = spawn_http_upstream({
        let attempts = attempts.clone();
        move |_body: Value| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                concat!(
                    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_cancel\"}}\n\n",
                    "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"overloaded_error\",\"message\":\"cancel during retry backoff\",\"retry_after\":0.1}}}\n\n"
                )
                .as_bytes()
                .to_vec()
            } else {
                panic!("request cancellation must prevent another upstream attempt");
            }
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let request = tokio::spawn(call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"hello"}]
    })));
    tokio::time::timeout(Duration::from_millis(200), async {
        while attempts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first upstream attempt must start");
    request.abort();
    let _ = request.await;

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_truncated_upstream_writes_reducer_diagnostic() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    let state = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let upstream = spawn_http_upstream(|_body: Value| {
        concat!(
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_up\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"partial\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n"
        )
        .as_bytes()
        .to_vec()
    })
    .await;

    let _traffic_env = EnvGuard::set("CCP_TRAFFIC_LOG", "1");
    let _state_env = EnvGuard::set("XDG_STATE_HOME", state.path());
    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let response = call_messages("gpt-5.5").await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let files = traffic_files(state.path());
    let diagnostic = traffic_json(&files, "060-codex-reducer-error.json");
    assert_eq!(diagnostic["kind"], "Transient");
    assert_eq!(
        diagnostic["diagnostics"]["saw_terminal_event"],
        Value::Bool(false)
    );
}

// ---------------------------------------------------------------------------
// Codex WebSocket smoke: mock upstream verifies request shape and returns
// Responses events over WebSocket.
// ---------------------------------------------------------------------------

// Multi-threaded runtime so the spawned accept task runs independently and
// the listener is registered with the I/O driver before connect_async starts.
// A single-threaded runtime risks the root task (connect_async) outpacing the
// spawned accept task, causing connection-refused races.
#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_messages_uses_mock_upstream() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();

    let captured = Arc::new(Mutex::new(None));
    let upstream = spawn_websocket_upstream(captured.clone()).await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");
    let response = call_messages("gpt-5.5").await;

    let ws_status = response.status();
    let ws_body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    if ws_status != StatusCode::OK {
        panic!(
            "WS: expected 200, got {}: {}",
            ws_status,
            String::from_utf8_lossy(&ws_body_bytes)
        );
    }
    let value: Value = serde_json::from_slice(&ws_body_bytes).unwrap();
    assert_eq!(value["content"][0]["text"], "codex websocket ok");

    let guard = captured.lock().unwrap();
    let sent = guard.clone().unwrap_or_else(|| {
        panic!(
            "WS mock did not capture a request. Response body: {}",
            String::from_utf8_lossy(&ws_body_bytes)
        );
    });
    assert_eq!(sent["type"], "response.create");
    assert_eq!(sent["model"], "gpt-5.5");
    assert!(sent.get("max_output_tokens").is_none());
    assert!(sent.get("stream").is_none());
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_uses_credits_after_included_limit() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();

    let upstream = spawn_websocket_credited_rate_limit_upstream().await;
    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");

    let response = call_messages("gpt-5.5").await;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    assert_eq!(
        status,
        StatusCode::OK,
        "credited Codex response must not be discarded: {}",
        String::from_utf8_lossy(&body)
    );
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["content"][0]["text"], "credited codex ok");
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_stream_uses_credits_after_included_limit() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();

    let upstream = spawn_websocket_credited_rate_limit_upstream().await;
    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");

    let response = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"hello"}]
    }))
    .await;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    assert_eq!(
        status,
        StatusCode::OK,
        "credited Codex stream must not be discarded: {}",
        String::from_utf8_lossy(&body)
    );
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("credited codex ok"), "stream body: {text}");
    assert!(text.contains("message_stop"), "stream body: {text}");
    assert!(!text.contains("event: error"), "stream body: {text}");
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_stream_returns_delta_before_terminal() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();

    let upstream = spawn_websocket_delayed_terminal_upstream().await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");

    let response = tokio::time::timeout(
        Duration::from_millis(1_500),
        call_messages_body(json!({
            "model": "gpt-5.5",
            "max_tokens": 64,
            "stream": true,
            "messages": [{"role":"user","content":"hello"}]
        })),
    )
    .await
    .expect("streaming response should start before terminal upstream event");
    assert_eq!(response.status(), StatusCode::OK);

    let mut body = response.into_body();
    let mut collected = Vec::new();
    let read = tokio::time::timeout(Duration::from_millis(500), async {
        while !String::from_utf8_lossy(&collected).contains("text_delta") {
            let Some(frame) = body.frame().await else {
                break;
            };
            let frame = frame.unwrap();
            if let Ok(data) = frame.into_data() {
                collected.extend_from_slice(&data);
            }
        }
    })
    .await;
    assert!(read.is_ok(), "stream did not yield an early text delta");
    let text = String::from_utf8_lossy(&collected);
    assert!(text.contains("early chunk"), "stream body: {text}");
    assert!(
        !text.contains(r#""input_tokens":0"#),
        "message_start should expose the request token estimate: {text}"
    );
    assert!(
        !text.contains("message_stop"),
        "stream finished too early: {text}"
    );
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_context_window_error_requests_compaction() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();

    let upstream = spawn_websocket_error_upstream("input exceeds context window").await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");

    let response = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"hello"}]
    }))
    .await;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "response body: {}",
        String::from_utf8_lossy(&body)
    );
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["type"], "error");
    assert_eq!(value["error"]["type"], "request_too_large");
    assert_eq!(value["error"]["message"], "input exceeds context window");
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_stream_uses_previous_response_id() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();
    clear_all_continuations_for_tests();

    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = spawn_websocket_sequence_upstream(captured.clone()).await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "1");

    let first = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"one"}]
    }))
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let _ = axum::body::to_bytes(first.into_body(), usize::MAX)
        .await
        .unwrap();

    let second = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [
            {"role":"user","content":"one"},
            {"role":"assistant","content":"first"},
            {"role":"user","content":"two"}
        ]
    }))
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = axum::body::to_bytes(second.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&second_body).contains("second"),
        "second response body: {}",
        String::from_utf8_lossy(&second_body)
    );

    let guard = captured.lock().unwrap();
    assert_eq!(guard.len(), 2, "expected two upstream websocket requests");
    assert!(guard[0].get("previous_response_id").is_none());
    assert_eq!(guard[1]["previous_response_id"], "resp_1");
    assert_eq!(
        guard[1]["input"].as_array().map(Vec::len),
        Some(1),
        "second request should send only the appended input delta"
    );
    assert_eq!(guard[1]["input"][0]["role"], "user");
    assert_eq!(guard[1]["input"][0]["content"][0]["text"], "two");

    clear_all_continuations_for_tests();
    clear_codex_websocket_pool_for_tests();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_stream_retries_missing_previous_response_id() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();
    clear_all_continuations_for_tests();

    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = spawn_websocket_previous_missing_then_retry_upstream(captured.clone()).await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "1");

    let first = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"one"}]
    }))
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let _ = axum::body::to_bytes(first.into_body(), usize::MAX)
        .await
        .unwrap();

    let second = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [
            {"role":"user","content":"one"},
            {"role":"assistant","content":"first"},
            {"role":"user","content":"two"}
        ]
    }))
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = axum::body::to_bytes(second.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&second_body).contains("retry"),
        "second response body: {}",
        String::from_utf8_lossy(&second_body)
    );

    let guard = captured.lock().unwrap();
    assert_eq!(guard.len(), 3, "expected retry websocket request");
    assert!(guard[0].get("previous_response_id").is_none());
    assert_eq!(guard[1]["previous_response_id"], "resp_1");
    assert!(guard[2].get("previous_response_id").is_none());
    assert_eq!(
        guard[2]["input"].as_array().map(Vec::len),
        Some(3),
        "retry request should send the full input"
    );

    clear_all_continuations_for_tests();
    clear_codex_websocket_pool_for_tests();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_stream_retries_empty_close_with_full_context() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();
    clear_all_continuations_for_tests();

    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = spawn_websocket_close_then_retry_upstream(captured.clone()).await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "1");

    let first = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"one"}]
    }))
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let _ = axum::body::to_bytes(first.into_body(), usize::MAX)
        .await
        .unwrap();

    let second = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [
            {"role":"user","content":"one"},
            {"role":"assistant","content":"first"},
            {"role":"user","content":"two"}
        ]
    }))
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = axum::body::to_bytes(second.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&second_body).contains("retry"),
        "second response body: {}",
        String::from_utf8_lossy(&second_body)
    );

    let guard = captured.lock().unwrap();
    assert_eq!(guard.len(), 3, "expected full-context retry request");
    assert!(guard[0].get("previous_response_id").is_none());
    assert_eq!(guard[1]["previous_response_id"], "resp_1");
    assert!(guard[2].get("previous_response_id").is_none());
    assert_eq!(
        guard[2]["input"].as_array().map(Vec::len),
        Some(3),
        "retry request should send the full input"
    );

    clear_all_continuations_for_tests();
    clear_codex_websocket_pool_for_tests();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_stream_retries_terminal_only_completion_with_full_context() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    let state = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();
    clear_all_continuations_for_tests();

    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = spawn_websocket_empty_completion_then_retry_upstream(captured.clone()).await;

    let _traffic_env = EnvGuard::set("CCP_TRAFFIC_LOG", "1");
    let _state_env = EnvGuard::set("XDG_STATE_HOME", state.path());
    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "1");

    let first = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"one"}]
    }))
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let _ = axum::body::to_bytes(first.into_body(), usize::MAX)
        .await
        .unwrap();

    let second = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [
            {"role":"user","content":"one"},
            {"role":"assistant","content":"first"},
            {"role":"user","content":"two"}
        ]
    }))
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = axum::body::to_bytes(second.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&second_body).contains("retry"),
        "second response body: {}",
        String::from_utf8_lossy(&second_body)
    );

    let downstream_end_turns = traffic_files(state.path())
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with("050-downstream-event.json"))
        })
        .filter_map(|path| std::fs::read(path).ok())
        .filter_map(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .filter(|event| event["data"]["delta"]["stop_reason"] == "end_turn")
        .count();
    assert_eq!(
        downstream_end_turns, 2,
        "discarded empty attempts must not be captured as downstream events"
    );

    let guard = captured.lock().unwrap();
    assert_eq!(guard.len(), 3, "expected full-context retry request");
    assert!(guard[0].get("previous_response_id").is_none());
    assert_eq!(guard[1]["previous_response_id"], "resp_1");
    assert!(guard[2].get("previous_response_id").is_none());
    assert_eq!(
        guard[2]["input"].as_array().map(Vec::len),
        Some(3),
        "retry request should send the full input"
    );

    clear_all_continuations_for_tests();
    clear_codex_websocket_pool_for_tests();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_empty_completions_exhaust_to_service_unavailable() {
    let _guard = env_lock();
    let _delay_guard = ZeroRetryDelayGuard::enable();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();
    clear_all_continuations_for_tests();

    let request_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let upstream = spawn_websocket_always_empty_completion_upstream(request_count.clone()).await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");

    let response = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "stream": true,
        "messages": [{"role":"user","content":"one"}]
    }))
    .await;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_text = String::from_utf8_lossy(&body);

    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "exhausted empty completions must surface an explicit error: {body_text}"
    );
    assert!(
        body_text.contains("Codex completed without producing output"),
        "unexpected exhaustion body: {body_text}"
    );
    // Initial attempt plus MAX_RETRYABLE_LIVE_STREAM_RETRIES full-context retries.
    assert_eq!(
        request_count.load(std::sync::atomic::Ordering::SeqCst),
        11,
        "retry loop must stay bounded"
    );

    clear_all_continuations_for_tests();
    clear_codex_websocket_pool_for_tests();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_previous_response_id_sends_delta_on_second_turn() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();
    clear_all_continuations_for_tests();

    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = spawn_websocket_sequence_upstream(captured.clone()).await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");
    let _previous_response_env = EnvGuard::set("CCP_CODEX_PREVIOUS_RESPONSE_ID", "1");

    let first = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "messages": [{"role":"user","content":"one"}]
    }))
    .await;
    assert_eq!(first.status(), StatusCode::OK);

    let second = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "messages": [
            {"role":"user","content":"one"},
            {"role":"assistant","content":"first"},
            {"role":"user","content":"two"}
        ]
    }))
    .await;
    let second_status = second.status();
    let second_body = axum::body::to_bytes(second.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        second_status,
        StatusCode::OK,
        "second response body: {}",
        String::from_utf8_lossy(&second_body)
    );
    let value: Value = serde_json::from_slice(&second_body).unwrap();
    assert_eq!(value["content"][0]["text"], "second");

    let third = call_messages_body(json!({
        "model": "gpt-5.5",
        "max_tokens": 64,
        "messages": [
            {"role":"user","content":"one"},
            {"role":"assistant","content":"first"},
            {"role":"user","content":"two"},
            {"role":"assistant","content":"second"},
            {"role":"user","content":"three"}
        ]
    }))
    .await;
    let third_status = third.status();
    let third_body = axum::body::to_bytes(third.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        third_status,
        StatusCode::OK,
        "third response body: {}",
        String::from_utf8_lossy(&third_body)
    );
    let value: Value = serde_json::from_slice(&third_body).unwrap();
    assert_eq!(value["content"][0]["text"], "third");

    let guard = captured.lock().unwrap();
    assert_eq!(guard.len(), 3, "expected three upstream websocket requests");
    assert!(guard[0].get("previous_response_id").is_none());
    assert_eq!(guard[1]["previous_response_id"], "resp_1");
    assert_eq!(
        guard[1]["input"].as_array().map(Vec::len),
        Some(1),
        "second request should send only the appended input delta"
    );
    assert_eq!(guard[1]["input"][0]["role"], "user");
    assert_eq!(guard[1]["input"][0]["content"][0]["text"], "two");
    assert_eq!(guard[2]["previous_response_id"], "resp_2");
    assert_eq!(
        guard[2]["input"].as_array().map(Vec::len),
        Some(1),
        "third request should keep reusing the pooled websocket continuation"
    );
    assert_eq!(guard[2]["input"][0]["role"], "user");
    assert_eq!(guard[2]["input"][0]["content"][0]["text"], "three");

    clear_all_continuations_for_tests();
    clear_codex_websocket_pool_for_tests();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_traffic_capture_writes_upstream_artifacts() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    let state = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();

    let captured = Arc::new(Mutex::new(None));
    let upstream = spawn_websocket_upstream(captured.clone()).await;

    let _traffic_env = EnvGuard::set("CCP_TRAFFIC_LOG", "1");
    let _state_env = EnvGuard::set("XDG_STATE_HOME", state.path());
    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");
    let response = call_messages("gpt-5.5").await;

    assert_eq!(response.status(), StatusCode::OK);
    let files = traffic_files(state.path());
    let request = traffic_json(&files, "020-upstream-request.json");
    assert_eq!(request["type"], "response.create");
    assert!(request.get("stream").is_none());

    let metadata = traffic_json(&files, "021-upstream-request-metadata.json");
    assert_eq!(metadata["transport"], "websocket");
    assert!(
        metadata["headers"]["authorization"]
            .as_str()
            .unwrap()
            .contains("redacted")
    );
    traffic_file(&files, "022-upstream-websocket-metadata.json");
    assert_eq!(
        traffic_json(&files, "030-upstream-response-headers.json")["status"],
        200
    );
    traffic_file(&files, "032-upstream-response-body.sse");
    traffic_file(&files, "040-upstream-event.json");
}
