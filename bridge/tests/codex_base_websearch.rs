use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use claude_code_proxy::{registry::Registry, server::app};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex, OnceLock};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tower::util::ServiceExt;

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    match lock.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
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

#[derive(Clone, Debug)]
struct CapturedRequest {
    path: String,
    authorization: Option<String>,
    account_id: Option<String>,
    body: Value,
}

async fn spawn_inspecting_upstream(
    captured: Arc<Mutex<Option<CapturedRequest>>>,
    content_type: &'static str,
    response_body: Vec<u8>,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = axum::Router::new().fallback(move |request: Request<Body>| {
        let captured = captured.clone();
        let response_body = response_body.clone();
        async move {
            let path = request.uri().path().to_string();
            let authorization = request
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let account_id = request
                .headers()
                .get("chatgpt-account-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let body_bytes = request.into_body().collect().await.unwrap().to_bytes();
            let body = serde_json::from_slice(&body_bytes).unwrap();
            *captured.lock().unwrap() = Some(CapturedRequest {
                path,
                authorization,
                account_id,
                body,
            });
            http::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", content_type)
                .body(Body::from(response_body))
                .unwrap()
        }
    });

    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });
    format!("http://{address}")
}

#[cfg(unix)]
fn write_bearer_token(directory: &std::path::Path) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;

    let path = directory.join("bearer.token");
    std::fs::write(&path, "fixture-bearer-token\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    path
}

async fn call_messages(body: Value) -> (StatusCode, Value) {
    let _no_proxy = EnvGuard::set("NO_PROXY", "127.0.0.1,localhost");
    let response = app(Arc::new(Registry::with_default_alias()))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .header("x-claude-code-session-id", "base-websearch-test")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        panic!(
            "downstream response was not JSON: {error}; body={}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, value)
}

fn base_success_sse() -> Vec<u8> {
    concat!(
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,",
        "\"item\":{\"type\":\"message\",\"id\":\"msg_base\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,",
        "\"delta\":\"base ok\"}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,",
        "\"item\":{\"type\":\"message\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_base\",",
        "\"usage\":{\"input_tokens\":7,\"output_tokens\":2}}}\n\n"
    )
    .as_bytes()
    .to_vec()
}

fn hosted_web_search_sse() -> Vec<u8> {
    concat!(
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,",
        "\"item\":{\"type\":\"web_search_call\",\"id\":\"ws_base\"}}\n\n",
        "data: {\"type\":\"response.web_search_call.in_progress\",\"output_index\":0,",
        "\"item_id\":\"ws_base\"}\n\n",
        "data: {\"type\":\"response.web_search_call.completed\",\"output_index\":0,",
        "\"item_id\":\"ws_base\"}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,",
        "\"item\":{\"type\":\"web_search_call\",\"id\":\"ws_base\",",
        "\"action\":{\"query\":\"official Base documentation\"}}}\n\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":1,",
        "\"item\":{\"type\":\"message\",\"id\":\"msg_search\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":1,",
        "\"delta\":\"See [Base documentation](https://example.com/base)\"}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":1,",
        "\"item\":{\"type\":\"message\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_search\",",
        "\"usage\":{\"input_tokens\":11,\"output_tokens\":4}}}\n\n"
    )
    .as_bytes()
    .to_vec()
}

fn install_bearer_file_env(
    config: &TempDir,
    upstream: &str,
    key_path: &std::path::Path,
) -> Vec<EnvGuard> {
    vec![
        EnvGuard::set("CCP_CONFIG_DIR", config.path()),
        EnvGuard::set("CCP_CODEX_AUTH_MODE", "bearer_file"),
        EnvGuard::set("CCP_CODEX_BEARER_TOKEN_FILE", key_path),
        EnvGuard::set("CCP_CODEX_BASE_URL", format!("{upstream}/v1/responses")),
        EnvGuard::set("CCP_CODEX_TRANSPORT", "http"),
    ]
}

fn assert_bearer_file_identity(request: &CapturedRequest) {
    assert_eq!(
        request.authorization.as_deref(),
        Some("Bearer fixture-bearer-token")
    );
    assert!(
        request.account_id.is_none(),
        "bearer_file mode must not send ChatGPT-Account-Id"
    );
}

#[cfg(unix)]
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn base_model_reaches_full_responses_lane_with_bearer_file_auth() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    let key_path = write_bearer_token(config.path());
    let captured = Arc::new(Mutex::new(None));
    let upstream =
        spawn_inspecting_upstream(captured.clone(), "text/event-stream", base_success_sse()).await;
    let _env = install_bearer_file_env(&config, &upstream, &key_path);

    let (status, response) = call_messages(json!({
        "model": "gpt-5.6",
        "max_tokens": 64,
        "messages": [{"role":"user","content":"hello Base"}]
    }))
    .await;

    assert_eq!(status, StatusCode::OK, "response={response}");
    assert_eq!(response["content"][0]["type"], "text");
    assert_eq!(response["content"][0]["text"], "base ok");
    let request = captured.lock().unwrap().clone().unwrap();
    assert_eq!(request.path, "/v1/responses");
    assert_eq!(request.body["model"], "gpt-5.6");
    assert!(request.body.get("metadata").is_none());
    assert_bearer_file_identity(&request);
}

#[cfg(unix)]
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn automatic_web_search_on_base_uses_native_responses_semantics() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    let key_path = write_bearer_token(config.path());
    let captured = Arc::new(Mutex::new(None));
    let upstream = spawn_inspecting_upstream(
        captured.clone(),
        "text/event-stream",
        hosted_web_search_sse(),
    )
    .await;
    let _env = install_bearer_file_env(&config, &upstream, &key_path);

    let (status, response) = call_messages(json!({
        "model": "gpt-5.6",
        "max_tokens": 64,
        "messages": [{"role":"user","content":"find Base documentation"}],
        "tools": [{
            "type":"web_search_20250305",
            "name":"web_search",
            "allowed_domains":["example.com"]
        }],
        "tool_choice":{"type":"auto"}
    }))
    .await;

    assert_eq!(status, StatusCode::OK, "response={response}");
    let content = response["content"].as_array().unwrap();
    assert_eq!(content[0]["type"], "server_tool_use");
    assert_eq!(content[0]["name"], "web_search");
    assert_eq!(content[1]["type"], "web_search_tool_result");
    assert_eq!(content[1]["content"][0]["url"], "https://example.com/base");
    let request = captured.lock().unwrap().clone().unwrap();
    assert_eq!(request.path, "/v1/responses");
    assert_eq!(request.body["model"], "gpt-5.6");
    assert_eq!(request.body["tools"][0]["type"], "web_search");
    assert_eq!(
        request.body["tools"][0]["filters"]["allowed_domains"],
        json!(["example.com"])
    );
    assert_bearer_file_identity(&request);
}

#[cfg(unix)]
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn forced_web_search_on_base_uses_alpha_search_and_structured_results() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    let key_path = write_bearer_token(config.path());
    let captured = Arc::new(Mutex::new(None));
    let upstream = spawn_inspecting_upstream(
        captured.clone(),
        "application/json",
        serde_json::to_vec(&json!({
            "encrypted_output":"opaque-fixture",
            "output":"standalone search ok",
            "results":[{
                "type":"text_result",
                "ref_id":"turn0search0",
                "url":"https://example.com/base",
                "title":"Base documentation"
            }]
        }))
        .unwrap(),
    )
    .await;
    let _env = install_bearer_file_env(&config, &upstream, &key_path);

    let (status, response) = call_messages(json!({
        "model": "gpt-5.6",
        "max_tokens": 64,
        "messages":[{
            "role":"user",
            "content":"Perform a web search for the query: official Base documentation"
        }],
        "tools": [{
            "type":"web_search_20250305",
            "name":"web_search",
            "allowed_domains":["example.com"]
        }],
        "tool_choice":{"type":"tool","name":"web_search"}
    }))
    .await;

    assert_eq!(status, StatusCode::OK, "response={response}");
    assert_eq!(response["content"][0]["type"], "server_tool_use");
    assert_eq!(response["content"][1]["type"], "web_search_tool_result");
    assert_eq!(
        response["content"][1]["content"][0]["url"],
        "https://example.com/base"
    );
    assert_eq!(response["content"][2]["text"], "standalone search ok");
    let request = captured.lock().unwrap().clone().unwrap();
    assert_eq!(request.path, "/v1/alpha/search");
    assert_eq!(request.body["model"], "gpt-5.6");
    assert_eq!(
        request.body["commands"]["search_query"][0]["q"],
        "official Base documentation"
    );
    assert_bearer_file_identity(&request);
}
