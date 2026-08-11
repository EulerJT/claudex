use base64::Engine;
use bytes::Bytes;
use futures_util::StreamExt;
use prost::Message;
use tokio::sync::mpsc;

use crate::config;
use crate::providers::cursor::connect::{
    ConnectFrame, ConnectFrameDecoder, FLAG_END, FLAG_GZIP, encode_connect_frame,
    parse_connect_error,
};
use crate::providers::cursor::model::CursorModelResolution;
use crate::providers::cursor::proto;
use crate::providers::cursor::request::CursorSelectedImage;

/// Upstream response from the Cursor API.
///
/// Contains the raw response bytes (or body bytes for streaming) and the
/// HTTP status.
pub struct CursorUpstreamResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub error_detail: Option<String>,
}

impl CursorUpstreamResponse {
    pub fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }
}

/// HTTP/2 client for the Cursor AgentService/Run endpoint.
pub struct CursorHttpClient {
    client: reqwest::Client,
    base_url: String,
}

impl Default for CursorHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl CursorHttpClient {
    pub fn new() -> Self {
        // Use HTTP/2 prior knowledge for cleartext URLs (mock testing) and
        // standard TLS for https URLs.
        let base_url = config::cursor_base_url();
        let is_cleartext = base_url.starts_with("http://");

        let mut builder = reqwest::Client::builder()
            .http2_keep_alive_timeout(std::time::Duration::from_secs(30))
            .http2_keep_alive_while_idle(true);

        if is_cleartext {
            builder = builder.http2_prior_knowledge();
        }

        let client = builder.build().expect("CursorHttpClient: reqwest client");

        Self { client, base_url }
    }

    /// Run the Cursor agent with the given prompt and token.
    ///
    /// Opens a bidirectional Connect stream, sends the agent request frames,
    /// and keeps the request body alive while collecting the response frames.
    pub async fn run_agent(
        &self,
        token: &str,
        prompt: &str,
        model: &str,
        images: &[CursorSelectedImage],
    ) -> Result<CursorUpstreamResponse, CursorError> {
        let resolved = super::model::resolve_cursor_model(model)
            .map_err(|e| CursorError::internal(format!("model resolution: {e}")))?;

        let request_id = uuid::Uuid::new_v4().to_string();
        let frames = build_run_frames(prompt, &resolved, images, &request_id);
        let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(8);
        let sender = tokio::spawn(async move {
            for (index, frame) in frames.into_iter().enumerate() {
                if tx.send(Ok(frame)).await.is_err() {
                    return;
                }
                let delay = match index {
                    0 => std::time::Duration::from_millis(1500),
                    1 => std::time::Duration::from_millis(800),
                    _ => std::time::Duration::from_millis(400),
                };
                tokio::time::sleep(delay).await;
            }

            let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(5));
            heartbeat.tick().await;
            loop {
                heartbeat.tick().await;
                if tx.send(Ok(heartbeat_frame())).await.is_err() {
                    return;
                }
            }
        });
        let body =
            reqwest::Body::wrap_stream(futures_util::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            }));

        let url = format!(
            "{}/agent.v1.AgentService/Run",
            self.base_url.trim_end_matches('/')
        );
        let client_version = config::cursor_client_version();

        let response = self
            .client
            .post(&url)
            .bearer_auth(token)
            .header("content-type", "application/connect+proto")
            .header("connect-protocol-version", "1")
            .header("connect-accept-encoding", "gzip,br")
            .header("user-agent", "connect-es/1.6.1")
            .header("x-cursor-client-type", "cli")
            .header("x-cursor-client-version", &client_version)
            .header("x-ghost-mode", "true")
            .header("x-request-id", &request_id)
            .header("x-original-request-id", &request_id)
            .header("x-cursor-streaming", "true")
            .header("te", "trailers")
            .body(body)
            .send()
            .await
            .map_err(CursorError::from_reqwest)?;

        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let error_detail = response
            .headers()
            .get("grpc-message")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let mut stream = response.bytes_stream();
        let mut body_bytes = Vec::new();
        let mut received_data = false;

        loop {
            let timeout = if received_data {
                std::time::Duration::from_secs(5)
            } else {
                std::time::Duration::from_secs(60)
            };
            match tokio::time::timeout(timeout, stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    received_data = true;
                    body_bytes.extend_from_slice(&chunk);
                    if contains_end_frame(&body_bytes) {
                        break;
                    }
                }
                Ok(Some(Err(error))) => {
                    sender.abort();
                    return Err(CursorError::internal(format!("read body: {error}")));
                }
                Ok(None) => break,
                Err(_) if received_data => break,
                Err(_) => {
                    sender.abort();
                    return Err(CursorError::internal(
                        "Cursor upstream timed out before sending a response",
                    ));
                }
            }
        }
        sender.abort();

        if status >= 400 {
            let detail = parse_error_body(&body_bytes, &headers);
            return Err(CursorError::new(status, "Cursor upstream error", detail));
        }

        Ok(CursorUpstreamResponse {
            status,
            body: body_bytes,
            error_detail,
        })
    }
}

fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push(((value as u8) & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn field_bytes(field: u64, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 4);
    encode_varint((field << 3) | 2, &mut out);
    encode_varint(value.len() as u64, &mut out);
    out.extend_from_slice(value);
    out
}

fn field_string(field: u64, value: &str) -> Vec<u8> {
    field_bytes(field, value.as_bytes())
}

fn field_varint(field: u64, value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    encode_varint(field << 3, &mut out);
    encode_varint(value, &mut out);
    out
}

fn model_message(model: &str, fast: bool) -> Vec<u8> {
    let mut out = field_string(1, model);
    let mut parameter = field_string(1, "fast");
    parameter.extend(field_string(2, if fast { "true" } else { "false" }));
    out.extend(field_bytes(3, &parameter));
    out
}

fn mode_value(resolved: &CursorModelResolution) -> u64 {
    match resolved.mode {
        super::model::CursorAgentMode::Agent => 1,
        super::model::CursorAgentMode::Ask => 2,
        super::model::CursorAgentMode::Plan => 3,
    }
}

fn selected_context(images: &[CursorSelectedImage]) -> Option<Vec<u8>> {
    if images.is_empty() {
        return None;
    }

    let mut context = Vec::new();
    for image in images {
        let data = base64::engine::general_purpose::STANDARD
            .decode(&image.data)
            .unwrap_or_default();
        let mut selected = field_string(2, &image.uuid);
        selected.extend(field_string(3, &image.path));
        selected.extend(field_string(7, &image.mime_type));
        selected.extend(field_bytes(8, &data));
        context.extend(field_bytes(1, &selected));
    }
    Some(context)
}

fn build_run_frames(
    prompt: &str,
    resolved: &CursorModelResolution,
    images: &[CursorSelectedImage],
    request_id: &str,
) -> Vec<Bytes> {
    let conversation_id = uuid::Uuid::new_v4().to_string();
    let mut user_message = field_string(1, prompt);
    user_message.extend(field_string(2, request_id));
    if let Some(context) = selected_context(images) {
        user_message.extend(field_bytes(3, &context));
    } else {
        user_message.extend(field_bytes(3, &[]));
    }
    user_message.extend(field_varint(4, mode_value(resolved)));

    let action = field_bytes(1, &field_bytes(1, &user_message));
    let mut request = field_bytes(1, &[]);
    request.extend(field_bytes(2, &action));
    request.extend(field_bytes(4, &[]));
    request.extend(field_string(5, &conversation_id));
    request.extend(field_bytes(
        9,
        &model_message(&resolved.model_id, resolved.fast),
    ));
    request.extend(field_varint(12, 0));
    request.extend(field_bytes(14, &field_string(1, "default")));
    request.extend(field_bytes(
        14,
        &model_message(&resolved.model_id, resolved.fast),
    ));
    request.extend(field_string(16, &conversation_id));
    let first = encode_connect_frame(field_bytes(1, &request), 0);

    let cwd = std::env::current_dir()
        .ok()
        .and_then(|path| path.to_str().map(str::to_string))
        .unwrap_or_default();
    let mut environment = field_string(1, std::env::consts::OS);
    environment.extend(field_string(2, &cwd));
    environment.extend(field_string(
        3,
        if cfg!(windows) { "powershell" } else { "bash" },
    ));
    environment.extend(field_string(10, "UTC"));
    environment.extend(field_string(11, &cwd));
    environment.extend(field_varint(14, 1));
    environment.extend(field_varint(16, 1));
    environment.extend(field_varint(19, 0));
    environment.extend(field_varint(20, 0));
    environment.extend(field_string(21, &cwd));
    environment.extend(field_varint(22, 0));
    let context = field_bytes(
        2,
        &field_bytes(
            10,
            &field_bytes(1, &field_bytes(1, &field_bytes(4, &environment))),
        ),
    );

    let mut frames = vec![first, encode_connect_frame(context, 0)];
    frames.push(encode_connect_frame(
        field_bytes(5, &field_string(1, "")),
        0,
    ));
    frames.push(encode_connect_frame(
        field_bytes(3, &field_string(3, "")),
        0,
    ));
    for sequence in 1..=8 {
        let mut marker = field_varint(1, sequence);
        marker.extend(field_string(3, ""));
        frames.push(encode_connect_frame(field_bytes(3, &marker), 0));
    }
    frames
}

fn heartbeat_frame() -> Bytes {
    encode_connect_frame(field_bytes(7, &[]), 0)
}

fn contains_end_frame(body: &[u8]) -> bool {
    let mut offset = 0;
    while body.len().saturating_sub(offset) >= 5 {
        let length = u32::from_be_bytes([
            body[offset + 1],
            body[offset + 2],
            body[offset + 3],
            body[offset + 4],
        ]) as usize;
        if body.len().saturating_sub(offset) < 5 + length {
            return false;
        }
        if body[offset] & FLAG_END != 0 {
            return true;
        }
        offset += 5 + length;
    }
    false
}

fn parse_error_body(body_bytes: &[u8], _headers: &reqwest::header::HeaderMap) -> Option<String> {
    if body_bytes.len() < 5 {
        return None;
    }
    // Try to parse as Connect end frame with JSON error
    if body_bytes.len() >= 5 {
        let flags = body_bytes[0];
        let len = u32::from_be_bytes([body_bytes[1], body_bytes[2], body_bytes[3], body_bytes[4]])
            as usize;
        if flags & FLAG_END != 0 && body_bytes.len() >= 5 + len {
            let payload = &body_bytes[5..5 + len];
            let err = parse_connect_error(payload);
            if err.is_some() {
                return err.map(|e| e.detail);
            }
        }
    }

    // Try plain text error
    if let Ok(text) = String::from_utf8(body_bytes.to_vec())
        && !text.is_empty()
    {
        return Some(text);
    }
    None
}

/// Decode upstream response bytes into Connect frames containing
/// AgentServerMessage values.
pub fn decode_upstream_frames(body: &[u8]) -> Result<Vec<ConnectFrame>, CursorError> {
    let mut decoder = ConnectFrameDecoder::new();
    let frames = decoder
        .push(body)
        .map_err(|e| CursorError::internal(format!("frame decode: {e}")))?;
    Ok(frames)
}

/// Decode a single Connect frame payload into an AgentServerMessage.
/// Handles gzip decompression if the FLAG_GZIP bit is set.
pub fn decode_frame_payload(
    frame: &ConnectFrame,
) -> Result<proto::AgentServerMessage, CursorError> {
    let payload = if frame.flags & FLAG_GZIP != 0 {
        super::connect::decode_gzip_frame(&frame.payload)
            .map_err(|e| CursorError::internal(format!("gzip decompress: {e}")))?
    } else {
        frame.payload.to_vec()
    };

    proto::AgentServerMessage::decode(&payload[..])
        .map_err(|e| CursorError::internal(format!("prost decode: {e}")))
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CursorError {
    pub status: u16,
    pub message: String,
    pub detail: Option<String>,
    pub retry_after: Option<String>,
}

impl CursorError {
    pub fn new(status: u16, message: impl Into<String>, detail: Option<String>) -> Self {
        Self {
            status,
            message: message.into(),
            detail,
            retry_after: None,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            status: 502,
            message: message.into(),
            detail: None,
            retry_after: None,
        }
    }

    pub fn from_reqwest(e: reqwest::Error) -> Self {
        let status = e.status().map(|s| s.as_u16()).unwrap_or(502);
        Self {
            status,
            message: e.to_string(),
            detail: None,
            retry_after: None,
        }
    }
}

impl std::fmt::Display for CursorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cursor error {}: {}", self.status, self.message)
    }
}

impl std::error::Error for CursorError {}
