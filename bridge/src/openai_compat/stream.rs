use std::{collections::VecDeque, convert::Infallible, sync::Arc};

use axum::{
    Json,
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes::{Bytes, BytesMut};
use http_body_util::BodyExt;
use serde_json::{Value, json};

use crate::{
    provider::{Generation, GenerationBody},
    providers::codex::native::NativeResponseOutcome,
    traffic::{MAX_SSE_CAPTURE_BYTES, TrafficCapture},
};

use super::{
    MAX_PROVIDER_STREAM_BYTES, MAX_SSE_EVENT_BYTES, OpenAiError, OpenAiResponseMetadata,
    OpenAiSurface,
    response::{
        AnthropicAccumulator, BlockKind, SseEvent, buffered_response, chat_citation,
        chat_finish_reason, hosted_search_action, normalized_arguments, responses_citation,
        responses_response,
    },
};

fn upstream_invalid(message: impl Into<String>, _param: Option<impl Into<String>>) -> OpenAiError {
    OpenAiError::upstream_protocol(message)
}

#[derive(Default)]
pub struct SseDecoder {
    pending: BytesMut,
}

impl SseDecoder {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, OpenAiError> {
        self.pending.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some((position, delimiter_len)) = find_event_delimiter(&self.pending) {
            if position > MAX_SSE_EVENT_BYTES {
                return Err(OpenAiError::upstream_protocol(
                    "Provider SSE event exceeded the size limit",
                ));
            }
            let frame = self.pending.split_to(position + delimiter_len);
            let payload = &frame[..position];
            if let Some(event) = parse_frame(payload)? {
                events.push(event);
            }
        }
        if self.pending.len() > MAX_SSE_EVENT_BYTES {
            return Err(OpenAiError::upstream_protocol(
                "Provider SSE event exceeded the size limit",
            ));
        }
        Ok(events)
    }

    pub fn finish(&self) -> Result<(), OpenAiError> {
        if self.pending.iter().all(u8::is_ascii_whitespace) {
            Ok(())
        } else {
            Err(OpenAiError::upstream_protocol(
                "Provider SSE stream ended with an incomplete event",
            ))
        }
    }
}

pub async fn openai_response(
    surface: OpenAiSurface,
    generation: Generation,
    stream: bool,
    include_usage: bool,
    response_metadata: OpenAiResponseMetadata,
    traffic: Option<Arc<TrafficCapture>>,
) -> Result<Response, OpenAiError> {
    let model = generation.resolved_model.clone();
    let response_id = match surface {
        OpenAiSurface::ChatCompletions => format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
        OpenAiSurface::Responses => format!("resp_{}", uuid::Uuid::new_v4().simple()),
    };
    let created = current_seconds();
    if stream {
        return Ok(streaming_response(
            generation.body,
            Renderer::new(
                surface,
                include_usage,
                response_id,
                model,
                created,
                response_metadata,
            ),
            traffic,
        ));
    }
    let bytes = collect_generation(generation.body).await?;
    let events = decode_all(&bytes)?;
    let value = buffered_response(
        surface,
        &events,
        &response_id,
        &model,
        created,
        &response_metadata,
    )?;
    if let Some(capture) = traffic.as_ref() {
        capture.write_json("071-openai-downstream-response", &value);
    }
    Ok((StatusCode::OK, Json(value)).into_response())
}

async fn collect_generation(body: GenerationBody) -> Result<Bytes, OpenAiError> {
    match body {
        GenerationBody::BufferedSse(bytes) => {
            if bytes.len() > MAX_PROVIDER_STREAM_BYTES {
                Err(OpenAiError::upstream_protocol(
                    "Provider response exceeded the size limit",
                ))
            } else {
                Ok(bytes)
            }
        }
        GenerationBody::LiveSse(mut body) => {
            let mut output = BytesMut::new();
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|error| OpenAiError {
                    status: StatusCode::BAD_GATEWAY,
                    kind: "api_error".into(),
                    message: format!("Provider stream read failed: {error}").into(),
                    param: None,
                    code: None,
                    retry_after: None,
                })?;
                if let Ok(data) = frame.into_data() {
                    if output.len().saturating_add(data.len()) > MAX_PROVIDER_STREAM_BYTES {
                        return Err(OpenAiError::upstream_protocol(
                            "Provider response exceeded the size limit",
                        ));
                    }
                    output.extend_from_slice(&data);
                }
            }
            Ok(output.freeze())
        }
    }
}

fn decode_all(bytes: &[u8]) -> Result<Vec<SseEvent>, OpenAiError> {
    let mut decoder = SseDecoder::default();
    let events = decoder.push(bytes)?;
    decoder.finish()?;
    Ok(events)
}

fn streaming_response(
    body: GenerationBody,
    renderer: Renderer,
    traffic: Option<Arc<TrafficCapture>>,
) -> Response {
    let outcome = NativeResponseOutcome::default();
    let state = StreamState {
        body: match body {
            GenerationBody::BufferedSse(bytes) => Body::from(bytes),
            GenerationBody::LiveSse(body) => body,
        },
        decoder: SseDecoder::default(),
        renderer,
        pending: VecDeque::new(),
        finished: false,
        bytes: 0,
        outcome: outcome.clone(),
        traffic,
        downstream: Vec::new(),
        downstream_truncated: false,
        capture_finished: false,
    };
    let stream = futures_util::stream::unfold(state, |mut state| async move {
        state
            .next()
            .await
            .map(|bytes| (Ok::<Bytes, Infallible>(bytes), state))
    });
    let mut response = (
        [
            (http::header::CONTENT_TYPE, "text/event-stream"),
            (http::header::CACHE_CONTROL, "no-cache"),
            (http::header::CONNECTION, "keep-alive"),
        ],
        Body::from_stream(stream),
    )
        .into_response();
    response.extensions_mut().insert(outcome);
    response
}

struct StreamState {
    body: Body,
    decoder: SseDecoder,
    renderer: Renderer,
    pending: VecDeque<Bytes>,
    finished: bool,
    bytes: usize,
    outcome: NativeResponseOutcome,
    traffic: Option<Arc<TrafficCapture>>,
    downstream: Vec<u8>,
    downstream_truncated: bool,
    capture_finished: bool,
}

impl StreamState {
    async fn next(&mut self) -> Option<Bytes> {
        loop {
            if let Some(bytes) = self.pending.pop_front() {
                self.capture_bytes(&bytes);
                return Some(bytes);
            }
            if self.finished {
                let outcome = if self.outcome.failure().is_some() {
                    "failed"
                } else {
                    "completed"
                };
                self.finish_capture(outcome);
                return None;
            }
            match self.body.frame().await {
                Some(Ok(frame)) => {
                    let Ok(data) = frame.into_data() else {
                        continue;
                    };
                    self.bytes = self.bytes.saturating_add(data.len());
                    if self.bytes > MAX_PROVIDER_STREAM_BYTES {
                        self.fail(OpenAiError::upstream_protocol(
                            "Provider response exceeded the size limit",
                        ));
                        continue;
                    }
                    match self.decoder.push(&data) {
                        Ok(events) => {
                            for event in events {
                                match self.renderer.render(&event) {
                                    Ok(frames) => self.pending.extend(frames),
                                    Err(error) => {
                                        self.fail(error);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(error) => self.fail(error),
                    }
                }
                Some(Err(error)) => self.fail(OpenAiError {
                    status: StatusCode::BAD_GATEWAY,
                    kind: "api_error".into(),
                    message: format!("Provider stream read failed: {error}").into(),
                    param: None,
                    code: None,
                    retry_after: None,
                }),
                None => {
                    if let Err(error) = self.decoder.finish() {
                        self.fail(error);
                    } else if !self.renderer.state.stopped {
                        self.fail(upstream_invalid(
                            "Provider stream ended before message_stop",
                            None::<String>,
                        ));
                    } else {
                        self.finished = true;
                    }
                }
            }
        }
    }

    fn capture_bytes(&mut self, bytes: &[u8]) {
        let remaining = MAX_SSE_CAPTURE_BYTES.saturating_sub(self.downstream.len());
        self.downstream
            .extend_from_slice(&bytes[..bytes.len().min(remaining)]);
        if bytes.len() > remaining {
            self.downstream_truncated = true;
        }
    }

    fn finish_capture(&mut self, outcome: &str) {
        if self.capture_finished {
            return;
        }
        self.capture_finished = true;
        if let Some(traffic) = self.traffic.as_ref() {
            traffic.write_bytes("071-openai-downstream.sse", &self.downstream);
            traffic.write_json(
                "072-openai-stream-summary",
                &json!({
                    "outcome":outcome,
                    "capturedBytes":self.downstream.len(),
                    "truncated":self.downstream_truncated,
                }),
            );
        }
    }

    fn fail(&mut self, error: OpenAiError) {
        self.outcome.fail(error.message.to_string());
        self.pending.extend(self.renderer.failure(&error));
        self.finished = true;
    }
}

impl Drop for StreamState {
    fn drop(&mut self) {
        if !self.capture_finished {
            self.finish_capture("abandoned");
        }
    }
}

struct Renderer {
    surface: OpenAiSurface,
    include_usage: bool,
    response_id: String,
    model: String,
    created: u64,
    sequence: u64,
    state: AnthropicAccumulator,
    response_metadata: OpenAiResponseMetadata,
}

impl Renderer {
    fn new(
        surface: OpenAiSurface,
        include_usage: bool,
        response_id: String,
        model: String,
        created: u64,
        response_metadata: OpenAiResponseMetadata,
    ) -> Self {
        Self {
            surface,
            include_usage,
            response_id,
            model,
            created,
            sequence: 0,
            state: AnthropicAccumulator::default(),
            response_metadata,
        }
    }

    fn render(&mut self, event: &SseEvent) -> Result<Vec<Bytes>, OpenAiError> {
        self.state.apply(event)?;
        match self.surface {
            OpenAiSurface::ChatCompletions => self.render_chat(event),
            OpenAiSurface::Responses => self.render_responses(event),
        }
    }

    fn render_chat(&self, event: &SseEvent) -> Result<Vec<Bytes>, OpenAiError> {
        let kind = event
            .data
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut out = Vec::new();
        match kind {
            "message_start" => out.push(chat_data(json!({
                "id":self.response_id,
                "object":"chat.completion.chunk",
                "created":self.created,
                "model":self.model,
                "choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null,"logprobs":null}],
            }))),
            "content_block_start" => {
                let index = event.data.get("index").and_then(Value::as_u64).unwrap_or_default();
                if let Some(block) = self.state.blocks.iter().rev().find(|block| block.index == index as usize)
                    && let BlockKind::Tool { id, name, .. } = &block.kind
                {
                    let tool_index = self.tool_index(index as usize);
                    out.push(chat_data(json!({
                        "id":self.response_id,
                        "object":"chat.completion.chunk",
                        "created":self.created,
                        "model":self.model,
                        "choices":[{"index":0,"delta":{"tool_calls":[{"index":tool_index,"id":id,"type":"function","function":{"name":name,"arguments":""}}]},"finish_reason":null,"logprobs":null}],
                    })));
                }
            }
            "content_block_delta" => {
                let index = event.data.get("index").and_then(Value::as_u64).unwrap_or_default();
                let delta = event.data.get("delta").unwrap_or(&Value::Null);
                let payload = match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") => json!({"content":delta.get("text").and_then(Value::as_str).unwrap_or_default()}),
                    Some("thinking_delta") => json!({"reasoning_content":delta.get("thinking").and_then(Value::as_str).unwrap_or_default()}),
                    Some("input_json_delta") => {
                        if self.state.blocks.iter().any(|block| {
                            block.index == index as usize
                                && matches!(block.kind, BlockKind::Tool { .. })
                        }) {
                            let tool_index = self.tool_index(index as usize);
                            json!({"tool_calls":[{"index":tool_index,"function":{"arguments":delta.get("partial_json").and_then(Value::as_str).unwrap_or_default()}}]})
                        } else {
                            return Ok(out);
                        }
                    }
                    Some("citations_delta") => json!({"annotations":self.state.citations.iter().map(responses_citation).collect::<Vec<_>>().last().map(chat_citation).into_iter().collect::<Vec<_>>()}),
                    Some("signature_delta") => return Ok(out),
                    _ => return Err(upstream_invalid("Unsupported provider content delta", None::<String>)),
                };
                out.push(chat_data(json!({
                    "id":self.response_id,
                    "object":"chat.completion.chunk",
                    "created":self.created,
                    "model":self.model,
                    "choices":[{"index":0,"delta":payload,"finish_reason":null,"logprobs":null}],
                })));
            }
            "message_delta" => {
                let mut chunk = json!({
                    "id":self.response_id,
                    "object":"chat.completion.chunk",
                    "created":self.created,
                    "model":self.model,
                    "choices":[{"index":0,"delta":{},"finish_reason":chat_finish_reason(self.state.stop_reason.as_deref()),"logprobs":null}],
                });
                if self.include_usage {
                    chunk["usage"] = self.state.usage.chat_value();
                }
                out.push(chat_data(chunk));
            }
            "message_stop" => out.push(Bytes::from_static(b"data: [DONE]\n\n")),
            _ => {}
        }
        Ok(out)
    }

    fn render_responses(&mut self, event: &SseEvent) -> Result<Vec<Bytes>, OpenAiError> {
        let kind = event
            .data
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut out = Vec::new();
        match kind {
            "message_start" => {
                let response = response_shell(
                    &self.response_id,
                    &self.model,
                    self.created,
                    "in_progress",
                    &self.response_metadata,
                );
                out.push(self.responses_event("response.created", json!({"response":response})));
                out.push(
                    self.responses_event("response.in_progress", json!({"response":response})),
                );
            }
            "content_block_start" => {
                let index = event
                    .data
                    .get("index")
                    .and_then(Value::as_u64)
                    .unwrap_or_default() as usize;
                let output_index = self.output_index(index);
                let block = self
                    .state
                    .blocks
                    .iter()
                    .rev()
                    .find(|block| block.index == index)
                    .cloned()
                    .ok_or_else(|| {
                        upstream_invalid("Provider content block is missing", None::<String>)
                    })?;
                match &block.kind {
                    BlockKind::Text { .. } => {
                        let item_id = self.message_item_id(index);
                        out.push(self.responses_event("response.output_item.added", json!({
                            "output_index":output_index,
                            "item":{"id":item_id,"type":"message","role":"assistant","status":"in_progress","content":[]},
                        })));
                        out.push(self.responses_event("response.content_part.added", json!({
                            "item_id":item_id,"output_index":output_index,"content_index":0,
                            "part":{"type":"output_text","text":"","annotations":[]},
                        })));
                    }
                    BlockKind::Thinking { .. } => {
                        out.push(self.responses_event("response.output_item.added", json!({
                            "output_index":output_index,
                            "item":{"id":self.block_item_id("rs", index),"type":"reasoning","status":"in_progress","summary":[]},
                        })));
                        out.push(self.responses_event("response.reasoning_summary_part.added", json!({
                            "item_id":self.block_item_id("rs", index),"output_index":output_index,"summary_index":0,
                            "part":{"type":"summary_text","text":""},
                        })));
                    }
                    BlockKind::Tool { id, name, .. } => out.push(self.responses_event("response.output_item.added", json!({
                        "output_index":output_index,
                        "item":{"id":self.block_item_id("fc", index),"type":"function_call","call_id":id,"name":name,"arguments":"","status":"in_progress"},
                    }))),
                    BlockKind::HostedSearch { id, .. } => out.push(self.responses_event("response.output_item.added", json!({
                        "output_index":output_index,
                        "item":{"id":id,"type":"web_search_call","status":"in_progress"},
                    }))),
                    BlockKind::HostedResult => {}
                }
            }
            "content_block_delta" => {
                let index = event
                    .data
                    .get("index")
                    .and_then(Value::as_u64)
                    .unwrap_or_default() as usize;
                let output_index = self.output_index(index);
                let delta = event.data.get("delta").unwrap_or(&Value::Null);
                match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") => out.push(self.responses_event("response.output_text.delta", json!({
                        "item_id":self.message_item_id(index),"output_index":output_index,"content_index":0,
                        "delta":delta.get("text").and_then(Value::as_str).unwrap_or_default(),"logprobs":[],
                    }))),
                    Some("thinking_delta") => out.push(self.responses_event("response.reasoning_summary_text.delta", json!({
                        "item_id":self.block_item_id("rs", index),"output_index":output_index,"summary_index":0,
                        "delta":delta.get("thinking").and_then(Value::as_str).unwrap_or_default(),
                    }))),
                    Some("input_json_delta") => {
                        if self.state.blocks.iter().any(|block| {
                            block.index == index && matches!(block.kind, BlockKind::Tool { .. })
                        }) {
                            out.push(self.responses_event("response.function_call_arguments.delta", json!({
                                "item_id":self.block_item_id("fc", index),"output_index":output_index,
                                "delta":delta.get("partial_json").and_then(Value::as_str).unwrap_or_default(),
                            })));
                        }
                    }
                    Some("citations_delta") => out.push(self.responses_event("response.output_text.annotation.added", json!({
                        "item_id":self.message_item_id(index),"output_index":output_index,"content_index":0,
                        "annotation_index":self.state.citations.len().saturating_sub(1),
                        "annotation":self.state.citations.last().map(responses_citation).unwrap_or(Value::Null),
                    }))),
                    Some("signature_delta") => {}
                    _ => return Err(upstream_invalid("Unsupported provider content delta", None::<String>)),
                }
            }
            "content_block_stop" => {
                let index = event
                    .data
                    .get("index")
                    .and_then(Value::as_u64)
                    .unwrap_or_default() as usize;
                let output_index = self.output_index(index);
                let block = self
                    .state
                    .blocks
                    .iter()
                    .rev()
                    .find(|block| block.index == index)
                    .cloned()
                    .ok_or_else(|| {
                        upstream_invalid("Provider content block is missing", None::<String>)
                    })?;
                match block.kind {
                    BlockKind::Text { text } => {
                        out.push(self.responses_event("response.output_text.done", json!({
                            "item_id":self.message_item_id(index),"output_index":output_index,"content_index":0,"text":text,"logprobs":[],
                        })));
                        out.push(self.responses_event("response.content_part.done", json!({
                            "item_id":self.message_item_id(index),"output_index":output_index,"content_index":0,
                            "part":{"type":"output_text","text":text,"annotations":self.state.citations.iter().map(responses_citation).collect::<Vec<_>>()},
                        })));
                        out.push(self.responses_event("response.output_item.done", json!({
                            "output_index":output_index,
                            "item":{"id":self.message_item_id(index),"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":text,"annotations":self.state.citations.iter().map(responses_citation).collect::<Vec<_>>()}]},
                        })));
                    }
                    BlockKind::Thinking { text } => {
                        out.push(self.responses_event("response.reasoning_summary_text.done", json!({
                            "item_id":self.block_item_id("rs", index),"output_index":output_index,"summary_index":0,"text":text,
                        })));
                        out.push(self.responses_event("response.reasoning_summary_part.done", json!({
                            "item_id":self.block_item_id("rs", index),"output_index":output_index,"summary_index":0,
                            "part":{"type":"summary_text","text":text},
                        })));
                        out.push(self.responses_event("response.output_item.done", json!({
                            "output_index":output_index,
                            "item":{"id":self.block_item_id("rs", index),"type":"reasoning","status":"completed","summary":[{"type":"summary_text","text":text}]},
                        })));
                    }
                    BlockKind::Tool {
                        id,
                        name,
                        arguments,
                    } => {
                        let arguments = normalized_arguments(&arguments);
                        out.push(self.responses_event("response.function_call_arguments.done", json!({
                            "item_id":self.block_item_id("fc", index),"output_index":output_index,"arguments":arguments,
                        })));
                        out.push(self.responses_event("response.output_item.done", json!({
                            "output_index":output_index,
                            "item":{"id":self.block_item_id("fc", index),"type":"function_call","call_id":id,"name":name,"arguments":arguments,"status":"completed"},
                        })));
                    }
                    BlockKind::HostedSearch {
                        id,
                        name,
                        arguments,
                    } => out.push(self.responses_event("response.output_item.done", json!({
                        "output_index":output_index,
                        "item":{"id":id,"type":"web_search_call","status":"completed","action":hosted_search_action(&name, &arguments)},
                    }))),
                    BlockKind::HostedResult => {}
                }
            }
            "message_stop" => {
                let response = responses_response(
                    &self.state,
                    &self.response_id,
                    &self.model,
                    self.created,
                    &self.response_metadata,
                );
                let kind = if self.state.stop_reason.as_deref() == Some("max_tokens") {
                    "response.incomplete"
                } else {
                    "response.completed"
                };
                out.push(self.responses_event(kind, json!({"response":response})));
            }
            _ => {}
        }
        Ok(out)
    }

    fn failure(&mut self, error: &OpenAiError) -> Vec<Bytes> {
        match self.surface {
            OpenAiSurface::ChatCompletions => vec![
                chat_data(json!({
                    "error":{"message":error.message,"type":error.kind,"param":error.param,"code":error.code}
                })),
                Bytes::from_static(b"data: [DONE]\n\n"),
            ],
            OpenAiSurface::Responses => {
                let mut response = response_shell(
                    &self.response_id,
                    &self.model,
                    self.created,
                    "failed",
                    &self.response_metadata,
                );
                response["error"] = json!({"code":error.code,"message":error.message});
                vec![self.responses_event("response.failed", json!({"response":response}))]
            }
        }
    }

    fn responses_event(&mut self, kind: &str, fields: Value) -> Bytes {
        let sequence = self.sequence;
        self.sequence = self.sequence.saturating_add(1);
        let mut value = fields.as_object().cloned().unwrap_or_default();
        value.insert("type".to_string(), Value::String(kind.to_string()));
        value.insert("sequence_number".to_string(), json!(sequence));
        named_sse(kind, Value::Object(value))
    }

    fn message_item_id(&self, block_index: usize) -> String {
        self.block_item_id("msg", block_index)
    }

    fn output_index(&self, block_index: usize) -> usize {
        self.state
            .blocks
            .iter()
            .filter(|block| {
                block.index < block_index && !matches!(block.kind, BlockKind::HostedResult)
            })
            .count()
    }

    fn tool_index(&self, block_index: usize) -> usize {
        self.state
            .blocks
            .iter()
            .filter(|block| {
                block.index < block_index && matches!(block.kind, BlockKind::Tool { .. })
            })
            .count()
    }

    fn block_item_id(&self, prefix: &str, index: usize) -> String {
        format!(
            "{prefix}_{}_{index}",
            self.response_id.trim_start_matches("resp_")
        )
    }
}

fn chat_data(value: Value) -> Bytes {
    Bytes::from(format!("data: {}\n\n", value))
}

fn named_sse(kind: &str, value: Value) -> Bytes {
    Bytes::from(format!("event: {kind}\ndata: {value}\n\n"))
}

fn response_shell(
    id: &str,
    model: &str,
    created: u64,
    status: &str,
    response_metadata: &OpenAiResponseMetadata,
) -> Value {
    json!({
        "id":id,
        "object":"response",
        "created_at":created,
        "status":status,
        "model":model,
        "output":[],
        "parallel_tool_calls":false,
        "tool_choice":response_metadata.tool_choice,
        "tools":response_metadata.tools,
        "error":null,
        "incomplete_details":null,
        "usage":null,
    })
}

fn parse_frame(frame: &[u8]) -> Result<Option<SseEvent>, OpenAiError> {
    let text = std::str::from_utf8(frame)
        .map_err(|_| OpenAiError::upstream_protocol("Provider SSE event is not UTF-8"))?;
    let mut event = None;
    let mut data = Vec::new();
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.starts_with(':') || line.is_empty() {
            continue;
        }
        if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim_start().to_string());
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.trim_start());
        }
    }
    if data.is_empty() {
        return Ok(None);
    }
    let data = data.join("\n");
    if data == "[DONE]" {
        return Ok(None);
    }
    let data = serde_json::from_str(&data).map_err(|error| {
        OpenAiError::upstream_protocol(format!("Provider SSE event contains invalid JSON: {error}"))
    })?;
    Ok(Some(SseEvent { event, data }))
}

fn find_event_delimiter(bytes: &[u8]) -> Option<(usize, usize)> {
    bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|position| (position, 2))
        .or_else(|| {
            bytes
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| (position, 4))
        })
}

fn current_seconds() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[test]
    fn decoder_handles_fragmented_and_batched_events() {
        let mut decoder = SseDecoder::default();
        assert!(
            decoder
                .push(b"event: message_start\nda")
                .unwrap()
                .is_empty()
        );
        let events = decoder
            .push(b"ta: {\"type\":\"message_start\",\"message\":{}}\n\nevent: ping\ndata: {\"type\":\"ping\"}\n\n")
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event.as_deref(), Some("message_start"));
        decoder.finish().unwrap();
    }

    #[test]
    fn decoder_accepts_large_batches_of_small_events() {
        let event = "event: ping\ndata: {\"type\":\"ping\"}\n\n";
        let count = MAX_SSE_EVENT_BYTES / event.len() + 1;
        let mut decoder = SseDecoder::default();
        let events = decoder.push(event.repeat(count).as_bytes()).unwrap();
        assert_eq!(events.len(), count);
        decoder.finish().unwrap();
    }

    #[tokio::test]
    async fn chat_stream_emits_tool_deltas_usage_and_done() {
        let input = concat!(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":2}}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call_1\",\"name\":\"lookup\",\"input\":{}}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{}\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":1}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        );
        let response = streaming_response(
            GenerationBody::BufferedSse(Bytes::from_static(input.as_bytes())),
            Renderer::new(
                OpenAiSurface::ChatCompletions,
                true,
                "chatcmpl_test".into(),
                "kimi-k2.6".into(),
                1,
                OpenAiResponseMetadata::default(),
            ),
            None,
        );
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("tool_calls"));
        assert!(text.contains("\"finish_reason\":\"tool_calls\""));
        assert!(text.contains("\"total_tokens\":3"));
        assert!(text.ends_with("data: [DONE]\n\n"));
    }

    #[tokio::test]
    async fn responses_stream_numbers_events_and_completes() {
        let input = concat!(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{}}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        );
        let response = streaming_response(
            GenerationBody::BufferedSse(Bytes::from_static(input.as_bytes())),
            Renderer::new(
                OpenAiSurface::Responses,
                false,
                "resp_test".into(),
                "grok-4.5".into(),
                1,
                OpenAiResponseMetadata::default(),
            ),
            None,
        );
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("event: response.created"));
        assert!(text.contains("event: response.output_text.delta"));
        assert!(text.contains("event: response.completed"));
        assert!(text.contains("\"sequence_number\":0"));
    }
}
