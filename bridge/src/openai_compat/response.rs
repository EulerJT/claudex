use serde_json::{Value, json};

use super::{OpenAiError, OpenAiResponseMetadata, OpenAiSurface};

fn upstream_invalid(message: impl Into<String>, _param: Option<impl Into<String>>) -> OpenAiError {
    OpenAiError::upstream_protocol(message)
}

#[derive(Debug, Clone)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: Value,
}

#[derive(Debug, Clone)]
pub enum BlockKind {
    Text {
        text: String,
    },
    Thinking {
        text: String,
    },
    Tool {
        id: String,
        name: String,
        arguments: String,
    },
    HostedSearch {
        id: String,
        name: String,
        arguments: String,
    },
    HostedResult,
}

#[derive(Debug, Clone)]
pub struct Block {
    pub index: usize,
    pub kind: BlockKind,
}

#[derive(Debug, Clone, Default)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
}

impl Usage {
    pub fn chat_value(&self) -> Value {
        json!({
            "prompt_tokens": self.input_tokens + self.cache_read_tokens,
            "completion_tokens": self.output_tokens,
            "total_tokens": self.input_tokens + self.cache_read_tokens + self.output_tokens,
            "prompt_tokens_details": {"cached_tokens": self.cache_read_tokens},
        })
    }

    pub fn responses_value(&self) -> Value {
        json!({
            "input_tokens": self.input_tokens + self.cache_read_tokens,
            "input_tokens_details": {"cached_tokens": self.cache_read_tokens},
            "output_tokens": self.output_tokens,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": self.input_tokens + self.cache_read_tokens + self.output_tokens,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct AnthropicAccumulator {
    pub upstream_id: Option<String>,
    pub model: Option<String>,
    pub blocks: Vec<Block>,
    pub stop_reason: Option<String>,
    pub usage: Usage,
    pub citations: Vec<Value>,
    pub stopped: bool,
}

impl AnthropicAccumulator {
    pub fn apply(&mut self, event: &SseEvent) -> Result<(), OpenAiError> {
        let kind = event
            .data
            .get("type")
            .and_then(Value::as_str)
            .or(event.event.as_deref())
            .unwrap_or_default();
        match kind {
            "message_start" => {
                let message = event.data.get("message").ok_or_else(|| {
                    upstream_invalid(
                        "Provider message_start is missing 'message'",
                        None::<String>,
                    )
                })?;
                self.upstream_id = message
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                self.model = message
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                self.update_usage(message.get("usage"));
            }
            "content_block_start" => {
                let index = required_index(&event.data)?;
                let block = event.data.get("content_block").ok_or_else(|| {
                    upstream_invalid("Provider content block is missing", None::<String>)
                })?;
                let kind = match block.get("type").and_then(Value::as_str) {
                    Some("text") => BlockKind::Text {
                        text: block
                            .get("text")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                    },
                    Some("thinking") => BlockKind::Thinking {
                        text: block
                            .get("thinking")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                    },
                    Some("tool_use") => BlockKind::Tool {
                        id: required_block_string(block, "id")?,
                        name: required_block_string(block, "name")?,
                        arguments: block
                            .get("input")
                            .filter(|value| !value.is_null())
                            .map(Value::to_string)
                            .filter(|value| value != "{}")
                            .unwrap_or_default(),
                    },
                    Some("server_tool_use") => BlockKind::HostedSearch {
                        id: required_block_string(block, "id")?,
                        name: required_block_string(block, "name")?,
                        arguments: String::new(),
                    },
                    Some(kind) if kind.ends_with("_tool_result") => BlockKind::HostedResult,
                    Some(other) => {
                        return Err(upstream_invalid(
                            format!("Unsupported provider output block '{other}'"),
                            None::<String>,
                        ));
                    }
                    None => {
                        return Err(upstream_invalid(
                            "Provider output block has no type",
                            None::<String>,
                        ));
                    }
                };
                self.blocks.push(Block { index, kind });
            }
            "content_block_delta" => {
                let index = required_index(&event.data)?;
                let delta = event.data.get("delta").ok_or_else(|| {
                    upstream_invalid("Provider content delta is missing", None::<String>)
                })?;
                let block = self
                    .blocks
                    .iter_mut()
                    .rev()
                    .find(|block| block.index == index)
                    .ok_or_else(|| {
                        upstream_invalid(
                            "Provider delta references an unknown block",
                            None::<String>,
                        )
                    })?;
                match (delta.get("type").and_then(Value::as_str), &mut block.kind) {
                    (Some("text_delta"), BlockKind::Text { text }) => {
                        text.push_str(
                            delta
                                .get("text")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                        );
                    }
                    (Some("thinking_delta"), BlockKind::Thinking { text }) => {
                        text.push_str(
                            delta
                                .get("thinking")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                        );
                    }
                    (Some("signature_delta"), BlockKind::Thinking { .. }) => {}
                    (
                        Some("input_json_delta"),
                        BlockKind::Tool { arguments, .. }
                        | BlockKind::HostedSearch { arguments, .. },
                    ) => {
                        arguments.push_str(
                            delta
                                .get("partial_json")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                        );
                    }
                    (Some("citations_delta"), BlockKind::Text { .. }) => {
                        if let Some(citation) = delta.get("citation") {
                            self.citations.push(citation.clone());
                        }
                    }
                    _ => {
                        return Err(upstream_invalid(
                            "Provider delta does not match its content block",
                            None::<String>,
                        ));
                    }
                }
            }
            "content_block_stop" => {
                let index = required_index(&event.data)?;
                if let Some(arguments) = self.blocks.iter().find_map(|block| {
                    if block.index != index {
                        return None;
                    }
                    match &block.kind {
                        BlockKind::Tool { arguments, .. }
                        | BlockKind::HostedSearch { arguments, .. } => Some(arguments),
                        _ => None,
                    }
                }) && !arguments.is_empty()
                    && serde_json::from_str::<Value>(arguments).is_err()
                {
                    return Err(OpenAiError::upstream_protocol(
                        "Provider emitted malformed tool arguments",
                    ));
                }
            }
            "message_delta" => {
                self.stop_reason = event
                    .data
                    .pointer("/delta/stop_reason")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                self.update_usage(event.data.get("usage"));
            }
            "message_stop" => self.stopped = true,
            "ping" => {}
            "error" => {
                return Err(OpenAiError {
                    status: http::StatusCode::BAD_GATEWAY,
                    kind: event
                        .data
                        .pointer("/error/type")
                        .and_then(Value::as_str)
                        .unwrap_or("api_error")
                        .into(),
                    message: event
                        .data
                        .pointer("/error/message")
                        .and_then(Value::as_str)
                        .unwrap_or("Provider stream failed")
                        .into(),
                    param: None,
                    code: None,
                    retry_after: None,
                });
            }
            other if !other.is_empty() => {
                return Err(upstream_invalid(
                    format!("Unsupported provider stream event '{other}'"),
                    None::<String>,
                ));
            }
            _ => {}
        }
        Ok(())
    }

    fn update_usage(&mut self, usage: Option<&Value>) {
        let Some(usage) = usage else { return };
        if let Some(value) = usage.get("input_tokens").and_then(Value::as_u64) {
            self.usage.input_tokens = value;
        }
        if let Some(value) = usage.get("output_tokens").and_then(Value::as_u64) {
            self.usage.output_tokens = value;
        }
        if let Some(value) = usage.get("cache_read_input_tokens").and_then(Value::as_u64) {
            self.usage.cache_read_tokens = value;
        }
        if let Some(value) = usage
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
        {
            self.usage.cache_creation_tokens = value;
        }
    }
}

pub fn buffered_response(
    surface: OpenAiSurface,
    events: &[SseEvent],
    response_id: &str,
    model: &str,
    created: u64,
    response_metadata: &OpenAiResponseMetadata,
) -> Result<Value, OpenAiError> {
    let mut state = AnthropicAccumulator::default();
    for event in events {
        state.apply(event)?;
    }
    if !state.stopped {
        return Err(upstream_invalid(
            "Provider stream ended before message_stop",
            None::<String>,
        ));
    }
    match surface {
        OpenAiSurface::ChatCompletions => Ok(chat_response(&state, response_id, model, created)),
        OpenAiSurface::Responses => Ok(responses_response(
            &state,
            response_id,
            model,
            created,
            response_metadata,
        )),
    }
}

pub fn chat_response(
    state: &AnthropicAccumulator,
    response_id: &str,
    model: &str,
    created: u64,
) -> Value {
    let content = state
        .blocks
        .iter()
        .filter_map(|block| match &block.kind {
            BlockKind::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    let reasoning = state
        .blocks
        .iter()
        .filter_map(|block| match &block.kind {
            BlockKind::Thinking { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    let tools: Vec<Value> = state
        .blocks
        .iter()
        .filter_map(|block| match &block.kind {
            BlockKind::Tool {
                id,
                name,
                arguments,
            } => Some(json!({
                "id":id,
                "type":"function",
                "function":{"name":name, "arguments":normalized_arguments(arguments)},
            })),
            _ => None,
        })
        .collect();
    let mut message = serde_json::Map::from_iter([
        ("role".to_string(), json!("assistant")),
        (
            "content".to_string(),
            if content.is_empty() {
                Value::Null
            } else {
                Value::String(content)
            },
        ),
        ("refusal".to_string(), Value::Null),
    ]);
    if !reasoning.is_empty() {
        message.insert("reasoning_content".to_string(), Value::String(reasoning));
    }
    if !tools.is_empty() {
        message.insert("tool_calls".to_string(), Value::Array(tools));
    }
    if !state.citations.is_empty() {
        message.insert(
            "annotations".to_string(),
            Value::Array(state.citations.iter().map(chat_citation).collect()),
        );
    }
    json!({
        "id":response_id,
        "object":"chat.completion",
        "created":created,
        "model":model,
        "choices":[{
            "index":0,
            "message":message,
            "finish_reason":chat_finish_reason(state.stop_reason.as_deref()),
            "logprobs":null,
        }],
        "usage":state.usage.chat_value(),
    })
}

pub fn responses_response(
    state: &AnthropicAccumulator,
    response_id: &str,
    model: &str,
    created: u64,
    response_metadata: &OpenAiResponseMetadata,
) -> Value {
    let mut output = Vec::new();
    for block in state
        .blocks
        .iter()
        .filter(|block| !matches!(block.kind, BlockKind::HostedResult))
    {
        match &block.kind {
            BlockKind::Text { text } => output.push(json!({
                "id":format!("msg_{}", stable_suffix(response_id, block.index)),
                "type":"message",
                "role":"assistant",
                "status":"completed",
                "content":[{"type":"output_text", "text":text, "annotations":state.citations.iter().map(responses_citation).collect::<Vec<_>>() }],
            })),
            BlockKind::Thinking { text } => output.push(json!({
                "id":format!("rs_{}", stable_suffix(response_id, block.index)),
                "type":"reasoning",
                "summary":[{"type":"summary_text", "text":text}],
                "status":"completed",
            })),
            BlockKind::Tool {
                id,
                name,
                arguments,
            } => output.push(json!({
                "id":format!("fc_{}", stable_suffix(response_id, block.index)),
                "type":"function_call",
                "call_id":id,
                "name":name,
                "arguments":normalized_arguments(arguments),
                "status":"completed",
            })),
            BlockKind::HostedSearch {
                id,
                name,
                arguments,
            } => output.push(json!({
                "id":id,
                "type":"web_search_call",
                "status":"completed",
                "action":hosted_search_action(name, arguments),
            })),
            BlockKind::HostedResult => unreachable!(),
        }
    }
    let incomplete = state.stop_reason.as_deref() == Some("max_tokens");
    json!({
        "id":response_id,
        "object":"response",
        "created_at":created,
        "status":if incomplete { "incomplete" } else { "completed" },
        "model":model,
        "output":output,
        "parallel_tool_calls":false,
        "tool_choice":response_metadata.tool_choice,
        "tools":response_metadata.tools,
        "error":null,
        "incomplete_details":if incomplete { json!({"reason":"max_output_tokens"}) } else { Value::Null },
        "usage":state.usage.responses_value(),
    })
}

pub fn chat_finish_reason(reason: Option<&str>) -> &'static str {
    match reason {
        Some("tool_use") => "tool_calls",
        Some("max_tokens") => "length",
        _ => "stop",
    }
}

pub fn hosted_search_action(_name: &str, arguments: &str) -> Value {
    let query = serde_json::from_str::<Value>(arguments)
        .ok()
        .and_then(|value| {
            value
                .get("query")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default();
    json!({"type":"search", "query":query})
}

pub fn chat_citation(citation: &Value) -> Value {
    json!({
        "type":"url_citation",
        "url_citation":{
            "url":citation.get("url").and_then(Value::as_str).unwrap_or_default(),
            "title":citation.get("title").and_then(Value::as_str).unwrap_or_default(),
            "start_index":citation.get("start_index").and_then(Value::as_u64).unwrap_or_default(),
            "end_index":citation.get("end_index").and_then(Value::as_u64).unwrap_or_default(),
        }
    })
}

pub fn responses_citation(citation: &Value) -> Value {
    json!({
        "type":"url_citation",
        "url":citation.get("url").and_then(Value::as_str).unwrap_or_default(),
        "title":citation.get("title").and_then(Value::as_str).unwrap_or_default(),
        "start_index":citation.get("start_index").and_then(Value::as_u64).unwrap_or_default(),
        "end_index":citation.get("end_index").and_then(Value::as_u64).unwrap_or_default(),
    })
}

pub fn normalized_arguments(arguments: &str) -> String {
    if arguments.is_empty() {
        "{}".to_string()
    } else {
        arguments.to_string()
    }
}

fn stable_suffix(response_id: &str, index: usize) -> String {
    format!("{}_{index}", response_id.trim_start_matches("resp_"))
}

fn required_index(value: &Value) -> Result<usize, OpenAiError> {
    value
        .get("index")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| upstream_invalid("Provider event has an invalid index", None::<String>))
}

fn required_block_string(block: &Value, key: &str) -> Result<String, OpenAiError> {
    block
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            upstream_invalid(
                format!("Provider tool block has an invalid '{key}'"),
                None::<String>,
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events() -> Vec<SseEvent> {
        vec![
            SseEvent {
                event: Some("message_start".into()),
                data: json!({"type":"message_start","message":{"id":"msg_1","model":"kimi-k2.6","usage":{"input_tokens":7}}}),
            },
            SseEvent {
                event: Some("content_block_start".into()),
                data: json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            },
            SseEvent {
                event: Some("content_block_delta".into()),
                data: json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}),
            },
            SseEvent {
                event: Some("content_block_stop".into()),
                data: json!({"type":"content_block_stop","index":0}),
            },
            SseEvent {
                event: Some("content_block_start".into()),
                data: json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"call_1","name":"lookup","input":{}}}),
            },
            SseEvent {
                event: Some("content_block_delta".into()),
                data: json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"q\":\"x\"}"}}),
            },
            SseEvent {
                event: Some("content_block_stop".into()),
                data: json!({"type":"content_block_stop","index":1}),
            },
            SseEvent {
                event: Some("message_delta".into()),
                data: json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":3}}),
            },
            SseEvent {
                event: Some("message_stop".into()),
                data: json!({"type":"message_stop"}),
            },
        ]
    }

    #[test]
    fn maps_hosted_search_and_citations_to_responses() {
        let events = vec![
            SseEvent {
                event: Some("message_start".into()),
                data: json!({"type":"message_start","message":{"id":"msg_1","usage":{}}}),
            },
            SseEvent {
                event: Some("content_block_start".into()),
                data: json!({"type":"content_block_start","index":0,"content_block":{"type":"server_tool_use","id":"search_1","name":"x_search","input":{}}}),
            },
            SseEvent {
                event: Some("content_block_delta".into()),
                data: json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"query\":\"rust\"}"}}),
            },
            SseEvent {
                event: Some("content_block_stop".into()),
                data: json!({"type":"content_block_stop","index":0}),
            },
            SseEvent {
                event: Some("content_block_start".into()),
                data: json!({"type":"content_block_start","index":1,"content_block":{"type":"x_search_tool_result","tool_use_id":"search_1","content":[]}}),
            },
            SseEvent {
                event: Some("content_block_stop".into()),
                data: json!({"type":"content_block_stop","index":1}),
            },
            SseEvent {
                event: Some("content_block_start".into()),
                data: json!({"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}),
            },
            SseEvent {
                event: Some("content_block_delta".into()),
                data: json!({"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"result"}}),
            },
            SseEvent {
                event: Some("content_block_delta".into()),
                data: json!({"type":"content_block_delta","index":2,"delta":{"type":"citations_delta","citation":{"url":"https://example.com","title":"Example"}}}),
            },
            SseEvent {
                event: Some("content_block_stop".into()),
                data: json!({"type":"content_block_stop","index":2}),
            },
            SseEvent {
                event: Some("message_delta".into()),
                data: json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}),
            },
            SseEvent {
                event: Some("message_stop".into()),
                data: json!({"type":"message_stop"}),
            },
        ];
        let response = buffered_response(
            OpenAiSurface::Responses,
            &events,
            "resp_test",
            "grok-4.5",
            1,
            &OpenAiResponseMetadata::default(),
        )
        .unwrap();
        assert!(
            response["output"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["type"] == "web_search_call")
        );
        let message = response["output"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "message")
            .unwrap();
        assert_eq!(
            message["content"][0]["annotations"][0]["type"],
            "url_citation"
        );
    }

    #[test]
    fn renders_chat_tool_calls_and_usage() {
        let response = buffered_response(
            OpenAiSurface::ChatCompletions,
            &events(),
            "chatcmpl_test",
            "kimi-k2.6",
            1,
            &OpenAiResponseMetadata::default(),
        )
        .unwrap();
        assert_eq!(response["choices"][0]["message"]["content"], "hello");
        assert_eq!(
            response["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            "lookup"
        );
        assert_eq!(response["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(response["usage"]["total_tokens"], 10);
    }

    #[test]
    fn renders_responses_function_items() {
        let response = buffered_response(
            OpenAiSurface::Responses,
            &events(),
            "resp_test",
            "kimi-k2.6",
            1,
            &OpenAiResponseMetadata::default(),
        )
        .unwrap();
        assert_eq!(response["object"], "response");
        assert_eq!(response["output"][0]["type"], "message");
        assert_eq!(response["output"][1]["type"], "function_call");
        assert_eq!(response["usage"]["total_tokens"], 10);
    }
}
