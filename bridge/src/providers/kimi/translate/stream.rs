use super::reducer::{map_usage_to_anthropic, reduce_upstream_bytes};
use super::signature::make_thinking_signature;
use crate::anthropic::sse::encode_sse_event;

pub fn translate_stream_bytes(
    input: &[u8],
    message_id: &str,
    model: &str,
) -> Result<Vec<u8>, anyhow::Error> {
    let events = reduce_upstream_bytes(input)
        .map_err(|e| anyhow::anyhow!("upstream stream error: {} ({:?})", e.message, e.kind))?;

    let mut out = Vec::new();
    let mut message_started = false;
    let mut active_tools: Vec<(usize, String, String)> = Vec::new();

    let mut emit = |event: Option<&str>, data: &str| {
        out.extend_from_slice(&encode_sse_event(event, data));
    };

    for event in &events {
        match event {
            super::reducer::ReducerEvent::ThinkingStart { index } => {
                if !message_started {
                    let data = serde_json::json!({
                        "type": "message_start",
                        "message": {
                            "id": message_id,
                            "type": "message",
                            "role": "assistant",
                            "model": model,
                            "content": [],
                            "stop_reason": null,
                            "stop_sequence": null,
                            "usage": {
                                "input_tokens": 0,
                                "output_tokens": 0,
                            }
                        }
                    });
                    emit(Some("message_start"), &data.to_string());
                    message_started = true;
                }
                let data = serde_json::json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {
                        "type": "thinking",
                        "thinking": ""
                    }
                });
                emit(Some("content_block_start"), &data.to_string());
            }
            super::reducer::ReducerEvent::ThinkingDelta { index, text } => {
                // Ensure message start
                if !message_started {
                    let data = serde_json::json!({
                        "type": "message_start",
                        "message": {
                            "id": message_id,
                            "type": "message",
                            "role": "assistant",
                            "model": model,
                            "content": [],
                            "stop_reason": null,
                            "stop_sequence": null,
                            "usage": {
                                "input_tokens": 0,
                                "output_tokens": 0,
                            }
                        }
                    });
                    emit(Some("message_start"), &data.to_string());
                    message_started = true;
                }
                let data = serde_json::json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {
                        "type": "thinking_delta",
                        "thinking": text
                    }
                });
                emit(Some("content_block_delta"), &data.to_string());
            }
            super::reducer::ReducerEvent::ThinkingStop { index } => {
                let data = serde_json::json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {
                        "type": "signature_delta",
                        "signature": make_thinking_signature(message_id, *index)
                    }
                });
                emit(Some("content_block_delta"), &data.to_string());
                let data = serde_json::json!({
                    "type": "content_block_stop",
                    "index": index,
                });
                emit(Some("content_block_stop"), &data.to_string());
            }
            super::reducer::ReducerEvent::TextStart { index } => {
                if !message_started {
                    let data = serde_json::json!({
                        "type": "message_start",
                        "message": {
                            "id": message_id,
                            "type": "message",
                            "role": "assistant",
                            "model": model,
                            "content": [],
                            "stop_reason": null,
                            "stop_sequence": null,
                            "usage": {
                                "input_tokens": 0,
                                "output_tokens": 0,
                            }
                        }
                    });
                    emit(Some("message_start"), &data.to_string());
                    message_started = true;
                }
                let data = serde_json::json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {
                        "type": "text",
                        "text": ""
                    }
                });
                emit(Some("content_block_start"), &data.to_string());
            }
            super::reducer::ReducerEvent::TextDelta { index, text } => {
                if !message_started {
                    let data = serde_json::json!({
                        "type": "message_start",
                        "message": {
                            "id": message_id,
                            "type": "message",
                            "role": "assistant",
                            "model": model,
                            "content": [],
                            "stop_reason": null,
                            "stop_sequence": null,
                            "usage": {
                                "input_tokens": 0,
                                "output_tokens": 0,
                            }
                        }
                    });
                    emit(Some("message_start"), &data.to_string());
                    message_started = true;
                }
                let data = serde_json::json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {
                        "type": "text_delta",
                        "text": text
                    }
                });
                emit(Some("content_block_delta"), &data.to_string());
            }
            super::reducer::ReducerEvent::TextStop { index } => {
                let data = serde_json::json!({
                    "type": "content_block_stop",
                    "index": index,
                });
                emit(Some("content_block_stop"), &data.to_string());
            }
            super::reducer::ReducerEvent::ToolStart { index, id, name } => {
                if !message_started {
                    let data = serde_json::json!({
                        "type": "message_start",
                        "message": {
                            "id": message_id,
                            "type": "message",
                            "role": "assistant",
                            "model": model,
                            "content": [],
                            "stop_reason": null,
                            "stop_sequence": null,
                            "usage": {
                                "input_tokens": 0,
                                "output_tokens": 0,
                            }
                        }
                    });
                    emit(Some("message_start"), &data.to_string());
                    message_started = true;
                }
                active_tools.push((*index, id.clone(), name.clone()));
                let data = serde_json::json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {
                        "type": "tool_use",
                        "id": id,
                        "name": name,
                        "input": {}
                    }
                });
                emit(Some("content_block_start"), &data.to_string());
            }
            super::reducer::ReducerEvent::ToolDelta {
                index,
                partial_json,
            } => {
                if !message_started {
                    let data = serde_json::json!({
                        "type": "message_start",
                        "message": {
                            "id": message_id,
                            "type": "message",
                            "role": "assistant",
                            "model": model,
                            "content": [],
                            "stop_reason": null,
                            "stop_sequence": null,
                            "usage": {
                                "input_tokens": 0,
                                "output_tokens": 0,
                            }
                        }
                    });
                    emit(Some("message_start"), &data.to_string());
                    message_started = true;
                }
                let data = serde_json::json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": partial_json
                    }
                });
                emit(Some("content_block_delta"), &data.to_string());
            }
            super::reducer::ReducerEvent::ToolStop { index } => {
                active_tools.retain(|(i, _, _)| *i != *index);
                let data = serde_json::json!({
                    "type": "content_block_stop",
                    "index": index,
                });
                emit(Some("content_block_stop"), &data.to_string());
            }
            super::reducer::ReducerEvent::Finish { stop_reason, usage } => {
                if !message_started {
                    let data = serde_json::json!({
                        "type": "message_start",
                        "message": {
                            "id": message_id,
                            "type": "message",
                            "role": "assistant",
                            "model": model,
                            "content": [],
                            "stop_reason": null,
                            "stop_sequence": null,
                            "usage": {
                                "input_tokens": 0,
                                "output_tokens": 0,
                            }
                        }
                    });
                    emit(Some("message_start"), &data.to_string());
                    message_started = true;
                }
                let sr = match stop_reason {
                    super::reducer::StopReason::EndTurn => "end_turn",
                    super::reducer::StopReason::ToolUse => "tool_use",
                    super::reducer::StopReason::MaxTokens => "max_tokens",
                };
                let mapped = map_usage_to_anthropic(usage);
                let data = serde_json::json!({
                    "type": "message_delta",
                    "delta": {
                        "stop_reason": sr,
                        "stop_sequence": null
                    },
                    "usage": mapped
                });
                emit(Some("message_delta"), &data.to_string());
                let data = serde_json::json!({
                    "type": "message_stop"
                });
                emit(Some("message_stop"), &data.to_string());
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_translates_simple_text() {
        let upstream = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n",
            "data: {\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":1}}\n\n",
            "data: [DONE]\n\n"
        );
        let result =
            translate_stream_bytes(upstream.as_bytes(), "msg_1", "kimi-for-coding").unwrap();
        let output = String::from_utf8_lossy(&result);

        // Should contain message_start, content_block_start, content_block_delta,
        // content_block_stop, message_delta, message_stop
        assert!(output.contains("message_start"), "missing message_start");
        assert!(
            output.contains("content_block_start"),
            "missing content_block_start"
        );
        assert!(output.contains("text_delta"), "missing text_delta");
        assert!(
            output.contains("content_block_stop"),
            "missing content_block_stop"
        );
        assert!(output.contains("message_delta"), "missing message_delta");
        assert!(output.contains("message_stop"), "missing message_stop");
        assert!(output.contains("end_turn"), "missing end_turn");
    }

    #[test]
    fn stream_translates_reasoning_content() {
        let upstream = concat!(
            "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n",
            "data: {\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n",
            "data: [DONE]\n\n"
        );
        let result = translate_stream_bytes(upstream.as_bytes(), "msg_2", "model").unwrap();
        let output = String::from_utf8_lossy(&result);

        assert!(output.contains("thinking_delta"), "missing thinking_delta");
        assert!(
            output.contains("signature_delta"),
            "missing signature_delta"
        );
        assert!(output.contains("text_delta"), "missing text_delta");
    }

    #[test]
    fn stream_translates_tool_calls() {
        let upstream = concat!(
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"search\",\"arguments\":\"{\\\"q\\\":\\\"rust\\\"}\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n"
        );
        let result = translate_stream_bytes(upstream.as_bytes(), "msg_3", "model").unwrap();
        let output = String::from_utf8_lossy(&result);

        assert!(output.contains("tool_use"), "missing tool_use");
        assert!(
            output.contains("input_json_delta"),
            "missing input_json_delta"
        );
        assert!(
            output.contains("tool_use"),
            "missing tool_use in stop_reason"
        );
    }

    #[test]
    fn stream_emits_message_start_before_content_blocks() {
        let upstream = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"direct\"}}]}\n\n",
            "data: {\"choices\":[{\"finish_reason\":\"stop\"}]}\n\n",
        );
        let result = translate_stream_bytes(upstream.as_bytes(), "msg_4", "model").unwrap();
        let output = String::from_utf8_lossy(&result);

        // message_start should come before content_block_start
        let msg_start_pos = output.find("message_start").unwrap();
        let cb_start_pos = output.find("content_block_start").unwrap();
        assert!(
            msg_start_pos < cb_start_pos,
            "message_start should be before content_block_start"
        );
    }
}
