use serde_json::Value;

use super::reducer::{
    AnthropicUsage, KimiUsage, ReducerEvent, StopReason, map_usage_to_anthropic,
    reduce_upstream_bytes,
};
use super::signature::make_thinking_signature;

pub fn accumulate_response(
    input: &[u8],
    message_id: &str,
    model: &str,
) -> Result<Value, anyhow::Error> {
    let events = reduce_upstream_bytes(input)
        .map_err(|e| anyhow::anyhow!("upstream error: {} ({:?})", e.message, e.kind))?;

    let mut content: Vec<Value> = Vec::new();
    let mut stop_reason: Option<StopReason> = None;
    let mut usage: Option<AnthropicUsage> = None;
    let mut _raw_usage: Option<KimiUsage> = None;

    struct AccumulatedBlock {
        index: usize,
        kind: BlockKind,
    }

    enum BlockKind {
        Thinking {
            text: String,
        },
        Text {
            text: String,
        },
        Tool {
            id: String,
            name: String,
            args: String,
        },
    }

    let mut blocks: Vec<AccumulatedBlock> = Vec::new();

    for event in &events {
        match event {
            ReducerEvent::ThinkingStart { index } => {
                blocks.push(AccumulatedBlock {
                    index: *index,
                    kind: BlockKind::Thinking {
                        text: String::new(),
                    },
                });
            }
            ReducerEvent::ThinkingDelta { index, text } => {
                if let Some(block) = blocks.iter_mut().rev().find(|b| b.index == *index)
                    && let BlockKind::Thinking { text: t } = &mut block.kind
                {
                    t.push_str(text);
                }
            }
            ReducerEvent::TextStart { index } => {
                blocks.push(AccumulatedBlock {
                    index: *index,
                    kind: BlockKind::Text {
                        text: String::new(),
                    },
                });
            }
            ReducerEvent::TextDelta { index, text } => {
                if let Some(block) = blocks.iter_mut().rev().find(|b| b.index == *index)
                    && let BlockKind::Text { text: t } = &mut block.kind
                {
                    t.push_str(text);
                }
            }
            ReducerEvent::ToolStart { index, id, name } => {
                blocks.push(AccumulatedBlock {
                    index: *index,
                    kind: BlockKind::Tool {
                        id: id.clone(),
                        name: name.clone(),
                        args: String::new(),
                    },
                });
            }
            ReducerEvent::ToolDelta {
                index,
                partial_json,
            } => {
                if let Some(block) = blocks.iter_mut().rev().find(|b| b.index == *index)
                    && let BlockKind::Tool { args, .. } = &mut block.kind
                {
                    args.push_str(partial_json);
                }
            }
            ReducerEvent::Finish {
                stop_reason: sr,
                usage: u,
            } => {
                stop_reason = Some(sr.clone());
                _raw_usage = u.clone();
                usage = Some(map_usage_to_anthropic(u));
            }
            _ => {}
        }
    }

    // Build content array from blocks
    for block in &blocks {
        match &block.kind {
            BlockKind::Thinking { text } => {
                if !text.is_empty() {
                    content.push(serde_json::json!({
                        "type": "thinking",
                        "thinking": text,
                        "signature": make_thinking_signature(message_id, block.index),
                    }));
                }
            }
            BlockKind::Text { text } => {
                if !text.is_empty() {
                    content.push(serde_json::json!({
                        "type": "text",
                        "text": text,
                    }));
                }
            }
            BlockKind::Tool { id, name, args } => {
                let parsed = serde_json::from_str::<Value>(args)
                    .unwrap_or_else(|_| Value::String(args.clone()));
                content.push(serde_json::json!({
                    "type": "tool_use",
                    "id": id,
                    "name": name,
                    "input": parsed,
                }));
            }
        }
    }

    let response = serde_json::json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": stop_reason.map(|s| match s {
            StopReason::EndTurn => "end_turn",
            StopReason::ToolUse => "tool_use",
            StopReason::MaxTokens => "max_tokens",
        }),
        "stop_sequence": null,
        "usage": usage.unwrap_or_default(),
    });

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_text_upstream() -> Vec<u8> {
        concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n",
            "data: {\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
            "data: [DONE]\n\n"
        )
        .as_bytes()
        .to_vec()
    }

    #[test]
    fn accumulate_outputs_anthropic_message() {
        let response =
            accumulate_response(&simple_text_upstream(), "msg_test", "kimi-for-coding").unwrap();
        assert_eq!(response["type"], "message");
        assert_eq!(response["content"][0]["type"], "text");
        assert_eq!(response["content"][0]["text"], "Hello world");
        assert_eq!(response["stop_reason"], "end_turn");
    }

    #[test]
    fn accumulate_with_reasoning_and_tools() {
        let upstream = concat!(
            "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"result\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"search\",\"arguments\":\"{\\\"q\\\":\\\"rust\\\"}\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n",
            "data: [DONE]\n\n"
        );
        let response =
            accumulate_response(upstream.as_bytes(), "msg_1", "kimi-for-coding").unwrap();
        assert_eq!(response["type"], "message");
        let content = response["content"].as_array().unwrap();
        assert!(content.len() >= 3);
        // First block should be thinking
        assert_eq!(content[0]["type"], "thinking");
        // Second block should be text
        assert_eq!(content[1]["type"], "text");
        // Third block should be tool_use
        assert_eq!(content[2]["type"], "tool_use");
    }

    #[test]
    fn accumulate_handles_upstream_error() {
        let upstream = "data: {\"error\":{\"message\":\"upstream failure\"}}\n\n";
        let result = accumulate_response(upstream.as_bytes(), "msg_e", "model");
        assert!(result.is_err());
    }
}
