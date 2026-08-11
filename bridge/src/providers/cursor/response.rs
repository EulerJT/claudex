use crate::anthropic::schema::MessagesRequest;
use crate::providers::cursor::client::{
    CursorUpstreamResponse, decode_frame_payload, decode_upstream_frames,
};
use crate::providers::cursor::connect::{ConnectEndError, FLAG_END, parse_connect_error};
use crate::providers::cursor::proto::AgentServerMessage;

/// A decoded event from the Cursor upstream response stream.
#[derive(Debug, Clone)]
pub enum CursorStreamEvent {
    Session {
        session_id: String,
    },
    ThinkingDelta {
        text: String,
    },
    TextDelta {
        text: String,
    },
    Usage {
        input_tokens: u64,
        output_tokens: u64,
        cache_read_tokens: u64,
        cache_write_tokens: u64,
    },
    End,
}

#[derive(Debug, Clone)]
pub enum CursorDecodeError {
    ConnectEnd(ConnectEndError),
    Decode(String),
}

impl CursorDecodeError {
    pub fn status(&self) -> Option<u16> {
        match self {
            CursorDecodeError::ConnectEnd(err) => Some(err.status),
            CursorDecodeError::Decode(_) => None,
        }
    }
}

impl std::fmt::Display for CursorDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CursorDecodeError::ConnectEnd(err) => write!(f, "{err}"),
            CursorDecodeError::Decode(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for CursorDecodeError {}

/// Decode upstream response bytes into a sequence of CursorStreamEvents.
///
/// Returns both the events and the final usage for the response, since the
/// upstream may send multiple update frames.
pub fn decode_upstream_response(body: &[u8]) -> Result<Vec<CursorStreamEvent>, CursorDecodeError> {
    let frames =
        decode_upstream_frames(body).map_err(|e| CursorDecodeError::Decode(e.to_string()))?;
    let mut events = Vec::new();

    for frame in &frames {
        if frame.flags & FLAG_END != 0 {
            // Check for Connect error in end frame
            if !frame.payload.is_empty()
                && let Some(err) = parse_connect_error(&frame.payload)
            {
                return Err(CursorDecodeError::ConnectEnd(err));
            }
            events.push(CursorStreamEvent::End);
            continue;
        }

        let decompressed;
        let payload = if frame.flags & crate::providers::cursor::connect::FLAG_GZIP != 0 {
            decompressed = crate::providers::cursor::connect::decode_gzip_frame(&frame.payload)
                .map_err(|error| CursorDecodeError::Decode(format!("gzip decompress: {error}")))?;
            &decompressed[..]
        } else {
            &frame.payload[..]
        };
        if events_from_current_payload(payload, &mut events) {
            continue;
        }

        let msg = match decode_frame_payload(frame) {
            Ok(message) => message,
            Err(_) => continue,
        };

        events_from_message(&msg, &mut events);
    }

    Ok(events)
}

/// Build an accumulated Anthropic response JSON from upstream bytes for
/// non-streaming mode.
pub fn decode_cursor_upstream(
    upstream: &CursorUpstreamResponse,
    message_id: &str,
    model: &str,
) -> Result<serde_json::Value, CursorDecodeError> {
    let events = decode_upstream_response(&upstream.body)?;

    let mut text_content = String::new();
    let mut final_input_tokens: u64 = 0;
    let mut final_output_tokens: u64 = 0;

    for event in &events {
        match event {
            CursorStreamEvent::TextDelta { text } => text_content.push_str(text),
            CursorStreamEvent::Usage {
                input_tokens,
                output_tokens,
                ..
            } => {
                final_input_tokens = *input_tokens;
                final_output_tokens = *output_tokens;
            }
            CursorStreamEvent::End => break,
            _ => {}
        }
    }

    let input_tokens = final_input_tokens.max(estimate_input_tokens(&text_content));

    Ok(serde_json::json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "content": [
            {"type": "text", "text": text_content}
        ],
        "model": model,
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": final_output_tokens,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0
        }
    }))
}

fn estimate_input_tokens(_content: &str) -> u64 {
    // Rough upper bound: 4 chars per token for input estimation
    (_content.len() / 4) as u64
}

struct ProtoField<'a> {
    number: u64,
    wire_type: u8,
    data: &'a [u8],
    value: u64,
}

fn read_varint(bytes: &[u8]) -> Option<(u64, &[u8])> {
    let mut value = 0u64;
    let mut shift = 0;
    for (index, byte) in bytes.iter().copied().enumerate() {
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some((value, &bytes[index + 1..]));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

fn proto_fields(mut bytes: &[u8]) -> impl Iterator<Item = ProtoField<'_>> {
    std::iter::from_fn(move || {
        let (tag, rest) = read_varint(bytes)?;
        bytes = rest;
        let number = tag >> 3;
        let wire_type = (tag & 7) as u8;
        match wire_type {
            0 => {
                let (value, rest) = read_varint(bytes)?;
                bytes = rest;
                Some(ProtoField {
                    number,
                    wire_type,
                    data: &[],
                    value,
                })
            }
            1 if bytes.len() >= 8 => {
                bytes = &bytes[8..];
                Some(ProtoField {
                    number,
                    wire_type,
                    data: &[],
                    value: 0,
                })
            }
            2 => {
                let (length, rest) = read_varint(bytes)?;
                let length = usize::try_from(length).ok()?;
                if rest.len() < length {
                    return None;
                }
                let data = &rest[..length];
                bytes = &rest[length..];
                Some(ProtoField {
                    number,
                    wire_type,
                    data,
                    value: 0,
                })
            }
            5 if bytes.len() >= 4 => {
                bytes = &bytes[4..];
                Some(ProtoField {
                    number,
                    wire_type,
                    data: &[],
                    value: 0,
                })
            }
            _ => None,
        }
    })
}

fn nested_text(payload: &[u8], update_type: u64) -> Option<String> {
    let interaction =
        proto_fields(payload).find(|field| field.number == 1 && field.wire_type == 2)?;
    let update = proto_fields(interaction.data)
        .find(|field| field.number == update_type && field.wire_type == 2)?;
    let text = proto_fields(update.data).find(|field| field.number == 1 && field.wire_type == 2)?;
    let text = std::str::from_utf8(text.data).ok()?;
    (!text.is_empty()).then(|| text.to_string())
}

fn current_usage(payload: &[u8]) -> Option<(u64, u64, u64, u64)> {
    let interaction =
        proto_fields(payload).find(|field| field.number == 1 && field.wire_type == 2)?;
    let ended =
        proto_fields(interaction.data).find(|field| field.number == 14 && field.wire_type == 2)?;
    let mut usage = [0; 4];
    for field in proto_fields(ended.data) {
        if field.wire_type == 0 && (1..=4).contains(&field.number) {
            usage[field.number as usize - 1] = field.value;
        }
    }
    Some((usage[0], usage[1], usage[2], usage[3]))
}

fn events_from_current_payload(payload: &[u8], events: &mut Vec<CursorStreamEvent>) -> bool {
    let mut decoded = false;
    if let Some(text) = nested_text(payload, 4) {
        events.push(CursorStreamEvent::ThinkingDelta { text });
        decoded = true;
    }
    if let Some(text) = nested_text(payload, 1) {
        events.push(CursorStreamEvent::TextDelta { text });
        decoded = true;
    }
    if let Some((input_tokens, output_tokens, cache_read_tokens, cache_write_tokens)) =
        current_usage(payload)
    {
        events.push(CursorStreamEvent::Usage {
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
        });
        events.push(CursorStreamEvent::End);
        decoded = true;
    }
    decoded
}

fn events_from_message(msg: &AgentServerMessage, events: &mut Vec<CursorStreamEvent>) {
    // Check for exec_server_message with session info
    if let Some(ref exec) = msg.exec_server_message
        && let Some(ref session_id) = exec.notes_session_id
        && !session_id.is_empty()
    {
        events.push(CursorStreamEvent::Session {
            session_id: session_id.clone(),
        });
    }

    if let Some(ref update) = msg.interaction_update {
        // Thinking delta
        if let Some(ref td) = update.thinking_delta
            && !td.text.is_empty()
        {
            events.push(CursorStreamEvent::ThinkingDelta {
                text: td.text.clone(),
            });
        }

        // Text delta
        if let Some(ref td) = update.text_delta
            && !td.text.is_empty()
        {
            events.push(CursorStreamEvent::TextDelta {
                text: td.text.clone(),
            });
        }

        // Turn ended (usage + end)
        if let Some(ref te) = update.turn_ended {
            events.push(CursorStreamEvent::Usage {
                input_tokens: te.input_tokens,
                output_tokens: te.output_tokens,
                cache_read_tokens: te.cache_read_tokens,
                cache_write_tokens: te.cache_write_tokens,
            });
            events.push(CursorStreamEvent::End);
        }
    }
}

/// Extract an estimate of input tokens from a MessagesRequest for usage
/// reporting. This is a rough heuristic based on JSON string length.
pub fn estimate_request_input_tokens(req: &MessagesRequest) -> u64 {
    let prompt = super::request::render_cursor_prompt(req);
    (prompt.len() / 4).max(1) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cursor::connect::encode_connect_frame;
    use crate::providers::cursor::proto::*;
    use crate::providers::cursor::test_frames;
    use prost::Message;

    #[test]
    fn decodes_text_and_usage_events() {
        let mut body = Vec::new();
        body.extend_from_slice(&test_frames::text_frame("Hello"));
        body.extend_from_slice(&test_frames::text_frame(" world"));
        body.extend_from_slice(&test_frames::usage_frame(10, 5));
        body.extend_from_slice(&test_frames::end_frame());

        let events = decode_upstream_response(&body).unwrap();
        assert_eq!(events.len(), 5);
        assert!(matches!(events[0], CursorStreamEvent::TextDelta { .. }));
        assert!(matches!(events[1], CursorStreamEvent::TextDelta { .. }));
        assert!(matches!(events[2], CursorStreamEvent::Usage { .. }));
        assert!(matches!(events[3], CursorStreamEvent::End));
        assert!(matches!(events[4], CursorStreamEvent::End));
    }

    #[test]
    fn decodes_thinking_delta() {
        let body = test_frames::thinking_frame("thinking...");

        let events = decode_upstream_response(&body).unwrap();
        assert_eq!(events.len(), 1);
        if let CursorStreamEvent::ThinkingDelta { text } = &events[0] {
            assert_eq!(text, "thinking...");
        } else {
            panic!("expected ThinkingDelta");
        }
    }

    #[test]
    fn decodes_session_event() {
        let msg = AgentServerMessage {
            interaction_update: None,
            exec_server_message: Some(ExecServerMessage {
                notes_session_id: Some("session-123".to_string()),
            }),
        };
        let mut payload = Vec::new();
        msg.encode(&mut payload).unwrap();
        let body = encode_connect_frame(&payload, 0).to_vec();

        let events = decode_upstream_response(&body).unwrap();
        assert_eq!(events.len(), 1);
        if let CursorStreamEvent::Session { session_id } = &events[0] {
            assert_eq!(session_id, "session-123");
        } else {
            panic!("expected Session");
        }
    }

    #[test]
    fn accumulate_response_produces_anthropic_json() {
        let mut body = Vec::new();
        body.extend_from_slice(&test_frames::text_frame("Hello world"));
        body.extend_from_slice(&test_frames::usage_frame(15, 3));
        body.extend_from_slice(&test_frames::end_frame());

        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let json = decode_cursor_upstream(&upstream, "msg_test", "cursor-test").unwrap();
        assert_eq!(json["id"], "msg_test");
        assert_eq!(json["content"][0]["text"], "Hello world");
        assert_eq!(json["usage"]["input_tokens"].as_u64(), Some(15));
        assert_eq!(json["usage"]["output_tokens"].as_u64(), Some(3));
        assert_eq!(
            json["usage"]["cache_creation_input_tokens"].as_u64(),
            Some(0)
        );
        assert_eq!(json["usage"]["cache_read_input_tokens"].as_u64(), Some(0));
        assert_eq!(json["stop_reason"], "end_turn");
    }

    #[test]
    fn empty_upstream_produces_empty_response() {
        let upstream = CursorUpstreamResponse {
            status: 200,
            body: Vec::new(),
            error_detail: None,
        };
        let json = decode_cursor_upstream(&upstream, "msg_empty", "cursor-test").unwrap();
        assert_eq!(json["content"][0]["text"], "");
    }

    #[test]
    fn connect_end_frame_with_error_is_rejected() {
        let json_err = serde_json::json!({
            "error": {"code": "resource_exhausted", "message": "quota exceeded"}
        });
        let payload = serde_json::to_vec(&json_err).unwrap();
        let frame = encode_connect_frame(&payload, FLAG_END);
        let result = decode_upstream_response(&frame);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), Some(429));
        assert!(err.to_string().contains("quota exceeded"));
    }

    #[test]
    fn multiple_text_deltas_accumulate() {
        let mut body = Vec::new();
        body.extend_from_slice(&test_frames::text_frame("Hello "));
        body.extend_from_slice(&test_frames::text_frame("world"));
        body.extend_from_slice(&test_frames::usage_frame(10, 2));
        body.extend_from_slice(&test_frames::end_frame());

        let events = decode_upstream_response(&body).unwrap();
        let text: String = events
            .iter()
            .filter_map(|e| {
                if let CursorStreamEvent::TextDelta { text } = e {
                    Some(text.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(text, "Hello world");
    }
}
