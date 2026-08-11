use crate::anthropic::schema::MessagesRequest;

// Approximate token counter. Kimi's tokenizer isn't replicated here;
// we use a simple monotonic estimator that satisfies Claude Code's
// compaction logic (needs approximate, not exact counts).

pub const IMAGE_TOKEN_ESTIMATE: u64 = 2000;
pub const MESSAGE_OVERHEAD_TOKENS: u64 = 4;

pub fn count_tokens(req: &MessagesRequest) -> u64 {
    let mut total = 0u64;

    // System text
    if let Some(system) = req.extra.get("system") {
        total += count_system_tokens(system);
    }

    // Messages
    for msg in &req.messages {
        total += count_message_tokens(&msg.role, &msg.content);
    }

    // Message overhead
    total += req.messages.len() as u64 * MESSAGE_OVERHEAD_TOKENS;

    // Tools
    if let Some(tools) = req.extra.get("tools").and_then(|v| v.as_array()) {
        total += count_tool_tokens(tools);
    }

    total
}

fn count_system_tokens(system: &serde_json::Value) -> u64 {
    match system {
        serde_json::Value::String(s) => approx_token_count(s),
        serde_json::Value::Array(arr) => {
            let mut total = 0u64;
            for block in arr {
                if let Some(text) = block.get("text").and_then(|v| v.as_str())
                    && !text.starts_with("x-anthropic-billing-header:")
                {
                    total += approx_token_count(text);
                }
            }
            total
        }
        _ => 0,
    }
}

fn count_message_tokens(role: &str, content: &serde_json::Value) -> u64 {
    match content {
        serde_json::Value::String(s) => approx_token_count(s),
        serde_json::Value::Array(arr) => {
            let mut total = 0u64;
            for block in arr {
                total += count_content_block_tokens(role, block);
            }
            total
        }
        _ => 0,
    }
}

fn count_content_block_tokens(_role: &str, block: &serde_json::Value) -> u64 {
    match block.get("type").and_then(|v| v.as_str()) {
        Some("text") => block
            .get("text")
            .and_then(|v| v.as_str())
            .map(approx_token_count)
            .unwrap_or(0),
        Some("image") => IMAGE_TOKEN_ESTIMATE,
        Some("thinking") => block
            .get("thinking")
            .and_then(|v| v.as_str())
            .map(approx_token_count)
            .unwrap_or(0),
        Some("tool_use") => {
            let mut total = 0u64;
            if let Some(name) = block.get("name").and_then(|v| v.as_str()) {
                total += approx_token_count(name);
            }
            if let Some(input) = block.get("input") {
                total += approx_token_count(&serde_json::to_string(input).unwrap_or_default());
            }
            total
        }
        Some("tool_result") => {
            let content = block.get("content");
            let role = "tool";
            count_message_tokens(role, content.unwrap_or(&serde_json::Value::Null))
        }
        _ => 0,
    }
}

fn count_tool_tokens(tools: &[serde_json::Value]) -> u64 {
    let mut total = 0u64;
    for tool in tools {
        if let Some(name) = tool.get("name").and_then(|v| v.as_str()) {
            total += approx_token_count(name);
        }
        if let Some(desc) = tool.get("description").and_then(|v| v.as_str()) {
            total += approx_token_count(desc);
        }
        if let Some(schema) = tool.get("input_schema") {
            total += approx_token_count(&serde_json::to_string(schema).unwrap_or_default());
        }
    }
    total
}

fn approx_token_count(text: &str) -> u64 {
    if text.is_empty() {
        return 0;
    }
    // Simple heuristic: count whitespace-separated groups plus punctuation
    // as approximate tokens. This gives a monotonic estimate that's roughly
    // proportional to actual token counts.
    let mut count = 0u64;
    let mut in_word = false;

    for ch in text.chars() {
        if ch.is_alphanumeric() || ch == '-' || ch == '_' {
            if !in_word {
                count += 1;
                in_word = true;
            }
        } else {
            in_word = false;
            // Count individual punctuation as tokens
            if !ch.is_whitespace() {
                count += 1;
            }
        }
    }

    count.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn token_count_is_positive() {
        let req: MessagesRequest = serde_json::from_value(json!({
            "model": "kimi-for-coding",
            "messages": [{"role": "user", "content": "hello world"}]
        }))
        .unwrap();
        assert!(count_tokens(&req) > 0);
    }

    #[test]
    fn token_count_is_monotonic() {
        let short: MessagesRequest = serde_json::from_value(json!({
            "model": "kimi-for-coding",
            "messages": [{"role": "user", "content": "short"}]
        }))
        .unwrap();
        let long: MessagesRequest = serde_json::from_value(json!({
            "model": "kimi-for-coding",
            "messages": [{"role": "user", "content": "this is a much longer message with many words in it"}]
        }))
        .unwrap();
        assert!(
            count_tokens(&long) >= count_tokens(&short),
            "longer message should have >= tokens"
        );
    }

    #[test]
    fn token_count_includes_overhead() {
        let single: MessagesRequest = serde_json::from_value(json!({
            "model": "kimi-for-coding",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .unwrap();
        let double: MessagesRequest = serde_json::from_value(json!({
            "model": "kimi-for-coding",
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"}
            ]
        }))
        .unwrap();
        // Two messages should have 4 more tokens for overhead
        assert!(
            count_tokens(&double) > count_tokens(&single),
            "more messages should have more tokens"
        );
    }

    #[test]
    fn image_adds_token_estimate() {
        let text_only: MessagesRequest = serde_json::from_value(json!({
            "model": "kimi-for-coding",
            "messages": [{"role": "user", "content": [{"type": "text", "text": "desc"}]}]
        }))
        .unwrap();
        let with_image: MessagesRequest = serde_json::from_value(json!({
            "model": "kimi-for-coding",
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "desc"},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "abc"}}
            ]}]
        }))
        .unwrap();
        assert!(
            count_tokens(&with_image) >= count_tokens(&text_only) + IMAGE_TOKEN_ESTIMATE - 1,
            "image should add ~2000 tokens"
        );
    }

    #[test]
    fn token_count_works_with_tools() {
        let req: MessagesRequest = serde_json::from_value(json!({
            "model": "kimi-for-coding",
            "messages": [{"role": "user", "content": "use a tool"}],
            "tools": [{"name": "search", "description": "Search tool", "input_schema": {"type": "object"}}]
        }))
        .unwrap();
        assert!(
            count_tokens(&req) > 0,
            "request with tools should have positive count"
        );
    }
}
