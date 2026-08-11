use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseParseStats {
    pub bytes_read: usize,
    pub chunk_count: usize,
    pub event_count: usize,
}

pub fn parse_sse_events(input: &[u8]) -> Vec<SseEvent> {
    parse_sse_events_with_stats(input).0
}

pub fn parse_sse_events_with_stats(input: &[u8]) -> (Vec<SseEvent>, SseParseStats) {
    let text = String::from_utf8_lossy(input);
    let normalized = normalize_lines(&text);
    let mut events = Vec::new();
    let mut block = String::new();
    let mut bytes_read = 0usize;
    let mut chunk_count = 0usize;

    for segment in normalized.split('\n') {
        chunk_count += 1;
        bytes_read += segment.len() + 1;
        if segment.is_empty() {
            events.extend(parse_block(&block));
            block.clear();
            continue;
        }
        if !block.is_empty() {
            block.push('\n');
        }
        block.push_str(segment);
    }

    if !block.is_empty() {
        events.extend(parse_block(&block));
    }

    let event_count = events.len();
    let payload = SseParseStats {
        bytes_read,
        chunk_count,
        event_count,
    };

    (events, payload)
}

pub fn encode_sse_event(event: Option<&str>, data: &str) -> Vec<u8> {
    let mut out = String::new();
    if let Some(event) = event {
        out.push_str("event: ");
        out.push_str(event);
        out.push('\n');
    }
    for line in data.split('\n') {
        out.push_str("data: ");
        out.push_str(line);
        out.push('\n');
    }
    out.push('\n');
    out.into_bytes()
}

fn normalize_lines(input: &str) -> String {
    let mut normalized = String::new();
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\r' {
            if chars.peek() == Some(&'\n') {
                chars.next();
            }
            normalized.push('\n');
            continue;
        }
        normalized.push(ch);
    }
    normalized
}

fn parse_block(block: &str) -> Vec<SseEvent> {
    let mut event: Option<String> = None;
    let mut data_lines = Vec::new();
    let mut events = Vec::new();
    let mut push_if_relevant = |event: Option<String>, data_lines: &mut Vec<String>| {
        if data_lines.is_empty() {
            return;
        }
        events.push(SseEvent {
            event,
            data: data_lines.join("\n"),
        });
        data_lines.clear();
    };

    for raw in block.lines() {
        if raw.is_empty() || raw.starts_with(':') {
            continue;
        }

        let mut split = raw.splitn(2, ':');
        let key = split.next().unwrap_or_default();
        let value = split
            .next()
            .unwrap_or_default()
            .trim_start_matches(' ')
            .to_string();

        match key {
            "event" => {
                push_if_relevant(event.take(), &mut data_lines);
                event = Some(value);
                continue;
            }
            "data" => data_lines.push(value),
            _ => {}
        }
    }

    push_if_relevant(event.take(), &mut data_lines);

    events
}
