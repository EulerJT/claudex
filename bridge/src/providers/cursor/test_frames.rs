use super::connect::encode_connect_frame;

fn varint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while value >= 0x80 {
        out.push(((value as u8) & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
    out
}

fn bytes_field(field: u64, value: &[u8]) -> Vec<u8> {
    let mut out = varint((field << 3) | 2);
    out.extend(varint(value.len() as u64));
    out.extend(value);
    out
}

fn varint_field(field: u64, value: u64) -> Vec<u8> {
    let mut out = varint(field << 3);
    out.extend(varint(value));
    out
}

fn interaction_frame(update_field: u64, payload: &[u8]) -> Vec<u8> {
    let update = bytes_field(update_field, payload);
    encode_connect_frame(bytes_field(1, &update), 0).to_vec()
}

pub(crate) fn text_frame(text: &str) -> Vec<u8> {
    interaction_frame(1, &bytes_field(1, text.as_bytes()))
}

pub(crate) fn thinking_frame(text: &str) -> Vec<u8> {
    interaction_frame(4, &bytes_field(1, text.as_bytes()))
}

pub(crate) fn usage_frame(input: u64, output: u64) -> Vec<u8> {
    usage_frame_full(input, output, 0, 0)
}

pub(crate) fn usage_frame_full(
    input: u64,
    output: u64,
    cache_read: u64,
    cache_write: u64,
) -> Vec<u8> {
    let mut usage = varint_field(1, input);
    usage.extend(varint_field(2, output));
    usage.extend(varint_field(3, cache_read));
    usage.extend(varint_field(4, cache_write));
    interaction_frame(14, &usage)
}

pub(crate) fn end_frame() -> Vec<u8> {
    encode_connect_frame(b"", 2).to_vec()
}
