pub fn make_thinking_signature(message_id: &str, index: usize) -> String {
    use base64::Engine;
    let input = format!("ccp:kimi:v1:{message_id}:{index}");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(input.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_is_reference_base64url() {
        assert_eq!(
            make_thinking_signature("msg_1", 2),
            "Y2NwOmtpbWk6djE6bXNnXzE6Mg"
        );
    }
}
