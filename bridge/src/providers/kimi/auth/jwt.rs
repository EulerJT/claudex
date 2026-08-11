use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct KimiClaims {
    #[serde(alias = "user_id")]
    user_id: Option<String>,
}

pub fn extract_user_id(access_token: &str) -> Option<String> {
    let parts: Vec<&str> = access_token.split('.').collect();
    if parts.len() < 2 {
        return None;
    }

    let payload_b64 = parts[1].replace('-', "+").replace('_', "/");

    let padded = match payload_b64.len() % 4 {
        2 => format!("{}==", payload_b64),
        3 => format!("{}=", payload_b64),
        _ => payload_b64,
    };

    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&padded)
        .ok()?;

    let claims: KimiClaims = serde_json::from_slice(&decoded).ok()?;
    claims.user_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_user_id_from_jwt() {
        // JWT with header {"alg":"HS256"}, payload {"user_id":"test_user","exp":9999999999}
        let token =
            "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoidGVzdF91c2VyIiwiZXhwIjo5OTk5OTk5OTk5fQ.signature";
        assert_eq!(extract_user_id(token), Some("test_user".to_string()));
    }

    #[test]
    fn extract_user_id_invalid_token() {
        assert_eq!(extract_user_id("invalid"), None);
    }

    #[test]
    fn extract_user_id_from_empty_parts() {
        assert_eq!(extract_user_id(""), None);
    }
}
