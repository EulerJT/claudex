use serde::Deserialize;

use super::constants::{CLIENT_ID, oauth_host};
use super::headers::common_headers;

#[derive(Debug, Clone, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub expires_in: Option<u64>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub token_type: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct DeviceAuthResponse {
    user_code: String,
    device_code: String,
    #[serde(default)]
    verification_uri: Option<String>,
    verification_uri_complete: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    interval: Option<u64>,
}

const GRANT_DEVICE_CODE: &str = "urn:ietf:params:oauth:grant-type:device_code";
const POLL_SAFETY_MARGIN_MS: u64 = 500;

pub fn run_device_login() -> Result<TokenResponse, anyhow::Error> {
    let headers = common_headers()?;
    let client = reqwest::blocking::Client::new();

    let init_resp = client
        .post(format!("{}/api/oauth/device_authorization", oauth_host()))
        .headers(build_header_map(&headers))
        .form(&[("client_id", CLIENT_ID)])
        .send()?;

    if !init_resp.status().is_success() {
        let status = init_resp.status();
        let text = init_resp.text().unwrap_or_default();
        anyhow::bail!("Device authorization failed: {status} {text}");
    }

    let auth: DeviceAuthResponse = init_resp.json()?;
    let interval_ms = (auth.interval.unwrap_or(5).max(1)) * 1000;

    eprintln!();
    eprintln!("Visit: {}", auth.verification_uri_complete);
    eprintln!("Code:  {}", auth.user_code);
    eprintln!();

    loop {
        let resp = client
            .post(format!("{}/api/oauth/token", oauth_host()))
            .headers(build_header_map(&headers))
            .form(&[
                ("client_id", CLIENT_ID),
                ("device_code", &auth.device_code),
                ("grant_type", GRANT_DEVICE_CODE),
            ])
            .send()?;

        let status = resp.status();
        if status.as_u16() == 200 {
            return Ok(resp.json::<TokenResponse>()?);
        }

        let body: serde_json::Value = resp.json().unwrap_or(serde_json::json!({}));
        let error = body
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        match error {
            "expired_token" => {
                anyhow::bail!("Device code expired. Run login again.");
            }
            "authorization_pending" | "slow_down" => {
                std::thread::sleep(std::time::Duration::from_millis(
                    interval_ms + POLL_SAFETY_MARGIN_MS,
                ));
                continue;
            }
            _ => {
                let desc = body
                    .get("error_description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                anyhow::bail!(
                    "Device token poll failed ({}): {}{}",
                    status,
                    error,
                    if desc.is_empty() {
                        "".to_string()
                    } else {
                        format!(" - {desc}")
                    }
                );
            }
        }
    }
}

fn build_header_map(
    headers: &std::collections::HashMap<String, String>,
) -> reqwest::header::HeaderMap {
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
    let mut map = HeaderMap::new();
    for (k, v) in headers {
        if let Ok(name) = HeaderName::from_bytes(k.as_bytes())
            && let Ok(value) = HeaderValue::from_str(v)
        {
            map.insert(name, value);
        }
    }
    map
}
