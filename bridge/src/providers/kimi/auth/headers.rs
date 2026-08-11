use std::collections::HashMap;

use super::constants::KIMI_CLI_VERSION;
use super::device_id::get_device_id;
use crate::config;

fn device_model() -> String {
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;
    format!("{} {}", os, arch)
}

fn ascii_only(value: &str, fallback: &str) -> String {
    let cleaned: String = value
        .chars()
        .filter(|&c| c.is_ascii() && !c.is_control())
        .collect();
    let trimmed = cleaned.trim().to_string();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed
    }
}

pub fn common_headers() -> Result<HashMap<String, String>, anyhow::Error> {
    let device_id = get_device_id()?;
    let hostname_str = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    let mut headers = HashMap::new();
    headers.insert("X-Msh-Platform".to_string(), "kimi_cli".to_string());
    headers.insert("X-Msh-Version".to_string(), KIMI_CLI_VERSION.to_string());
    headers.insert(
        "X-Msh-Device-Name".to_string(),
        ascii_only(&hostname_str, "unknown"),
    );
    headers.insert(
        "X-Msh-Device-Model".to_string(),
        ascii_only(&device_model(), "unknown"),
    );
    headers.insert(
        "X-Msh-Os-Version".to_string(),
        ascii_only(std::env::consts::ARCH, "unknown"),
    );
    headers.insert("X-Msh-Device-Id".to_string(), device_id);
    headers.insert(
        "User-Agent".to_string(),
        config::kimi_user_agent(&format!("KimiCLI/{KIMI_CLI_VERSION}")),
    );
    Ok(headers)
}
