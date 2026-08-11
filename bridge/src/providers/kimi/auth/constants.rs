use crate::config;

pub const CLIENT_ID: &str = "17e5f671-d194-4dfb-9706-5516cb48c098";
pub const KIMI_CLI_VERSION: &str = "1.37.0";
pub const REFRESH_MARGIN_MS: u64 = 5 * 60 * 1000;

pub fn oauth_host() -> String {
    config::kimi_oauth_host()
}

pub fn api_base_url() -> String {
    config::kimi_base_url()
}
