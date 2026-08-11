use std::sync::Arc;
use std::sync::LazyLock;
#[cfg(test)]
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, fs, path::Path};
use tokio::sync::Mutex as AsyncMutex;

use super::constants::{CLIENT_ID, ISSUER, REFRESH_MARGIN_MS};
use super::jwt::{TokenResponse, extract_account_id, validate_token_response};
use super::token_store::{CodexTokenStore, StoredAuth};
use crate::auth::AuthStorage;

static CODEX_REFRESH_LOCK: LazyLock<Arc<AsyncMutex<()>>> =
    LazyLock::new(|| Arc::new(AsyncMutex::new(())));

const CODEX_AUTH_MODE_ENV: &str = "CCP_CODEX_AUTH_MODE";
const BEARER_TOKEN_FILE_ENV: &str = "CCP_CODEX_BEARER_TOKEN_FILE";
const MAX_BEARER_TOKEN_FILE_BYTES: u64 = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodexAuthMode {
    OAuth,
    BearerFile,
}

fn codex_auth_mode() -> Result<CodexAuthMode, anyhow::Error> {
    let Some(raw) = env::var_os(CODEX_AUTH_MODE_ENV) else {
        return Ok(CodexAuthMode::OAuth);
    };
    let mode = raw
        .into_string()
        .map_err(|_| anyhow::anyhow!("{CODEX_AUTH_MODE_ENV} must be valid UTF-8"))?;
    match mode.trim().to_ascii_lowercase().as_str() {
        "" | "oauth" => Ok(CodexAuthMode::OAuth),
        "bearer_file" => Ok(CodexAuthMode::BearerFile),
        _ => anyhow::bail!("{CODEX_AUTH_MODE_ENV} must be oauth or bearer_file"),
    }
}

fn load_bearer_file_auth() -> Result<StoredAuth, anyhow::Error> {
    let raw_path = env::var_os(BEARER_TOKEN_FILE_ENV).ok_or_else(|| {
        anyhow::anyhow!("{BEARER_TOKEN_FILE_ENV} is required in bearer_file mode")
    })?;
    let path = raw_path
        .into_string()
        .map_err(|_| anyhow::anyhow!("{BEARER_TOKEN_FILE_ENV} must be valid UTF-8"))?;
    if path.trim().is_empty() {
        anyhow::bail!("{BEARER_TOKEN_FILE_ENV} must not be empty");
    }
    load_bearer_file_auth_from_path(Path::new(&path))
}

fn load_bearer_file_auth_from_path(path: &Path) -> Result<StoredAuth, anyhow::Error> {
    let link_metadata = fs::symlink_metadata(path)
        .map_err(|error| anyhow::anyhow!("Unable to read bearer token metadata: {error}"))?;
    if link_metadata.file_type().is_symlink() {
        anyhow::bail!("Bearer token file must not be a symlink");
    }
    if !link_metadata.is_file() {
        anyhow::bail!("Bearer token path must be a regular file");
    }
    if link_metadata.len() == 0 || link_metadata.len() > MAX_BEARER_TOKEN_FILE_BYTES {
        anyhow::bail!("Bearer token file has an invalid size");
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = link_metadata.permissions().mode() & 0o777;
        if mode != 0o600 {
            anyhow::bail!("Bearer token file permissions must be 0600");
        }
    }

    let raw = fs::read_to_string(path)
        .map_err(|error| anyhow::anyhow!("Unable to read bearer token file: {error}"))?;
    let token = raw.trim_end_matches(['\r', '\n']);
    if token.is_empty() {
        anyhow::bail!("Bearer token file is empty");
    }
    if token.chars().any(char::is_whitespace) {
        anyhow::bail!("Bearer token file must contain exactly one token");
    }

    Ok(StoredAuth {
        access: token.to_string(),
        refresh: String::new(),
        expires: u64::MAX,
        account_id: None,
    })
}

pub struct CodexAuthManager<S: AuthStorage<StoredAuth>> {
    pub store: CodexTokenStore<S>,
    #[cfg(test)]
    test_auth: Arc<Mutex<Option<StoredAuth>>>,
    refresh_lock: Arc<AsyncMutex<()>>,
    refresh_client: reqwest::Client,
    token_endpoint: String,
}

impl<S: AuthStorage<StoredAuth>> CodexAuthManager<S> {
    pub fn new(store: CodexTokenStore<S>) -> Self {
        Self::new_with_token_endpoint(store, format!("{ISSUER}/oauth/token"))
    }

    fn new_with_token_endpoint(store: CodexTokenStore<S>, token_endpoint: String) -> Self {
        Self {
            store,
            #[cfg(test)]
            test_auth: Arc::new(Mutex::new(None)),
            refresh_lock: CODEX_REFRESH_LOCK.clone(),
            refresh_client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(15))
                .timeout(Duration::from_secs(30))
                .build()
                .expect("failed to create Codex OAuth refresh client"),
            token_endpoint,
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    pub async fn get_auth(&self) -> Result<StoredAuth, anyhow::Error> {
        if codex_auth_mode()? == CodexAuthMode::BearerFile {
            return load_bearer_file_auth();
        }

        let stored = self.load_auth()?.ok_or_else(|| {
            anyhow::anyhow!("Not authenticated. Run: claude-code-proxy codex auth login")
        })?;

        if stored.expires > Self::now_ms() + REFRESH_MARGIN_MS {
            return Ok(stored);
        }

        self.refresh(false, None).await
    }

    pub async fn force_refresh(&self, rejected_access: &str) -> Result<StoredAuth, anyhow::Error> {
        if codex_auth_mode()? == CodexAuthMode::BearerFile {
            let current = load_bearer_file_auth()?;
            if current.access != rejected_access {
                return Ok(current);
            }
            anyhow::bail!("Configured bearer token was rejected; OAuth refresh is disabled");
        }
        self.refresh(true, Some(rejected_access)).await
    }

    fn load_auth(&self) -> Result<Option<StoredAuth>, anyhow::Error> {
        #[cfg(test)]
        if let Some(auth) = self
            .test_auth
            .lock()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .clone()
        {
            return Ok(Some(auth));
        }

        self.store.load_auth()
    }

    async fn refresh(
        &self,
        force: bool,
        rejected_access: Option<&str>,
    ) -> Result<StoredAuth, anyhow::Error> {
        let _refresh_guard = self.refresh_lock.lock().await;

        // Reload from durable storage after acquiring the single-flight lock.
        // Another request may have rotated and persisted the token while this
        // caller was waiting.
        let current = self
            .load_auth()?
            .ok_or_else(|| anyhow::anyhow!("Not authenticated"))?;

        if (!force && current.expires > Self::now_ms() + REFRESH_MARGIN_MS)
            || rejected_access.is_some_and(|access| current.access != access)
        {
            return Ok(current);
        }

        self.refresh_now(&current).await
    }

    async fn refresh_now(&self, current: &StoredAuth) -> Result<StoredAuth, anyhow::Error> {
        if current.refresh.is_empty() {
            anyhow::bail!("No refresh token stored; re-authenticate");
        }

        let form = [
            ("client_id", CLIENT_ID.to_string()),
            ("grant_type", "refresh_token".to_string()),
            ("refresh_token", current.refresh.clone()),
        ];

        let resp = self
            .refresh_client
            .post(&self.token_endpoint)
            .form(&form)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("refresh network error: {e}"))?;

        let status = resp.status().as_u16();
        if status == 401 || status == 403 {
            if let Some(latest) = self.store.load_auth()?
                && latest != *current
            {
                return Ok(latest);
            }
            self.store.clear_auth()?;
            let err_msg = resp
                .text()
                .await
                .unwrap_or_else(|_| "Token refresh unauthorized".to_string());
            anyhow::bail!("{err_msg}");
        }

        if !resp.status().is_success() {
            anyhow::bail!("Token refresh failed: {status}");
        }

        let tokens: TokenResponse = resp
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("failed to parse token response: {e}"))?;
        validate_token_response(&tokens)?;
        let account_id = extract_account_id(&tokens).or_else(|| current.account_id.clone());
        let expires = Self::now_ms() + (tokens.expires_in.unwrap_or(3600) * 1000);
        let next = StoredAuth {
            access: tokens.access_token,
            refresh: tokens.refresh_token,
            expires,
            account_id,
        };
        self.store.save_auth(next.clone())?;
        Ok(next)
    }

    pub fn persist_initial_tokens(
        &self,
        tokens: &TokenResponse,
    ) -> Result<StoredAuth, anyhow::Error> {
        validate_token_response(tokens)?;
        let account_id = extract_account_id(tokens);
        let expires = Self::now_ms() + (tokens.expires_in.unwrap_or(3600) * 1000);
        let auth = StoredAuth {
            access: tokens.access_token.clone(),
            refresh: tokens.refresh_token.clone(),
            expires,
            account_id,
        };
        self.store.save_auth(auth.clone())?;
        Ok(auth)
    }

    #[cfg(test)]
    pub fn set_test_auth(&self, auth: StoredAuth) {
        if let Ok(mut guard) = self.test_auth.lock() {
            *guard = Some(auth);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::InMemoryAuthStore;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    fn test_store() -> CodexTokenStore<InMemoryAuthStore<StoredAuth>> {
        CodexTokenStore::new(InMemoryAuthStore::new())
    }

    #[cfg(unix)]
    fn write_bearer_token(path: &Path, contents: &str, mode: u32) {
        fs::write(path, contents).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn bearer_file_auth_reads_secure_token_without_oauth_identity() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("bearer.token");
        write_bearer_token(&path, "test-bearer-token\n", 0o600);

        let auth = load_bearer_file_auth_from_path(&path).unwrap();
        assert_eq!(auth.access, "test-bearer-token");
        assert!(auth.refresh.is_empty());
        assert_eq!(auth.expires, u64::MAX);
        assert!(auth.account_id.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn bearer_file_auth_rejects_group_or_other_permissions() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("bearer.token");
        write_bearer_token(&path, "test-bearer-token\n", 0o640);

        let error = load_bearer_file_auth_from_path(&path).unwrap_err();
        assert!(error.to_string().contains("0600"));
    }

    #[cfg(unix)]
    #[test]
    fn bearer_file_auth_rejects_multiline_token_files() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("bearer.token");
        write_bearer_token(&path, "first-token\nsecond-token\n", 0o600);

        let error = load_bearer_file_auth_from_path(&path).unwrap_err();
        assert!(error.to_string().contains("exactly one token"));
    }

    #[tokio::test]
    async fn get_auth_returns_stored() {
        let store = test_store();
        let auth = StoredAuth {
            access: "test_access".into(),
            refresh: "test_refresh".into(),
            expires: 9999999999999,
            account_id: Some("acct_1".into()),
        };
        store.save_auth(auth.clone()).unwrap();
        let manager = CodexAuthManager::new(store);
        let result = manager.get_auth().await.unwrap();
        assert_eq!(result.access, "test_access");
        assert_eq!(result.account_id.as_deref(), Some("acct_1"));
    }

    #[tokio::test]
    async fn get_auth_fails_when_no_auth() {
        let store = test_store();
        let manager = CodexAuthManager::new(store);
        assert!(manager.get_auth().await.is_err());
        assert!(
            manager
                .get_auth()
                .await
                .unwrap_err()
                .to_string()
                .contains("Not authenticated")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_expired_auth_refreshes_once() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let refreshes = Arc::new(AtomicUsize::new(0));
        let server_refreshes = refreshes.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 4096];
            let read = stream.read(&mut request).unwrap();
            assert!(read > 0);
            assert!(String::from_utf8_lossy(&request[..read]).contains("refresh_token=stale"));
            server_refreshes.fetch_add(1, Ordering::SeqCst);

            let body = br#"{"access_token":"rotated","refresh_token":"rotated-refresh","expires_in":3600}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
            stream.write_all(body).unwrap();
        });

        let store = test_store();
        store
            .save_auth(StoredAuth {
                access: "expired".into(),
                refresh: "stale".into(),
                expires: 0,
                account_id: Some("acct_1".into()),
            })
            .unwrap();
        let manager = Arc::new(CodexAuthManager::new_with_token_endpoint(
            store,
            format!("http://{addr}/oauth/token"),
        ));
        let (first, second) = tokio::join!(manager.get_auth(), manager.get_auth());
        let results = [first.unwrap(), second.unwrap()];
        server.join().unwrap();

        assert_eq!(refreshes.load(Ordering::SeqCst), 1);
        assert!(results.iter().all(|auth| auth.access == "rotated"));
        assert!(results.iter().all(|auth| auth.refresh == "rotated-refresh"));
    }

    #[tokio::test]
    async fn stale_401_reuses_already_rotated_auth() {
        let store = test_store();
        store
            .save_auth(StoredAuth {
                access: "rotated".into(),
                refresh: "rotated-refresh".into(),
                expires: u64::MAX,
                account_id: Some("acct_1".into()),
            })
            .unwrap();
        let manager = CodexAuthManager::new_with_token_endpoint(
            store,
            "http://127.0.0.1:1/should-not-be-called".into(),
        );

        let auth = manager.force_refresh("rejected").await.unwrap();
        assert_eq!(auth.access, "rotated");
        assert_eq!(auth.refresh, "rotated-refresh");
    }

    #[tokio::test]
    async fn unauthorized_refresh_preserves_changed_refresh_token() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let backing = InMemoryAuthStore::new();
        let server_backing = backing.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 4096];
            assert!(stream.read(&mut request).unwrap() > 0);
            server_backing
                .save(StoredAuth {
                    access: "same-access".into(),
                    refresh: "replacement-refresh".into(),
                    expires: u64::MAX,
                    account_id: Some("acct_1".into()),
                })
                .unwrap();
            let body = b"rejected refresh token";
            let response = format!(
                "HTTP/1.1 401 Unauthorized\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
            stream.write_all(body).unwrap();
        });

        let store = CodexTokenStore::new(backing);
        store
            .save_auth(StoredAuth {
                access: "same-access".into(),
                refresh: "rejected-refresh".into(),
                expires: 0,
                account_id: Some("acct_1".into()),
            })
            .unwrap();
        let manager =
            CodexAuthManager::new_with_token_endpoint(store, format!("http://{addr}/oauth/token"));

        let auth = manager.get_auth().await.unwrap();
        server.join().unwrap();
        assert_eq!(auth.access, "same-access");
        assert_eq!(auth.refresh, "replacement-refresh");
        assert_eq!(manager.store.load_auth().unwrap(), Some(auth));
    }

    #[tokio::test]
    async fn durable_rotation_and_logout_are_observed_by_shared_manager() {
        let store = test_store();
        store
            .save_auth(StoredAuth {
                access: "first".into(),
                refresh: "first-refresh".into(),
                expires: u64::MAX,
                account_id: Some("acct_1".into()),
            })
            .unwrap();
        let manager = CodexAuthManager::new(store);
        assert_eq!(manager.get_auth().await.unwrap().access, "first");

        manager
            .store
            .save_auth(StoredAuth {
                access: "rotated".into(),
                refresh: "rotated-refresh".into(),
                expires: u64::MAX,
                account_id: Some("acct_2".into()),
            })
            .unwrap();
        let rotated = manager.get_auth().await.unwrap();
        assert_eq!(rotated.access, "rotated");
        assert_eq!(rotated.account_id.as_deref(), Some("acct_2"));

        manager.store.clear_auth().unwrap();
        assert!(manager.get_auth().await.is_err());
    }
}
