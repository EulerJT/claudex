use serde::{Deserialize, Serialize};

use crate::auth::{AuthStorage, FileAuthStore};
use crate::paths;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredAuth {
    pub access: String,
    pub refresh: String,
    pub expires_at_ms: u64,
    pub issuer: String,
    pub client_id: String,
}

pub struct GrokTokenStore<S: AuthStorage<StoredAuth>> {
    store: S,
}

impl<S: AuthStorage<StoredAuth>> GrokTokenStore<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
    pub fn load_auth(&self) -> anyhow::Result<Option<StoredAuth>> {
        self.store.load()
    }
    pub fn save_auth(&self, auth: StoredAuth) -> anyhow::Result<()> {
        self.store.save(auth)
    }
    pub fn clear_auth(&self) -> anyhow::Result<()> {
        self.store.clear()
    }
    pub fn auth_path(&self) -> String {
        self.store.path()
    }
}

pub fn file_store() -> GrokTokenStore<FileAuthStore<StoredAuth>> {
    let primary = paths::provider_auth_file("grok");
    let legacy = paths::provider_legacy_auth_file("grok");
    GrokTokenStore::new(FileAuthStore::new(
        primary.to_string_lossy().into_owned(),
        legacy.to_string_lossy().into_owned(),
    ))
}
