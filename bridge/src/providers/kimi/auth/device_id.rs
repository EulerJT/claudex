use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use crate::paths;

fn device_id_path() -> PathBuf {
    let deps = paths::DirResolverEnv::default();
    paths::kimi_device_id_file(&deps)
}

fn legacy_device_id_path() -> PathBuf {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| "/".to_string());
    std::path::Path::new(&home)
        .join(".config")
        .join("claude-code-proxy")
        .join("kimi")
        .join("device_id")
}

pub fn get_device_id() -> Result<String, anyhow::Error> {
    for candidate in [device_id_path(), legacy_device_id_path()] {
        if let Ok(raw) = fs::read_to_string(&candidate) {
            let trimmed = raw.trim().to_string();
            if !trimmed.is_empty() {
                return Ok(trimmed);
            }
        }
    }

    let id = uuid::Uuid::new_v4().to_string().replace('-', "");
    let target = device_id_path();
    if let Some(dir) = target.parent() {
        fs::create_dir_all(dir)?;
        set_mode(dir, 0o700);
    }
    fs::write(&target, &id)?;
    set_mode(&target, 0o600);
    Ok(id)
}

fn set_mode(path: &std::path::Path, mode: u32) {
    #[cfg(unix)]
    {
        if let Ok(meta) = fs::metadata(path) {
            let mut permissions = meta.permissions();
            permissions.set_mode(mode);
            let _ = fs::set_permissions(path, permissions);
        }
    }
}
