use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

use crate::request_identity::ConversationIdentity;

use crate::anthropic::sse::parse_sse_events;
use crate::config;

use super::translate::request::{
    ResponsesContextManagement, ResponsesInputItem, ResponsesRequest, is_compact_message_text,
};

const SIDECAR_VERSION: u32 = 1;
const MIN_COMPACTION_THRESHOLD: u64 = 32_768;
const MAX_STATE_BYTES: u64 = 64 * 1024 * 1024;
const INLINE_THRESHOLD_ENV: &str = "CCP_CODEX_INLINE_COMPACTION_THRESHOLD";
const STATE_DIR_ENV: &str = "CCP_CODEX_COMPACTION_STATE_DIR";
const WRITER_LOCK_FILENAME: &str = ".writer.lock";
const TEMP_SIDECAR_PREFIX: &str = ".inline-compaction-";
const TEMP_SIDECAR_SUFFIX: &str = ".tmp";
const DURABILITY_PROBE_PREFIX: &str = ".durability-probe-";
const SESSION_MAX_CONCURRENT_ENV: &str = "CCP_CODEX_SESSION_MAX_CONCURRENT_REQUESTS";
const SESSION_QUEUE_TIMEOUT_ENV: &str = "CCP_CODEX_SESSION_QUEUE_TIMEOUT_SECS";
const DEFAULT_QUEUE_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_SESSION_CONCURRENCY: usize = 64;
const MAX_QUEUE_TIMEOUT_SECS: u64 = 3_600;

#[cfg(test)]
static TEST_WRITE_FAILURE: Mutex<Option<TestWriteFailure>> = Mutex::new(None);
#[cfg(test)]
static TEST_WRITE_FAILURE_SERIAL: Mutex<()> = Mutex::new(());

#[cfg(test)]
struct TestWriteFailure {
    path: PathBuf,
    point: &'static str,
    raw_os_error: Option<i32>,
}

#[derive(Debug)]
pub struct DurabilityIndeterminateDetails {
    pub old_revision: Option<u64>,
    pub candidate_revision: u64,
    pub old_sidecar_hash: Option<String>,
    pub new_sidecar_hash: String,
    pub temp_fsynced_at_ms: u64,
    pub rename_at_ms: u64,
    pub directory_fsync_started_at_ms: u64,
    pub directory_fsync_failed_at_ms: u64,
    pub errno: Option<i32>,
    pub filesystem_type: Option<String>,
    pub mount_id: Option<u64>,
    pub mount_options: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum InlineCompactionError {
    #[error("inline compaction configuration error: {0}")]
    Config(&'static str),
    #[error("request timed out waiting for the same conversation lane")]
    LaneWaitTimeout,
    #[error("request timed out in the conversation concurrency queue")]
    SessionQueueTimeout,
    #[error("inline compaction state directory is already owned by another bridge process")]
    StateDirWriterLocked,
    #[error("inline compaction state directory does not support strict durable commits")]
    StateDurabilityUnsupported {
        #[source]
        source: io::Error,
    },
    #[error("inline compaction state is invalid: {0}")]
    InvalidState(&'static str),
    #[error("inline compaction upstream stream is invalid: {0}")]
    InvalidUpstream(&'static str),
    #[error("inline compaction state I/O failed during {operation}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error(
        "inline compaction state durability is indeterminate after the canonical namespace switch"
    )]
    DurabilityIndeterminate {
        details: Box<DurabilityIndeterminateDetails>,
        #[source]
        source: io::Error,
    },
}

impl InlineCompactionError {
    pub fn reason_code(&self) -> &'static str {
        match self {
            Self::Config(_) => "configuration_error",
            Self::LaneWaitTimeout => "lane_wait_timeout",
            Self::SessionQueueTimeout => "session_queue_timeout",
            Self::StateDirWriterLocked => "state_dir_writer_locked",
            Self::StateDurabilityUnsupported { .. } => "state_durability_unsupported",
            Self::InvalidState(_) => "invalid_state",
            Self::InvalidUpstream(_) => "invalid_upstream",
            Self::Io { .. } => "state_io_error",
            Self::DurabilityIndeterminate { .. } => "state_durability_indeterminate",
        }
    }

    pub fn durability_details(&self) -> Option<&DurabilityIndeterminateDetails> {
        match self {
            Self::DurabilityIndeterminate { details, .. } => Some(details),
            _ => None,
        }
    }

    pub fn degrades_state_backend(&self) -> bool {
        let Self::DurabilityIndeterminate { source, .. } = self else {
            return false;
        };
        state_backend_degrading_errno(source.raw_os_error())
    }
}

fn io_error(operation: &'static str) -> impl FnOnce(io::Error) -> InlineCompactionError {
    move |source| InlineCompactionError::Io { operation, source }
}

#[cfg(target_os = "linux")]
fn state_backend_degrading_errno(errno: Option<i32>) -> bool {
    // Linux: EIO, ENOSPC, EROFS, EDQUOT.
    matches!(errno, Some(5 | 28 | 30 | 122))
}

#[cfg(not(target_os = "linux"))]
fn state_backend_degrading_errno(_errno: Option<i32>) -> bool {
    false
}

#[derive(Debug, Clone)]
pub struct InlineCompactionConfig {
    threshold: u64,
    state_dir: PathBuf,
}

impl InlineCompactionConfig {
    pub fn from_environment() -> Result<Option<Self>, InlineCompactionError> {
        let Some(raw_threshold) = std::env::var_os(INLINE_THRESHOLD_ENV) else {
            return Ok(None);
        };
        let raw_threshold = raw_threshold
            .to_str()
            .ok_or(InlineCompactionError::Config(
                "threshold must be valid UTF-8",
            ))?
            .trim();
        let threshold = raw_threshold
            .parse::<u64>()
            .map_err(|_| InlineCompactionError::Config("threshold must be an integer"))?;
        if threshold < MIN_COMPACTION_THRESHOLD {
            return Err(InlineCompactionError::Config(
                "threshold is below the verified upstream minimum",
            ));
        }

        let state_dir = std::env::var_os(STATE_DIR_ENV)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .ok_or(InlineCompactionError::Config(
                "state directory is required when inline compaction is enabled",
            ))?;
        if !state_dir.is_absolute() || state_dir.parent().is_none() {
            return Err(InlineCompactionError::Config(
                "state directory must be an absolute non-root path",
            ));
        }
        if config::codex_server_compaction() {
            return Err(InlineCompactionError::Config(
                "legacy server compaction must be disabled",
            ));
        }
        if config::codex_previous_response_id() {
            return Err(InlineCompactionError::Config(
                "previous_response_id must be disabled",
            ));
        }
        if config::codex_transport() != config::CodexTransport::Http {
            return Err(InlineCompactionError::Config(
                "the verified inline compaction path requires HTTP transport",
            ));
        }

        Ok(Some(Self {
            threshold,
            state_dir,
        }))
    }

    #[cfg(test)]
    pub(super) fn for_tests(state_dir: PathBuf, threshold: u64) -> Self {
        Self {
            threshold,
            state_dir,
        }
    }
}

/// Holds the operating-system file lock for the complete server lifetime.
/// The lock file is intentionally persistent; ownership is attached to the
/// open file description and is released automatically when the process exits.
pub struct StateDirWriterLock {
    _file: File,
    capability: StateDirDurabilityCapability,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateDirDurabilityCapability {
    pub filesystem_type: Option<String>,
    pub mount_id: Option<u64>,
    pub mount_options: Option<String>,
    pub directory_fsync_supported: bool,
}

impl StateDirWriterLock {
    pub fn capability(&self) -> &StateDirDurabilityCapability {
        &self.capability
    }
}

pub fn acquire_state_dir_writer_lock_from_environment()
-> Result<Option<StateDirWriterLock>, InlineCompactionError> {
    let Some(config) = InlineCompactionConfig::from_environment()? else {
        return Ok(None);
    };
    acquire_state_dir_writer_lock(&config.state_dir).map(Some)
}

fn acquire_state_dir_writer_lock(
    state_dir: &Path,
) -> Result<StateDirWriterLock, InlineCompactionError> {
    ensure_state_dir(state_dir)?;
    let lock_path = state_dir.join(WRITER_LOCK_FILENAME);
    if let Ok(metadata) = fs::symlink_metadata(&lock_path)
        && (metadata.file_type().is_symlink() || !metadata.is_file())
    {
        return Err(InlineCompactionError::InvalidState(
            "writer lock path is not a regular file",
        ));
    }

    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(&lock_path)
        .map_err(io_error("opening state directory writer lock"))?;
    let metadata = file
        .metadata()
        .map_err(io_error("inspecting state directory writer lock"))?;
    if !metadata.is_file() {
        return Err(InlineCompactionError::InvalidState(
            "writer lock path is not a regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600))
            .map_err(io_error("setting writer lock permissions"))?;
    }
    file.try_lock().map_err(|source| {
        let source: std::io::Error = source.into();
        if source.kind() == std::io::ErrorKind::WouldBlock {
            InlineCompactionError::StateDirWriterLocked
        } else {
            InlineCompactionError::Io {
                operation: "locking state directory writer",
                source,
            }
        }
    })?;
    cleanup_orphan_temp_sidecars(state_dir)?;
    let capability = probe_state_dir_durability(state_dir)?;
    Ok(StateDirWriterLock {
        _file: file,
        capability,
    })
}

fn cleanup_orphan_temp_sidecars(state_dir: &Path) -> Result<(), InlineCompactionError> {
    for entry in fs::read_dir(state_dir).map_err(io_error("listing state directory"))? {
        let entry = entry.map_err(io_error("reading state directory entry"))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let is_sidecar_temp =
            name.starts_with(TEMP_SIDECAR_PREFIX) && name.ends_with(TEMP_SIDECAR_SUFFIX);
        let is_probe_artifact = name.starts_with(DURABILITY_PROBE_PREFIX);
        if !is_sidecar_temp && !is_probe_artifact {
            continue;
        }
        let path = entry.path();
        let metadata =
            fs::symlink_metadata(&path).map_err(io_error("inspecting orphan temporary sidecar"))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(InlineCompactionError::InvalidState(
                "orphan temporary sidecar is not a regular file",
            ));
        }
        fs::remove_file(path).map_err(io_error("removing orphan temporary sidecar"))?;
    }
    Ok(())
}

fn probe_state_dir_durability(
    state_dir: &Path,
) -> Result<StateDirDurabilityCapability, InlineCompactionError> {
    #[cfg(not(unix))]
    {
        let _ = state_dir;
        return Err(InlineCompactionError::StateDurabilityUnsupported {
            source: io::Error::new(
                io::ErrorKind::Unsupported,
                "directory fsync is required for strict durable commits",
            ),
        });
    }

    #[cfg(unix)]
    {
        let nonce = rand::random::<u64>();
        let probe_stem = format!(
            "{DURABILITY_PROBE_PREFIX}{}-{nonce:016x}",
            std::process::id()
        );
        let temp_path = state_dir.join(format!("{probe_stem}.tmp"));
        let committed_path = state_dir.join(format!("{probe_stem}.commit"));
        let result = (|| {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
            let mut probe = options
                .open(&temp_path)
                .map_err(io_error("creating state durability probe"))?;
            probe
                .write_all(b"claudex-state-durability-probe-v1\n")
                .map_err(io_error("writing state durability probe"))?;
            maybe_fail_test_io(state_dir, "probe_temp_fsync")
                .map_err(io_error("syncing state durability probe"))?;
            retry_interrupted(|| probe.sync_all())
                .map_err(io_error("syncing state durability probe"))?;
            drop(probe);

            let directory =
                File::open(state_dir).map_err(io_error("opening state directory for probe"))?;
            maybe_fail_test_io(state_dir, "probe_rename")
                .map_err(io_error("renaming state durability probe"))?;
            fs::rename(&temp_path, &committed_path)
                .map_err(io_error("renaming state durability probe"))?;
            sync_directory_for_probe(
                state_dir,
                &directory,
                "probe_directory_fsync",
                "syncing state directory after probe rename",
            )?;
            fs::remove_file(&committed_path)
                .map_err(io_error("removing state durability probe"))?;
            sync_directory_for_probe(
                state_dir,
                &directory,
                "probe_cleanup_directory_fsync",
                "syncing state directory after probe cleanup",
            )?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
            let _ = fs::remove_file(&committed_path);
        }
        result?;

        let mount = state_dir_mount_metadata(state_dir);
        Ok(StateDirDurabilityCapability {
            filesystem_type: mount.filesystem_type,
            mount_id: mount.mount_id,
            mount_options: mount.mount_options,
            directory_fsync_supported: true,
        })
    }
}

#[cfg(unix)]
fn sync_directory_for_probe(
    state_dir: &Path,
    directory: &File,
    test_point: &'static str,
    operation: &'static str,
) -> Result<(), InlineCompactionError> {
    let result = retry_interrupted(|| {
        maybe_fail_test_io(state_dir, test_point)?;
        directory.sync_all()
    });
    match result {
        Ok(()) => Ok(()),
        Err(source) if directory_sync_unsupported(&source) => {
            Err(InlineCompactionError::StateDurabilityUnsupported { source })
        }
        Err(source) => Err(InlineCompactionError::Io { operation, source }),
    }
}

fn retry_interrupted<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    loop {
        match operation() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

#[cfg(target_os = "linux")]
fn directory_sync_unsupported(error: &io::Error) -> bool {
    // Linux EINVAL is the documented signal for an unsupported fsync target.
    error.raw_os_error() == Some(22)
}

#[cfg(not(target_os = "linux"))]
fn directory_sync_unsupported(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::Unsupported
}

#[derive(Default)]
struct StateDirMountMetadata {
    filesystem_type: Option<String>,
    mount_id: Option<u64>,
    mount_options: Option<String>,
}

#[cfg(target_os = "linux")]
fn state_dir_mount_metadata(state_dir: &Path) -> StateDirMountMetadata {
    let Ok(canonical) = fs::canonicalize(state_dir) else {
        return StateDirMountMetadata::default();
    };
    let Ok(mountinfo) = fs::read_to_string("/proc/self/mountinfo") else {
        return StateDirMountMetadata::default();
    };
    let mut best: Option<(usize, StateDirMountMetadata)> = None;
    for line in mountinfo.lines() {
        let Some((before_separator, after_separator)) = line.split_once(" - ") else {
            continue;
        };
        let before = before_separator.split_whitespace().collect::<Vec<_>>();
        let after = after_separator.split_whitespace().collect::<Vec<_>>();
        if before.len() < 6 || after.len() < 3 {
            continue;
        }
        let mount_point = PathBuf::from(decode_mountinfo_field(before[4]));
        if !canonical.starts_with(&mount_point) {
            continue;
        }
        let specificity = mount_point.components().count();
        if best
            .as_ref()
            .is_some_and(|(current_specificity, _)| *current_specificity >= specificity)
        {
            continue;
        }
        best = Some((
            specificity,
            StateDirMountMetadata {
                filesystem_type: Some(after[0].to_string()),
                mount_id: before[0].parse::<u64>().ok(),
                mount_options: sanitized_mount_options(before[5], after[2]),
            },
        ));
    }
    best.map(|(_, metadata)| metadata).unwrap_or_default()
}

#[cfg(not(target_os = "linux"))]
fn state_dir_mount_metadata(_state_dir: &Path) -> StateDirMountMetadata {
    StateDirMountMetadata::default()
}

#[cfg(target_os = "linux")]
fn decode_mountinfo_field(field: &str) -> String {
    let bytes = field.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\'
            && index + 3 < bytes.len()
            && bytes[index + 1..=index + 3]
                .iter()
                .all(|byte| matches!(byte, b'0'..=b'7'))
        {
            let value = (bytes[index + 1] - b'0') * 64
                + (bytes[index + 2] - b'0') * 8
                + (bytes[index + 3] - b'0');
            decoded.push(value);
            index += 4;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

#[cfg(target_os = "linux")]
fn sanitized_mount_options(mount_options: &str, super_options: &str) -> Option<String> {
    const SAFE_KEYS: &[&str] = &[
        "async",
        "barrier",
        "commit",
        "data",
        "delalloc",
        "dirsync",
        "discard",
        "errors",
        "index",
        "journal_async_commit",
        "journal_checksum",
        "lazytime",
        "metacopy",
        "noatime",
        "nobarrier",
        "nodev",
        "nodelalloc",
        "nodiratime",
        "nodiscard",
        "noexec",
        "nosuid",
        "relatime",
        "redirect_dir",
        "ro",
        "rw",
        "strictatime",
        "sync",
        "volatile",
        "xino",
    ];
    let mut safe = mount_options
        .split(',')
        .chain(super_options.split(','))
        .filter(|option| {
            let key = option.split_once('=').map_or(*option, |(key, _)| key);
            SAFE_KEYS.contains(&key)
        })
        .map(str::to_string)
        .collect::<Vec<_>>();
    safe.sort_unstable();
    safe.dedup();
    (!safe.is_empty()).then(|| safe.join(","))
}

#[derive(Default)]
pub struct LaneLocks {
    lanes: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
}

#[derive(Default)]
pub struct StateRecoveryRegistry {
    lanes: Mutex<HashSet<String>>,
    backend_degraded: AtomicBool,
}

impl StateRecoveryRegistry {
    pub fn recovery_required(&self, lane_hash: &str) -> bool {
        self.lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(lane_hash)
    }

    pub fn backend_degraded(&self) -> bool {
        self.backend_degraded.load(Ordering::Acquire)
    }

    pub fn mark_from_error(&self, lane_hash: &str, error: &InlineCompactionError) {
        if error.durability_details().is_none() {
            return;
        }
        self.lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(lane_hash.to_string());
        if error.degrades_state_backend() {
            self.backend_degraded.store(true, Ordering::Release);
        }
    }

    pub fn note_durable_commit(&self, report: &CommitReport) {
        if !matches!(
            report.outcome,
            CommitOutcome::Created | CommitOutcome::Advanced | CommitOutcome::Replaced
        ) {
            return;
        }
        self.lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&report.lane_hash);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SessionConcurrencyConfig {
    max_concurrent: usize,
    wait_timeout: Duration,
}

impl SessionConcurrencyConfig {
    pub fn from_environment() -> Result<Option<Self>, InlineCompactionError> {
        let Some(raw_max) = std::env::var_os(SESSION_MAX_CONCURRENT_ENV) else {
            return Ok(None);
        };
        let max_concurrent = raw_max
            .to_str()
            .ok_or(InlineCompactionError::Config(
                "session concurrency must be valid UTF-8",
            ))?
            .trim()
            .parse::<usize>()
            .map_err(|_| InlineCompactionError::Config("session concurrency must be an integer"))?;
        if !(1..=MAX_SESSION_CONCURRENCY).contains(&max_concurrent) {
            return Err(InlineCompactionError::Config(
                "session concurrency must be between 1 and 64",
            ));
        }

        let wait_timeout = match std::env::var_os(SESSION_QUEUE_TIMEOUT_ENV) {
            Some(raw_timeout) => {
                let seconds = raw_timeout
                    .to_str()
                    .ok_or(InlineCompactionError::Config(
                        "session queue timeout must be valid UTF-8",
                    ))?
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| {
                        InlineCompactionError::Config("session queue timeout must be an integer")
                    })?;
                if !(1..=MAX_QUEUE_TIMEOUT_SECS).contains(&seconds) {
                    return Err(InlineCompactionError::Config(
                        "session queue timeout must be between 1 and 3600 seconds",
                    ));
                }
                Duration::from_secs(seconds)
            }
            None => DEFAULT_QUEUE_TIMEOUT,
        };

        Ok(Some(Self {
            max_concurrent,
            wait_timeout,
        }))
    }
}

#[derive(Default)]
pub struct SessionConcurrencyGates {
    sessions: Mutex<HashMap<String, Weak<Semaphore>>>,
}

pub fn state_lane_id(session_id: Option<&str>, agent_id: Option<&str>) -> Option<String> {
    let session_id = session_id.filter(|value| !value.is_empty())?;
    let Some(agent_id) = agent_id.filter(|value| !value.is_empty()) else {
        return Some(session_id.to_string());
    };
    let mut lane_id = String::with_capacity(session_id.len() + agent_id.len() + 1);
    lane_id.push_str(session_id);
    lane_id.push('\0');
    lane_id.push_str(agent_id);
    Some(lane_id)
}

pub fn state_lane_id_from_identity(identity: Option<&ConversationIdentity>) -> Option<String> {
    match identity? {
        ConversationIdentity::Main(session_id) => state_lane_id(Some(session_id), None),
        ConversationIdentity::Agent(session_id, agent_id) => {
            state_lane_id(Some(session_id), Some(agent_id))
        }
    }
}

pub fn telemetry_lane_hash(lane_id: &str) -> String {
    sha256_hex(lane_id.as_bytes())
}

impl LaneLocks {
    pub async fn lock(
        &self,
        session_id: &str,
    ) -> Result<OwnedMutexGuard<()>, InlineCompactionError> {
        let lane_hash = sha256_hex(session_id.as_bytes());
        let lane = {
            let mut lanes = self
                .lanes
                .lock()
                .map_err(|_| InlineCompactionError::InvalidState("lane lock registry poisoned"))?;
            lanes.retain(|_, lane| lane.strong_count() > 0);
            match lanes.get(&lane_hash).and_then(Weak::upgrade) {
                Some(lane) => lane,
                None => {
                    let lane = Arc::new(AsyncMutex::new(()));
                    lanes.insert(lane_hash, Arc::downgrade(&lane));
                    lane
                }
            }
        };
        tokio::time::timeout(DEFAULT_QUEUE_TIMEOUT, lane.lock_owned())
            .await
            .map_err(|_| InlineCompactionError::LaneWaitTimeout)
    }
}

impl SessionConcurrencyGates {
    pub async fn acquire(
        &self,
        session_id: &str,
        config: &SessionConcurrencyConfig,
    ) -> Result<OwnedSemaphorePermit, InlineCompactionError> {
        let session_hash = sha256_hex(session_id.as_bytes());
        let gate = {
            let mut sessions = self.sessions.lock().map_err(|_| {
                InlineCompactionError::InvalidState("session gate registry poisoned")
            })?;
            sessions.retain(|_, gate| gate.strong_count() > 0);
            match sessions.get(&session_hash).and_then(Weak::upgrade) {
                Some(gate) => gate,
                None => {
                    let gate = Arc::new(Semaphore::new(config.max_concurrent));
                    sessions.insert(session_hash, Arc::downgrade(&gate));
                    gate
                }
            }
        };

        tokio::time::timeout(config.wait_timeout, gate.acquire_owned())
            .await
            .map_err(|_| InlineCompactionError::SessionQueueTimeout)?
            .map_err(|_| InlineCompactionError::InvalidState("session concurrency gate closed"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SidecarState {
    version: u32,
    lane_hash: String,
    model: String,
    semantic_fingerprint: String,
    compact_threshold: u64,
    source_prefix_count: usize,
    source_prefix_sha256: String,
    last_output_match: Vec<Value>,
    compaction_item_json: String,
    compaction_item_sha256: String,
    opaque_sha256: String,
    raw_suffix_json: Vec<String>,
    raw_suffix_sha256: String,
    usage_input_tokens: u64,
    updated_at_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SidecarEnvelope {
    state: SidecarState,
    state_sha256: String,
}

pub struct PreparedInlineCompaction {
    config: InlineCompactionConfig,
    state_path: PathBuf,
    lane_hash: String,
    model: String,
    semantic_fingerprint: String,
    source_prefix_count: usize,
    source_prefix_sha256: String,
    previous: Option<SidecarState>,
    pending_input_suffix_json: Vec<String>,
    state_invalidation_reason: Option<&'static str>,
    portable_compact_boundary: bool,
}

impl PreparedInlineCompaction {
    pub fn lane_hash(&self) -> &str {
        &self.lane_hash
    }

    pub fn portable_compact_boundary(&self) -> bool {
        self.portable_compact_boundary
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    NoState,
    Created,
    Advanced,
    Replaced,
    Preserved,
}

impl CommitOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoState => "no_state",
            Self::Created => "created",
            Self::Advanced => "advanced",
            Self::Replaced => "replaced",
            Self::Preserved => "preserved",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitReport {
    pub outcome: CommitOutcome,
    pub compaction_count: usize,
    pub last_item_index: Option<u64>,
    pub suffix_count: usize,
    pub usage_input_tokens: u64,
    pub state_bytes: u64,
    pub lane_hash: String,
    pub invalidation_reason: Option<&'static str>,
    pub anomaly_reason: Option<&'static str>,
    pub effective_reasoning_context: Option<String>,
}

pub fn prepare_request(
    config: &InlineCompactionConfig,
    session_id: &str,
    request: &mut ResponsesRequest,
    portable_compact_boundary: bool,
) -> Result<PreparedInlineCompaction, InlineCompactionError> {
    prepare_request_with_recovery(
        config,
        session_id,
        request,
        portable_compact_boundary,
        false,
    )
}

pub fn prepare_request_with_recovery(
    config: &InlineCompactionConfig,
    session_id: &str,
    request: &mut ResponsesRequest,
    portable_compact_boundary: bool,
    recovery_required: bool,
) -> Result<PreparedInlineCompaction, InlineCompactionError> {
    ensure_state_dir(&config.state_dir)?;
    request.raw_input_override = None;
    request.context_management = Some(vec![ResponsesContextManagement::Compaction {
        compact_threshold: config.threshold,
    }]);
    let include = request.include.get_or_insert_with(Vec::new);
    if !include
        .iter()
        .any(|item| item == "reasoning.encrypted_content")
    {
        include.push("reasoning.encrypted_content".to_string());
    }

    let lane_hash = sha256_hex(session_id.as_bytes());
    let state_path = config.state_dir.join(format!("{lane_hash}.json"));
    let mut state_invalidation_reason =
        portable_compact_boundary.then_some("portable_compact_new_lineage");

    let envelope_len = request
        .input
        .iter()
        .take_while(|item| is_envelope_item(item))
        .count();
    let envelope = serialize_typed_items(&request.input[..envelope_len])?;
    let conversation = request.input[envelope_len..]
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| InlineCompactionError::InvalidState("request input could not be encoded"))?;
    let semantic_fingerprint = request_fingerprint(request, &request.input[..envelope_len])?;
    let source_prefix_count = conversation.len();
    let source_prefix_sha256 = hash_transcript(&conversation)?;

    let mut previous = if recovery_required {
        state_invalidation_reason = Some("state_recovery_required");
        None
    } else {
        read_state(&state_path)?
    };
    if let Some(state) = previous.as_ref() {
        if state.lane_hash != lane_hash {
            return Err(InlineCompactionError::InvalidState(
                "sidecar lane fingerprint mismatch",
            ));
        }
        let mismatch_reason = if state.model != request.model {
            Some("model_mismatch")
        } else if state.compact_threshold != config.threshold {
            Some("threshold_mismatch")
        } else if !portable_compact_boundary && state.semantic_fingerprint != semantic_fingerprint {
            Some("semantic_fingerprint_mismatch")
        } else {
            None
        };
        if let Some(reason) = mismatch_reason {
            clear_state_path(&state_path)?;
            previous = None;
            state_invalidation_reason = Some(reason);
        }
    }

    let mut pending_input_suffix_json = Vec::new();
    if let Some(state) = previous.as_ref() {
        let replay_plan = replay_start(state, &conversation)
            .map(|conversation_start| (state.raw_suffix_json.len(), conversation_start))
            .or_else(|| {
                portable_compact_boundary
                    .then(|| portable_replay_plan(state, &conversation))
                    .flatten()
            });
        if let Some((raw_suffix_count, conversation_start)) = replay_plan {
            let mut wire_input = envelope;
            wire_input.push(state.compaction_item_json.clone());
            wire_input.extend(state.raw_suffix_json[..raw_suffix_count].iter().cloned());
            pending_input_suffix_json = serialize_values(&conversation[conversation_start..])?;
            wire_input.extend(pending_input_suffix_json.iter().cloned());
            validate_raw_fragments(&wire_input)?;
            request.raw_input_override = Some(wire_input);
        } else {
            clear_state_path(&state_path)?;
            previous = None;
            state_invalidation_reason = Some("transcript_mismatch");
        }
    }

    Ok(PreparedInlineCompaction {
        config: config.clone(),
        state_path,
        lane_hash,
        model: request.model.clone(),
        semantic_fingerprint,
        source_prefix_count,
        source_prefix_sha256,
        previous,
        pending_input_suffix_json,
        state_invalidation_reason,
        portable_compact_boundary,
    })
}

pub fn commit_response(
    prepared: &PreparedInlineCompaction,
    upstream_sse: &[u8],
    visible_output_items: &[ResponsesInputItem],
) -> Result<CommitOutcome, InlineCompactionError> {
    Ok(commit_response_report(prepared, upstream_sse, visible_output_items)?.outcome)
}

pub fn commit_response_report(
    prepared: &PreparedInlineCompaction,
    upstream_sse: &[u8],
    visible_output_items: &[ResponsesInputItem],
) -> Result<CommitReport, InlineCompactionError> {
    let captured = capture_upstream(upstream_sse, prepared.config.threshold)?;
    if let Some(anomaly_reason) = captured.anomaly_reason {
        // Usage is only corroborating telemetry: it cannot prove that the
        // upstream emitted a reusable compaction item. Preserve the last
        // committed sidecar byte-for-byte and let the next request replay from
        // that known-good boundary instead of pruning on an inferred event.
        return Ok(CommitReport {
            outcome: if prepared.previous.is_some() {
                CommitOutcome::Preserved
            } else {
                CommitOutcome::NoState
            },
            compaction_count: captured.compaction_count,
            last_item_index: captured.last_compaction_item_index,
            suffix_count: prepared
                .previous
                .as_ref()
                .map_or(0, |state| state.raw_suffix_json.len()),
            usage_input_tokens: captured.usage_input_tokens,
            state_bytes: fs::metadata(&prepared.state_path)
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            lane_hash: prepared.lane_hash.clone(),
            invalidation_reason: prepared.state_invalidation_reason,
            anomaly_reason: Some(anomaly_reason),
            effective_reasoning_context: captured.effective_reasoning_context,
        });
    }
    let last_output_match = visible_output_items
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            InlineCompactionError::InvalidState("visible output items could not be encoded")
        })?;

    // Claude portable compaction is itself a lineage boundary. Native inline
    // compaction may still be required to let that large summarization request
    // complete, but its opaque item belongs only to the summarization request.
    // The next normal Claude request is anchored by the portable transcript,
    // so persisting this intermediate opaque state would stack two lineages.
    if prepared.portable_compact_boundary {
        // Keep the previous native state while the portable summary is
        // in-flight so an upstream failure can retry through the same compact
        // context. A completed summary is the boundary that retires it.
        clear_state_path(&prepared.state_path)?;
        return Ok(CommitReport {
            outcome: CommitOutcome::NoState,
            compaction_count: captured.compaction_count,
            last_item_index: captured.last_compaction_item_index,
            suffix_count: 0,
            usage_input_tokens: captured.usage_input_tokens,
            state_bytes: 0,
            lane_hash: prepared.lane_hash.clone(),
            invalidation_reason: prepared.state_invalidation_reason,
            anomaly_reason: None,
            effective_reasoning_context: captured.effective_reasoning_context,
        });
    }

    let previous_existed = prepared.previous.is_some();
    let mut state = if let Some(compaction_position) = captured.compaction_position {
        let compaction = captured.items.get(compaction_position).ok_or(
            InlineCompactionError::InvalidUpstream("compaction item position is missing"),
        )?;
        let opaque = compaction
            .value
            .get("encrypted_content")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or(InlineCompactionError::InvalidUpstream(
                "compaction item has no opaque payload",
            ))?;
        let raw_suffix_json = captured.items[compaction_position + 1..]
            .iter()
            .map(|item| item.raw.clone())
            .collect::<Vec<_>>();
        SidecarState {
            version: SIDECAR_VERSION,
            lane_hash: prepared.lane_hash.clone(),
            model: prepared.model.clone(),
            semantic_fingerprint: prepared.semantic_fingerprint.clone(),
            compact_threshold: prepared.config.threshold,
            source_prefix_count: prepared.source_prefix_count,
            source_prefix_sha256: prepared.source_prefix_sha256.clone(),
            last_output_match,
            compaction_item_json: compaction.raw.clone(),
            compaction_item_sha256: sha256_hex(compaction.raw.as_bytes()),
            opaque_sha256: sha256_hex(opaque.as_bytes()),
            raw_suffix_sha256: hash_raw_fragments(&raw_suffix_json),
            raw_suffix_json,
            usage_input_tokens: captured.usage_input_tokens,
            updated_at_ms: next_revision(prepared.previous.as_ref()),
        }
    } else if let Some(mut state) = prepared.previous.clone() {
        state
            .raw_suffix_json
            .extend(prepared.pending_input_suffix_json.iter().cloned());
        state
            .raw_suffix_json
            .extend(captured.items.iter().map(|item| item.raw.clone()));
        state.raw_suffix_sha256 = hash_raw_fragments(&state.raw_suffix_json);
        state.source_prefix_count = prepared.source_prefix_count;
        state.source_prefix_sha256 = prepared.source_prefix_sha256.clone();
        state.last_output_match = last_output_match;
        state.usage_input_tokens = captured.usage_input_tokens;
        state.updated_at_ms = next_revision(prepared.previous.as_ref());
        state
    } else {
        return Ok(CommitReport {
            outcome: CommitOutcome::NoState,
            compaction_count: captured.compaction_count,
            last_item_index: captured.last_compaction_item_index,
            suffix_count: 0,
            usage_input_tokens: captured.usage_input_tokens,
            state_bytes: 0,
            lane_hash: prepared.lane_hash.clone(),
            invalidation_reason: prepared.state_invalidation_reason,
            anomaly_reason: None,
            effective_reasoning_context: captured.effective_reasoning_context,
        });
    };

    state.raw_suffix_sha256 = hash_raw_fragments(&state.raw_suffix_json);
    validate_state(&state)?;
    let suffix_count = state.raw_suffix_json.len();
    let state_bytes = write_state_atomic(
        &prepared.config.state_dir,
        &prepared.state_path,
        &state,
        prepared
            .previous
            .as_ref()
            .map(|previous| previous.updated_at_ms),
    )?;

    let outcome = if captured.compaction_position.is_some() {
        if previous_existed {
            CommitOutcome::Replaced
        } else {
            CommitOutcome::Created
        }
    } else {
        CommitOutcome::Advanced
    };
    Ok(CommitReport {
        outcome,
        compaction_count: captured.compaction_count,
        last_item_index: captured.last_compaction_item_index,
        suffix_count,
        usage_input_tokens: captured.usage_input_tokens,
        state_bytes,
        lane_hash: prepared.lane_hash.clone(),
        invalidation_reason: prepared.state_invalidation_reason,
        anomaly_reason: None,
        effective_reasoning_context: captured.effective_reasoning_context,
    })
}

fn replay_start(state: &SidecarState, conversation: &[Value]) -> Option<usize> {
    if state.source_prefix_count > conversation.len() {
        return None;
    }
    let prefix = &conversation[..state.source_prefix_count];
    let exact_match =
        hash_json(prefix).ok().as_deref() == Some(state.source_prefix_sha256.as_str());
    let semantic_match =
        hash_transcript(prefix).ok().as_deref() == Some(state.source_prefix_sha256.as_str());
    if !exact_match && !semantic_match {
        return None;
    }
    let mut actual_index = state.source_prefix_count;
    let mut stable_anchor_seen = false;
    for expected in &state.last_output_match {
        if item_type(expected) == Some("reasoning") {
            // Claude Code only echoes encrypted reasoning when the Anthropic
            // response carried a replayable thinking block. Empty-summary
            // reasoning is legitimately omitted from the next Messages
            // request, while the raw reasoning item remains in our suffix.
            if conversation
                .get(actual_index)
                .is_some_and(|actual| item_type(actual) == Some("reasoning"))
            {
                if !replay_item_matches(expected, &conversation[actual_index]) {
                    return None;
                }
                actual_index += 1;
            }
            continue;
        }
        stable_anchor_seen = true;
        let actual = conversation.get(actual_index)?;
        if !replay_item_matches(expected, actual) {
            return None;
        }
        actual_index += 1;
    }
    if !state.last_output_match.is_empty() && !stable_anchor_seen {
        // A reasoning-only response has no client-visible delivery anchor. Do
        // not risk replaying state committed before downstream delivery.
        return None;
    }
    Some(actual_index)
}

/// Claude Code's portable `/compact` request deliberately replaces the most
/// recent user/assistant pair with its compact instruction. The omitted pair
/// remains outside the portable summary and is reattached by Claude Code after
/// the summary completes. A normal replay therefore cannot use the response
/// anchor for this one request.
///
/// Recover the exact truncation point without weakening normal lineage checks:
/// append candidate input fragments from the authenticated sidecar suffix to
/// the retained portable history and require that they reconstruct the stored
/// source-prefix hash. Exactly one reconstruction is accepted. The wire replay
/// stops before that omitted tail, then appends only the portable instruction.
fn portable_replay_plan(state: &SidecarState, conversation: &[Value]) -> Option<(usize, usize)> {
    let marker_index = conversation.len().checked_sub(1)?;
    if !is_portable_compact_message(&conversation[marker_index]) {
        return None;
    }
    let retained_history = &conversation[..marker_index];
    if retained_history.len() >= state.source_prefix_count {
        return None;
    }
    let missing_source_items = state.source_prefix_count - retained_history.len();
    let suffix_values = state
        .raw_suffix_json
        .iter()
        .map(|raw| serde_json::from_str::<Value>(raw).ok())
        .collect::<Option<Vec<_>>>()?;
    if missing_source_items > suffix_values.len() {
        return None;
    }

    let mut matching_cutoff = None;
    for cutoff in 0..=suffix_values.len() - missing_source_items {
        let mut reconstructed = retained_history.to_vec();
        reconstructed.extend_from_slice(&suffix_values[cutoff..cutoff + missing_source_items]);
        if hash_transcript(&reconstructed).ok().as_deref()
            == Some(state.source_prefix_sha256.as_str())
        {
            if matching_cutoff.is_some() {
                return None;
            }
            matching_cutoff = Some(cutoff);
        }
    }
    matching_cutoff.map(|cutoff| (cutoff, marker_index))
}

fn is_portable_compact_message(item: &Value) -> bool {
    if item.get("type").and_then(Value::as_str) != Some("message")
        || item.get("role").and_then(Value::as_str) != Some("user")
    {
        return false;
    }
    match item.get("content") {
        Some(Value::String(text)) => is_compact_message_text(text),
        Some(Value::Array(parts)) => parts.iter().any(|part| {
            part.get("text")
                .and_then(Value::as_str)
                .is_some_and(is_compact_message_text)
        }),
        _ => false,
    }
}

fn item_type(item: &Value) -> Option<&str> {
    item.get("type").and_then(Value::as_str)
}

fn replay_item_matches(expected: &Value, actual: &Value) -> bool {
    if item_type(expected) != item_type(actual) {
        return false;
    }
    match item_type(expected) {
        Some("reasoning") => {
            expected.get("id") == actual.get("id")
                && expected.get("encrypted_content") == actual.get("encrypted_content")
        }
        Some("function_call") => {
            expected.get("call_id") == actual.get("call_id")
                && expected.get("name") == actual.get("name")
                && function_call_arguments_match(
                    expected.get("name").and_then(Value::as_str).unwrap_or(""),
                    expected.get("arguments").and_then(Value::as_str),
                    actual.get("arguments").and_then(Value::as_str),
                )
        }
        _ => expected == actual,
    }
}

fn function_call_arguments_match(
    tool_name: &str,
    expected: Option<&str>,
    actual: Option<&str>,
) -> bool {
    let (Some(expected), Some(actual)) = (expected, actual) else {
        return false;
    };
    match (
        serde_json::from_str::<Value>(expected),
        serde_json::from_str::<Value>(actual),
    ) {
        (Ok(expected), Ok(actual)) => {
            canonical_function_call_arguments(tool_name, expected)
                == canonical_function_call_arguments(tool_name, actual)
        }
        _ => expected == actual,
    }
}

fn canonical_function_call_arguments(tool_name: &str, mut value: Value) -> Value {
    if tool_name == "Edit"
        && let Value::Object(object) = &mut value
        && ["file_path", "old_string", "new_string"]
            .iter()
            .all(|field| object.contains_key(*field))
    {
        object
            .entry("replace_all".to_string())
            .or_insert(Value::Bool(false));
    }
    canonical_json(value)
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        Value::Object(values) => {
            let sorted = values
                .into_iter()
                .map(|(key, value)| (key, canonical_json(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect())
        }
        value => value,
    }
}

fn hash_transcript(values: &[Value]) -> Result<String, InlineCompactionError> {
    let normalized = values
        .iter()
        .cloned()
        .map(canonical_transcript_item)
        .collect::<Vec<_>>();
    hash_json(&normalized)
}

fn canonical_transcript_item(mut item: Value) -> Value {
    if item_type(&item) == Some("function_call")
        && let Some(tool_name) = item.get("name").and_then(Value::as_str).map(str::to_owned)
        && let Some(arguments) = item.get("arguments").and_then(Value::as_str)
        && let Ok(arguments) = serde_json::from_str::<Value>(arguments)
        && let Some(object) = item.as_object_mut()
    {
        object.insert(
            "arguments".to_string(),
            Value::String(
                serde_json::to_string(&canonical_function_call_arguments(&tool_name, arguments))
                    .expect("serializing a JSON value cannot fail"),
            ),
        );
    }
    item
}

fn request_fingerprint(
    request: &ResponsesRequest,
    envelope: &[ResponsesInputItem],
) -> Result<String, InlineCompactionError> {
    let mut value = serde_json::to_value(request).map_err(|_| {
        InlineCompactionError::InvalidState("request fingerprint could not be encoded")
    })?;
    let object = value
        .as_object_mut()
        .ok_or(InlineCompactionError::InvalidState(
            "request fingerprint is not an object",
        ))?;
    object.remove("input");
    object.remove("prompt_cache_key");
    // These are per-turn controls and can legitimately change between a
    // function call and the matching tool result. The stable tool definitions,
    // model, reasoning effort, instructions, and lane remain fingerprinted.
    object.remove("tool_choice");
    object.remove("parallel_tool_calls");
    object.remove("text");
    if let Some(Value::String(instructions)) = object.get_mut("instructions") {
        *instructions = stable_instructions(instructions);
    }
    object.insert(
        "input_envelope".to_string(),
        serde_json::to_value(envelope).map_err(|_| {
            InlineCompactionError::InvalidState("request envelope fingerprint could not be encoded")
        })?,
    );
    hash_json(&value)
}

fn stable_instructions(instructions: &str) -> String {
    let lines = instructions.split('\n').collect::<Vec<_>>();
    let mut normalized = Vec::with_capacity(lines.len());
    let mut index = 0;
    while index < lines.len() {
        let line = lines[index];
        let is_status_header = line.trim().eq_ignore_ascii_case("status:");
        let nearby_git_context = lines[index.saturating_sub(8)..index]
            .iter()
            .any(|candidate| candidate.to_ascii_lowercase().contains("git"));
        if is_status_header && nearby_git_context {
            normalized.push(line);
            normalized.push("<runtime-git-status>");
            index += 1;
            while index < lines.len() && !lines[index].trim().is_empty() {
                index += 1;
            }
            continue;
        }
        normalized.push(line);
        index += 1;
    }
    normalized.join("\n")
}

fn is_envelope_item(item: &ResponsesInputItem) -> bool {
    match item {
        ResponsesInputItem::AdditionalTools { .. } => true,
        ResponsesInputItem::Message { role, .. } => role == "developer",
        _ => false,
    }
}

fn serialize_typed_items(
    items: &[ResponsesInputItem],
) -> Result<Vec<String>, InlineCompactionError> {
    items
        .iter()
        .map(|item| {
            serde_json::to_string(item).map_err(|_| {
                InlineCompactionError::InvalidState("request input could not be encoded")
            })
        })
        .collect()
}

fn serialize_values(values: &[Value]) -> Result<Vec<String>, InlineCompactionError> {
    values
        .iter()
        .map(|value| {
            serde_json::to_string(value).map_err(|_| {
                InlineCompactionError::InvalidState("request suffix could not be encoded")
            })
        })
        .collect()
}

fn validate_raw_fragments(items: &[String]) -> Result<(), InlineCompactionError> {
    for item in items {
        RawValue::from_string(item.clone())
            .map_err(|_| InlineCompactionError::InvalidState("stored output item is not JSON"))?;
    }
    Ok(())
}

#[derive(Debug)]
struct CapturedRawItem {
    output_index: u64,
    raw: String,
    value: Value,
}

#[derive(Debug)]
struct CapturedUpstream {
    items: Vec<CapturedRawItem>,
    compaction_position: Option<usize>,
    compaction_count: usize,
    last_compaction_item_index: Option<u64>,
    usage_input_tokens: u64,
    anomaly_reason: Option<&'static str>,
    effective_reasoning_context: Option<String>,
}

#[derive(Deserialize)]
struct RawOutputDoneEvent {
    output_index: Option<u64>,
    item: Box<RawValue>,
}

fn capture_upstream(
    body: &[u8],
    threshold: u64,
) -> Result<CapturedUpstream, InlineCompactionError> {
    let mut items = BTreeMap::<usize, CapturedRawItem>::new();
    let mut completed_count = 0usize;
    let mut usage_input_tokens = None;
    let mut completed_seen = false;
    let mut effective_reasoning_context = None;

    for event in parse_sse_events(body) {
        let data = event.data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        let payload: Value = serde_json::from_str(data).map_err(|_| {
            InlineCompactionError::InvalidUpstream("SSE event data is not valid JSON")
        })?;
        let event_type = payload.get("type").and_then(Value::as_str).unwrap_or("");
        if let Some(context) = payload
            .pointer("/response/reasoning/context")
            .and_then(Value::as_str)
            .and_then(sanitize_reasoning_context)
        {
            effective_reasoning_context = Some(context);
        }
        match event_type {
            "error" | "response.error" | "response.failed" | "response.incomplete" => {
                return Err(InlineCompactionError::InvalidUpstream(
                    "upstream emitted a terminal failure",
                ));
            }
            "response.output_item.done" => {
                if completed_seen {
                    return Err(InlineCompactionError::InvalidUpstream(
                        "output item arrived after response.completed",
                    ));
                }
                let event: RawOutputDoneEvent = serde_json::from_str(data).map_err(|_| {
                    InlineCompactionError::InvalidUpstream(
                        "output_item.done is missing its raw item",
                    )
                })?;
                let output_index = event
                    .output_index
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or(InlineCompactionError::InvalidUpstream(
                        "output_item.done has no valid output_index",
                    ))?;
                let raw = event.item.get().to_string();
                let value = serde_json::from_str(&raw).map_err(|_| {
                    InlineCompactionError::InvalidUpstream("output item is not valid JSON")
                })?;
                if items
                    .insert(
                        output_index,
                        CapturedRawItem {
                            output_index: output_index as u64,
                            raw,
                            value,
                        },
                    )
                    .is_some()
                {
                    return Err(InlineCompactionError::InvalidUpstream(
                        "duplicate output_index in output_item.done",
                    ));
                }
            }
            "response.completed" => {
                completed_count += 1;
                completed_seen = true;
                usage_input_tokens = payload
                    .pointer("/response/usage/input_tokens")
                    .and_then(Value::as_u64);
                if payload
                    .pointer("/response/status")
                    .and_then(Value::as_str)
                    .is_some_and(|status| status != "completed")
                {
                    return Err(InlineCompactionError::InvalidUpstream(
                        "response.completed carried a non-completed status",
                    ));
                }
            }
            _ => {}
        }
    }

    if completed_count != 1 {
        return Err(InlineCompactionError::InvalidUpstream(
            "expected exactly one response.completed",
        ));
    }
    let usage_input_tokens = usage_input_tokens.ok_or(InlineCompactionError::InvalidUpstream(
        "response.completed has no input token usage",
    ))?;
    let items = items.into_values().collect::<Vec<_>>();
    // A single Responses run can compact more than once when the rendered
    // context crosses a low threshold again after the first compaction. The
    // latest compaction item is the canonical continuation boundary: it
    // subsumes earlier output, while only items after it remain as raw suffix.
    let compaction_count = items
        .iter()
        .filter(|item| item.value.get("type").and_then(Value::as_str) == Some("compaction"))
        .count();
    let compaction_position = items
        .iter()
        .rposition(|item| item.value.get("type").and_then(Value::as_str) == Some("compaction"));
    let last_compaction_item_index =
        compaction_position.map(|position| items[position].output_index);
    let anomaly_reason = (usage_input_tokens >= threshold && compaction_position.is_none())
        .then_some("threshold_crossed_without_compaction");
    // `compact_threshold` applies to the rendered context before the server
    // compacts and prunes it. `response.completed.usage.input_tokens` can
    // describe the smaller post-prune inference context, so a valid
    // compaction item with usage below the threshold is not contradictory.

    Ok(CapturedUpstream {
        items,
        compaction_position,
        compaction_count,
        last_compaction_item_index,
        usage_input_tokens,
        anomaly_reason,
        effective_reasoning_context,
    })
}

fn sanitize_reasoning_context(value: &str) -> Option<String> {
    if value.len() <= 32
        && !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        Some(value.to_string())
    } else {
        None
    }
}

fn ensure_state_dir(path: &Path) -> Result<(), InlineCompactionError> {
    fs::create_dir_all(path).map_err(io_error("creating state directory"))?;
    let metadata = fs::symlink_metadata(path).map_err(io_error("inspecting state directory"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(InlineCompactionError::InvalidState(
            "state directory is not a real directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(io_error("setting state directory permissions"))?;
        let mode = fs::symlink_metadata(path)
            .map_err(io_error("verifying state directory permissions"))?
            .permissions()
            .mode()
            & 0o777;
        if mode != 0o700 {
            return Err(InlineCompactionError::InvalidState(
                "state directory permissions are not 0700",
            ));
        }
    }
    Ok(())
}

fn read_state(path: &Path) -> Result<Option<SidecarState>, InlineCompactionError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error("inspecting sidecar")(error)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(InlineCompactionError::InvalidState(
            "sidecar is not a regular file",
        ));
    }
    if metadata.len() > MAX_STATE_BYTES {
        return Err(InlineCompactionError::InvalidState(
            "sidecar exceeds the size limit",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            return Err(InlineCompactionError::InvalidState(
                "sidecar permissions are not 0600",
            ));
        }
    }
    let bytes = fs::read(path).map_err(io_error("reading sidecar"))?;
    let envelope: SidecarEnvelope = serde_json::from_slice(&bytes)
        .map_err(|_| InlineCompactionError::InvalidState("sidecar JSON is corrupt"))?;
    let state_bytes = serde_json::to_vec(&envelope.state)
        .map_err(|_| InlineCompactionError::InvalidState("sidecar could not be checksummed"))?;
    if sha256_hex(&state_bytes) != envelope.state_sha256 {
        return Err(InlineCompactionError::InvalidState(
            "sidecar checksum mismatch",
        ));
    }
    validate_state(&envelope.state)?;
    Ok(Some(envelope.state))
}

fn validate_state(state: &SidecarState) -> Result<(), InlineCompactionError> {
    if state.version != SIDECAR_VERSION {
        return Err(InlineCompactionError::InvalidState(
            "unsupported sidecar version",
        ));
    }
    let compaction: Value = serde_json::from_str(&state.compaction_item_json).map_err(|_| {
        InlineCompactionError::InvalidState("stored compaction item is not valid JSON")
    })?;
    if compaction.get("type").and_then(Value::as_str) != Some("compaction") {
        return Err(InlineCompactionError::InvalidState(
            "stored item is not a compaction item",
        ));
    }
    let opaque = compaction
        .get("encrypted_content")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(InlineCompactionError::InvalidState(
            "stored compaction item has no opaque payload",
        ))?;
    if sha256_hex(state.compaction_item_json.as_bytes()) != state.compaction_item_sha256
        || sha256_hex(opaque.as_bytes()) != state.opaque_sha256
    {
        return Err(InlineCompactionError::InvalidState(
            "stored compaction hashes do not match",
        ));
    }
    validate_raw_fragments(&state.raw_suffix_json)?;
    for item in &state.raw_suffix_json {
        let item: Value = serde_json::from_str(item)
            .map_err(|_| InlineCompactionError::InvalidState("stored output item is not JSON"))?;
        if item.get("type").and_then(Value::as_str) == Some("compaction") {
            return Err(InlineCompactionError::InvalidState(
                "stored suffix contains an extra compaction item",
            ));
        }
        validate_replayable_output_item(&item)?;
    }
    if hash_raw_fragments(&state.raw_suffix_json) != state.raw_suffix_sha256 {
        return Err(InlineCompactionError::InvalidState(
            "stored suffix hash does not match",
        ));
    }
    Ok(())
}

fn validate_replayable_output_item(item: &Value) -> Result<(), InlineCompactionError> {
    let kind =
        item.get("type")
            .and_then(Value::as_str)
            .ok_or(InlineCompactionError::InvalidState(
                "stored output item has no type",
            ))?;
    let valid = match kind {
        "message" => {
            item.get("role").and_then(Value::as_str).is_some()
                && item.get("content").and_then(Value::as_array).is_some()
        }
        "function_call" => {
            item.get("call_id").and_then(Value::as_str).is_some()
                && item.get("name").and_then(Value::as_str).is_some()
                && item.get("arguments").and_then(Value::as_str).is_some()
        }
        "reasoning" => {
            item.get("id").and_then(Value::as_str).is_some()
                && item
                    .get("encrypted_content")
                    .and_then(Value::as_str)
                    .is_some()
        }
        // Future and hosted-tool output item types remain opaque by design.
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(InlineCompactionError::InvalidState(
            "stored known output item is incomplete",
        ))
    }
}

fn write_state_atomic(
    state_dir: &Path,
    state_path: &Path,
    state: &SidecarState,
    old_revision: Option<u64>,
) -> Result<u64, InlineCompactionError> {
    let state_bytes = serde_json::to_vec(state)
        .map_err(|_| InlineCompactionError::InvalidState("sidecar could not be encoded"))?;
    let envelope = SidecarEnvelope {
        state: state.clone(),
        state_sha256: sha256_hex(&state_bytes),
    };
    let bytes = serde_json::to_vec(&envelope)
        .map_err(|_| InlineCompactionError::InvalidState("sidecar could not be encoded"))?;
    if bytes.len() as u64 > MAX_STATE_BYTES {
        return Err(InlineCompactionError::InvalidState(
            "sidecar exceeds the size limit",
        ));
    }
    let old_sidecar_hash = match fs::read(state_path) {
        Ok(previous_bytes) => Some(sha256_hex(&previous_bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(io_error("hashing previous sidecar")(error)),
    };
    let new_sidecar_hash = sha256_hex(&bytes);

    let temp_path = state_dir.join(format!(
        ".inline-compaction-{}-{:016x}.tmp",
        std::process::id(),
        rand::random::<u64>()
    ));
    let state_size = bytes.len() as u64;
    let mut temp_fsynced_at_ms = 0;
    let mut rename_at_ms = 0;
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temp_path)
            .map_err(io_error("creating temporary sidecar"))?;
        log_state_write_event("state_temp_created", state, &new_sidecar_hash);
        maybe_exit_at_test_crash_point("before_temp_write");
        file.write_all(&bytes)
            .map_err(io_error("writing temporary sidecar"))?;
        log_state_write_event("state_temp_written", state, &new_sidecar_hash);
        maybe_exit_at_test_crash_point("after_temp_write");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&temp_path, fs::Permissions::from_mode(0o600))
                .map_err(io_error("setting sidecar permissions"))?;
        }
        maybe_fail_test_io(state_path, "temp_fsync")
            .map_err(io_error("syncing temporary sidecar"))?;
        retry_interrupted(|| file.sync_all()).map_err(io_error("syncing temporary sidecar"))?;
        temp_fsynced_at_ms = now_ms();
        log_state_write_event("state_temp_fsynced", state, &new_sidecar_hash);
        maybe_exit_at_test_crash_point("after_fsync");
        drop(file);
        #[cfg(unix)]
        let state_directory = File::open(state_dir).map_err(io_error("opening state directory"))?;
        maybe_fail_test_io(state_path, "rename").map_err(io_error("committing sidecar"))?;
        fs::rename(&temp_path, state_path).map_err(io_error("committing sidecar"))?;
        rename_at_ms = now_ms();
        log_state_write_event("state_namespace_switched", state, &new_sidecar_hash);
        maybe_exit_at_test_crash_point("after_rename");
        #[cfg(unix)]
        {
            let directory_fsync_started_at_ms = now_ms();
            if let Err(source) = retry_interrupted(|| {
                maybe_fail_test_io(state_path, "directory_fsync")?;
                state_directory.sync_all()
            }) {
                let directory_fsync_failed_at_ms = now_ms();
                let mount = state_dir_mount_metadata(state_dir);
                return Err(InlineCompactionError::DurabilityIndeterminate {
                    details: Box::new(DurabilityIndeterminateDetails {
                        old_revision,
                        candidate_revision: state.updated_at_ms,
                        old_sidecar_hash: old_sidecar_hash.clone(),
                        new_sidecar_hash: new_sidecar_hash.clone(),
                        temp_fsynced_at_ms,
                        rename_at_ms,
                        directory_fsync_started_at_ms,
                        directory_fsync_failed_at_ms,
                        errno: source.raw_os_error(),
                        filesystem_type: mount.filesystem_type,
                        mount_id: mount.mount_id,
                        mount_options: mount.mount_options,
                    }),
                    source,
                });
            }
            log_state_write_event("state_directory_fsynced", state, &new_sidecar_hash);
            log_state_write_event("state_sidecar_committed", state, &new_sidecar_hash);
        }
        #[cfg(not(unix))]
        return Err(InlineCompactionError::StateDurabilityUnsupported {
            source: io::Error::new(
                io::ErrorKind::Unsupported,
                "directory fsync is required for strict durable commits",
            ),
        });
        #[cfg(unix)]
        Ok(state_size)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn log_state_write_event(event: &str, state: &SidecarState, new_sidecar_hash: &str) {
    crate::logging::create_logger("codex_inline_compaction_state").info(
        event,
        Some(serde_json::Map::from_iter([
            ("laneHash".to_string(), serde_json::json!(&state.lane_hash)),
            (
                "candidateRevision".to_string(),
                serde_json::json!(state.updated_at_ms),
            ),
            (
                "newSidecarHash".to_string(),
                serde_json::json!(new_sidecar_hash),
            ),
        ])),
    );
}

#[cfg(test)]
fn maybe_exit_at_test_crash_point(point: &str) {
    if std::env::var("CCP_INLINE_TEST_CRASH_POINT").as_deref() == Ok(point) {
        std::process::exit(86);
    }
}

#[cfg(not(test))]
fn maybe_exit_at_test_crash_point(_point: &str) {}

#[cfg(test)]
fn maybe_fail_test_io(path: &Path, point: &'static str) -> io::Result<()> {
    let mut failure = TEST_WRITE_FAILURE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if failure
        .as_ref()
        .is_some_and(|failure| failure.path == path && failure.point == point)
    {
        let failure = failure.take().expect("matched test write failure");
        return Err(failure.raw_os_error.map_or_else(
            || io::Error::other(format!("injected test failure at {point}")),
            io::Error::from_raw_os_error,
        ));
    }
    Ok(())
}

#[cfg(not(test))]
fn maybe_fail_test_io(_path: &Path, _point: &'static str) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
pub(super) fn lock_test_write_failures() -> std::sync::MutexGuard<'static, ()> {
    TEST_WRITE_FAILURE_SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
pub(super) fn inject_directory_fsync_failure_for_tests(
    prepared: &PreparedInlineCompaction,
    raw_os_error: i32,
) {
    let mut failure = TEST_WRITE_FAILURE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(
        failure.is_none(),
        "another write failure is already injected"
    );
    *failure = Some(TestWriteFailure {
        path: prepared.state_path.clone(),
        point: "directory_fsync",
        raw_os_error: Some(raw_os_error),
    });
}

fn clear_state_path(path: &Path) -> Result<(), InlineCompactionError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => Err(
            InlineCompactionError::InvalidState("sidecar removal target is not a regular file"),
        ),
        Ok(_) => fs::remove_file(path).map_err(io_error("removing stale sidecar")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("inspecting stale sidecar")(error)),
    }
}

fn hash_json<T: Serialize + ?Sized>(value: &T) -> Result<String, InlineCompactionError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| InlineCompactionError::InvalidState("JSON hash input could not be encoded"))?;
    Ok(sha256_hex(&bytes))
}

fn hash_raw_fragments(items: &[String]) -> String {
    let mut hasher = Sha256::new();
    for item in items {
        hasher.update((item.len() as u64).to_be_bytes());
        hasher.update(item.as_bytes());
    }
    bytes_to_hex(&hasher.finalize())
}

fn sha256_hex(bytes: &[u8]) -> String {
    bytes_to_hex(&Sha256::digest(bytes))
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn next_revision(previous: Option<&SidecarState>) -> u64 {
    let wall_clock = now_ms();
    previous.map_or(wall_clock, |state| {
        wall_clock.max(state.updated_at_ms.saturating_add(1))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::providers::codex::translate::request::{
        ResponsesContentPart, ResponsesFunctionCallOutput, ResponsesText, ResponsesToolChoice,
        ResponsesToolChoiceMode,
    };

    struct TestWriteFailureGuard {
        state_path: PathBuf,
        point: &'static str,
    }

    impl Drop for TestWriteFailureGuard {
        fn drop(&mut self) {
            let mut failure = TEST_WRITE_FAILURE
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if failure.as_ref().is_some_and(|failure| {
                failure.path == self.state_path && failure.point == self.point
            }) {
                *failure = None;
            }
        }
    }

    fn fail_write_at(state_path: &Path, point: &'static str) -> TestWriteFailureGuard {
        fail_write_at_with_errno(state_path, point, None)
    }

    fn fail_write_at_with_errno(
        state_path: &Path,
        point: &'static str,
        raw_os_error: Option<i32>,
    ) -> TestWriteFailureGuard {
        let mut failure = TEST_WRITE_FAILURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            failure.is_none(),
            "another write failure is already injected"
        );
        *failure = Some(TestWriteFailure {
            path: state_path.to_path_buf(),
            point,
            raw_os_error,
        });
        TestWriteFailureGuard {
            state_path: state_path.to_path_buf(),
            point,
        }
    }

    fn request(input: Vec<ResponsesInputItem>) -> ResponsesRequest {
        ResponsesRequest {
            model: "gpt-5.6".to_string(),
            instructions: Some("keep continuity".to_string()),
            input,
            tools: None,
            tool_choice: None,
            store: false,
            stream: true,
            parallel_tool_calls: true,
            include: None,
            client_metadata: None,
            service_tier: None,
            prompt_cache_key: Some("session".to_string()),
            text: ResponsesText {
                verbosity: Some("low".to_string()),
                format: None,
            },
            reasoning: None,
            context_management: None,
            raw_input_override: None,
        }
    }

    fn user(text: &str) -> ResponsesInputItem {
        ResponsesInputItem::Message {
            role: "user".to_string(),
            content: vec![ResponsesContentPart::InputText {
                text: text.to_string(),
            }],
        }
    }

    fn assistant(text: &str) -> ResponsesInputItem {
        ResponsesInputItem::Message {
            role: "assistant".to_string(),
            content: vec![ResponsesContentPart::OutputText {
                text: text.to_string(),
            }],
        }
    }

    fn portable_compact_message() -> ResponsesInputItem {
        user(concat!(
            "CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.\n\n",
            "Your task is to create a detailed summary of the conversation so far."
        ))
    }

    fn call() -> ResponsesInputItem {
        ResponsesInputItem::FunctionCall {
            call_id: "call_1".to_string(),
            name: "lookup".to_string(),
            arguments: "{\"x\":1}".to_string(),
        }
    }

    fn call_output() -> ResponsesInputItem {
        ResponsesInputItem::FunctionCallOutput {
            call_id: "call_1".to_string(),
            output: ResponsesFunctionCallOutput::Text("tool-result".to_string()),
        }
    }

    fn reasoning(id: &str, encrypted_content: &str) -> ResponsesInputItem {
        ResponsesInputItem::Reasoning {
            id: id.to_string(),
            summary: Vec::new(),
            encrypted_content: encrypted_content.to_string(),
        }
    }

    fn sse(items: &[(usize, &str)], usage: u64) -> Vec<u8> {
        let mut body = String::new();
        for (index, item) in items {
            body.push_str(&format!(
                "data: {{\"type\":\"response.output_item.done\",\"output_index\":{index},\"item\":{item}}}\n\n"
            ));
        }
        body.push_str(&format!(
            "data: {{\"type\":\"response.completed\",\"response\":{{\"status\":\"completed\",\"reasoning\":{{\"context\":\"all_turns\"}},\"usage\":{{\"input_tokens\":{usage}}}}}}}\n\n"
        ));
        body.into_bytes()
    }

    fn raw_sse(events: &[&str]) -> Vec<u8> {
        let mut body = String::new();
        for event in events {
            body.push_str("data: ");
            body.push_str(event);
            body.push_str("\n\n");
        }
        body.into_bytes()
    }

    #[test]
    fn semantic_fingerprint_normalizes_only_runtime_git_status_entries() {
        let clean = "stable policy\nGit working tree snapshot\n\nCurrent branch: main\n\nMain branch: origin/main\n\nStatus:\n(clean)\n\nstable tail";
        let dirty = "stable policy\nGit working tree snapshot\n\nCurrent branch: main\n\nMain branch: origin/main\n\nStatus:\n M pipeline.py\n?? test_pipeline.py\n\nstable tail";
        let mut clean_request = request(vec![user("history")]);
        clean_request.instructions = Some(clean.to_string());
        let mut dirty_request = request(vec![user("history")]);
        dirty_request.instructions = Some(dirty.to_string());
        assert_eq!(
            request_fingerprint(&clean_request, &[]).unwrap(),
            request_fingerprint(&dirty_request, &[]).unwrap()
        );

        let mut changed_branch = request(vec![user("history")]);
        changed_branch.instructions = Some(dirty.replace("branch: main", "branch: feature"));
        assert_ne!(
            request_fingerprint(&clean_request, &[]).unwrap(),
            request_fingerprint(&changed_branch, &[]).unwrap()
        );

        let mut changed_policy = request(vec![user("history")]);
        changed_policy.instructions = Some(dirty.replace("stable policy", "different policy"));
        assert_ne!(
            request_fingerprint(&clean_request, &[]).unwrap(),
            request_fingerprint(&changed_policy, &[]).unwrap()
        );
    }

    #[test]
    fn edit_default_normalization_is_narrow() {
        let omitted = r#"{"file_path":"pipeline.py","old_string":"before","new_string":"after"}"#;
        let explicit_false = r#"{"new_string":"after","replace_all":false,"old_string":"before","file_path":"pipeline.py"}"#;
        let explicit_true = r#"{"file_path":"pipeline.py","old_string":"before","new_string":"after","replace_all":true}"#;
        let changed_text = r#"{"file_path":"pipeline.py","old_string":"different","new_string":"after","replace_all":false}"#;
        let extra_field = r#"{"file_path":"pipeline.py","old_string":"before","new_string":"after","replace_all":false,"unexpected":0}"#;

        assert!(function_call_arguments_match(
            "Edit",
            Some(omitted),
            Some(explicit_false)
        ));
        assert!(!function_call_arguments_match(
            "Edit",
            Some(omitted),
            Some(explicit_true)
        ));
        assert!(!function_call_arguments_match(
            "Edit",
            Some(omitted),
            Some(changed_text)
        ));
        assert!(!function_call_arguments_match(
            "Edit",
            Some(omitted),
            Some(extra_field)
        ));
        assert!(!function_call_arguments_match(
            "DifferentEditTool",
            Some(omitted),
            Some(explicit_false)
        ));
    }

    #[test]
    fn replays_exact_raw_items_and_closes_a_tool_pair_after_restart() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("project-sentinel")]);
        let prepared = prepare_request(&config, "lane-a", &mut first, false).unwrap();
        assert!(first.raw_input_override.is_none());
        assert_eq!(
            first.context_management,
            Some(vec![ResponsesContextManagement::Compaction {
                compact_threshold: 32_768
            }])
        );

        let compact_raw = r#"{"type":"compaction", "encrypted_content":"opaque-A","future":7}"#;
        let future_raw = r#"{"type":"future_output", "opaque":{"order":[3,2,1]}}"#;
        let call_raw = r#"{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"x\":1}"}"#;
        let outcome = commit_response(
            &prepared,
            &sse(&[(0, compact_raw), (1, future_raw), (2, call_raw)], 40_000),
            &[call()],
        )
        .unwrap();
        assert_eq!(outcome, CommitOutcome::Created);

        let mut second = request(vec![user("project-sentinel"), call(), call_output()]);
        second.tool_choice = Some(ResponsesToolChoice::Mode(ResponsesToolChoiceMode::Required));
        second.parallel_tool_calls = false;
        let prepared_after_restart =
            prepare_request(&config, "lane-a", &mut second, false).unwrap();
        assert!(prepared_after_restart.previous.is_some());
        let wire = second.wire_json_string().unwrap();
        assert!(wire.contains(compact_raw));
        assert!(wire.contains(future_raw));
        assert!(wire.contains(call_raw));
        let wire_value: Value = serde_json::from_str(&wire).unwrap();
        assert_eq!(
            wire_value["input"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "compaction",
                "future_output",
                "function_call",
                "function_call_output"
            ]
        );

        let message_raw = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"continuity-ok"}]}"#;
        assert_eq!(
            commit_response(
                &prepared_after_restart,
                &sse(&[(0, message_raw)], 700),
                &[assistant("continuity-ok")],
            )
            .unwrap(),
            CommitOutcome::Advanced
        );

        let mut third = request(vec![
            user("project-sentinel"),
            call(),
            call_output(),
            assistant("continuity-ok"),
            user("next"),
        ]);
        let prepared_third = prepare_request(&config, "lane-a", &mut third, false).unwrap();
        assert!(prepared_third.previous.is_some());
        let third_wire = third.wire_json_string().unwrap();
        assert!(third_wire.contains(compact_raw));
        assert!(third_wire.contains(future_raw));
        assert!(third_wire.contains(call_raw));
        assert!(third_wire.contains(message_raw));
        let third_value: Value = serde_json::from_str(&third_wire).unwrap();
        assert_eq!(
            third_value["input"].as_array().unwrap().last().unwrap()["content"][0]["text"],
            json!("next")
        );
    }

    #[test]
    fn transcript_branch_discards_the_old_lineage_without_replay() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("original")]);
        let prepared = prepare_request(&config, "lane-a", &mut first, false).unwrap();
        let compact_raw = r#"{"type":"compaction","encrypted_content":"opaque"}"#;
        commit_response(&prepared, &sse(&[(0, compact_raw)], 40_000), &[]).unwrap();

        let mut branch = request(vec![user("different branch")]);
        let prepared_branch = prepare_request(&config, "lane-a", &mut branch, false).unwrap();
        assert!(prepared_branch.previous.is_none());
        assert!(branch.raw_input_override.is_none());
        assert!(!prepared_branch.state_path.exists());
    }

    #[test]
    fn encrypted_reasoning_and_post_compaction_suffix_replay_exactly() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("reasoning history")]);
        let prepared = prepare_request(&config, "lane-r", &mut first, false).unwrap();
        let compact_raw = r#"{"type":"compaction","encrypted_content":"opaque-reasoning"}"#;
        let reasoning_raw = r#"{"type":"reasoning", "id":"rs_1","summary":[],"encrypted_content":"encrypted-reasoning-A"}"#;
        let message_raw = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"reasoned"}]}"#;
        commit_response(
            &prepared,
            &sse(
                &[(0, compact_raw), (1, reasoning_raw), (2, message_raw)],
                40_000,
            ),
            &[
                reasoning("rs_1", "encrypted-reasoning-A"),
                assistant("reasoned"),
            ],
        )
        .unwrap();

        let mut second = request(vec![
            user("reasoning history"),
            reasoning("rs_1", "encrypted-reasoning-A"),
            assistant("reasoned"),
            user("continue"),
        ]);
        let prepared_second = prepare_request(&config, "lane-r", &mut second, false).unwrap();
        assert!(prepared_second.previous.is_some());
        let wire = second.wire_json_string().unwrap();
        assert!(wire.contains(compact_raw));
        assert!(wire.contains(reasoning_raw));
        assert!(wire.contains(message_raw));
        let value: Value = serde_json::from_str(&wire).unwrap();
        assert_eq!(
            value["input"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["compaction", "reasoning", "message", "message"]
        );
    }

    #[test]
    fn omitted_reasoning_and_reserialized_tool_arguments_keep_the_lineage() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("coding history")]);
        let prepared = prepare_request(&config, "lane-coding", &mut first, false).unwrap();
        let compact_raw = r#"{"type":"compaction","encrypted_content":"opaque-coding"}"#;
        let reasoning_raw = r#"{"type":"reasoning","id":"rs_coding","summary":[],"encrypted_content":"encrypted-coding"}"#;
        let call_raw = r#"{"type":"function_call","call_id":"call_coding","name":"Edit","arguments":"{\"file_path\":\"pipeline.py\",\"old_string\":\"before\",\"new_string\":\"after\"}"}"#;
        let visible_call = ResponsesInputItem::FunctionCall {
            call_id: "call_coding".to_string(),
            name: "Edit".to_string(),
            arguments:
                "{\"file_path\":\"pipeline.py\",\"old_string\":\"before\",\"new_string\":\"after\"}"
                    .to_string(),
        };
        commit_response(
            &prepared,
            &sse(
                &[(0, compact_raw), (1, reasoning_raw), (2, call_raw)],
                40_000,
            ),
            &[
                reasoning("rs_coding", "encrypted-coding"),
                visible_call.clone(),
            ],
        )
        .unwrap();

        let echoed_call = ResponsesInputItem::FunctionCall {
            call_id: "call_coding".to_string(),
            name: "Edit".to_string(),
            arguments: "{\"new_string\":\"after\",\"replace_all\":false,\"old_string\":\"before\",\"file_path\":\"pipeline.py\"}".to_string(),
        };
        let output = ResponsesInputItem::FunctionCallOutput {
            call_id: "call_coding".to_string(),
            output: ResponsesFunctionCallOutput::Text("edited".to_string()),
        };
        let mut second = request(vec![user("coding history"), echoed_call, output]);
        let prepared_second = prepare_request(&config, "lane-coding", &mut second, false).unwrap();
        assert!(prepared_second.previous.is_some());
        let wire = second.wire_value().unwrap();
        let input = wire["input"].as_array().unwrap();
        assert_eq!(
            input
                .iter()
                .map(|item| item["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "compaction",
                "reasoning",
                "function_call",
                "function_call_output"
            ]
        );
        assert_eq!(
            input
                .iter()
                .filter(|item| item["type"] == "reasoning")
                .count(),
            1
        );

        let next_call_raw = r#"{"type":"function_call","call_id":"call_next","name":"Bash","arguments":"{\"command\":\"pytest\"}"}"#;
        let next_call = ResponsesInputItem::FunctionCall {
            call_id: "call_next".to_string(),
            name: "Bash".to_string(),
            arguments: "{\"command\":\"pytest\"}".to_string(),
        };
        let report = commit_response_report(
            &prepared_second,
            &sse(&[(0, next_call_raw)], 700),
            std::slice::from_ref(&next_call),
        )
        .unwrap();
        assert_eq!(report.outcome, CommitOutcome::Advanced);

        let historical_call_reformatted = ResponsesInputItem::FunctionCall {
            call_id: "call_coding".to_string(),
            name: "Edit".to_string(),
            arguments: "{\n  \"new_string\": \"after\",\n  \"old_string\": \"before\",\n  \"file_path\": \"pipeline.py\"\n}"
                .to_string(),
        };
        let historical_output = ResponsesInputItem::FunctionCallOutput {
            call_id: "call_coding".to_string(),
            output: ResponsesFunctionCallOutput::Text("edited".to_string()),
        };
        let next_output = ResponsesInputItem::FunctionCallOutput {
            call_id: "call_next".to_string(),
            output: ResponsesFunctionCallOutput::Text("passed".to_string()),
        };
        let mut third = request(vec![
            user("coding history"),
            historical_call_reformatted,
            historical_output,
            next_call,
            next_output,
        ]);
        let prepared_third = prepare_request(&config, "lane-coding", &mut third, false).unwrap();
        assert!(prepared_third.previous.is_some());
        assert!(
            third.wire_value().unwrap()["input"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["call_id"] == "call_next")
        );
    }

    #[test]
    fn explicit_true_edit_default_still_changes_the_tool_branch() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("edit branch")]);
        let prepared = prepare_request(&config, "lane-edit-true", &mut first, false).unwrap();
        let compact_raw = r#"{"type":"compaction","encrypted_content":"opaque-edit"}"#;
        let call_raw = r#"{"type":"function_call","call_id":"call_edit","name":"Edit","arguments":"{\"file_path\":\"pipeline.py\",\"old_string\":\"before\",\"new_string\":\"after\"}"}"#;
        let expected_call = ResponsesInputItem::FunctionCall {
            call_id: "call_edit".to_string(),
            name: "Edit".to_string(),
            arguments:
                "{\"file_path\":\"pipeline.py\",\"old_string\":\"before\",\"new_string\":\"after\"}"
                    .to_string(),
        };
        commit_response(
            &prepared,
            &sse(&[(0, compact_raw), (1, call_raw)], 40_000),
            &[expected_call],
        )
        .unwrap();

        let changed_call = ResponsesInputItem::FunctionCall {
            call_id: "call_edit".to_string(),
            name: "Edit".to_string(),
            arguments: "{\"file_path\":\"pipeline.py\",\"old_string\":\"before\",\"new_string\":\"after\",\"replace_all\":true}"
                .to_string(),
        };
        let mut branch = request(vec![user("edit branch"), changed_call]);
        let prepared_branch =
            prepare_request(&config, "lane-edit-true", &mut branch, false).unwrap();
        assert!(prepared_branch.previous.is_none());
        assert!(branch.raw_input_override.is_none());
    }

    #[test]
    fn omitted_reasoning_cannot_anchor_a_different_tool_call_branch() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("branch history")]);
        let prepared = prepare_request(&config, "lane-branch-tool", &mut first, false).unwrap();
        let compact_raw = r#"{"type":"compaction","encrypted_content":"opaque-branch"}"#;
        let call_raw =
            r#"{"type":"function_call","call_id":"call_expected","name":"Read","arguments":"{}"}"#;
        let expected_call = ResponsesInputItem::FunctionCall {
            call_id: "call_expected".to_string(),
            name: "Read".to_string(),
            arguments: "{}".to_string(),
        };
        commit_response(
            &prepared,
            &sse(&[(0, compact_raw), (1, call_raw)], 40_000),
            &[reasoning("rs_branch", "encrypted-branch"), expected_call],
        )
        .unwrap();

        let different_call = ResponsesInputItem::FunctionCall {
            call_id: "call_different".to_string(),
            name: "Read".to_string(),
            arguments: "{}".to_string(),
        };
        let mut branch = request(vec![user("branch history"), different_call]);
        let prepared_branch =
            prepare_request(&config, "lane-branch-tool", &mut branch, false).unwrap();
        assert!(prepared_branch.previous.is_none());
        assert!(branch.raw_input_override.is_none());
    }

    #[test]
    fn multiple_compactions_keep_only_the_latest_item_and_its_suffix() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("multi-compaction history")]);
        let prepared = prepare_request(&config, "lane-multi", &mut first, false).unwrap();
        let compact_a = r#"{"type":"compaction","encrypted_content":"opaque-a"}"#;
        let subsumed_message = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"subsumed"}]}"#;
        let compact_b = r#"{"type":"compaction", "encrypted_content":"opaque-b","future":9}"#;
        let reasoning_raw = r#"{"type":"reasoning","id":"rs_latest","summary":[],"encrypted_content":"reasoning-latest"}"#;
        let suffix_message = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"latest"}]}"#;
        let call_raw = r#"{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"x\":1}"}"#;

        let report = commit_response_report(
            &prepared,
            &sse(
                &[
                    (0, compact_a),
                    (1, subsumed_message),
                    (2, compact_b),
                    (3, reasoning_raw),
                    (4, suffix_message),
                    (5, call_raw),
                ],
                40_000,
            ),
            &[assistant("subsumed"), assistant("latest"), call()],
        )
        .unwrap();
        assert_eq!(report.outcome, CommitOutcome::Created);
        assert_eq!(report.compaction_count, 2);
        assert_eq!(report.last_item_index, Some(2));
        assert_eq!(report.suffix_count, 3);
        assert_eq!(report.usage_input_tokens, 40_000);
        assert!(report.state_bytes > 0);
        assert_eq!(report.lane_hash.len(), 64);
        assert_eq!(
            report.effective_reasoning_context.as_deref(),
            Some("all_turns")
        );

        let mut second = request(vec![
            user("multi-compaction history"),
            assistant("subsumed"),
            assistant("latest"),
            call(),
            call_output(),
            user("continue"),
        ]);
        let prepared_second = prepare_request(&config, "lane-multi", &mut second, false).unwrap();
        assert!(prepared_second.previous.is_some());
        let wire = second.wire_json_string().unwrap();
        assert!(!wire.contains("opaque-a"));
        assert!(!wire.contains("subsumed"));
        assert!(wire.contains(compact_b));
        assert!(wire.contains(reasoning_raw));
        assert!(wire.contains(suffix_message));
        assert!(wire.contains(call_raw));

        let wire_value: Value = serde_json::from_str(&wire).unwrap();
        assert_eq!(
            wire_value["input"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "compaction",
                "reasoning",
                "message",
                "function_call",
                "function_call_output",
                "message"
            ]
        );
        assert_eq!(
            wire_value["input"].as_array().unwrap().last().unwrap()["content"][0]["text"],
            json!("continue")
        );
    }

    #[test]
    fn output_indexes_define_replay_order_and_parallel_tool_pairs_stay_closed() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("parallel history")]);
        let prepared = prepare_request(&config, "lane-parallel", &mut first, false).unwrap();
        let compact = r#"{"type":"compaction","encrypted_content":"opaque-parallel"}"#;
        let call_a = r#"{"type":"function_call","call_id":"call_a","name":"lookup","arguments":"{\"x\":1}"}"#;
        let call_b = r#"{"type":"function_call","call_id":"call_b","name":"lookup","arguments":"{\"x\":2}"}"#;
        let typed_call_a = ResponsesInputItem::FunctionCall {
            call_id: "call_a".to_string(),
            name: "lookup".to_string(),
            arguments: "{\"x\":1}".to_string(),
        };
        let typed_call_b = ResponsesInputItem::FunctionCall {
            call_id: "call_b".to_string(),
            name: "lookup".to_string(),
            arguments: "{\"x\":2}".to_string(),
        };

        let report = commit_response_report(
            &prepared,
            &sse(&[(2, call_b), (0, compact), (1, call_a)], 40_000),
            &[typed_call_a.clone(), typed_call_b.clone()],
        )
        .unwrap();
        assert_eq!(report.last_item_index, Some(0));
        assert_eq!(report.suffix_count, 2);

        let output_a = ResponsesInputItem::FunctionCallOutput {
            call_id: "call_a".to_string(),
            output: ResponsesFunctionCallOutput::Text("result-a".to_string()),
        };
        let output_b = ResponsesInputItem::FunctionCallOutput {
            call_id: "call_b".to_string(),
            output: ResponsesFunctionCallOutput::Text("result-b".to_string()),
        };
        let mut second = request(vec![
            user("parallel history"),
            typed_call_a,
            typed_call_b,
            output_a,
            output_b,
            user("continue"),
        ]);
        prepare_request(&config, "lane-parallel", &mut second, false).unwrap();
        let wire = second.wire_value().unwrap();
        let input = wire["input"].as_array().unwrap();
        assert_eq!(
            input
                .iter()
                .map(|item| item["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "compaction",
                "function_call",
                "function_call",
                "function_call_output",
                "function_call_output",
                "message"
            ]
        );
        assert_eq!(input[1]["call_id"], "call_a");
        assert_eq!(input[2]["call_id"], "call_b");
        assert_eq!(input[3]["call_id"], "call_a");
        assert_eq!(input[4]["call_id"], "call_b");
    }

    #[test]
    fn malformed_upstream_protocol_matrix_fails_closed() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut request_body = request(vec![user("protocol")]);
        let prepared = prepare_request(&config, "lane-protocol", &mut request_body, false).unwrap();
        let item = r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"message","role":"assistant","content":[]}}"#;
        let completed_low = r#"{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":100}}}"#;
        let missing_usage =
            r#"{"type":"response.completed","response":{"status":"completed","usage":{}}}"#;
        let failed_completed = r#"{"type":"response.completed","response":{"status":"failed","usage":{"input_tokens":100}}}"#;
        let cases = vec![
            raw_sse(&[r#"{"type":"response.failed"}"#]),
            raw_sse(&[r#"{"type":"response.incomplete"}"#]),
            raw_sse(&[failed_completed]),
            raw_sse(&[item]),
            raw_sse(&[missing_usage]),
            raw_sse(&[completed_low, completed_low]),
            raw_sse(&[completed_low, item]),
            raw_sse(&[item, item, completed_low]),
            raw_sse(&[
                r#"{"type":"response.output_item.done","item":{"type":"message","role":"assistant","content":[]}}"#,
                completed_low,
            ]),
        ];
        for body in cases {
            assert!(matches!(
                commit_response(&prepared, &body, &[]),
                Err(InlineCompactionError::InvalidUpstream(_))
            ));
        }
    }

    #[test]
    fn accepts_compaction_with_post_prune_usage_below_threshold() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut request_body = request(vec![user("portable compact history")]);
        let prepared =
            prepare_request(&config, "lane-post-prune", &mut request_body, false).unwrap();
        let compact = r#"{"type":"compaction","encrypted_content":"opaque-post-prune"}"#;

        let report = commit_response_report(&prepared, &sse(&[(0, compact)], 14_000), &[])
            .expect("post-prune usage below the trigger threshold is valid");

        assert_eq!(report.outcome, CommitOutcome::Created);
        assert_eq!(report.compaction_count, 1);
        assert_eq!(report.usage_input_tokens, 14_000);
    }

    #[test]
    fn missing_compaction_preserves_previous_state_but_missing_usage_fails_closed() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("input")]);
        let prepared = prepare_request(&config, "lane-a", &mut first, false).unwrap();
        let no_state = commit_response_report(&prepared, &sse(&[], 40_000), &[]).unwrap();
        assert_eq!(no_state.outcome, CommitOutcome::NoState);
        assert_eq!(
            no_state.anomaly_reason,
            Some("threshold_crossed_without_compaction")
        );
        assert!(!prepared.state_path.exists());

        let compact_a = r#"{"type":"compaction","encrypted_content":"opaque-a"}"#;
        commit_response(&prepared, &sse(&[(0, compact_a)], 40_000), &[]).unwrap();
        let committed_state = fs::read(&prepared.state_path).unwrap();

        let mut second = request(vec![user("input"), user("next")]);
        let prepared_second = prepare_request(&config, "lane-a", &mut second, false).unwrap();
        assert!(prepared_second.previous.is_some());
        let message = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}"#;
        let preserved =
            commit_response_report(&prepared_second, &sse(&[(0, message)], 40_000), &[]).unwrap();
        assert_eq!(preserved.outcome, CommitOutcome::Preserved);
        assert_eq!(
            preserved.anomaly_reason,
            Some("threshold_crossed_without_compaction")
        );
        assert_eq!(fs::read(&prepared.state_path).unwrap(), committed_state);

        let missing_usage = b"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{}}}\n\n";
        assert!(matches!(
            commit_response(&prepared_second, missing_usage, &[]),
            Err(InlineCompactionError::InvalidUpstream(_))
        ));

        let incomplete_message = r#"{"type":"message"}"#;
        assert!(matches!(
            commit_response(
                &prepared,
                &sse(&[(0, compact_a), (1, incomplete_message)], 40_000),
                &[]
            ),
            Err(InlineCompactionError::InvalidState(_))
        ));
    }

    #[tokio::test]
    async fn same_lane_concurrency_waits_while_different_lanes_proceed() {
        let locks = Arc::new(LaneLocks::default());
        let first = locks.lock("lane-a").await.unwrap();
        let waiting_locks = locks.clone();
        let waiting = tokio::spawn(async move { waiting_locks.lock("lane-a").await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        assert!(locks.lock("lane-b").await.is_ok());
        drop(first);
        assert!(waiting.await.unwrap().is_ok());
        assert!(locks.lock("lane-a").await.is_ok());
    }

    #[test]
    fn state_directory_has_one_process_lifetime_writer() {
        let temp = tempfile::TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        let first = acquire_state_dir_writer_lock(&state_dir).unwrap();
        assert!(first.capability().directory_fsync_supported);
        assert!(fs::read_dir(&state_dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(DURABILITY_PROBE_PREFIX)
        }));
        assert!(matches!(
            acquire_state_dir_writer_lock(&state_dir),
            Err(InlineCompactionError::StateDirWriterLocked)
        ));
        drop(first);
        assert!(acquire_state_dir_writer_lock(&state_dir).is_ok());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn state_directory_probe_retries_eintr_and_rejects_einval() {
        let _failure_serial = TEST_WRITE_FAILURE_SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let temp = tempfile::TempDir::new().unwrap();
        let retry_dir = temp.path().join("retry");
        ensure_state_dir(&retry_dir).unwrap();
        let interrupted = fail_write_at_with_errno(
            &retry_dir,
            "probe_directory_fsync",
            Some(4), // EINTR
        );
        let writer = acquire_state_dir_writer_lock(&retry_dir).unwrap();
        drop(interrupted);
        assert!(writer.capability().directory_fsync_supported);
        drop(writer);

        let unsupported_dir = temp.path().join("unsupported");
        ensure_state_dir(&unsupported_dir).unwrap();
        let unsupported = fail_write_at_with_errno(
            &unsupported_dir,
            "probe_directory_fsync",
            Some(22), // EINVAL
        );
        assert!(matches!(
            acquire_state_dir_writer_lock(&unsupported_dir),
            Err(InlineCompactionError::StateDurabilityUnsupported { .. })
        ));
        drop(unsupported);
        assert!(fs::read_dir(&unsupported_dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(DURABILITY_PROBE_PREFIX)
        }));
    }

    #[test]
    fn agent_lanes_are_distinct_while_parent_lane_stays_compatible() {
        assert_eq!(
            state_lane_id(Some("session-a"), None).as_deref(),
            Some("session-a")
        );
        let parent = state_lane_id(Some("session-a"), None).unwrap();
        let agent_a = state_lane_id(Some("session-a"), Some("agent-a")).unwrap();
        let agent_b = state_lane_id(Some("session-a"), Some("agent-b")).unwrap();
        assert_ne!(parent, agent_a);
        assert_ne!(agent_a, agent_b);
        assert_eq!(
            agent_a.as_bytes(),
            b"session-a\0agent-a",
            "the separator is impossible in an HTTP header value"
        );
        assert!(state_lane_id(None, Some("agent-a")).is_none());
    }

    #[test]
    fn agent_lanes_persist_and_replay_only_their_own_sidecars() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let lane_a = state_lane_id(Some("session-a"), Some("agent-a")).unwrap();
        let lane_b = state_lane_id(Some("session-a"), Some("agent-b")).unwrap();

        let mut first_a = request(vec![user("shared history")]);
        let mut first_b = request(vec![user("shared history")]);
        let prepared_a = prepare_request(&config, &lane_a, &mut first_a, false).unwrap();
        let prepared_b = prepare_request(&config, &lane_b, &mut first_b, false).unwrap();
        assert_ne!(prepared_a.state_path, prepared_b.state_path);

        let compact_a = r#"{"type":"compaction","encrypted_content":"agent-a-only"}"#;
        let compact_b = r#"{"type":"compaction","encrypted_content":"agent-b-only"}"#;
        let message_a = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer-a"}]}"#;
        let message_b = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer-b"}]}"#;
        commit_response(
            &prepared_a,
            &sse(&[(0, compact_a), (1, message_a)], 40_000),
            &[assistant("answer-a")],
        )
        .unwrap();
        commit_response(
            &prepared_b,
            &sse(&[(0, compact_b), (1, message_b)], 40_000),
            &[assistant("answer-b")],
        )
        .unwrap();

        let mut next_a = request(vec![
            user("shared history"),
            assistant("answer-a"),
            user("continue-a"),
        ]);
        let mut next_b = request(vec![
            user("shared history"),
            assistant("answer-b"),
            user("continue-b"),
        ]);
        prepare_request(&config, &lane_a, &mut next_a, false).unwrap();
        prepare_request(&config, &lane_b, &mut next_b, false).unwrap();
        let wire_a = next_a.wire_json_string().unwrap();
        let wire_b = next_b.wire_json_string().unwrap();
        assert!(wire_a.contains("agent-a-only"));
        assert!(wire_a.contains("continue-a"));
        assert!(!wire_a.contains("agent-b-only"));
        assert!(wire_b.contains("agent-b-only"));
        assert!(wire_b.contains("continue-b"));
        assert!(!wire_b.contains("agent-a-only"));
    }

    #[test]
    fn corrupt_sidecar_fails_closed() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("input")]);
        let prepared = prepare_request(&config, "lane-a", &mut first, false).unwrap();
        let compact_raw = r#"{"type":"compaction","encrypted_content":"opaque"}"#;
        commit_response(&prepared, &sse(&[(0, compact_raw)], 40_000), &[]).unwrap();
        fs::write(&prepared.state_path, b"not-json").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&prepared.state_path, fs::Permissions::from_mode(0o600)).unwrap();
        }

        let mut next = request(vec![user("input"), user("next")]);
        assert!(matches!(
            prepare_request(&config, "lane-a", &mut next, false),
            Err(InlineCompactionError::InvalidState(_))
        ));
    }

    #[test]
    fn portable_compact_boundary_starts_a_new_lineage() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("input")]);
        let prepared = prepare_request(&config, "lane-a", &mut first, false).unwrap();
        let compact_raw = r#"{"type":"compaction","encrypted_content":"opaque"}"#;
        commit_response(&prepared, &sse(&[(0, compact_raw)], 40_000), &[]).unwrap();
        let old_state = fs::read(&prepared.state_path).unwrap();

        let mut portable = request(vec![user("input"), user("portable summary")]);
        portable.instructions = Some("portable summary instructions".to_string());
        let prepared_portable = prepare_request(&config, "lane-a", &mut portable, true).unwrap();
        assert!(prepared_portable.previous.is_some());
        let replay = portable.raw_input_override.as_ref().unwrap();
        assert_eq!(replay.len(), 2);
        assert_eq!(
            serde_json::from_str::<Value>(&replay[0]).unwrap()["type"],
            "compaction"
        );
        assert_eq!(
            serde_json::from_str::<Value>(&replay[1]).unwrap()["type"],
            "message"
        );
        assert_eq!(fs::read(&prepared_portable.state_path).unwrap(), old_state);

        let portable_compaction =
            r#"{"type":"compaction","encrypted_content":"portable-only-opaque"}"#;
        let report = commit_response_report(
            &prepared_portable,
            &sse(&[(0, portable_compaction)], 40_000),
            &[],
        )
        .unwrap();
        assert_eq!(report.outcome, CommitOutcome::NoState);
        assert_eq!(report.compaction_count, 1);
        assert_eq!(
            report.invalidation_reason,
            Some("portable_compact_new_lineage")
        );
        assert!(!prepared_portable.state_path.exists());
    }

    #[test]
    fn portable_compact_replays_only_the_retained_history_before_replaced_tail() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let compact_raw = r#"{"type":"compaction","encrypted_content":"opaque-history"}"#;
        let retained_output_raw = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"retained-output"}]}"#;
        let omitted_output_raw = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"omitted-output"}]}"#;

        let mut first = request(vec![user("old-history")]);
        let prepared_first =
            prepare_request(&config, "lane-portable-tail", &mut first, false).unwrap();
        commit_response(
            &prepared_first,
            &sse(&[(0, compact_raw), (1, retained_output_raw)], 40_000),
            &[assistant("retained-output")],
        )
        .unwrap();

        let mut latest = request(vec![
            user("old-history"),
            assistant("retained-output"),
            user("omitted-user"),
        ]);
        let prepared_latest =
            prepare_request(&config, "lane-portable-tail", &mut latest, false).unwrap();
        commit_response(
            &prepared_latest,
            &sse(&[(0, omitted_output_raw)], 1_000),
            &[assistant("omitted-output")],
        )
        .unwrap();
        let old_state = fs::read(&prepared_latest.state_path).unwrap();

        // Real Claude Code portable compaction keeps the older history, drops
        // the latest user/assistant pair, and inserts one compact instruction.
        let mut portable = request(vec![
            user("old-history"),
            assistant("retained-output"),
            portable_compact_message(),
        ]);
        portable.instructions = Some("portable summary instructions".to_string());
        let prepared_portable =
            prepare_request(&config, "lane-portable-tail", &mut portable, true).unwrap();
        assert!(prepared_portable.previous.is_some());
        assert_eq!(fs::read(&prepared_portable.state_path).unwrap(), old_state);

        let wire: Value = serde_json::from_str(&portable.wire_json_string().unwrap()).unwrap();
        let input = wire["input"].as_array().unwrap();
        assert_eq!(input.len(), 3);
        assert_eq!(input[0]["type"], json!("compaction"));
        assert_eq!(input[1]["content"][0]["text"], json!("retained-output"));
        assert!(is_portable_compact_message(&input[2]));
        let encoded = serde_json::to_string(input).unwrap();
        assert!(!encoded.contains("omitted-user"));
        assert!(!encoded.contains("omitted-output"));

        let portable_compaction =
            r#"{"type":"compaction","encrypted_content":"portable-only-opaque"}"#;
        let report = commit_response_report(
            &prepared_portable,
            &sse(&[(0, portable_compaction)], 40_000),
            &[],
        )
        .unwrap();
        assert_eq!(report.outcome, CommitOutcome::NoState);
        assert!(!prepared_portable.state_path.exists());
    }

    #[test]
    fn portable_compact_tail_reconciliation_fails_closed_on_changed_history() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("original-history")]);
        let prepared_first =
            prepare_request(&config, "lane-portable-branch", &mut first, false).unwrap();
        let compact_raw = r#"{"type":"compaction","encrypted_content":"opaque-branch"}"#;
        let retained_output_raw = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"retained-output"}]}"#;
        commit_response(
            &prepared_first,
            &sse(&[(0, compact_raw), (1, retained_output_raw)], 40_000),
            &[assistant("retained-output")],
        )
        .unwrap();

        let mut latest = request(vec![
            user("original-history"),
            assistant("retained-output"),
            user("latest-user"),
        ]);
        let prepared_latest =
            prepare_request(&config, "lane-portable-branch", &mut latest, false).unwrap();
        let latest_output_raw = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"latest-output"}]}"#;
        commit_response(
            &prepared_latest,
            &sse(&[(0, latest_output_raw)], 1_000),
            &[assistant("latest-output")],
        )
        .unwrap();

        let mut divergent = request(vec![
            user("different-history"),
            assistant("retained-output"),
            portable_compact_message(),
        ]);
        divergent.instructions = Some("portable summary instructions".to_string());
        let prepared_divergent =
            prepare_request(&config, "lane-portable-branch", &mut divergent, true).unwrap();
        assert!(prepared_divergent.previous.is_none());
        assert!(divergent.raw_input_override.is_none());
        assert_eq!(
            prepared_divergent.state_invalidation_reason,
            Some("transcript_mismatch")
        );
        assert!(!prepared_divergent.state_path.exists());
    }

    #[test]
    fn portable_compact_failure_keeps_previous_state_for_retry() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("input")]);
        let prepared = prepare_request(&config, "lane-a", &mut first, false).unwrap();
        let compact_raw = r#"{"type":"compaction","encrypted_content":"opaque-before-retry"}"#;
        commit_response(&prepared, &sse(&[(0, compact_raw)], 40_000), &[]).unwrap();
        let old_state = fs::read(&prepared.state_path).unwrap();

        let mut portable = request(vec![user("input"), user("portable summary")]);
        portable.instructions = Some("portable summary instructions".to_string());
        let prepared_portable = prepare_request(&config, "lane-a", &mut portable, true).unwrap();
        assert!(prepared_portable.previous.is_some());
        assert!(
            portable
                .wire_json_string()
                .unwrap()
                .contains("opaque-before-retry")
        );
        drop(prepared_portable);
        assert_eq!(fs::read(&prepared.state_path).unwrap(), old_state);

        let mut retry = request(vec![user("input"), user("portable summary")]);
        retry.instructions = Some("portable summary instructions".to_string());
        let prepared_retry = prepare_request(&config, "lane-a", &mut retry, true).unwrap();
        assert!(prepared_retry.previous.is_some());
        assert!(
            retry
                .wire_json_string()
                .unwrap()
                .contains("opaque-before-retry")
        );
        assert_eq!(fs::read(&prepared.state_path).unwrap(), old_state);
    }

    fn seed_crash_recovery_state(state_dir: &Path) {
        let config = InlineCompactionConfig::for_tests(state_dir.to_path_buf(), 32_768);
        let mut first = request(vec![user("crash history")]);
        let prepared = prepare_request(&config, "lane-crash", &mut first, false).unwrap();
        let compact = r#"{"type":"compaction","encrypted_content":"opaque-before-crash"}"#;
        let first_message = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"first-output"}]}"#;
        commit_response(
            &prepared,
            &sse(&[(0, compact), (1, first_message)], 40_000),
            &[assistant("first-output")],
        )
        .unwrap();
    }

    #[test]
    #[ignore = "child process used by crash_recovery_matrix"]
    fn crash_commit_child() {
        let state_dir = PathBuf::from(
            std::env::var_os("CCP_INLINE_TEST_STATE_DIR").expect("missing child state dir"),
        );
        let config = InlineCompactionConfig::for_tests(state_dir, 32_768);
        let mut second = request(vec![
            user("crash history"),
            assistant("first-output"),
            user("second-turn"),
        ]);
        let prepared = prepare_request(&config, "lane-crash", &mut second, false).unwrap();
        let second_message = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"second-output"}]}"#;
        let report = commit_response_report(
            &prepared,
            &sse(&[(0, second_message)], 700),
            &[assistant("second-output")],
        )
        .unwrap();
        assert_eq!(report.outcome, CommitOutcome::Advanced);
        maybe_exit_at_test_crash_point("before_downstream_return");
        panic!("crash child reached the end without terminating");
    }

    #[test]
    fn crash_recovery_matrix_preserves_or_safely_discards_lineage() {
        let points = [
            "before_temp_write",
            "after_temp_write",
            "after_fsync",
            "after_rename",
            "before_downstream_return",
        ];
        for point in points {
            let temp = tempfile::TempDir::new().unwrap();
            let state_dir = temp.path().join("state");
            seed_crash_recovery_state(&state_dir);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--ignored")
                .arg("--exact")
                .arg("providers::codex::inline_compaction::tests::crash_commit_child")
                .arg("--nocapture")
                .env("CCP_INLINE_TEST_STATE_DIR", &state_dir)
                .env("CCP_INLINE_TEST_CRASH_POINT", point)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "crash point {point}");

            let process_lock = acquire_state_dir_writer_lock(&state_dir).unwrap();
            assert!(
                fs::read_dir(&state_dir).unwrap().all(|entry| {
                    let name = entry.unwrap().file_name();
                    let name = name.to_string_lossy();
                    !name.starts_with(TEMP_SIDECAR_PREFIX) || !name.ends_with(TEMP_SIDECAR_SUFFIX)
                }),
                "orphan temporary sidecar remained after {point}"
            );
            drop(process_lock);

            let config = InlineCompactionConfig::for_tests(state_dir, 32_768);
            let mut retry = request(vec![
                user("crash history"),
                assistant("first-output"),
                user("second-turn"),
            ]);
            let prepared = prepare_request(&config, "lane-crash", &mut retry, false).unwrap();
            if matches!(
                point,
                "before_temp_write" | "after_temp_write" | "after_fsync"
            ) {
                assert!(prepared.previous.is_some(), "old state lost at {point}");
                let replay = retry.wire_json_string().unwrap();
                assert!(replay.contains("opaque-before-crash"));
                assert!(replay.contains("second-turn"));
            } else {
                assert!(prepared.previous.is_none(), "new state replayed at {point}");
                assert!(retry.raw_input_override.is_none());
                assert_eq!(
                    prepared.state_invalidation_reason,
                    Some("transcript_mismatch")
                );
            }
        }
    }

    #[test]
    fn temporary_sidecar_fsync_failure_preserves_previous_state_bytes() {
        let _failure_serial = TEST_WRITE_FAILURE_SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("durable history")]);
        let prepared = prepare_request(&config, "lane-fsync", &mut first, false).unwrap();
        let compact = r#"{"type":"compaction","encrypted_content":"durable-old"}"#;
        let first_message = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"first-output"}]}"#;
        commit_response(
            &prepared,
            &sse(&[(0, compact), (1, first_message)], 40_000),
            &[assistant("first-output")],
        )
        .unwrap();
        let old_state = fs::read(&prepared.state_path).unwrap();

        let mut second = request(vec![
            user("durable history"),
            assistant("first-output"),
            user("second-turn"),
        ]);
        let prepared_second = prepare_request(&config, "lane-fsync", &mut second, false).unwrap();
        let second_message = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"second-output"}]}"#;
        let failure_guard = fail_write_at(&prepared_second.state_path, "temp_fsync");
        let error = commit_response_report(
            &prepared_second,
            &sse(&[(0, second_message)], 700),
            &[assistant("second-output")],
        )
        .unwrap_err();
        drop(failure_guard);

        assert!(matches!(
            error,
            InlineCompactionError::Io {
                operation: "syncing temporary sidecar",
                ..
            }
        ));
        assert_eq!(fs::read(&prepared_second.state_path).unwrap(), old_state);
        assert!(fs::read_dir(&config.state_dir).unwrap().all(|entry| {
            let name = entry.unwrap().file_name();
            let name = name.to_string_lossy();
            !name.starts_with(TEMP_SIDECAR_PREFIX) || !name.ends_with(TEMP_SIDECAR_SUFFIX)
        }));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn directory_fsync_failure_is_indeterminate_and_requires_exact_recovery() {
        let _failure_serial = TEST_WRITE_FAILURE_SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("durable history")]);
        let prepared = prepare_request(&config, "lane-dir-fsync", &mut first, false).unwrap();
        let compact = r#"{"type":"compaction","encrypted_content":"durable-old"}"#;
        let first_message = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"first-output"}]}"#;
        commit_response(
            &prepared,
            &sse(&[(0, compact), (1, first_message)], 40_000),
            &[assistant("first-output")],
        )
        .unwrap();
        let old_state_bytes = fs::read(&prepared.state_path).unwrap();

        let mut second = request(vec![
            user("durable history"),
            assistant("first-output"),
            user("second-turn"),
        ]);
        let prepared_second =
            prepare_request(&config, "lane-dir-fsync", &mut second, false).unwrap();
        let old_revision = prepared_second
            .previous
            .as_ref()
            .expect("old committed state")
            .updated_at_ms;
        let second_message = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"second-output"}]}"#;
        let failure_guard = fail_write_at_with_errno(
            &prepared_second.state_path,
            "directory_fsync",
            Some(5), // EIO
        );
        let error = commit_response_report(
            &prepared_second,
            &sse(&[(0, second_message)], 700),
            &[assistant("second-output")],
        )
        .unwrap_err();
        drop(failure_guard);

        let details = error
            .durability_details()
            .expect("post-rename directory fsync failure must be distinct");
        assert_eq!(details.old_revision, Some(old_revision));
        assert!(details.candidate_revision > old_revision);
        assert_eq!(details.errno, Some(5));
        assert!(details.temp_fsynced_at_ms > 0);
        assert!(details.rename_at_ms >= details.temp_fsynced_at_ms);
        assert!(details.directory_fsync_failed_at_ms >= details.directory_fsync_started_at_ms);
        assert!(error.degrades_state_backend());
        assert_eq!(
            prepared_second
                .previous
                .as_ref()
                .expect("old committed state remains in request memory")
                .updated_at_ms,
            old_revision
        );

        let current_state_bytes = fs::read(&prepared_second.state_path).unwrap();
        assert_ne!(current_state_bytes, old_state_bytes);
        let current_state = read_state(&prepared_second.state_path)
            .unwrap()
            .expect("namespace now exposes candidate state");
        assert_eq!(current_state.updated_at_ms, details.candidate_revision);
        assert!(
            current_state
                .raw_suffix_json
                .iter()
                .any(|item| item.contains("second-output"))
        );

        let registry = StateRecoveryRegistry::default();
        registry.mark_from_error(prepared_second.lane_hash(), &error);
        assert!(registry.recovery_required(prepared_second.lane_hash()));
        assert!(registry.backend_degraded());

        // Claude did not accept a success terminal, so it retries with the old
        // transcript. Recovery mode must not replay the visible candidate.
        let mut old_transcript = request(vec![
            user("durable history"),
            assistant("first-output"),
            user("second-turn"),
        ]);
        let recovery = prepare_request_with_recovery(
            &config,
            "lane-dir-fsync",
            &mut old_transcript,
            false,
            registry.recovery_required(prepared_second.lane_hash()),
        )
        .unwrap();
        assert!(recovery.previous.is_none());
        assert!(old_transcript.raw_input_override.is_none());
        assert_eq!(
            recovery.state_invalidation_reason,
            Some("state_recovery_required")
        );
        let recovery_wire = old_transcript.wire_json_string().unwrap();
        assert!(recovery_wire.contains("second-turn"));
        assert!(!recovery_wire.contains("durable-old"));
        assert!(!recovery_wire.contains("second-output"));
        assert_eq!(fs::read(&recovery.state_path).unwrap(), current_state_bytes);

        let recovered_compact = r#"{"type":"compaction","encrypted_content":"durable-recovered"}"#;
        let recovered_message = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"recovered-output"}]}"#;
        let recovered_report = commit_response_report(
            &recovery,
            &sse(&[(0, recovered_compact), (1, recovered_message)], 40_000),
            &[assistant("recovered-output")],
        )
        .unwrap();
        assert_eq!(recovered_report.outcome, CommitOutcome::Created);
        registry.note_durable_commit(&recovered_report);
        assert!(!registry.recovery_required(prepared_second.lane_hash()));
    }

    #[test]
    fn restart_outcomes_use_transcript_fingerprint_before_replay() {
        fn restore_state(path: &Path, bytes: &[u8]) {
            fs::write(path, bytes).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
            }
        }

        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("restart history")]);
        let prepared =
            prepare_request(&config, "lane-restart-outcomes", &mut first, false).unwrap();
        let compact = r#"{"type":"compaction","encrypted_content":"restart-old"}"#;
        let first_message = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"first-output"}]}"#;
        commit_response(
            &prepared,
            &sse(&[(0, compact), (1, first_message)], 40_000),
            &[assistant("first-output")],
        )
        .unwrap();
        let old_state = fs::read(&prepared.state_path).unwrap();

        let old_transcript_items = || {
            vec![
                user("restart history"),
                assistant("first-output"),
                user("second-turn"),
            ]
        };
        let mut second = request(old_transcript_items());
        let prepared_second =
            prepare_request(&config, "lane-restart-outcomes", &mut second, false).unwrap();
        let second_message = r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"second-output"}]}"#;
        commit_response(
            &prepared_second,
            &sse(&[(0, second_message)], 700),
            &[assistant("second-output")],
        )
        .unwrap();
        let new_state = fs::read(&prepared_second.state_path).unwrap();
        assert_ne!(new_state, old_state);

        // A crash may expose the old durable directory entry. Its exact
        // transcript anchor is accepted.
        restore_state(&prepared.state_path, &old_state);
        let mut old_visible = request(old_transcript_items());
        let old_recovered =
            prepare_request(&config, "lane-restart-outcomes", &mut old_visible, false).unwrap();
        assert!(old_recovered.previous.is_some());
        assert!(
            old_visible
                .wire_json_string()
                .unwrap()
                .contains("restart-old")
        );

        // If the new namespace entry survives but Claude did not accept the
        // terminal, the missing output anchor rejects that leading state.
        restore_state(&prepared.state_path, &new_state);
        let mut new_visible = request(old_transcript_items());
        let rejected =
            prepare_request(&config, "lane-restart-outcomes", &mut new_visible, false).unwrap();
        assert!(rejected.previous.is_none());
        assert!(new_visible.raw_input_override.is_none());
        assert_eq!(
            rejected.state_invalidation_reason,
            Some("transcript_mismatch")
        );

        // A missing state falls back to the complete transcript. A corrupt
        // state fails closed because no fingerprint can be authenticated.
        assert!(!prepared.state_path.exists());
        let mut missing = request(old_transcript_items());
        let missing_recovery =
            prepare_request(&config, "lane-restart-outcomes", &mut missing, false).unwrap();
        assert!(missing_recovery.previous.is_none());
        assert!(missing.raw_input_override.is_none());

        restore_state(&prepared.state_path, b"{corrupt");
        let mut corrupt = request(old_transcript_items());
        assert!(matches!(
            prepare_request(&config, "lane-restart-outcomes", &mut corrupt, false),
            Err(InlineCompactionError::InvalidState(
                "sidecar JSON is corrupt"
            ))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn sidecar_permissions_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::TempDir::new().unwrap();
        let config = InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut first = request(vec![user("input")]);
        let prepared = prepare_request(&config, "lane-a", &mut first, false).unwrap();
        let compact_raw = r#"{"type":"compaction","encrypted_content":"opaque"}"#;
        commit_response(&prepared, &sse(&[(0, compact_raw)], 40_000), &[]).unwrap();

        assert_eq!(
            fs::metadata(&config.state_dir)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&prepared.state_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}
