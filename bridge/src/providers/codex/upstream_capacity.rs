use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const GLOBAL_MAX_ENV: &str = "CCP_CODEX_UPSTREAM_GLOBAL_MAX_CONCURRENT_REQUESTS";
const DATA_MAX_ENV: &str = "CCP_CODEX_UPSTREAM_DATA_MAX_CONCURRENT_REQUESTS";
const CONTROL_MAX_ENV: &str = "CCP_CODEX_UPSTREAM_CONTROL_MAX_CONCURRENT_REQUESTS";
const QUEUE_TIMEOUT_ENV: &str = "CCP_CODEX_UPSTREAM_QUEUE_TIMEOUT_SECS";
const DEFAULT_QUEUE_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_CAPACITY: usize = 64;
const MAX_QUEUE_TIMEOUT_SECS: u64 = 3_600;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamClass {
    Data,
    Control,
}

impl UpstreamClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Control => "control",
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum UpstreamCapacityError {
    #[error("Codex upstream capacity configuration error: {0}")]
    Config(&'static str),
    #[error("request timed out in the Codex upstream {class} queue")]
    QueueTimeout { class: &'static str },
    #[error("Codex upstream capacity gate closed")]
    GateClosed,
}

#[derive(Debug, Clone, Copy)]
struct UpstreamCapacityConfig {
    global_max: usize,
    data_max: usize,
    control_max: usize,
    wait_timeout: Duration,
}

impl UpstreamCapacityConfig {
    fn from_environment() -> Result<Option<Self>, UpstreamCapacityError> {
        let global = std::env::var_os(GLOBAL_MAX_ENV);
        let data = std::env::var_os(DATA_MAX_ENV);
        let control = std::env::var_os(CONTROL_MAX_ENV);
        if global.is_none() && data.is_none() && control.is_none() {
            return Ok(None);
        }
        let (Some(global), Some(data), Some(control)) = (global, data, control) else {
            return Err(UpstreamCapacityError::Config(
                "global, data, and control limits must be configured together",
            ));
        };
        let global_max = parse_capacity(global, "global upstream capacity must be an integer")?;
        let data_max = parse_capacity(data, "data upstream capacity must be an integer")?;
        let control_max = parse_capacity(control, "control upstream capacity must be an integer")?;
        if data_max.checked_add(control_max) != Some(global_max) {
            return Err(UpstreamCapacityError::Config(
                "data plus control capacity must equal global capacity",
            ));
        }
        let wait_timeout = match std::env::var_os(QUEUE_TIMEOUT_ENV) {
            Some(raw) => {
                let seconds = raw
                    .to_str()
                    .ok_or(UpstreamCapacityError::Config(
                        "upstream queue timeout must be valid UTF-8",
                    ))?
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| {
                        UpstreamCapacityError::Config("upstream queue timeout must be an integer")
                    })?;
                if !(1..=MAX_QUEUE_TIMEOUT_SECS).contains(&seconds) {
                    return Err(UpstreamCapacityError::Config(
                        "upstream queue timeout must be between 1 and 3600 seconds",
                    ));
                }
                Duration::from_secs(seconds)
            }
            None => DEFAULT_QUEUE_TIMEOUT,
        };
        Ok(Some(Self {
            global_max,
            data_max,
            control_max,
            wait_timeout,
        }))
    }
}

fn parse_capacity(
    raw: std::ffi::OsString,
    invalid_integer: &'static str,
) -> Result<usize, UpstreamCapacityError> {
    let value = raw
        .to_str()
        .ok_or(UpstreamCapacityError::Config(
            "upstream capacity must be valid UTF-8",
        ))?
        .trim()
        .parse::<usize>()
        .map_err(|_| UpstreamCapacityError::Config(invalid_integer))?;
    if !(1..=MAX_CAPACITY).contains(&value) {
        return Err(UpstreamCapacityError::Config(
            "upstream capacity must be between 1 and 64",
        ));
    }
    Ok(value)
}

pub struct UpstreamCapacityGates {
    global: Arc<Semaphore>,
    data: Arc<Semaphore>,
    control: Arc<Semaphore>,
    wait_timeout: Duration,
}

impl UpstreamCapacityGates {
    pub fn from_environment() -> Result<Option<Self>, UpstreamCapacityError> {
        Ok(UpstreamCapacityConfig::from_environment()?.map(Self::new))
    }

    fn new(config: UpstreamCapacityConfig) -> Self {
        Self {
            global: Arc::new(Semaphore::new(config.global_max)),
            data: Arc::new(Semaphore::new(config.data_max)),
            control: Arc::new(Semaphore::new(config.control_max)),
            wait_timeout: config.wait_timeout,
        }
    }

    pub async fn acquire(
        &self,
        class: UpstreamClass,
    ) -> Result<UpstreamCapacityPermit, UpstreamCapacityError> {
        let class_gate = match class {
            UpstreamClass::Data => self.data.clone(),
            UpstreamClass::Control => self.control.clone(),
        };
        tokio::time::timeout(self.wait_timeout, async {
            // Take the reserved class permit first. A queued data request must
            // not occupy global capacity needed by the control lane.
            let class_permit = class_gate
                .acquire_owned()
                .await
                .map_err(|_| UpstreamCapacityError::GateClosed)?;
            let global_permit = self
                .global
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| UpstreamCapacityError::GateClosed)?;
            Ok(UpstreamCapacityPermit {
                _class_permit: class_permit,
                _global_permit: global_permit,
            })
        })
        .await
        .map_err(|_| UpstreamCapacityError::QueueTimeout {
            class: class.as_str(),
        })?
    }
}

pub struct UpstreamCapacityPermit {
    _class_permit: OwnedSemaphorePermit,
    _global_permit: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::{
        UpstreamCapacityConfig, UpstreamCapacityError, UpstreamCapacityGates, UpstreamClass,
    };
    use std::time::Duration;

    #[tokio::test]
    async fn data_queue_cannot_consume_reserved_control_capacity() {
        let gates = UpstreamCapacityGates::new(UpstreamCapacityConfig {
            global_max: 3,
            data_max: 2,
            control_max: 1,
            wait_timeout: Duration::from_millis(20),
        });

        let _data_one = gates.acquire(UpstreamClass::Data).await.unwrap();
        let _data_two = gates.acquire(UpstreamClass::Data).await.unwrap();
        assert!(matches!(
            gates.acquire(UpstreamClass::Data).await,
            Err(UpstreamCapacityError::QueueTimeout { class: "data" })
        ));

        let _control = gates.acquire(UpstreamClass::Control).await.unwrap();
    }
}
