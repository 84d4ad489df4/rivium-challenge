//! Static application configuration.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::CoreError;

/// Root configuration, loaded by the Rivium host.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Desktop HTTP API settings.
    pub api: ApiConfig,
    /// SQLite message store settings.
    pub database: DatabaseConfig,
    /// Queue admission and retry settings.
    pub queue: QueueConfig,
    /// TCP transport settings.
    pub transport: TransportConfig,
    /// Background maintenance settings.
    pub maintenance: MaintenanceConfig,
    /// File-drop ingestion used by the embedded Android host.
    pub inbox: InboxConfig,
    /// Periodic status snapshot written for the embedded host.
    pub status: StatusConfig,
}

impl Config {
    /// Reject values that would make the daemon behave incorrectly.
    pub fn validate(&self) -> Result<(), CoreError> {
        self.queue.validate()?;
        self.transport.validate()?;
        self.maintenance.validate()?;
        self.inbox.validate()?;
        self.status.validate()?;
        if self.database.path.as_os_str().is_empty() {
            return Err(CoreError::Invalid("database.path must not be empty".into()));
        }
        Ok(())
    }

    /// Resolve relative data paths against the host root.
    pub fn resolve_paths(&mut self, root: &Path) {
        resolve(&mut self.database.path, root);
        resolve(&mut self.inbox.path, root);
        resolve(&mut self.status.path, root);
    }

    /// Maximum number of delivery jobs processed concurrently by one worker.
    #[must_use]
    pub fn worker_concurrency(&self) -> usize {
        4
    }
}

/// Desktop HTTP API configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApiConfig {
    /// Whether the loopback HTTP API service runs.
    pub enabled: bool,
    /// Address the local HTTP API binds to.
    pub listen: SocketAddr,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen: "127.0.0.1:8080".parse().expect("valid default address"),
        }
    }
}

/// SQLite message store configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseConfig {
    /// Path to the authoritative SQLite database.
    pub path: PathBuf,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("linkspool.db"),
        }
    }
}

/// Queue admission and retry configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueConfig {
    /// Destination accepted by this instance.
    pub destination: String,
    /// Maximum serialized payload size in bytes.
    pub max_payload_bytes: usize,
    /// Maximum total bytes of messages occupying queue capacity.
    pub max_queued_bytes: i64,
    /// Maximum number of messages occupying queue capacity.
    pub max_queued_messages: i64,
    /// Delivery attempts allowed for a message before dead-lettering.
    pub max_attempts: usize,
    /// First retry delay in milliseconds.
    pub retry_base_ms: u64,
    /// Upper bound for retry delays in milliseconds.
    pub retry_cap_ms: u64,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            destination: "local-sink".into(),
            max_payload_bytes: 256 * 1024,
            max_queued_bytes: 64 * 1024 * 1024,
            max_queued_messages: 10_000,
            max_attempts: 8,
            retry_base_ms: 1_000,
            retry_cap_ms: 60_000,
        }
    }
}

impl QueueConfig {
    fn validate(&self) -> Result<(), CoreError> {
        if self.destination.trim().is_empty() {
            return Err(CoreError::Invalid(
                "queue.destination must not be empty".into(),
            ));
        }
        if self.max_payload_bytes == 0 {
            return Err(CoreError::Invalid(
                "queue.max_payload_bytes must be greater than zero".into(),
            ));
        }
        if self.max_queued_bytes <= 0 {
            return Err(CoreError::Invalid(
                "queue.max_queued_bytes must be greater than zero".into(),
            ));
        }
        if self.max_queued_messages <= 0 {
            return Err(CoreError::Invalid(
                "queue.max_queued_messages must be greater than zero".into(),
            ));
        }
        if self.max_attempts == 0 {
            return Err(CoreError::Invalid(
                "queue.max_attempts must be greater than zero".into(),
            ));
        }
        if self.retry_base_ms < 1_000 {
            return Err(CoreError::Invalid(
                "queue.retry_base_ms must be at least 1000; Apalis schedules in whole seconds"
                    .into(),
            ));
        }
        if self.retry_cap_ms < self.retry_base_ms {
            return Err(CoreError::Invalid(
                "queue.retry_cap_ms must not be smaller than queue.retry_base_ms".into(),
            ));
        }
        Ok(())
    }

    /// Exponential backoff for an attempt number, starting at one.
    #[must_use]
    pub fn retry_delay_ms(&self, attempt: usize) -> u64 {
        let exponent = attempt.saturating_sub(1).min(63) as u32;
        self.retry_base_ms
            .saturating_mul(1_u64 << exponent)
            .min(self.retry_cap_ms)
    }
}

/// TCP transport configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransportConfig {
    /// Receiver address.
    pub sink_addr: SocketAddr,
    /// TCP connect timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// Time allowed for the write and acknowledgement round trip.
    pub ack_timeout_ms: u64,
    /// Maximum accepted frame size in bytes.
    pub max_frame_bytes: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            sink_addr: "127.0.0.1:9000".parse().expect("valid default address"),
            connect_timeout_ms: 3_000,
            ack_timeout_ms: 5_000,
            max_frame_bytes: 1024 * 1024,
        }
    }
}

impl TransportConfig {
    fn validate(&self) -> Result<(), CoreError> {
        if self.connect_timeout_ms == 0 {
            return Err(CoreError::Invalid(
                "transport.connect_timeout_ms must be greater than zero".into(),
            ));
        }
        if self.ack_timeout_ms == 0 {
            return Err(CoreError::Invalid(
                "transport.ack_timeout_ms must be greater than zero".into(),
            ));
        }
        if self.max_frame_bytes == 0 {
            return Err(CoreError::Invalid(
                "transport.max_frame_bytes must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

/// Background maintenance configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MaintenanceConfig {
    /// Interval between maintenance passes in milliseconds.
    pub interval_ms: u64,
    /// How long terminal records are retained before deletion.
    pub terminal_retention_hours: u64,
    /// How long a delivery may stay claimed before it is considered interrupted.
    pub stale_delivery_ms: u64,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            interval_ms: 30_000,
            terminal_retention_hours: 168,
            stale_delivery_ms: 300_000,
        }
    }
}

impl MaintenanceConfig {
    fn validate(&self) -> Result<(), CoreError> {
        if self.interval_ms == 0 {
            return Err(CoreError::Invalid(
                "maintenance.interval_ms must be greater than zero".into(),
            ));
        }
        if self.terminal_retention_hours == 0 {
            return Err(CoreError::Invalid(
                "maintenance.terminal_retention_hours must be greater than zero".into(),
            ));
        }
        if self.stale_delivery_ms == 0 {
            return Err(CoreError::Invalid(
                "maintenance.stale_delivery_ms must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

/// File-drop ingestion for hosts that cannot call the core directly.
///
/// A producer writes a complete file into `path`; the host ingests it and
/// removes the file only after the message is durable.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct InboxConfig {
    /// Whether the inbox service runs.
    pub enabled: bool,
    /// Directory watched for new messages.
    pub path: PathBuf,
    /// Producer identity recorded for inbox messages.
    pub producer: String,
    /// How often the directory is scanned, in milliseconds.
    pub poll_interval_ms: u64,
    /// Maximum accepted file size in bytes.
    pub max_file_bytes: usize,
}

impl Default for InboxConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: PathBuf::from("inbox"),
            producer: "android-share".into(),
            poll_interval_ms: 250,
            max_file_bytes: 64 * 1024,
        }
    }
}

impl InboxConfig {
    fn validate(&self) -> Result<(), CoreError> {
        if self.enabled && self.producer.trim().is_empty() {
            return Err(CoreError::Invalid(
                "inbox.producer must not be empty".into(),
            ));
        }
        if self.path.as_os_str().is_empty() {
            return Err(CoreError::Invalid("inbox.path must not be empty".into()));
        }
        if self.poll_interval_ms == 0 {
            return Err(CoreError::Invalid(
                "inbox.poll_interval_ms must be greater than zero".into(),
            ));
        }
        if self.max_file_bytes == 0 {
            return Err(CoreError::Invalid(
                "inbox.max_file_bytes must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

/// Periodic status snapshot for hosts without a query interface.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct StatusConfig {
    /// Whether the status service runs.
    pub enabled: bool,
    /// File that receives the JSON snapshot.
    pub path: PathBuf,
    /// How often the snapshot is refreshed, in milliseconds.
    pub interval_ms: u64,
}

impl Default for StatusConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: PathBuf::from("status.json"),
            interval_ms: 2_000,
        }
    }
}

impl StatusConfig {
    fn validate(&self) -> Result<(), CoreError> {
        if self.path.as_os_str().is_empty() {
            return Err(CoreError::Invalid("status.path must not be empty".into()));
        }
        if self.interval_ms == 0 {
            return Err(CoreError::Invalid(
                "status.interval_ms must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

fn resolve(path: &mut PathBuf, root: &Path) {
    if path.is_relative() {
        *path = root.join(&*path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        Config::default().validate().unwrap();
    }

    #[test]
    fn retry_delay_is_exponential_and_capped() {
        let queue = QueueConfig::default();
        assert_eq!(queue.retry_delay_ms(1), 1_000);
        assert_eq!(queue.retry_delay_ms(2), 2_000);
        assert_eq!(queue.retry_delay_ms(3), 4_000);
        assert_eq!(queue.retry_delay_ms(7), 60_000);
        assert_eq!(queue.retry_delay_ms(usize::MAX), 60_000);
    }

    #[test]
    fn relative_database_paths_are_resolved() {
        let mut config = Config::default();
        config.resolve_paths(Path::new("/tmp/root"));
        assert_eq!(config.database.path, Path::new("/tmp/root/linkspool.db"));
        assert_eq!(config.inbox.path, Path::new("/tmp/root/inbox"));
        assert_eq!(config.status.path, Path::new("/tmp/root/status.json"));

        config.database.path = PathBuf::from("/var/lib/linkspool.db");
        config.resolve_paths(Path::new("/tmp/root"));
        assert_eq!(config.database.path, Path::new("/var/lib/linkspool.db"));
    }
}
