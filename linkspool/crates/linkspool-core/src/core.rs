//! The shared application core used by desktop and Android hosts.

use std::{collections::BTreeMap, sync::Arc};

use linkspool_protocol::{AckStatus, Envelope, PROTOCOL_VERSION};
use tokio::sync::OnceCell;

use crate::{
    Claim, Config, CoreError, DeliveryJob, Message, MessageState, NewMessage, Store, Submission,
    persistence::MaintenanceReport, transport,
};

/// Lazily initialized message core.
///
/// Rivium constructs services synchronously, so the SQLite pool is opened on
/// the first asynchronous call and shared by every service afterwards.
#[derive(Debug)]
pub struct Core {
    config: Config,
    store: OnceCell<Store>,
}

impl Core {
    /// Build a core from static configuration.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            store: OnceCell::new(),
        }
    }

    /// Configuration captured at startup.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Open the database once and return the shared store.
    pub async fn open(&self) -> Result<&Store, CoreError> {
        self.store
            .get_or_try_init(|| async {
                Store::open(&self.config.database, &self.config.queue).await
            })
            .await
    }

    /// Validate and persist a producer submission.
    pub async fn submit(&self, new: NewMessage) -> Result<Submission, CoreError> {
        self.validate(&new)?;
        let now = now_millis();
        if new.expires_at.is_some_and(|expires_at| expires_at <= now) {
            return Err(CoreError::Invalid(
                "expires_at must be in the future".into(),
            ));
        }
        self.open().await?.submit(&new, now).await
    }

    /// Fetch one message.
    pub async fn message(&self, id: &str) -> Result<Option<Message>, CoreError> {
        self.open().await?.get(id).await
    }

    /// List messages, newest first.
    pub async fn list(
        &self,
        state: Option<MessageState>,
        limit: usize,
    ) -> Result<Vec<Message>, CoreError> {
        self.open().await?.list(state, limit.clamp(1, 500)).await
    }

    /// Retry a dead-lettered message.
    pub async fn retry(&self, id: &str) -> Result<Message, CoreError> {
        self.open().await?.retry(id, now_millis()).await
    }

    /// Discard a dead-lettered or expired message.
    pub async fn discard(&self, id: &str) -> Result<(), CoreError> {
        self.open().await?.discard(id).await
    }

    /// Run one maintenance pass.
    pub async fn maintain(&self) -> Result<MaintenanceReport, CoreError> {
        self.open()
            .await?
            .maintain(
                now_millis(),
                self.config.maintenance.terminal_retention_hours,
                self.config.maintenance.stale_delivery_ms,
            )
            .await
    }

    /// Message counts grouped by lifecycle state.
    pub async fn counts(&self) -> Result<BTreeMap<String, i64>, CoreError> {
        self.open().await?.counts().await
    }

    /// Execute one durable delivery job.
    ///
    /// Returning `Ok` acknowledges the job. Infrastructure failures return an
    /// error so Apalis retries the job; the message keeps its current state and
    /// is later recovered by the maintenance sweep.
    pub async fn deliver(&self, job: DeliveryJob) -> Result<(), CoreError> {
        let store = self.open().await?;
        let now = now_millis();

        let Some(message) = store.get(&job.message_id).await? else {
            tracing::warn!(
                message_id = %job.message_id,
                "delivery job references a missing message"
            );
            return Ok(());
        };
        if message.state.is_terminal() {
            tracing::debug!(
                message_id = %job.message_id,
                state = message.state.as_str(),
                "skipping delivery for terminal message"
            );
            return Ok(());
        }
        if message.expired(now) {
            store.expire(&message.id, now).await?;
            return Ok(());
        }

        let Claim::Ready(claimed) = store.claim(&message.id, job.attempt, now).await? else {
            tracing::debug!(
                message_id = %job.message_id,
                attempt = job.attempt,
                "skipping stale delivery job"
            );
            return Ok(());
        };
        if claimed.expired(now) {
            store.expire_claimed(&claimed.id, job.attempt, now).await?;
            return Ok(());
        }

        let envelope = Envelope {
            version: PROTOCOL_VERSION,
            message_id: claimed.id.clone(),
            idempotency_key: claimed.idempotency_key.clone(),
            created_at: claimed.created_at,
            expires_at: claimed.expires_at,
            payload: claimed.payload.clone(),
        };

        match transport::send(&envelope, &self.config.transport).await {
            Ok(ack) => match ack.status {
                AckStatus::Accepted | AckStatus::Duplicate => {
                    store.mark_delivered(&claimed.id, job.attempt, now).await?;
                    tracing::info!(
                        message_id = %claimed.id,
                        attempt = job.attempt,
                        duplicate = ack.status == AckStatus::Duplicate,
                        "message delivered"
                    );
                }
                AckStatus::Rejected => {
                    let reason = ack
                        .reason
                        .unwrap_or_else(|| "receiver rejected the message".into());
                    store
                        .mark_dead_lettered(&claimed.id, job.attempt, &reason, now)
                        .await?;
                    tracing::warn!(
                        message_id = %claimed.id,
                        attempt = job.attempt,
                        reason = %reason,
                        "receiver rejected the message"
                    );
                }
            },
            Err(error) if error.is_transient() => {
                let detail = error.to_string();
                if job.attempt >= claimed.max_attempts {
                    store
                        .mark_dead_lettered(&claimed.id, job.attempt, &detail, now)
                        .await?;
                    tracing::warn!(
                        message_id = %claimed.id,
                        attempt = job.attempt,
                        error = %detail,
                        "attempt limit reached; message dead-lettered"
                    );
                } else {
                    let delay = self.config.queue.retry_delay_ms(job.attempt);
                    store
                        .schedule_retry(&claimed.id, job.attempt, delay, &detail, now)
                        .await?;
                    tracing::info!(
                        message_id = %claimed.id,
                        attempt = job.attempt,
                        delay_ms = delay,
                        error = %detail,
                        "delivery failed; retry scheduled"
                    );
                }
            }
            Err(error) => {
                store
                    .mark_dead_lettered(&claimed.id, job.attempt, &error.to_string(), now)
                    .await?;
                tracing::warn!(
                    message_id = %claimed.id,
                    attempt = job.attempt,
                    error = %error,
                    "permanent delivery failure; message dead-lettered"
                );
            }
        }

        Ok(())
    }

    fn validate(&self, new: &NewMessage) -> Result<(), CoreError> {
        if new.producer.trim().is_empty() || new.producer.len() > 128 {
            return Err(CoreError::Invalid(
                "producer must contain 1 to 128 bytes".into(),
            ));
        }
        if new.destination != self.config.queue.destination {
            return Err(CoreError::Invalid(format!(
                "destination must be {}",
                self.config.queue.destination
            )));
        }
        if let Some(key) = &new.idempotency_key
            && (key.is_empty() || key.len() > 256)
        {
            return Err(CoreError::Invalid(
                "idempotency_key must contain 1 to 256 bytes".into(),
            ));
        }
        Ok(())
    }
}

/// Current wall-clock time in Unix milliseconds.
#[must_use]
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            elapsed.as_millis().min(i64::MAX as u128) as i64
        })
}

/// Build a shared core for a host.
#[must_use]
pub fn shared(config: Config) -> Arc<Core> {
    Arc::new(Core::new(config))
}
