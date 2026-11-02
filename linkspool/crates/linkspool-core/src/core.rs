//! The shared application core used by desktop and Android hosts.

use std::sync::Arc;

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
        self.open()
            .await?
            .list(state, limit.clamp(1, 500))
            .await
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
            )
            .await
    }

    /// Execute one durable delivery job.
    ///
    /// Returning `Ok` acknowledges the job. Database failures return an error
    /// so Apalis records the job as failed; the message keeps its current state.
    pub async fn deliver(&self, raw: &[u8]) -> Result<(), CoreError> {
        let job: DeliveryJob = serde_json::from_slice(raw)?;
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
            return Ok(());
        }
        if message.expired(now) {
            store.expire(&message.id, now).await?;
            return Ok(());
        }

        let Claim::Ready(claimed) = store.claim(&message.id, job.attempt, now).await? else {
            return Ok(());
        };
        if claimed.expired(now) {
            store
                .expire_claimed(&claimed.id, job.attempt, now)
                .await?;
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
                }
                AckStatus::Rejected => {
                    let reason = ack
                        .reason
                        .unwrap_or_else(|| "receiver rejected the message".into());
                    store
                        .mark_dead_lettered(&claimed.id, job.attempt, &reason, now)
                        .await?;
                }
            },
            Err(error) if error.is_transient() => {
                let detail = error.to_string();
                if job.attempt >= claimed.max_attempts {
                    store
                        .mark_dead_lettered(&claimed.id, job.attempt, &detail, now)
                        .await?;
                } else {
                    let delay = self.config.queue.retry_delay_ms(job.attempt);
                    store
                        .schedule_retry(&claimed.id, job.attempt, delay, &detail, now)
                        .await?;
                }
            }
            Err(error) => {
                store
                    .mark_dead_lettered(&claimed.id, job.attempt, &error.to_string(), now)
                    .await?;
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
