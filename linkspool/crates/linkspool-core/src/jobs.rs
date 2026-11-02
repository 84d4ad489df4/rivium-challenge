//! Durable delivery jobs handed to Apalis.

use apalis::prelude::TaskBuilder;
use apalis_sqlite::SqliteTask;
use serde::{Deserialize, Serialize};

use crate::CoreError;

/// Queue name shared by the producer and the worker.
pub const DELIVERY_QUEUE: &str = "linkspool-delivery";

/// Job arguments stored by Apalis. The message payload stays in SQLite.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DeliveryJob {
    /// Message to deliver.
    pub message_id: String,
    /// One-based attempt number this job represents.
    pub attempt: usize,
}

impl DeliveryJob {
    /// Stable idempotency key for one attempt of one message.
    #[must_use]
    pub fn idempotency_key(&self) -> String {
        format!("{}:{}", self.message_id, self.attempt)
    }
}

/// Build an Apalis task for a delivery attempt.
///
/// Apalis retries are deliberately disabled: LinkSpool schedules each retry as
/// a new durable job so the backoff delay and attempt count remain authoritative
/// in the message store.
pub fn task(job: &DeliveryJob) -> Result<SqliteTask, CoreError> {
    let args = serde_json::to_vec(job)?;
    Ok(TaskBuilder::new(args)
        .idempotency_key(job.idempotency_key())
        .max_attempts(1)
        .build())
}

/// Build a task that should run after `delay_ms`.
pub fn delayed_task(job: &DeliveryJob, delay_ms: u64) -> Result<SqliteTask, CoreError> {
    let args = serde_json::to_vec(job)?;
    Ok(TaskBuilder::new(args)
        .idempotency_key(job.idempotency_key())
        .max_attempts(1)
        .run_after(std::time::Duration::from_millis(delay_ms))
        .build())
}
