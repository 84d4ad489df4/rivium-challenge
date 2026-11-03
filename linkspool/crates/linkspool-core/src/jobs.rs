//! Durable delivery jobs handed to Apalis.

use std::time::Duration;

use apalis::prelude::TaskBuilder;
use apalis_sqlite::SqliteTask;
use serde::{Deserialize, Serialize};

use crate::CoreError;

/// Queue name shared by the producer and the worker.
pub const DELIVERY_QUEUE: &str = "linkspool-delivery";

/// Attempts Apalis may make before it gives up on infrastructure failures.
///
/// Message-level retries are scheduled by LinkSpool as new jobs; these attempts
/// only cover transient database or worker failures.
const INFRASTRUCTURE_ATTEMPTS: usize = 3;

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
/// Apalis retries are deliberately limited: LinkSpool schedules each delivery
/// retry as a new durable job so the backoff delay and attempt count remain
/// authoritative in the message store. The remaining attempts only recover from
/// infrastructure failures that never reached the message state machine.
pub fn task(job: &DeliveryJob) -> Result<SqliteTask, CoreError> {
    build(job, job.idempotency_key(), None)
}

/// Build a task that should run after `delay_ms`.
pub fn delayed_task(job: &DeliveryJob, delay_ms: u64) -> Result<SqliteTask, CoreError> {
    build(
        job,
        job.idempotency_key(),
        Some(Duration::from_millis(delay_ms)),
    )
}

/// Build a task that re-runs an attempt whose worker disappeared.
///
/// The key differs from the original attempt so a completed job row cannot
/// suppress the recovery run.
pub fn recovery_task(job: &DeliveryJob) -> Result<SqliteTask, CoreError> {
    build(job, format!("{}:recovery", job.idempotency_key()), None)
}

fn build(
    job: &DeliveryJob,
    key: String,
    delay: Option<Duration>,
) -> Result<SqliteTask, CoreError> {
    let args = serde_json::to_vec(job)?;
    let builder = TaskBuilder::new(args)
        .idempotency_key(key)
        .max_attempts(INFRASTRUCTURE_ATTEMPTS);
    let builder = match delay {
        Some(delay) => builder.run_after(delay),
        None => builder,
    };
    Ok(builder.build())
}
