//! SQLite persistence for messages and their delivery jobs.

use std::{collections::BTreeMap, path::Path, time::Duration};

use apalis_sqlite::SqliteStorage;
use serde_json::Value;
use sqlx::{
    Connection, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use ulid::Ulid;

use crate::{
    Claim, CoreError, DatabaseConfig, DeliveryJob, Message, MessageState, NewMessage, QueueConfig,
    Submission,
    jobs::{self, DELIVERY_QUEUE},
};

/// One open message database.
#[derive(Clone, Debug)]
pub struct Store {
    pool: SqlitePool,
    queue: QueueConfig,
}

impl Store {
    /// Open or create the database, then apply both migration sets.
    pub async fn open(database: &DatabaseConfig, queue: &QueueConfig) -> Result<Self, CoreError> {
        create_parent(&database.path)?;

        let options = SqliteConnectOptions::new()
            .filename(&database.path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5))
            .foreign_keys(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;

        // Apalis owns its own migration table; never call SqliteStorage::setup()
        // because that also disables synchronous writes.
        let mut apalis = SqliteStorage::<()>::migrations();
        apalis.set_ignore_missing(true);
        apalis.run(&pool).await.map_err(sqlx::Error::from)?;

        let mut migrator = sqlx::migrate!("../../migrations");
        migrator.dangerous_set_table_name("_linkspool_migrations");
        migrator.set_ignore_missing(true);
        migrator.run(&pool).await.map_err(sqlx::Error::from)?;

        Ok(Self {
            pool,
            queue: queue.clone(),
        })
    }

    /// Underlying pool, exposed for integration tests and diagnostics.
    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Queue settings captured at startup.
    #[must_use]
    pub fn queue(&self) -> &QueueConfig {
        &self.queue
    }

    /// Validate and durably store a message together with its first job.
    pub async fn submit(&self, new: &NewMessage, now: i64) -> Result<Submission, CoreError> {
        let payload = serde_json::to_vec(&new.payload)?;
        if payload.len() > self.queue.max_payload_bytes {
            return Err(CoreError::PayloadTooLarge {
                size: payload.len(),
                max: self.queue.max_payload_bytes,
            });
        }

        let mut connection = self.pool.acquire().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;

        if let Some(key) = new.idempotency_key.as_deref() {
            let existing = sqlx::query_as::<_, MessageRow>(
                "SELECT * FROM messages WHERE producer = ? AND idempotency_key = ?",
            )
            .bind(&new.producer)
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(row) = existing {
                let message = Message::try_from(row)?;
                tx.rollback().await?;
                return Ok(Submission {
                    message,
                    duplicate: true,
                });
            }
        }

        let (count, bytes): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0) \
             FROM messages WHERE state IN ('pending','delivering','retry_scheduled','dead_lettered')",
        )
        .fetch_one(&mut *tx)
        .await?;

        if count + 1 > self.queue.max_queued_messages
            || bytes + payload.len() as i64 > self.queue.max_queued_bytes
        {
            tx.rollback().await?;
            return Err(CoreError::QueueFull);
        }

        let id = Ulid::generate().to_string();
        sqlx::query(
            "INSERT INTO messages (\
                id, producer, destination, idempotency_key, payload, payload_bytes, \
                created_at, expires_at, state, attempts, max_attempts, next_attempt_at, \
                last_error, delivered_at, updated_at\
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, NULL, NULL, NULL, ?)",
        )
        .bind(&id)
        .bind(&new.producer)
        .bind(&new.destination)
        .bind(&new.idempotency_key)
        .bind(&payload)
        .bind(payload.len() as i64)
        .bind(now)
        .bind(new.expires_at)
        .bind(self.queue.max_attempts as i64)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        let job = DeliveryJob {
            message_id: id.clone(),
            attempt: 1,
        };
        apalis_sqlite::queries::push_tasks(&mut tx, DELIVERY_QUEUE, &[jobs::task(&job)?])
            .await
            .map_err(|e| CoreError::Corrupt(format!("apalis push_tasks: {e}")))?;

        tx.commit().await?;

        let message = self
            .get(&id)
            .await?
            .ok_or_else(|| CoreError::NotFound(id))?;
        Ok(Submission {
            message,
            duplicate: false,
        })
    }

    /// Fetch one message by identifier.
    pub async fn get(&self, id: &str) -> Result<Option<Message>, CoreError> {
        let row = sqlx::query_as::<_, MessageRow>("SELECT * FROM messages WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(Message::try_from).transpose()
    }

    /// List messages, newest first, optionally filtered by state.
    pub async fn list(
        &self,
        state: Option<MessageState>,
        limit: usize,
    ) -> Result<Vec<Message>, CoreError> {
        let state = state.map(MessageState::as_str);
        let rows = sqlx::query_as::<_, MessageRow>(
            "SELECT * FROM messages \
             WHERE (? IS NULL OR state = ?) \
             ORDER BY created_at DESC LIMIT ?",
        )
        .bind(state)
        .bind(state)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Message::try_from).collect()
    }

    /// Claim one attempt for delivery.
    pub async fn claim(&self, id: &str, attempt: usize, now: i64) -> Result<Claim, CoreError> {
        let Some(message) = self.get(id).await? else {
            return Ok(Claim::Skip);
        };
        if message.state.is_terminal() {
            return Ok(Claim::Skip);
        }
        if message.expired(now) {
            self.expire(id, now).await?;
            return Ok(Claim::Skip);
        }

        match message.state {
            MessageState::Pending | MessageState::RetryScheduled => {
                if message.attempts + 1 != attempt {
                    return Ok(Claim::Skip);
                }
                let result = sqlx::query(
                    "UPDATE messages SET state = 'delivering', attempts = ?, \
                     next_attempt_at = NULL, updated_at = ? \
                     WHERE id = ? AND state = ? AND attempts = ?",
                )
                .bind(attempt as i64)
                .bind(now)
                .bind(id)
                .bind(message.state.as_str())
                .bind(message.attempts as i64)
                .execute(&self.pool)
                .await?;
                if result.rows_affected() == 0 {
                    return Ok(Claim::Skip);
                }
                match self.get(id).await? {
                    Some(claimed) => Ok(Claim::Ready(Box::new(claimed))),
                    None => Ok(Claim::Skip),
                }
            }
            MessageState::Delivering => {
                // A crash between claiming and transmission leaves the job to be
                // recovered by Apalis. The same attempt may then run again.
                if message.attempts == attempt {
                    Ok(Claim::Ready(Box::new(message)))
                } else {
                    Ok(Claim::Skip)
                }
            }
            _ => Ok(Claim::Skip),
        }
    }

    /// Record a successful receiver acknowledgement.
    pub async fn mark_delivered(
        &self,
        id: &str,
        attempt: usize,
        now: i64,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE messages SET state = 'delivered', delivered_at = ?, \
             next_attempt_at = NULL, last_error = NULL, updated_at = ? \
             WHERE id = ? AND state = 'delivering' AND attempts = ?",
        )
        .bind(now)
        .bind(now)
        .bind(id)
        .bind(attempt as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record a permanent failure or exhausted attempt budget.
    pub async fn mark_dead_lettered(
        &self,
        id: &str,
        attempt: usize,
        error: &str,
        now: i64,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE messages SET state = 'dead_lettered', last_error = ?, \
             next_attempt_at = NULL, updated_at = ? \
             WHERE id = ? AND state = 'delivering' AND attempts = ?",
        )
        .bind(error)
        .bind(now)
        .bind(id)
        .bind(attempt as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Mark a message expired if it is not already being delivered.
    pub async fn expire(&self, id: &str, now: i64) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE messages SET state = 'expired', next_attempt_at = NULL, \
             last_error = COALESCE(last_error, 'message expired'), updated_at = ? \
             WHERE id = ? AND state IN ('pending','retry_scheduled') \
             AND expires_at IS NOT NULL AND expires_at <= ?",
        )
        .bind(now)
        .bind(id)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Expire a claimed message that passed its deadline after being claimed.
    pub async fn expire_claimed(
        &self,
        id: &str,
        attempt: usize,
        now: i64,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE messages SET state = 'expired', next_attempt_at = NULL, \
             last_error = COALESCE(last_error, 'message expired'), updated_at = ? \
             WHERE id = ? AND state = 'delivering' AND attempts = ?",
        )
        .bind(now)
        .bind(id)
        .bind(attempt as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Schedule the next attempt in the same transaction as its job.
    pub async fn schedule_retry(
        &self,
        id: &str,
        attempt: usize,
        delay_ms: u64,
        error: &str,
        now: i64,
    ) -> Result<(), CoreError> {
        let next_attempt_at = now.saturating_add(delay_ms.min(i64::MAX as u64) as i64);
        let mut connection = self.pool.acquire().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;

        let updated = sqlx::query(
            "UPDATE messages SET state = 'retry_scheduled', next_attempt_at = ?, \
             last_error = ?, updated_at = ? \
             WHERE id = ? AND state = 'delivering' AND attempts = ?",
        )
        .bind(next_attempt_at)
        .bind(error)
        .bind(now)
        .bind(id)
        .bind(attempt as i64)
        .execute(&mut *tx)
        .await?;

        if updated.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(());
        }

        let job = DeliveryJob {
            message_id: id.to_owned(),
            attempt: attempt + 1,
        };
        apalis_sqlite::queries::push_tasks(
            &mut tx,
            DELIVERY_QUEUE,
            &[jobs::delayed_task(&job, delay_ms)?],
        )
        .await
        .map_err(|e| CoreError::Corrupt(format!("apalis push_tasks: {e}")))?;

        tx.commit().await?;
        Ok(())
    }

    /// Re-enqueue a dead-lettered message under an extended attempt budget.
    pub async fn retry(&self, id: &str, now: i64) -> Result<Message, CoreError> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;

        let row = sqlx::query_as::<_, MessageRow>("SELECT * FROM messages WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| CoreError::NotFound(id.to_owned()))?;
        let message = Message::try_from(row)?;

        if message.state != MessageState::DeadLettered {
            tx.rollback().await?;
            return Err(CoreError::Conflict(id.to_owned()));
        }

        let next_attempt = message.attempts + 1;
        let max_attempts = message.max_attempts.max(next_attempt);
        let updated = sqlx::query(
            "UPDATE messages SET state = 'retry_scheduled', max_attempts = ?, \
             next_attempt_at = ?, last_error = NULL, updated_at = ? \
             WHERE id = ? AND state = 'dead_lettered'",
        )
        .bind(max_attempts as i64)
        .bind(now)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?;

        if updated.rows_affected() == 0 {
            tx.rollback().await?;
            return Err(CoreError::Conflict(id.to_owned()));
        }

        let job = DeliveryJob {
            message_id: id.to_owned(),
            attempt: next_attempt,
        };
        apalis_sqlite::queries::push_tasks(&mut tx, DELIVERY_QUEUE, &[jobs::task(&job)?])
            .await
            .map_err(|e| CoreError::Corrupt(format!("apalis push_tasks: {e}")))?;

        tx.commit().await?;
        self.get(id)
            .await?
            .ok_or_else(|| CoreError::NotFound(id.to_owned()))
    }

    /// Delete a terminal message from the stores.
    pub async fn discard(&self, id: &str) -> Result<(), CoreError> {
        let result = sqlx::query(
            "DELETE FROM messages WHERE id = ? AND state IN ('dead_lettered','expired')",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() > 0 {
            return Ok(());
        }
        match self.get(id).await? {
            Some(_) => Err(CoreError::Conflict(id.to_owned())),
            None => Err(CoreError::NotFound(id.to_owned())),
        }
    }

    /// Re-enqueue deliveries that were claimed by a worker that never finished.
    ///
    /// A claim increments `attempts` before transmission. Recovery rewinds that
    /// increment and schedules the same attempt again, so the message state and
    /// the Apalis job stay consistent.
    pub async fn recover_stale_deliveries(
        &self,
        now: i64,
        stale_ms: u64,
    ) -> Result<u64, CoreError> {
        let cutoff = now.saturating_sub(stale_ms.min(i64::MAX as u64) as i64);
        let mut connection = self.pool.acquire().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;

        let stale = sqlx::query_as::<_, (String, i64)>(
            "SELECT id, attempts FROM messages \
             WHERE state = 'delivering' AND updated_at < ? AND attempts > 0",
        )
        .bind(cutoff)
        .fetch_all(&mut *tx)
        .await?;

        let mut tasks = Vec::with_capacity(stale.len());
        for (id, attempts) in &stale {
            let updated = sqlx::query(
                "UPDATE messages SET state = 'retry_scheduled', attempts = attempts - 1, \
                 next_attempt_at = ?, last_error = COALESCE(last_error, 'delivery interrupted'), \
                 updated_at = ? \
                 WHERE id = ? AND state = 'delivering' AND attempts = ?",
            )
            .bind(now)
            .bind(now)
            .bind(id)
            .bind(attempts)
            .execute(&mut *tx)
            .await?;
            if updated.rows_affected() == 0 {
                continue;
            }
            let job = DeliveryJob {
                message_id: id.clone(),
                attempt: usize::try_from(*attempts)
                    .map_err(|_| CoreError::Corrupt("negative attempt count".into()))?,
            };
            tasks.push(jobs::recovery_task(&job)?);
        }

        if !tasks.is_empty() {
            apalis_sqlite::queries::push_tasks(&mut tx, DELIVERY_QUEUE, &tasks)
                .await
                .map_err(|e| CoreError::Corrupt(format!("apalis push_tasks: {e}")))?;
        }

        tx.commit().await?;
        Ok(tasks.len() as u64)
    }

    /// Message counts grouped by lifecycle state.
    pub async fn counts(&self) -> Result<BTreeMap<String, i64>, CoreError> {
        let rows = sqlx::query_as::<_, (String, i64)>(
            "SELECT state, COUNT(*) FROM messages GROUP BY state",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().collect())
    }

    /// Expire due messages, recover interrupted deliveries, and purge old records.
    pub async fn maintain(
        &self,
        now: i64,
        retention_hours: u64,
        stale_delivery_ms: u64,
    ) -> Result<MaintenanceReport, CoreError> {
        let recovered = self
            .recover_stale_deliveries(now, stale_delivery_ms)
            .await?;

        let expired = sqlx::query(
            "UPDATE messages SET state = 'expired', next_attempt_at = NULL, \
             last_error = COALESCE(last_error, 'message expired'), updated_at = ? \
             WHERE state IN ('pending','retry_scheduled') \
             AND expires_at IS NOT NULL AND expires_at <= ?",
        )
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?
        .rows_affected();

        let cutoff = now.saturating_sub(
            retention_hours
                .saturating_mul(3_600_000)
                .min(i64::MAX as u64) as i64,
        );

        // Dead letters are retained for inspection, then purged with the rest
        // of the terminal records so the bounded queue cannot fill permanently.
        let purged = sqlx::query(
            "DELETE FROM messages WHERE state IN ('delivered','expired','discarded','dead_lettered') \
             AND updated_at < ?",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?
        .rows_affected();

        let jobs_purged = sqlx::query(
            "DELETE FROM Jobs WHERE done_at IS NOT NULL AND done_at < ? \
             AND status IN ('Done','Failed','Killed')",
        )
        .bind(cutoff / 1_000)
        .execute(&self.pool)
        .await?
        .rows_affected();

        Ok(MaintenanceReport {
            recovered,
            expired,
            purged,
            jobs_purged,
        })
    }
}

/// Counts produced by one maintenance pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MaintenanceReport {
    /// Interrupted deliveries re-enqueued.
    pub recovered: u64,
    /// Messages moved to `expired`.
    pub expired: u64,
    /// Terminal message rows deleted.
    pub purged: u64,
    /// Finished Apalis jobs deleted.
    pub jobs_purged: u64,
}

#[derive(Debug, sqlx::FromRow)]
struct MessageRow {
    id: String,
    producer: String,
    destination: String,
    idempotency_key: Option<String>,
    payload: Vec<u8>,
    payload_bytes: i64,
    created_at: i64,
    expires_at: Option<i64>,
    state: String,
    attempts: i64,
    max_attempts: i64,
    next_attempt_at: Option<i64>,
    last_error: Option<String>,
    delivered_at: Option<i64>,
    updated_at: i64,
}

impl TryFrom<MessageRow> for Message {
    type Error = CoreError;

    fn try_from(row: MessageRow) -> Result<Self, Self::Error> {
        let state = MessageState::parse(&row.state)
            .ok_or_else(|| CoreError::Corrupt(format!("unknown message state {}", row.state)))?;
        let payload: Value = serde_json::from_slice(&row.payload)?;
        Ok(Self {
            id: row.id,
            producer: row.producer,
            destination: row.destination,
            idempotency_key: row.idempotency_key,
            payload,
            payload_bytes: row.payload_bytes,
            created_at: row.created_at,
            expires_at: row.expires_at,
            state,
            attempts: usize::try_from(row.attempts)
                .map_err(|_| CoreError::Corrupt("negative attempt count".into()))?,
            max_attempts: usize::try_from(row.max_attempts)
                .map_err(|_| CoreError::Corrupt("negative attempt limit".into()))?,
            next_attempt_at: row.next_attempt_at,
            last_error: row.last_error,
            delivered_at: row.delivered_at,
            updated_at: row.updated_at,
        })
    }
}

fn create_parent(path: &Path) -> Result<(), CoreError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}
