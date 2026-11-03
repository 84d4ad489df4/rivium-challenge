//! Companion TCP receiver for LinkSpool.
//!
//! The sink persists each received message exactly once and answers with an
//! explicit acknowledgement. Duplicate message identifiers are counted and
//! acknowledged again without repeating the receiver-side effect.

use std::{
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use linkspool_protocol::{
    Ack, AckStatus, Envelope, ProtocolError, decode_envelope, encode_ack, read_frame, write_frame,
};
use sqlx::{
    Connection, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tokio::net::{TcpListener, TcpStream};

/// Failure while receiving or recording a message.
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    /// The receiver-side SQLite database failed.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// The wire protocol rejected a frame.
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// The socket failed.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// A payload could not be encoded.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Outcome of recording one envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordOutcome {
    /// Acknowledgement status the receiver should return.
    pub status: AckStatus,
    /// Whether the acknowledgement should be intentionally dropped.
    pub drop_ack: bool,
}

/// Counters shown by the CLI and used by tests.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SinkStats {
    /// Unique messages recorded.
    pub received: u64,
    /// Duplicate deliveries observed.
    pub duplicates: u64,
    /// Acknowledgements intentionally dropped.
    pub dropped: u64,
}

#[derive(Debug, Default)]
struct Counters {
    received: AtomicU64,
    duplicates: AtomicU64,
    dropped: AtomicU64,
}

/// Durable receiver-side message store.
#[derive(Debug)]
pub struct Sink {
    pool: SqlitePool,
    drop_ack: bool,
    counters: Counters,
}

impl Sink {
    /// Open or create the receiver database.
    pub async fn open(path: &Path, drop_ack: bool) -> Result<Self, SinkError> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5))
            .foreign_keys(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS received_messages (\
                message_id TEXT PRIMARY KEY NOT NULL,\
                idempotency_key TEXT,\
                created_at INTEGER NOT NULL,\
                received_at INTEGER NOT NULL,\
                payload TEXT NOT NULL,\
                duplicate_count INTEGER NOT NULL DEFAULT 0,\
                ack_dropped INTEGER NOT NULL DEFAULT 0\
             )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS received_messages_received_at \
             ON received_messages (received_at)",
        )
        .execute(&pool)
        .await?;

        Ok(Self {
            pool,
            drop_ack,
            counters: Counters::default(),
        })
    }

    /// Underlying pool, exposed for diagnostics and tests.
    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Record one envelope and decide how to answer it.
    pub async fn record(&self, envelope: &Envelope) -> Result<RecordOutcome, SinkError> {
        let payload = serde_json::to_string(&envelope.payload)?;
        let mut connection = self.pool.acquire().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;

        let existing: Option<(i64,)> =
            sqlx::query_as("SELECT duplicate_count FROM received_messages WHERE message_id = ?")
                .bind(&envelope.message_id)
                .fetch_optional(&mut *tx)
                .await?;

        if existing.is_some() {
            sqlx::query(
                "UPDATE received_messages SET duplicate_count = duplicate_count + 1 \
                 WHERE message_id = ?",
            )
            .bind(&envelope.message_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;

            let duplicates = self.counters.duplicates.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::info!(
                message_id = %envelope.message_id,
                status = "duplicate",
                received = self.counters.received.load(Ordering::Relaxed),
                duplicates,
                "message recorded"
            );
            return Ok(RecordOutcome {
                status: AckStatus::Duplicate,
                drop_ack: false,
            });
        }

        let now = now_millis();
        sqlx::query(
            "INSERT INTO received_messages (\
                message_id, idempotency_key, created_at, received_at, payload, ack_dropped\
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&envelope.message_id)
        .bind(&envelope.idempotency_key)
        .bind(envelope.created_at)
        .bind(now)
        .bind(payload)
        .bind(i64::from(self.drop_ack))
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        let received = self.counters.received.fetch_add(1, Ordering::Relaxed) + 1;
        if self.drop_ack {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
        }
        tracing::info!(
            message_id = %envelope.message_id,
            status = if self.drop_ack { "accepted (ack dropped)" } else { "accepted" },
            received,
            duplicates = self.counters.duplicates.load(Ordering::Relaxed),
            "message recorded"
        );

        Ok(RecordOutcome {
            status: AckStatus::Accepted,
            drop_ack: self.drop_ack,
        })
    }

    /// Live counters for this process.
    #[must_use]
    pub fn stats(&self) -> SinkStats {
        SinkStats {
            received: self.counters.received.load(Ordering::Relaxed),
            duplicates: self.counters.duplicates.load(Ordering::Relaxed),
            dropped: self.counters.dropped.load(Ordering::Relaxed),
        }
    }

    /// Durable totals from the receiver database.
    pub async fn totals(&self) -> Result<SinkStats, SinkError> {
        let (received, duplicates): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), COALESCE(SUM(duplicate_count), 0) FROM received_messages",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(SinkStats {
            received: received.max(0) as u64,
            duplicates: duplicates.max(0) as u64,
            dropped: self.counters.dropped.load(Ordering::Relaxed),
        })
    }
}

/// Accept connections until the listener is dropped.
pub async fn serve(
    listener: TcpListener,
    sink: Arc<Sink>,
    max_frame_bytes: usize,
) -> Result<(), SinkError> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let sink = sink.clone();
        tokio::spawn(async move {
            if let Err(error) = handle(stream, sink, max_frame_bytes).await {
                tracing::warn!(%peer, %error, "connection closed with error");
            }
        });
    }
}

async fn handle(
    mut stream: TcpStream,
    sink: Arc<Sink>,
    max_frame_bytes: usize,
) -> Result<(), SinkError> {
    while let Some(payload) = read_frame(&mut stream, max_frame_bytes).await? {
        let envelope = decode_envelope(&payload)?;
        let outcome = sink.record(&envelope).await?;
        if outcome.drop_ack {
            continue;
        }
        let ack = match outcome.status {
            AckStatus::Accepted => Ack::accepted(&envelope.message_id),
            AckStatus::Duplicate => Ack::duplicate(&envelope.message_id),
            AckStatus::Rejected => Ack::rejected(&envelope.message_id, "unexpected status"),
        };
        write_frame(&mut stream, &encode_ack(&ack)?, max_frame_bytes).await?;
    }
    Ok(())
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            elapsed.as_millis().min(i64::MAX as u128) as i64
        })
}
