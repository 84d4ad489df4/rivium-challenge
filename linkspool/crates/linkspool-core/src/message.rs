//! Message domain types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Lifecycle state of one message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageState {
    /// Accepted and waiting for its first delivery attempt.
    Pending,
    /// A worker has claimed the message for an attempt.
    Delivering,
    /// The receiver acknowledged durable storage.
    Delivered,
    /// A failed attempt is waiting for its backoff delay.
    RetryScheduled,
    /// The attempt limit or a permanent rejection stopped delivery.
    DeadLettered,
    /// The expiry time passed before delivery.
    Expired,
    /// An operator discarded the message.
    Discarded,
}

impl MessageState {
    /// Stable database and API representation.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Delivering => "delivering",
            Self::Delivered => "delivered",
            Self::RetryScheduled => "retry_scheduled",
            Self::DeadLettered => "dead_lettered",
            Self::Expired => "expired",
            Self::Discarded => "discarded",
        }
    }

    /// Parse the stored representation.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "delivering" => Some(Self::Delivering),
            "delivered" => Some(Self::Delivered),
            "retry_scheduled" => Some(Self::RetryScheduled),
            "dead_lettered" => Some(Self::DeadLettered),
            "expired" => Some(Self::Expired),
            "discarded" => Some(Self::Discarded),
            _ => None,
        }
    }

    /// Whether no further delivery work should run.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Delivered | Self::DeadLettered | Self::Expired | Self::Discarded
        )
    }

    /// Whether the state occupies queue capacity.
    #[must_use]
    pub fn occupies_capacity(self) -> bool {
        matches!(
            self,
            Self::Pending | Self::Delivering | Self::RetryScheduled | Self::DeadLettered
        )
    }
}

/// One authoritative message record.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    /// Stable identifier assigned at ingestion.
    pub id: String,
    /// Producer identity used to scope idempotency keys.
    pub producer: String,
    /// Destination accepted by this instance.
    pub destination: String,
    /// Producer-supplied deduplication key.
    pub idempotency_key: Option<String>,
    /// Application payload.
    pub payload: Value,
    /// Size of the serialized payload in bytes.
    pub payload_bytes: i64,
    /// Creation time in Unix milliseconds.
    pub created_at: i64,
    /// Optional expiry time in Unix milliseconds.
    pub expires_at: Option<i64>,
    /// Current lifecycle state.
    pub state: MessageState,
    /// Delivery attempts already started.
    pub attempts: usize,
    /// Delivery attempts allowed before dead-lettering.
    pub max_attempts: usize,
    /// Earliest time the next attempt may run, in Unix milliseconds.
    pub next_attempt_at: Option<i64>,
    /// Last delivery error, if any.
    pub last_error: Option<String>,
    /// Time the receiver acknowledged the message.
    pub delivered_at: Option<i64>,
    /// Last state change time in Unix milliseconds.
    pub updated_at: i64,
}

impl Message {
    /// Whether the expiry time has passed.
    #[must_use]
    pub fn expired(&self, now: i64) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }
}

/// A validated submission ready for persistence.
#[derive(Clone, Debug)]
pub struct NewMessage {
    /// Producer identity.
    pub producer: String,
    /// Destination requested by the producer.
    pub destination: String,
    /// Optional deduplication key.
    pub idempotency_key: Option<String>,
    /// Application payload.
    pub payload: Value,
    /// Optional expiry time in Unix milliseconds.
    pub expires_at: Option<i64>,
}

/// Result of an idempotent submission.
#[derive(Clone, Debug)]
pub struct Submission {
    /// Existing or newly created message.
    pub message: Message,
    /// Whether an existing message matched the idempotency key.
    pub duplicate: bool,
}

/// Outcome of an attempted delivery claim.
#[derive(Clone, Debug)]
pub enum Claim {
    /// The caller owns this attempt and may transmit.
    Ready(Box<Message>),
    /// The job was stale, already terminal, or otherwise not actionable.
    Skip,
}
