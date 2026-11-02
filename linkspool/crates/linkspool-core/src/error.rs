//! Errors raised by the LinkSpool core.

use linkspool_protocol::ProtocolError;

use crate::TransportError;

/// Failure while validating, persisting or delivering a message.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    /// The local SQLite database failed.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// A JSON payload could not be encoded or decoded.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A TCP socket failed outside the protocol layer.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The wire protocol rejected a frame.
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// Delivery to the receiver failed.
    #[error("delivery error: {0}")]
    Transport(#[from] TransportError),

    /// The request itself was invalid.
    #[error("invalid request: {0}")]
    Invalid(String),

    /// The serialized payload exceeds the configured limit.
    #[error("payload is {size} bytes, exceeding the {max} byte limit")]
    PayloadTooLarge {
        /// Actual serialized size.
        size: usize,
        /// Configured maximum.
        max: usize,
    },

    /// The bounded queue has no remaining capacity.
    #[error("queue capacity exhausted")]
    QueueFull,

    /// The requested message does not exist.
    #[error("message {0} not found")]
    NotFound(String),

    /// The message is not in a state that permits the operation.
    #[error("message {0} is not dead-lettered")]
    Conflict(String),

    /// A message ID was not a valid ULID.
    #[error("invalid message id: {0}")]
    InvalidId(String),

    /// A stored row contained an unexpected value.
    #[error("stored message is corrupt: {0}")]
    Corrupt(String),

    /// The Apalis job queue failed.
    #[error("apalis: {0}")]
    Apalis(String),
}

impl CoreError {
    /// Whether retrying the same delivery could succeed.
    #[must_use]
    pub fn is_transient_delivery(&self) -> bool {
        match self {
            Self::Transport(error) => error.is_transient(),
            Self::Protocol(error) => error.is_transient(),
            Self::Io(_) => true,
            _ => false,
        }
    }
}

impl From<apalis_sqlite::Error> for CoreError {
    fn from(error: apalis_sqlite::Error) -> Self {
        Self::Apalis(error.to_string())
    }
}
