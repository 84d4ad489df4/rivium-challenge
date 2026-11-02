use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Version of the envelope and acknowledgement formats.
pub const PROTOCOL_VERSION: u8 = 1;

/// A message transmitted from LinkSpool to a receiver.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Envelope {
    /// Wire protocol version.
    pub version: u8,
    /// Stable identifier assigned by LinkSpool.
    pub message_id: String,
    /// Producer-supplied deduplication key, when one was supplied.
    pub idempotency_key: Option<String>,
    /// Creation time in Unix milliseconds.
    pub created_at: i64,
    /// Optional expiry time in Unix milliseconds.
    pub expires_at: Option<i64>,
    /// Application payload.
    pub payload: Value,
}

impl Envelope {
    /// Validate the fields that the receiver relies on for routing and dedup.
    pub fn validate(&self) -> Result<(), crate::ProtocolError> {
        if self.version != PROTOCOL_VERSION {
            return Err(crate::ProtocolError::UnsupportedVersion(self.version));
        }
        if self.message_id.is_empty() || self.message_id.len() > 128 {
            return Err(crate::ProtocolError::InvalidEnvelope(
                "message_id must contain 1 to 128 bytes",
            ));
        }
        if self
            .idempotency_key
            .as_ref()
            .is_some_and(|key| key.len() > 256)
        {
            return Err(crate::ProtocolError::InvalidEnvelope(
                "idempotency_key must not exceed 256 bytes",
            ));
        }
        Ok(())
    }
}
