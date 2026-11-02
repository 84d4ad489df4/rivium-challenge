use serde::{Deserialize, Serialize};

use crate::PROTOCOL_VERSION;

/// Receiver response to one message.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Ack {
    /// Wire protocol version.
    pub version: u8,
    /// Identifier of the message being acknowledged.
    pub message_id: String,
    /// Outcome recorded by the receiver.
    pub status: AckStatus,
    /// Optional human-readable explanation for a rejection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Receiver-side outcome for a message.
#[derive(Clone, Copy, Debug, Eq, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AckStatus {
    /// The message was durably recorded for the first time.
    Accepted,
    /// The message was already recorded; the receiver did not repeat its effect.
    Duplicate,
    /// The receiver rejected the message permanently.
    Rejected,
}

impl Ack {
    /// Build an acceptance acknowledgement.
    #[must_use]
    pub fn accepted(message_id: impl Into<String>) -> Self {
        Self::new(message_id, AckStatus::Accepted, None)
    }

    /// Build a duplicate acknowledgement.
    #[must_use]
    pub fn duplicate(message_id: impl Into<String>) -> Self {
        Self::new(message_id, AckStatus::Duplicate, None)
    }

    /// Build a permanent rejection acknowledgement.
    #[must_use]
    pub fn rejected(message_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::new(message_id, AckStatus::Rejected, Some(reason.into()))
    }

    fn new(message_id: impl Into<String>, status: AckStatus, reason: Option<String>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            message_id: message_id.into(),
            status,
            reason,
        }
    }

    /// Validate the version and identifier carried by an acknowledgement.
    pub fn validate(&self) -> Result<(), crate::ProtocolError> {
        if self.version != PROTOCOL_VERSION {
            return Err(crate::ProtocolError::UnsupportedVersion(self.version));
        }
        if self.message_id.len() > 128 {
            return Err(crate::ProtocolError::InvalidAck(
                "message_id must not exceed 128 bytes",
            ));
        }
        Ok(())
    }
}
