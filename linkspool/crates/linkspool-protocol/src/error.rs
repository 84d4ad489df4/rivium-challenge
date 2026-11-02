use std::{fmt, io};

/// Errors produced while framing or parsing LinkSpool messages.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// The peer closed the connection in the middle of a frame.
    #[error("unexpected end of stream while reading a frame")]
    UnexpectedEof,

    /// A frame declared zero payload bytes.
    #[error("empty frame")]
    EmptyFrame,

    /// A frame exceeded the configured maximum payload size.
    #[error("frame of {size} bytes exceeds the maximum of {max} bytes")]
    FrameTooLarge {
        /// Declared frame payload size.
        size: usize,
        /// Configured maximum payload size.
        max: usize,
    },

    /// The envelope or acknowledgement used an unsupported protocol version.
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u8),

    /// The envelope was structurally invalid.
    #[error("invalid envelope: {0}")]
    InvalidEnvelope(&'static str),

    /// The acknowledgement was structurally invalid.
    #[error("invalid acknowledgement: {0}")]
    InvalidAck(&'static str),

    /// JSON encoding or decoding failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// The underlying socket failed.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// A payload could not fit in the protocol's length prefix.
    #[error("payload is too large for a frame")]
    PayloadTooLarge,
}

impl ProtocolError {
    /// Whether retrying the same message could succeed.
    ///
    /// Structural errors are permanent; socket errors are transient.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Io(_) | Self::UnexpectedEof)
    }
}

impl From<fmt::Error> for ProtocolError {
    fn from(_: fmt::Error) -> Self {
        Self::InvalidEnvelope("formatting failed")
    }
}
