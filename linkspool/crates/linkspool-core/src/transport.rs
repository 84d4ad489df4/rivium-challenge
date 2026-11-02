//! TCP transport for framed envelopes and acknowledgements.

use std::{io, net::SocketAddr, time::Duration};

use linkspool_protocol::{
    Ack, Envelope, ProtocolError, decode_ack, encode_envelope, read_frame, write_frame,
};
use tokio::{net::TcpStream, time::timeout};

use crate::TransportConfig;

/// Failure to exchange one message with the receiver.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The wire protocol rejected a frame or acknowledgement.
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// The socket failed.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// The TCP connection was not established in time.
    #[error("connection to {addr} timed out")]
    ConnectTimeout {
        /// Receiver address.
        addr: SocketAddr,
    },

    /// The acknowledgement did not arrive in time.
    #[error("acknowledgement timed out")]
    AckTimeout,

    /// The receiver acknowledged a different message.
    #[error("acknowledgement was for message {actual}, expected {expected}")]
    MessageMismatch {
        /// Identifier the sender transmitted.
        expected: String,
        /// Identifier in the acknowledgement.
        actual: String,
    },
}

impl TransportError {
    /// Whether retrying the same delivery could succeed.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Protocol(error) => error.is_transient(),
            Self::Io(_) | Self::ConnectTimeout { .. } | Self::AckTimeout => true,
            Self::MessageMismatch { .. } => false,
        }
    }
}

/// Send one envelope and wait for the receiver's acknowledgement.
pub async fn send(envelope: &Envelope, config: &TransportConfig) -> Result<Ack, TransportError> {
    let payload = encode_envelope(envelope)?;

    let mut stream = timeout(
        Duration::from_millis(config.connect_timeout_ms),
        TcpStream::connect(config.sink_addr),
    )
    .await
    .map_err(|_| TransportError::ConnectTimeout {
        addr: config.sink_addr,
    })??;

    let exchange = async {
        write_frame(&mut stream, &payload, config.max_frame_bytes).await?;
        let reply = read_frame(&mut stream, config.max_frame_bytes)
            .await?
            .ok_or(ProtocolError::UnexpectedEof)?;
        let ack = decode_ack(&reply)?;
        if ack.message_id != envelope.message_id {
            return Err(TransportError::MessageMismatch {
                expected: envelope.message_id.clone(),
                actual: ack.message_id.clone(),
            });
        }
        Ok(ack)
    };

    timeout(Duration::from_millis(config.ack_timeout_ms), exchange)
        .await
        .map_err(|_| TransportError::AckTimeout)?
}
