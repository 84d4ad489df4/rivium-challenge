use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{Ack, Envelope, LENGTH_PREFIX_BYTES, ProtocolError};

/// Default maximum frame payload size: one mebibyte.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Serialize and validate an envelope.
pub fn encode_envelope(envelope: &Envelope) -> Result<Vec<u8>, ProtocolError> {
    envelope.validate()?;
    Ok(serde_json::to_vec(envelope)?)
}

/// Parse and validate an envelope.
pub fn decode_envelope(payload: &[u8]) -> Result<Envelope, ProtocolError> {
    let envelope: Envelope = serde_json::from_slice(payload)?;
    envelope.validate()?;
    Ok(envelope)
}

/// Serialize and validate an acknowledgement.
pub fn encode_ack(ack: &Ack) -> Result<Vec<u8>, ProtocolError> {
    ack.validate()?;
    Ok(serde_json::to_vec(ack)?)
}

/// Parse and validate an acknowledgement.
pub fn decode_ack(payload: &[u8]) -> Result<Ack, ProtocolError> {
    let ack: Ack = serde_json::from_slice(payload)?;
    ack.validate()?;
    Ok(ack)
}

/// Write one length-prefixed frame.
pub async fn write_frame<W>(
    writer: &mut W,
    payload: &[u8],
    max_frame_bytes: usize,
) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    if payload.is_empty() {
        return Err(ProtocolError::EmptyFrame);
    }
    if payload.len() > max_frame_bytes {
        return Err(ProtocolError::FrameTooLarge {
            size: payload.len(),
            max: max_frame_bytes,
        });
    }
    let length = u32::try_from(payload.len()).map_err(|_| ProtocolError::PayloadTooLarge)?;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

/// Read one length-prefixed frame.
///
/// Returns `Ok(None)` only when the peer closes the stream between frames.
/// A stream that ends inside a frame produces [`ProtocolError::UnexpectedEof`].
pub async fn read_frame<R>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<Option<Vec<u8>>, ProtocolError>
where
    R: AsyncRead + Unpin,
{
    let mut prefix = [0_u8; LENGTH_PREFIX_BYTES];
    let read = reader.read(&mut prefix[..1]).await?;
    if read == 0 {
        return Ok(None);
    }
    if let Err(error) = reader.read_exact(&mut prefix[1..]).await {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            return Err(ProtocolError::UnexpectedEof);
        }
        return Err(ProtocolError::Io(error));
    }

    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 {
        return Err(ProtocolError::EmptyFrame);
    }
    if length > max_frame_bytes {
        return Err(ProtocolError::FrameTooLarge {
            size: length,
            max: max_frame_bytes,
        });
    }

    let mut payload = vec![0_u8; length];
    if let Err(error) = reader.read_exact(&mut payload).await {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            return Err(ProtocolError::UnexpectedEof);
        }
        return Err(ProtocolError::Io(error));
    }
    Ok(Some(payload))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::io::duplex;

    use super::*;
    use crate::{AckStatus, PROTOCOL_VERSION};

    fn envelope() -> Envelope {
        Envelope {
            version: PROTOCOL_VERSION,
            message_id: "01TEST".into(),
            idempotency_key: Some("key-1".into()),
            created_at: 1_760_000_000_000,
            expires_at: None,
            payload: json!({ "event": "device_status", "status": "online" }),
        }
    }

    #[tokio::test]
    async fn frame_round_trip() {
        let (mut client, mut server) = duplex(1024);
        let encoded = encode_envelope(&envelope()).unwrap();
        let write = tokio::spawn(async move {
            write_frame(&mut client, &encoded, 1024).await.unwrap();
        });

        let payload = read_frame(&mut server, 1024).await.unwrap().unwrap();
        assert_eq!(decode_envelope(&payload).unwrap(), envelope());
        write.await.unwrap();
    }

    #[tokio::test]
    async fn clean_eof_is_not_an_error() {
        let (client, mut server) = duplex(16);
        drop(client);
        assert!(read_frame(&mut server, 1024).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn oversized_frame_is_rejected_before_allocation() {
        let (mut client, mut server) = duplex(16);
        tokio::spawn(async move {
            client.write_all(&8_u32.to_be_bytes()).await.unwrap();
        });
        let error = read_frame(&mut server, 4).await.unwrap_err();
        assert!(matches!(error, ProtocolError::FrameTooLarge { .. }));
    }

    #[tokio::test]
    async fn zero_length_frame_is_rejected() {
        let (mut client, mut server) = duplex(16);
        tokio::spawn(async move {
            client.write_all(&0_u32.to_be_bytes()).await.unwrap();
        });
        let error = read_frame(&mut server, 1024).await.unwrap_err();
        assert!(matches!(error, ProtocolError::EmptyFrame));
    }

    #[test]
    fn envelope_and_ack_validate_version() {
        let mut value = envelope();
        value.version = 2;
        assert!(matches!(
            encode_envelope(&value),
            Err(ProtocolError::UnsupportedVersion(2))
        ));

        let mut ack = Ack::accepted("01TEST");
        ack.version = 2;
        assert!(matches!(
            encode_ack(&ack),
            Err(ProtocolError::UnsupportedVersion(2))
        ));
        assert_eq!(Ack::accepted("01TEST").status, AckStatus::Accepted);
    }
}
