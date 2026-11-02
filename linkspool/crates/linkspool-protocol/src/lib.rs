//! LinkSpool wire protocol.
//!
//! Messages are transported as JSON envelopes inside four-byte big-endian
//! length-prefixed frames. The framing layer is independent of the JSON
//! encoding so a binary format can replace it later without changing the
//! transport loop.

mod ack;
mod envelope;
mod error;
mod frame;

pub use ack::{Ack, AckStatus};
pub use envelope::{Envelope, PROTOCOL_VERSION};
pub use error::ProtocolError;
pub use frame::{
    DEFAULT_MAX_FRAME_BYTES, decode_ack, decode_envelope, encode_ack, encode_envelope, read_frame,
    write_frame,
};

/// Size of the frame length prefix.
pub const LENGTH_PREFIX_BYTES: usize = 4;
