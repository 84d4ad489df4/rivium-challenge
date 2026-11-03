//! LinkSpool desktop host.
//!
//! The host translates the producer interfaces (loopback HTTP, file inbox) into
//! [`linkspool_core::Core`] calls and supervises delivery, maintenance and
//! observability services under Rivium.

pub mod api;
pub mod app;
pub mod delivery;
pub mod inbox;
pub mod maintenance;
pub mod status;

/// Map any host-level failure into a Rivium internal error with its source.
pub(crate) fn internal(error: impl std::error::Error + Send + Sync + 'static) -> rivium::Error {
    rivium::Error::internal(error.to_string()).with_source(error)
}
