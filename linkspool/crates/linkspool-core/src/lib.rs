//! Persistence, ingestion, delivery and transport for LinkSpool.
//!
//! This crate owns the authoritative message record and the delivery policy.
//! Hosts (desktop and Android) only translate their producer interface into
//! [`Core::submit`] calls and supervise [`Core::deliver`] jobs.

mod config;
mod core;
mod error;
mod jobs;
mod message;
mod persistence;
mod transport;

pub use config::{
    ApiConfig, Config, DatabaseConfig, InboxConfig, MaintenanceConfig, QueueConfig, StatusConfig,
    TransportConfig,
};
pub use core::{Core, now_millis, shared};
pub use error::CoreError;
pub use jobs::{DELIVERY_QUEUE, DeliveryJob};
pub use message::{Claim, Message, MessageState, NewMessage, Submission};
pub use persistence::{MaintenanceReport, Store};
pub use transport::TransportError;
