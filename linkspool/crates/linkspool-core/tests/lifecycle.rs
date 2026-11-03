//! Lifecycle tests for ingestion, admission, expiry and recovery.
//!
//! These tests drive [`Core`] directly so they observe the authoritative
//! message state without depending on the Rivium supervisor or the delivery
//! worker.

use std::{net::TcpListener, path::Path};

use linkspool_core::{
    Claim, Config, Core, CoreError, DeliveryJob, MessageState, NewMessage,
};
use serde_json::json;
use tempfile::TempDir;

/// Test core plus the temporary directory that owns its database.
struct Harness {
    _directory: TempDir,
    core: Core,
}

impl Harness {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let core = Core::new(config(directory.path()));
        Self {
            _directory: directory,
            core,
        }
    }

    fn with_queue(queue: impl FnOnce(&mut linkspool_core::QueueConfig)) -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let mut config = config(directory.path());
        queue(&mut config.queue);
        Self {
            _directory: directory,
            core: Core::new(config),
        }
    }

    async fn submit(&self, key: &str) -> linkspool_core::Message {
        self.core
            .submit(message(key))
            .await
            .expect("submission succeeds")
            .message
    }
}

fn config(root: &Path) -> Config {
    let mut config = Config::default();
    config.database.path = root.join("linkspool.db");
    config.transport.sink_addr = unused_address();
    config.transport.connect_timeout_ms = 250;
    config.transport.ack_timeout_ms = 250;
    config.queue.retry_base_ms = 1_000;
    config
}

fn message(key: &str) -> NewMessage {
    NewMessage {
        producer: "tests".into(),
        destination: "local-sink".into(),
        idempotency_key: Some(key.into()),
        payload: json!({ "event": "device_status", "status": "online" }),
        expires_at: None,
    }
}

/// Reserve an ephemeral loopback port, then release it so connecting fails.
fn unused_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local address")
}

#[tokio::test]
async fn submit_persists_a_pending_message() {
    let harness = Harness::new();
    let submission = harness.core.submit(message("event-001")).await.unwrap();

    assert!(!submission.duplicate);
    assert_eq!(submission.message.state, MessageState::Pending);
    assert_eq!(submission.message.attempts, 0);
    assert_eq!(submission.message.producer, "tests");
    assert_eq!(submission.message.destination, "local-sink");

    let stored = harness
        .core
        .message(&submission.message.id)
        .await
        .unwrap()
        .expect("message is durable");
    assert_eq!(stored.id, submission.message.id);
    assert_eq!(stored.payload, message("event-001").payload);

    let counts = harness.core.counts().await.unwrap();
    assert_eq!(counts.get("pending"), Some(&1));
}

#[tokio::test]
async fn repeated_idempotency_key_returns_the_existing_message() {
    let harness = Harness::new();
    let first = harness.core.submit(message("event-001")).await.unwrap();
    let second = harness.core.submit(message("event-001")).await.unwrap();

    assert!(second.duplicate);
    assert_eq!(second.message.id, first.message.id);
    assert_eq!(harness.core.counts().await.unwrap().get("pending"), Some(&1));

    let mut other = message("event-001");
    other.producer = "another-producer".into();
    let third = harness.core.submit(other).await.unwrap();
    assert!(!third.duplicate);
    assert_ne!(third.message.id, first.message.id);
}

#[tokio::test]
async fn admission_control_bounds_message_count() {
    let harness = Harness::with_queue(|queue| queue.max_queued_messages = 1);
    harness.submit("event-001").await;

    let error = harness.core.submit(message("event-002")).await.unwrap_err();
    assert!(matches!(error, CoreError::QueueFull));
}

#[tokio::test]
async fn admission_control_bounds_queued_bytes() {
    let harness = Harness::with_queue(|queue| queue.max_queued_bytes = 8);

    let error = harness.core.submit(message("event-001")).await.unwrap_err();
    assert!(matches!(error, CoreError::QueueFull));
}

#[tokio::test]
async fn payload_limit_is_enforced_before_insertion() {
    let harness = Harness::with_queue(|queue| queue.max_payload_bytes = 4);

    let error = harness.core.submit(message("event-001")).await.unwrap_err();
    assert!(matches!(
        error,
        CoreError::PayloadTooLarge { max: 4, .. }
    ));
    assert!(harness.core.counts().await.unwrap().is_empty());
}

#[tokio::test]
async fn expired_messages_are_removed_from_delivery() {
    let harness = Harness::new();
    let now = linkspool_core::now_millis();
    let mut new = message("event-001");
    new.expires_at = Some(now + 60_000);
    let submitted = harness.core.submit(new).await.unwrap().message;

    let report = harness
        .core
        .open()
        .await
        .unwrap()
        .maintain(now + 120_000, 168, 300_000)
        .await
        .unwrap();
    assert_eq!(report.expired, 1);

    let stored = harness
        .core
        .message(&submitted.id)
        .await
        .unwrap()
        .expect("message remains inspectable");
    assert_eq!(stored.state, MessageState::Expired);
    assert_eq!(stored.last_error.as_deref(), Some("message expired"));
}

#[tokio::test]
async fn expired_submissions_are_rejected() {
    let harness = Harness::new();
    let mut new = message("event-001");
    new.expires_at = Some(linkspool_core::now_millis() - 1);

    let error = harness.core.submit(new).await.unwrap_err();
    assert!(matches!(error, CoreError::Invalid(_)));
}

#[tokio::test]
async fn transient_failures_schedule_backoff_then_dead_letter() {
    let harness = Harness::with_queue(|queue| queue.max_attempts = 2);
    let submitted = harness.submit("event-001").await;

    harness
        .core
        .deliver(DeliveryJob {
            message_id: submitted.id.clone(),
            attempt: 1,
        })
        .await
        .unwrap();

    let retrying = harness.core.message(&submitted.id).await.unwrap().unwrap();
    assert_eq!(retrying.state, MessageState::RetryScheduled);
    assert_eq!(retrying.attempts, 1);
    assert!(retrying.last_error.is_some());
    assert!(retrying.next_attempt_at.is_some());

    harness
        .core
        .deliver(DeliveryJob {
            message_id: submitted.id.clone(),
            attempt: 2,
        })
        .await
        .unwrap();

    let dead = harness.core.message(&submitted.id).await.unwrap().unwrap();
    assert_eq!(dead.state, MessageState::DeadLettered);
    assert_eq!(dead.attempts, 2);
    assert!(dead.last_error.is_some());
}

#[tokio::test]
async fn stale_delivery_jobs_are_skipped() {
    let harness = Harness::new();
    let submitted = harness.submit("event-001").await;

    harness
        .core
        .deliver(DeliveryJob {
            message_id: submitted.id.clone(),
            attempt: 2,
        })
        .await
        .unwrap();

    let stored = harness.core.message(&submitted.id).await.unwrap().unwrap();
    assert_eq!(stored.state, MessageState::Pending);
    assert_eq!(stored.attempts, 0);
}

#[tokio::test]
async fn interrupted_deliveries_are_recovered() {
    let harness = Harness::new();
    let submitted = harness.submit("event-001").await;
    let now = linkspool_core::now_millis();

    let store = harness.core.open().await.unwrap();
    let Claim::Ready(claimed) = store.claim(&submitted.id, 1, now).await.unwrap() else {
        panic!("first attempt should be claimable");
    };
    assert_eq!(claimed.state, MessageState::Delivering);
    assert_eq!(claimed.attempts, 1);

    let recovered = store
        .recover_stale_deliveries(now + 60_001, 60_000)
        .await
        .unwrap();
    assert_eq!(recovered, 1);

    let stored = harness.core.message(&submitted.id).await.unwrap().unwrap();
    assert_eq!(stored.state, MessageState::RetryScheduled);
    assert_eq!(stored.attempts, 0);
    assert_eq!(
        stored.last_error.as_deref(),
        Some("delivery interrupted")
    );
}

#[tokio::test]
async fn dead_letters_can_be_retried_or_discarded() {
    let harness = Harness::with_queue(|queue| queue.max_attempts = 1);
    let submitted = harness.submit("event-001").await;
    harness
        .core
        .deliver(DeliveryJob {
            message_id: submitted.id.clone(),
            attempt: 1,
        })
        .await
        .unwrap();

    let retried = harness.core.retry(&submitted.id).await.unwrap();
    assert_eq!(retried.state, MessageState::RetryScheduled);
    assert_eq!(retried.attempts, 1);
    assert_eq!(retried.max_attempts, 2);

    let conflict = harness.core.discard(&submitted.id).await.unwrap_err();
    assert!(matches!(conflict, CoreError::Conflict(_)));

    harness
        .core
        .deliver(DeliveryJob {
            message_id: submitted.id.clone(),
            attempt: 2,
        })
        .await
        .unwrap();
    let dead = harness.core.message(&submitted.id).await.unwrap().unwrap();
    assert_eq!(dead.state, MessageState::DeadLettered);

    harness.core.discard(&submitted.id).await.unwrap();
    assert!(harness.core.message(&submitted.id).await.unwrap().is_none());
}

#[tokio::test]
async fn counts_group_messages_by_state() {
    let harness = Harness::with_queue(|queue| queue.max_attempts = 1);
    let first = harness.submit("event-001").await;
    harness.submit("event-002").await;

    harness
        .core
        .deliver(DeliveryJob {
            message_id: first.id,
            attempt: 1,
        })
        .await
        .unwrap();

    let counts = harness.core.counts().await.unwrap();
    assert_eq!(counts.get("pending"), Some(&1));
    assert_eq!(counts.get("dead_lettered"), Some(&1));
}

#[tokio::test]
async fn list_filters_by_state_and_respects_the_limit() {
    let harness = Harness::new();
    harness.submit("event-001").await;
    harness.submit("event-002").await;

    let all = harness.core.list(None, 10).await.unwrap();
    assert_eq!(all.len(), 2);

    let pending = harness
        .core
        .list(Some(MessageState::Pending), 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 2);

    let limited = harness.core.list(None, 1).await.unwrap();
    assert_eq!(limited.len(), 1);
}

#[test]
fn unused_address_is_loopback() {
    let address = unused_address();
    assert!(address.ip().is_loopback());
    assert_ne!(address.port(), 0);
}
