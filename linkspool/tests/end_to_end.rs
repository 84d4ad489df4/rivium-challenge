//! End-to-end delivery between the core and the companion sink.
//!
//! Each test binds the sink to an ephemeral loopback port, so it exercises the
//! real framed TCP protocol instead of a mock transport.

use std::{net::SocketAddr, sync::Arc};

use linkspool_core::{Config, Core, DeliveryJob, MessageState, NewMessage};
use linkspool_protocol::{
    Ack, DEFAULT_MAX_FRAME_BYTES, decode_envelope, encode_ack, read_frame, write_frame,
};
use linkspool_sink::{Sink, SinkStats, serve};
use serde_json::json;
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};

/// One running sink plus the resources that keep it alive.
struct Receiver {
    address: SocketAddr,
    sink: Arc<Sink>,
    task: JoinHandle<()>,
    _directory: TempDir,
}

impl Receiver {
    async fn start(drop_ack: bool) -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let sink = Arc::new(
            Sink::open(&directory.path().join("sink.db"), drop_ack)
                .await
                .expect("sink database"),
        );
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ephemeral listener");
        let address = listener.local_addr().expect("local address");
        let task_sink = sink.clone();
        let task = tokio::spawn(async move {
            let _ = serve(listener, task_sink, DEFAULT_MAX_FRAME_BYTES).await;
        });
        Self {
            address,
            sink,
            task,
            _directory: directory,
        }
    }

    async fn totals(&self) -> SinkStats {
        self.sink.totals().await.expect("sink totals")
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Test core plus the temporary directory that owns its database.
struct Harness {
    core: Core,
    _directory: TempDir,
}

impl Harness {
    fn new(address: SocketAddr) -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let mut config = Config::default();
        config.database.path = directory.path().join("linkspool.db");
        config.transport.sink_addr = address;
        config.transport.connect_timeout_ms = 1_000;
        config.transport.ack_timeout_ms = 500;
        Self {
            core: Core::new(config),
            _directory: directory,
        }
    }
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

async fn deliver(core: &Core, id: &str, attempt: usize) {
    core.deliver(DeliveryJob {
        message_id: id.into(),
        attempt,
    })
    .await
    .expect("delivery runs");
}

#[tokio::test]
async fn accepted_message_is_delivered_and_recorded_once() {
    let receiver = Receiver::start(false).await;
    let harness = Harness::new(receiver.address);
    let core = &harness.core;
    let submitted = core.submit(message("event-001")).await.unwrap().message;

    deliver(core, &submitted.id, 1).await;

    let stored = core.message(&submitted.id).await.unwrap().unwrap();
    assert_eq!(stored.state, MessageState::Delivered);
    assert_eq!(stored.attempts, 1);
    assert!(stored.delivered_at.is_some());

    let totals = receiver.totals().await;
    assert_eq!(totals.received, 1);
    assert_eq!(totals.duplicates, 0);

    // A repeated job for a terminal message is acknowledged without sending.
    deliver(&core, &submitted.id, 2).await;
    let totals = receiver.totals().await;
    assert_eq!(totals.received, 1);
    assert_eq!(totals.duplicates, 0);
}

#[tokio::test]
async fn lost_acknowledgement_is_retried_and_deduplicated() {
    let receiver = Receiver::start(true).await;
    let harness = Harness::new(receiver.address);
    let core = &harness.core;
    let submitted = core.submit(message("event-001")).await.unwrap().message;

    deliver(core, &submitted.id, 1).await;

    let retrying = core.message(&submitted.id).await.unwrap().unwrap();
    assert_eq!(retrying.state, MessageState::RetryScheduled);
    assert_eq!(retrying.attempts, 1);
    assert!(
        retrying
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("timed out"))
    );
    assert_eq!(receiver.totals().await.received, 1);

    deliver(core, &submitted.id, 2).await;

    let delivered = core.message(&submitted.id).await.unwrap().unwrap();
    assert_eq!(delivered.state, MessageState::Delivered);
    assert_eq!(delivered.attempts, 2);

    let totals = receiver.totals().await;
    assert_eq!(totals.received, 1);
    assert_eq!(totals.duplicates, 1);
}

#[tokio::test]
async fn receiver_rejection_dead_letters_the_message() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let payload = read_frame(&mut stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap();
        let envelope = decode_envelope(&payload).unwrap();
        let ack = Ack::rejected(&envelope.message_id, "payload rejected by policy");
        let encoded = encode_ack(&ack).unwrap();
        write_frame(&mut stream, &encoded, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap();
    });

    let harness = Harness::new(address);
    let core = &harness.core;
    let submitted = core.submit(message("event-001")).await.unwrap().message;
    deliver(core, &submitted.id, 1).await;

    let stored = core.message(&submitted.id).await.unwrap().unwrap();
    assert_eq!(stored.state, MessageState::DeadLettered);
    assert_eq!(
        stored.last_error.as_deref(),
        Some("payload rejected by policy")
    );

    server.await.unwrap();
}

#[tokio::test]
async fn sink_database_survives_reopening() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sink.db");

    let first = Sink::open(&path, false).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve(
        listener,
        Arc::new(first),
        DEFAULT_MAX_FRAME_BYTES,
    ));

    let harness = Harness::new(address);
    let core = &harness.core;
    let submitted = core.submit(message("event-001")).await.unwrap().message;
    deliver(core, &submitted.id, 1).await;
    task.abort();

    let reopened = Sink::open(&path, false).await.unwrap();
    let totals = reopened.totals().await.unwrap();
    assert_eq!(totals.received, 1);
    assert_eq!(totals.duplicates, 0);
}
