//! HTTP API contract tests.
//!
//! The router is exercised directly through `tower`, so the tests cover
//! request parsing, status codes and error mapping without binding a socket.

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use linkspool::api;
use linkspool_core::{Config, Core};
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

struct Harness {
    router: Router,
    _directory: TempDir,
}

impl Harness {
    fn new(configure: impl FnOnce(&mut Config)) -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let mut config = Config::default();
        config.database.path = directory.path().join("linkspool.db");
        config.transport.sink_addr = "127.0.0.1:1".parse().expect("valid address");
        configure(&mut config);
        Self {
            router: api::router(Arc::new(Core::new(config))),
            _directory: directory,
        }
    }
}

async fn send(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.expect("router responds");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("body is readable");
    let value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).expect("body is JSON")
    };
    (status, value)
}

fn submit(body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("request builds")
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .expect("request builds")
}

fn delete(uri: &str) -> Request<Body> {
    Request::builder()
        .method("DELETE")
        .uri(uri)
        .body(Body::empty())
        .expect("request builds")
}

fn post(uri: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .body(Body::empty())
        .expect("request builds")
}

fn payload(key: &str) -> Value {
    json!({
        "idempotency_key": key,
        "payload": { "event": "device_status", "status": "online" }
    })
}

#[tokio::test]
async fn health_endpoint_reports_ok() {
    let harness = Harness::new(|_| {});
    let (status, body) = send(&harness.router, get("/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "status": "ok" }));
}

#[tokio::test]
async fn submission_returns_created_then_ok_for_a_duplicate() {
    let harness = Harness::new(|_| {});

    let (status, first) = send(&harness.router, submit(payload("event-001"))).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(first["duplicate"], false);
    let id = first["message"]["id"].as_str().expect("message id").to_owned();
    assert_eq!(first["message"]["state"], "pending");

    let (status, second) = send(&harness.router, submit(payload("event-001"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["duplicate"], true);
    assert_eq!(second["message"]["id"], id);
}

#[tokio::test]
async fn message_can_be_fetched_and_listed() {
    let harness = Harness::new(|_| {});
    let (_, created) = send(&harness.router, submit(payload("event-001"))).await;
    let id = created["message"]["id"].as_str().unwrap().to_owned();

    let (status, fetched) = send(&harness.router, get(&format!("/v1/messages/{id}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["id"], id);

    let (status, listed) = send(&harness.router, get("/v1/messages?state=pending&limit=5")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(listed["messages"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn unknown_state_is_a_bad_request() {
    let harness = Harness::new(|_| {});
    let (status, body) = send(&harness.router, get("/v1/messages?state=unknown")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "invalid_request");
}

#[tokio::test]
async fn missing_message_is_not_found() {
    let harness = Harness::new(|_| {});
    let (status, body) = send(&harness.router, get("/v1/messages/01MISSING")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "not_found");
}

#[tokio::test]
async fn pending_message_cannot_be_discarded_or_retried() {
    let harness = Harness::new(|_| {});
    let (_, created) = send(&harness.router, submit(payload("event-001"))).await;
    let id = created["message"]["id"].as_str().unwrap().to_owned();

    let (status, body) = send(&harness.router, delete(&format!("/v1/messages/{id}"))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], "conflict");

    let (status, body) = send(&harness.router, post(&format!("/v1/messages/{id}/retry"))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], "conflict");
}

#[tokio::test]
async fn oversized_payload_is_rejected() {
    let harness = Harness::new(|config| config.queue.max_payload_bytes = 4);
    let (status, body) = send(&harness.router, submit(payload("event-001"))).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["error"]["code"], "payload_too_large");
}

#[tokio::test]
async fn exhausted_queue_is_rejected() {
    let harness = Harness::new(|config| config.queue.max_queued_messages = 1);
    let (status, _) = send(&harness.router, submit(payload("event-001"))).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = send(&harness.router, submit(payload("event-002"))).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["error"]["code"], "queue_full");
}

#[tokio::test]
async fn invalid_payload_shape_is_a_bad_request() {
    let harness = Harness::new(|_| {});
    let (status, body) = send(
        &harness.router,
        submit(json!({ "idempotency_key": "event-001" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "invalid_request");
}
