//! Loopback HTTP producer API.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{
        FromRequest, Path, Query, Request, State, rejection::JsonRejection,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use linkspool_core::{ApiConfig, Core, CoreError, Message, MessageState, NewMessage};
use rivium::{Kind, Service};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::internal;

/// Build the loopback HTTP API service.
pub fn service(core: Arc<Core>, config: ApiConfig) -> Box<dyn Service> {
    rivium::service("api", Kind::Frontline, move |context| async move {
        core.open().await.map_err(internal)?;
        let listener = tokio::net::TcpListener::bind(config.listen)
            .await
            .map_err(internal)?;
        context.ready();
        tracing::info!(address = %config.listen, "HTTP API listening");

        axum::serve(listener, router(core))
            .with_graceful_shutdown(async move { context.stopped().await })
            .await
            .map_err(internal)
    })
}

/// Build the router independently of the listener so tests can exercise it.
pub fn router(core: Arc<Core>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/messages", post(submit).get(list))
        .route("/v1/messages/{id}", get(fetch).delete(discard))
        .route("/v1/messages/{id}/retry", post(retry))
        .with_state(ApiState { core })
}

#[derive(Clone)]
struct ApiState {
    core: Arc<Core>,
}

/// JSON body extractor that renders rejections in the API error shape.
struct ApiJson<T>(T);

impl<T, S> FromRequest<S> for ApiJson<T>
where
    Json<T>: FromRequest<S, Rejection = JsonRejection>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(request, state).await {
            Ok(Json(value)) => Ok(Self(value)),
            Err(rejection) => Err(ApiError::invalid(rejection.body_text())),
        }
    }
}

#[derive(Debug, Deserialize)]
struct SubmitRequest {
    #[serde(default)]
    producer: Option<String>,
    #[serde(default)]
    destination: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
    payload: Value,
    #[serde(default)]
    expires_at: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SubmitResponse {
    message: Message,
    duplicate: bool,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ListResponse {
    messages: Vec<Message>,
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn submit(
    State(state): State<ApiState>,
    ApiJson(request): ApiJson<SubmitRequest>,
) -> Result<(StatusCode, Json<SubmitResponse>), ApiError> {
    let destination = request
        .destination
        .unwrap_or_else(|| state.core.config().queue.destination.clone());
    let producer = request
        .producer
        .unwrap_or_else(|| "desktop-api".to_owned());
    let new = NewMessage {
        producer,
        destination,
        idempotency_key: request.idempotency_key,
        payload: request.payload,
        expires_at: request.expires_at,
    };

    let submission = state.core.submit(new).await?;
    let status = if submission.duplicate {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((
        status,
        Json(SubmitResponse {
            message: submission.message,
            duplicate: submission.duplicate,
        }),
    ))
}

async fn list(
    State(state): State<ApiState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<ListResponse>, ApiError> {
    let state_filter = match query.state.as_deref() {
        None | Some("") => None,
        Some(value) => Some(
            MessageState::parse(value)
                .ok_or_else(|| ApiError::invalid(format!("unknown message state: {value}")))?,
        ),
    };
    let messages = state
        .core
        .list(state_filter, query.limit.unwrap_or(50))
        .await?;
    Ok(Json(ListResponse { messages }))
}

async fn fetch(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<Message>, ApiError> {
    state
        .core
        .message(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("message {id} not found")))
}

async fn retry(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<Message>, ApiError> {
    Ok(Json(state.core.retry(&id).await?))
}

async fn discard(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state.core.discard(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: message.into(),
        }
    }
}

impl From<CoreError> for ApiError {
    fn from(error: CoreError) -> Self {
        match error {
            CoreError::Invalid(message) => Self::invalid(message),
            CoreError::PayloadTooLarge { size, max } => Self {
                status: StatusCode::PAYLOAD_TOO_LARGE,
                code: "payload_too_large",
                message: format!("payload is {size} bytes, exceeding the {max} byte limit"),
            },
            CoreError::QueueFull => Self {
                status: StatusCode::TOO_MANY_REQUESTS,
                code: "queue_full",
                message: "queue capacity exhausted".into(),
            },
            CoreError::NotFound(id) => Self::not_found(format!("message {id} not found")),
            CoreError::Conflict(id) => Self {
                status: StatusCode::CONFLICT,
                code: "conflict",
                message: format!("message {id} is not dead-lettered"),
            },
            CoreError::InvalidId(id) => Self::invalid(format!("invalid message id: {id}")),
            other => {
                tracing::error!(error = %other, "request failed");
                Self {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: "internal",
                    message: "internal error".into(),
                }
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({
            "error": {
                "code": self.code,
                "message": self.message,
            }
        });
        (self.status, Json(body)).into_response()
    }
}
