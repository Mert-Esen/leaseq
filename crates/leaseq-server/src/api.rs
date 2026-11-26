//! Request handlers.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{FromRequest, FromRequestParts, Path, State};
use axum::http::{StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use futures_util::{Stream, StreamExt, stream};
use leaseq_engine::core::JobSpec;
use leaseq_engine::{JobFilter, JobRecord, LeasedJob};
use leaseq_proto::{
    AckRequest, Delivery, Empty, EnqueueRequest, EnqueueResponse, HeartbeatRequest, JobId, JobInfo,
    JobsQuery, JobsResponse, LeaseRequest, LeaseResponse, MAX_WAIT_MS, NackRequest, QueueName,
    RawValue, StatsResponse, Token,
};

use crate::AppState;
use crate::error::ApiError;

const DEFAULT_LIST: usize = 100;
const MAX_LIST: usize = 500;
/// Event streams send at most one update per interval.
const EVENT_INTERVAL: Duration = Duration::from_millis(250);

/// [`axum::Json`] with this API's error format.
#[derive(FromRequest)]
#[from_request(via(axum::Json), rejection(ApiError))]
pub(crate) struct Json<T>(T);

impl<T: serde::Serialize> IntoResponse for Json<T> {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self.0).into_response()
    }
}

/// [`axum::extract::Query`] with this API's error format.
#[derive(FromRequestParts)]
#[from_request(via(axum::extract::Query), rejection(ApiError))]
pub(crate) struct Query<T>(T);

type Id = Result<Path<u64>, axum::extract::rejection::PathRejection>;

fn job_id(path: Id) -> Result<JobId, ApiError> {
    Ok(JobId(path?.0))
}

fn queue_name(name: &str) -> Result<QueueName, ApiError> {
    name.parse()
        .map_err(|error: leaseq_engine::core::InvalidQueueName| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                error.to_string(),
            )
        })
}

pub(crate) async fn enqueue(
    State(app): State<AppState>,
    Path(queue): Path<String>,
    Json(request): Json<EnqueueRequest>,
) -> Result<(StatusCode, Json<EnqueueResponse>), ApiError> {
    let spec = JobSpec {
        queue: queue_name(&queue)?,
        payload: request.payload.get().to_owned(),
        priority: request.priority,
        max_attempts: request.max_attempts,
    };
    let delay = Duration::from_millis(request.delay_ms);
    let id = app.engine.enqueue(spec, delay).await?;
    Ok((StatusCode::CREATED, Json(EnqueueResponse { id })))
}

pub(crate) async fn lease(
    State(app): State<AppState>,
    Path(queue): Path<String>,
    Json(request): Json<LeaseRequest>,
) -> Result<Json<LeaseResponse>, ApiError> {
    let queue = queue_name(&queue)?;
    let ttl = Duration::from_millis(request.lease_ms);
    let wait = Duration::from_millis(request.wait_ms.min(MAX_WAIT_MS));
    let leased = tokio::select! {
        // Prefer a finished lease so granted jobs are not dropped on shutdown.
        biased;
        leased = app.engine.lease(queue, request.max, ttl, wait) => leased?,
        () = app.closing.cancelled() => Vec::new(),
    };
    let jobs = leased.into_iter().map(delivery).collect::<Result<_, _>>()?;
    Ok(Json(LeaseResponse { jobs }))
}

fn delivery(job: LeasedJob) -> Result<Delivery, ApiError> {
    // Payloads written through this API are valid JSON by construction.
    let payload = RawValue::from_string(job.payload).map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "corrupt_payload",
            "stored payload is not JSON",
        )
    })?;
    Ok(Delivery {
        id: job.id,
        token: Token(job.token),
        queue: job.queue,
        payload,
        attempt: job.attempt,
        expires_at_ms: job.expires_at.as_millis(),
    })
}

pub(crate) async fn ack(
    State(app): State<AppState>,
    id: Id,
    Json(request): Json<AckRequest>,
) -> Result<StatusCode, ApiError> {
    app.engine.ack(job_id(id)?, request.token.0).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn nack(
    State(app): State<AppState>,
    id: Id,
    Json(request): Json<NackRequest>,
) -> Result<StatusCode, ApiError> {
    app.engine
        .nack(job_id(id)?, request.token.0, request.error)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn heartbeat(
    State(app): State<AppState>,
    id: Id,
    Json(request): Json<HeartbeatRequest>,
) -> Result<StatusCode, ApiError> {
    let ttl = Duration::from_millis(request.lease_ms);
    app.engine.extend(job_id(id)?, request.token.0, ttl).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn retry(
    State(app): State<AppState>,
    id: Id,
    Json(Empty {}): Json<Empty>,
) -> Result<StatusCode, ApiError> {
    app.engine.revive(job_id(id)?).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn jobs(
    State(app): State<AppState>,
    Query(query): Query<JobsQuery>,
) -> Result<Json<JobsResponse>, ApiError> {
    let filter = JobFilter {
        queue: query.queue,
        status: query.state,
        limit: query.limit.unwrap_or(DEFAULT_LIST).clamp(1, MAX_LIST),
    };
    let jobs = app.engine.list(filter).await?;
    Ok(Json(JobsResponse {
        jobs: jobs.into_iter().map(info).collect(),
    }))
}

fn info(job: JobRecord) -> JobInfo {
    JobInfo {
        id: job.id,
        queue: job.queue,
        state: job.status,
        priority: job.priority,
        attempts: job.attempts,
        max_attempts: job.max_attempts,
        last_error: job.last_error,
        payload: job.payload,
    }
}

pub(crate) async fn stats(State(app): State<AppState>) -> Json<StatsResponse> {
    Json(StatsResponse {
        queues: app.engine.stats().to_vec(),
    })
}

/// Sends the current counters, then an update after every change.
pub(crate) async fn events(
    State(app): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let updates = stream::unfold(
        (app.engine.watch_stats(), true),
        |(mut receiver, first)| async move {
            if !first {
                tokio::time::sleep(EVENT_INTERVAL).await;
                receiver.changed().await.ok()?;
            }
            let queues = receiver.borrow_and_update().to_vec();
            let event = Event::default()
                .event("stats")
                .json_data(StatsResponse { queues })
                .ok()?;
            Some((Ok(event), (receiver, false)))
        },
    );
    Sse::new(updates.take_until(app.closing.cancelled_owned())).keep_alive(KeepAlive::default())
}

pub(crate) async fn health(State(app): State<AppState>) -> Result<&'static str, ApiError> {
    app.engine.tick().await?;
    Ok("ok")
}

pub(crate) async fn not_found() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "not_found", "no such endpoint")
}

pub(crate) async fn index() -> Html<&'static str> {
    Html(include_str!("../assets/index.html"))
}

pub(crate) async fn script() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../assets/app.js"),
    )
}

pub(crate) async fn style() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../assets/style.css"),
    )
}
