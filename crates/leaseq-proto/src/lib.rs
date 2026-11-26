//! Request and response bodies of the HTTP API.
//!
//! Job payloads are arbitrary JSON carried as [`RawValue`], so they are passed
//! through byte for byte without being parsed into a tree.

use std::fmt;

pub use leaseq_core::{JobId, LeaseToken, Priority, QueueName, QueueStats, Status};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
pub use serde_json::value::RawValue;

/// Lease duration used when a request does not give one.
pub const DEFAULT_LEASE_MS: u64 = 30_000;
/// Longest time a lease request may wait for work.
pub const MAX_WAIT_MS: u64 = 30_000;
/// Attempt budget used when a request does not give one.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// A lease token, encoded as a string because JavaScript numbers cannot hold
/// every 64-bit integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token(pub LeaseToken);

impl Serialize for Token {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0.0)
    }
}

impl<'de> Deserialize<'de> for Token {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse()
            .map(|n| Self(LeaseToken(n)))
            .map_err(|_| D::Error::custom("token must be a decimal string"))
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.0.fmt(f)
    }
}

/// `POST /api/queues/{queue}/jobs`
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnqueueRequest {
    /// Application data.
    pub payload: Box<RawValue>,
    /// 0 to 9, higher runs first.
    #[serde(default)]
    pub priority: Priority,
    /// Milliseconds to wait before the job is ready.
    #[serde(default)]
    pub delay_ms: u64,
    /// Attempts before dead-lettering.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
}

impl EnqueueRequest {
    /// A request with default options.
    pub fn new(payload: Box<RawValue>) -> Self {
        Self {
            payload,
            priority: Priority::default(),
            delay_ms: 0,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }
}

fn default_max_attempts() -> u32 {
    DEFAULT_MAX_ATTEMPTS
}

/// Reply to [`EnqueueRequest`].
#[derive(Debug, Serialize, Deserialize)]
pub struct EnqueueResponse {
    /// The new job.
    pub id: JobId,
}

/// `POST /api/queues/{queue}/lease`
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LeaseRequest {
    /// Most jobs to return.
    pub max: usize,
    /// How long the lease lasts.
    pub lease_ms: u64,
    /// How long to wait for work; capped at [`MAX_WAIT_MS`].
    pub wait_ms: u64,
}

impl Default for LeaseRequest {
    fn default() -> Self {
        Self {
            max: 1,
            lease_ms: DEFAULT_LEASE_MS,
            wait_ms: 0,
        }
    }
}

/// Reply to [`LeaseRequest`].
#[derive(Debug, Serialize, Deserialize)]
pub struct LeaseResponse {
    /// Leased jobs, possibly none.
    pub jobs: Vec<Delivery>,
}

/// A job handed to a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delivery {
    /// Identifier.
    pub id: JobId,
    /// Proof of the lease.
    pub token: Token,
    /// Source queue.
    pub queue: QueueName,
    /// Application data.
    pub payload: Box<RawValue>,
    /// Attempt number, starting at 1.
    pub attempt: u32,
    /// Lease expiry in epoch milliseconds.
    pub expires_at_ms: u64,
}

/// `POST /api/jobs/{id}/ack`
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckRequest {
    /// Proof of the lease.
    pub token: Token,
}

/// `POST /api/jobs/{id}/nack`
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NackRequest {
    /// Proof of the lease.
    pub token: Token,
    /// Why the attempt failed.
    #[serde(default)]
    pub error: String,
}

/// `POST /api/jobs/{id}/heartbeat`
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatRequest {
    /// Proof of the lease.
    pub token: Token,
    /// New lease duration, counted from now.
    pub lease_ms: u64,
}

/// A body with no fields, required so that every mutation is sent as JSON.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Empty {}

/// `GET /api/stats` and the `stats` server-sent event.
#[derive(Debug, Serialize, Deserialize)]
pub struct StatsResponse {
    /// One entry per queue.
    pub queues: Vec<QueueStats>,
}

/// Query string of `GET /api/jobs`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobsQuery {
    /// Only this queue.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<QueueName>,
    /// Only this state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<Status>,
    /// Most results, 1 to 500.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Reply to [`JobsQuery`].
#[derive(Debug, Serialize, Deserialize)]
pub struct JobsResponse {
    /// Matching jobs, oldest first.
    pub jobs: Vec<JobInfo>,
}

/// A job as shown to operators.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobInfo {
    /// Identifier.
    pub id: JobId,
    /// Queue.
    pub queue: QueueName,
    /// Current state.
    pub state: Status,
    /// Priority.
    pub priority: Priority,
    /// Leases granted so far.
    pub attempts: u32,
    /// Attempt budget.
    pub max_attempts: u32,
    /// Most recent failure.
    pub last_error: Option<String>,
    /// Payload as JSON text.
    pub payload: String,
}

/// Body of every error response.
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorBody {
    /// Details.
    pub error: ErrorDetail,
}

/// Machine-readable code plus human-readable message.
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorDetail {
    /// Stable identifier such as `lease_lost`.
    pub code: String,
    /// Explanation.
    pub message: String,
}
