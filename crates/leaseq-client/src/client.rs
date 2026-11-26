//! A typed HTTP client.

use std::time::Duration;

use leaseq_proto::{
    AckRequest, Delivery, Empty, EnqueueRequest, EnqueueResponse, ErrorBody, HeartbeatRequest,
    JobId, JobInfo, JobsQuery, JobsResponse, LeaseRequest, LeaseResponse, MAX_WAIT_MS, NackRequest,
    QueueName, QueueStats, StatsResponse, Token,
};
use reqwest::{RequestBuilder, Response, StatusCode, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Time allowed per request on top of any long-poll wait.
const TIMEOUT_MARGIN: Duration = Duration::from_secs(10);

/// Why a call failed.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The server address is unusable.
    #[error("invalid server url: {0}")]
    InvalidUrl(String),
    /// The request did not complete or the reply was unreadable.
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    /// The server refused the request.
    #[error("server replied {status}: {message} ({code})")]
    Api {
        /// HTTP status.
        status: StatusCode,
        /// Stable error code.
        code: String,
        /// Explanation.
        message: String,
    },
}

impl ClientError {
    /// Whether the job is no longer leased to this caller.
    pub fn is_lease_lost(&self) -> bool {
        matches!(self, Self::Api { code, .. } if code == "lease_lost" || code == "not_found")
    }
}

/// A connection to one server. Cloning shares the connection pool.
#[derive(Debug, Clone)]
pub struct Client {
    base: Url,
    http: reqwest::Client,
}

impl Client {
    /// Creates a client for a server such as `http://127.0.0.1:7878`.
    ///
    /// Only plain HTTP is supported.
    pub fn new(base: &str) -> Result<Self, ClientError> {
        let base = Url::parse(base).map_err(|e| ClientError::InvalidUrl(e.to_string()))?;
        if base.scheme() != "http" || !base.has_host() {
            return Err(ClientError::InvalidUrl(
                "expected http://host[:port]".to_owned(),
            ));
        }
        let http = reqwest::Client::builder()
            // The server is addressed directly; a proxy would see job payloads.
            .no_proxy()
            .timeout(Duration::from_millis(MAX_WAIT_MS) + TIMEOUT_MARGIN)
            .build()?;
        Ok(Self { base, http })
    }

    /// Adds a job.
    pub async fn enqueue(
        &self,
        queue: &QueueName,
        request: &EnqueueRequest,
    ) -> Result<JobId, ClientError> {
        let url = self.url(&["queues", queue.as_str(), "jobs"]);
        let reply: EnqueueResponse = decode(self.post(url, request)).await?;
        Ok(reply.id)
    }

    /// Leases jobs, possibly waiting for them.
    pub async fn lease(
        &self,
        queue: &QueueName,
        request: &LeaseRequest,
    ) -> Result<Vec<Delivery>, ClientError> {
        let url = self.url(&["queues", queue.as_str(), "lease"]);
        let reply: LeaseResponse = decode(self.post(url, request)).await?;
        Ok(reply.jobs)
    }

    /// Completes a job.
    pub async fn ack(&self, id: JobId, token: Token) -> Result<(), ClientError> {
        self.command(id, "ack", &AckRequest { token }).await
    }

    /// Reports a failed attempt.
    pub async fn nack(&self, id: JobId, token: Token, error: &str) -> Result<(), ClientError> {
        let error = error.to_owned();
        self.command(id, "nack", &NackRequest { token, error })
            .await
    }

    /// Extends a lease to `ttl` from now.
    pub async fn heartbeat(
        &self,
        id: JobId,
        token: Token,
        ttl: Duration,
    ) -> Result<(), ClientError> {
        let lease_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        self.command(id, "heartbeat", &HeartbeatRequest { token, lease_ms })
            .await
    }

    /// Returns a dead job to its queue.
    pub async fn retry(&self, id: JobId) -> Result<(), ClientError> {
        self.command(id, "retry", &Empty {}).await
    }

    /// Per-queue counters.
    pub async fn stats(&self) -> Result<Vec<QueueStats>, ClientError> {
        let reply: StatsResponse = decode(self.http.get(self.url(&["stats"]))).await?;
        Ok(reply.queues)
    }

    /// Live jobs.
    pub async fn jobs(&self, query: &JobsQuery) -> Result<Vec<JobInfo>, ClientError> {
        let request = self.http.get(self.url(&["jobs"])).query(query);
        let reply: JobsResponse = decode(request).await?;
        Ok(reply.jobs)
    }

    async fn command(
        &self,
        id: JobId,
        action: &str,
        body: &impl Serialize,
    ) -> Result<(), ClientError> {
        let url = self.url(&["jobs", &id.to_string(), action]);
        check(self.post(url, body).send().await?).await.map(drop)
    }

    fn post(&self, url: Url, body: &impl Serialize) -> RequestBuilder {
        self.http.post(url).json(body)
    }

    fn url(&self, segments: &[&str]) -> Url {
        let mut url = self.base.clone();
        // `new` guarantees a base that can have a path.
        if let Ok(mut path) = url.path_segments_mut() {
            path.pop_if_empty().push("api").extend(segments);
        }
        url
    }
}

async fn decode<T: DeserializeOwned>(request: RequestBuilder) -> Result<T, ClientError> {
    Ok(check(request.send().await?).await?.json().await?)
}

async fn check(response: Response) -> Result<Response, ClientError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let (code, message) = match response.json::<ErrorBody>().await {
        Ok(body) => (body.error.code, body.error.message),
        Err(_) => ("unknown".to_owned(), "unreadable error body".to_owned()),
    };
    Err(ClientError::Api {
        status,
        code,
        message,
    })
}
