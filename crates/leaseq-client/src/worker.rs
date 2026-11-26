//! A concurrent worker with automatic heartbeats and graceful shutdown.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use leaseq_proto::{Delivery, LeaseRequest, QueueName};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::Client;

/// Most jobs requested in one lease call.
const BATCH: usize = 100;
/// Shortest pause between two lease requests that found nothing.
const IDLE_PAUSE: Duration = Duration::from_millis(100);

/// Processes one job. Implemented for async closures.
///
/// A job may be delivered more than once, so handling should be idempotent. If
/// the lease is lost while the handler runs, its future is dropped.
pub trait Handler: Send + Sync + 'static {
    /// Returns `Ok` to complete the job, or the reason the attempt failed.
    fn handle(&self, job: Delivery) -> impl Future<Output = Result<(), String>> + Send;
}

impl<F, Fut> Handler for F
where
    F: Fn(Delivery) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), String>> + Send,
{
    fn handle(&self, job: Delivery) -> impl Future<Output = Result<(), String>> + Send {
        self(job)
    }
}

/// Worker settings.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Queue to consume.
    pub queue: QueueName,
    /// Jobs processed at once.
    pub concurrency: usize,
    /// Lease duration; renewed every third of it while a job runs.
    pub lease: Duration,
    /// How long each lease request waits for work. Also bounds shutdown latency.
    pub poll_wait: Duration,
    /// Pause after a failed lease request.
    pub error_backoff: Duration,
}

impl WorkerConfig {
    /// Defaults for `queue`.
    pub fn new(queue: QueueName) -> Self {
        Self {
            queue,
            concurrency: 8,
            lease: Duration::from_secs(30),
            poll_wait: Duration::from_secs(5),
            error_backoff: Duration::from_secs(1),
        }
    }
}

/// Leases jobs and feeds them to a [`Handler`].
#[derive(Debug)]
pub struct Worker<H> {
    client: Client,
    config: WorkerConfig,
    handler: Arc<H>,
}

impl<H: Handler> Worker<H> {
    /// Creates a worker.
    pub fn new(client: Client, config: WorkerConfig, handler: H) -> Self {
        Self {
            client,
            config,
            handler: Arc::new(handler),
        }
    }

    /// Runs until `shutdown` is cancelled, then finishes the jobs in progress.
    pub async fn run(self, shutdown: CancellationToken) {
        let concurrency = self.config.concurrency.max(1);
        let slots = Arc::new(Semaphore::new(concurrency));
        let mut running = JoinSet::new();

        while !shutdown.is_cancelled() {
            // Wait for capacity before asking for work, so no lease sits idle.
            let first = tokio::select! {
                () = shutdown.cancelled() => break,
                permit = slots.clone().acquire_owned() => permit,
            };
            let Ok(first) = first else { break };
            let mut permits = vec![first];
            while permits.len() < BATCH {
                match slots.clone().try_acquire_owned() {
                    Ok(permit) => permits.push(permit),
                    Err(_) => break,
                }
            }

            let request = LeaseRequest {
                max: permits.len(),
                lease_ms: millis(self.config.lease),
                wait_ms: millis(self.config.poll_wait),
            };
            // Deliberately not cancelled: abandoning this request could strand
            // jobs the server has already leased to us.
            match self.client.lease(&self.config.queue, &request).await {
                Ok(jobs) => {
                    // A server that is shutting down answers at once; do not spin.
                    if jobs.is_empty() {
                        tokio::select! {
                            () = shutdown.cancelled() => break,
                            () = tokio::time::sleep(IDLE_PAUSE) => {}
                        }
                    }
                    for (job, permit) in jobs.into_iter().zip(permits) {
                        let client = self.client.clone();
                        let handler = self.handler.clone();
                        let lease = self.config.lease;
                        running.spawn(async move {
                            process(&client, handler.as_ref(), job, lease).await;
                            drop(permit);
                        });
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "could not lease jobs");
                    tokio::select! {
                        () = shutdown.cancelled() => break,
                        () = tokio::time::sleep(self.config.error_backoff) => {}
                    }
                }
            }
            while let Some(result) = running.try_join_next() {
                report(result);
            }
        }
        while let Some(result) = running.join_next().await {
            report(result);
        }
    }
}

fn report(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result {
        // The lease will lapse and the job will be retried.
        tracing::error!(%error, "job handler panicked");
    }
}

async fn process(client: &Client, handler: &impl Handler, job: Delivery, lease: Duration) {
    let (id, token) = (job.id, job.token);

    // The heartbeat is a separate future, so a slow renewal never stalls the
    // handler. It only completes if the lease is gone.
    let heartbeat = async {
        loop {
            tokio::time::sleep(lease / 3).await;
            match client.heartbeat(id, token, lease).await {
                Ok(()) => {}
                Err(error) if error.is_lease_lost() => return,
                Err(error) => tracing::warn!(%id, %error, "heartbeat failed"),
            }
        }
    };
    let outcome = tokio::select! {
        biased;
        outcome = handler.handle(job) => outcome,
        () = heartbeat => {
            tracing::warn!(%id, "lease lost; abandoning the job");
            return;
        }
    };

    let reported = match outcome {
        Ok(()) => client.ack(id, token).await,
        Err(reason) => client.nack(id, token, &reason).await,
    };
    if let Err(error) = reported {
        tracing::warn!(%id, %error, "could not report the result");
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
