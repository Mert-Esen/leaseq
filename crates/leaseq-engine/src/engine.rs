//! The asynchronous handle and the writer loop behind it.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use leaseq_core::{
    Event, JobId, JobSpec, JobView, LeaseToken, Priority, QueueName, QueueStats, Status, Timestamp,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

use crate::store::Store;
use crate::{Clock, Config, EngineError, Limits, OpenError, SystemClock};

const CHANNEL_CAPACITY: usize = 1_024;
const MAX_BATCH: usize = 512;
const MAX_ERROR_BYTES: usize = 2_048;
/// The writer wakes at least this often, which also absorbs wall-clock jumps.
const MAX_SLEEP: Duration = Duration::from_secs(1);

/// Sends the outcome to the caller once the batch's commit result is known.
type Reply = Box<dyn FnOnce(Result<(), EngineError>) + Send>;
/// A request: runs against the store and defers its answer until after commit.
type Op = Box<dyn FnOnce(&mut Store) -> Reply + Send>;

enum Message {
    Run(Op),
    Shutdown(oneshot::Sender<()>),
}

/// A job handed to a worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeasedJob {
    /// Identifier.
    pub id: JobId,
    /// Token required to acknowledge or extend this lease.
    pub token: LeaseToken,
    /// Source queue.
    pub queue: QueueName,
    /// Application data.
    pub payload: String,
    /// Which attempt this is, starting at 1.
    pub attempt: u32,
    /// When the lease lapses unless extended.
    pub expires_at: Timestamp,
}

/// A description of a live job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRecord {
    /// Identifier.
    pub id: JobId,
    /// Queue.
    pub queue: QueueName,
    /// Current classification.
    pub status: Status,
    /// Priority.
    pub priority: Priority,
    /// Leases granted so far.
    pub attempts: u32,
    /// Attempt budget.
    pub max_attempts: u32,
    /// Most recent failure.
    pub last_error: Option<String>,
    /// Application data.
    pub payload: String,
}

impl From<JobView<'_>> for JobRecord {
    fn from(view: JobView<'_>) -> Self {
        let job = view.job;
        Self {
            id: job.id,
            queue: job.spec.queue.clone(),
            status: view.status,
            priority: job.spec.priority,
            attempts: job.attempts,
            max_attempts: job.spec.max_attempts,
            last_error: job.last_error.clone(),
            payload: job.spec.payload.clone(),
        }
    }
}

/// Selects jobs for [`Engine::list`].
#[derive(Debug, Clone, Default)]
pub struct JobFilter {
    /// Only this queue.
    pub queue: Option<QueueName>,
    /// Only this status.
    pub status: Option<Status>,
    /// At most this many results.
    pub limit: usize,
}

/// A cheaply cloneable handle to a running engine.
#[derive(Debug, Clone)]
pub struct Engine {
    sender: mpsc::Sender<Message>,
    ready: watch::Receiver<u64>,
    stats: watch::Receiver<Arc<[QueueStats]>>,
    limits: Limits,
}

impl Engine {
    /// Opens a data directory using the system clock.
    pub fn open(config: Config) -> Result<Self, OpenError> {
        Self::open_with_clock(config, Arc::new(SystemClock))
    }

    /// Opens a data directory with a custom time source.
    ///
    /// Recovery happens before this returns, so errors surface here. It blocks on
    /// disk I/O; from async code with large logs, prefer `spawn_blocking`.
    pub fn open_with_clock(config: Config, clock: Arc<dyn Clock>) -> Result<Self, OpenError> {
        let limits = config.limits;
        let store = Store::open(config, clock.now())?;
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        let (ready_tx, ready) = watch::channel(0);
        let (stats_tx, stats) = watch::channel(Arc::from(store.state.stats()));

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|source| OpenError::Io {
                path: store.config.data_dir.clone(),
                source,
            })?;
        let path = store.config.data_dir.clone();
        thread::Builder::new()
            .name("leaseq-writer".to_owned())
            .spawn(move || {
                let writer = Writer {
                    receiver,
                    store,
                    clock,
                    ready: ready_tx,
                    stats: stats_tx,
                };
                runtime.block_on(writer.run());
            })
            .map_err(|source| OpenError::Io { path, source })?;

        Ok(Self {
            sender,
            ready,
            stats,
            limits,
        })
    }

    /// Stops the writer and releases the data directory.
    ///
    /// Requests that were already answered are durable; later ones fail with
    /// [`EngineError::Closed`].
    pub async fn shutdown(self) {
        let (done, finished) = oneshot::channel();
        if self.sender.send(Message::Shutdown(done)).await.is_ok() {
            let _ = finished.await;
        }
    }

    /// Adds a job that becomes ready after `delay`.
    pub async fn enqueue(&self, spec: JobSpec, delay: Duration) -> Result<JobId, EngineError> {
        if spec.payload.len() > self.limits.payload_bytes {
            return Err(EngineError::Invalid("payload is too large"));
        }
        if !(1..=self.limits.max_attempts).contains(&spec.max_attempts) {
            return Err(EngineError::Invalid("max_attempts is out of range"));
        }
        if delay > self.limits.delay {
            return Err(EngineError::Invalid("delay is too long"));
        }
        self.submit(move |store| {
            let event = store
                .state
                .decide_enqueue(spec, store.now.saturating_add(delay));
            let Event::Enqueued { id, .. } = event else {
                return Err(EngineError::Invalid("enqueue produced no job"));
            };
            store.record(&event)?;
            store.promote();
            Ok(id)
        })
        .await
    }

    /// Leases up to `max` jobs, waiting up to `wait` for at least one.
    ///
    /// If the caller goes away while a lease is being granted, the job returns to
    /// the queue when that lease lapses.
    pub async fn lease(
        &self,
        queue: QueueName,
        max: usize,
        ttl: Duration,
        wait: Duration,
    ) -> Result<Vec<LeasedJob>, EngineError> {
        if !(1..=self.limits.lease_batch).contains(&max) {
            return Err(EngineError::Invalid("max is out of range"));
        }
        self.check_ttl(ttl)?;
        // `None` means the wait is too long to represent, i.e. wait forever.
        let deadline = Instant::now().checked_add(wait);
        let mut ready = self.ready.clone();
        loop {
            // Mark the current version as seen *before* looking, so a job that
            // becomes ready in between still wakes us.
            ready.borrow_and_update();
            let queue = queue.clone();
            let jobs = self
                .submit(move |store| lease_now(store, &queue, max, ttl))
                .await?;
            if !jobs.is_empty() || wait.is_zero() {
                return Ok(jobs);
            }
            let changed = match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, ready.changed()).await,
                None => Ok(ready.changed().await),
            };
            match changed {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(EngineError::Closed),
                Err(_) => return Ok(Vec::new()),
            }
        }
    }

    /// Completes a leased job.
    pub async fn ack(&self, id: JobId, token: LeaseToken) -> Result<(), EngineError> {
        self.submit(move |store| {
            let event = store.state.decide_ack(id, token, store.now)?;
            store.record(&event)
        })
        .await
    }

    /// Reports a failed attempt. Long messages are truncated.
    pub async fn nack(
        &self,
        id: JobId,
        token: LeaseToken,
        mut error: String,
    ) -> Result<(), EngineError> {
        error.truncate(error.floor_char_boundary(MAX_ERROR_BYTES));
        self.submit(move |store| {
            let event =
                store
                    .state
                    .decide_nack(id, token, error, store.now, &store.config.retry)?;
            store.record(&event)?;
            store.promote();
            Ok(())
        })
        .await
    }

    /// Renews a lease so that it lapses `ttl` from now.
    pub async fn extend(
        &self,
        id: JobId,
        token: LeaseToken,
        ttl: Duration,
    ) -> Result<(), EngineError> {
        self.check_ttl(ttl)?;
        self.submit(move |store| {
            let expires_at = store.now.saturating_add(ttl);
            let event = store
                .state
                .decide_extend(id, token, store.now, expires_at)?;
            store.record(&event)
        })
        .await
    }

    /// Returns a dead job to its queue.
    pub async fn revive(&self, id: JobId) -> Result<(), EngineError> {
        self.submit(move |store| {
            let event = store.state.decide_revive(id, store.now)?;
            store.record(&event)?;
            store.promote();
            Ok(())
        })
        .await
    }

    /// One live job.
    pub async fn get(&self, id: JobId) -> Result<Option<JobRecord>, EngineError> {
        self.submit(move |store| Ok(store.state.job(id).map(JobRecord::from)))
            .await
    }

    /// Live jobs matching `filter`, oldest first. This scans every job.
    pub async fn list(&self, filter: JobFilter) -> Result<Vec<JobRecord>, EngineError> {
        self.submit(move |store| {
            Ok(store
                .state
                .jobs()
                .filter(|v| filter.queue.as_ref().is_none_or(|q| *q == v.job.spec.queue))
                .filter(|v| filter.status.is_none_or(|s| s == v.status))
                .take(filter.limit)
                .map(JobRecord::from)
                .collect())
        })
        .await
    }

    /// Forces the writer to process everything that is due.
    pub async fn tick(&self) -> Result<(), EngineError> {
        self.submit(|_| Ok(())).await
    }

    /// Counters as of the latest commit.
    pub fn stats(&self) -> Arc<[QueueStats]> {
        self.stats.borrow().clone()
    }

    /// A receiver that is notified whenever the counters change.
    pub fn watch_stats(&self) -> watch::Receiver<Arc<[QueueStats]>> {
        self.stats.clone()
    }

    fn check_ttl(&self, ttl: Duration) -> Result<(), EngineError> {
        if ttl.as_millis() == 0 || ttl > self.limits.lease_ttl {
            return Err(EngineError::Invalid("lease duration is out of range"));
        }
        Ok(())
    }

    async fn submit<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Store) -> Result<T, EngineError> + Send + 'static,
    ) -> Result<T, EngineError> {
        let (answer, response) = oneshot::channel();
        let op: Op = Box::new(move |store| {
            let outcome = store.check().and_then(|()| operation(store));
            Box::new(move |committed| {
                // The caller may have stopped waiting; nothing to do then.
                let _ = answer.send(committed.and(outcome));
            })
        });
        self.sender
            .send(Message::Run(op))
            .await
            .map_err(|_| EngineError::Closed)?;
        response.await.map_err(|_| EngineError::Closed)?
    }
}

fn lease_now(
    store: &mut Store,
    queue: &QueueName,
    max: usize,
    ttl: Duration,
) -> Result<Vec<LeasedJob>, EngineError> {
    store.promote();
    let expires_at = store.now.saturating_add(ttl);
    let events = store.state.decide_lease(queue, max, expires_at);
    let mut jobs = Vec::with_capacity(events.len());
    for event in events {
        store.record(&event)?;
        let Event::Leased {
            id,
            token,
            expires_at,
        } = event
        else {
            continue;
        };
        if let Some(view) = store.state.job(id) {
            jobs.push(LeasedJob {
                id,
                token,
                queue: view.job.spec.queue.clone(),
                payload: view.job.spec.payload.clone(),
                attempt: view.job.attempts,
                expires_at,
            });
        }
    }
    Ok(jobs)
}

struct Writer {
    receiver: mpsc::Receiver<Message>,
    store: Store,
    clock: Arc<dyn Clock>,
    ready: watch::Sender<u64>,
    stats: watch::Sender<Arc<[QueueStats]>>,
}

impl Writer {
    async fn run(mut self) {
        let mut batch = Vec::with_capacity(MAX_BATCH);
        loop {
            let sleep = self.until_next_deadline();
            let first = tokio::select! {
                message = self.receiver.recv() => match message {
                    Some(message) => Some(message),
                    None => break, // every handle was dropped
                },
                () = tokio::time::sleep(sleep) => None,
            };

            // Everything already waiting joins this commit.
            let mut shutdown = None;
            let mut next = first;
            while let Some(message) = next {
                match message {
                    Message::Run(op) => batch.push(op),
                    Message::Shutdown(done) => {
                        shutdown = Some(done);
                        break;
                    }
                }
                next = if batch.len() < MAX_BATCH {
                    self.receiver.try_recv().ok()
                } else {
                    None
                };
            }

            self.process(&mut batch);

            if let Some(done) = shutdown {
                drop(self.store); // release the directory before confirming
                let _ = done.send(());
                return;
            }
        }
    }

    fn process(&mut self, batch: &mut Vec<Op>) {
        self.store.begin(self.clock.now());
        let replies: Vec<Reply> = batch.drain(..).map(|op| op(&mut self.store)).collect();
        let committed = self.store.commit();

        // Publish before replying, so a caller sees its own change in the stats.
        if committed.is_ok() {
            let stats: Arc<[QueueStats]> = self.store.state.stats().into();
            self.stats.send_if_modified(|current| {
                let changed = *current != stats;
                if changed {
                    *current = stats;
                }
                changed
            });
        }
        for reply in replies {
            reply(committed.clone());
        }
        if self.store.take_became_ready() {
            self.ready
                .send_modify(|version| *version = version.wrapping_add(1));
        }
    }

    fn until_next_deadline(&self) -> Duration {
        match self.store.state.next_deadline() {
            Some(deadline) if self.store.check().is_ok() => {
                deadline.saturating_since(self.clock.now()).min(MAX_SLEEP)
            }
            _ => MAX_SLEEP,
        }
    }
}
