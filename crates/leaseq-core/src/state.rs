//! The state machine and its indexes.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{Event, JobId, JobSpec, LeaseToken, Priority, QueueName, RetryPolicy, Timestamp};

/// Where a job is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum JobState {
    /// Waiting to be leased, no earlier than `run_at`.
    Pending {
        /// Earliest lease time.
        run_at: Timestamp,
    },
    /// Held by a worker.
    Leased {
        /// Fencing token.
        token: LeaseToken,
        /// Lease expiry.
        expires_at: Timestamp,
    },
    /// Out of attempts; kept for inspection.
    Dead,
}

/// A job that has not completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    /// Identifier.
    pub id: JobId,
    /// Producer's request.
    pub spec: JobSpec,
    /// Leases granted so far.
    pub attempts: u32,
    /// Lifecycle state.
    pub state: JobState,
    /// Reason for the most recent failure.
    pub last_error: Option<String>,
}

/// Operator-facing classification of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Can be leased now.
    Ready,
    /// Scheduled for later.
    Delayed,
    /// Held by a worker.
    Leased,
    /// Dead-lettered.
    Dead,
}

/// A borrowed job together with its [`Status`].
#[derive(Debug, Clone, Copy)]
pub struct JobView<'a> {
    /// The job.
    pub job: &'a Job,
    /// Its classification at the last [`State::promote`].
    pub status: Status,
}

/// Counters for one queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueStats {
    /// Queue name.
    pub name: QueueName,
    /// Jobs that can be leased now.
    pub ready: u64,
    /// Jobs scheduled for later.
    pub delayed: u64,
    /// Jobs held by workers.
    pub leased: u64,
    /// Dead-lettered jobs.
    pub dead: u64,
    /// Jobs ever completed.
    pub completed: u64,
}

/// A request that the current state does not allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum QueueError {
    /// No live job has this identifier.
    #[error("job {0} does not exist")]
    NotFound(JobId),
    /// The token is stale or the lease has lapsed.
    #[error("the lease on job {0} is no longer held")]
    LeaseLost(JobId),
    /// Only dead jobs can be revived.
    #[error("job {0} is not dead")]
    NotDead(JobId),
}

/// An event that contradicts the state, which means the history is damaged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ApplyError {
    /// The event names a job that does not exist.
    #[error("event refers to unknown job {0}")]
    UnknownJob(JobId),
    /// An identifier was used twice.
    #[error("job {0} already exists")]
    Duplicate(JobId),
    /// The event is impossible from the job's current state.
    #[error("invalid transition for job {0}")]
    InvalidTransition(JobId),
}

/// The persistent part of [`State`]. Indexes are rebuilt from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    next_id: u64,
    next_token: u64,
    jobs: Vec<Job>,
    completed: BTreeMap<QueueName, u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct QueueIndex {
    /// Ordered by priority (highest first), then age.
    ready: BTreeSet<(Reverse<Priority>, JobId)>,
    dead: BTreeSet<JobId>,
    delayed: u64,
    leased: u64,
    completed: u64,
}

/// All live jobs, with indexes for scheduling.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct State {
    jobs: BTreeMap<JobId, Job>,
    queues: BTreeMap<QueueName, QueueIndex>,
    /// Pending jobs not yet promoted, ordered by due time.
    delayed: BTreeSet<(Timestamp, JobId)>,
    /// Leased jobs ordered by expiry.
    leases: BTreeSet<(Timestamp, JobId)>,
    next_id: u64,
    next_token: u64,
}

impl State {
    // ----- decisions -------------------------------------------------------

    /// Accepts a job that may run from `run_at`.
    pub fn decide_enqueue(&self, mut spec: JobSpec, run_at: Timestamp) -> Event {
        spec.max_attempts = spec.max_attempts.max(1);
        Event::Enqueued {
            id: JobId(self.next_id),
            spec,
            run_at,
        }
    }

    /// Leases up to `max` ready jobs from `queue`, best first.
    ///
    /// Call [`State::promote`] beforehand so that due jobs are visible.
    pub fn decide_lease(&self, queue: &QueueName, max: usize, expires_at: Timestamp) -> Vec<Event> {
        let Some(index) = self.queues.get(queue) else {
            return Vec::new();
        };
        index
            .ready
            .iter()
            .take(max)
            .zip(self.next_token..)
            .map(|(&(_, id), token)| Event::Leased {
                id,
                token: LeaseToken(token),
                expires_at,
            })
            .collect()
    }

    /// Completes a leased job.
    pub fn decide_ack(
        &self,
        id: JobId,
        token: LeaseToken,
        now: Timestamp,
    ) -> Result<Event, QueueError> {
        self.held(id, token, now)?;
        Ok(Event::Completed { id })
    }

    /// Records a failed attempt.
    pub fn decide_nack(
        &self,
        id: JobId,
        token: LeaseToken,
        error: String,
        now: Timestamp,
        policy: &RetryPolicy,
    ) -> Result<Event, QueueError> {
        let job = self.held(id, token, now)?;
        Ok(failure(job, error, now, policy))
    }

    /// Moves a lease's expiry to `expires_at`.
    pub fn decide_extend(
        &self,
        id: JobId,
        token: LeaseToken,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Event, QueueError> {
        self.held(id, token, now)?;
        Ok(Event::Extended { id, expires_at })
    }

    /// Returns a dead job to its queue.
    pub fn decide_revive(&self, id: JobId, now: Timestamp) -> Result<Event, QueueError> {
        match self.jobs.get(&id) {
            None => Err(QueueError::NotFound(id)),
            Some(job) if job.state == JobState::Dead => Ok(Event::Revived { id, run_at: now }),
            Some(_) => Err(QueueError::NotDead(id)),
        }
    }

    /// Fails up to `max` leases that have lapsed by `now`.
    pub fn decide_expired(&self, now: Timestamp, max: usize, policy: &RetryPolicy) -> Vec<Event> {
        self.leases
            .iter()
            .take_while(|(expires_at, _)| *expires_at <= now)
            .take(max)
            .filter_map(|(_, id)| self.jobs.get(id))
            .map(|job| failure(job, "lease expired".to_owned(), now, policy))
            .collect()
    }

    fn held(&self, id: JobId, token: LeaseToken, now: Timestamp) -> Result<&Job, QueueError> {
        let job = self.jobs.get(&id).ok_or(QueueError::NotFound(id))?;
        match job.state {
            JobState::Leased {
                token: held,
                expires_at,
            } if held == token && expires_at > now => Ok(job),
            _ => Err(QueueError::LeaseLost(id)),
        }
    }

    // ----- application -----------------------------------------------------

    /// Folds one event into the state. On error the state is unchanged.
    pub fn apply(&mut self, event: &Event) -> Result<(), ApplyError> {
        match event {
            Event::Enqueued { id, spec, run_at } => {
                if self.jobs.contains_key(id) {
                    return Err(ApplyError::Duplicate(*id));
                }
                self.next_id = self.next_id.max(id.0.saturating_add(1));
                self.insert(Job {
                    id: *id,
                    spec: spec.clone(),
                    attempts: 0,
                    state: JobState::Pending { run_at: *run_at },
                    last_error: None,
                });
            }
            Event::Leased {
                id,
                token,
                expires_at,
            } => {
                self.transition(*id, is_pending, |job| {
                    job.attempts = job.attempts.saturating_add(1);
                    job.state = JobState::Leased {
                        token: *token,
                        expires_at: *expires_at,
                    };
                })?;
                self.next_token = self.next_token.max(token.0.saturating_add(1));
            }
            Event::Extended { id, expires_at } => {
                self.transition(*id, is_leased, |job| {
                    if let JobState::Leased {
                        expires_at: current,
                        ..
                    } = &mut job.state
                    {
                        *current = *expires_at;
                    }
                })?;
            }
            Event::Completed { id } => {
                let job = self.take(*id, is_leased)?;
                let index = self.queues.entry(job.spec.queue).or_default();
                index.completed = index.completed.saturating_add(1);
            }
            Event::Failed {
                id,
                error,
                retry_at,
            } => {
                self.transition(*id, is_leased, |job| {
                    job.last_error = Some(error.clone());
                    job.state = match retry_at {
                        Some(run_at) => JobState::Pending { run_at: *run_at },
                        None => JobState::Dead,
                    };
                })?;
            }
            Event::Revived { id, run_at } => {
                self.transition(*id, is_dead, |job| {
                    job.attempts = 0;
                    job.state = JobState::Pending { run_at: *run_at };
                })?;
            }
        }
        Ok(())
    }

    /// Makes delayed jobs that are due by `now` available for leasing.
    ///
    /// This only maintains indexes, so it is not recorded as an event. Returns
    /// whether any job became ready.
    pub fn promote(&mut self, now: Timestamp) -> bool {
        let mut promoted = false;
        while let Some(&(run_at, id)) = self.delayed.first() {
            if run_at > now {
                break;
            }
            self.delayed.pop_first();
            if let Some(job) = self.jobs.get(&id) {
                let index = self.queues.entry(job.spec.queue.clone()).or_default();
                index.delayed -= 1;
                index.ready.insert((Reverse(job.spec.priority), id));
                promoted = true;
            }
        }
        promoted
    }

    fn insert(&mut self, job: Job) {
        let index = self.queues.entry(job.spec.queue.clone()).or_default();
        match job.state {
            JobState::Pending { run_at } => {
                index.delayed += 1;
                self.delayed.insert((run_at, job.id));
            }
            JobState::Leased { expires_at, .. } => {
                index.leased += 1;
                self.leases.insert((expires_at, job.id));
            }
            JobState::Dead => {
                index.dead.insert(job.id);
            }
        }
        self.jobs.insert(job.id, job);
    }

    /// Removes a job and its index entries, provided `allowed` accepts its state.
    fn take(&mut self, id: JobId, allowed: fn(JobState) -> bool) -> Result<Job, ApplyError> {
        let state = self.jobs.get(&id).ok_or(ApplyError::UnknownJob(id))?.state;
        if !allowed(state) {
            return Err(ApplyError::InvalidTransition(id));
        }
        let job = self.jobs.remove(&id).ok_or(ApplyError::UnknownJob(id))?;
        let index = self.queues.entry(job.spec.queue.clone()).or_default();
        match job.state {
            JobState::Pending { run_at } => {
                if self.delayed.remove(&(run_at, id)) {
                    index.delayed -= 1;
                } else {
                    index.ready.remove(&(Reverse(job.spec.priority), id));
                }
            }
            JobState::Leased { expires_at, .. } => {
                self.leases.remove(&(expires_at, id));
                index.leased -= 1;
            }
            JobState::Dead => {
                index.dead.remove(&id);
            }
        }
        Ok(job)
    }

    /// Every state change goes through `take` and `insert`, which keeps the
    /// indexes consistent in one place.
    fn transition(
        &mut self,
        id: JobId,
        allowed: fn(JobState) -> bool,
        change: impl FnOnce(&mut Job),
    ) -> Result<(), ApplyError> {
        let mut job = self.take(id, allowed)?;
        change(&mut job);
        self.insert(job);
        Ok(())
    }

    // ----- queries ---------------------------------------------------------

    /// The job with this identifier, if it has not completed.
    pub fn job(&self, id: JobId) -> Option<JobView<'_>> {
        self.jobs.get(&id).map(|job| JobView {
            job,
            status: self.status(job),
        })
    }

    /// Jobs in ascending identifier order.
    pub fn jobs(&self) -> impl Iterator<Item = JobView<'_>> {
        self.jobs.values().map(|job| JobView {
            job,
            status: self.status(job),
        })
    }

    fn status(&self, job: &Job) -> Status {
        match job.state {
            JobState::Pending { run_at } => {
                if self.delayed.contains(&(run_at, job.id)) {
                    Status::Delayed
                } else {
                    Status::Ready
                }
            }
            JobState::Leased { .. } => Status::Leased,
            JobState::Dead => Status::Dead,
        }
    }

    /// Counters for every queue that has ever held a job.
    pub fn stats(&self) -> Vec<QueueStats> {
        self.queues
            .iter()
            .map(|(name, index)| QueueStats {
                name: name.clone(),
                ready: index.ready.len() as u64,
                delayed: index.delayed,
                leased: index.leased,
                dead: index.dead.len() as u64,
                completed: index.completed,
            })
            .collect()
    }

    /// The next time at which [`State::promote`] or [`State::decide_expired`]
    /// would have something to do.
    pub fn next_deadline(&self) -> Option<Timestamp> {
        let due = self.delayed.first().map(|&(at, _)| at);
        let expiry = self.leases.first().map(|&(at, _)| at);
        due.into_iter().chain(expiry).min()
    }

    // ----- snapshots -------------------------------------------------------

    /// Captures the persistent state.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            next_id: self.next_id,
            next_token: self.next_token,
            jobs: self.jobs.values().cloned().collect(),
            completed: self
                .queues
                .iter()
                .map(|(name, index)| (name.clone(), index.completed))
                .collect(),
        }
    }

    /// Rebuilds a state and its indexes. Call [`State::promote`] afterwards.
    pub fn restore(snapshot: Snapshot) -> Result<Self, ApplyError> {
        let mut state = Self {
            next_id: snapshot.next_id,
            next_token: snapshot.next_token,
            ..Self::default()
        };
        for (name, completed) in snapshot.completed {
            state.queues.entry(name).or_default().completed = completed;
        }
        for job in snapshot.jobs {
            if state.jobs.contains_key(&job.id) {
                return Err(ApplyError::Duplicate(job.id));
            }
            let stale_id = job.id.0 >= state.next_id;
            let stale_token =
                matches!(job.state, JobState::Leased { token, .. } if token.0 >= state.next_token);
            if stale_id || stale_token {
                return Err(ApplyError::InvalidTransition(job.id));
            }
            state.insert(job);
        }
        Ok(state)
    }
}

fn is_pending(state: JobState) -> bool {
    matches!(state, JobState::Pending { .. })
}

fn is_leased(state: JobState) -> bool {
    matches!(state, JobState::Leased { .. })
}

fn is_dead(state: JobState) -> bool {
    state == JobState::Dead
}

fn failure(job: &Job, error: String, now: Timestamp, policy: &RetryPolicy) -> Event {
    let retry_at = (job.attempts < job.spec.max_attempts)
        .then(|| now.saturating_add(policy.delay(job.id, job.attempts)));
    Event::Failed {
        id: job.id,
        error,
        retry_at,
    }
}
