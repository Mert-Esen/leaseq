//! Facts about the queue, in the form they are persisted.

use serde::{Deserialize, Serialize};

use crate::{JobId, JobSpec, LeaseToken, Timestamp};

/// A state change that has been decided.
///
/// Events record outcomes, not requests. Anything that depended on the clock or
/// on configuration (such as a retry time) is stored, so replay never has to
/// recompute it and changing the configuration cannot rewrite history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// A job was accepted.
    Enqueued {
        /// Assigned identifier.
        id: JobId,
        /// Producer's request.
        spec: JobSpec,
        /// Earliest time the job may be leased.
        run_at: Timestamp,
    },
    /// A job was handed to a worker.
    Leased {
        /// Leased job.
        id: JobId,
        /// Fencing token for this lease.
        token: LeaseToken,
        /// When the lease lapses unless extended.
        expires_at: Timestamp,
    },
    /// A worker renewed its lease.
    Extended {
        /// Leased job.
        id: JobId,
        /// New expiry.
        expires_at: Timestamp,
    },
    /// A worker finished the job; it is removed.
    Completed {
        /// Finished job.
        id: JobId,
    },
    /// An attempt failed or its lease lapsed.
    Failed {
        /// Failed job.
        id: JobId,
        /// Reason, for operators.
        error: String,
        /// When to try again, or `None` to dead-letter the job.
        retry_at: Option<Timestamp>,
    },
    /// An operator returned a dead job to the queue with a fresh attempt budget.
    Revived {
        /// Revived job.
        id: JobId,
        /// Earliest time the job may be leased.
        run_at: Timestamp,
    },
}
