//! The queue's rules as a deterministic state machine.
//!
//! This crate performs no I/O and never reads the clock. Time is always passed in.
//! A change happens in two steps:
//!
//! 1. a `decide_*` method inspects the state and returns [`Event`]s, without
//!    mutating anything;
//! 2. [`State::apply`] folds an event into the state.
//!
//! Persisting the events between those steps makes the queue durable, and
//! replaying them reconstructs exactly the same state.
//!
//! ```
//! use leaseq_core::{JobSpec, Priority, QueueName, State, Timestamp};
//!
//! let mut state = State::default();
//! let queue: QueueName = "mail".parse()?;
//! let now = Timestamp::from_millis(1_000);
//!
//! let spec = JobSpec { queue: queue.clone(), payload: "{}".into(), priority: Priority::default(), max_attempts: 3 };
//! let event = state.decide_enqueue(spec, now);
//! state.apply(&event)?;
//!
//! state.promote(now);
//! let leases = state.decide_lease(&queue, 10, Timestamp::from_millis(31_000));
//! assert_eq!(leases.len(), 1);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod event;
mod retry;
mod state;
mod types;

pub use event::Event;
pub use retry::RetryPolicy;
pub use state::{
    ApplyError, Job, JobState, JobView, QueueError, QueueStats, Snapshot, State, Status,
};
pub use types::{
    InvalidPriority, InvalidQueueName, JobId, JobSpec, LeaseToken, Priority, QueueName, Timestamp,
};
