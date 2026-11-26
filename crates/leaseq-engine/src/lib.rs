//! The durable queue engine.
//!
//! All state is owned by one writer thread. Callers send it closures; it runs a
//! batch of them, commits the resulting events with a single `fsync`, and only
//! then answers. Under load many requests share each `fsync` (group commit), and
//! no caller ever observes a change that could be lost.
//!
//! Delivery is **at least once**: a job whose lease lapses is handed out again,
//! so handlers should be idempotent.

mod clock;
mod engine;
mod error;
mod store;

pub use clock::{Clock, ManualClock, SystemClock};
pub use engine::{Engine, JobFilter, JobRecord, LeasedJob};
pub use error::{EngineError, OpenError};
pub use leaseq_core as core;
pub use leaseq_wal::SyncPolicy;
pub use store::{Config, Limits};
