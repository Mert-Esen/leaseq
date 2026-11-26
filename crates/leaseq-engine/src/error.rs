//! Error types.

use std::path::PathBuf;
use std::sync::Arc;

use leaseq_core::{ApplyError, QueueError};
use leaseq_wal::WalError;

/// Why a request to a running engine failed.
#[derive(Debug, Clone, thiserror::Error)]
pub enum EngineError {
    /// The queue's rules do not allow the request.
    #[error(transparent)]
    Queue(#[from] QueueError),
    /// A parameter is outside its limits.
    #[error("invalid request: {0}")]
    Invalid(&'static str),
    /// Persisting failed. The engine stops serving until it is reopened.
    #[error("storage failure: {0}")]
    Storage(Arc<str>),
    /// The engine has shut down.
    #[error("the engine is shut down")]
    Closed,
}

/// Why a data directory could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// The log or snapshot is unreadable.
    #[error(transparent)]
    Wal(#[from] WalError),
    /// A filesystem operation failed.
    #[error("i/o error on {path}: {source}")]
    Io {
        /// File or directory involved.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// An intact record does not parse.
    #[error("unreadable record: {0}")]
    Decode(#[from] serde_json::Error),
    /// The history contradicts itself.
    #[error("inconsistent history: {0}")]
    Apply(#[from] ApplyError),
    /// Another process is using the directory.
    #[error("data directory {0} is already in use")]
    InUse(PathBuf),
    /// Files are missing or do not belong together.
    #[error("data directory is inconsistent: {0}")]
    Inconsistent(String),
}
