//! A small, crash-safe write-ahead log.
//!
//! # On-disk format
//!
//! A log is a sequence of frames. All integers are little-endian.
//!
//! ```text
//! +----------+----------+------------------+
//! | len: u32 | crc: u32 | payload: [u8]    |
//! +----------+----------+------------------+
//! ```
//!
//! `crc` is the CRC-32 of the length field followed by the payload, so a damaged
//! length is detected as well as damaged data.
//!
//! # Recovery rules
//!
//! A crash can only damage the *end* of an append-only file. On open:
//!
//! * an incomplete frame, a bad checksum on the final frame, or an all-zero tail
//!   is a **torn write**: it is truncated away;
//! * a bad frame that is followed by more data is **corruption**: opening fails,
//!   because silently dropping everything after it would lose acknowledged writes.

mod frame;
mod log;
mod snapshot;

pub use frame::MAX_PAYLOAD;
pub use log::{Recovery, SyncPolicy, Wal};
pub use snapshot::{read_snapshot, sync_dir, write_snapshot};

use std::path::PathBuf;

/// Errors returned by the log and snapshot primitives.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    /// An operating system I/O error.
    #[error("i/o error on {path}: {source}")]
    Io {
        /// File or directory being accessed.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// Damaged data that cannot be explained by an interrupted write.
    #[error("corrupt data in {path} at byte {offset}: {reason}")]
    Corrupt {
        /// Damaged file.
        path: PathBuf,
        /// Offset of the damaged frame.
        offset: u64,
        /// What failed to validate.
        reason: &'static str,
    },
    /// Another process holds the log open.
    #[error("{path} is locked by another process")]
    Locked {
        /// Locked file.
        path: PathBuf,
    },
    /// A payload exceeded [`MAX_PAYLOAD`].
    #[error("payload of {len} bytes exceeds the size limit")]
    TooLarge {
        /// Rejected payload length.
        len: usize,
    },
    /// An earlier write or sync failed, so the on-disk state is unknown.
    #[error("log is poisoned by an earlier write failure; reopen it to recover")]
    Poisoned,
}

impl WalError {
    pub(crate) fn io(path: impl Into<PathBuf>) -> impl FnOnce(std::io::Error) -> Self {
        let path = path.into();
        move |source| Self::Io { path, source }
    }
}
