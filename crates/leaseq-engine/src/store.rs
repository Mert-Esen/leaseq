//! State plus its persistence. Owned exclusively by the writer thread.
//!
//! A data directory contains:
//!
//! * `LOCK` – held while the engine runs;
//! * `snapshot.bin` – state as of the start of log generation `g`;
//! * `wal-g.log` – events since that snapshot.
//!
//! Compaction creates the empty log `g + 1`, then atomically replaces the
//! snapshot (the commit point), then deletes log `g`. A crash between any two
//! steps leaves a directory that opens correctly.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use leaseq_core::{Event, RetryPolicy, Snapshot, State, Timestamp};
use leaseq_wal::{SyncPolicy, Wal, WalError, read_snapshot, sync_dir, write_snapshot};
use serde::{Deserialize, Serialize};

use crate::{EngineError, OpenError};

const SNAPSHOT: &str = "snapshot.bin";
/// Cap on lapsed leases processed per batch, so one sweep cannot stall requests.
const SWEEP_LIMIT: usize = 4_096;

/// Request limits enforced by the engine.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Largest job payload in bytes.
    pub payload_bytes: usize,
    /// Most jobs a single lease request may return.
    pub lease_batch: usize,
    /// Longest lease a worker may request or extend to.
    pub lease_ttl: Duration,
    /// Longest delay before a new job becomes ready.
    pub delay: Duration,
    /// Largest attempt budget.
    pub max_attempts: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            payload_bytes: 256 * 1024,
            lease_batch: 100,
            lease_ttl: Duration::from_hours(1),
            delay: Duration::from_hours(30 * 24),
            max_attempts: 100,
        }
    }
}

/// How to open an engine.
#[derive(Debug, Clone)]
pub struct Config {
    /// Where data is stored. Created if missing.
    pub data_dir: PathBuf,
    /// Durability of each commit.
    pub sync: SyncPolicy,
    /// Backoff between attempts.
    pub retry: RetryPolicy,
    /// Log size that triggers compaction.
    pub compact_after_bytes: u64,
    /// Request limits.
    pub limits: Limits,
}

impl Config {
    /// Default settings for `data_dir`.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            sync: SyncPolicy::Always,
            retry: RetryPolicy::default(),
            compact_after_bytes: 64 * 1024 * 1024,
            limits: Limits::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SnapshotFile {
    generation: u64,
    state: Snapshot,
}

#[derive(Debug)]
pub(crate) struct Store {
    pub(crate) config: Config,
    pub(crate) state: State,
    /// Time of the current batch.
    pub(crate) now: Timestamp,
    wal: Wal,
    generation: u64,
    became_ready: bool,
    failure: Option<EngineError>,
    // Declared last so the directory stays locked until the log is closed.
    _lock: File,
}

impl Store {
    pub(crate) fn open(config: Config, now: Timestamp) -> Result<Self, OpenError> {
        let dir = config.data_dir.clone();
        fs::create_dir_all(&dir).map_err(io(&dir))?;
        let lock = lock_dir(&dir)?;

        let (generation, mut state) = match read_snapshot(&dir.join(SNAPSHOT))? {
            Some(bytes) => {
                let file: SnapshotFile = serde_json::from_slice(&bytes)?;
                (file.generation, State::restore(file.state)?)
            }
            None => (0, State::default()),
        };

        let logs = list_logs(&dir)?;
        let current = log_path(&dir, generation);
        let must_exist = generation > 0 || !logs.is_empty();
        if must_exist && !logs.iter().any(|&(g, _)| g == generation) {
            return Err(OpenError::Inconsistent(format!(
                "log generation {generation} is missing"
            )));
        }

        let (wal, recovery) = Wal::open(&current, config.sync, |record| {
            let event: Event = serde_json::from_slice(record)?;
            state.apply(&event)?;
            Ok::<_, OpenError>(())
        })?;
        if recovery.truncated_bytes > 0 {
            tracing::warn!(
                bytes = recovery.truncated_bytes,
                "discarded a torn write at the end of the log"
            );
        }
        remove_stale_logs(&logs, generation)?;
        sync_dir(&dir)?;

        let mut store = Self {
            config,
            state,
            now,
            wal,
            generation,
            became_ready: false,
            failure: None,
            _lock: lock,
        };
        store.promote();
        Ok(store)
    }

    /// The error that stopped the engine, if any.
    pub(crate) fn check(&self) -> Result<(), EngineError> {
        self.failure.clone().map_or(Ok(()), Err)
    }

    /// Applies a decided event and queues it for the next commit.
    pub(crate) fn record(&mut self, event: &Event) -> Result<(), EngineError> {
        self.check()?;
        let result = serde_json::to_vec(event)
            .map_err(|e| e.to_string())
            .and_then(|bytes| self.wal.append(&bytes).map_err(|e| e.to_string()))
            .and_then(|()| self.state.apply(event).map_err(|e| e.to_string()));
        result.map_err(|reason| self.fail(&reason))
    }

    pub(crate) fn promote(&mut self) {
        self.became_ready |= self.state.promote(self.now);
    }

    /// Whether any job became ready since the last call.
    pub(crate) fn take_became_ready(&mut self) -> bool {
        std::mem::take(&mut self.became_ready)
    }

    /// Starts a batch: sets the time, releases due jobs and fails lapsed leases.
    pub(crate) fn begin(&mut self, now: Timestamp) {
        self.now = now;
        if self.failure.is_some() {
            return;
        }
        self.promote();
        for event in self
            .state
            .decide_expired(now, SWEEP_LIMIT, &self.config.retry)
        {
            if self.record(&event).is_err() {
                return;
            }
        }
        self.promote();
    }

    /// Makes the batch durable, compacting afterwards if the log is large.
    pub(crate) fn commit(&mut self) -> Result<(), EngineError> {
        self.check()?;
        if let Err(error) = self.wal.commit() {
            return Err(self.fail(&error.to_string()));
        }
        if self.wal.len() >= self.config.compact_after_bytes {
            // The batch is already durable, so a compaction failure does not
            // fail it; it stops the engine for later requests.
            if let Err(error) = self.compact() {
                self.fail(&format!("compaction failed: {error}"));
            }
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<(), OpenError> {
        let dir = &self.config.data_dir;
        let next = self.generation + 1;
        let path = log_path(dir, next);
        let (wal, _) = Wal::open(&path, self.config.sync, |_| {
            Err(OpenError::Inconsistent("new log is not empty".to_owned()))
        })?;
        sync_dir(dir)?;
        let snapshot = serde_json::to_vec(&SnapshotFile {
            generation: next,
            state: self.state.snapshot(),
        })?;
        write_snapshot(&dir.join(SNAPSHOT), &snapshot)?;

        let old = std::mem::replace(&mut self.wal, wal);
        let old_path = old.path().to_path_buf();
        drop(old);
        self.generation = next;
        if let Err(error) = fs::remove_file(&old_path) {
            // Harmless: the next open removes it.
            tracing::warn!(%error, path = %old_path.display(), "could not delete old log");
        }
        tracing::info!(generation = next, "compacted the log");
        Ok(())
    }

    fn fail(&mut self, reason: &str) -> EngineError {
        tracing::error!(reason, "storage failure; the engine will refuse requests");
        let error = EngineError::Storage(Arc::from(reason));
        self.failure.get_or_insert(error).clone()
    }
}

fn io(path: &Path) -> impl FnOnce(std::io::Error) -> OpenError + '_ {
    move |source| OpenError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn lock_dir(dir: &Path) -> Result<File, OpenError> {
    let path = dir.join("LOCK");
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(io(&path))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(OpenError::InUse(dir.to_path_buf())),
        Err(TryLockError::Error(source)) => Err(io(&path)(source)),
    }
}

fn log_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("wal-{generation}.log"))
}

fn list_logs(dir: &Path) -> Result<Vec<(u64, PathBuf)>, WalError> {
    let wrap = |source| WalError::Io {
        path: dir.to_path_buf(),
        source,
    };
    let mut logs = Vec::new();
    for entry in fs::read_dir(dir).map_err(wrap)? {
        let path = entry.map_err(wrap)?.path();
        let generation = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("wal-")?.strip_suffix(".log"))
            .and_then(|digits| digits.parse().ok());
        if let Some(generation) = generation {
            logs.push((generation, path));
        }
    }
    Ok(logs)
}

/// Older logs are covered by the snapshot. A newer one is only legitimate if it
/// is the empty file left by an interrupted compaction.
fn remove_stale_logs(logs: &[(u64, PathBuf)], current: u64) -> Result<(), OpenError> {
    for (generation, path) in logs {
        if *generation == current {
            continue;
        }
        if *generation > current && fs::metadata(path).map_err(io(path))?.len() > 0 {
            return Err(OpenError::Inconsistent(format!(
                "log generation {generation} is newer than the snapshot"
            )));
        }
        fs::remove_file(path).map_err(io(path))?;
    }
    Ok(())
}
