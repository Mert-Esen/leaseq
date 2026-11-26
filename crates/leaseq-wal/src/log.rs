//! The append-only log file.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::WalError;
use crate::frame::{self, Decoded, MAX_PAYLOAD};

/// When appended data is forced to stable storage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SyncPolicy {
    /// `fsync` on every commit. Committed data survives power loss.
    #[default]
    Always,
    /// Leave flushing to the operating system. Committed data survives a process
    /// crash but may be lost on power failure.
    Never,
}

/// What [`Wal::open`] found on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recovery {
    /// Number of intact frames replayed.
    pub frames: u64,
    /// Bytes of torn tail that were discarded.
    pub truncated_bytes: u64,
}

/// An exclusively locked, append-only log.
///
/// Appends are buffered in memory until [`Wal::commit`], which lets a caller
/// batch many records into a single `fsync` (group commit).
#[derive(Debug)]
pub struct Wal {
    file: File,
    path: PathBuf,
    sync: SyncPolicy,
    committed: u64,
    pending: Vec<u8>,
    poisoned: bool,
}

impl Wal {
    /// Opens or creates the log at `path`, passing each intact payload to `replay`.
    ///
    /// A torn tail is truncated. `replay` errors abort the open and leave the
    /// file untouched.
    pub fn open<E>(
        path: impl AsRef<Path>,
        sync: SyncPolicy,
        mut replay: impl FnMut(&[u8]) -> Result<(), E>,
    ) -> Result<(Self, Recovery), E>
    where
        E: From<WalError>,
    {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(WalError::io(&path))?;
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => WalError::Locked { path: path.clone() },
            TryLockError::Error(source) => WalError::Io {
                path: path.clone(),
                source,
            },
        })?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(WalError::io(&path))?;

        let mut offset = 0;
        let mut frames = 0;
        while offset < bytes.len() {
            match frame::decode(&bytes[offset..], MAX_PAYLOAD) {
                Decoded::Frame { payload, size } => {
                    replay(payload)?;
                    offset += size;
                    frames += 1;
                }
                Decoded::Torn => break,
                Decoded::Corrupt(reason) => {
                    return Err(WalError::Corrupt {
                        path,
                        offset: offset as u64,
                        reason,
                    }
                    .into());
                }
            }
        }

        let committed = offset as u64;
        let truncated_bytes = (bytes.len() - offset) as u64;
        if truncated_bytes > 0 {
            file.set_len(committed).map_err(WalError::io(&path))?;
            file.sync_all().map_err(WalError::io(&path))?;
        }
        file.seek(SeekFrom::Start(committed))
            .map_err(WalError::io(&path))?;

        let wal = Self {
            file,
            path,
            sync,
            committed,
            pending: Vec::new(),
            poisoned: false,
        };
        let recovery = Recovery {
            frames,
            truncated_bytes,
        };
        Ok((wal, recovery))
    }

    /// Buffers a record. Nothing is durable until [`Wal::commit`] returns.
    pub fn append(&mut self, payload: &[u8]) -> Result<(), WalError> {
        if self.poisoned {
            return Err(WalError::Poisoned);
        }
        if payload.len() > MAX_PAYLOAD {
            return Err(WalError::TooLarge { len: payload.len() });
        }
        frame::encode(payload, &mut self.pending);
        Ok(())
    }

    /// Writes all buffered records and applies the [`SyncPolicy`].
    ///
    /// After a failure the log refuses further writes: once `fsync` has failed
    /// the kernel may have dropped dirty pages, so retrying could report success
    /// for data that never reached disk.
    pub fn commit(&mut self) -> Result<(), WalError> {
        if self.poisoned {
            return Err(WalError::Poisoned);
        }
        if self.pending.is_empty() {
            return Ok(());
        }
        let result = self
            .file
            .write_all(&self.pending)
            .and_then(|()| match self.sync {
                SyncPolicy::Always => self.file.sync_data(),
                SyncPolicy::Never => Ok(()),
            });
        if let Err(source) = result {
            self.poisoned = true;
            return Err(WalError::Io {
                path: self.path.clone(),
                source,
            });
        }
        self.committed += self.pending.len() as u64;
        self.pending.clear();
        Ok(())
    }

    /// Size in bytes of the committed log.
    pub fn len(&self) -> u64 {
        self.committed
    }

    /// Whether nothing has been committed.
    pub fn is_empty(&self) -> bool {
        self.committed == 0
    }

    /// Path of the underlying file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}
