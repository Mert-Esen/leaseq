//! Atomically replaced, checksummed snapshot files.

use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::Path;

use crate::WalError;
use crate::frame::{self, Decoded, MAX_SNAPSHOT};

/// Replaces `path` with a snapshot holding `payload`.
///
/// The data is written to a sibling temporary file, synced, then renamed over the
/// destination, so readers see either the old snapshot or the new one.
pub fn write_snapshot(path: &Path, payload: &[u8]) -> Result<(), WalError> {
    if payload.len() > MAX_SNAPSHOT {
        return Err(WalError::TooLarge { len: payload.len() });
    }
    let tmp = path.with_extension("tmp");
    let mut buf = Vec::new();
    frame::encode(payload, &mut buf);

    let mut file = File::create(&tmp).map_err(WalError::io(&tmp))?;
    file.write_all(&buf).map_err(WalError::io(&tmp))?;
    file.sync_all().map_err(WalError::io(&tmp))?;
    drop(file);

    fs::rename(&tmp, path).map_err(WalError::io(path))?;
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => sync_dir(parent),
        _ => Ok(()),
    }
}

/// Reads the snapshot at `path`, or `None` if it does not exist.
///
/// Because snapshots are replaced atomically, any damage is reported as corruption.
pub fn read_snapshot(path: &Path) -> Result<Option<Vec<u8>>, WalError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(WalError::io(path)(error)),
    };
    match frame::decode(&bytes, MAX_SNAPSHOT) {
        Decoded::Frame { payload, size } if size == bytes.len() => Ok(Some(payload.to_vec())),
        _ => Err(WalError::Corrupt {
            path: path.to_path_buf(),
            offset: 0,
            reason: "invalid snapshot",
        }),
    }
}

/// Makes creations and renames inside `dir` durable.
///
/// Only Unix exposes this; elsewhere it is a no-op.
pub fn sync_dir(dir: &Path) -> Result<(), WalError> {
    #[cfg(unix)]
    {
        File::open(dir)
            .and_then(|handle| handle.sync_all())
            .map_err(WalError::io(dir))?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
}
