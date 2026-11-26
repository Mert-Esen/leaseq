//! Crash-recovery behaviour of the log, including property-based fault injection.

use std::fs;
use std::path::Path;

use leaseq_wal::{SyncPolicy, Wal, WalError, read_snapshot, write_snapshot};
use proptest::prelude::*;

fn open(path: &Path) -> Result<(Wal, Vec<Vec<u8>>), WalError> {
    let mut records = Vec::new();
    let (wal, _) = Wal::open(path, SyncPolicy::Never, |payload| {
        records.push(payload.to_vec());
        Ok::<_, WalError>(())
    })?;
    Ok((wal, records))
}

fn write(path: &Path, records: &[Vec<u8>]) {
    let (mut wal, _) = open(path).unwrap();
    for record in records {
        wal.append(record).unwrap();
    }
    wal.commit().unwrap();
}

#[test]
fn committed_records_survive_reopen_and_uncommitted_do_not() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    {
        let (mut wal, _) = open(&path).unwrap();
        wal.append(b"kept").unwrap();
        wal.commit().unwrap();
        wal.append(b"never committed").unwrap();
    }
    assert_eq!(open(&path).unwrap().1, vec![b"kept".to_vec()]);
}

#[test]
fn a_second_writer_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let _first = open(&path).unwrap();
    assert!(matches!(open(&path), Err(WalError::Locked { .. })));
}

#[test]
fn corruption_before_the_tail_refuses_to_open_and_preserves_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    write(&path, &[b"one".to_vec(), b"two".to_vec()]);
    let mut bytes = fs::read(&path).unwrap();
    bytes[8] ^= 0xff; // first payload byte
    fs::write(&path, &bytes).unwrap();

    assert!(matches!(
        open(&path),
        Err(WalError::Corrupt { offset: 0, .. })
    ));
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

#[test]
fn snapshot_round_trips_and_detects_damage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    assert_eq!(read_snapshot(&path).unwrap(), None);

    write_snapshot(&path, b"v1").unwrap();
    write_snapshot(&path, b"v2").unwrap();
    assert_eq!(read_snapshot(&path).unwrap(), Some(b"v2".to_vec()));

    let mut bytes = fs::read(&path).unwrap();
    bytes.pop();
    fs::write(&path, bytes).unwrap();
    assert!(matches!(
        read_snapshot(&path),
        Err(WalError::Corrupt { .. })
    ));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Cutting the file at *any* byte yields exactly the records that were fully
    /// written, and the log accepts new appends afterwards.
    #[test]
    fn truncation_at_any_offset_recovers_a_prefix(
        records in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 1..12),
        cut in any::<prop::sample::Index>(),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        write(&path, &records);

        let full = fs::read(&path).unwrap();
        let cut = cut.index(full.len() + 1);
        fs::write(&path, &full[..cut]).unwrap();

        let mut end = 0;
        let intact = records.iter().take_while(|record| {
            end += 8 + record.len();
            end <= cut
        }).count();

        let (mut wal, recovered) = open(&path).unwrap();
        prop_assert_eq!(&recovered[..], &records[..intact]);

        wal.append(b"after crash").unwrap();
        wal.commit().unwrap();
        drop(wal);
        let (_, recovered) = open(&path).unwrap();
        prop_assert_eq!(recovered.len(), intact + 1);
        prop_assert_eq!(recovered.last().unwrap().as_slice(), b"after crash");
    }
}
