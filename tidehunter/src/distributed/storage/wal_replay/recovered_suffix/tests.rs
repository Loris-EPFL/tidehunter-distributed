use super::hook::{Event, Fault, Hook};
use super::*;
use crate::key_shape::{KeySpace, KeyType};
use crate::wal::position::WalFileId;

fn shape() -> KeyShape {
    KeyShape::new_single(4, 1, KeyType::uniform(1))
}

fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}

fn config(fragment_bytes: u64, wal_file_bytes: u64) -> PartitionConfig {
    PartitionConfig {
        inventory: StaticInventory {
            database: [13; 16],
            epoch: 3,
            segment_generation: 9,
            partitions: vec![0],
        },
        partition: 0,
        batch_limits: BatchLimits {
            max_operations: 10,
            max_encoded_bytes: 500,
        },
        group_limits: GroupLimits {
            max_batches: 4,
            max_encoded_bytes: 1008,
        },
        fragment_bytes,
        wal_file_bytes,
        max_batches: 64,
        max_reference_metadata_bytes: 1024 * 1024,
    }
}

fn batch(config: &PartitionConfig, n: u64) -> CompleteBatch {
    let mut operation = [0; 16];
    operation[..8].copy_from_slice(&n.to_be_bytes());
    CompleteBatch::new(
        &shape(),
        BatchId {
            database: config.inventory.database,
            operation,
        },
        BatchVersion {
            epoch: config.inventory.epoch,
            sequence: n,
        },
        vec![Mutation::Put {
            keyspace: KeySpace(0),
            key: (n as u32).to_be_bytes().to_vec().into(),
            value: vec![n as u8; 64].into(),
        }],
        config.batch_limits,
    )
    .unwrap()
}

/// A WAL-replay stream whose first `synced` batches are below its persisted
/// root position, followed by `unsynced` batches, closed without a sync.
/// Returns the root position and the appended end.
fn stream(path: &Path, config: &PartitionConfig, synced: u64, unsynced: u64) -> (u64, u64) {
    let log = PartitionLog::create(path, config.clone(), &shape(), metrics()).unwrap();
    let authority = RequestAuthority {
        epoch: config.inventory.epoch,
    };
    for n in 0..synced + unsynced {
        log.append_complete_batch(authority, &batch(config, n))
            .unwrap();
        if n + 1 == synced {
            let cut = log.state.lock().writer.as_ref().unwrap().position();
            assert_eq!(log.persist_wal_through(cut).unwrap(), cut);
        }
    }
    let root = log.position_frontier_for_test();
    let appended = log.durable_progress().appended_end;
    assert_eq!(root < appended, unsynced > 0);
    (root, appended)
}

fn wal_bytes(path: &Path, config: &PartitionConfig, end: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    for start in (0..end).step_by(config.wal_file_bytes as usize) {
        let name = config
            .layout()
            .wal_file_name(path, WalFileId::from_u64(start / config.wal_file_bytes));
        let file = fs::read(name).unwrap();
        let len = (end - start).min(config.wal_file_bytes) as usize;
        bytes.extend_from_slice(&file[..len]);
    }
    bytes
}

/// Model a device that missed the unsynced suffix (the clean-but-stale cache
/// case): it holds zeros above the synced end and takes a rewritten range
/// only when the sync of its file follows. After recovery it must hold the
/// recovered bytes, so a later no-op fsync can no longer certify stale data.
#[test]
fn restart_makes_the_recovered_unsynced_suffix_durable() {
    let temp = tempdir::TempDir::new("recovered-suffix").unwrap();
    let path = temp.path().join("stream");
    let config = config(1024, 2048);
    // The unsynced batches cross into the second WAL file.
    let (root, appended) = stream(&path, &config, 1, 24);
    assert!(appended > config.wal_file_bytes);
    let recovered_bytes = wal_bytes(&path, &config, appended);
    let mut device = recovered_bytes.clone();
    device[root as usize..].fill(0);
    hook::install(Hook::default());
    let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    let events = hook::take().unwrap().events;
    assert_eq!(log.receipt_count(), 25);
    let mut pending = Vec::new();
    let mut written = 0;
    for event in events {
        match event {
            Event::Write { offset, bytes } => pending.push((offset, bytes)),
            Event::Sync { file_start } => {
                for (offset, bytes) in pending.drain(..) {
                    assert_eq!(
                        offset / config.wal_file_bytes * config.wal_file_bytes,
                        file_start
                    );
                    assert!(offset >= root);
                    // Exactly the validated bytes.
                    let start = offset as usize;
                    let end = start + bytes.len();
                    assert_eq!(bytes[..], recovered_bytes[start..end]);
                    device[start..end].copy_from_slice(&bytes);
                    written += bytes.len();
                }
            }
        }
    }
    assert!(pending.is_empty(), "rewrite without a following sync");
    assert!(written > 0);
    drop(log);
    // The device image alone recovers every batch.
    for start in (0..appended).step_by(config.wal_file_bytes as usize) {
        let name = config
            .layout()
            .wal_file_name(&path, WalFileId::from_u64(start / config.wal_file_bytes));
        let end = (start + config.wal_file_bytes).min(appended) as usize;
        OpenOptions::new()
            .write(true)
            .open(name)
            .unwrap()
            .write_all_at(&device[start as usize..end], 0)
            .unwrap();
    }
    let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(log.receipt_count(), 25);
}

#[test]
fn a_synced_stream_rewrites_nothing() {
    let temp = tempdir::TempDir::new("recovered-suffix-synced").unwrap();
    let path = temp.path().join("stream");
    let config = config(1024, 2048);
    stream(&path, &config, 3, 0);
    hook::install(Hook::default());
    let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(hook::take().unwrap().events, Vec::new());
    assert_eq!(log.receipt_count(), 3);
}

/// A rewrite or sync error fails recovery and leaves the stream recoverable.
#[test]
fn a_rewrite_or_sync_error_fails_recovery() {
    for fault in [Fault::Write, Fault::Sync] {
        let temp = tempdir::TempDir::new("recovered-suffix-fault").unwrap();
        let path = temp.path().join("stream");
        let config = config(1024, 2048);
        stream(&path, &config, 1, 2);
        hook::install(Hook {
            fault: Some(fault),
            ..Hook::default()
        });
        let error = PartitionLog::recover_sealed(&path, &config, metrics())
            .err()
            .unwrap();
        assert!(hook::take().is_some());
        assert!(matches!(error, StorageError::Io(_)), "{fault:?}: {error:?}");
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        assert_eq!(log.receipt_count(), 3, "{fault:?}");
    }
}

/// A clean page evicted after the scan validated it no longer holds the
/// frame. The guard writes the validated bytes, not what it would read now,
/// so the frames still become durable.
#[test]
fn a_page_evicted_after_validation_still_becomes_durable() {
    let temp = tempdir::TempDir::new("recovered-suffix-evicted").unwrap();
    let path = temp.path().join("stream");
    let config = config(1024, 2048);
    stream(&path, &config, 1, 2);
    hook::install(Hook {
        fault: Some(Fault::EvictBeforeWrite),
        ..Hook::default()
    });
    let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert!(hook::take().is_some());
    assert_eq!(log.receipt_count(), 3);
    drop(log);
    let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(log.receipt_count(), 3);
}

/// Restart cost of the guard; run explicitly with `--ignored --nocapture`.
#[test]
#[ignore = "measurement only"]
fn measure_restart_rewrite() {
    for unsynced in [16, 256, 2048, 8192] {
        let temp = tempdir::TempDir::new("recovered-suffix-cost").unwrap();
        let path = temp.path().join("stream");
        let config = config(64 * 1024, 1024 * 1024);
        let (root, appended) = stream(&path, &config, 1, unsynced);
        hook::install(Hook::default());
        let started = Instant::now();
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        let recovery = started.elapsed();
        let hook = hook::take().unwrap();
        let rewritten: usize = hook
            .events
            .iter()
            .map(|event| match event {
                Event::Write { bytes, .. } => bytes.len(),
                Event::Sync { .. } => 0,
            })
            .sum();
        eprintln!(
            "unsynced_batches={unsynced} region_bytes={} rewritten_bytes={rewritten} \
                 guard_ms={:.3} recovery_ms={:.3} receipts={}",
            appended - root,
            hook.elapsed.as_secs_f64() * 1e3,
            recovery.as_secs_f64() * 1e3,
            log.receipt_count()
        );
    }
}
