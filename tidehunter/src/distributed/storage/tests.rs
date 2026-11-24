use super::*;
use crate::distributed::codec::Mutation;
use crate::key_shape::{KeySpace, KeyType};
use std::os::unix::fs::FileExt;

fn shape() -> KeyShape {
    KeyShape::new_single(4, 1, KeyType::uniform(1))
}

fn config(partitions: Vec<u32>, partition: u32) -> PartitionConfig {
    PartitionConfig {
        inventory: StaticInventory {
            database: [1; 16],
            epoch: 5,
            segment_generation: 27,
            partitions,
        },
        partition,
        batch_limits: BatchLimits {
            max_operations: 100,
            max_encoded_bytes: 500,
        },
        group_limits: GroupLimits {
            max_batches: 4,
            max_encoded_bytes: 1008,
        },
        fragment_bytes: 1024,
        wal_file_bytes: 2048,
        max_batches: 64,
        max_reference_metadata_bytes: 1024 * 1024,
    }
}

fn id_for(config: &PartitionConfig, number: u64) -> BatchId {
    let mut operation = [0; 16];
    operation[..8].copy_from_slice(&number.to_be_bytes());
    let mut id = BatchId {
        database: config.inventory.database,
        operation,
    };
    for b in 0..=255 {
        id.operation[15] = b;
        if config.inventory.partition_for(id).unwrap() == config.partition {
            return id;
        }
    }
    panic!("test inventory cannot place identity");
}

fn batch(config: &PartitionConfig, n: u64) -> CompleteBatch {
    CompleteBatch::new(
        &shape(),
        id_for(config, n),
        BatchVersion {
            epoch: config.inventory.epoch,
            sequence: n,
        },
        vec![
            Mutation::Put {
                keyspace: KeySpace(0),
                key: b"key1"[..].into(),
                value: vec![n as u8; 64].into(),
            },
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: b"key2"[..].into(),
            },
        ],
        config.batch_limits,
    )
    .unwrap()
}

fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}

fn live_config() -> PartitionConfig {
    let mut config = config(vec![3], 3);
    config.group_limits.max_encoded_bytes = 4088;
    config.fragment_bytes = 4096;
    config.wal_file_bytes = 8192;
    config
}

/// Leased-route position for a frame of `span` bytes: the open extent when the
/// frame fits, otherwise the next extent after closing the open one behind a
/// skip marker (or exactly at its end).
fn live_fixture_extent(
    log: &PartitionLog,
    config: &PartitionConfig,
    span: u64,
) -> (
    crate::distributed::nvmeof::live_storage::LiveWriteExtent,
    u64,
) {
    let open = log
        .state
        .lock()
        .open_live_extent
        .as_ref()
        .map(|open| (open.extent.clone(), open.adoption.adopted_end));
    match open {
        Some((extent, end)) if end + span <= extent.end => (extent, end),
        Some((extent, end)) => {
            if end < extent.end {
                let mut block = [0; 4096];
                block[..8].copy_from_slice(CrcFrame::skip_marker().as_ref());
                write_live_fixture(&extent, end, &block);
            }
            log.adopt_live_extent(authority(config), extent.token, end, true)
                .unwrap();
            let next = log.lease_live_extent(authority(config)).unwrap().0;
            let start = next.start;
            (next, start)
        }
        None => {
            let next = log.lease_live_extent(authority(config)).unwrap().0;
            let start = next.start;
            (next, start)
        }
    }
}

fn write_live_fixture(
    extent: &crate::distributed::nvmeof::live_storage::LiveWriteExtent,
    offset: u64,
    bytes: &[u8],
) {
    OpenOptions::new()
        .write(true)
        .open(&extent.segment.path)
        .unwrap()
        .write_all_at(bytes, offset % extent.segment.bytes)
        .unwrap();
}

/// Write one single-batch live frame on the leased route and adopt it, closing
/// the extent when the frame fills it. Returns the adopted receipt.
fn append_live_fixture(log: &PartitionLog, config: &PartitionConfig, n: u64) -> AppendedBatch {
    use crate::distributed::nvmeof::live_storage::LiveGroupDescriptor;
    let group = CompleteGroup::new_live(
        &shape(),
        vec![batch(config, n)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
    let span = (frame.len() as u64).div_ceil(4096) * 4096;
    let (extent, offset) = live_fixture_extent(log, config, span);
    write_live_fixture(&extent, offset, &frame);
    let through = offset + span;
    log.adopt_live_extent(
        authority(config),
        extent.token,
        through,
        through == extent.end,
    )
    .unwrap();
    let count = log.receipt_count();
    log.receipt_page(count - 1, 1).unwrap()[0]
}

/// Live geometry whose fragment holds several 4 KiB frames, so the syncer can
/// defer the root within a fragment.
fn live_multi_frame_config() -> PartitionConfig {
    let mut config = live_config();
    config.fragment_bytes = 64 * 1024;
    config.wal_file_bytes = 128 * 1024;
    config
}

fn live_root_end(path: &Path) -> u64 {
    read_metadata::<LiveFrontier>(&path.join(LIVE_FRONTIER))
        .unwrap()
        .end
}

fn live_log_for(
    path: &Path,
    config: &PartitionConfig,
    tier: crate::distributed::durability::DurabilityTier,
) -> PartitionLog {
    let log = PartitionLog::create_live(path, config.clone(), &shape(), metrics()).unwrap();
    log.attach_frontier(&crate::distributed::durability::DurabilityConfig::with_tier(tier))
        .unwrap();
    log
}

const TIERS: [crate::distributed::durability::DurabilityTier; 2] = [
    crate::distributed::durability::DurabilityTier::Native,
    crate::distributed::durability::DurabilityTier::Synchronous,
];

/// The syncer syncs the WAL but defers the root inside a fragment (WAL-replay
/// history, the only history). A crash recovers every synced frame by
/// scanning past the root.
#[test]
fn syncer_defers_live_root_and_recovers_by_scan() {
    for tier in TIERS {
        let temp = tempdir::TempDir::new("live-root-deferred").unwrap();
        let config = live_multi_frame_config();
        let path = temp.path().join("stream");
        let log = live_log_for(&path, &config, tier);
        let first = append_live_fixture(&log, &config, 0);
        log.sync_for_frontier().unwrap();
        let second = append_live_fixture(&log, &config, 1);
        log.sync_for_frontier().unwrap();
        let stats = log.stats();
        assert_eq!(stats.wal_file_syncs, 2);
        log.validate_durable_receipt(DurableBatch(second)).unwrap();
        assert_eq!(
            (stats.live_frontier_installs, stats.live_frontier_deferred),
            (0, 2)
        );
        assert_eq!(live_root_end(&path), 0);
        drop(log);
        let recovered =
            PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
        assert_eq!(recovered.lookup(first.id()).unwrap(), Some(first));
        assert_eq!(recovered.lookup(second.id()).unwrap(), Some(second));
    }
}

/// A fail-stopped live stream has no position proof, so the durable frontier
/// its acknowledgements installed is the witness a restart compares a
/// truncated WAL against. Only the frontier covers the acknowledged end here.
#[test]
fn live_synced_end_includes_the_durable_frontier() {
    let temp = tempdir::TempDir::new("live-synced-frontier").unwrap();
    let config = live_config();
    let path = temp.path().join("stream");
    let log = PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
    let acknowledged = append_live_fixture(&log, &config, 0);
    log.persist_through(acknowledged).unwrap();
    let frontier = live_root_end(&path);
    assert_eq!(
        frontier, 4096,
        "the barrier installed the durable frontier before the receipt"
    );
    let root_end = WalRoot::open(&path, &config).unwrap().synced_end;
    assert!(
        root_end < frontier,
        "no position proof ran, so the WAL root must lag the frontier: {root_end} >= {frontier}"
    );
    assert_eq!(
        PartitionLog::live_synced_end(&path, &config).unwrap(),
        Some(frontier),
        "the acknowledged end is the pre-restart witness without a position proof"
    );
}

/// Crash at every step of the explicit root catch-up after a deferral: the
/// failed barrier reports an unknown outcome, and recovery still finds every
/// synced frame whichever root (old or new) survived.
#[test]
fn live_root_catch_up_crash_cuts_recover_every_synced_frame() {
    for tier in TIERS {
        for phase in 0..=3 {
            let temp = tempdir::TempDir::new("live-root-catch-up").unwrap();
            let config = live_multi_frame_config();
            let path = temp.path().join("stream");
            let log = live_log_for(&path, &config, tier);
            let first = append_live_fixture(&log, &config, 0);
            let second = append_live_fixture(&log, &config, 1);
            log.sync_for_frontier().unwrap();
            assert_eq!(live_root_end(&path), 0);
            log.fail_live_frontier.store(phase, Ordering::Release);
            let result = log.persist_live_root();
            if phase == 0 {
                result.unwrap();
                assert_eq!(live_root_end(&path), 8192);
                assert_eq!(log.stats().live_frontier_installs, 1);
                // The root is current: a second barrier installs nothing.
                log.persist_live_root().unwrap();
                assert_eq!(log.stats().live_frontier_installs, 1);
            } else {
                assert!(matches!(result, Err(StorageError::Unknown(_))));
                assert_eq!(live_root_end(&path), if phase == 3 { 8192 } else { 0 });
            }
            drop(log);
            let recovered =
                PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
            assert_eq!(recovered.lookup(first.id()).unwrap(), Some(first));
            assert_eq!(recovered.lookup(second.id()).unwrap(), Some(second));
        }
    }
}

/// A fragment crossing installs the root even under the bounded policy, and
/// only that install reports durable progress.
#[test]
fn syncer_installs_live_root_on_fragment_crossing() {
    let temp = tempdir::TempDir::new("live-root-crossing").unwrap();
    let mut config = live_multi_frame_config();
    config.fragment_bytes = 8192;
    let path = temp.path().join("stream");
    let log = live_log_for(
        &path,
        &config,
        crate::distributed::durability::DurabilityTier::Synchronous,
    );
    append_live_fixture(&log, &config, 0);
    log.sync_for_frontier().unwrap();
    assert_eq!(log.stats().live_frontier_installs, 0);
    let deferred = log.durable_progress();
    assert_eq!((deferred.synced, deferred.synced_end), (None, 0));
    append_live_fixture(&log, &config, 1);
    log.sync_for_frontier().unwrap();
    assert_eq!(log.stats().live_frontier_installs, 1);
    assert_eq!(live_root_end(&path), 8192);
    let rooted = log.durable_progress();
    assert_eq!(
        (rooted.synced, rooted.synced_end, rooted.generation),
        (Some(version(&config, 1)), 8192, 1)
    );
}

/// An abrupt crash after one synced live append, then the owner operations of
/// a recovery open on the recovered sealed log: per-receipt persistence
/// barriers, flush, the fence/retire root catch-up and durable-progress status.
/// Recovery leaves no unsynced data and no root debt, so none of them may need
/// a live tail. A second recovery proves these operations leave the stream
/// recoverable.
#[test]
fn recovered_live_log_serves_recovery_barriers_after_abrupt_crash() {
    use crate::distributed::durability::{DurabilityConfig, DurabilityTier};
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        {
            let temp = tempdir::TempDir::new("live-abrupt-recovery").unwrap();
            let mut config = live_config();
            config.fragment_bytes = 64 * 1024;
            config.wal_file_bytes = 128 * 1024;
            let path = temp.path().join("stream");
            let log =
                PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
            log.attach_frontier(&DurabilityConfig::with_tier(tier))
                .unwrap();
            let appended = append_live_fixture(&log, &config, 0);
            log.sync_for_frontier().unwrap();
            // Abrupt: no flush, fence or shutdown barrier runs before the drop.
            drop(log);
            let mut durable_state = None;
            // Each pass drops the recovered handle without shutdown: a second
            // abrupt crash after the post-recovery barriers.
            for _ in 0..2 {
                let recovered =
                    PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(()))
                        .unwrap();
                // Recovery itself leaves a root covering the recovered end.
                assert_eq!(live_root_end(&path), 4096);
                let receipts = recovered.receipt_page(0, 16).unwrap();
                assert_eq!(receipts, vec![appended]);
                assert_eq!(
                    recovered.persist_through(appended).unwrap(),
                    DurableBatch(appended)
                );
                assert_eq!(
                    recovered.persist_many(&receipts).unwrap(),
                    vec![DurableBatch(appended)]
                );
                assert_eq!(recovered.persist_appended().unwrap(), 4096);
                recovered.persist_live_root().unwrap();
                // The recovered handle owes no root: the barriers install none.
                assert_eq!(recovered.stats().live_frontier_installs, 0);
                let progress = recovered.durable_progress();
                assert_eq!((progress.appended_end, progress.synced_end), (4096, 4096));
                assert_eq!(recovered.lookup(appended.id()).unwrap(), Some(appended));
                assert_eq!(
                    recovered.read_batch(appended).unwrap().encoded(),
                    batch(&config, 0).encoded()
                );
                let state = (
                    read_metadata::<LiveFrontier>(&path.join(LIVE_FRONTIER)).unwrap(),
                    read_metadata::<SealedEnd>(&path.join(SEAL)).unwrap(),
                );
                assert_eq!(state.1.physical_end, 4096);
                assert_eq!(*durable_state.get_or_insert_with(|| state.clone()), state);
            }
        }
    }
}

/// With a lagging root, recovery scans forward and stops at the first invalid
/// frame beyond it (a torn tail), keeping every valid frame before it.
#[test]
fn lagging_live_root_recovery_stops_at_first_invalid_frame() {
    for tier in TIERS {
        let temp = tempdir::TempDir::new("live-root-lagging").unwrap();
        let config = live_multi_frame_config();
        let path = temp.path().join("stream");
        let log = live_log_for(&path, &config, tier);
        let first = append_live_fixture(&log, &config, 0);
        let second = append_live_fixture(&log, &config, 1);
        let third = append_live_fixture(&log, &config, 2);
        log.sync_for_frontier().unwrap();
        assert_eq!(live_root_end(&path), 0);
        drop(log);
        // Tear the middle frame: everything from it on is an unproven tail.
        tear_live_frame(&path, &config, second);
        let recovered =
            PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
        assert_eq!(recovered.lookup(first.id()).unwrap(), Some(first));
        assert_eq!(recovered.lookup(second.id()).unwrap(), None);
        assert_eq!(recovered.lookup(third.id()).unwrap(), None);
    }
}

fn version(config: &PartitionConfig, sequence: u64) -> BatchVersion {
    BatchVersion {
        epoch: config.inventory.epoch,
        sequence,
    }
}

/// Overwrite the CRC-covered start of `receipt`'s frame and sync the file. The
/// first WAL file is the only directory entry of the full file size.
fn tear_live_frame(path: &Path, config: &PartitionConfig, receipt: AppendedBatch) {
    let wal = fs::read_dir(path)
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| {
            entry
                .metadata()
                .is_ok_and(|meta| meta.is_file() && meta.len() == config.wal_file_bytes)
        })
        .unwrap()
        .path();
    let file = OpenOptions::new().write(true).open(wal).unwrap();
    file.write_all_at(&[0x5a; 16], receipt.address().offset + 8)
        .unwrap();
    file.sync_all().unwrap();
}

/// A durable-progress report covers only the prefix of the installed strict
/// live root. With the root deferred, the report names no version, so no cut
/// built from it covers a frame that recovery drops as a torn tail. After the
/// nudge barrier rooted and reported all three frames (a cut `next = 3`), the
/// same corruption of the middle frame is fatal at recovery: certified batches
/// are never dropped silently. Each run stops abruptly, without a barrier.
#[test]
fn durable_progress_covers_only_the_rooted_prefix_and_recovery_enforces_it() {
    for tier in TIERS {
        for nudge in [false, true] {
            let temp = tempdir::TempDir::new("live-root-progress").unwrap();
            let config = live_multi_frame_config();
            let path = temp.path().join("stream");
            let log = live_log_for(&path, &config, tier);
            let first = append_live_fixture(&log, &config, 0);
            let second = append_live_fixture(&log, &config, 1);
            let third = append_live_fixture(&log, &config, 2);
            log.sync_for_frontier().unwrap();
            // Every frame is synced, but the bounded syncer deferred the root.
            let end = log.appended_frontier();
            assert_eq!(log.synced_frontier(), end);
            assert_eq!(live_root_end(&path), 0);
            let deferred = log.durable_progress();
            assert_eq!(
                (
                    deferred.appended,
                    deferred.synced,
                    deferred.synced_end,
                    deferred.generation
                ),
                (Some(version(&config, 2)), None, 0, 0)
            );
            if nudge {
                assert!(log.persist_live_progress().unwrap());
                assert_eq!(live_root_end(&path), end);
                let rooted = log.durable_progress();
                assert_eq!(
                    (rooted.synced, rooted.synced_end, rooted.generation),
                    (Some(version(&config, 2)), end, 1)
                );
                assert!(rooted.idle());
            }
            // The cut the coordinator may persist from this report.
            let cut_next = log
                .durable_progress()
                .synced
                .map_or(0, |version| version.sequence + 1);
            assert_eq!(cut_next, if nudge { 3 } else { 0 });
            drop(log);
            tear_live_frame(&path, &config, second);
            let recovered = PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(()));
            if nudge {
                // Below the strict root a torn frame is corrupt durable state.
                assert!(
                    matches!(recovered, Err(StorageError::Invalid(_))),
                    "{:?}",
                    recovered.as_ref().err()
                );
            } else {
                // Below the cut nothing is lost; the torn tail starts above it.
                let recovered = recovered.unwrap();
                assert_eq!(recovered.lookup(first.id()).unwrap(), Some(first));
                assert_eq!(recovered.lookup(second.id()).unwrap(), None);
                assert_eq!(recovered.lookup(third.id()).unwrap(), None);
            }
        }
    }
}

/// The nudge barrier syncs and roots what the syncer left unreported, reports
/// it, and installs nothing when the report is current. A recovered handle has
/// identical byte ends and its recovery root, so its nudge needs no live tail.
/// A stream that is not live leaves the nudge to its syncer.
#[test]
fn progress_nudge_roots_the_appended_prefix_once() {
    for tier in TIERS {
        let temp = tempdir::TempDir::new("live-progress-nudge").unwrap();
        let config = live_multi_frame_config();
        let path = temp.path().join("stream");
        let log = live_log_for(&path, &config, tier);
        append_live_fixture(&log, &config, 0);
        log.sync_for_frontier().unwrap();
        assert!(log.persist_live_progress().unwrap());
        let first_end = log.appended_frontier();
        assert_eq!(live_root_end(&path), first_end);
        assert_eq!(log.stats().live_frontier_installs, 1);
        // Unsynced: the barrier syncs the WAL and installs the root over it.
        let appended = append_live_fixture(&log, &config, 1);
        let syncs = log.stats().wal_file_syncs;
        assert!(log.persist_live_progress().unwrap());
        let end = log.appended_frontier();
        assert!(end > first_end);
        let stats = log.stats();
        assert_eq!(
            (stats.wal_file_syncs, stats.live_frontier_installs),
            (syncs + 1, 2)
        );
        assert_eq!(live_root_end(&path), end);
        let progress = log.durable_progress();
        assert_eq!(
            (progress.synced, progress.synced_end, progress.generation),
            (Some(version(&config, 1)), end, 2)
        );
        // Current report: no further install.
        assert!(log.persist_live_progress().unwrap());
        assert_eq!(log.stats().live_frontier_installs, 2);
        drop(log);
        let recovered =
            PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
        assert!(recovered.persist_live_progress().unwrap());
        assert_eq!(recovered.stats().live_frontier_installs, 0);
        let progress = recovered.durable_progress();
        assert_eq!(
            (progress.synced, progress.appended_end, progress.synced_end),
            (None, end, end)
        );
        assert_eq!(recovered.lookup(appended.id()).unwrap(), Some(appended));
    }
    let temp = tempdir::TempDir::new("progress-nudge-not-live").unwrap();
    let config = config(vec![3], 3);
    let log =
        PartitionLog::create(&temp.path().join("stream"), config, &shape(), metrics()).unwrap();
    assert!(!log.persist_live_progress().unwrap());
}

/// Adoption requests no per-frame sync. An owner read of the newest batch
/// returns it in both tiers: Synchronous persists the WAL on demand first
/// (one fdatasync, no root install); Native serves the page-cache copy without
/// a sync. Either way a crash after the read recovers the batch.
#[test]
fn owner_read_of_newest_batch_persists_on_demand_only_in_synchronous_tier() {
    for tier in TIERS {
        let temp = tempdir::TempDir::new("live-read-on-demand").unwrap();
        let config = live_multi_frame_config();
        let path = temp.path().join("stream");
        let log = live_log_for(&path, &config, tier);
        let newest = append_live_fixture(&log, &config, 0);
        let reference = log.record_reference(newest, 0).unwrap();
        let before = log.stats();
        // The owner serving boundary (`validate_durable_record_reference`, used
        // by the owner read handler, read workers and record grants) obtains its
        // own covering barrier before any byte leaves the owner.
        log.validate_durable_record_reference(&reference).unwrap();
        assert_eq!(
            log.read_record(&reference).unwrap(),
            batch(&config, 0).mutations()[0],
            "{tier}"
        );
        let after = log.stats();
        let synchronous = tier == crate::distributed::durability::DurabilityTier::Synchronous;
        assert_eq!(
            after.wal_file_syncs - before.wal_file_syncs,
            u64::from(synchronous),
            "{tier}"
        );
        assert_eq!(after.live_frontier_installs, before.live_frontier_installs);
        assert_eq!(
            after.read_persist_requests - before.read_persist_requests,
            u64::from(synchronous),
            "{tier}"
        );
        if synchronous {
            log.validate_durable_receipt(DurableBatch(newest)).unwrap();
        }
        drop(log);
        let recovered =
            PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
        assert_eq!(recovered.lookup(newest.id()).unwrap(), Some(newest));
    }
}

#[test]
fn live_recovery_fails_on_acknowledged_corruption_even_skip_header() {
    for skip_header in [false, true] {
        let temp = tempdir::TempDir::new("live-frontier-corruption").unwrap();
        let config = live_config();
        let path = temp.path().join("stream");
        let log = PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
        let receipt = append_live_fixture(&log, &config, 0);
        // This first frame of a fresh log sits at file offset 0 (the corruption
        // offsets below rely on it); its receipt names the only member, at
        // frame offset + 8-byte outer frame header + member offset.
        let group = CompleteGroup::new_live(
            &shape(),
            vec![batch(&config, 0)],
            config.batch_limits,
            config.group_limits,
        )
        .unwrap();
        let frame_offset = 0;
        let member_offset = group.batch_offsets()[0];
        assert_eq!(
            receipt.address().offset,
            frame_offset + 8 + u64::from(member_offset)
        );
        log.persist_many(&[receipt]).unwrap();
        let file = OpenOptions::new()
            .write(true)
            .open(
                config
                    .layout()
                    .wal_file_name(&path, crate::wal::position::WalFileId::from_u64(0)),
            )
            .unwrap();
        drop(log);
        if skip_header {
            file.write_all_at(&[0xff; 8], 0).unwrap();
        } else {
            file.write_all_at(&[0x55], 32).unwrap();
        }
        file.sync_all().unwrap();
        assert!(PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).is_err());
    }
}

#[test]
fn live_corrupt_frontier_is_not_treated_as_empty() {
    let temp = tempdir::TempDir::new("live-frontier-root").unwrap();
    let config = live_config();
    let path = temp.path().join("stream");
    let log = PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
    drop(log);
    let file = OpenOptions::new()
        .write(true)
        .open(path.join(LIVE_FRONTIER))
        .unwrap();
    file.write_all_at(&[0xff], 0).unwrap();
    file.sync_all().unwrap();
    assert!(PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).is_err());
}

#[test]
fn live_frontier_commit_failures_preserve_prior_ack_and_return_unknown() {
    for phase in 1..=3 {
        let temp = tempdir::TempDir::new("live-frontier-phase-failure").unwrap();
        let config = live_config();
        let path = temp.path().join("stream");
        let log = PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
        let acknowledged = append_live_fixture(&log, &config, 0);
        log.persist_through(acknowledged).unwrap();
        let unacknowledged = append_live_fixture(&log, &config, 1);
        let before = log.durable_progress();
        assert_eq!(before.synced, Some(acknowledged.version()));
        assert_eq!(before.appended, Some(unacknowledged.version()));
        assert_eq!(before.generation, 1);
        log.fail_live_frontier.store(phase, Ordering::Release);
        let mut profile = StorageWriteProfile::default();
        assert!(matches!(
            log.persist_many_profiled(&[unacknowledged], &mut profile),
            Err(StorageError::Unknown(_))
        ));
        assert_eq!(profile.persist_errors, 1);
        assert_eq!(profile.live_frontier_file_calls, 1);
        assert_eq!(profile.live_frontier_file_successes, u64::from(phase != 1));
        assert_eq!(profile.live_frontier_namespace_calls, u64::from(phase == 3));
        assert_eq!(profile.live_frontier_namespace_successes, 0);
        // A failed live-root sync publishes nothing: the reported synced tail stays.
        let after = log.durable_progress();
        assert_eq!(
            (after.synced, after.synced_end, after.generation),
            (before.synced, before.synced_end, before.generation)
        );
        assert!(matches!(
            log.validate_durable_receipt(DurableBatch(unacknowledged)),
            Err(StorageError::Poisoned)
        ));
        let frontier: LiveFrontier = read_metadata(&path.join(LIVE_FRONTIER)).unwrap();
        assert_eq!(frontier.end, if phase == 3 { 8192 } else { 4096 });
        drop(log);
        // No target namespace exists in this positional-write test.
        let recovered =
            PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
        assert_eq!(
            recovered.lookup(acknowledged.id()).unwrap(),
            Some(acknowledged)
        );
        // The frame is complete and valid, so recovery adopts it whether or not
        // the failed frontier publication left the new root visible.
        assert_eq!(
            recovered.lookup(unacknowledged.id()).unwrap(),
            Some(unacknowledged)
        );
    }
}

#[test]
fn live_frontier_commit_uses_capture_not_later_completed_append() {
    let temp = tempdir::TempDir::new("live-frontier-capture").unwrap();
    let config = live_config();
    let path = temp.path().join("stream");
    let log =
        Arc::new(PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap());
    let first = append_live_fixture(&log, &config, 0);
    let (entered, resume) = log.pause_next_persist();
    let writer_log = Arc::clone(&log);
    let writer = std::thread::spawn(move || writer_log.persist_through(first));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let later = append_live_fixture(&log, &config, 1);
    resume.send(()).unwrap();
    let durable = writer.join().unwrap().unwrap();
    assert_eq!(durable.appended(), first);
    let frontier: LiveFrontier = read_metadata(&path.join(LIVE_FRONTIER)).unwrap();
    assert_eq!(frontier.end, 4096);
    assert_eq!(frontier.tail.unwrap().id, first.id());
    assert!(matches!(
        log.validate_durable_receipt(DurableBatch(later)),
        Err(StorageError::NeedsPersistence)
    ));
    drop(log);
    // No target namespace exists in this positional-write test.
    let recovered =
        PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
    assert_eq!(recovered.lookup(first.id()).unwrap(), Some(first));
    // The completed, valid frame beyond the captured frontier is adopted: it was
    // appended (and, under the native tier, acknowledged) before the crash.
    assert_eq!(recovered.lookup(later.id()).unwrap(), Some(later));
}

#[test]
fn direct_append_panic_after_native_allocation_requires_recovery() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let temp = tempdir::TempDir::new("native-append-unwind").unwrap();
    let config = config(vec![3], 3);
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let first = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(first).unwrap();
    log.state
        .lock()
        .writer
        .as_ref()
        .unwrap()
        .fp
        .0
        .store(Arc::new(crate::wal::WalFailPointsInner {
            fp_multi_write_before_write_buf: crate::failpoints::FailPoint::panic_after_n_calls(0),
        }));
    let group = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 1)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let mut profile = StorageWriteProfile::default();
    // Exercise the public direct API without any caller-side panic guard.
    let result = catch_unwind(AssertUnwindSafe(|| {
        log.append_group_profiled(authority(&config), &group, &mut profile)
    }));
    assert!(result.is_err());
    assert_eq!(profile.append_calls, 1);
    assert_eq!(profile.append_errors, 1);
    assert_eq!(profile.appended_groups, 0);
    assert_eq!(profile.appended_receipts, 0);
    assert_eq!(profile.append_publication_ns, 0);
    assert_profile_stage_bounds(profile);
    log.state
        .lock()
        .writer
        .as_ref()
        .unwrap()
        .fp
        .0
        .store(Arc::new(crate::wal::WalFailPointsInner::default()));
    assert!(matches!(
        log.append_complete_batch(authority(&config), &batch(&config, 2)),
        Err(StorageError::Poisoned)
    ));
    assert!(matches!(
        log.persist_through(first),
        Err(StorageError::Poisoned)
    ));
    assert!(matches!(
        log.seal_epoch(authority(&config)),
        Err(StorageError::Poisoned)
    ));
    // Earlier immutable authority remains readable for diagnosis/recovery; this
    // does not authorize a serving database to bypass its own health gate.
    assert_eq!(log.read_batch(first).unwrap().id(), first.id());
    assert_eq!(log.stats().appended_groups, 1);
}

#[test]
fn group_members_share_one_persisted_boundary_and_retry_without_reappend() {
    let temp = tempdir::TempDir::new("native-group-persist").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let group = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 0), batch(&config, 1)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let receipts = log.append_group(authority(&config), &group).unwrap();
    assert_eq!(receipts.len(), 2);
    assert_eq!(log.stats().appended_groups, 1);
    assert_eq!(log.stats().persistence_barriers, 0);
    let third = batch(&config, 2);
    let third_receipt = log
        .append_complete_batch(authority(&config), &third)
        .unwrap();
    assert_eq!(log.stats().appended_groups, 2);
    assert!(matches!(
        log.validate_durable_receipt(DurableBatch(third_receipt)),
        Err(StorageError::NeedsPersistence)
    ));
    // Even the FIRST member's receipt syncs the entire captured appended prefix.
    log.persist_through(receipts[0]).unwrap();
    log.persist_through(receipts[1]).unwrap();
    log.persist_through(third_receipt).unwrap();
    assert_eq!(log.stats().persistence_barriers, 1);
    assert_eq!(
        log.append_group(authority(&config), &group).unwrap(),
        receipts
    );
    assert_eq!(log.stats().appended_groups, 2);
    let fourth = batch(&config, 3);
    let mixed = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 0), fourth.clone()],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    assert!(matches!(
        log.append_group(authority(&config), &mixed),
        Err(StorageError::MixedRetry)
    ));
    assert_eq!(log.lookup(fourth.id()).unwrap(), None);
    assert_eq!(log.lookup(third.id()).unwrap(), Some(third_receipt));
    drop(log);
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    let recovered_receipts = [receipts[0], receipts[1], third_receipt];
    assert_eq!(recovered.receipt_page(0, 256).unwrap(), recovered_receipts);
    for receipt in recovered_receipts {
        let reference = recovered.record_reference(receipt, 0).unwrap();
        assert_eq!(
            recovered.read_record(&reference).unwrap(),
            batch(&config, receipt.version.sequence).mutations()[0]
        );
    }
}

#[test]
fn bulk_persistence_validates_every_receipt_before_io_and_keeps_exact_order() {
    let temp = tempdir::TempDir::new("bulk-persist-receipts").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    assert!(log.persist_many(&[]).unwrap().is_empty());
    let group = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 0), batch(&config, 1)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let receipts = log.append_group(authority(&config), &group).unwrap();
    let mut foreign = receipts[1];
    foreign.address.segment_generation += 1;
    assert!(matches!(
        log.persist_many(&[receipts[0], foreign]),
        Err(StorageError::Invalid(_))
    ));
    assert_eq!(log.stats().persistence_barriers, 0);
    assert_eq!(log.stats().namespace_syncs, 0);
    let later = log
        .append_complete_batch(authority(&config), &batch(&config, 2))
        .unwrap();
    let reordered = [receipts[1], receipts[0], receipts[1]];
    let durable = log.persist_many(&reordered).unwrap();
    assert_eq!(
        durable.iter().map(|r| r.appended()).collect::<Vec<_>>(),
        reordered
    );
    assert_eq!(log.stats().persistence_barriers, 1);
    assert_eq!(log.stats().namespace_syncs, 1);
    // A persistence request captures all complete groups appended so far.
    log.persist_many(&reordered).unwrap();
    assert_eq!(log.stats().persistence_barriers, 1);
    assert!(
        pending_persistence(&log.state.lock(), later)
            .unwrap()
            .is_none()
    );
    let mixed = [receipts[0], later, receipts[1]];
    assert_eq!(
        log.persist_many(&mixed)
            .unwrap()
            .iter()
            .map(|r| r.appended())
            .collect::<Vec<_>>(),
        mixed
    );
    assert_eq!(log.stats().persistence_barriers, 1);
    drop(log);
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(recovered.persist_many(&mixed).unwrap().len(), 3);
    assert_eq!(recovered.stats().persistence_barriers, 0);
}

#[test]
fn namespace_sync_is_amortized_only_for_the_exact_wal_file() {
    use std::collections::BTreeSet;
    let temp = tempdir::TempDir::new("persist-namespace-files").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let mut files = BTreeSet::new();
    let mut receipts = Vec::new();
    for n in 0..12 {
        let receipt = log
            .append_complete_batch(authority(&config), &batch(&config, n))
            .unwrap();
        files.insert(config.layout().locate_file(receipt.address.offset));
        log.persist_many(&[receipt]).unwrap();
        receipts.push(receipt);
        assert_eq!(log.stats().persistence_barriers, n + 1);
        assert_eq!(log.stats().namespace_syncs, files.len() as u64);
        assert_eq!(
            log.state.lock().namespace_synced_file,
            Some(config.layout().locate_file(receipt.address.offset))
        );
    }
    assert!(files.len() > 1);
    assert!(log.stats().namespace_syncs < log.stats().persistence_barriers);
    drop(log);
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(recovered.receipt_page(0, 256).unwrap(), receipts);
    assert!(recovered.state.lock().namespace_synced_file.is_none());
    assert_eq!(recovered.persist_many(&receipts).unwrap().len(), 12);
    assert_eq!(recovered.stats().namespace_syncs, 0);
}

#[test]
fn failed_new_file_namespace_sync_never_acknowledges_or_advances_proof() {
    let temp = tempdir::TempDir::new("persist-namespace-failure").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let mut all = Vec::new();
    let first_file = config.layout().locate_file(0);
    let pending = loop {
        let n = all.len() as u64;
        let receipt = log
            .append_complete_batch(authority(&config), &batch(&config, n))
            .unwrap();
        all.push(receipt);
        if config.layout().locate_file(receipt.address.offset) != first_file {
            break receipt;
        }
        log.persist_through(receipt).unwrap();
    };
    let before = log.stats();
    let durable_end = log.state.lock().durable_end;
    log.fail_namespace_persist.store(true, Ordering::Release);
    let mut profile = StorageWriteProfile::default();
    assert!(matches!(
        log.persist_many_profiled(&[all[0], pending], &mut profile),
        Err(StorageError::Unknown(_))
    ));
    assert_eq!(profile.persist_calls, 1);
    assert_eq!(profile.persist_errors, 1);
    assert_eq!(profile.persist_noops, 0);
    assert_eq!(profile.persisted_receipts, 0);
    assert!(profile.file_persist_calls > 0);
    assert!(profile.file_persist_successes > 0);
    assert_eq!(profile.namespace_persist_calls, 1);
    assert_eq!(profile.namespace_persist_successes, 0);
    assert_eq!(profile.durable_publications, 0);
    assert_profile_stage_bounds(profile);
    assert_eq!(log.stats(), before);
    assert_eq!(log.state.lock().durable_end, durable_end);
    assert_eq!(log.state.lock().namespace_synced_file, Some(first_file));
    assert!(matches!(
        log.persist_through(pending),
        Err(StorageError::Poisoned)
    ));
    assert!(matches!(
        log.append_complete_batch(authority(&config), &batch(&config, all.len() as u64)),
        Err(StorageError::Poisoned)
    ));
    drop(log);
    // Fault injection occurs after file fsync; a process restart can find all
    // records. It remains UNKNOWN at the failed call, never a definite abort.
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(recovered.receipt_page(0, 256).unwrap(), all);
    let retry = recovered.persist_many(&[pending]).unwrap();
    assert_eq!(retry[0].appended(), pending);
}

#[test]
fn bulk_receipts_are_not_returned_before_the_shared_barrier() {
    use std::sync::mpsc;
    let temp = tempdir::TempDir::new("bulk-barrier-gate").unwrap();
    let config = config(vec![3], 3);
    let log = Arc::new(
        PartitionLog::create(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    let group = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 0), batch(&config, 1)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let receipts = log.append_group(authority(&config), &group).unwrap();
    let (entered, entered_rx) = mpsc::sync_channel(1);
    let (resume, resume_rx) = mpsc::sync_channel(1);
    *log.before_persist.lock() = Some((entered, resume_rx));
    let (done, result) = mpsc::sync_channel(1);
    let thread_log = log.clone();
    let writer = std::thread::spawn(move || {
        let mut profile = StorageWriteProfile::default();
        let durable = thread_log.persist_many_profiled(&receipts, &mut profile);
        done.send((durable, profile))
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(result.try_recv(), Err(mpsc::TryRecvError::Empty)));
    assert_eq!(log.stats().persistence_barriers, 0);
    let next = log
        .append_complete_batch(authority(&config), &batch(&config, 2))
        .unwrap();
    assert!(matches!(
        log.validate_durable_receipt(DurableBatch(next)),
        Err(StorageError::NeedsPersistence)
    ));
    resume.send(()).unwrap();
    writer.join().unwrap().unwrap();
    let (durable, profile) = result.recv().unwrap();
    assert_eq!(durable.unwrap().len(), 2);
    assert_eq!(profile.persist_calls, 1);
    assert_eq!(profile.persist_errors, 0);
    assert_eq!(profile.persisted_receipts, 2);
    assert_eq!(profile.file_persist_calls, 1);
    assert_eq!(profile.file_persist_successes, 1);
    assert_eq!(profile.namespace_persist_calls, 1);
    assert_eq!(profile.namespace_persist_successes, 1);

    assert_eq!(profile.durable_publications, 1);
    assert_profile_stage_bounds(profile);
    assert_eq!(log.stats().persistence_barriers, 1);
    log.persist_through(next).unwrap();
}

#[test]
fn one_captured_cohort_syncs_every_touched_wal_file() {
    let temp = tempdir::TempDir::new("captured-multifile-cohort").unwrap();
    let config = config(vec![3], 3);
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let mut receipts = Vec::new();
    for number in 0..12 {
        let receipt = log
            .append_complete_batch(authority(&config), &batch(&config, number))
            .unwrap();
        receipts.push(receipt);
    }
    let mut profile = StorageWriteProfile::default();
    assert_eq!(
        log.persist_many_profiled(&receipts, &mut profile)
            .unwrap()
            .len(),
        receipts.len()
    );
    let state = log.state.lock();
    let touched_files = config.layout().locate_file(state.appended_end - 1).as_u64() + 1;
    drop(state);
    assert!(touched_files > 1);
    assert_eq!(profile.file_persist_calls, touched_files);
    assert_eq!(profile.namespace_persist_calls, 1);
    assert_eq!(log.stats().persistence_barriers, 1);
    let state = log.state.lock();
    assert_eq!(state.durable_end, state.appended_end);
}

#[test]
fn scalar_read_checks_membership_and_reads_only_the_selected_record() {
    let temp = tempdir::TempDir::new("native-scalar-record").unwrap();
    let mut config = config(vec![3], 3);
    config.batch_limits.max_encoded_bytes = 2 * 1024 * 1024;
    config.group_limits.max_encoded_bytes = 3 * 1024 * 1024;
    config.fragment_bytes = 4 * 1024 * 1024;
    config.wal_file_bytes = 8 * 1024 * 1024;
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let value = b"tiny".to_vec();
    let batch = CompleteBatch::new(
        &shape(),
        id_for(&config, 0),
        BatchVersion {
            epoch: config.inventory.epoch,
            sequence: 0,
        },
        vec![
            Mutation::Put {
                keyspace: KeySpace(0),
                key: b"key1"[..].into(),
                value: value.clone().into(),
            },
            Mutation::Put {
                keyspace: KeySpace(0),
                key: b"key2"[..].into(),
                value: vec![17; 1024 * 1024].into(),
            },
        ],
        config.batch_limits,
    )
    .unwrap();
    let receipt = log
        .append_complete_batch(authority(&config), &batch)
        .unwrap();
    log.persist_through(receipt).unwrap();
    let reference = log.record_reference(receipt, 0).unwrap();
    assert_eq!(log.read_record(&reference).unwrap(), batch.mutations()[0]); // mmap path
    let check_policies = |log: &PartitionLog| {
        let parent = WalPosition::new(reference.batch_address.offset, reference.batch_address.len);
        let start = reference.descriptor.offset as usize;
        let end = start + reference.descriptor.len as usize;
        let mut encoded = None;
        for policy in [WalMemberReadPolicy::Mapped, WalMemberReadPolicy::Positional] {
            let (kind, bytes) = log
                .wal
                .read_record_payload_range(parent, start..end, policy)
                .unwrap();
            assert!(matches!(
                (policy, kind),
                (
                    WalMemberReadPolicy::Mapped,
                    crate::context::ReadType::Mapped
                ) | (
                    WalMemberReadPolicy::Positional,
                    crate::context::ReadType::Syscall
                )
            ));
            let bytes = bytes.unwrap();
            assert_eq!(bytes.len(), reference.descriptor.len as usize);
            assert_eq!(
                CompleteBatch::decode_record(&shape(), &reference, bytes.clone()).unwrap(),
                batch.mutations()[0]
            );
            if let Some(previous) = &encoded {
                assert_eq!(previous, &bytes);
            }
            encoded = Some(bytes);
            assert_eq!(
                log.wal
                    .read_record_payload_range(parent, end..end, policy)
                    .unwrap()
                    .1
                    .unwrap()
                    .len(),
                0
            );
            assert!(
                log.wal
                    .read_record_payload_range(parent, (end + 1)..end, policy)
                    .is_err()
            );
            assert!(
                log.wal
                    .read_record_payload_range(parent, 0..parent.frame_len(), policy)
                    .is_err()
            );
            assert!(
                log.wal
                    .read_record_payload_range(WalPosition::INVALID, 0..1, policy)
                    .is_err()
            );
            let crossing = WalPosition::new(config.fragment_bytes - 8, 32);
            assert!(
                log.wal
                    .read_record_payload_range(crossing, 0..1, policy)
                    .is_err()
            );
            let missing = WalPosition::new(128 * config.wal_file_bytes, 32);
            assert!(
                log.wal
                    .read_record_payload_range(missing, 0..1, policy)
                    .unwrap()
                    .1
                    .is_none()
            );
        }
    };
    check_policies(&log);
    let mut forged = reference;
    forged.batch_address.segment_generation += 1;
    assert!(log.read_record(&forged).is_err());
    forged = reference;
    forged.descriptor.offset += 1;
    assert!(log.read_record(&forged).is_err());
    drop(log);
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    check_policies(&recovered);
    assert_eq!(
        recovered.read_record(&reference).unwrap(),
        batch.mutations()[0]
    ); // explicit mapped fallback for recovered, nonresident history
    let sibling = recovered.record_reference(receipt, 1).unwrap();
    let file = OpenOptions::new()
        .write(true)
        .open(path.join("wal_0000000000000000"))
        .unwrap();
    // After complete authority validation, damage an unrelated member. Reading
    // the selected scalar must not scan/copy/hash the whole parent batch.
    file.write_all_at(
        &[99],
        receipt.address.offset
            + 8
            + u64::from(sibling.descriptor.offset)
            + u64::from(sibling.descriptor.value_offset),
    )
    .unwrap();
    assert_eq!(
        recovered.read_record(&reference).unwrap(),
        batch.mutations()[0]
    );
    assert!(recovered.read_record(&sibling).is_err());
    for policy in [WalMemberReadPolicy::Mapped, WalMemberReadPolicy::Positional] {
        let bytes = recovered
            .read_record_encoded_with_policy(&reference, policy)
            .unwrap();
        assert_eq!(
            CompleteBatch::decode_record(&shape(), &reference, bytes).unwrap(),
            batch.mutations()[0]
        );
        let bytes = recovered
            .read_record_encoded_with_policy(&sibling, policy)
            .unwrap();
        assert!(CompleteBatch::decode_record(&shape(), &sibling, bytes).is_err());
    }
    assert!(recovered.read_batch(receipt).is_err());
    file.write_all_at(
        &[99],
        receipt.address.offset
            + 8
            + u64::from(reference.descriptor.offset)
            + u64::from(reference.descriptor.value_offset),
    )
    .unwrap();
    assert!(recovered.read_record(&reference).is_err());
}

#[test]
fn persistence_io_does_not_block_value_reads_or_already_durable_retries() {
    use std::sync::mpsc;
    let temp = tempdir::TempDir::new("native-read-during-persist").unwrap();
    let config = config(vec![3], 3);
    let log = Arc::new(
        PartitionLog::create(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    let old = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(old).unwrap();
    let reference = log.record_reference(old, 0).unwrap();
    let current = log
        .append_complete_batch(authority(&config), &batch(&config, 1))
        .unwrap();
    let (entered, entered_rx) = mpsc::sync_channel(1);
    let (resume, resume_rx) = mpsc::sync_channel(1);
    *log.before_persist.lock() = Some((entered, resume_rx));
    let writer_log = log.clone();
    let writer = std::thread::spawn(move || writer_log.persist_through(current));
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let (read_done, read_rx) = mpsc::sync_channel(1);
    let reader_log = log.clone();
    let read_config = config.clone();
    let reader = std::thread::spawn(move || {
        let read = reader_log.read_record(&reference);
        let retry = reader_log.persist_through(old);
        let next =
            reader_log.append_complete_batch(authority(&read_config), &batch(&read_config, 2));
        let barriers = reader_log.stats().persistence_barriers;
        read_done.send((read, retry, next, barriers)).unwrap();
    });
    let outcome = read_rx.recv_timeout(Duration::from_secs(2));
    // Always release/join before asserting so a regression cannot deadlock the
    // test runner's shutdown. The barrier is demonstrably still pending here.
    resume.send(()).unwrap();
    writer.join().unwrap().unwrap();
    reader.join().unwrap();
    let (value, retry, next, barriers) =
        outcome.expect("disk persistence held the reference lookup mutex");
    assert_eq!(barriers, 1);
    assert!(next.is_ok());
    assert_eq!(value.unwrap(), batch(&config, 0).mutations()[0]);
    assert_eq!(retry.unwrap().appended(), old);
    assert_eq!(log.stats().persistence_barriers, 2);
}

#[test]
fn registered_value_reads_do_not_acquire_the_partition_metadata_mutex() {
    use std::sync::mpsc;
    let temp = tempdir::TempDir::new("native-mutex-free-reference").unwrap();
    let config = config(vec![3], 3);
    let log = Arc::new(
        PartitionLog::create(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    let batch = batch(&config, 0);
    let receipt = log
        .append_complete_batch(authority(&config), &batch)
        .unwrap();
    log.persist_through(receipt).unwrap();
    let guard = log.state.lock();
    let (sent, received) = mpsc::sync_channel(1);
    let reader_log = log.clone();
    let reader = std::thread::spawn(move || {
        let reference = reader_log.record_reference(receipt, 0).unwrap();
        let value = reader_log.read_record(&reference);
        let whole = reader_log.read_batch(receipt);
        sent.send((value, whole)).unwrap();
    });
    let outcome = received.recv_timeout(Duration::from_secs(2));
    drop(guard); // Always unblock a regressed implementation before joining.
    reader.join().unwrap();
    let (value, whole) = outcome.expect("registered read waited for writer metadata ownership");
    assert_eq!(value.unwrap(), batch.mutations()[0]);
    assert_eq!(whole.unwrap().mutations(), batch.mutations());
}

// Owned read plans (`plan_record_read`) exist only with the live route.
#[test]
fn reference_metadata_budget_rejects_before_any_authority_bytes() {
    let temp = tempdir::TempDir::new("native-reference-budget").unwrap();
    let mut config = config(vec![3], 3);
    config.max_reference_metadata_bytes = 64 * 1024;
    config.max_batches = 4096;
    let log = Arc::new(
        PartitionLog::create(
            &temp.path().join("bounded"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    let first = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(first).unwrap();
    // The cache table adapts to the budget. Exercise actual exhaustion rather
    // than assuming that a large-budget probe has the same fixed table charge.
    // Owned read plans retain real descriptor credits across cache eviction.
    // 512 batches remain below the configured batch-count limit.
    let mut retained = Vec::new();
    for number in 1..=256 {
        let later = batch(&config, number * 2 - 1);
        let last = batch(&config, number * 2);
        let group = CompleteGroup::new(
            &shape(),
            vec![later.clone(), last.clone()],
            config.batch_limits,
            config.group_limits,
        )
        .unwrap();
        let before = log.stats();
        assert!(before.reference_metadata_bytes <= config.max_reference_metadata_bytes);
        match log.append_group(authority(&config), &group) {
            Ok(receipts) => {
                log.persist_through(receipts[0]).unwrap();
                for receipt in receipts {
                    let reference = log.record_reference(receipt, 0).unwrap();
                    retained.push(log.plan_record_read(&reference).unwrap());
                }
            }
            Err(StorageError::Capacity) => {
                assert_eq!(log.lookup(later.id()).unwrap(), None);
                assert_eq!(log.lookup(last.id()).unwrap(), None);
                assert_eq!(log.stats().appended_groups, before.appended_groups);
                assert!(
                    log.stats().reference_metadata_bytes <= config.max_reference_metadata_bytes
                );
                assert_eq!(
                    log.append_complete_batch(authority(&config), &batch(&config, 0))
                        .unwrap(),
                    first
                );
                return;
            }
            Err(error) => panic!("unexpected append error: {error:?}"),
        }
    }
    panic!("bounded authority metadata did not reject a group");
}
fn authority(config: &PartitionConfig) -> RequestAuthority {
    RequestAuthority {
        epoch: config.inventory.epoch,
    }
}

#[test]
fn native_wal_values_survive_multiple_files_and_sealed_reopen() {
    let temp = tempdir::TempDir::new("native-partition").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let receipts: Vec<_> = (0..12)
        .map(|n| {
            let receipt = log
                .append_complete_batch(authority(&config), &batch(&config, n))
                .unwrap();
            log.persist_through(receipt).unwrap();
            receipt
        })
        .collect();
    assert!(receipts.last().unwrap().address.offset > config.wal_file_bytes);
    // Persist through an older file even if mapper has opened later files.
    log.persist_through(receipts[0]).unwrap();
    for (n, receipt) in receipts.iter().enumerate() {
        assert_eq!(
            log.read_batch(*receipt).unwrap().mutations(),
            batch(&config, n as u64).mutations()
        );
    }
    log.seal_epoch(authority(&config)).unwrap();
    assert!(matches!(
        log.append_complete_batch(authority(&config), &batch(&config, 20)),
        Err(StorageError::Sealed)
    ));
    drop(log);
    let reopened = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(reopened.receipt_page(0, 256).unwrap(), receipts);
    for receipt in receipts {
        assert_eq!(
            reopened.persist_through(receipt).unwrap().appended(),
            receipt
        );
    }
    assert!(matches!(
        reopened.append_complete_batch(authority(&config), &batch(&config, 20)),
        Err(StorageError::Sealed)
    ));
}

#[test]
fn retries_are_idempotent_and_conflicts_reject_before_authority() {
    let temp = tempdir::TempDir::new("native-retry").unwrap();
    let mut config = config(vec![3], 3);
    config.max_batches = 1;
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let first = batch(&config, 0);
    let receipt = log
        .append_complete_batch(authority(&config), &first)
        .unwrap();
    assert_eq!(
        log.append_complete_batch(authority(&config), &first)
            .unwrap(),
        receipt
    );
    let conflicting = CompleteBatch::new(
        &shape(),
        first.id(),
        first.version(),
        vec![Mutation::Delete {
            keyspace: KeySpace(0),
            key: b"key1"[..].into(),
        }],
        config.batch_limits,
    )
    .unwrap();
    assert!(matches!(
        log.append_complete_batch(authority(&config), &conflicting),
        Err(StorageError::IdentityConflict)
    ));
    let duplicate_version = CompleteBatch::new(
        &shape(),
        id_for(&config, 4),
        first.version(),
        first.mutations().to_vec(),
        config.batch_limits,
    )
    .unwrap();
    assert!(matches!(
        log.append_complete_batch(authority(&config), &duplicate_version),
        Err(StorageError::VersionConflict)
    ));
    assert!(matches!(
        log.append_complete_batch(RequestAuthority { epoch: 4 }, &first),
        Err(StorageError::Fenced)
    ));
    assert_eq!(log.receipt_page(0, 256).unwrap(), vec![receipt]);
    // WAL-replay history: the next batch is admitted without a persistence
    // barrier (the OnDisk `max_batches` → `NeedsPersistence` bound is gone).
    let second = log
        .append_complete_batch(authority(&config), &batch(&config, 1))
        .unwrap();
    assert_eq!(log.receipt_page(0, 256).unwrap(), vec![receipt, second]);
}

#[test]
fn complete_unacknowledged_batch_recovers_but_torn_batch_is_never_partial() {
    let temp = tempdir::TempDir::new("native-tail").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let first = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(first).unwrap();
    let unacknowledged = log
        .append_complete_batch(authority(&config), &batch(&config, 1))
        .unwrap();
    // Persist but discard the reply: caller cannot infer the result from its
    // missing acknowledgement, while storage can admit the next batch.
    let _lost_reply = log.persist_through(unacknowledged).unwrap();
    let torn = log
        .append_complete_batch(authority(&config), &batch(&config, 2))
        .unwrap();
    drop(log);
    // Simulate a torn suffix after writer exit. This is fault injection, not a
    // hardware power-loss experiment. Prior durable prefix is unchanged.
    let filename = format!("wal_{:016x}", torn.address.offset / config.wal_file_bytes);
    let file = OpenOptions::new()
        .write(true)
        .open(path.join(filename))
        .unwrap();
    file.write_all_at(
        &[0xff],
        torn.address.offset % config.wal_file_bytes + u64::from(torn.address.len) - 1,
    )
    .unwrap();
    file.sync_all().unwrap();
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(recovered.lookup(first.id()).unwrap(), Some(first));
    assert_eq!(
        recovered.lookup(unacknowledged.id()).unwrap(),
        Some(unacknowledged)
    );
    assert_eq!(recovered.lookup(torn.id()).unwrap(), None);
    assert_eq!(
        recovered
            .read_batch(unacknowledged)
            .unwrap()
            .mutations()
            .len(),
        2
    );
    assert!(matches!(
        recovered.append_complete_batch(authority(&config), &batch(&config, 2)),
        Err(StorageError::Sealed)
    ));
}

#[test]
fn foreign_schema_identity_and_address_do_not_cross_partition_boundaries() {
    let temp = tempdir::TempDir::new("native-validation").unwrap();
    let c0 = config(vec![0, 1], 0);
    let c1 = config(vec![0, 1], 1);
    let a = PartitionLog::create(&temp.path().join("a"), c0.clone(), &shape(), metrics()).unwrap();
    let b = PartitionLog::create(&temp.path().join("b"), c1.clone(), &shape(), metrics()).unwrap();
    let receipt = a
        .append_complete_batch(authority(&c0), &batch(&c0, 0))
        .unwrap();
    assert!(b.read_batch(receipt).is_err());
    assert!(b.persist_through(receipt).is_err());
    assert!(matches!(
        b.append_complete_batch(authority(&c1), &batch(&c0, 0)),
        Err(StorageError::WrongPartition)
    ));
    let wrong_shape = KeyShape::new_single(2, 1, KeyType::uniform(1));
    let wrong_batch = CompleteBatch::new(
        &wrong_shape,
        id_for(&c0, 1),
        BatchVersion {
            epoch: 5,
            sequence: 1,
        },
        vec![Mutation::Delete {
            keyspace: KeySpace(0),
            key: b"xx"[..].into(),
        }],
        c0.batch_limits,
    )
    .unwrap();
    assert!(matches!(
        a.append_complete_batch(authority(&c0), &wrong_batch),
        Err(StorageError::Group(GroupError::Batch(_)))
    ));
    assert_eq!(a.receipt_count(), 1);
}

/// WAL-replay history only (the on-disk authenticated receipt root and its
/// corruption case were removed with OnDisk history): a competing open and a
/// recovery of an unsealed stream fail, and a sealed stream with a missing
/// WAL file fails closed.
#[test]
fn sealed_history_missing_files_and_competing_open_fail_closed() {
    let temp = tempdir::TempDir::new("native-fail-closed").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    assert!(PartitionLog::create(&path, config.clone(), &shape(), metrics()).is_err());
    assert!(PartitionLog::recover_sealed(&path, &config, metrics()).is_err());
    log.append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.seal_epoch(authority(&config)).unwrap();
    drop(log);
    fs::remove_file(path.join("wal_0000000000000000")).unwrap();
    assert!(PartitionLog::recover_sealed(&path, &config, metrics()).is_err());
}

#[test]
fn concurrent_duplicate_requests_share_one_authority_frame() {
    let temp = tempdir::TempDir::new("native-concurrent-retry").unwrap();
    let config = config(vec![3], 3);
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let batch = batch(&config, 0);
    let receipts = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    log.append_complete_batch(authority(&config), &batch)
                        .unwrap()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(receipts.iter().all(|r| *r == receipts[0]));
    assert_eq!(log.receipt_count(), 1);
}

fn assert_profile_stage_bounds(profile: StorageWriteProfile) {
    assert_eq!(
        profile.append_validation_ns,
        profile
            .append_basic_validation_ns
            .saturating_add(profile.append_conflict_lookup_ns)
            .saturating_add(profile.append_geometry_capacity_ns)
            .saturating_add(profile.append_metadata_reservation_ns)
            .saturating_add(profile.append_descriptor_preparation_ns)
    );
    assert!(
        profile.append_state_lock_wait_ns
            + profile.append_validation_ns
            + profile.append_wal_ns
            + profile.append_publication_ns
            <= profile.append_total_ns
    );
    assert!(
        profile.persist_serialization_lock_wait_ns
            + profile.persist_state_lock_wait_ns
            + profile.persist_validation_ns
            + profile.persist_wal_file_ns
            + profile.persist_namespace_ns
            + profile.persist_inventory_file_ns
            + profile.persist_inventory_namespace_ns
            + profile.persist_publication_ns
            <= profile.persist_total_ns
    );
}

#[test]
fn append_validation_timing_closes_on_unwind_and_aggregates() {
    let mut profile = StorageWriteProfile::default();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut timing = WriteTiming::new(Some(&mut profile), true);
        // Make the artificial stage intervals subsets of the artificial total.
        timing.started = Some(Instant::now() - Duration::from_secs(1));
        timing.stage(Some(WriteStage::AppendBasicValidation));
        timing.stage_started = Some(Instant::now() - Duration::from_millis(1));
        timing.stage(Some(WriteStage::AppendConflictLookup));
        timing.stage_started = Some(Instant::now() - Duration::from_millis(1));
        timing.stage(Some(WriteStage::AppendBasicValidation));
        timing.stage_started = Some(Instant::now() - Duration::from_millis(1));
        timing.stage(Some(WriteStage::AppendGeometryCapacity));
        timing.stage_started = Some(Instant::now() - Duration::from_millis(1));
        timing.stage(Some(WriteStage::AppendMetadataReservation));
        timing.stage_started = Some(Instant::now() - Duration::from_millis(1));
        timing.stage(Some(WriteStage::AppendDescriptorPreparation));
        timing.stage_started = Some(Instant::now() - Duration::from_millis(1));
        panic!("test unwind");
    }));
    assert!(result.is_err());
    assert_eq!(profile.append_calls, 1);
    assert_eq!(profile.append_errors, 1);
    assert_eq!(profile.append_wal_ns, 0);
    assert!(profile.append_basic_validation_ns >= 2_000_000);
    assert!(profile.append_conflict_lookup_ns >= 1_000_000);
    assert!(profile.append_geometry_capacity_ns >= 1_000_000);
    assert!(profile.append_metadata_reservation_ns >= 1_000_000);
    assert!(profile.append_descriptor_preparation_ns >= 1_000_000);
    assert_profile_stage_bounds(profile);

    let mut aggregate = StorageWriteProfile::default();
    aggregate.add(profile);
    aggregate.add(profile);
    assert_eq!(aggregate.append_calls, 2);
    assert_eq!(
        aggregate.append_descriptor_preparation_ns,
        profile.append_descriptor_preparation_ns.saturating_mul(2)
    );
    assert_profile_stage_bounds(aggregate);

    let mut disabled = WriteTiming::new(None, true);
    disabled.stage(Some(WriteStage::AppendBasicValidation));
    assert!(disabled.started.is_none());
    assert!(disabled.stage_started.is_none());
}

#[test]
fn profiled_writes_preserve_retries_admission_and_durability_frontiers() {
    let temp = tempdir::TempDir::new("profiled-storage-write").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let group = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 0), batch(&config, 1)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let mut append = StorageWriteProfile::default();
    let receipts = log
        .append_group_profiled(authority(&config), &group, &mut append)
        .unwrap();
    assert_eq!(append.append_calls, 1);
    assert_eq!(append.append_errors, 0);
    assert_eq!(append.appended_groups, 1);
    assert_eq!(append.appended_receipts, 2);
    assert_eq!(append.append_retries, 0);
    assert_eq!(append.persist_total_ns, 0);
    assert!(append.append_basic_validation_ns > 0);
    assert!(append.append_conflict_lookup_ns > 0);
    assert!(append.append_geometry_capacity_ns > 0);
    assert!(append.append_metadata_reservation_ns > 0);
    assert!(append.append_descriptor_preparation_ns > 0);
    assert_profile_stage_bounds(append);
    assert_eq!(log.stats().persistence_barriers, 0);
    assert_eq!(log.state.lock().durable_end, 0);

    let mut retry = StorageWriteProfile::default();
    assert_eq!(
        log.append_group_profiled(authority(&config), &group, &mut retry)
            .unwrap(),
        receipts
    );
    assert_eq!(retry.append_calls, 1);
    assert_eq!(retry.append_retries, 1);
    assert_eq!(retry.appended_groups, 0);
    assert_eq!(retry.appended_receipts, 2);
    assert!(retry.append_basic_validation_ns > 0);
    assert!(retry.append_conflict_lookup_ns > 0);
    assert_eq!(retry.append_geometry_capacity_ns, 0);
    assert_eq!(retry.append_metadata_reservation_ns, 0);
    assert_eq!(retry.append_descriptor_preparation_ns, 0);
    assert_eq!(retry.append_wal_ns, 0);
    assert_eq!(retry.append_publication_ns, 0);
    assert_profile_stage_bounds(retry);

    let mut fenced_authority = authority(&config);
    fenced_authority.epoch += 1;
    let mut fenced = StorageWriteProfile::default();
    assert!(matches!(
        log.append_group_profiled(fenced_authority, &group, &mut fenced),
        Err(StorageError::Fenced)
    ));
    assert_eq!(fenced.append_calls, 1);
    assert_eq!(fenced.append_errors, 1);
    assert!(fenced.append_basic_validation_ns > 0);
    assert_eq!(fenced.append_state_lock_wait_ns, 0);
    assert_eq!(fenced.append_conflict_lookup_ns, 0);
    assert_eq!(fenced.append_geometry_capacity_ns, 0);
    assert_eq!(fenced.append_metadata_reservation_ns, 0);
    assert_eq!(fenced.append_descriptor_preparation_ns, 0);
    assert_profile_stage_bounds(fenced);

    let mixed = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 0), batch(&config, 2)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let mut rejected = StorageWriteProfile::default();
    assert!(matches!(
        log.append_group_profiled(authority(&config), &mixed, &mut rejected),
        Err(StorageError::MixedRetry)
    ));
    assert_eq!(rejected.append_errors, 1);
    assert_eq!(rejected.append_retries, 0);
    assert!(rejected.append_basic_validation_ns > 0);
    assert!(rejected.append_conflict_lookup_ns > 0);
    assert_eq!(rejected.append_geometry_capacity_ns, 0);
    assert_eq!(rejected.append_metadata_reservation_ns, 0);
    assert_eq!(rejected.append_descriptor_preparation_ns, 0);
    assert_profile_stage_bounds(rejected);

    let next = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 2)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let mut next_append = StorageWriteProfile::default();
    let appended_next = log
        .append_group_profiled(authority(&config), &next, &mut next_append)
        .unwrap();
    assert_eq!(appended_next.len(), 1);
    assert_eq!(next_append.append_calls, 1);
    assert_eq!(next_append.append_errors, 0);
    assert_eq!(next_append.appended_groups, 1);
    assert_eq!(next_append.appended_receipts, 1);
    assert!(next_append.append_wal_ns > 0);
    assert!(next_append.append_publication_ns > 0);
    assert_profile_stage_bounds(next_append);

    let mut foreign = receipts[0];
    foreign.address.segment_generation += 1;
    let mut invalid = StorageWriteProfile::default();
    assert!(
        log.persist_many_profiled(&[receipts[0], foreign], &mut invalid)
            .is_err()
    );
    assert_eq!(invalid.persist_calls, 1);
    assert_eq!(invalid.persist_errors, 1);
    assert_eq!(invalid.persisted_receipts, 0);
    assert_eq!(invalid.persist_serialization_lock_wait_ns, 0);
    assert_eq!(invalid.persist_wal_file_ns, 0);
    assert_eq!(invalid.persist_namespace_ns, 0);
    assert_eq!(invalid.file_persist_calls, 0);
    assert_eq!(invalid.namespace_persist_calls, 0);
    assert_eq!(invalid.durable_publications, 0);
    assert!(invalid.persistence_capture.is_none());
    assert_profile_stage_bounds(invalid);

    let mut persist = StorageWriteProfile::default();
    let reordered = [receipts[1], receipts[0], receipts[1]];
    assert_eq!(
        log.persist_many_profiled(&reordered, &mut persist)
            .unwrap()
            .iter()
            .map(|durable| durable.appended())
            .collect::<Vec<_>>(),
        reordered
    );
    assert_eq!(persist.persist_calls, 1);
    assert_eq!(persist.persist_errors, 0);
    assert_eq!(persist.persisted_receipts, 3);
    assert_eq!(persist.file_persist_calls, 1);
    assert_eq!(persist.file_persist_successes, 1);
    assert_eq!(persist.namespace_persist_calls, 1);
    assert_eq!(persist.namespace_persist_successes, 1);
    assert_eq!(persist.durable_publications, 1);
    let capture = persist.persistence_capture.unwrap();
    assert!(capture.captured_appended_end > capture.durable_end);
    assert_eq!(capture.cumulative_appended_groups, 2);
    assert_profile_stage_bounds(persist);
    log.persist_many_profiled(&[], &mut persist).unwrap();
    assert!(persist.persistence_capture.is_none());
    for receipt in &receipts {
        assert!(
            pending_persistence(&log.state.lock(), *receipt)
                .unwrap()
                .is_none()
        );
    }

    let mut noops = StorageWriteProfile::default();
    log.persist_many_profiled(&reordered, &mut noops).unwrap();
    log.persist_many_profiled(&[], &mut noops).unwrap();
    assert_eq!(noops.persist_calls, 2);
    assert_eq!(noops.persist_noops, 2);
    assert!(noops.persistence_capture.is_none());
    assert_eq!(noops.persist_errors, 0);
    assert_eq!(noops.persisted_receipts, 3);
    assert_eq!(noops.persist_serialization_lock_wait_ns, 0);
    assert_eq!(noops.persist_wal_file_ns, 0);
    assert_eq!(noops.persist_namespace_ns, 0);
    assert_eq!(noops.persist_publication_ns, 0);
    assert_eq!(noops.file_persist_calls, 0);
    assert_eq!(noops.durable_publications, 0);
    assert_profile_stage_bounds(noops);
    let later_batch = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 3)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let mut later_append = StorageWriteProfile::default();
    let later = log
        .append_group_profiled(authority(&config), &later_batch, &mut later_append)
        .unwrap();
    assert_eq!(later_append.append_calls, 1);
    assert_eq!(later_append.append_errors, 0);
    assert_eq!(later_append.appended_groups, 1);
    assert_eq!(later_append.appended_receipts, 1);
    assert!(later_append.append_wal_ns > 0);
    assert!(later_append.append_publication_ns > 0);
    assert_profile_stage_bounds(later_append);
    assert!(
        pending_persistence(&log.state.lock(), later[0])
            .unwrap()
            .is_some()
    );
    log.persist_many_profiled(&later, &mut persist).unwrap();
    assert_eq!(persist.persisted_receipts, 4);
    assert_eq!(persist.file_persist_calls, 2);
    assert_eq!(persist.file_persist_successes, 2);
    assert_eq!(persist.durable_publications, 2);
    assert_eq!(
        persist.namespace_persist_successes,
        log.stats().namespace_syncs
    );
    assert_eq!(
        persist.file_persist_successes,
        log.stats().persistence_barriers
    );
    assert_profile_stage_bounds(persist);

    drop(log);
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(
        recovered.receipt_page(0, 256).unwrap(),
        [receipts, appended_next, later].concat()
    );
}

#[test]
fn profiled_pre_io_failure_never_counts_a_barrier_or_durable_receipt() {
    let temp = tempdir::TempDir::new("profiled-persist-failure").unwrap();
    let config = config(vec![3], 3);
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let receipt = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.fail_next_persist();
    let mut profile = StorageWriteProfile::default();
    assert!(matches!(
        log.persist_many_profiled(&[receipt], &mut profile),
        Err(StorageError::Unknown(_))
    ));
    assert_eq!(profile.persist_calls, 1);
    assert_eq!(profile.persist_errors, 1);
    assert_eq!(profile.persist_noops, 0);
    assert_eq!(profile.persisted_receipts, 0);
    assert_eq!(profile.file_persist_calls, 0);
    assert_eq!(profile.file_persist_successes, 0);
    assert_eq!(profile.namespace_persist_calls, 0);
    assert_eq!(profile.namespace_persist_successes, 0);
    assert_eq!(profile.durable_publications, 0);
    assert_eq!(profile.persist_wal_file_ns, 0);
    assert_eq!(profile.persist_namespace_ns, 0);
    assert_profile_stage_bounds(profile);
    assert_eq!(log.state.lock().durable_end, 0);
    assert_eq!(log.stats().persistence_barriers, 0);
    assert!(matches!(
        log.persist_many_profiled(&[receipt], &mut profile),
        Err(StorageError::Poisoned)
    ));
    assert_eq!(profile.persist_calls, 2);
    assert_eq!(profile.persist_errors, 2);
    assert_eq!(profile.persisted_receipts, 0);
    assert_eq!(profile.file_persist_calls, 0);
    assert_profile_stage_bounds(profile);
}

#[test]
fn failed_seal_cannot_be_acknowledged_by_a_retry() {
    let temp = tempdir::TempDir::new("failed-seal-retry").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let appended = log
        .append_complete_batch(authority(&config), &batch(&config, 1))
        .unwrap();
    log.persist_through(appended).unwrap();
    fs::create_dir(path.join(SEAL)).unwrap();
    assert!(log.seal_epoch(authority(&config)).is_err());
    assert!(matches!(
        log.seal_epoch(authority(&config)),
        Err(StorageError::Poisoned)
    ));
    fs::remove_dir(path.join(SEAL)).unwrap();
    drop(log);
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(
        recovered.read_batch(appended).unwrap().id(),
        batch(&config, 1).id()
    );
}

#[test]
fn missing_live_registration_cannot_downgrade_frontier_recovery() {
    let temp = tempdir::TempDir::new("missing-live-registration").unwrap();
    let config = live_config();
    let path = temp.path().join("stream");
    let log = PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
    let appended = append_live_fixture(&log, &config, 1);
    log.persist_through(appended).unwrap();
    drop(log);
    fs::remove_file(path.join(LIVE_REGISTRATION)).unwrap();
    assert!(matches!(
        PartitionLog::is_live_path(&path),
        Err(StorageError::Invalid(_))
    ));
    assert!(matches!(
        PartitionLog::recover_sealed(&path, &config, metrics()),
        Err(StorageError::Invalid(_))
    ));
}

mod conflict_index;
mod file_retirement;
mod live_extent;
mod live_native;
mod relocation_scan;
mod wal_replay;
