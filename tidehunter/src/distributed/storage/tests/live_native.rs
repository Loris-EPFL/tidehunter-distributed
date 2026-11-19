//! Live-stream recovery of frames beyond the persisted frontier root. Native-tier
//! acknowledgements precede the sync, so an owner process crash must keep every
//! acknowledged frame; a kernel crash may lose only a clean unsynced suffix.
use super::*;
use crate::distributed::nvmeof::live_storage::LiveGroupDescriptor;
use std::path::PathBuf;

fn frame_file(path: &Path, config: &PartitionConfig, frame: u64) -> PathBuf {
    let file = crate::wal::position::WalFileId::from_u64(frame * 4096 / config.wal_file_bytes);
    config.layout().wal_file_name(path, file)
}

/// Overwrite part of frame `frame`, modelling pages the kernel never wrote back.
fn damage(path: &Path, config: &PartitionConfig, frame: u64, from: u64, len: u64) {
    let file = OpenOptions::new()
        .write(true)
        .open(frame_file(path, config, frame))
        .unwrap();
    file.write_all_at(
        &vec![0; len as usize],
        frame * 4096 % config.wal_file_bytes + from,
    )
    .unwrap();
}

/// A live stream with `synced` frames covered by the frontier root followed by
/// `unsynced` appended frames, then an owner process exit without any sync.
fn crashed(
    synced: u64,
    unsynced: u64,
) -> (
    tempdir::TempDir,
    PathBuf,
    PartitionConfig,
    Vec<AppendedBatch>,
) {
    let temp = tempdir::TempDir::new("live-native-crash").unwrap();
    let config = live_config();
    let path = temp.path().join("stream");
    let log = PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
    let mut receipts = Vec::new();
    for n in 0..synced {
        receipts.push(append_live_fixture(&log, &config, n));
    }
    if synced > 0 {
        log.persist_many(&receipts).unwrap();
    }
    for n in synced..synced + unsynced {
        receipts.push(append_live_fixture(&log, &config, n));
    }
    assert_eq!(log.synced_frontier(), synced * 4096);
    assert_eq!(log.appended_frontier(), (synced + unsynced) * 4096);
    drop(log);
    (temp, path, config, receipts)
}

fn recover(path: &Path, config: &PartitionConfig) -> Result<PartitionLog, StorageError> {
    PartitionLog::recover_live_sealed(path, config, metrics(), || Ok(()))
}

#[test]
fn process_crash_recovers_every_acknowledged_frame_above_the_root() {
    let (_temp, path, config, receipts) = crashed(2, 3);
    let recovered = recover(&path, &config).unwrap();
    assert_eq!(recovered.receipt_count(), 5);
    for receipt in receipts {
        assert_eq!(recovered.read_batch(receipt).unwrap().id(), receipt.id());
    }
    // Recovery makes the adopted suffix durable before sealing.
    assert_eq!(recovered.synced_frontier(), 5 * 4096);
}

#[test]
fn process_crash_with_no_root_progress_recovers_all_frames() {
    let (_temp, path, config, receipts) = crashed(0, 3);
    let recovered = recover(&path, &config).unwrap();
    assert_eq!(recovered.receipt_count(), receipts.len());
}

#[test]
fn adopted_suffix_is_covered_by_the_frontier_root_and_recovers_again() {
    let (_temp, path, config, receipts) = crashed(1, 3);
    drop(recover(&path, &config).unwrap());
    // The root now covers every adopted frame, so history never outruns it.
    let root: LiveFrontier = read_metadata(&path.join(LIVE_FRONTIER)).unwrap();
    assert_eq!(root.end, receipts.len() as u64 * 4096);
    let again = recover(&path, &config).unwrap();
    assert_eq!(again.receipt_count(), receipts.len());
    for receipt in receipts {
        assert_eq!(again.read_batch(receipt).unwrap().id(), receipt.id());
    }
}

#[test]
fn kernel_crash_drops_only_the_unsynced_suffix_without_error() {
    // The last unsynced frame never reached the disk.
    let (_temp, path, config, _) = crashed(2, 3);
    damage(&path, &config, 4, 0, 4096);
    assert_eq!(recover(&path, &config).unwrap().receipt_count(), 4);
    // A torn frame (second half missing) fails its CRC and ends the replay.
    let (_temp, path, config, _) = crashed(2, 3);
    damage(&path, &config, 3, 24, 64);
    assert_eq!(recover(&path, &config).unwrap().receipt_count(), 3);
}

#[test]
fn replay_stops_at_the_first_invalid_frame_even_if_a_later_one_is_valid() {
    // Page writeback is unordered: frame 3 is lost, frame 4 survived.
    let (_temp, path, config, _) = crashed(2, 3);
    damage(&path, &config, 3, 0, 4096);
    assert_eq!(recover(&path, &config).unwrap().receipt_count(), 3);
}

#[test]
fn a_defect_below_the_synced_root_stays_fatal() {
    let (_temp, path, config, _) = crashed(3, 1);
    damage(&path, &config, 1, 100, 200);
    assert!(matches!(
        recover(&path, &config),
        Err(StorageError::Invalid(_))
    ));
}

#[test]
fn completed_frames_publish_into_the_contiguous_appended_frontier() {
    use crate::distributed::durability::DurabilityConfig;
    let temp = tempdir::TempDir::new("live-native-frontier").unwrap();
    let config = live_config();
    let log = PartitionLog::create_live(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let frontier = log.attach_frontier(&DurabilityConfig::default()).unwrap();
    for n in 0..3 {
        append_live_fixture(&log, &config, n);
        assert_eq!(frontier.appended(), (n + 1) * 4096);
    }
    // Appended is ahead of synced until a barrier runs.
    assert_eq!(frontier.synced(), 0);
    log.persist_appended().unwrap();
    assert_eq!(log.synced_frontier(), 3 * 4096);
}

/// Lease and fully write a frame for `item` without adopting it: the owner
/// never learns it finished, as when a coordinator or owner dies in between.
fn write_unfinished(
    log: &PartitionLog,
    config: &PartitionConfig,
    item: CompleteBatch,
) -> AppendedBatch {
    let group = CompleteGroup::new_live(
        &shape(),
        vec![item],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
    let span = (frame.len() as u64).div_ceil(4096) * 4096;
    let (extent, offset) = live_fixture_extent(log, config, span);
    write_live_fixture(&extent, offset, &frame);
    let item = &group.batches()[0];
    receipt_for(
        config,
        item,
        member_position(
            WalPosition::new(offset, frame.len() as u32),
            group.batch_offsets()[0],
            item,
        ),
    )
}

#[test]
fn an_abandoned_reservation_with_a_complete_frame_is_adopted_but_never_acknowledged() {
    // Rule: replay judges bytes, not intent. A complete valid frame beyond the
    // root is adopted even if its completion RPC never arrived. It was never
    // acknowledged, and a coordinator retry of the same batch is idempotent
    // (same identity, version and digest return the adopted receipt).
    let temp = tempdir::TempDir::new("live-native-abandoned").unwrap();
    let config = live_config();
    let path = temp.path().join("stream");
    let log = PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
    let first = append_live_fixture(&log, &config, 0);
    log.persist_many(&[first]).unwrap();
    let abandoned = write_unfinished(&log, &config, batch(&config, 1));
    // The owner has not published it: no acknowledgement exists.
    assert_eq!(log.lookup(abandoned.id()).unwrap(), None);
    drop(log);
    let recovered = recover(&path, &config).unwrap();
    assert_eq!(recovered.lookup(first.id()).unwrap(), Some(first));
    assert_eq!(recovered.lookup(abandoned.id()).unwrap(), Some(abandoned));
    assert_eq!(recovered.receipt_count(), 2);
}

#[test]
fn an_abandoned_reservation_with_a_torn_frame_ends_replay_before_it() {
    let temp = tempdir::TempDir::new("live-native-torn").unwrap();
    let config = live_config();
    let path = temp.path().join("stream");
    let log = PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
    let first = append_live_fixture(&log, &config, 0);
    let abandoned = write_unfinished(&log, &config, batch(&config, 1));
    drop(log);
    damage(&path, &config, 1, 24, 64);
    let recovered = recover(&path, &config).unwrap();
    assert_eq!(recovered.lookup(first.id()).unwrap(), Some(first));
    assert_eq!(recovered.lookup(abandoned.id()).unwrap(), None);
}

#[test]
fn replay_continues_across_a_fragment_skip_marker_beyond_the_root() {
    let temp = tempdir::TempDir::new("live-native-fragment").unwrap();
    let mut config = live_config();
    config.batch_limits.max_encoded_bytes = 6000;
    config.group_limits.max_encoded_bytes = 8184;
    config.fragment_bytes = 8192;
    config.wal_file_bytes = 16384;
    let path = temp.path().join("stream");
    let log = PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
    let large = CompleteBatch::new(
        &shape(),
        id_for(&config, 1),
        BatchVersion {
            epoch: config.inventory.epoch,
            sequence: 1,
        },
        vec![Mutation::Put {
            keyspace: KeySpace(0),
            key: b"key3"[..].into(),
            value: vec![9; 5000].into(),
        }],
        config.batch_limits,
    )
    .unwrap();
    // The small frame is synced; the large one starts the next fragment after a
    // skip marker and is acknowledged but unsynced when the owner process dies.
    let first = append_live_fixture(&log, &config, 0);
    log.persist_many(&[first]).unwrap();
    let second = write_unfinished(&log, &config, large);
    drop(log);
    let recovered = recover(&path, &config).unwrap();
    assert_eq!(recovered.lookup(first.id()).unwrap(), Some(first));
    assert_eq!(recovered.lookup(second.id()).unwrap(), Some(second));
    assert_eq!(second.address().offset / config.fragment_bytes, 1);
}

#[test]
fn conflict_lookups_stay_exact_after_recovery() {
    let (_temp, path, config, receipts) = crashed(1, 3);
    let recovered = recover(&path, &config).unwrap();
    for receipt in &receipts {
        assert_eq!(recovered.lookup(receipt.id()).unwrap(), Some(*receipt));
        assert_eq!(
            recovered.lookup_version(receipt.version()).unwrap(),
            Some(*receipt)
        );
    }
    assert_eq!(recovered.lookup(id_for(&config, 99)).unwrap(), None);
    assert_eq!(
        recovered
            .lookup_version(BatchVersion {
                epoch: config.inventory.epoch,
                sequence: 99
            })
            .unwrap(),
        None
    );
}

/// The earlier valid native group is still unsynced when a later external
/// reservation fails validation. No worker can publish beyond that reservation.
fn incomplete_after_valid_prefix() -> (tempdir::TempDir, PartitionConfig, Arc<PartitionLog>) {
    use crate::distributed::durability::DurabilityConfig;
    let temp = tempdir::TempDir::new("live-native-incomplete-sync").unwrap();
    let mut config = live_config();
    config.fragment_bytes = 16 * 1024;
    config.wal_file_bytes = 32 * 1024;
    let log = Arc::new(
        PartitionLog::create_live(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    let frontier = log.attach_frontier(&DurabilityConfig::default()).unwrap();
    append_live_fixture(&log, &config, 0);
    assert_eq!(frontier.synced(), 0);
    assert_eq!(frontier.appended(), 4096);
    // Close the open extent beyond its adopted end: the later frame was never
    // written, so the close fails as an incomplete live frame.
    let (token, end) = log
        .state
        .lock()
        .open_live_extent
        .as_ref()
        .map(|open| (open.extent.token, open.adoption.adopted_end))
        .unwrap();
    assert!(matches!(
        log.adopt_live_extent(authority(&config), token, end + 4096, true),
        Err(StorageError::IncompleteLiveFrame)
    ));
    assert!(matches!(
        log.state.lock().poison,
        Some(WriterPoison::IncompleteLiveFrame)
    ));
    (temp, config, log)
}

#[test]
fn syncer_shutdown_drains_only_the_validated_prefix_after_an_incomplete_live_frame() {
    use crate::distributed::durability::frontier::Syncer;
    let (_temp, config, log) = incomplete_after_valid_prefix();
    let frontier = log.frontier().unwrap().clone();
    let worker = log.clone();
    let syncer = Syncer::spawn("test-live-incomplete-drain", frontier.clone(), move |_| {
        worker.sync_for_frontier()
    })
    .unwrap();
    syncer.shutdown().unwrap();
    assert_eq!(frontier.synced(), 4096);
    assert_eq!(log.synced_frontier(), 4096);
    let root: LiveFrontier = read_metadata(&log.path.join(LIVE_FRONTIER)).unwrap();
    assert_eq!(root.end, 4096, "the unwritten frame is never covered");
    assert!(matches!(
        log.lease_live_extent(authority(&config)),
        Err(StorageError::Poisoned)
    ));
    assert!(matches!(
        log.state.lock().poison,
        Some(WriterPoison::IncompleteLiveFrame)
    ));
}

#[test]
fn persistence_failure_after_an_incomplete_frame_stays_fatal() {
    let (_temp, config, log) = incomplete_after_valid_prefix();
    log.fail_next_persist();
    assert!(log.sync_for_frontier().is_err());
    assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
    assert_eq!(log.synced_frontier(), 0);
    assert!(log.frontier().unwrap().failure().is_some());
    assert!(matches!(
        log.lease_live_extent(authority(&config)),
        Err(StorageError::Poisoned)
    ));
    assert!(log.sync_for_frontier().is_err());
    assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
}

#[test]
fn fatal_poison_during_an_incomplete_prefix_sync_blocks_durable_publication() {
    let (_temp, _config, log) = incomplete_after_valid_prefix();
    let (entered, resume) = log.pause_next_persist();
    let worker = log.clone();
    let syncing = std::thread::spawn(move || worker.sync_for_frontier());
    entered
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    log.poison_writer();
    resume.send(()).unwrap();
    assert!(syncing.join().unwrap().is_err());
    assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
    assert_eq!(log.synced_frontier(), 0);
    assert_eq!(log.frontier().unwrap().synced(), 0);
    assert!(log.frontier().unwrap().failure().is_some());
}
