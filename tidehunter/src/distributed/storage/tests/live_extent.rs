//! Real final-inode leased frames: prefix authority, close, and crash replay.
use super::*;
use crate::distributed::durability::{DurabilityConfig, DurabilityTier};
use crate::distributed::nvmeof::live_storage::{
    LiveGroupDescriptor, LiveWriteExtent, extend_live_receipt_digest,
};

mod leased_declared_invalid_frames_cases;
mod leased_split_block_cases;
mod leased_validation_and_cold_grant_cases;

fn fixture() -> (tempdir::TempDir, PartitionConfig, PartitionLog) {
    fixture_in_partition(live_config().partition)
}

fn fixture_in_partition(partition: u32) -> (tempdir::TempDir, PartitionConfig, PartitionLog) {
    let temp = tempdir::TempDir::new("live-leased-extent").unwrap();
    let mut config = live_config();
    config.partition = partition;
    config.inventory.partitions = vec![partition];
    config.fragment_bytes = 16 * 1024;
    config.wal_file_bytes = 32 * 1024;
    let log = PartitionLog::create_live(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    (temp, config, log)
}

fn encoded(config: &PartitionConfig, items: Vec<CompleteBatch>) -> (CompleteGroup, Vec<u8>) {
    let group =
        CompleteGroup::new_live(&shape(), items, config.batch_limits, config.group_limits).unwrap();
    let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
    (group, frame)
}

fn write(extent: &LiveWriteExtent, offset: u64, bytes: &[u8]) {
    OpenOptions::new()
        .write(true)
        .open(&extent.segment.path)
        .unwrap()
        .write_all_at(bytes, offset % extent.segment.bytes)
        .unwrap();
}

fn skip(extent: &LiveWriteExtent, offset: u64) {
    let mut block = [0; 4096];
    block[..8].copy_from_slice(CrcFrame::skip_marker().as_ref());
    write(extent, offset, &block);
}

fn expected(
    config: &PartitionConfig,
    group: &CompleteGroup,
    offset: u64,
    len: usize,
) -> Vec<AppendedBatch> {
    group
        .batches()
        .iter()
        .zip(group.batch_offsets())
        .map(|(batch, member)| {
            receipt_for(
                config,
                batch,
                member_position(WalPosition::new(offset, len as u32), *member, batch),
            )
        })
        .collect()
}

fn close_for_preparation(
    log: &PartitionLog,
    config: &PartitionConfig,
    extent: &LiveWriteExtent,
    sequence: u64,
) {
    let (_, frame) = encoded(config, vec![batch(config, sequence)]);
    write(extent, extent.start, &frame);
    skip(extent, extent.start + frame.len() as u64);
    log.adopt_live_extent(
        authority(config),
        extent.token,
        extent.start + frame.len() as u64,
        true,
    )
    .unwrap();
}

/// Leave the second extent open with an adopted frame and file 1 ready.
fn prepared_boundary() -> (
    tempdir::TempDir,
    PartitionConfig,
    Arc<PartitionLog>,
    LiveWriteExtent,
) {
    let (temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    close_for_preparation(&log, &config, &first, 0);
    let current = log.lease_live_extent(authority(&config)).unwrap().0;
    assert_eq!(log.prepare_next_live_file().unwrap(), Some(1));
    let (_, frame) = encoded(&config, vec![batch(&config, 1)]);
    write(&current, current.start, &frame);
    log.adopt_live_extent(
        authority(&config),
        current.token,
        current.start + frame.len() as u64,
        false,
    )
    .unwrap();
    (temp, config, Arc::new(log), current)
}

/// Two fragments: v0 and v1 then a skip marker at 8192 in fragment 0, v2 at the start
/// of fragment 1. With `damaged`, unsynced bytes inside v1 stop the scan there and
/// recovery probes fragment 1. Returns the stream path and fragment 1's start.
fn two_fragment_stream(damaged: bool) -> (tempdir::TempDir, PartitionConfig, u64) {
    let (temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    for n in 0..2 {
        let (_, frame) = encoded(&config, vec![batch(&config, n)]);
        write(&first, n * 4096, &frame);
    }
    skip(&first, 8192);
    log.adopt_live_extent(authority(&config), first.token, 8192, true)
        .unwrap();
    let second = log.lease_live_extent(authority(&config)).unwrap().0;
    assert_eq!(second.start, config.fragment_bytes);
    let (_, frame) = encoded(&config, vec![batch(&config, 2)]);
    write(&second, second.start, &frame);
    if damaged {
        write(&first, 4096 + 100, &[0x5a; 16]);
    }
    drop(log);
    (temp, config, second.start)
}

fn assert_recovery_fails_with_eio(path: &Path, config: &PartitionConfig, case: &str) {
    let error = PartitionLog::recover_live_sealed(path, config, metrics(), || Ok(()))
        .err()
        .unwrap_or_else(|| panic!("{case}: recovery reported a result despite EIO"));
    assert!(
        matches!(&error, StorageError::Io(io) if io.raw_os_error() == Some(libc::EIO)),
        "{case}: {error}"
    );
}

fn split_block_fixture() -> (tempdir::TempDir, PartitionConfig, PartitionLog) {
    let temp = tempdir::TempDir::new("live-leased-split-block").unwrap();
    let mut config = live_config();
    config.batch_limits.max_encoded_bytes = 32 * 1024;
    config.group_limits.max_encoded_bytes = 64 * 1024 - 8;
    config.fragment_bytes = 64 * 1024;
    config.wal_file_bytes = 128 * 1024;
    let log = PartitionLog::create_live(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    (temp, config, log)
}

fn sized_batch(config: &PartitionConfig, sequence: u64, size: usize) -> CompleteBatch {
    CompleteBatch::new(
        &shape(),
        id_for(config, sequence),
        BatchVersion {
            epoch: config.inventory.epoch,
            sequence,
        },
        vec![Mutation::Put {
            keyspace: KeySpace(0),
            key: b"key1"[..].into(),
            value: vec![sequence as u8; size].into(),
        }],
        config.batch_limits,
    )
    .unwrap()
}

fn parallel_adoption_fixture() -> (tempdir::TempDir, PartitionConfig, Arc<PartitionLog>) {
    let temp = tempdir::TempDir::new("live-parallel-adoption").unwrap();
    let mut config = live_config();
    config.batch_limits.max_encoded_bytes = 2 * 1024 * 1024;
    config.group_limits.max_encoded_bytes = 4 * 1024 * 1024 - 8;
    config.fragment_bytes = 8 * 1024 * 1024;
    config.wal_file_bytes = 2 * config.fragment_bytes;
    let log = PartitionLog::create_live(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    (temp, config, Arc::new(log))
}

fn cold_record(
    config: &PartitionConfig,
    extent: &LiveWriteExtent,
    sequence: u64,
) -> (ValueRecordReference, Vec<u8>) {
    let (group, frame) = encoded(config, vec![batch(config, sequence)]);
    write(extent, extent.start, &frame);
    let receipt = expected(config, &group, extent.start, frame.len())[0];
    (
        group.batches()[0]
            .record_reference(receipt.address(), 0)
            .unwrap(),
        frame,
    )
}

fn cold_frontiers(log: &PartitionLog) -> (u64, u64, u64, u64, usize) {
    let state = log.state.lock();
    (
        state.appended_end,
        state.durable_end,
        state.wal_root.as_ref().map_or(0, |root| root.synced_end),
        log.readable_frontier(),
        log.receipt_count(),
    )
}

#[test]
fn owner_events_trace_preparation_and_position_proof_in_order() {
    use crate::distributed::owner_events::{self, OwnerEventKind::*};
    let _capture = owner_events::OWNER_EVENTS_TEST_LOCK.lock();
    owner_events::enable(65_536);
    let (temp, config, log) = fixture_in_partition(u32::MAX - 1);
    owner_events::drain_json(&temp.path().join("prior-owner-events.json")).unwrap();
    log.attach_frontier(&DurabilityConfig::with_tier(DurabilityTier::Native))
        .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (before, dropped) = owner_events::snapshot();
    assert_eq!(dropped, 0);
    let before = before.len();
    assert_eq!(log.prepare_next_live_file().unwrap(), Some(1));
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, extent.start, &frame);
    let through = extent.start + frame.len() as u64;
    assert!(log.persist_wal_through(through).unwrap() >= through);
    let (events, dropped) = owner_events::snapshot();
    assert_eq!(dropped, 0);
    let actual: Vec<_> = events[before..]
        .iter()
        .filter(|event| event.partition == config.partition)
        .map(|event| (event.kind, event.a, event.b))
        .collect();
    let expected = [
        (PrepareEnsureStart, 1),
        (PrepareEnsureEnd, 1),
        (PrepareFileSyncStart, 1),
        (PrepareFileSyncEnd, 1),
        (PrepareWitnessStart, 1),
        (PrepareWitnessEnd, 1),
        (PreparePublished, 1),
        (ProofStart, through),
        (ProofFileSyncStart, 0),
        (ProofFileSyncEnd, 0),
        (ProofNamespaceSyncStart, 0),
        (ProofNamespaceSyncEnd, 0),
        (ProofEnd, through),
    ]
    .map(|(kind, a)| (kind, a, 0));
    assert_eq!(actual, expected);
}

#[test]
fn owner_events_close_proof_and_lease_spans_on_failure() {
    use crate::distributed::owner_events::{self, OwnerEventKind::*};
    let _capture = owner_events::OWNER_EVENTS_TEST_LOCK.lock();
    owner_events::enable(65_536);
    let (temp, config, log) = fixture_in_partition(u32::MAX - 2);
    owner_events::drain_json(&temp.path().join("prior-owner-events.json")).unwrap();
    log.attach_frontier(&DurabilityConfig::with_tier(DurabilityTier::Native))
        .unwrap();

    let (before, dropped) = owner_events::snapshot();
    assert_eq!(dropped, 0);
    let before = before.len();
    assert!(log.persist_wal_through(u64::MAX).is_err());
    let (events, dropped) = owner_events::snapshot();
    assert_eq!(dropped, 0);
    let proof: Vec<_> = events[before..]
        .iter()
        .filter(|event| event.partition == config.partition && event.a == u64::MAX)
        .map(|event| event.kind)
        .collect();
    assert_eq!(proof, [ProofStart, ProofEnd]);

    log.fail_namespace_persist.store(true, Ordering::Release);
    let (before, dropped) = owner_events::snapshot();
    assert_eq!(dropped, 0);
    let before = before.len();
    assert!(log.lease_live_extent(authority(&config)).is_err());
    let (events, dropped) = owner_events::snapshot();
    assert_eq!(dropped, 0);
    let actual: Vec<_> = events[before..]
        .iter()
        .filter(|event| event.partition == config.partition)
        .map(|event| event.kind)
        .collect();
    assert_eq!(
        actual,
        [
            LeaseGrantStart,
            LeaseNamespaceSyncStart,
            LeaseNamespaceSyncEnd,
            LeaseGrantEnd,
        ]
    );
}

#[test]
fn prepared_prelease_bypasses_receipt_sync_and_preserves_its_capture() {
    for fail in [false, true] {
        let (_temp, config, log, current) = prepared_boundary();
        let end = log.appended_frontier();
        let namespace = log.state.lock().namespace_synced_file;
        let stats = log.stats();
        let (entered, resume) = log.pause_next_persist();
        let owner = log.clone();
        let sync = std::thread::spawn(move || {
            let mut profile = StorageWriteProfile::default();
            let result = owner.persist_appended_profiled(&mut profile);
            (result, profile)
        });
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(log.persistence.is_locked());
        let owner = log.clone();
        let auth = authority(&config);
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let grant = std::thread::spawn(move || {
            tx.send(owner.prelease_live_extent(auth, current.token))
                .unwrap();
        });
        let granted = rx.recv_timeout(Duration::from_secs(5));
        let before_resume = (log.synced_frontier(), log.appended_frontier(), log.stats());
        if fail {
            log.fail_live_frontier.store(1, Ordering::Release);
        }
        resume.send(()).unwrap();
        grant.join().unwrap();
        let (result, profile) = sync.join().unwrap();
        let (next, lease_profile) = granted.unwrap().unwrap();
        assert_eq!(next.segment.file_index, 1);
        assert_eq!(
            (next.start, next.end),
            (
                config.wal_file_bytes,
                config.wal_file_bytes + config.fragment_bytes
            )
        );
        assert_ne!(next.token, current.token);
        assert_eq!(lease_profile.namespace_sync_ns, 0);
        assert_eq!(
            log.state
                .lock()
                .live_test_diagnostics
                .lease
                .persistence_wait_ns,
            0
        );
        assert_eq!(before_resume, (0, end, stats));
        assert_eq!(log.state.lock().namespace_synced_file, namespace);
        let capture = profile.persistence_capture.unwrap();
        assert_eq!(capture.durable_end, 0);
        assert_eq!(capture.captured_appended_end, end);
        if fail {
            assert!(result.is_err());
            assert_eq!(log.synced_frontier(), 0);
            assert!(matches!(
                log.prelease_live_extent(auth, current.token),
                Err(StorageError::Poisoned)
            ));
        } else {
            assert_eq!(result.unwrap(), end);
            assert_eq!(log.synced_frontier(), end);
        }
    }
}

#[test]
fn prepared_prelease_bypasses_position_root_install_without_changing_cut() {
    let (_temp, config, log, current) = prepared_boundary();
    let through = log.appended_frontier();
    let old_root = fs::read(log.path.join(super::super::wal_replay::ROOT)).unwrap();
    let old_frontier = log.synced_frontier();
    let (entered, resume) = log.pause_live(LivePausePoint::PositionRootLocked);
    let owner = log.clone();
    let proof = std::thread::spawn(move || owner.persist_wal_through(through));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(log.persistence.is_locked());
    let owner = log.clone();
    let auth = authority(&config);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let grant = std::thread::spawn(move || {
        tx.send(owner.prelease_live_extent(auth, current.token))
            .unwrap();
    });
    let granted = rx.recv_timeout(Duration::from_secs(5));
    let root_before_resume = fs::read(log.path.join(super::super::wal_replay::ROOT)).unwrap();
    let cut_before_resume = log.position_frontier_for_test();
    let receipt_before_resume = log.synced_frontier();
    resume.send(()).unwrap();
    grant.join().unwrap();
    granted.unwrap().unwrap();
    assert_eq!(root_before_resume, old_root);
    assert_eq!(cut_before_resume, 0);
    assert_eq!(receipt_before_resume, old_frontier);
    assert_eq!(proof.join().unwrap().unwrap(), through);
    assert_eq!(log.position_frontier_for_test(), through);
    assert_eq!(log.synced_frontier(), old_frontier);
    assert!(log.state.lock().writer.as_ref().unwrap().position() > through);
}

#[test]
fn unprepared_prelease_waits_for_persistence_and_namespace_witness() {
    for fail_witness in [false, true] {
        let (temp, config, log) = fixture();
        let first = log.lease_live_extent(authority(&config)).unwrap().0;
        close_for_preparation(&log, &config, &first, 0);
        let current = log.lease_live_extent(authority(&config)).unwrap().0;
        let log = Arc::new(log);
        let persistence = log.persistence.lock();
        let (waiting, release_wait) = log.pause_live(LivePausePoint::LeaseWaitingForPersistence);
        let (syncing, release_sync) = log.pause_next_persist();
        let owner = log.clone();
        let auth = authority(&config);
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let grant = std::thread::spawn(move || {
            tx.send(owner.prelease_live_extent(auth, current.token))
                .unwrap();
        });
        waiting.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        assert!(log.state.lock().next_live_extent.is_none());
        release_wait.send(()).unwrap();
        drop(persistence);
        syncing.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        assert_eq!(log.state.lock().namespace_synced_file.unwrap().as_u64(), 0);
        let witness = temp
            .path()
            .join("stream/partition-required-wal-v1-0000000000000001");
        assert!(!witness.exists());
        if fail_witness {
            log.fail_next_inventory_witness(false);
        }
        release_sync.send(()).unwrap();
        let result = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        grant.join().unwrap();
        if fail_witness {
            assert!(matches!(result, Err(StorageError::Unknown(_))));
            assert!(!witness.exists());
            assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
            assert!(matches!(
                log.prelease_live_extent(auth, current.token),
                Err(StorageError::Poisoned)
            ));
        } else {
            let (next, profile) = result.unwrap();
            assert_eq!(next.segment.file_index, 1);
            assert!(profile.namespace_sync_ns > 0);
            assert!(witness.is_file());
        }
    }
}

#[test]
fn concurrent_preleases_share_validation_and_never_overlap() {
    let (_temp, config, log, current) = prepared_boundary();
    let persistence = log.persistence.lock();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let (tx, rx) = std::sync::mpsc::sync_channel(2);
    let mut workers = Vec::new();
    for _ in 0..2 {
        let owner = log.clone();
        let start = barrier.clone();
        let done = tx.clone();
        let auth = authority(&config);
        workers.push(std::thread::spawn(move || {
            start.wait();
            done.send(owner.prelease_live_extent(auth, current.token))
                .unwrap();
        }));
    }
    barrier.wait();
    let first = rx.recv_timeout(Duration::from_secs(5));
    let second = rx.recv_timeout(Duration::from_secs(5));
    drop(persistence);
    for worker in workers {
        worker.join().unwrap();
    }
    let results = [first.unwrap(), second.unwrap()];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let next = &results
        .iter()
        .find_map(|result| result.as_ref().ok())
        .unwrap()
        .0;
    assert_eq!(
        (next.start, next.end),
        (
            config.wal_file_bytes,
            config.wal_file_bytes + config.fragment_bytes
        )
    );
    assert!(current.end <= next.start);
    assert!(results.iter().any(|result| matches!(
        result,
        Err(StorageError::Invalid("live extent already preleased"))
    )));
    assert_eq!(
        log.state.lock().writer.as_ref().unwrap().position(),
        next.end
    );
}

#[test]
fn prepared_fast_validation_rejects_stale_terminal_and_wrong_epoch_requests() {
    let (_temp, config, log, current) = prepared_boundary();
    let persistence = log.persistence.lock();
    let owner = log.clone();
    let auth = authority(&config);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        assert!(matches!(
            owner.prelease_live_extent(auth, [0; 16]),
            Err(StorageError::Invalid(
                "prelease requires the open live extent"
            ))
        ));
        assert!(matches!(
            owner.prelease_live_extent(
                RequestAuthority {
                    epoch: auth.epoch + 1
                },
                current.token
            ),
            Err(StorageError::Fenced)
        ));
        owner.state.lock().lifecycle = Lifecycle::Sealing;
        assert!(matches!(
            owner.prelease_live_extent(auth, current.token),
            Err(StorageError::Invalid("live stream is sealing"))
        ));
        owner.state.lock().lifecycle = Lifecycle::Running;
        owner.state.lock().sealed = true;
        assert!(matches!(
            owner.prelease_live_extent(auth, current.token),
            Err(StorageError::Sealed)
        ));
        owner.state.lock().sealed = false;
        owner.state.lock().poison = Some(WriterPoison::Fatal);
        assert!(matches!(
            owner.prelease_live_extent(auth, current.token),
            Err(StorageError::Poisoned)
        ));
        tx.send(()).unwrap();
    });
    let complete = rx.recv_timeout(Duration::from_secs(5));
    drop(persistence);
    worker.join().unwrap();
    complete.unwrap();
    assert_eq!(
        log.state.lock().writer.as_ref().unwrap().position(),
        config.wal_file_bytes
    );
    assert!(log.state.lock().next_live_extent.is_none());
}

#[test]
fn prepared_live_file_grant_does_no_namespace_work() {
    let (temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    assert_eq!(log.prepare_next_live_file().unwrap(), Some(1));
    assert_eq!(log.state.lock().namespace_synced_file.unwrap().as_u64(), 1);
    assert!(
        temp.path()
            .join("stream/partition-required-wal-v1-0000000000000001")
            .is_file()
    );
    close_for_preparation(&log, &config, &first, 0);
    let second = log.lease_live_extent(authority(&config)).unwrap().0;
    close_for_preparation(&log, &config, &second, 1);
    let syncs = log.stats().namespace_syncs;
    let (entered, resume) = log.pause_next_persist();
    let log = Arc::new(log);
    let owner = log.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let auth = authority(&config);
    let worker = std::thread::spawn(move || tx.send(owner.lease_live_extent(auth)).unwrap());
    let result = rx.recv_timeout(Duration::from_secs(5));
    let hook_entered = entered.try_recv().is_ok();
    drop(resume); // Unblock a broken implementation before reporting failure.
    worker.join().unwrap();
    let (third, profile) = result.unwrap().unwrap();
    assert!(!hook_entered);
    assert_eq!(third.segment.file_index, 1);
    assert_eq!(profile.namespace_sync_ns, 0);
    assert_eq!(log.stats().namespace_syncs, syncs);
    assert_eq!(log.live_file_preparation_stats().live_files_prepared, 1);
}

/// A fenced epoch starts with no published namespace. Preparation must publish
/// the allocator's file itself (allocation, file sync, directory witness), so
/// the epoch's first lease is memory work instead of two syncs under the
/// writer's deadline.
#[test]
fn first_live_file_is_prepared_before_any_grant() {
    // The lease returns when the owner makes progress; a broken
    // implementation parks it behind the paused persist until `resume` drops.
    const PROGRESS_WAIT: Duration = Duration::from_secs(60);
    let (_temp, config, log) = fixture();
    assert!(log.state.lock().namespace_synced_file.is_none());
    assert_eq!(log.prepare_next_live_file().unwrap(), Some(0));
    assert_eq!(log.state.lock().namespace_synced_file.unwrap().as_u64(), 0);
    assert_eq!(log.live_file_preparation_stats().live_files_prepared, 1);
    let syncs = log.stats().namespace_syncs;
    let (entered, resume) = log.pause_next_persist();
    let log = Arc::new(log);
    let owner = log.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let auth = authority(&config);
    let worker = std::thread::spawn(move || tx.send(owner.lease_live_extent(auth)).unwrap());
    let result = rx.recv_timeout(PROGRESS_WAIT);
    let hook_entered = entered.try_recv().is_ok();
    drop(resume); // Unblock a broken implementation before reporting failure.
    worker.join().unwrap();
    let (first, profile) = result
        .expect("the first lease waited on file preparation")
        .unwrap();
    assert!(
        !hook_entered,
        "the first lease ran the inline file persistence"
    );
    assert_eq!(first.segment.file_index, 0);
    assert_eq!(first.start, 0);
    assert_eq!(profile.namespace_sync_ns, 0);
    assert_eq!(log.stats().namespace_syncs, syncs);
    assert_eq!(
        log.state
            .lock()
            .live_test_diagnostics
            .lease
            .persistence_wait_ns,
        0
    );
}

#[test]
fn live_file_preparation_waits_until_near_file_end() {
    let temp = tempdir::TempDir::new("live-preparation-distance").unwrap();
    let mut config = live_config();
    config.fragment_bytes = 16 * 1024;
    config.wal_file_bytes = 8 * config.fragment_bytes;
    let log = PartitionLog::create_live(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    log.lease_live_extent(authority(&config)).unwrap();
    let before = log.state.lock().namespace_synced_file;
    assert_eq!(log.prepare_next_live_file().unwrap(), None);
    assert_eq!(log.state.lock().namespace_synced_file, before);
    assert_eq!(log.live_file_preparation_stats().live_files_prepared, 0);
}

#[test]
fn live_file_preparation_is_idempotent() {
    let (_temp, config, log) = fixture();
    log.lease_live_extent(authority(&config)).unwrap();
    assert_eq!(log.prepare_next_live_file().unwrap(), Some(1));
    let before = log.stats();
    assert_eq!(log.prepare_next_live_file().unwrap(), None);
    assert_eq!(log.stats(), before);
    assert_eq!(log.live_file_preparation_stats().live_files_prepared, 1);
}

#[test]
fn same_file_prelease_completes_during_file_preparation() {
    let (_temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    let log = Arc::new(log);
    let (entered, resume) = log.pause_live(LivePausePoint::PrepareBeforePublish);
    let owner = log.clone();
    let preparation = std::thread::spawn(move || owner.prepare_next_live_file());
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let owner = log.clone();
    let auth = authority(&config);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let grant = std::thread::spawn(move || {
        tx.send(owner.prelease_live_extent(auth, first.token))
            .unwrap()
    });
    let result = rx.recv_timeout(Duration::from_secs(5));
    resume.send(()).unwrap();
    grant.join().unwrap();
    result.unwrap().unwrap();
    // The same-file prelease moves the writer to the target's start, but does
    // not publish that target's namespace. Preparation must still finish.
    assert_eq!(preparation.join().unwrap().unwrap(), Some(1));
    assert_eq!(log.state.lock().namespace_synced_file.unwrap().as_u64(), 1);
    assert_eq!(log.live_file_preparation_stats().live_files_prepared, 1);
}

#[test]
fn live_file_preparation_starts_at_unpublished_file_boundary() {
    let (_temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    log.prelease_live_extent(authority(&config), first.token)
        .unwrap();
    assert_eq!(log.prepare_next_live_file().unwrap(), Some(1));
    assert_eq!(log.state.lock().namespace_synced_file.unwrap().as_u64(), 1);
    assert_eq!(log.live_file_preparation_stats().live_files_prepared, 1);
}

#[test]
fn late_file_preparation_does_not_republish_after_a_grant() {
    let (_temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    let log = Arc::new(log);
    let (entered, resume) = log.pause_live(LivePausePoint::PrepareBeforePublish);
    let owner = log.clone();
    let preparation = std::thread::spawn(move || owner.prepare_next_live_file());
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let second = log
        .prelease_live_extent(authority(&config), first.token)
        .unwrap()
        .0;
    close_for_preparation(&log, &config, &first, 0);
    close_for_preparation(&log, &config, &second, 1);
    let (third, profile) = log.lease_live_extent(authority(&config)).unwrap();
    assert_eq!(third.segment.file_index, 1);
    assert!(profile.namespace_sync_ns > 0);
    let stats = log.stats();
    resume.send(()).unwrap();
    assert_eq!(preparation.join().unwrap().unwrap(), None);
    assert_eq!(log.state.lock().namespace_synced_file.unwrap().as_u64(), 1);
    assert_eq!(log.live_file_preparation_stats().live_files_prepared, 0);
    assert_eq!(log.stats(), stats);
}

#[test]
fn seal_during_file_preparation_prevents_publication() {
    let (_temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    close_for_preparation(&log, &config, &first, 0);
    log.persist_appended().unwrap();
    let log = Arc::new(log);
    let (entered, resume) = log.pause_live(LivePausePoint::PrepareBeforePublish);
    let owner = log.clone();
    let preparation = std::thread::spawn(move || owner.prepare_next_live_file());
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let owner = log.clone();
    let auth = authority(&config);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let seal = std::thread::spawn(move || tx.send(owner.seal_live_epoch(auth, || Ok(()))).unwrap());
    let sealed = rx.recv_timeout(Duration::from_secs(5));
    resume.send(()).unwrap();
    seal.join().unwrap();
    sealed.unwrap().unwrap();
    assert_eq!(preparation.join().unwrap().unwrap(), None);
    let state = log.state.lock();
    assert!(state.sealed);
    assert!(state.poison.is_none());
    assert_eq!(state.live_file_preparation.live_files_prepared, 0);
}

#[test]
fn prepared_file_crash_cuts_recover_acknowledged_state_in_both_tiers() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        for cut in 0..6 {
            let (temp, config, log) = fixture();
            let log = Arc::new(log);
            log.attach_frontier(&DurabilityConfig::with_tier(tier))
                .unwrap();
            let first = log.lease_live_extent(authority(&config)).unwrap().0;
            let (group, frame) = encoded(&config, vec![batch(&config, 0)]);
            write(&first, first.start, &frame);
            log.adopt_live_extent(authority(&config), first.token, frame.len() as u64, false)
                .unwrap();
            if tier == DurabilityTier::Synchronous && cut != 5 {
                log.persist_appended().unwrap();
            }
            let mut receipts = expected(&config, &group, 0, frame.len());
            let mut acknowledged_end = frame.len() as u64;
            let mut paused_sync = None;
            match cut {
                0 => {
                    let preparer = log
                        .state
                        .lock()
                        .writer
                        .as_ref()
                        .unwrap()
                        .external_file_preparer();
                    preparer
                        .ensure(crate::wal::position::WalFileId::from_u64(1))
                        .unwrap();
                }
                1 => {
                    // File synced; no namespace/witness work has started.
                    let preparer = log
                        .state
                        .lock()
                        .writer
                        .as_ref()
                        .unwrap()
                        .external_file_preparer();
                    preparer
                        .ensure(crate::wal::position::WalFileId::from_u64(1))
                        .unwrap();
                    log.wal
                        .persist_frame_file(WalPosition::new(config.wal_file_bytes, 1))
                        .unwrap();
                }
                2 | 3 => {
                    // Cut after namespace sync, before or after the witness,
                    // without publishing the prepared highwater in memory.
                    log.fail_next_inventory_witness(cut == 3);
                    assert!(log.prepare_next_live_file().is_err());
                    assert_eq!(log.live_file_preparation_stats().live_files_prepared, 0);
                }
                4 => assert_eq!(log.prepare_next_live_file().unwrap(), Some(1)),
                _ => {
                    skip(&first, first.start + frame.len() as u64);
                    log.adopt_live_extent(
                        authority(&config),
                        first.token,
                        first.start + frame.len() as u64,
                        true,
                    )
                    .unwrap();
                    let current = log.lease_live_extent(authority(&config)).unwrap().0;
                    let (group, bytes) = encoded(&config, vec![batch(&config, 1)]);
                    write(&current, current.start, &bytes);
                    acknowledged_end = current.start + bytes.len() as u64;
                    skip(&current, acknowledged_end);
                    log.adopt_live_extent(
                        authority(&config),
                        current.token,
                        acknowledged_end,
                        false,
                    )
                    .unwrap();
                    receipts.extend(expected(&config, &group, current.start, bytes.len()));
                    if tier == DurabilityTier::Synchronous {
                        // Both direct-write acknowledgements have completed the
                        // initiator's file sync, not the owner's receipt sync.
                        OpenOptions::new()
                            .write(true)
                            .open(&current.segment.path)
                            .unwrap()
                            .sync_data()
                            .unwrap();
                    }
                    assert_eq!(log.prepare_next_live_file().unwrap(), Some(1));
                    let (entered, resume) = log.pause_next_persist();
                    let owner = log.clone();
                    let sync = std::thread::spawn(move || owner.persist_appended());
                    entered.recv_timeout(Duration::from_secs(5)).unwrap();
                    assert!(log.persistence.is_locked());
                    let owner = log.clone();
                    let auth = authority(&config);
                    let (tx, rx) = std::sync::mpsc::sync_channel(1);
                    let grant = std::thread::spawn(move || {
                        tx.send(owner.prelease_live_extent(auth, current.token))
                            .unwrap();
                    });
                    let granted = rx.recv_timeout(Duration::from_secs(5));
                    if granted.is_err() {
                        // Unblock a broken serialized implementation before failure.
                        resume.send(()).unwrap();
                        grant.join().unwrap();
                        sync.join().unwrap().unwrap();
                        panic!("prepared grant waited for receipt sync");
                    }
                    grant.join().unwrap();
                    let (next, profile) = granted.unwrap().unwrap();
                    assert_eq!(next.segment.file_index, 1);
                    assert_eq!(next.start, config.wal_file_bytes);
                    assert_eq!(next.end, config.wal_file_bytes + config.fragment_bytes);
                    assert_eq!(profile.namespace_sync_ns, 0);
                    assert_eq!(log.synced_frontier(), 0);
                    // Preserve only a frame prefix and leave the unwritten body
                    // zero. Force a bad CRC for this exact torn body so rejection
                    // does not depend on the checksum of omitted bytes.
                    let (_, bytes) = encoded(&config, vec![batch(&config, 2)]);
                    let mut torn = vec![0; bytes.len()];
                    torn[..128].copy_from_slice(&bytes[..128]);
                    let crc = crc32fast::hash(&torn[8..]) ^ 1;
                    torn[4..8].copy_from_slice(&crc.to_be_bytes());
                    write(&next, next.start, &torn);
                    paused_sync = Some((resume, sync));
                }
            }
            let image = temp.path().join("crash-image");
            fs::create_dir(&image).unwrap();
            for entry in fs::read_dir(temp.path().join("stream")).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_file() {
                    fs::copy(entry.path(), image.join(entry.file_name())).unwrap();
                }
            }
            if let Some((resume, sync)) = paused_sync {
                resume.send(()).unwrap();
                assert_eq!(sync.join().unwrap().unwrap(), acknowledged_end);
            }
            let recovered =
                PartitionLog::recover_live_sealed(&image, &config, metrics(), || Ok(())).unwrap();
            assert_eq!(
                recovered.receipt_page(0, 16).unwrap(),
                receipts,
                "cut {cut}, tier {tier}"
            );
            if cut == 5 {
                assert_eq!(recovered.appended_frontier(), acknowledged_end);
                assert!(recovered.appended_frontier() < config.wal_file_bytes);
                assert!(recovered.lookup(batch(&config, 2).id()).unwrap().is_none());
                for (sequence, receipt) in receipts.iter().copied().enumerate() {
                    assert_eq!(
                        recovered.read_batch(receipt).unwrap().mutations(),
                        batch(&config, sequence as u64).mutations()
                    );
                }
            }
            let mut next_config = config.clone();
            next_config.inventory.epoch += 1;
            let next = PartitionLog::create_live(
                &temp.path().join("next-epoch"),
                next_config.clone(),
                &shape(),
                metrics(),
            )
            .unwrap();
            assert!(next.lease_live_extent(authority(&next_config)).is_ok());
        }
    }
}

/// The first file of a fenced epoch is prepared in the background, so its
/// allocation, file sync, namespace sync and inventory witness run outside the
/// first lease. Cut each durable boundary before the in-memory publication: the
/// stream must recover, no acknowledged state exists, and the next epoch's
/// first lease succeeds. A cut inside the witness poisons the owner, exactly as
/// the inline path does (`leased_namespace_or_witness_failure_never_grants_an_extent`).
#[test]
fn first_file_preparation_crash_cuts_recover_and_grant_the_next_epoch() {
    use crate::wal::position::WalFileId;
    for cut in 0..5 {
        let (temp, config, log) = fixture();
        match cut {
            // 0: allocated only. 1: allocated and file synced; no namespace or
            // witness work has started.
            0 | 1 => {
                let preparer = log
                    .state
                    .lock()
                    .writer
                    .as_ref()
                    .unwrap()
                    .external_file_preparer();
                preparer.ensure(WalFileId::from_u64(0)).unwrap();
                if cut == 1 {
                    log.wal.persist_frame_file(WalPosition::new(0, 1)).unwrap();
                }
            }
            // 4: the file is prepared and its namespace is published (in memory
            // and on disk) with no extent granted.
            4 => {
                assert_eq!(log.prepare_next_live_file().unwrap(), Some(0));
                assert_eq!(log.state.lock().namespace_synced_file.unwrap().as_u64(), 0);
            }
            // 2: cut after the namespace sync, before the witness. 3: cut after
            // the witness, without publishing the prepared highwater.
            _ => {
                log.fail_next_inventory_witness(cut == 3);
                assert!(log.prepare_next_live_file().is_err());
                assert_eq!(log.live_file_preparation_stats().live_files_prepared, 0);
                let state = log.state.lock();
                assert!(state.namespace_synced_file.is_none());
                assert!(matches!(state.poison, Some(WriterPoison::Fatal)));
            }
        }
        let image = temp.path().join("crash-image");
        fs::create_dir(&image).unwrap();
        for entry in fs::read_dir(temp.path().join("stream")).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                fs::copy(entry.path(), image.join(entry.file_name())).unwrap();
            }
        }
        // The durable namespace highwater never precedes its witness, and the
        // witness is what a crash cut leaves for file 0.
        let witnessed =
            crate::distributed::storage::file_inventory::witnessed_files(&image).unwrap();
        assert_eq!(
            witnessed.contains(&0),
            cut >= 3,
            "cut {cut} left witnesses {witnessed:?}"
        );
        if cut < 2 {
            // Nothing was published and nothing is durable: the inline path
            // still grants file 0 on the running log.
            assert!(log.state.lock().namespace_synced_file.is_none());
            let (first, profile) = log.lease_live_extent(authority(&config)).unwrap();
            assert_eq!(first.segment.file_index, 0, "cut {cut}");
            assert_eq!(first.start, 0, "cut {cut}");
            assert!(
                profile.namespace_sync_ns > 0,
                "cut {cut}: the inline path did no namespace work"
            );
        }
        let recovered =
            PartitionLog::recover_live_sealed(&image, &config, metrics(), || Ok(())).unwrap();
        assert!(
            recovered.receipt_page(0, 16).unwrap().is_empty(),
            "cut {cut}"
        );
        assert_eq!(recovered.appended_frontier(), 0, "cut {cut}");
        let mut next_config = config.clone();
        next_config.inventory.epoch += 1;
        let next = PartitionLog::create_live(
            &temp.path().join("next-epoch"),
            next_config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap();
        let (extent, _) = next.lease_live_extent(authority(&next_config)).unwrap();
        assert_eq!(extent.segment.file_index, 0, "cut {cut}");
        assert_eq!(extent.start, 0, "cut {cut}");
        assert_eq!(extent.end, next_config.fragment_bytes, "cut {cut}");
    }
}

#[test]
fn live_file_preparation_namespace_failure_poison_fences_leases() {
    let (_temp, config, log) = fixture();
    log.lease_live_extent(authority(&config)).unwrap();
    log.fail_namespace_persist.store(true, Ordering::Release);
    assert!(matches!(
        log.prepare_next_live_file(),
        Err(StorageError::Unknown(_))
    ));
    assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
    assert!(matches!(
        log.lease_live_extent(authority(&config)),
        Err(StorageError::Poisoned)
    ));
}

#[test]
fn leased_two_step_adoption_matches_reserved_receipts_and_frontier_rules() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let (_temp, config, log) = fixture();
        log.attach_frontier(&DurabilityConfig::with_tier(tier))
            .unwrap();
        let (extent, lease_profile) = log.lease_live_extent(authority(&config)).unwrap();
        assert!(lease_profile.reserve_ns > 0);
        assert!(lease_profile.namespace_sync_ns > 0);
        assert_eq!(lease_profile.publish_ns, 0);
        assert!(matches!(
            log.lease_live_extent(authority(&config)),
            Err(StorageError::Invalid("live extent already open"))
        ));
        assert!(log.state.lock().poison.is_none());
        let reserved_temp = tempdir::TempDir::new("live-reserved-parity").unwrap();
        let reserved = PartitionLog::create_live(
            &reserved_temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap();
        let mut receipts = Vec::new();
        for n in 0..3 {
            let (group, frame) = encoded(&config, vec![batch(&config, n)]);
            write(&extent, extent.start + n * 4096, &frame);
            let actual = append_live_fixture(&reserved, &config, n);
            assert_eq!(
                expected(&config, &group, extent.start + n * 4096, frame.len()),
                vec![actual]
            );
            receipts.push(actual);
        }
        let (first, profile) = log
            .adopt_live_extent(authority(&config), extent.token, 4096, false)
            .unwrap();
        assert_eq!(profile.frames, 1);
        assert!(profile.adopt_ns > 0);
        assert_eq!((profile.close_scan_ns, profile.close_scan_bytes), (0, 0));
        assert_eq!(first.receipt_count, 1);
        assert_eq!(
            first.receipt_digest,
            extend_live_receipt_digest([0; 32], receipts[0])
        );
        let (second, profile) = log
            .adopt_live_extent(authority(&config), extent.token, 12288, false)
            .unwrap();
        assert_eq!(profile.frames, 2);
        let digest = receipts.iter().fold([0; 32], |digest, receipt| {
            extend_live_receipt_digest(digest, *receipt)
        });
        assert_eq!(second.receipt_count, 3);
        assert_eq!(second.receipt_digest, digest);
        assert_eq!(log.receipt_page(0, 3).unwrap(), receipts);
        assert_eq!(
            log.adopt_live_extent(authority(&config), extent.token, 12288, false)
                .unwrap()
                .0,
            second
        );
        assert_eq!(
            log.adopt_live_extent(authority(&config), extent.token, 4096, false)
                .unwrap()
                .0,
            second
        );
        for receipt in &receipts {
            let item = batch(&config, receipt.version().sequence);
            let reference = item.record_reference(receipt.address(), 0).unwrap();
            if tier == DurabilityTier::Synchronous {
                assert!(matches!(
                    log.read_record(&reference),
                    Err(StorageError::NeedsPersistence)
                ));
            } else {
                assert_eq!(log.read_record(&reference).unwrap(), item.mutations()[0]);
            }
        }
        log.persist_appended().unwrap();
        for receipt in receipts {
            let item = batch(&config, receipt.version().sequence);
            let reference = item.record_reference(receipt.address(), 0).unwrap();
            assert_eq!(log.read_record(&reference).unwrap(), item.mutations()[0]);
        }
    }
}
