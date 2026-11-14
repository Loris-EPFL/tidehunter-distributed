//! Real final-inode leased frames: prefix authority, close, and crash replay.
use super::*;
use crate::distributed::durability::{DurabilityConfig, DurabilityTier};
use crate::distributed::nvmeof::live_storage::{
    LiveGroupDescriptor, LiveWriteExtent, extend_live_receipt_digest,
};

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
        owner.state.lock().sealing = true;
        assert!(matches!(
            owner.prelease_live_extent(auth, current.token),
            Err(StorageError::Invalid("live stream is sealing"))
        ));
        owner.state.lock().sealing = false;
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

#[test]
fn leased_declared_invalid_frames_poison_only_the_unvalidated_suffix() {
    for defect in ["middle", "crc", "epoch", "database", "identity", "version"] {
        let (_temp, config, log) = fixture();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
        write(&extent, 0, &frame);
        let mut item = batch(&config, 1);
        if ["epoch", "database", "identity", "version"].contains(&defect) {
            let mut id = item.id();
            let mut version = item.version();
            match defect {
                "epoch" => version.epoch += 1,
                "database" => id.database = [99; 16],
                "identity" => id = batch(&config, 0).id(),
                "version" => version = batch(&config, 0).version(),
                _ => unreachable!(),
            }
            item = CompleteBatch::new(
                &shape(),
                id,
                version,
                item.mutations().to_vec(),
                config.batch_limits,
            )
            .unwrap();
        }
        let (_, mut frame) = encoded(&config, vec![item]);
        if defect == "crc" {
            frame[100] ^= 1;
        }
        write(&extent, 4096, &frame);
        let through = if defect == "middle" { 6144 } else { 8192 };
        assert!(
            matches!(
                log.adopt_live_extent(authority(&config), extent.token, through, false),
                Err(StorageError::IncompleteLiveFrame)
            ),
            "{defect}"
        );
        assert_eq!(log.receipt_count(), 1, "{defect}");
        assert_eq!(log.appended_frontier(), 4096);
        assert!(matches!(
            log.state.lock().poison,
            Some(WriterPoison::IncompleteLiveFrame)
        ));
        assert_eq!(log.persist_appended().unwrap(), 4096);
        assert!(matches!(
            log.lease_live_extent(authority(&config)),
            Err(StorageError::Poisoned)
        ));
    }
}

#[test]
fn leased_close_validates_skip_and_suffix_and_releases_tracker() {
    for full in [false, true] {
        let (_temp, config, log) = fixture();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let count = if full { 4 } else { 1 };
        for n in 0..count {
            let (_, frame) = encoded(&config, vec![batch(&config, n)]);
            write(&extent, n * 4096, &frame);
        }
        let through = count * 4096;
        if !full {
            skip(&extent, through);
        }
        let (closed, profile) = log
            .adopt_live_extent(authority(&config), extent.token, through, true)
            .unwrap();
        assert_eq!(profile.frames, count);
        assert_eq!(profile.close_scan_bytes, extent.end - through);
        assert!(profile.adopt_ns >= profile.close_scan_ns);
        assert!(closed.closed);
        assert_eq!(closed.adopted_end, through);
        assert_eq!(
            log.adopt_live_extent(authority(&config), extent.token, through, true)
                .unwrap()
                .0,
            closed
        );
        let next = log.lease_live_extent(authority(&config)).unwrap().0;
        assert_eq!(next.start, extent.end);
        assert_eq!(next.end, extent.end + config.fragment_bytes);
        let (_, frame) = encoded(&config, vec![batch(&config, 10)]);
        write(&next, next.start, &frame);
        skip(&next, next.start + 4096);
        log.adopt_live_extent(authority(&config), next.token, next.start + 4096, true)
            .unwrap();
        // A third extent enters the next file. This cannot finish if the held
        // external guard did not unblock the native fragment tracker.
        let third = log.lease_live_extent(authority(&config)).unwrap().0;
        assert_eq!(third.start, 2 * config.fragment_bytes);
        assert_eq!(third.segment.file_index, 1);
    }
}

#[test]
fn leased_close_rejects_missing_marker_and_nonzero_suffix() {
    for nonzero in [false, true] {
        let (_temp, config, log) = fixture();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
        write(&extent, 0, &frame);
        if nonzero {
            skip(&extent, 4096);
            write(&extent, 12288, &[1]);
        }
        assert!(matches!(
            log.adopt_live_extent(authority(&config), extent.token, 4096, true),
            Err(StorageError::IncompleteLiveFrame)
        ));
        assert_eq!(log.receipt_count(), 1);
        assert_eq!(log.persist_appended().unwrap(), 4096);
    }
}

#[test]
fn leased_self_adoption_stops_at_torn_frame_and_keeps_sync_checkpoint_barrier() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let (_temp, config, log) = fixture();
        let frontier = log
            .attach_frontier(&DurabilityConfig::with_tier(tier))
            .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (group, frame) = encoded(&config, vec![batch(&config, 0)]);
        let receipt = expected(&config, &group, 0, frame.len())[0];
        let reference = group.batches()[0]
            .record_reference(receipt.address(), 0)
            .unwrap();
        write(&extent, 0, &frame[..100]);
        log.self_adopt_live_extent().unwrap();
        assert_eq!(log.receipt_count(), 0);
        assert!(log.state.lock().poison.is_none());
        write(&extent, 0, &frame);
        if tier == DurabilityTier::Synchronous {
            assert!(matches!(
                log.read_record(&reference),
                Err(StorageError::NeedsPersistence)
            ));
        } else {
            assert_eq!(
                log.read_record(&reference).unwrap(),
                batch(&config, 0).mutations()[0]
            );
        }
        assert_eq!(frontier.appended(), 4096);
        assert_eq!(log.synced_frontier(), 0);
        assert!(matches!(
            log.checkpoint_prefix(1),
            Err(StorageError::NeedsPersistence)
        ));
        let (_, frame) = encoded(&config, vec![batch(&config, 1)]);
        write(&extent, 4096, &frame);
        // Flush discovers complete frames which no explicit adoption reported.
        assert_eq!(log.persist_appended().unwrap(), 8192);
        assert_eq!(log.receipt_count(), 2);
        log.checkpoint_prefix(2).unwrap();
        assert_eq!(
            log.read_record(&reference).unwrap(),
            batch(&config, 0).mutations()[0]
        );
    }
}

#[test]
fn leased_mode_excludes_reservations_and_rpc_append() {
    let (_temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (group, _) = encoded(&config, vec![batch(&config, 0)]);
    let (descriptor, _) = LiveGroupDescriptor::from_group(&group).unwrap();
    assert!(matches!(
        log.reserve_live_group(authority(&config), descriptor.clone()),
        Err(StorageError::Invalid(_))
    ));
    assert!(matches!(
        log.append_group(authority(&config), &group),
        Err(StorageError::Invalid(_))
    ));
    skip(&extent, 0);
    log.adopt_live_extent(authority(&config), extent.token, 0, true)
        .unwrap();
    log.reserve_live_group(authority(&config), descriptor)
        .unwrap();
    assert!(matches!(
        log.lease_live_extent(authority(&config)),
        Err(StorageError::Invalid(_))
    ));
}

#[test]
fn leased_sealed_recovery_accepts_gap_free_frames_and_stops_at_first_torn_frame() {
    for torn in [None, Some(0u64), Some(1), Some(2)] {
        let (temp, config, log) = fixture();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        for n in 0..3 {
            let (_, mut frame) = encoded(&config, vec![batch(&config, n)]);
            if torn == Some(n) {
                frame[100..].fill(0);
            }
            write(&extent, n * 4096, &frame);
        }
        drop(log);
        let recovered = PartitionLog::recover_live_sealed(
            &temp.path().join("stream"),
            &config,
            metrics(),
            || Ok(()),
        )
        .unwrap();
        let count = torn.unwrap_or(3) as usize;
        assert_eq!(recovered.receipt_count(), count);
        for n in 0..count {
            assert_eq!(
                recovered.receipt_page(n, 1).unwrap()[0].version().sequence,
                n as u64
            );
        }
    }
}

#[test]
fn leased_recovery_crosses_skip_and_retains_fatal_root_corruption() {
    for rooted_defect in [false, true] {
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
        let (_, frame) = encoded(&config, vec![batch(&config, 2)]);
        write(&second, second.start, &frame);
        if rooted_defect {
            log.persist_appended().unwrap();
            write(&first, 100, &[99]);
        }
        drop(log);
        let result = PartitionLog::recover_live_sealed(
            &temp.path().join("stream"),
            &config,
            metrics(),
            || Ok(()),
        );
        if rooted_defect {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap().receipt_count(), 3);
        }
    }
}

#[test]
fn leased_seal_adopts_and_persists_complete_frames_after_revocation() {
    let (temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, 0, &frame);
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
    assert_eq!(log.receipt_count(), 1);
    assert_eq!(log.synced_frontier(), 4096);
    drop(log);
    let recovered =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Ok(())
        })
        .unwrap();
    assert_eq!(recovered.receipt_count(), 1);
}

#[test]
fn leased_metadata_pressure_persists_spills_and_finishes_declared_range() {
    let temp = tempdir::TempDir::new("live-lease-pressure").unwrap();
    let mut config = live_config();
    config.max_batches = 2;
    config.fragment_bytes = 16 * 1024;
    config.wal_file_bytes = 32 * 1024;
    let log = PartitionLog::create_live(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    log.set_history_persistence(HistoryPersistence::OnDisk)
        .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    for n in 0..4 {
        let (_, frame) = encoded(&config, vec![batch(&config, n)]);
        write(&extent, n * 4096, &frame);
    }
    let adopted = log
        .adopt_live_extent(authority(&config), extent.token, extent.end, false)
        .unwrap()
        .0;
    assert_eq!(adopted.receipt_count, 4);
    assert_eq!(adopted.adopted_end, extent.end);
    assert!(log.synced_frontier() >= 8192);
    assert_eq!(log.receipt_count(), 4);
}

#[test]
fn leased_namespace_and_witness_are_durable_before_export_authority() {
    let (temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    // Inspection is the power-loss ordering oracle: a process crash alone
    // cannot simulate loss of an unsynced directory entry or file witness.
    assert_eq!(
        log.state.lock().namespace_synced_file.unwrap().as_u64(),
        extent.segment.file_index
    );
    assert_eq!(log.stats().namespace_syncs, 1);
    assert_eq!(log.stats().inventory_witnesses, 1);
    assert_eq!(log.synced_frontier(), 0);
    assert_eq!(log.receipt_count(), 0);
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    let file = OpenOptions::new()
        .write(true)
        .open(&extent.segment.path)
        .unwrap();
    file.write_all_at(&frame, extent.start % extent.segment.bytes)
        .unwrap();
    file.sync_data().unwrap();
    drop(file);
    drop(log);
    let path = temp.path().join("stream");
    let recovered =
        PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
    assert_eq!(recovered.receipt_count(), 1);
    assert!(matches!(
        recovered.lease_live_extent(authority(&config)),
        Err(StorageError::Sealed)
    ));
    assert!(PartitionLog::create_live(&path, config, &shape(), metrics()).is_err());
}

#[test]
fn leased_namespace_or_witness_failure_never_grants_an_extent() {
    for witness in [false, true] {
        let (_temp, config, log) = fixture();
        if witness {
            log.fail_next_inventory_witness(false);
        } else {
            log.fail_next_namespace_persist();
        }
        assert!(matches!(
            log.lease_live_extent(authority(&config)),
            Err(StorageError::Unknown(_))
        ));
        assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
        assert_eq!(log.receipt_count(), 0);
        assert!(matches!(
            log.lease_live_extent(authority(&config)),
            Err(StorageError::Poisoned)
        ));
    }
}

#[test]
fn leased_recovery_refuses_uncertain_revocation_before_opening_wal() {
    let (temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, extent.start, &frame);
    drop(log);
    let result =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Err(StorageError::Invalid("uncertain target drain"))
        });
    assert!(matches!(
        result,
        Err(StorageError::Invalid("uncertain target drain"))
    ));
    // A failed revocation did not adopt or seal any bytes.
    assert!(!temp.path().join("stream").join(SEAL).exists());
    let recovered =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Ok(())
        })
        .unwrap();
    assert_eq!(recovered.receipt_count(), 1);
}

#[test]
fn leased_namespace_persistence_exposes_no_transient_poison_and_preserves_terminal_poison() {
    for terminal in [false, true] {
        let (_temp, config, log) = fixture();
        let log = Arc::new(log);
        let (entered, resume) = log.pause_next_persist();
        let worker = log.clone();
        let lease = std::thread::spawn(move || worker.lease_live_extent(authority(&config)));
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        // The syncer validates state before acquiring persistence ownership.
        // A temporarily marked Fatal here would permanently fail its frontier.
        assert!(log.state.lock().poison.is_none());
        assert_eq!(log.persist_appended().unwrap(), 0);
        if terminal {
            log.poison_writer();
        }
        resume.send(()).unwrap();
        let result = lease.join().unwrap();
        if terminal {
            assert!(matches!(result, Err(StorageError::Poisoned)));
            assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
        } else {
            assert!(result.is_ok());
            assert!(log.state.lock().poison.is_none());
        }
    }
}

#[test]
fn leased_large_fragment_skip_does_not_charge_an_unadoptable_gap_as_metadata_pressure() {
    for rooted in [false, true] {
        let temp = tempdir::TempDir::new("live-lease-large-skip").unwrap();
        let mut config = live_config();
        config.fragment_bytes = 128 * 1024 * 1024;
        config.wal_file_bytes = 2 * config.fragment_bytes;
        let path = temp.path().join("stream");
        let log = PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
        let first = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
        write(&first, first.start, &frame);
        skip(&first, first.start + 4096);
        log.adopt_live_extent(authority(&config), first.token, first.start + 4096, false)
            .unwrap();
        if rooted {
            log.persist_appended().unwrap();
            log.state
                .lock()
                .references
                .start_background(4096, 1, &path)
                .unwrap();
            log.wait_history().unwrap();
            assert_eq!(log.references.root().end, 4096);
        }
        log.adopt_live_extent(authority(&config), first.token, first.start + 4096, true)
            .unwrap();
        let next = log.lease_live_extent(authority(&config)).unwrap().0;
        assert_eq!(next.start, first.end);
        assert_eq!(next.start, 128 * 1024 * 1024);
        for n in 1..=2 {
            let (_, frame) = encoded(&config, vec![batch(&config, n)]);
            write(&next, next.start + (n - 1) * 4096, &frame);
            let adoption = log
                .adopt_live_extent(authority(&config), next.token, next.start + n * 4096, false)
                .unwrap()
                .0;
            assert_eq!(adoption.adopted_end, next.start + n * 4096);
            assert_eq!(adoption.receipt_count, n);
            assert!(log.state.lock().poison.is_none());
        }
        // Once the bridging frame exists, subsequent pressure can root it.
        assert_eq!(log.receipt_count(), 3);
        log.persist_appended().unwrap();
        drop(log);
        let recovered =
            PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
        assert_eq!(recovered.receipt_count(), 3);
    }
}

#[test]
fn leased_header_probe_changes_never_publish_a_different_tail_crc() {
    for declared in [false, true] {
        let (temp, config, log) = fixture();
        let log = Arc::new(log);
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
        let mut unfinished = frame.clone();
        unfinished[4] ^= 1;
        write(&extent, 0, &unfinished);
        let (entered, resume) = log.pause_live(LivePausePoint::FrameProbe);
        let worker = log.clone();
        let token = extent.token;
        let auth = authority(&config);
        let scan = std::thread::spawn(move || {
            if declared {
                worker
                    .adopt_live_extent(auth, token, 4096, false)
                    .map(|_| ())
            } else {
                worker.self_adopt_live_extent()
            }
        });
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        write(&extent, 0, &frame);
        resume.send(()).unwrap();
        let result = scan.join().unwrap();
        assert_eq!(log.receipt_count(), 0);
        assert_eq!(log.appended_frontier(), 0);
        if declared {
            assert!(matches!(result, Err(StorageError::IncompleteLiveFrame)));
        } else {
            result.unwrap();
            assert!(log.state.lock().poison.is_none());
            log.self_adopt_live_extent().unwrap();
            log.persist_appended().unwrap();
            let tail = log.state.lock().live_tail.unwrap();
            assert_eq!(
                tail.crc,
                u32::from_be_bytes(frame[4..8].try_into().unwrap())
            );
        }
        drop(log);
        let recovered = PartitionLog::recover_live_sealed(
            &temp.path().join("stream"),
            &config,
            metrics(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(recovered.receipt_count(), 1);
    }
}

#[test]
fn leased_stale_opportunistic_scan_tolerates_close_and_two_rollovers() {
    for rollovers in 0..=2 {
        let (_temp, config, log) = fixture();
        let log = Arc::new(log);
        let frontier = log.attach_frontier(&DurabilityConfig::default()).unwrap();
        let first = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
        write(&first, 0, &frame);
        log.adopt_live_extent(authority(&config), first.token, 4096, false)
            .unwrap();
        skip(&first, 4096);
        let (entered, resume) = log.pause_live(LivePausePoint::ScanSnapshot);
        let worker = log.clone();
        let scan = std::thread::spawn(move || worker.sync_for_frontier());
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        log.adopt_live_extent(authority(&config), first.token, 4096, true)
            .unwrap();
        for _ in 0..rollovers {
            let next = log.lease_live_extent(authority(&config)).unwrap().0;
            skip(&next, next.start);
            log.adopt_live_extent(authority(&config), next.token, next.start, true)
                .unwrap();
        }
        if rollovers != 0 {
            log.lease_live_extent(authority(&config)).unwrap();
        }
        resume.send(()).unwrap();
        scan.join().unwrap().unwrap();
        assert!(frontier.request_sync().is_ok());
        assert!(log.state.lock().poison.is_none());
        assert_eq!(log.synced_frontier(), 4096);
    }
}

#[test]
fn leased_whole_batch_reads_keep_the_synchronous_owner_barrier() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let (_temp, config, log) = fixture();
        log.attach_frontier(&DurabilityConfig::with_tier(tier))
            .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (group, frame) = encoded(&config, vec![batch(&config, 0)]);
        let receipt = expected(&config, &group, 0, frame.len())[0];
        write(&extent, 0, &frame);
        if tier == DurabilityTier::Synchronous {
            assert!(matches!(
                log.read_batch(receipt),
                Err(StorageError::NeedsPersistence)
            ));
            assert_eq!(log.synced_frontier(), 0);
            assert_eq!(log.receipt_count(), 1);
            assert!(matches!(
                log.read_batch(receipt),
                Err(StorageError::NeedsPersistence)
            ));
        } else {
            assert_eq!(log.read_batch(receipt).unwrap().id(), receipt.id());
        }
        log.persist_appended().unwrap();
        assert_eq!(log.read_batch(receipt).unwrap().id(), receipt.id());
    }
}

#[test]
fn leased_large_close_releases_state_and_excludes_other_extent_mutations() {
    let temp = tempdir::TempDir::new("live-close-unlocked").unwrap();
    let mut config = live_config();
    config.fragment_bytes = 128 * 1024 * 1024;
    config.wal_file_bytes = 2 * config.fragment_bytes;
    let log = Arc::new(
        PartitionLog::create_live(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, 0, &frame);
    skip(&extent, 4096);
    log.adopt_live_extent(authority(&config), extent.token, 4096, false)
        .unwrap();
    let (entered, resume) = log.pause_live(LivePausePoint::CloseSuffix);
    let worker = log.clone();
    let auth = authority(&config);
    let token = extent.token;
    let close = std::thread::spawn(move || worker.adopt_live_extent(auth, token, 4096, true));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // Capacity one; the stats reader owns the send and closes it on exit.
    let (sent, received) = std::sync::mpsc::sync_channel(1);
    let reader = log.clone();
    let stats = std::thread::spawn(move || sent.send(reader.stats().appended_groups).unwrap());
    assert_eq!(received.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
    stats.join().unwrap();
    assert!(matches!(
        log.lease_live_extent(authority(&config)),
        Err(StorageError::Invalid(_))
    ));
    assert!(matches!(
        log.adopt_live_extent(authority(&config), token, 8192, false),
        Err(StorageError::Invalid(_))
    ));
    assert!(matches!(
        log.adopt_live_extent(authority(&config), token, 4096, true),
        Err(StorageError::Invalid(_))
    ));
    log.self_adopt_live_extent().unwrap();
    assert!(log.state.lock().poison.is_none());
    resume.send(()).unwrap();
    assert!(close.join().unwrap().unwrap().0.closed);
    assert_eq!(
        log.lease_live_extent(authority(&config)).unwrap().0.start,
        extent.end
    );
}

#[test]
fn leased_close_rejects_a_valid_unadopted_frame_at_the_marker_cursor() {
    let (_temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    for n in 0..2 {
        let (_, frame) = encoded(&config, vec![batch(&config, n)]);
        write(&extent, n * 4096, &frame);
    }
    assert!(matches!(
        log.adopt_live_extent(authority(&config), extent.token, 4096, true),
        Err(StorageError::IncompleteLiveFrame)
    ));
    assert_eq!(log.receipt_count(), 1);
    assert_eq!(log.persist_appended().unwrap(), 4096);
}

#[test]
fn leased_crash_after_reference_publication_recovers_the_same_complete_prefix() {
    let (temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (group, frame) = encoded(&config, vec![batch(&config, 0)]);
    let receipts = expected(&config, &group, 0, frame.len());
    write(&extent, 0, &frame);
    // Release publishes the one-shot crash injection to the adopting thread;
    // its AcqRel swap consumes this flag once before frontier publication.
    log.fail_after_live_publish.store(true, Ordering::Release);
    assert!(matches!(
        log.adopt_live_extent(authority(&config), extent.token, 4096, false),
        Err(StorageError::Unknown(_))
    ));
    assert_eq!(log.receipt_count(), 1);
    assert_eq!(log.appended_frontier(), 0);
    drop(log);
    let recovered =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Ok(())
        })
        .unwrap();
    assert_eq!(recovered.receipt_page(0, 1).unwrap(), receipts);
}

#[test]
fn leased_concurrent_reader_and_control_match_independent_canonical_receipt_digest() {
    use bincode::Options;
    use blake2::{Blake2b, Digest, digest::consts::U32};
    for _ in 0..8 {
        let (_temp, config, log) = fixture();
        let log = Arc::new(log);
        log.attach_frontier(&DurabilityConfig::with_tier(DurabilityTier::Native))
            .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let mut receipts = Vec::new();
        let mut reference = None;
        for n in 0..3 {
            let (group, frame) = encoded(&config, vec![batch(&config, n)]);
            write(&extent, n * 4096, &frame);
            let receipt = expected(&config, &group, n * 4096, frame.len())[0];
            reference = Some(
                group.batches()[0]
                    .record_reference(receipt.address(), 0)
                    .unwrap(),
            );
            receipts.push(receipt);
        }
        let mut independent = [0; 32];
        for receipt in &receipts {
            // Independent serializer, not the production digest field arithmetic.
            let encoding = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_little_endian()
                .serialize(receipt)
                .unwrap();
            let mut hash = Blake2b::<U32>::new();
            hash.update(b"tidehunter-live-extent-receipt-v1");
            hash.update(independent);
            hash.update(encoding);
            independent = hash.finalize().into();
        }
        let barrier = std::sync::Barrier::new(3);
        std::thread::scope(|scope| {
            let read = scope.spawn(|| {
                barrier.wait();
                log.read_record(&reference.unwrap())
            });
            let adopt = scope.spawn(|| {
                barrier.wait();
                log.adopt_live_extent(authority(&config), extent.token, 12288, false)
            });
            barrier.wait();
            read.join().unwrap().unwrap();
            let adoption = adopt.join().unwrap().unwrap().0;
            assert_eq!(adoption.receipt_count, 3);
            assert_eq!(adoption.receipt_digest, independent);
        });
        assert_eq!(log.receipt_count(), 3);
        assert_eq!(log.receipt_page(0, 3).unwrap(), receipts);
    }
}

#[test]
fn leased_seal_flush_and_checkpoint_scan_past_a_concurrent_adopter() {
    for operation in 0..3 {
        let (temp, config, log) = fixture();
        let log = Arc::new(log);
        log.attach_frontier(&DurabilityConfig::with_tier(DurabilityTier::Native))
            .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let mut reference = None;
        for n in 0..2 {
            let (group, frame) = encoded(&config, vec![batch(&config, n)]);
            write(&extent, n * 4096, &frame);
            let receipt = expected(&config, &group, n * 4096, frame.len())[0];
            reference = Some(
                group.batches()[0]
                    .record_reference(receipt.address(), 0)
                    .unwrap(),
            );
        }
        let (snapshot, resume_snapshot) = log.pause_live(LivePausePoint::ScanSnapshot);
        let target = log.clone();
        let auth = authority(&config);
        let complete = std::thread::spawn(move || match operation {
            0 => target.seal_live_epoch(auth, || Ok(())),
            1 => target.persist_appended().map(|_| ()),
            _ => target.checkpoint_prefix(2).map(|_| ()),
        });
        snapshot.recv_timeout(Duration::from_secs(5)).unwrap();
        let (between, resume_reader) = log.pause_live(LivePausePoint::AfterFrame);
        let reader = log.clone();
        let read = std::thread::spawn(move || reader.read_record(&reference.unwrap()));
        between.recv_timeout(Duration::from_secs(5)).unwrap();
        // The hook runs after the completed round releases its gate.
        assert_eq!(log.appended_frontier(), 8192);
        resume_snapshot.send(()).unwrap();
        let result = complete.join().unwrap();
        if operation == 2 {
            assert!(matches!(result, Err(StorageError::NeedsPersistence)));
        } else {
            result.unwrap();
        }
        assert_eq!(log.receipt_count(), 2);
        assert_eq!(log.appended_frontier(), 8192);
        if operation == 2 {
            log.persist_appended().unwrap();
        }
        assert_eq!(log.synced_frontier(), 8192);
        log.checkpoint_prefix(2).unwrap();
        resume_reader.send(()).unwrap();
        read.join().unwrap().unwrap();
        drop(log);
        let recovered = PartitionLog::recover_live_sealed(
            &temp.path().join("stream"),
            &config,
            metrics(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(recovered.receipt_count(), 2);
    }
}

#[test]
fn leased_seal_refuses_an_unlocked_close_without_poisoning_it() {
    let (_temp, config, log) = fixture();
    let log = Arc::new(log);
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, 0, &frame);
    skip(&extent, 4096);
    let (entered, resume) = log.pause_live(LivePausePoint::CloseSuffix);
    let closer = log.clone();
    let auth = authority(&config);
    let token = extent.token;
    let close = std::thread::spawn(move || closer.adopt_live_extent(auth, token, 4096, true));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        log.seal_live_epoch(authority(&config), || {
            panic!("revocation must not run while close owns the extent")
        }),
        Err(StorageError::Invalid("live extent is closing"))
    ));
    assert!(log.state.lock().poison.is_none());
    assert!(!log.state.lock().sealing);
    resume.send(()).unwrap();
    assert!(close.join().unwrap().unwrap().0.closed);
    log.persist_appended().unwrap();
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
}

#[test]
fn leased_pressure_rechecks_capacity_after_an_independent_history_drain() {
    let temp = tempdir::TempDir::new("live-lease-history-race").unwrap();
    let mut config = live_config();
    config.max_batches = 1;
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
    log.set_history_persistence(HistoryPersistence::OnDisk)
        .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, first) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, 0, &first);
    log.adopt_live_extent(authority(&config), extent.token, 4096, false)
        .unwrap();
    let (history_entered, history_resume) = log.references.pause_next_history();
    log.persist_appended().unwrap();
    history_entered
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    let (background, _) = log.state.lock().references.background_handle();
    let (_, second) = encoded(&config, vec![batch(&config, 1)]);
    write(&extent, 4096, &second);
    let (pressure, pressure_resume) = log.pause_live(LivePausePoint::PressureObserved);
    let adopter = log.clone();
    let auth = authority(&config);
    let token = extent.token;
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, 8192, false));
    pressure.recv_timeout(Duration::from_secs(5)).unwrap();
    history_resume.send(()).unwrap();
    background.lock().take().unwrap().join().unwrap();
    assert_eq!(log.references.root().end, 4096);
    pressure_resume.send(()).unwrap();
    let reply = adopt.join().unwrap().unwrap().0;
    assert_eq!(reply.receipt_count, 2);
    assert_eq!(reply.adopted_end, 8192);
    assert!(log.state.lock().poison.is_none());
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

#[test]
fn leased_split_block_reads_keep_a_high_water_buffer_and_exact_receipts() {
    let (_temp, config, log) = split_block_fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let mut end = extent.start;
    let mut high_water = 0;
    let mut digest = [0; 32];
    let mut receipts = Vec::new();
    for (sequence, size) in [5000, 64, 14000, 64, 5000].into_iter().enumerate() {
        let (group, frame) = encoded(&config, vec![sized_batch(&config, sequence as u64, size)]);
        let receipt = expected(&config, &group, end, frame.len())[0];
        write(&extent, end, &frame);
        end += frame.len() as u64;
        high_water = high_water.max(frame.len());
        digest = extend_live_receipt_digest(digest, receipt);
        receipts.push(receipt);
        let adopted = log
            .adopt_live_extent(authority(&config), extent.token, end, false)
            .unwrap()
            .0;
        assert_eq!(adopted.receipt_count, sequence as u64 + 1);
        assert_eq!(adopted.receipt_digest, digest);
        assert_eq!(adopted.adopted_end, end);
        let gate = {
            let state = log.state.lock();
            Arc::clone(&state.open_live_extent.as_ref().unwrap().round)
        };
        assert_eq!(gate.lock().buffers[0].len(), high_water);
    }
    assert_eq!(log.receipt_page(0, receipts.len()).unwrap(), receipts);
}

#[test]
fn leased_split_block_reads_reject_short_declarations_and_corrupt_suffixes() {
    for defect in ["short", "torn", "value", "padding"] {
        let (_temp, config, log) = split_block_fixture();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (first, prefix) = encoded(&config, vec![sized_batch(&config, 0, 64)]);
        write(&extent, extent.start, &prefix);
        let (_, mut suffix) = encoded(&config, vec![sized_batch(&config, 1, 5000)]);
        assert!(suffix.len() > 4096);
        let end = prefix.len() as u64 + suffix.len() as u64;
        match defect {
            "value" => suffix[4096] ^= 1,
            "padding" => *suffix.last_mut().unwrap() ^= 1,
            _ => {}
        }
        write(
            &extent,
            prefix.len() as u64,
            if defect == "torn" {
                &suffix[..4096]
            } else {
                &suffix
            },
        );
        let through = if defect == "short" { end - 4096 } else { end };
        assert!(
            matches!(
                log.adopt_live_extent(authority(&config), extent.token, through, false),
                Err(StorageError::IncompleteLiveFrame),
            ),
            "{defect}"
        );
        assert_eq!(log.appended_frontier(), prefix.len() as u64, "{defect}");
        assert_eq!(
            log.receipt_page(0, 1).unwrap(),
            expected(&config, &first, extent.start, prefix.len()),
            "{defect}",
        );
        assert_eq!(
            log.state
                .lock()
                .open_live_extent
                .as_ref()
                .unwrap()
                .adoption
                .receipt_count,
            1,
        );
    }
}

#[test]
fn prelease_requires_open_extent_and_is_single() {
    let (_temp, config, log) = fixture();
    assert!(matches!(
        log.prelease_live_extent(authority(&config), [1; 16]),
        Err(StorageError::Invalid(
            "prelease requires the open live extent"
        ))
    ));
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    assert!(matches!(
        log.prelease_live_extent(authority(&config), [1; 16]),
        Err(StorageError::Invalid(
            "prelease requires the open live extent"
        ))
    ));
    let next = log
        .prelease_live_extent(authority(&config), first.token)
        .unwrap()
        .0;
    assert_eq!(next.start, first.end);
    assert!(matches!(
        log.prelease_live_extent(authority(&config), first.token),
        Err(StorageError::Invalid("live extent already preleased"))
    ));
    assert!(
        !log.can_release_live_segment(next.segment.file_index)
            .unwrap()
    );
}

#[test]
fn close_promotes_preleased_extent() {
    let (_temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    let next = log
        .prelease_live_extent(authority(&config), first.token)
        .unwrap()
        .0;
    skip(&first, first.start);
    log.adopt_live_extent(authority(&config), first.token, first.start, true)
        .unwrap();
    let state = log.state.lock();
    assert_eq!(
        state.open_live_extent.as_ref().unwrap().extent.token,
        next.token
    );
    assert!(state.next_live_extent.is_none());
}

#[test]
fn leased_recovery_with_empty_preleased_extent_keeps_all_frames() {
    let (temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&first, first.start, &frame);
    log.persist_appended().unwrap();
    log.prelease_live_extent(authority(&config), first.token)
        .unwrap();
    drop(log);
    let recovered =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Ok(())
        })
        .unwrap();
    assert_eq!(recovered.receipt_count(), 1);
    assert_eq!(recovered.appended_frontier(), 4096);
    let receipt = recovered.receipt_page(0, 1).unwrap()[0];
    assert_eq!(
        recovered.read_batch(receipt).unwrap().mutations(),
        batch(&config, 0).mutations()
    );
}

#[test]
fn leased_recovery_after_crash_between_skip_marker_and_first_frame_of_next_extent() {
    for next_written in [false, true] {
        let (temp, config, log) = fixture();
        let first = log.lease_live_extent(authority(&config)).unwrap().0;
        let next = log
            .prelease_live_extent(authority(&config), first.token)
            .unwrap()
            .0;
        let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
        write(&first, first.start, &frame);
        log.persist_appended().unwrap();
        skip(&first, first.start + 4096);
        if next_written {
            let (_, frame) = encoded(&config, vec![batch(&config, 1)]);
            write(&next, next.start, &frame);
        }
        // No close metadata is needed: these are also the bytes at a crash
        // during the background close, before it publishes the next extent.
        drop(log);
        let recovered = PartitionLog::recover_live_sealed(
            &temp.path().join("stream"),
            &config,
            metrics(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(recovered.receipt_count(), if next_written { 2 } else { 1 });
        for (sequence, receipt) in recovered
            .receipt_page(0, 2)
            .unwrap()
            .into_iter()
            .enumerate()
        {
            assert_eq!(
                recovered.read_batch(receipt).unwrap().mutations(),
                batch(&config, sequence as u64).mutations()
            );
        }
    }
}

#[test]
fn seal_with_open_and_preleased_extents_adopts_across_the_skip_marker() {
    let (temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    let next = log
        .prelease_live_extent(authority(&config), first.token)
        .unwrap()
        .0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&first, first.start, &frame);
    skip(&first, first.start + 4096);
    let (_, frame) = encoded(&config, vec![batch(&config, 1)]);
    write(&next, next.start, &frame);
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
    assert_eq!(log.receipt_count(), 2);
    assert_eq!(log.synced_frontier(), next.start + 4096);
    assert!(log.state.lock().next_live_extent.is_none());
    drop(log);
    let recovered =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Ok(())
        })
        .unwrap();
    assert_eq!(recovered.receipt_count(), 2);
}

#[test]
fn seal_rejects_live_frames_beyond_a_torn_extent_tail() {
    let (_temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    let next = log
        .prelease_live_extent(authority(&config), first.token)
        .unwrap()
        .0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&first, first.start, &frame);
    // No valid skip bridges the unwritten old tail into the next frame.
    let (_, frame) = encoded(&config, vec![batch(&config, 1)]);
    write(&next, next.start, &frame);
    assert!(matches!(
        log.seal_live_epoch(authority(&config), || Ok(())),
        Err(StorageError::Invalid(
            "live frames beyond a torn extent tail"
        ))
    ));
    assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
}

#[test]
fn owner_read_of_preleased_extent_frame_while_old_extent_closes() {
    for mode in [HistoryPersistence::OnDisk, HistoryPersistence::WalReplay] {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let (_temp, config, log) = fixture();
            log.set_history_persistence(mode).unwrap();
            log.attach_frontier(&DurabilityConfig::with_tier(tier))
                .unwrap();
            let log = Arc::new(log);
            let first = log.lease_live_extent(authority(&config)).unwrap().0;
            let next = log
                .prelease_live_extent(authority(&config), first.token)
                .unwrap()
                .0;
            let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
            write(&first, first.start, &frame);
            skip(&first, first.start + frame.len() as u64);
            let (group, frame) = encoded(&config, vec![batch(&config, 1)]);
            write(&next, next.start, &frame);
            let receipt = expected(&config, &group, next.start, frame.len())[0];
            let reference = group.batches()[0]
                .record_reference(receipt.address(), 0)
                .unwrap();
            let (entered, resume) = log.pause_live(LivePausePoint::CloseSuffix);
            let closer = log.clone();
            let auth = authority(&config);
            let close = std::thread::spawn(move || {
                closer.adopt_live_extent(auth, first.token, first.start + 4096, true)
            });
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(log.state.lock().open_live_extent.as_ref().unwrap().closing);
            if tier == DurabilityTier::Native {
                let plan = log.plan_record_read(&reference).unwrap();
                let mut bytes = vec![0; plan.member.len];
                plan.member
                    .file
                    .read_exact_at(&mut bytes, plan.member.offset)
                    .unwrap();
                assert_eq!(
                    CompleteBatch::decode_record(&shape(), &reference, bytes.into()).unwrap(),
                    group.batches()[0].mutations()[0]
                );
                drop(plan);
                assert!(log.resolve_live_record(&reference).is_ok());
            } else {
                assert!(matches!(
                    log.plan_record_read(&reference),
                    Err(StorageError::NeedsPersistence)
                ));
                assert!(matches!(
                    log.resolve_live_record(&reference),
                    Err(StorageError::NeedsPersistence)
                ));
            }
            let mut invalid = reference;
            invalid.batch_address.len += 8;
            assert!(matches!(
                log.plan_record_read(&invalid),
                Err(StorageError::Invalid(
                    "WAL member differs from the reference"
                ))
            ));
            resume.send(()).unwrap();
            close.join().unwrap().unwrap();
            log.adopt_live_extent(authority(&config), next.token, next.start + 4096, false)
                .unwrap();
            log.persist_appended().unwrap();
            assert_eq!(
                log.read_record(&reference).unwrap(),
                group.batches()[0].mutations()[0]
            );
        }
    }
}

#[test]
fn position_crash_cuts_with_interleaved_adoption_recover_in_both_tiers() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        // Root write, root file sync, rename, before data, after data, after root.
        for phase in 1..=6 {
            let (temp, config, log) = fixture();
            let log = Arc::new(log);
            log.attach_frontier(&DurabilityConfig::with_tier(tier))
                .unwrap();
            let extent = log.lease_live_extent(authority(&config)).unwrap().0;
            let (first, bytes) = encoded(&config, vec![batch(&config, 0)]);
            write(&extent, 0, &bytes);
            log.self_adopt_live_extent().unwrap();
            let (second, bytes) = encoded(&config, vec![batch(&config, 1)]);
            write(&extent, 4096, &bytes);
            if tier == DurabilityTier::Synchronous {
                // The initiator's data-sync completion is independent of proof metadata.
                OpenOptions::new()
                    .write(true)
                    .open(&extent.segment.path)
                    .unwrap()
                    .sync_data()
                    .unwrap();
            }
            let (entered, release) = log.pause_live(LivePausePoint::PositionBeforeData);
            let proof_log = log.clone();
            let proof = std::thread::spawn(move || proof_log.persist_wal_through(4096));
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            // Adoption moves beyond the captured cut while the proof has no lock.
            let adopted = log.self_adopt_live_extent();
            log.fail_wal_root.store(phase, Ordering::Release);
            release.send(()).unwrap();
            adopted.unwrap();
            assert!(proof.join().unwrap().is_err(), "phase {phase}, tier {tier}");
            drop(log);
            let recovered = PartitionLog::recover_live_sealed(
                &temp.path().join("stream"),
                &config,
                metrics(),
                // This direct-file fixture has no exported writable target.
                || Ok(()),
            )
            .unwrap();
            for (n, group, offset) in [(0, first, 0), (1, second, 4096)] {
                let receipt = expected(&config, &group, offset, 4096)[0];
                let reference = group.batches()[0]
                    .record_reference(receipt.address(), 0)
                    .unwrap();
                assert_eq!(
                    recovered.read_record(&reference).unwrap(),
                    batch(&config, n).mutations()[0]
                );
            }
        }
    }
}

#[test]
fn position_proof_covers_completed_leased_bytes_before_owner_adoption() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        for read_kind in 0..3 {
            let (_temp, config, log) = fixture();
            let log = Arc::new(log);
            log.attach_frontier(&DurabilityConfig::with_tier(tier))
                .unwrap();
            let extent = log.lease_live_extent(authority(&config)).unwrap().0;
            let (group, bytes) = encoded(&config, vec![batch(&config, 0)]);
            write(&extent, 0, &bytes);
            let through = bytes.len() as u64;
            if tier == DurabilityTier::Synchronous {
                OpenOptions::new()
                    .write(true)
                    .open(&extent.segment.path)
                    .unwrap()
                    .sync_data()
                    .unwrap();
            }
            assert!(
                through > log.appended_frontier(),
                "the requested leased cut must exceed the owner's adopted frontier"
            );
            assert!(log.persist_wal_through(through).unwrap() >= through);
            assert_eq!(log.position_frontier_for_test(), through);
            assert!(
                log.appended_frontier() < through,
                "proof must not require adoption"
            );
            assert!(log.synced_frontier() < through);
            assert_eq!(log.receipt_count(), 0);
            let receipt = expected(&config, &group, extent.start, bytes.len())[0];
            let reference = group.batches()[0]
                .record_reference(receipt.address(), 0)
                .unwrap();
            match read_kind {
                0 => assert_eq!(
                    log.read_record(&reference).unwrap(),
                    batch(&config, 0).mutations()[0]
                ),
                1 => assert_eq!(
                    log.read_batch(receipt).unwrap().mutations(),
                    batch(&config, 0).mutations()
                ),
                _ => assert_eq!(
                    log.resolve_live_batch(receipt).unwrap().len,
                    receipt.address().len
                ),
            }
            // Existing lookup still discovers and validates/adopts the frame;
            // the durable position root is not a substitute for that authority.
            assert_eq!(log.receipt_count(), 1);
            if tier == DurabilityTier::Synchronous {
                assert!(
                    log.synced_frontier() >= through,
                    "the read must obtain its own receipt barrier"
                );
            }
        }
    }
}

#[test]
fn position_root_does_not_authorize_an_above_cut_read() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let (_temp, config, log) = fixture();
        let log = Arc::new(log);
        log.attach_frontier(&DurabilityConfig::with_tier(tier))
            .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, first) = encoded(&config, vec![batch(&config, 0)]);
        write(&extent, 0, &first);
        let cut = first.len() as u64;
        assert_eq!(log.persist_wal_through(cut).unwrap(), cut);
        let (second, bytes) = encoded(&config, vec![batch(&config, 1)]);
        write(&extent, cut, &bytes);
        let receipt = expected(&config, &second, cut, bytes.len())[0];
        assert!(receipt.address().offset + u64::from(receipt.address().len) > cut);
        let reference = second.batches()[0]
            .record_reference(receipt.address(), 0)
            .unwrap();
        let record_read = log.read_record(&reference);
        let profiled_read = log.read_record_profiled(&reference);
        let batch_read = log.read_batch(receipt);
        let lease_read = log.resolve_live_batch(receipt);
        if tier == DurabilityTier::Synchronous {
            assert!(matches!(batch_read, Err(StorageError::NeedsPersistence)));
            assert!(matches!(lease_read, Err(StorageError::NeedsPersistence)));
            assert!(matches!(record_read, Err(StorageError::NeedsPersistence)));
            assert!(matches!(profiled_read, Err(StorageError::NeedsPersistence)));
        } else {
            // Native already permits complete, validated page-cache reads.
            assert_eq!(
                batch_read.unwrap().mutations(),
                batch(&config, 1).mutations()
            );
            assert_eq!(lease_read.unwrap().len, receipt.address().len);
            assert_eq!(record_read.unwrap(), batch(&config, 1).mutations()[0]);
            assert_eq!(profiled_read.unwrap().0, batch(&config, 1).mutations()[0]);
        }
        assert_eq!(
            log.receipt_count(),
            2,
            "existing lookup must adopt/validate"
        );
        assert_eq!(log.position_frontier_for_test(), cut);
        assert_eq!(log.synced_frontier(), 0);
    }
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

#[test]
fn leased_prelease_completes_and_opportunistic_scan_waits_during_validation() {
    let (_temp, config, log) = parallel_adoption_fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![sized_batch(&config, 0, 600 * 1024)]);
    write(&extent, 0, &frame);
    write(
        &extent,
        frame.len() as u64,
        &encoded(&config, vec![sized_batch(&config, 1, 600 * 1024)]).1,
    );
    let through = 2 * frame.len() as u64;
    let (entered, resume) = log.pause_live(LivePausePoint::ValidateUnlocked);
    let adopter = log.clone();
    let auth = authority(&config);
    let token = extent.token;
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, through, false));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // Capacity one, one result per worker; dropping the receiver ends the send.
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let grant_log = log.clone();
    let grant = std::thread::spawn(move || {
        tx.send(grant_log.prelease_live_extent(auth, token))
            .unwrap();
    });
    let granted = rx.recv_timeout(Duration::from_secs(5));
    if granted.is_err() {
        resume.send(()).unwrap();
        grant.join().unwrap();
        adopt.join().unwrap().unwrap();
        panic!("prelease waited for unlocked adoption validation");
    }
    assert_eq!(granted.unwrap().unwrap().0.start, extent.end);
    grant.join().unwrap();
    let (snapshot, resume_snapshot) = log.pause_live(LivePausePoint::ScanSnapshot);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let scanner = log.clone();
    let scan = std::thread::spawn(move || {
        let result = scanner.self_adopt_live_extent();
        tx.send((result, scanner.receipt_count())).unwrap();
    });
    snapshot.recv_timeout(Duration::from_secs(5)).unwrap();
    resume_snapshot.send(()).unwrap();
    assert!(matches!(
        rx.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    resume.send(()).unwrap();
    let (result, count) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    result.unwrap();
    assert_eq!(count, 2);
    scan.join().unwrap();
    assert_eq!(adopt.join().unwrap().unwrap().0.receipt_count, 2);
}

#[test]
fn leased_parallel_publication_matches_frame_by_frame_authority() {
    let (_temp, config, log) = parallel_adoption_fixture();
    let other_temp = tempdir::TempDir::new("live-serial-adoption").unwrap();
    let serial = PartitionLog::create_live(
        &other_temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let other = serial.lease_live_extent(authority(&config)).unwrap().0;
    let mut end = 0;
    let mut receipts = Vec::new();
    let mut serial_summary = None;
    for sequence in 0..12 {
        let (group, frame) = encoded(&config, vec![sized_batch(&config, sequence, 64 * 1024)]);
        receipts.extend(expected(&config, &group, end, frame.len()));
        write(&extent, end, &frame);
        write(&other, end, &frame);
        end += frame.len() as u64;
        serial_summary = Some(
            serial
                .adopt_live_extent(authority(&config), other.token, end, false)
                .unwrap()
                .0,
        );
    }
    let adopted = log
        .adopt_live_extent(authority(&config), extent.token, end, false)
        .unwrap()
        .0;
    let serial_summary = serial_summary.unwrap();
    assert_eq!(adopted.adopted_end, serial_summary.adopted_end);
    assert_eq!(adopted.receipt_count, serial_summary.receipt_count);
    assert_eq!(adopted.receipt_digest, serial_summary.receipt_digest);
    assert_eq!(log.receipt_page(0, 16).unwrap(), receipts);
    assert_eq!(serial.receipt_page(0, 16).unwrap(), receipts);
    log.persist_many(&receipts).unwrap();
    serial.persist_many(&receipts).unwrap();
    for receipt in receipts {
        assert_eq!(
            log.read_batch(receipt).unwrap().mutations(),
            serial.read_batch(receipt).unwrap().mutations()
        );
        assert_eq!(
            log.record_reference(receipt, 0).unwrap(),
            serial.record_reference(receipt, 0).unwrap()
        );
    }
    let diagnostics = log.live_test_diagnostics().adoption;
    assert_eq!(diagnostics.frames, 12);
    assert_eq!(diagnostics.rounds, 1);
    assert_eq!(diagnostics.parallel_rounds, 1);
    assert_eq!(diagnostics.bytes, end);
    assert_eq!(serial.live_test_diagnostics().adoption.parallel_rounds, 0);
}

#[test]
fn leased_parallel_validation_publishes_only_the_prefix_before_corruption() {
    for digest_defect in [false, true] {
        for opportunistic in [false, true] {
            let (_temp, config, log) = parallel_adoption_fixture();
            let extent = log.lease_live_extent(authority(&config)).unwrap().0;
            let mut end = 0;
            let mut corrupt_start = 0;
            let mut prefix = Vec::new();
            // Equal frame sizes split after frame 5. Frame 7 fails in the
            // second worker's half, after all of the first half is valid.
            for sequence in 0..12 {
                let (group, mut frame) =
                    encoded(&config, vec![sized_batch(&config, sequence, 64 * 1024)]);
                if sequence < 7 {
                    prefix.extend(expected(&config, &group, end, frame.len()));
                }
                if sequence == 7 {
                    corrupt_start = end;
                    if digest_defect {
                        let member = 8 + group.batch_offsets()[0] as usize;
                        let len = u32::from_be_bytes(frame[member..member + 4].try_into().unwrap())
                            as usize;
                        // Change only the stored batch digest, then repair both
                        // enclosing CRCs so failure must reach digest validation.
                        frame[member + 8 + super::super::super::codec::HEADER_LEN - 1] ^= 1;
                        let crc = crc32fast::hash(&frame[member + 8..member + 8 + len]);
                        frame[member + 4..member + 8].copy_from_slice(&crc.to_be_bytes());
                        let crc = crc32fast::hash(&frame[8..]);
                        frame[4..8].copy_from_slice(&crc.to_be_bytes());
                    } else {
                        frame[4] ^= 1;
                    }
                }
                write(&extent, end, &frame);
                end += frame.len() as u64;
            }
            let result = if opportunistic {
                log.self_adopt_live_extent()
            } else {
                log.adopt_live_extent(authority(&config), extent.token, end, false)
                    .map(|_| ())
            };
            if opportunistic {
                result.unwrap();
                assert!(log.state.lock().poison.is_none());
            } else {
                assert!(matches!(result, Err(StorageError::IncompleteLiveFrame)));
                assert!(matches!(
                    log.state.lock().poison,
                    Some(WriterPoison::IncompleteLiveFrame)
                ));
            }
            assert_eq!(log.receipt_count(), 7);
            assert_eq!(log.appended_frontier(), corrupt_start);
            assert_eq!(
                log.state
                    .lock()
                    .open_live_extent
                    .as_ref()
                    .unwrap()
                    .adoption
                    .adopted_end,
                corrupt_start
            );
            assert_eq!(log.receipt_page(0, 16).unwrap(), prefix);
            let digest = prefix
                .iter()
                .copied()
                .fold([0; 32], extend_live_receipt_digest);
            assert_eq!(
                log.state
                    .lock()
                    .open_live_extent
                    .as_ref()
                    .unwrap()
                    .adoption
                    .receipt_digest,
                digest,
            );
            for sequence in 7..12 {
                assert!(log.lookup(batch(&config, sequence).id()).unwrap().is_none());
            }
            let diagnostics = log.live_test_diagnostics().adoption;
            assert_eq!(diagnostics.rounds, 1);
            assert_eq!(diagnostics.parallel_rounds, 1);
        }
    }
}

#[test]
fn leased_concurrent_declared_adopters_share_one_round_and_publish_once() {
    let (_temp, config, log) = parallel_adoption_fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let mut end = 0;
    for sequence in 0..4 {
        let (_, frame) = encoded(&config, vec![sized_batch(&config, sequence, 600 * 1024)]);
        write(&extent, end, &frame);
        end += frame.len() as u64;
    }
    let gate = Arc::clone(&log.state.lock().open_live_extent.as_ref().unwrap().round);
    let active = Arc::clone(&gate.lock().active);
    let maximum = Arc::clone(&gate.lock().maximum_active);
    let (entered, resume) = log.pause_live(LivePausePoint::ValidateUnlocked);
    let auth = authority(&config);
    let token = extent.token;
    let first_log = log.clone();
    let first = std::thread::spawn(move || first_log.adopt_live_extent(auth, token, end, false));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(active.load(Ordering::Relaxed), 1);
    assert!(gate.try_lock().is_none());
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let second_barrier = barrier.clone();
    let second_log = log.clone();
    let second = std::thread::spawn(move || {
        second_barrier.wait();
        second_log.adopt_live_extent(auth, token, end, false)
    });
    barrier.wait();
    resume.send(()).unwrap();
    assert_eq!(
        first.join().unwrap().unwrap().0,
        second.join().unwrap().unwrap().0
    );
    assert_eq!(maximum.load(Ordering::Relaxed), 1);
    assert_eq!(active.load(Ordering::Relaxed), 0);
    assert_eq!(log.receipt_count(), 4);
    assert_eq!(log.live_test_diagnostics().adoption.frames, 4);
}

#[test]
fn leased_close_during_validation_discards_the_stale_round() {
    let (_temp, config, log) = fixture();
    let log = Arc::new(log);
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, 0, &frame);
    skip(&extent, 4096);
    log.adopt_live_extent(authority(&config), extent.token, 4096, false)
        .unwrap();
    let (entered, resume) = log.pause_live(LivePausePoint::ValidatedBeforePublish);
    let adopter = log.clone();
    let token = extent.token;
    let auth = authority(&config);
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, 8192, false));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let (closing, close_resume) = log.pause_live(LivePausePoint::CloseSuffix);
    let closer = log.clone();
    let close = std::thread::spawn(move || {
        closer.close_live_extent(
            token,
            4096,
            &mut super::super::super::tcp::AdoptProfile::default(),
        )
    });
    closing.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        log.lease_live_extent(auth),
        Err(StorageError::Invalid(_))
    ));
    assert!(matches!(
        log.prelease_live_extent(auth, token),
        Err(StorageError::Invalid(_))
    ));
    close_resume.send(()).unwrap();
    assert!(close.join().unwrap().unwrap().closed);
    assert_eq!(log.lease_live_extent(auth).unwrap().0.start, extent.end);
    resume.send(()).unwrap();
    assert!(matches!(
        adopt.join().unwrap(),
        Err(StorageError::Invalid("closed live extent"))
    ));
    assert_eq!(log.receipt_count(), 1);
    assert!(log.state.lock().poison.is_none());
}

#[test]
fn leased_seal_during_validation_discards_the_stale_round() {
    let (_temp, config, log) = fixture();
    let log = Arc::new(log);
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    for sequence in 0..2 {
        write(
            &extent,
            sequence * 4096,
            &encoded(&config, vec![batch(&config, sequence)]).1,
        );
    }
    let (entered, resume) = log.pause_live(LivePausePoint::ValidatedBeforePublish);
    let adopter = log.clone();
    let token = extent.token;
    let auth = authority(&config);
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, 8192, false));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let (sealing, resume_seal) = log.pause_live(LivePausePoint::ScanSnapshot);
    let sealer = log.clone();
    let seal = std::thread::spawn(move || sealer.seal_live_epoch(auth, || Ok(())));
    sealing.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(log.state.lock().sealing);
    resume.send(()).unwrap();
    assert!(matches!(
        adopt.join().unwrap(),
        Err(StorageError::Invalid("live stream is sealing"))
    ));
    // The invalidated declared round publishes nothing. Seal then takes the
    // gate itself and adopts the complete immutable bytes under its old rules.
    assert_eq!(log.receipt_count(), 0);
    assert_eq!(log.live_test_diagnostics().adoption.frames, 0);
    resume_seal.send(()).unwrap();
    seal.join().unwrap().unwrap();
    assert_eq!(log.receipt_count(), 2);
    assert!(log.state.lock().sealed);
}

#[test]
fn leased_crash_after_validation_before_publication_recovers_acknowledged_bytes() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let (temp, config, log) = parallel_adoption_fixture();
        log.attach_frontier(&DurabilityConfig::with_tier(tier))
            .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let mut end = 0;
        let mut receipts = Vec::new();
        for sequence in 0..3 {
            let (group, frame) = encoded(&config, vec![sized_batch(&config, sequence, 600 * 1024)]);
            receipts.extend(expected(&config, &group, end, frame.len()));
            write(&extent, end, &frame);
            end += frame.len() as u64;
        }
        if tier == DurabilityTier::Synchronous {
            OpenOptions::new()
                .write(true)
                .open(&extent.segment.path)
                .unwrap()
                .sync_data()
                .unwrap();
        }
        let (entered, resume) = log.pause_live(LivePausePoint::ValidatedBeforePublish);
        let adopter = log.clone();
        let token = extent.token;
        let auth = authority(&config);
        let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, end, false));
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(log.receipt_count(), 0);
        let image = temp.path().join("crash-image");
        fs::create_dir(&image).unwrap();
        for entry in fs::read_dir(temp.path().join("stream")).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                fs::copy(entry.path(), image.join(entry.file_name())).unwrap();
            }
        }
        resume.send(()).unwrap();
        adopt.join().unwrap().unwrap();
        let recovered =
            PartitionLog::recover_live_sealed(&image, &config, metrics(), || Ok(())).unwrap();
        assert_eq!(recovered.receipt_page(0, 16).unwrap(), receipts);
        assert_eq!(recovered.appended_frontier(), end);
        for (sequence, receipt) in receipts.into_iter().enumerate() {
            assert_eq!(
                recovered.read_batch(receipt).unwrap().mutations(),
                sized_batch(&config, sequence as u64, 600 * 1024).mutations()
            );
        }
    }
}

#[test]
fn leased_parallel_round_restarts_after_history_pressure() {
    let mut config = live_config();
    config.batch_limits.max_encoded_bytes = 2 * 1024 * 1024;
    config.group_limits.max_encoded_bytes = 4 * 1024 * 1024 - 8;
    config.fragment_bytes = 8 * 1024 * 1024;
    config.wal_file_bytes = 2 * config.fragment_bytes;
    config.max_batches = 1;
    let temp = tempdir::TempDir::new("live-parallel-pressure").unwrap();
    let log = Arc::new(
        PartitionLog::create_live(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    log.set_history_persistence(HistoryPersistence::OnDisk)
        .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let mut end = 0;
    let mut receipts = Vec::new();
    for sequence in 0..4 {
        let (group, frame) = encoded(&config, vec![sized_batch(&config, sequence, 600 * 1024)]);
        receipts.extend(expected(&config, &group, end, frame.len()));
        write(&extent, end, &frame);
        end += frame.len() as u64;
    }
    let (pressure, resume_pressure) = log.pause_live(LivePausePoint::PressureObserved);
    let adopter = log.clone();
    let token = extent.token;
    let auth = authority(&config);
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, end, false));
    pressure.recv_timeout(Duration::from_secs(5)).unwrap();
    resume_pressure.send(()).unwrap();
    let adoption = adopt.join().unwrap().unwrap().0;
    assert_eq!(adoption.receipt_count, 4);
    assert_eq!(adoption.adopted_end, end);
    assert_eq!(log.receipt_page(0, 16).unwrap(), receipts);
    assert!(log.synced_frontier() >= 2 * (end / 4));
    assert!(log.live_test_diagnostics().adoption.parallel_rounds >= 1);
    assert!(log.state.lock().poison.is_none());
}

#[test]
fn leased_round_byte_cap_and_oversized_frame_bound_validation_work() {
    for (sizes, rounds, parallel_rounds) in [
        ([3 * 1024 * 1024, 3 * 1024 * 1024], 1, 1),
        ([6 * 1024 * 1024, 6 * 1024 * 1024], 2, 0),
        ([10 * 1024 * 1024, 600 * 1024], 2, 0),
    ] {
        let temp = tempdir::TempDir::new("live-round-byte-bound").unwrap();
        let mut config = live_config();
        config.batch_limits.max_encoded_bytes = 12 * 1024 * 1024;
        config.group_limits.max_encoded_bytes = 16 * 1024 * 1024 - 8;
        config.fragment_bytes = 32 * 1024 * 1024;
        config.wal_file_bytes = 2 * config.fragment_bytes;
        let log = PartitionLog::create_live(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let mut end = 0;
        for (sequence, size) in sizes.into_iter().enumerate() {
            let (_, frame) = encoded(&config, vec![sized_batch(&config, sequence as u64, size)]);
            write(&extent, end, &frame);
            end += frame.len() as u64;
        }
        let adoption = log
            .adopt_live_extent(authority(&config), extent.token, end, false)
            .unwrap()
            .0;
        assert_eq!(adoption.receipt_count, 2);
        assert_eq!(adoption.adopted_end, end);
        let diagnostics = log.live_test_diagnostics().adoption;
        assert_eq!(diagnostics.frames, 2);
        assert_eq!(diagnostics.rounds, rounds);
        assert_eq!(diagnostics.parallel_rounds, parallel_rounds);
    }
}

#[test]
fn leased_validation_keeps_abandoned_reservation_recovery_exclusive() {
    let (temp, config, log) = fixture();
    let log = Arc::new(log);
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, 0, &frame);
    let (entered, resume) = log.pause_live(LivePausePoint::ValidatedBeforePublish);
    let adopter = log.clone();
    let token = extent.token;
    let auth = authority(&config);
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, 4096, false));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || Ok(
            ()
        ),)
        .is_err()
    );
    assert_eq!(log.receipt_count(), 0);
    resume.send(()).unwrap();
    assert_eq!(adopt.join().unwrap().unwrap().0.receipt_count, 1);
    assert!(log.state.lock().poison.is_none());
}

#[test]
fn leased_read_and_checkpoint_wait_for_validated_prefix_publication() {
    let (_temp, config, log) = parallel_adoption_fixture();
    log.attach_frontier(&DurabilityConfig::with_tier(DurabilityTier::Native))
        .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let mut end = 0;
    let mut reference = None;
    for sequence in 0..2 {
        let (group, frame) = encoded(&config, vec![sized_batch(&config, sequence, 600 * 1024)]);
        let receipt = expected(&config, &group, end, frame.len())[0];
        reference = Some(
            group.batches()[0]
                .record_reference(receipt.address(), 0)
                .unwrap(),
        );
        write(&extent, end, &frame);
        end += frame.len() as u64;
    }
    let (entered, resume) = log.pause_live(LivePausePoint::ValidatedBeforePublish);
    let adopter = log.clone();
    let token = extent.token;
    let auth = authority(&config);
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, end, false));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // Each snapshot confirms its caller reached self-adoption before publish.
    let (read_snapshot, resume_read) = log.pause_live(LivePausePoint::ScanSnapshot);
    let reader = log.clone();
    let read = std::thread::spawn(move || reader.read_record(&reference.unwrap()));
    read_snapshot.recv_timeout(Duration::from_secs(5)).unwrap();
    let (checkpoint_snapshot, resume_checkpoint) = log.pause_live(LivePausePoint::ScanSnapshot);
    let checker = log.clone();
    let checkpoint = std::thread::spawn(move || checker.checkpoint_prefix(2));
    checkpoint_snapshot
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    resume_read.send(()).unwrap();
    resume_checkpoint.send(()).unwrap();
    resume.send(()).unwrap();
    assert_eq!(adopt.join().unwrap().unwrap().0.receipt_count, 2);
    assert_eq!(
        read.join().unwrap().unwrap(),
        sized_batch(&config, 1, 600 * 1024).mutations()[0]
    );
    assert!(matches!(
        checkpoint.join().unwrap(),
        Err(StorageError::NeedsPersistence)
    ));
    assert_eq!(log.receipt_count(), 2);
    assert_eq!(log.appended_frontier(), end);
    log.persist_appended().unwrap();
    log.checkpoint_prefix(2).unwrap();
}

#[test]
fn leased_seal_self_adoption_drains_history_pressure() {
    let temp = tempdir::TempDir::new("live-seal-adoption-pressure").unwrap();
    let mut config = live_config();
    config.max_batches = 1;
    config.fragment_bytes = 16 * 1024;
    config.wal_file_bytes = 2 * config.fragment_bytes;
    let log = Arc::new(
        PartitionLog::create_live(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    log.set_history_persistence(HistoryPersistence::OnDisk)
        .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    for sequence in 0..4 {
        write(
            &extent,
            sequence * 4096,
            &encoded(&config, vec![batch(&config, sequence)]).1,
        );
    }
    let (pressure, resume_pressure) = log.pause_live(LivePausePoint::PressureObserved);
    let sealer = log.clone();
    let auth = authority(&config);
    let seal = std::thread::spawn(move || sealer.seal_live_epoch(auth, || Ok(())));
    pressure.recv_timeout(Duration::from_secs(5)).unwrap();
    resume_pressure.send(()).unwrap();
    seal.join().unwrap().unwrap();
    assert_eq!(log.receipt_count(), 4);
    assert_eq!(log.appended_frontier(), extent.end);
    assert_eq!(log.synced_frontier(), extent.end);
    assert!(log.state.lock().sealed);
    assert!(log.state.lock().poison.is_none());
}

#[test]
fn leased_many_small_frames_respect_the_round_frame_budget() {
    let (_temp, config, log) = parallel_adoption_fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let count = ADOPTION_ROUND_FRAMES + 8;
    let mut end = 0;
    let mut receipts = Vec::new();
    for sequence in 0..count {
        let (group, frame) = encoded(
            &config,
            vec![sized_batch(&config, sequence as u64, 64 * 1024)],
        );
        receipts.extend(expected(&config, &group, end, frame.len()));
        write(&extent, end, &frame);
        end += frame.len() as u64;
    }
    assert!(end < ADOPTION_ROUND_BYTES);
    let adoption = log
        .adopt_live_extent(authority(&config), extent.token, end, false)
        .unwrap()
        .0;
    assert_eq!(adoption.adopted_end, end);
    assert_eq!(adoption.receipt_count, count as u64);
    assert_eq!(
        adoption.receipt_digest,
        receipts
            .iter()
            .copied()
            .fold([0; 32], extend_live_receipt_digest)
    );
    assert_eq!(log.receipt_page(0, count).unwrap(), receipts);
    let diagnostics = log.live_test_diagnostics().adoption;
    assert_eq!(diagnostics.frames, count as u64);
    assert_eq!(
        diagnostics.rounds,
        count.div_ceil(ADOPTION_ROUND_FRAMES) as u64
    );
    assert_eq!(diagnostics.parallel_rounds, 2);
}
