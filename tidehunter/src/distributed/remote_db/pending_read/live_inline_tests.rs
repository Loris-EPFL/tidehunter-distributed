use super::*;
use crate::distributed::remote_db::pipeline_tests::Fixture;
use crate::distributed::tcp::live_read::tests as local;
use crate::distributed::test_bounds::PROGRESS_CAP;
use std::io::{Seek, SeekFrom, Write};

/// One resume message for a worker parked outside both lane mutexes. Capacity
/// one accepts the sole drop-time send without waiting; a stopped worker drops
/// its receiver and makes the cleanup send fail harmlessly.
struct IdleLane(mpsc::SyncSender<()>);
impl Drop for IdleLane {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

struct Case {
    db: RemoteDatabase,
    reference: ValueRecordReference,
    node: usize,
    path: std::path::PathBuf,
    _fixture: Fixture,
}
impl Case {
    fn new() -> Self {
        Self::with_inline_reads(true)
    }
    fn with_inline_reads(inline_reads: bool) -> Self {
        let mut fixture = Fixture::new();
        fixture.config.read_lanes = Some(1);
        fixture.config.read_cache_bytes = 0;
        // The core's read service is immutable: configure the read service for
        // the inline policy at open. Read lanes keep the fixture's live
        // identity, so they can also read other owners' files.
        let mut db = RemoteDatabase::open_with_read_hook(
            &fixture.temp.path().join("authority"),
            &fixture.shape,
            fixture.config.clone(),
            |config, _clients| {
                config.live_nvme = crate::distributed::tcp::LiveClientConfig {
                    lease_ahead: true,
                    polled_writes: false,
                    // The fixture targets admit only this host (and writes
                    // already cached controllers under it).
                    host_nqn: super::super::pipeline_tests::LIVE_HOST_NQN.into(),
                    host_id: [1; 16],
                    owner_identities: Default::default(),
                    leased_write_window: Some(4),
                    leased_sync: Default::default(),
                    leased_flush_min_frame_bytes: 0,
                    max_inflight_requests: 4,
                    max_inflight_bytes: 128 * 1024,
                    connect_timeout_ms: 1000,
                    io_timeout_ms: 10_000,
                    leased_durability: crate::distributed::durability::DurabilityTier::Native,
                    inline_reads,
                    queue_geometry: Default::default(),
                };
            },
        )
        .unwrap();
        db.write_batch(fixture.operation(0, 0), vec![super::tests::put(0, 7)])
            .unwrap();
        db.write_batch(fixture.operation(0, 1), vec![super::tests::put(1, 8)])
            .unwrap();
        let reference = db
            .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
            .unwrap()
            .unwrap();
        let node = db.core.partition_indexes[&reference.batch_address.partition];
        let path = {
            let lanes = db.core.reads.lanes.read();
            let lane = &lanes[node][0];
            let mut driver = lane.driver.lock();
            local::install_owner_local(
                driver.live_cache.as_mut().unwrap(),
                &fixture.nodes[node],
                reference,
            )
        };
        db.set_read_profile_limit(128);
        Self {
            db,
            reference,
            node,
            path,
            _fixture: fixture,
        }
    }
    fn park_idle_lane(&self) -> IdleLane {
        // Admission resume alone promises neither unlocked worker startup nor
        // a warm grant. Wait on an actual unlocked worker boundary, then warm
        // the cache while its idle-grant cleanup cannot race this test.
        let (entered, waiting) = mpsc::sync_channel(1);
        let (release, resume) = mpsc::sync_channel(1);
        let parked = IdleLane(release);
        *self.db.core.reads.state.reactor_pause[self.node].lock() = Some((entered, resume));
        waiting.recv_timeout(self.db.core.config.timeout).unwrap();
        let lane = self.db.core.reads.lanes.read()[self.node][0].clone();
        assert_eq!(lane.outstanding.load(Ordering::Acquire), 0);
        assert_eq!(
            self.db.core.reads.state.live_drains.load(Ordering::Acquire),
            0
        );
        assert_eq!(*self.db.core.reads.state.retained.lock(), (0, 0));
        {
            let mut driver = lane
                .driver
                .try_lock()
                .expect("parked worker owns no driver");
            let cache = driver.live_cache.as_mut().unwrap();
            assert_eq!(local::local_snapshot(cache).0, 0);
            assert_eq!(
                local::owner_local_pin_count(&self._fixture.nodes[self.node]),
                0
            );
            local::install_owner_local(cache, &self._fixture.nodes[self.node], self.reference);
            assert!(cache.inline_ready(&self.reference));
            assert!(cache.inline_credit(&self.reference));
        }
        assert!(lane.client.try_lock().is_some());
        parked
    }
    fn inline(&self) -> Result<Option<Vec<Mutation>>, LocalDbError> {
        self.db.core.reads.read_inline(
            self.node,
            self.db.epoch(),
            &self.reference,
            0,
            Some(self.db.core.index.pin_cut(IndexReadCut::Latest).unwrap()),
        )
    }
    fn snapshot(&self) -> (usize, usize, usize) {
        let lanes = self.db.core.reads.lanes.read();
        let driver = lanes[self.node][0].driver.lock();
        let snapshot = local::local_snapshot(driver.live_cache.as_ref().unwrap());
        drop(driver);
        // A retired registry flight no longer owns its index-cut pin.
        assert!(
            self.db
                .core
                .reads
                .state
                .flights
                .lock()
                .slots
                .iter()
                .all(|flight| flight.strong_count() == 0)
        );
        assert_eq!(
            local::owner_local_pin_count(&self._fixture.nodes[self.node]),
            snapshot.0
        );
        snapshot
    }
}
impl Drop for Case {
    fn drop(&mut self) {
        let node = &self._fixture.nodes[self.node];
        let _ = self.db.core.reads.pause_live_for_retirement();
        let _ = self.db.shutdown();
        assert_eq!(local::owner_local_pin_count(node), 0);
    }
}

#[test]
fn live_inline_matches_worker_and_bypasses_actor() {
    let mut case = Case::new();
    let idle_lane = case.park_idle_lane();
    let before = case.snapshot();
    let values = case.inline().unwrap().unwrap();
    assert_eq!(values, vec![super::tests::put(0, 7)]);
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .actor_completed
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        case.db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(),
        super::tests::value(7)
    );
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline
            .load(Ordering::Relaxed),
        2
    );
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .actor_completed
            .load(Ordering::Relaxed),
        0
    );
    drop(idle_lane);
    let worker = case
        .db
        .core
        .reads
        .submit(case.node, vec![case.reference], 0, false, None)
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(values, worker);
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
    assert_eq!(case.snapshot(), before);
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .inline_result_copies
            .load(Ordering::Relaxed),
        0
    );
}

#[test]
fn live_inline_disabled_uses_worker_without_inline_counters() {
    let mut case = Case::with_inline_reads(false);
    assert!(case.inline().unwrap().is_none());
    assert_eq!(
        case.db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(),
        super::tests::value(7)
    );
    let diagnostics = &case.db.core.reads.state.diagnostics;
    for counter in [
        &diagnostics.live_inline,
        &diagnostics.live_inline_fallback_drain,
        &diagnostics.live_inline_fallback_busy,
        &diagnostics.live_inline_fallback_grant,
        &diagnostics.live_inline_fallback_credit,
        &diagnostics.live_inline_fallback_paused,
        &diagnostics.live_inline_fallback_coalesced,
    ] {
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
    assert_eq!(diagnostics.actor_completed.load(Ordering::Relaxed), 1);
}

#[test]
fn live_inline_busy_mutex_falls_back_without_waiting() {
    let case = Case::new();
    let _idle_lane = case.park_idle_lane();
    let lanes = case.db.core.reads.lanes.read();
    let held = lanes[case.node][0].client.lock();
    assert!(case.inline().unwrap().is_none());
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline_fallback_busy
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .inline_mutex_fallbacks
            .load(Ordering::Relaxed),
        1
    );
    drop(held);
    drop(lanes);
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
    assert!(case.inline().unwrap().is_some());
}

#[test]
fn live_inline_grant_miss_uses_existing_admission() {
    let case = Case::new();
    let _idle_lane = case.park_idle_lane();
    let mut reference = case.reference;
    reference.batch_address.offset += case.db.core.config.storage.wal_file_bytes;
    assert!(
        case.db
            .core
            .reads
            .read_inline(case.node, case.db.epoch(), &reference, 0, None)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline_fallback_grant
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
}

#[test]
fn live_inline_paused_admission_falls_back() {
    let case = Case::new();
    let idle_lane = case.park_idle_lane();
    let pause = case.db.core.reads.pause_idle_nvmeof(false).unwrap();
    assert!(case.inline().unwrap().is_none());
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline_fallback_paused
            .load(Ordering::Relaxed),
        1
    );
    drop(pause);
    assert!(case.inline().unwrap().is_some());
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline_fallback_busy
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline_fallback_grant
            .load(Ordering::Relaxed),
        0
    );
    drop(idle_lane);
}

#[test]
fn live_inline_blocks_retirement_and_coalesces_identical_references() {
    let case = Case::new();
    let _idle_lane = case.park_idle_lane();
    let before = case.snapshot();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    *case.db.core.reads.state.inline_pause.lock() = Some((entered, resume));
    let reads = case.db.core.reads.clone();
    let (node, epoch, reference) = (case.node, case.db.epoch(), case.reference);
    let producer = thread::spawn(move || reads.read_inline(node, epoch, &reference, 0, None));
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(case.db.core.reads.pause_live_for_retirement().is_err());
    assert!(!case.db.core.reads.failed());
    assert!(case.inline().unwrap().is_none());
    let joined = case
        .db
        .core
        .reads
        .submit(node, vec![reference], 0, false, None)
        .unwrap();
    release.send(()).unwrap();
    let producer = producer.join().unwrap().unwrap().unwrap();
    assert_ne!(*case.db.core.reads.state.retained.lock(), (0, 0));
    assert_eq!(joined.finish().unwrap(), producer);
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
    assert_eq!(case.snapshot(), before);
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline
            .load(Ordering::Relaxed),
        1
    );
}

#[test]
fn live_inline_corruption_poisons_and_releases_credit() {
    let case = Case::new();
    let _idle_lane = case.park_idle_lane();
    let before = case.snapshot();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&case.path)
        .unwrap();
    let offset = case.reference.batch_address.offset % case.db.core.config.storage.wal_file_bytes
        + 8
        + u64::from(case.reference.descriptor.offset)
        + u64::from(case.reference.descriptor.len)
        - 1;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[0xff]).unwrap();
    assert!(case.inline().is_err());
    assert!(case.db.core.reads.failed());
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .failed_owner_profile_not_applicable
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
    assert_eq!(case.snapshot(), before);
}

#[test]
fn live_inline_deadline_releases_credit_only_after_retirement() {
    let case = Case::new();
    let _idle_lane = case.park_idle_lane();
    let before = case.snapshot();
    case.db
        .core
        .reads
        .set_limits_for_test(None, Some(Duration::from_millis(50)));
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    {
        let lanes = case.db.core.reads.lanes.read();
        let mut driver = lanes[case.node][0].driver.lock();
        local::pause_inline_submitted(driver.live_cache.as_mut().unwrap(), entered, resume);
    }
    let reads = case.db.core.reads.clone();
    let (node, epoch, reference) = (case.node, case.db.epoch(), case.reference);
    let pin = case.db.core.index.pin_cut(IndexReadCut::Latest).unwrap();
    let worker = thread::spawn(move || reads.read_inline(node, epoch, &reference, 0, Some(pin)));
    // The pause handshake follows worker progress.
    waiting.recv_timeout(PROGRESS_CAP).unwrap();
    let admitted_credit = *case.db.core.reads.state.retained.lock();
    assert_eq!(admitted_credit.0, 1);
    assert!(admitted_credit.1 > 0);
    assert_eq!(local::owner_local_pin_count(&case._fixture.nodes[node]), 1);
    assert!(case.db.core.reads.pause_live_for_retirement().is_err());
    // The read's 50 ms I/O deadline was taken before the worker entered the
    // pause, so 60 ms after entry is strictly past it. A floor: machine load
    // only lengthens the overdue window.
    thread::sleep(Duration::from_millis(60));
    // The SQE is now overdue but still paused before any terminal harvest.
    assert_eq!(*case.db.core.reads.state.retained.lock(), admitted_credit);
    {
        let registry = case.db.core.reads.state.flights.lock();
        let (flight, _) = registry.find(node, &reference).unwrap();
        assert!(flight._pin.is_some());
        assert!(flight.completion.result.lock().is_none());
        assert!(!case.db.core.reads.state.flight_ready[flight.slot].is_ready());
    }
    assert_eq!(local::owner_local_pin_count(&case._fixture.nodes[node]), 1);
    release.send(()).unwrap();
    assert!(worker.join().unwrap().is_err());
    assert!(case.db.core.reads.failed());
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
    assert_eq!(case.snapshot(), before);
}

/// Native parity: time an inline read spends after admission and before its
/// transport submission does not count against its I/O deadline.
#[test]
fn live_inline_pause_before_submission_does_not_spend_deadline() {
    let case = Case::new();
    let _idle_lane = case.park_idle_lane();
    let before = case.snapshot();
    // The paused-time proof scales with the budget: three timeouts of paused
    // time still exceed one timeout of live I/O, and a 1 s device budget
    // keeps a healthy read safe under heavy machine load.
    let timeout = Duration::from_secs(1);
    case.db.core.reads.set_limits_for_test(None, Some(timeout));
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    *case.db.core.reads.state.inline_pause.lock() = Some((entered, resume));
    let reads = case.db.core.reads.clone();
    let (node, epoch, reference) = (case.node, case.db.epoch(), case.reference);
    let pin = case.db.core.index.pin_cut(IndexReadCut::Latest).unwrap();
    let producer = thread::spawn(move || reads.read_inline(node, epoch, &reference, 0, Some(pin)));
    // The pause handshake follows producer progress.
    waiting.recv_timeout(PROGRESS_CAP).unwrap();
    {
        let registry = case.db.core.reads.state.flights.lock();
        let (flight, _) = registry.find(node, &reference).unwrap();
        assert_eq!(flight.io_deadline(), None, "not yet submitted");
    }
    // Floor: the producer paused before submission, so three timeouts of
    // paused time prove the pause did not spend the I/O deadline. Machine
    // load only lengthens the paused window, strengthening the check.
    thread::sleep(3 * timeout);
    release.send(()).unwrap();
    let values = producer.join().unwrap().unwrap().unwrap();
    assert_eq!(values, vec![super::tests::put(0, 7)]);
    assert!(!case.db.core.reads.failed());
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
    assert_eq!(case.snapshot(), before);
}

#[test]
fn live_inline_eight_callers_share_one_lane_and_exact_flight() {
    let case = Case::new();
    let idle_lane = case.park_idle_lane();
    assert!(case.inline().unwrap().is_some());
    drop(idle_lane);
    let before = case.snapshot();
    let gate = Arc::new(std::sync::Barrier::new(8));
    let mut workers = Vec::new();
    for _ in 0..8 {
        let reads = case.db.core.reads.clone();
        let gate = gate.clone();
        let (node, epoch, reference) = (case.node, case.db.epoch(), case.reference);
        workers.push(thread::spawn(move || {
            gate.wait();
            for _ in 0..8 {
                let values = match reads.read_inline(node, epoch, &reference, 0, None).unwrap() {
                    Some(values) => values,
                    None => reads
                        .submit(node, vec![reference], 0, false, None)
                        .unwrap()
                        .finish()
                        .unwrap(),
                };
                assert_eq!(values, vec![super::tests::put(0, 7)]);
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    assert!(!case.db.core.reads.failed());
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
    assert_eq!(case.snapshot(), before);
    assert!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline
            .load(Ordering::Relaxed)
            > 0
    );
}

#[test]
fn live_inline_drain_in_progress_falls_back_without_taking_lane() {
    let case = Case::new();
    let lane = case.db.core.reads.lanes.read()[case.node][0].clone();
    let client = lane.client.lock();
    let (reply, responses) = mpsc::sync_channel(1);
    case.db.core.reads.sender.lock()[case.node]
        .send(ReadCommand::DrainLive(reply))
        .unwrap();
    // Progress wait on the live-drain counter: the drain worker sets it once
    // the in-flight drain is observed.
    let drained = Instant::now() + PROGRESS_CAP;
    while case.db.core.reads.state.live_drains.load(Ordering::Acquire) == 0 {
        assert!(
            Instant::now() < drained,
            "DrainLive command never reached the lane worker"
        );
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        case.db.core.reads.state.live_drains.load(Ordering::Acquire),
        1
    );
    assert!(case.inline().unwrap().is_none());
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline_fallback_drain
            .load(Ordering::Relaxed),
        1
    );
    assert!(matches!(
        responses.recv_timeout(Duration::from_millis(20)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    drop(client);
    responses
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(!case.db.core.reads.failed());
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
}

#[test]
fn live_inline_terminal_retirement_precedes_drain_reply() {
    let case = Case::new();
    let idle_lane = case.park_idle_lane();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    *case.db.core.reads.state.inline_pause.lock() = Some((entered, resume));
    let reads = case.db.core.reads.clone();
    let (node, epoch, reference) = (case.node, case.db.epoch(), case.reference);
    let producer = thread::spawn(move || reads.read_inline(node, epoch, &reference, 0, None));
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(idle_lane);
    let (reply, responses) = mpsc::sync_channel(1);
    case.db.core.reads.sender.lock()[node]
        .send(ReadCommand::DrainLive(reply))
        .unwrap();
    assert!(matches!(
        responses.recv_timeout(Duration::from_millis(20)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release.send(()).unwrap();
    assert_eq!(
        producer.join().unwrap().unwrap().unwrap(),
        vec![super::tests::put(0, 7)]
    );
    responses
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(local::owner_local_pin_count(&case._fixture.nodes[node]), 0);
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
    assert_eq!(case.snapshot().2, 0);
}

#[test]
fn live_inline_queued_lane_falls_back_and_preserves_both_reads() {
    let case = Case::new();
    let reference = case
        .db
        .index_reference(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let lane = case.db.core.reads.lanes.read()[case.node][0].clone();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    *case.db.core.reads.state.reactor_pause[case.node].lock() = Some((entered, resume));
    let pending = case
        .db
        .core
        .reads
        .submit(case.node, vec![reference], 0, false, None)
        .unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    // The worker has received the other reference but owns neither mutex.
    // Without the outstanding/idle check this cached inline read would succeed.
    assert!(lane.driver.try_lock().is_some());
    assert!(lane.client.try_lock().is_some());
    let mutex_fallbacks = case
        .db
        .core
        .reads
        .state
        .inline_mutex_fallbacks
        .load(Ordering::Relaxed);
    assert!(case.inline().unwrap().is_none());
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline_fallback_busy
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .inline_mutex_fallbacks
            .load(Ordering::Relaxed),
        mutex_fallbacks
    );
    release.send(()).unwrap();
    assert_eq!(pending.finish().unwrap(), vec![super::tests::put(1, 8)]);
    let _idle_lane = case.park_idle_lane();
    assert_eq!(
        case.inline().unwrap().unwrap(),
        vec![super::tests::put(0, 7)]
    );
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
}

#[test]
fn live_inline_io_error_retires_pin_and_credit_before_poison() {
    let case = Case::new();
    let _idle_lane = case.park_idle_lane();
    let before = case.snapshot();
    {
        let lanes = case.db.core.reads.lanes.read();
        let mut driver = lanes[case.node][0].driver.lock();
        local::override_inline_completion(driver.live_cache.as_mut().unwrap(), -libc::EIO);
    }
    assert!(case.inline().is_err());
    assert!(case.db.core.reads.failed());
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
    assert_eq!(case.snapshot(), before);
}

#[test]
fn live_inline_wait_failure_drains_error_cqe_before_releasing_pins() {
    let case = Case::new();
    let _idle_lane = case.park_idle_lane();
    let before = case.snapshot();
    {
        let lanes = case.db.core.reads.lanes.read();
        let mut driver = lanes[case.node][0].driver.lock();
        let cache = driver.live_cache.as_mut().unwrap();
        local::fail_inline_wait(cache);
        local::override_inline_completion(cache, -libc::EIO);
    }
    let error = case.inline().unwrap_err();
    assert!(error.to_string().contains("injected inline wait failure"));
    assert!(case.db.core.reads.failed());
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
    // The wait fails after submission, before any ordinary progress. Only the
    // fatal cleanup drain can retire the injected error CQE and restore credit.
    assert_eq!(case.snapshot(), before);
}

#[test]
fn live_inline_error_reaches_coalesced_joiner_and_retires_credit_once() {
    let case = Case::new();
    let _idle_lane = case.park_idle_lane();
    let before = case.snapshot();
    {
        let lanes = case.db.core.reads.lanes.read();
        let mut driver = lanes[case.node][0].driver.lock();
        local::override_inline_completion(driver.live_cache.as_mut().unwrap(), -libc::EIO);
    }
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    *case.db.core.reads.state.inline_pause.lock() = Some((entered, resume));
    let reads = case.db.core.reads.clone();
    let (node, epoch, reference) = (case.node, case.db.epoch(), case.reference);
    let pin = case.db.core.index.pin_cut(IndexReadCut::Latest).unwrap();
    let producer = thread::spawn(move || reads.read_inline(node, epoch, &reference, 0, Some(pin)));
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let joined = case
        .db
        .core
        .reads
        .submit(node, vec![reference], 0, false, None)
        .unwrap();
    let flight = {
        let registry = case.db.core.reads.state.flights.lock();
        let (flight, _) = registry.find(node, &reference).unwrap();
        assert!(Arc::ptr_eq(&flight, &joined.parts[0].0));
        assert!(flight._pin.is_some());
        Arc::downgrade(&flight)
    };
    let joined_credit = *case.db.core.reads.state.retained.lock();
    assert_eq!(joined_credit.0, 1);
    release.send(()).unwrap();
    let producer_error = producer.join().unwrap().unwrap_err();
    let expected = crate::distributed::nvmeof::live_io::LiveIoError::from(
        std::io::Error::from_raw_os_error(libc::EIO),
    )
    .to_string();
    assert_eq!(producer_error.to_string(), expected);
    assert!(case.db.core.reads.failed());
    assert_eq!(*case.db.core.reads.state.retained.lock(), joined_credit);
    assert!(flight.upgrade().is_some());
    let Err(joined_error) = joined.finish() else {
        panic!("coalesced inline error was not delivered");
    };
    assert_eq!(joined_error.to_string(), producer_error.to_string());
    assert!(flight.upgrade().is_none());
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
    assert_eq!(case.snapshot(), before);
    let diagnostics = &case.db.core.reads.state.diagnostics;
    assert_eq!(diagnostics.live_inline.load(Ordering::Relaxed), 1);
    assert_eq!(diagnostics.completed.load(Ordering::Relaxed), 1);
    assert_eq!(diagnostics.errors.load(Ordering::Relaxed), 1);
    assert_eq!(diagnostics.actor_completed.load(Ordering::Relaxed), 0);
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .coalesced_members
            .load(Ordering::Relaxed),
        1
    );
}

#[test]
fn live_inline_joiner_after_io_completion_receives_one_value() {
    let case = Case::new();
    let _idle_lane = case.park_idle_lane();
    let before = case.snapshot();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    *case.db.core.reads.state.inline_retirement_pause.lock() = Some((entered, resume));
    let reads = case.db.core.reads.clone();
    let (node, epoch, reference) = (case.node, case.db.epoch(), case.reference);
    let pin = case.db.core.index.pin_cut(IndexReadCut::Latest).unwrap();
    let producer = thread::spawn(move || reads.read_inline(node, epoch, &reference, 0, Some(pin)));
    // The member has completed and decoded; publication and retirement have
    // not started. Admission can still find and join this exact flight.
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let joined = case
        .db
        .core
        .reads
        .submit(node, vec![reference], 0, false, None)
        .unwrap();
    assert_eq!(joined.parts.len(), 1);
    let flight = {
        let registry = case.db.core.reads.state.flights.lock();
        let (flight, _) = registry.find(node, &reference).unwrap();
        assert!(Arc::ptr_eq(&flight, &joined.parts[0].0));
        assert!(flight.completion.result.lock().is_none());
        assert!(!case.db.core.reads.state.flight_ready[flight.slot].is_ready());
        Arc::downgrade(&flight)
    };
    release.send(()).unwrap();
    let produced = producer.join().unwrap().unwrap().unwrap();
    assert_eq!(produced, vec![super::tests::put(0, 7)]);
    assert_eq!(case.db.core.reads.state.retained.lock().0, 1);
    let values = joined.finish().unwrap();
    assert_eq!(values, produced);
    assert_eq!(values.len(), 1);
    assert!(flight.upgrade().is_none());
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
    assert_eq!(case.snapshot(), before);
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .inline_result_copies
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .live_inline
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .diagnostics
            .actor_completed
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        case.db
            .core
            .reads
            .state
            .coalesced_members
            .load(Ordering::Relaxed),
        1
    );
}

/// Ported from the owner-RPC `busy_scalar_reservation_remains_exclusive`: a
/// paused inline scalar keeps its lane reservation, so a new request for
/// another reference on that owner is refused rather than queued behind it.
#[test]
fn live_inline_busy_scalar_reservation_remains_exclusive() {
    let case = Case::new();
    let idle_lane = case.park_idle_lane();
    let second = case
        .db
        .index_reference(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    assert_eq!(
        case.db.core.partition_indexes[&second.batch_address.partition],
        case.node
    );
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    *case.db.core.reads.state.inline_pause.lock() = Some((entered, resume));
    let reads = case.db.core.reads.clone();
    let (node, epoch, first) = (case.node, case.db.epoch(), case.reference);
    let scalar = thread::spawn(move || reads.read_inline(node, epoch, &first, 0, None));
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let admitted = case
        .db
        .core
        .reads
        .submit(node, vec![second], 0, false, None);
    release.send(()).unwrap();
    assert!(admitted.is_err());
    assert_eq!(
        scalar.join().unwrap().unwrap(),
        Some(vec![super::tests::put(0, 7)])
    );
    drop(idle_lane);
    assert_eq!(
        case.db
            .core
            .reads
            .submit(node, vec![second], 0, false, None)
            .unwrap()
            .finish()
            .unwrap(),
        vec![super::tests::put(1, 8)]
    );
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
}

/// Ported from the owner-RPC `grouped_join_of_inline_flight_notifies_admission_after_readiness`:
/// a batch that joins a paused inline flight takes no lane, and the producer's
/// readiness (not a credit drop) advances the admission generation.
#[test]
fn live_inline_grouped_join_notifies_admission_after_readiness() {
    let case = Case::new();
    let _idle_lane = case.park_idle_lane();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    *case.db.core.reads.state.inline_pause.lock() = Some((entered, resume));
    let reads = case.db.core.reads.clone();
    let (node, epoch, reference) = (case.node, case.db.epoch(), case.reference);
    let scalar = thread::spawn(move || reads.read_inline(node, epoch, &reference, 0, None));
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut groups = vec![Vec::new(); case.db.core.partition_indexes.len()];
    groups[node].push((0, reference));
    let mut batch = ReadBatch::new(&case.db.core.reads, vec![None], groups).unwrap();
    batch.dispatch(&case.db.core.reads).unwrap();
    assert_eq!(batch.reads.len(), 1);
    let generation = case.db.core.reads.admission_generation();
    release.send(()).unwrap();
    assert!(scalar.join().unwrap().unwrap().is_some());
    // The joiner still owns the flight and both credits, so a credit drop
    // cannot be responsible for advancing this generation.
    assert!(case.db.core.reads.admission_generation() > generation);
    batch.finish_one().unwrap();
    assert_eq!(batch.values[0], Some(super::tests::put(0, 7)));
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
}

/// Ported from the owner-RPC `inline_reads_reserve_idle_lanes_with_unrelated_retained_credits`:
/// a retained credit queued at another owner neither blocks an inline read of
/// an idle lane nor is stolen by an inline read of the queued owner.
#[test]
fn live_inline_reserves_idle_lane_with_unrelated_retained_credit() {
    let mut case = Case::new();
    let other = (0..case.db.core.config.endpoints.len() as u32)
        .find(|&partition| case.db.core.partition_indexes[&partition] != case.node)
        .unwrap();
    case.db
        .write_batch(
            case._fixture.operation(other, 2),
            vec![super::tests::put(2, 9)],
        )
        .unwrap();
    let second = case
        .db
        .index_reference(KeySpace(0), &2u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let second_node = case.db.core.partition_indexes[&second.batch_address.partition];
    assert_ne!(second_node, case.node);
    let _idle_lane = case.park_idle_lane();

    let lane = case.db.core.reads.lane_client(second_node, 0);
    let held = lane.lock();
    let pending = case
        .db
        .submit_get(KeySpace(0), &2u32.to_be_bytes())
        .unwrap();
    assert_eq!(case.db.core.reads.state.retained.lock().0, 1);

    let inline = case
        .inline()
        .unwrap()
        .expect("idle independent lane should make progress inline");
    assert_eq!(inline, vec![super::tests::put(0, 7)]);
    assert_eq!(case.db.core.reads.state.retained.lock().0, 1);

    assert!(
        case.db
            .core
            .reads
            .read_inline(second_node, case.db.epoch(), &second, 0, None)
            .unwrap()
            .is_none(),
        "queued actor ownership must block inline theft"
    );
    drop(held);
    assert_eq!(case.db.finish_get(pending).unwrap(), super::tests::value(9));
    assert_eq!(*case.db.core.reads.state.retained.lock(), (0, 0));
}

/// Ported from the owner-RPC `inline_transport_failure_poison_is_terminal`:
/// after a failed inline read every later read and the shutdown fail.
#[test]
fn live_inline_failure_poison_is_terminal() {
    let mut case = Case::new();
    let idle_lane = case.park_idle_lane();
    {
        let lanes = case.db.core.reads.lanes.read();
        let mut driver = lanes[case.node][0].driver.lock();
        local::override_inline_completion(driver.live_cache.as_mut().unwrap(), -libc::EIO);
    }
    assert!(case.inline().is_err());
    assert!(case.db.core.reads.failed());
    // Shutdown joins the lane worker: release it from its pause first.
    drop(idle_lane);
    assert!(case.db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert!(case.db.shutdown().is_err());
}
