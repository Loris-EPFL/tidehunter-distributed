//! Real TCP control and local-file io_uring tests for the leased owner worker.
use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::tcp::leased_tests::{Fixture, leased_latencies};
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) fn worker(fixture: &Fixture, client: Client) -> (ClientWorker, Arc<AtomicBool>) {
    worker_with_capacity(fixture, client, 16)
}

fn worker_with_capacity(
    fixture: &Fixture,
    client: Client,
    capacity: usize,
) -> (ClientWorker, Arc<AtomicBool>) {
    worker_with_failure_context(
        fixture,
        client,
        capacity,
        Arc::new(std::sync::OnceLock::new()),
    )
}

fn worker_with_failure_context(
    fixture: &Fixture,
    client: Client,
    capacity: usize,
    first_failure: Arc<std::sync::OnceLock<FirstFailure>>,
) -> (ClientWorker, Arc<AtomicBool>) {
    let max_pending = capacity.max(crate::distributed::tcp::APPEND_COMMAND_WINDOW);
    let completion = write_core::test_completion::TestCompletion::new(max_pending);
    worker_with_completion(
        fixture,
        client,
        capacity,
        first_failure,
        (completion.clone(), completion),
        true,
    )
}

/// `completion` is the test reply sink and the route the worker calls.
fn worker_with_completion(
    fixture: &Fixture,
    client: Client,
    capacity: usize,
    first_failure: Arc<std::sync::OnceLock<FirstFailure>>,
    completion: (
        Arc<write_core::test_completion::TestCompletion>,
        Arc<dyn write_core::completion::AppendCompletion>,
    ),
    leased_submit_redrain: bool,
) -> (ClientWorker, Arc<AtomicBool>) {
    let (sink, route) = completion;
    let failure = Arc::new(AtomicBool::new(false));
    let max_pending = capacity.max(crate::distributed::tcp::APPEND_COMMAND_WINDOW);
    let mut worker = ClientWorker::start(
        client,
        capacity,
        failure.clone(),
        WorkerDiagnostics {
            timeout: Duration::from_secs(5),
            write_profile_limit: 0,
            write_profile: Arc::new(Mutex::new(WriteProfileSnapshot::default())),
            write_timeline: Arc::new(WriteTimeline::default()),
            first_failure,
            partition: fixture.config.partition.partition,
            policy: WorkerPolicy::new(
                fixture.config.partition.clone(),
                max_pending,
                crate::distributed::tcp::APPEND_COMMAND_WINDOW,
                leased_submit_redrain,
            )
            .unwrap(),
        },
        route,
    )
    .unwrap();
    worker.test_completion = Some(sink);
    (worker, failure)
}

/// Test completion route that runs one action in the worker's first durable
/// callback. The worker releases replies after the drain at the top of its
/// turn and before that turn's frame submission, so the action runs exactly
/// between the two.
struct ActOnFirstDurable {
    sink: Arc<write_core::test_completion::TestCompletion>,
    action: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl write_core::completion::AppendCompletion for ActOnFirstDurable {
    fn durable(&self, owner: u32, batches: Vec<(u64, DurableBatch)>) {
        self.sink.durable(owner, batches);
        if let Some(action) = self.action.lock().take() {
            action();
        }
    }

    fn failed(&self, owner: u32, sequences: &[u64], error: LocalDbError) {
        self.sink.failed(owner, sequences, error);
    }
}

/// A group that becomes READY after the top-of-turn drain: with the
/// pre-submit re-drain it joins that turn's frame; without it, it waits for
/// the next frame, as the single drain did before the re-drain existed.
#[test]
fn leased_submit_redrain_switch_selects_the_frame_of_a_late_ready_group() {
    for redrain in [true, false] {
        let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
        let mut client = fixture.writer();
        client.leased_test_set_window(1);
        client.leased_test_hold_completions(&[1]);
        let capacity = 16;
        let sink = write_core::test_completion::TestCompletion::new(
            capacity.max(crate::distributed::tcp::APPEND_COMMAND_WINDOW),
        );
        let route = Arc::new(ActOnFirstDurable {
            sink: sink.clone(),
            action: Mutex::new(None),
        });
        let (worker, failure) = worker_with_completion(
            &fixture,
            client,
            capacity,
            Arc::new(std::sync::OnceLock::new()),
            (sink.clone(), route.clone()),
            redrain,
        );
        let batches: Vec<_> = (0..3).map(|n| fixture.batch(n)).collect();
        let first = worker.enqueue(1, vec![batches[0].clone()]).unwrap();
        // Flight 1 carries batch 0; its completion is reaped and held.
        let deadline = Instant::now() + Duration::from_secs(5);
        while worker.client.lock().leased_test_reaped_writes() == 0 {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        let mut guard = worker.client.lock();
        // The next turn drains batch 1 and stops at ALLOCATED batch 2. Its
        // release of flight 1 then makes batch 2 READY.
        let second = worker.enqueue(1, vec![batches[1].clone()]).unwrap();
        let third = sink.register(&batches[2..]).unwrap();
        let late = batches[2].clone();
        let (owner_sequence, _) = worker.queue.reserve(|| late.version().sequence);
        let queue = Arc::clone(&worker.queue);
        *route.action.lock() = Some(Box::new(move || {
            queue.ready(owner_sequence, late.version().sequence, late);
        }));
        guard.leased_test_release_held();
        drop(guard);
        for (batch, reply) in batches.iter().zip([first, second, third]) {
            let receipts = reply.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
            assert_eq!(receipts.len(), 1);
            assert_eq!(receipts[0].appended().version(), batch.version());
        }
        assert!(route.action.lock().is_none());
        assert_eq!(
            *worker.accounting.formed_groups.lock(),
            [[0], [1], [2]],
            "redrain={redrain}"
        );
        assert_eq!(
            fixture.metrics.snapshot().counts.leased_frames,
            if redrain { 2 } else { 3 },
            "redrain={redrain}"
        );
        drop(worker);
        // Acquire observes any fatal outcome the worker published.
        assert!(!failure.load(Ordering::Acquire));
    }
}

#[test]
fn leased_worker_group_flush_replies_and_checkpoint_cover_the_durable_prefix() {
    let fixture = Fixture::new(DurabilityTier::Synchronous, 64 * 1024);
    let mut client = fixture.writer();
    client.leased_test_group_flush();
    client.leased_test_set_window(4);
    client.leased_test_single_batch_groups();
    client.leased_test_hold_completions(&[1, 2, 3, 4]);
    let (worker, failure) = worker(&fixture, client);
    let guard = worker.client.lock();
    let replies: Vec<_> = (0..8)
        .map(|sequence| worker.enqueue(1, vec![fixture.batch(sequence)]).unwrap())
        .collect();
    drop(guard);
    let deadline = Instant::now() + Duration::from_secs(5);
    while fixture.metrics.snapshot().counts.max_leased_in_flight < 4 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    for reply in &replies {
        assert!(matches!(reply.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }
    worker.client.lock().leased_test_release_held();
    for (sequence, reply) in replies.into_iter().enumerate() {
        let receipt = reply.recv_timeout(Duration::from_secs(5)).unwrap().unwrap()[0];
        assert_eq!(
            receipt.appended().version(),
            fixture.batch(sequence as u64).version()
        );
    }
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!(counts.leased_frames, 8);
    assert!(counts.leased_flushes > 0);
    assert_eq!(counts.leased_flushes, counts.leased_flush_samples);
    assert_eq!(counts.fua_writes + counts.leased_flush_covered_frames, 8);
    let written = worker.client.lock().leased_test_frontier().0;
    let (reply, checkpoint) = mpsc::sync_channel(1);
    worker
        .send(Command::CheckpointPosition(1, 1, written, reply))
        .unwrap();
    assert!(
        checkpoint
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .durable_end
            >= written
    );
    // A position proof persists the WAL position root, not adopted-receipt
    // bookkeeping (the removed receipt-prefix proof did that).
    assert!(fixture.node.log_for_test(1).position_frontier_for_test() >= written);
    drop(worker);
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn full_local_controls_wake_on_ready_and_keep_each_owner_fence() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let (worker, failure) = worker_with_capacity(&fixture, fixture.writer_without_ahead(), 1);
    let first = fixture.batch(0);
    let second = fixture.batch(1);
    let completion = worker.test_completion.as_ref().unwrap();
    let first_reply = completion.register(std::slice::from_ref(&first)).unwrap();
    let second_reply = completion.register(std::slice::from_ref(&second)).unwrap();
    let guard = worker.client.lock();
    let (first_owner, _) = worker.queue.reserve(|| first.version().sequence);
    let (observed, observations) = mpsc::sync_channel(2);
    let first_observed = observed.clone();
    let metrics = Arc::clone(&fixture.metrics);
    worker
        .send(Command::Control(Box::new(move |_| {
            let _ = first_observed.send((1, metrics.snapshot().counts.leased_frames));
        })))
        .unwrap();
    let (second_owner, _) = worker.queue.reserve(|| second.version().sequence);
    worker
        .queue
        .ready(second_owner, second.version().sequence, second.clone());
    let (entered, parked) = mpsc::sync_channel(1);
    let sender = worker.sender.as_ref().unwrap();
    sender.observe_full_park(entered);
    drop(guard);
    parked.recv_timeout(Duration::from_secs(5)).unwrap();
    let guard = worker.client.lock();
    let metrics = Arc::clone(&fixture.metrics);
    worker
        .send(Command::Control(Box::new(move |_| {
            let _ = observed.send((2, metrics.snapshot().counts.leased_frames));
        })))
        .unwrap();
    assert!(matches!(
        observations.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    // The earlier ALLOCATED entry held the local control; the later control
    // fills the bounded channel. READY must wake without a channel permit.
    worker
        .queue
        .ready(first_owner, first.version().sequence, first.clone());
    assert!(matches!(
        sender.try_send(Command::QueueReady),
        Err(mpsc::TrySendError::Full(_))
    ));
    signal_worker_wake(&worker.worker_wake).unwrap();
    drop(guard);
    let first_receipts = first_reply
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let second_receipts = second_reply
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(first_receipts[0].appended().id(), first.id());
    assert_eq!(second_receipts[0].appended().id(), second.id());
    assert_eq!(
        observations.recv_timeout(Duration::from_secs(5)).unwrap(),
        (1, 1)
    );
    assert_eq!(
        observations.recv_timeout(Duration::from_secs(5)).unwrap(),
        (2, 2)
    );
    assert!(second_receipts[0].appended().address().offset >= 16384);
    drop(worker);
    assert!(!failure.load(Ordering::Acquire));
}

/// PR #62 P2: a pending adoption with no leased flight, and full local
/// controls behind an earlier ALLOCATED owner entry. The worker must keep its
/// 1 ms adoption cadence: after the owner replies, no sender, READY entry or
/// eventfd signal wakes it, yet it still harvests the adoption.
#[test]
fn full_local_controls_keep_driving_pending_adoption() {
    use crate::distributed::storage::LivePausePoint;
    let fixture = Fixture::new(DurabilityTier::Native, 128 * 1024);
    let mut client = fixture.writer();
    client
        .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
        .unwrap();
    client.wait_leased_control(&mut Vec::new(), None).unwrap(); // Initial prelease.
    let (entered, release) = fixture
        .node
        .log_for_test(1)
        .pause_live(LivePausePoint::FrameProbe);
    client.submit_leased_adoption().unwrap();
    let (worker, failure) = worker_with_capacity(&fixture, client, 1);
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let guard = worker.client.lock();
    // The earlier allocation stays ALLOCATED, so the control behind its
    // owner-sequence fence fills the one-entry local control metadata.
    let (fence, _) = worker.queue.reserve(|| 1);
    let (reply, control) = mpsc::sync_channel(1);
    worker
        .send(Command::Control(Box::new(move |_| {
            let _ = reply.send(());
        })))
        .unwrap();
    let (parked_entered, parked) = mpsc::sync_channel(1);
    let sender = worker.sender.as_ref().unwrap();
    sender.observe_full_park(parked_entered);
    let before = fixture.metrics.snapshot().counts.adoption_async_samples;
    drop(guard);
    let parked_full = parked.recv_timeout(Duration::from_secs(5));
    let pending_when_parked = worker.client.lock().leased_adoption_pending();
    let released = release.send(());
    // Liveness guard only: the harvest needs one 1 ms worker turn.
    let deadline = Instant::now() + Duration::from_secs(5);
    let harvested = loop {
        if fixture.metrics.snapshot().counts.adoption_async_samples != before {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        thread::yield_now();
    };
    let pending_after = worker.client.lock().leased_adoption_pending();
    let control_fenced = matches!(control.try_recv(), Err(mpsc::TryRecvError::Empty));
    // Abort the fence before any assertion, so a failure cannot leave the
    // control (and worker teardown) behind an entry that never resolves.
    worker.queue.abort(fence);
    let _ = sender.try_send(Command::QueueReady);
    parked_full.unwrap();
    released.unwrap();
    assert!(pending_when_parked);
    assert!(
        harvested,
        "full local controls stalled the pending adoption"
    );
    assert!(!pending_after);
    assert!(control_fenced, "the control must wait behind its fence");
    control.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(worker);
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn leased_worker_merges_ready_appends_and_splits_receipts_in_order() {
    let fixture = Fixture::new(DurabilityTier::Synchronous, 16384);
    let mut writer = fixture.writer();
    writer.leased_test_set_window(1);
    let (worker, failure) = worker(&fixture, writer);
    let batches: Vec<_> = (0..3).map(|n| fixture.batch(n)).collect();
    let client = worker.client.lock();
    let replies: Vec<_> = batches
        .iter()
        .map(|batch| worker.enqueue(1, vec![batch.clone()]).unwrap())
        .collect();
    drop(client);
    for (batch, reply) in batches.iter().zip(replies) {
        let receipts = reply.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].appended().id(), batch.id());
        assert_eq!(receipts[0].appended().version(), batch.version());
    }
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!((counts.leased_frames, counts.fua_writes), (1, 1));
    let client = worker.client.clone();
    drop(worker);
    assert!(!client.lock().leased_test_frontier().1);
    // Acquire observes the worker's fatal-outcome publication, if any.
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn leased_worker_clean_shutdown_closes_owner_allocation_guard() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let (worker, failure) = worker(&fixture, fixture.writer_without_ahead());
    receive(worker.enqueue(1, vec![fixture.batch(0)]).unwrap()).unwrap();
    let client = worker.client.clone();
    drop(worker);
    assert!(!client.lock().leased_test_frontier().1);
    assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 1);
    // A second real allocation succeeds only if the first guard was released.
    let mut next = fixture.client();
    let extent = next.lease_live_extent(1).unwrap();
    assert_eq!(extent.extent.start, 16384);
    // Acquire observes the worker's fatal-outcome publication, if any.
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn leased_worker_unknown_write_poison_stops_queued_commands() {
    for result in [Some(-libc::EIO), Some(2048), None] {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        if let Some(result) = result {
            client.leased_test_fail_write(result);
        } else {
            client.leased_test_timeout_write();
        }
        let (worker, failure) = worker(&fixture, client);
        let guard = worker.client.lock();
        let first = worker.enqueue(1, vec![fixture.batch(0)]).unwrap();
        let (reply, checkpoint) = mpsc::sync_channel(1);
        // The position covers the failed first frame, so it can never succeed.
        worker
            .send(Command::CheckpointPosition(1, 1, 4096, reply))
            .unwrap();
        let later = worker.enqueue(1, vec![fixture.batch(1)]).unwrap();
        drop(guard);
        assert!(first.recv_timeout(Duration::from_secs(5)).unwrap().is_err());
        assert!(!matches!(
            checkpoint.recv_timeout(Duration::from_secs(5)),
            Ok(Ok(_))
        ));
        assert!(!matches!(
            later.recv_timeout(Duration::from_secs(5)),
            Ok(Ok(_))
        ));
        drop(worker);
        // Acquire observes the worker's fatal-outcome publication.
        assert!(failure.load(Ordering::Acquire));
        assert_eq!(fixture.metrics.snapshot().counts.leased_frames, 0);
        assert_eq!(fixture.metrics.snapshot().counts.unknown_writes, 1);
    }
}

#[test]
fn leased_worker_harvests_last_adoption_while_idle() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let (worker, failure) = worker(&fixture, fixture.writer());
    receive(worker.enqueue(1, vec![fixture.batch(0)]).unwrap()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (_, _, pending) = worker.client.lock().leased_test_frontier();
        if fixture.metrics.snapshot().counts.adoption_async_exchanges == 1 && !pending {
            break;
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fixture.node.log_for_test(1).appended_frontier(), 4096);
    assert_eq!(fixture.metrics.snapshot().counts.adoption_async_samples, 1);
    // Acquire observes the worker's fatal-outcome publication, if any.
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn leased_worker_failed_adoption_and_digest_mismatch_poison_while_idle() {
    for bad_token in [false, true] {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        client.leased_test_corrupt_adoption(bad_token);
        let (worker, failure) = worker(&fixture, client);
        receive(worker.enqueue(1, vec![fixture.batch(0)]).unwrap()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        // Failure publication precedes channel teardown. Join only after the
        // actual worker exit, so append rejection does not race receiver drop.
        while !failure.load(Ordering::Acquire)
            || !worker.thread.lock().as_ref().unwrap().is_finished()
        {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        worker.stop().unwrap();
        assert!(worker.enqueue(1, vec![fixture.batch(1)]).is_err());
        assert_eq!(fixture.metrics.snapshot().counts.leased_frames, 1);
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.adoption_async_samples, 0);
        assert_eq!(counts.adoption_settle_samples, 0);
        drop(worker);
    }
}

#[test]
fn leased_measurement_gate_observations_keep_warm_extent() {
    let mut cold_restarts = Vec::new();
    for (ahead, timeline) in [(false, false), (false, true), (true, false), (true, true)] {
        let fixture = Fixture::new(DurabilityTier::Native, 128 * 1024);
        let mut client = if ahead {
            fixture.writer()
        } else {
            fixture.writer_without_ahead()
        };
        let warmup = client
            .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
            .unwrap()[0];
        // Complete the warmup driver's prelease and adoption before the gate.
        for _ in 0..2 {
            client.submit_leased_adoption().unwrap();
            client.wait_leased_control(&mut Vec::new(), None).unwrap();
        }
        let (worker, failure) = worker(&fixture, client);
        let through =
            warmup.appended().address().offset + u64::from(warmup.appended().address().len);
        let (reply, checkpoint) = mpsc::sync_channel(1);
        worker
            .send(Command::CheckpointPosition(1, 1, through, reply))
            .unwrap();
        assert!(receive(checkpoint).unwrap().durable_end >= through);
        receive(worker.stats().unwrap()).unwrap();
        assert!(worker.client.lock().leased_test_frontier().1);
        // The gate can arm the owner timeline before taking read observations.
        // Neither typed command should terminate the warm extents.
        let before = fixture.metrics.snapshot().counts;
        if timeline {
            worker.control_handle().owner_timeline_arm(64).unwrap();
        }
        // A typed observation command; generic `Control` would close the extent.
        receive(worker.stats().unwrap()).unwrap();
        let gate = fixture.metrics.snapshot().counts;
        let open = worker.client.lock().leased_test_frontier().1;
        let measured = receive(worker.enqueue(1, vec![fixture.batch(1)]).unwrap()).unwrap()[0];
        let first = fixture.metrics.snapshot().counts;
        eprintln!(
            "lease_ahead={ahead} timeline={timeline}: gate open={open}, skips={}, synchronous closes={}, first-put leases={}",
            gate.leased_skip_markers - before.leased_skip_markers,
            gate.adoption_sync_exchanges - before.adoption_sync_exchanges,
            first.lease_exchanges - gate.lease_exchanges,
        );
        if !open
            || first.lease_exchanges != before.lease_exchanges
            || gate.leased_skip_markers != before.leased_skip_markers
            || gate.adoption_sync_exchanges != before.adoption_sync_exchanges
        {
            cold_restarts.push((ahead, timeline));
        }
        let log = fixture.node.log_for_test(1);
        for (sequence, receipt) in [(0, warmup), (1, measured)] {
            let batch = fixture.batch(sequence);
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(log.read_record(&reference).unwrap(), batch.mutations()[0]);
        }
        drop(worker);
        assert!(!failure.load(Ordering::Acquire));
        // The worker is gone, but the owner may still publish its frontier
        // root asynchronously. Copy the WAL and root like a crash snapshot: a
        // temporary root renamed away during the copy did not exist at that
        // point of the snapshot.
        let recovered_root = tempdir::TempDir::new("leased-gate-recovery").unwrap();
        let stream = fixture.config.root.join("epoch_0000000000000001");
        for entry in std::fs::read_dir(stream).unwrap() {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            match std::fs::copy(entry.path(), recovered_root.path().join(entry.file_name())) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("crash snapshot copy failed: {error}"),
            }
        }
        let recovered = crate::distributed::storage::PartitionLog::recover_live_sealed(
            recovered_root.path(),
            &fixture.config.partition,
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
            // These copied inodes were never exposed by a live target.
            || Ok(()),
        )
        .unwrap();
        for (sequence, receipt) in [(0, warmup), (1, measured)] {
            let batch = fixture.batch(sequence);
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(
                recovered.read_record(&reference).unwrap(),
                batch.mutations()[0]
            );
        }
    }
    assert!(
        cold_restarts.is_empty(),
        "gate discarded warm extents: {cold_restarts:?}"
    );
}

#[test]
fn leased_worker_control_closes_extent_but_stats_does_not() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let (worker, failure) = worker(&fixture, fixture.writer_without_ahead());
    receive(worker.enqueue(1, vec![fixture.batch(0)]).unwrap()).unwrap();
    receive(worker.stats().unwrap()).unwrap();
    assert!(worker.client.lock().leased_test_frontier().1);
    assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 0);
    worker.control(|client| client.stats()).unwrap();
    assert!(!worker.client.lock().leased_test_frontier().1);
    assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 1);
    let receipt = receive(worker.enqueue(1, vec![fixture.batch(1)]).unwrap()).unwrap()[0];
    assert!(receipt.appended().address().offset >= 16384);
    drop(worker);
    // Acquire observes the worker's fatal-outcome publication, if any.
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn leased_worker_queued_checkpoint_then_control_closes_at_dispatch() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let (worker, failure) = worker(&fixture, fixture.writer_without_ahead());
    let guard = worker.client.lock();
    let append = worker.enqueue(1, vec![fixture.batch(0)]).unwrap();
    let (reply, checkpoint) = mpsc::sync_channel(1);
    worker
        .send(Command::CheckpointPosition(1, 1, 4096, reply))
        .unwrap();
    let (reply, control) = mpsc::sync_channel(1);
    worker
        .send(Command::Control(Box::new(move |client| {
            let _ = reply.send(client.leased_test_frontier().1);
        })))
        .unwrap();
    drop(guard);
    receive(append).unwrap();
    assert!(
        checkpoint
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_ok()
    );
    assert!(!control.recv_timeout(Duration::from_secs(5)).unwrap());
    assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 1);
    drop(worker);
    // Acquire observes a fatal outcome, if any, before accepting the test result.
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn leased_worker_queued_checkpoint_then_seal_closes_at_dispatch() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let (worker, failure) = worker(&fixture, fixture.writer_without_ahead());
    let guard = worker.client.lock();
    let append = worker.enqueue(1, vec![fixture.batch(0)]).unwrap();
    let (reply, checkpoint) = mpsc::sync_channel(1);
    worker
        .send(Command::CheckpointPosition(1, 1, 4096, reply))
        .unwrap();
    let sealed = worker.seal(1).unwrap();
    drop(guard);
    receive(append).unwrap();
    assert!(
        checkpoint
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_ok()
    );
    assert!(sealed.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
    assert!(!worker.client.lock().leased_test_frontier().1);
    assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 1);
    drop(worker);
    // Acquire observes a fatal outcome, if any, before accepting the test result.
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn leased_worker_failed_checkpoint_never_dispatches_queued_stats_or_control() {
    for control_first in [false, true] {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let (worker, failure) = worker(&fixture, fixture.writer());
        // A position proof is fixed-through and is submitted with frames
        // still in flight; its fatal reply leaves those frames unknown. So
        // acknowledge the earlier append before queueing the proof.
        let append = worker.enqueue(1, vec![fixture.batch(0)]).unwrap();
        receive(append).unwrap();
        let guard = worker.client.lock();
        let (reply, checkpoint) = mpsc::sync_channel(1);
        // No owner WAL reaches this position: the proof request must fail.
        worker
            .send(Command::CheckpointPosition(1, 1, u64::MAX, reply))
            .unwrap();
        let (reply, stats) = mpsc::sync_channel(1);
        let stats_command = Command::Stats(reply);
        let (reply, control) = mpsc::sync_channel(1);
        let control_command = Command::Control(Box::new(move |_| {
            let _ = reply.send(());
        }));
        let commands = if control_first {
            [control_command, stats_command]
        } else {
            [stats_command, control_command]
        };
        for command in commands {
            worker.send(command).unwrap();
        }
        let later = worker.enqueue(1, vec![fixture.batch(1)]).unwrap();
        drop(guard);
        assert!(
            checkpoint
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .is_err()
        );
        drop(worker);
        assert!(!matches!(
            stats.recv_timeout(Duration::from_secs(5)),
            Ok(Ok(_))
        ));
        assert!(control.recv_timeout(Duration::from_secs(5)).is_err());
        assert!(!matches!(
            later.recv_timeout(Duration::from_secs(5)),
            Ok(Ok(_))
        ));
        // Acquire observes the failure recorded before dispatch was stopped.
        assert!(failure.load(Ordering::Acquire));
        assert_eq!(fixture.metrics.snapshot().counts.leased_frames, 1);
        assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 0);
    }
}

#[test]
fn leased_worker_timeout_detaches_dma_before_blocking_drain() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    client.leased_test_timeout_write();
    let (entered, draining) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    client.leased_test_pause_drain(entered, resume);
    let (worker, failure) = worker(&fixture, client);
    let reply = worker.enqueue(1, vec![fixture.batch(0)]).unwrap();
    assert!(reply.recv_timeout(Duration::from_secs(5)).unwrap().is_err());
    draining.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut guard = worker
        .client
        .try_lock_for(Duration::from_secs(1))
        .expect("terminal drain must not hold the client mutex");
    assert!(
        guard.take_failed_live_writer().is_none(),
        "worker must own the detached DMA queue"
    );
    guard.close_live_writer(&mut Vec::new(), None).unwrap();
    assert_eq!(fixture.metrics.snapshot().counts.leased_write_samples, 0);
    assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 0);
    drop(guard);
    release.send(()).unwrap();
    drop(worker);
    // Acquire observes the failure delivered before the detached drain.
    assert!(failure.load(Ordering::Acquire));
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!(leased_latencies(&counts), [(0, 0); 5]);
    assert_eq!(counts.completed_write_ops, 1);
    assert_eq!(counts.completed_write_bytes, 4096);
    assert_eq!((counts.failed_write_ops, counts.short_write_ops), (0, 0));
}

#[test]
fn leased_worker_failed_merged_frame_marks_every_command_unknown() {
    for result in [-libc::EIO, 2048] {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        client.leased_test_fail_write(result);
        let (worker, failure) = worker(&fixture, client);
        let guard = worker.client.lock();
        let batches = [fixture.batch(0), fixture.batch(1), fixture.batch(2)];
        let replies: Vec<_> = batches
            .iter()
            .map(|batch| worker.enqueue(1, vec![batch.clone()]).unwrap())
            .collect();
        drop(guard);
        for (batch, reply) in batches.iter().zip(replies) {
            let error = reply
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap_err();
            assert_eq!(error.uncertain_version, Some(batch.version()));
        }
        drop(worker);
        assert_eq!(fixture.metrics.snapshot().counts.unknown_writes, 1);
        assert_eq!(fixture.metrics.snapshot().counts.leased_write_samples, 0);
        // Acquire observes the failure of the one merged frame.
        assert!(failure.load(Ordering::Acquire));
    }
}

#[test]
fn leased_worker_completes_flight_while_adoption_pending_without_timer_wait() {
    use crate::distributed::storage::LivePausePoint;
    let fixture = Fixture::new(DurabilityTier::Synchronous, 64 * 1024);
    let mut client = fixture.writer();
    client.leased_test_set_window(1);
    let (entered, release) = fixture
        .node
        .log_for_test(1)
        .pause_live(LivePausePoint::FrameProbe);
    let (worker, failure) = worker(&fixture, client);
    receive(worker.enqueue(1, vec![fixture.batch(0)]).unwrap()).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let before = worker.client.lock().leased_test_flight_adoption_waits();
    for sequence in 1..=20 {
        let batch = fixture.batch(sequence);
        let version = batch.version();
        let replies = receive(worker.enqueue(1, vec![batch]).unwrap()).unwrap();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].appended().version(), version);
    }
    let waits = worker.client.lock().leased_test_flight_adoption_waits() - before;
    // CQE waits may time out on slow local storage. Only the adoption timer
    // wait with a frame in flight identifies the acknowledgement-path bug.
    release.send(()).unwrap();
    drop(worker);
    assert_eq!(waits, 0, "adoption timer blocked an in-flight frame");
    // Acquire observes the worker's fatal-outcome publication, if any.
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn leased_database_shutdown_detaches_failed_skip_without_waking_idle_worker() {
    use super::pipeline_tests::Fixture as DatabaseFixture;
    use crate::key_shape::KeyType;
    for (result, already_poisoned) in [
        (Some(-libc::EIO), false),
        (Some(2048), false),
        (None, false),
        (None, true),
    ] {
        let fixture = DatabaseFixture::new_with_root(
            tempdir::TempDir::new("leased-shutdown").unwrap(),
            1,
            4096,
            KeyShape::new_single(4, 1, KeyType::uniform(1)),
            8,
            |config| {
                // Leave room after one 4 KiB frame for shutdown to write a skip
                // marker. Keep one live flight and no cache or flush batching.
                config.storage.batch_limits.max_operations = 8;
                config.storage.batch_limits.max_encoded_bytes = 8192;
                config.storage.group_limits.max_encoded_bytes = 16376;
                config.storage.fragment_bytes = 16384;
                config.storage.wal_file_bytes = 32768;
                config.storage.max_reference_metadata_bytes = 64 * 1024 * 1024;
                config.timeout = Duration::from_secs(5);
                config.max_admitted_batches = 16;
                config.index_shards = 2;
                config.max_pending_batches = 16;
                config.max_pending_bytes = 4 * 1024 * 1024;
                config.replay_limits = ReplayLimits {
                    max_batches: 4096,
                    max_encoded_bytes: 16 * 1024 * 1024,
                };
                config.owner_append_occupancy = 1;
                config.read_cache_bytes = 0;
                config.read_memory_allowance_bytes = 64 * 1024 * 1024;
                config.scan_prefetch_pages = 1;
                config.read_lanes = Some(1);
                config.max_pending_reads = 4;
                config.max_pending_read_bytes = 128 * 1024;
                config.live_nvme.leased_write_window = Some(1);
                config.live_nvme.leased_flush_min_frame_bytes = 0;
                config.live_nvme.max_inflight_requests = 4;
                config.live_nvme.max_inflight_bytes = 128 * 1024;
                config.live_nvme.connect_timeout_ms = 1000;
                config.live_nvme.io_timeout_ms = 1000;
            },
            |_| {},
        );
        let database = fixture.open();
        let client = database.core.clients[0].client.clone();
        let operation = fixture.operation(0, 0);
        database
            .write_batch(
                operation,
                vec![Mutation::Put {
                    keyspace: KeySpace(0),
                    key: b"key1"[..].into(),
                    value: vec![0; 32].into(),
                }],
            )
            .unwrap();
        let metrics = database.core.live_metrics.as_ref().unwrap().clone();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if metrics.snapshot().counts.adoption_async_exchanges == 1
                && !client.lock().leased_adoption_pending()
            {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        let before = metrics.snapshot().counts;
        if already_poisoned {
            // An existing authority failure is visible before shutdown starts;
            // begin_failure is atomic-only and does not wake the idle worker.
            database
                .core
                .begin_failure(&LocalDbError::new("injected existing failure"));
            assert!(
                database
                    .core
                    .shutdown_with(|_| panic!("poison cannot seal"))
                    .is_err()
            );
            let after = metrics.snapshot().counts;
            assert_eq!(leased_latencies(&after), leased_latencies(&before));
            assert_eq!(after.completed_write_ops, before.completed_write_ops);
            assert_eq!(after.leased_skip_markers, 0);
            assert_eq!(after.adoption_sync_exchanges, 0);
            assert_eq!(after.metadata_requests, before.metadata_requests);
            continue;
        }
        let (entered, draining) = mpsc::sync_channel(1);
        let (release, resume) = mpsc::sync_channel(1);
        {
            let mut client = client.lock();
            if let Some(result) = result {
                client.leased_test_fail_write(result);
            } else {
                client.leased_test_timeout_write();
            }
            client.leased_test_pause_drain(entered, resume);
        }
        // Skip the ordinary checkpoint so shutdown itself initiates the first
        // failing marker write while the healthy owner worker is idle. As the
        // removed `healthy = false`, a non-write failure (read poison) is the
        // primary shutdown error; write poison stays clear, so the close is a
        // normal close rather than a poisoned DMA retirement.
        database.core.test_hooks.lock().skip_shutdown_checkpoint = true;
        database.core.reads.poison();
        thread::scope(|scope| {
            let shutdown = scope.spawn(|| {
                database
                    .core
                    .shutdown_with(|_| panic!("a failed close must not seal"))
            });
            draining.recv_timeout(Duration::from_secs(5)).unwrap();
            let mut guard = client
                .try_lock_for(Duration::from_secs(1))
                .expect("shutdown drain must release the client mutex");
            assert!(guard.take_failed_live_writer().is_none());
            guard.close_live_writer(&mut Vec::new(), None).unwrap();
            assert_eq!(
                leased_latencies(&metrics.snapshot().counts),
                leased_latencies(&before)
            );
            drop(guard);
            release.send(()).unwrap();
            let error = shutdown.join().unwrap().unwrap_err();
            assert!(
                error.to_string().contains("live writer drain failed"),
                "{error}"
            );
        });
        // Acquire observes shutdown's poison before any subsequent worker pass.
        assert!(database.core.concurrent_failure.load(Ordering::Acquire));
        let after = metrics.snapshot().counts;
        assert_eq!(leased_latencies(&after), leased_latencies(&before));
        assert_eq!(
            after.completed_write_ops,
            before.completed_write_ops + u64::from(result.is_none())
        );
        assert_eq!(
            after.completed_write_bytes,
            before.completed_write_bytes + if result.is_none() { 4096 } else { 0 }
        );
        assert_eq!(
            after.failed_write_ops,
            u64::from(result.is_some_and(|n| n < 0))
        );
        assert_eq!(after.short_write_ops, u64::from(result == Some(2048)));
        assert_eq!(
            after.short_write_bytes,
            if result == Some(2048) { 2048 } else { 0 }
        );
        // A retry may find that the failed queue has already been taken; it must
        // neither strand DMA nor double-publish the eventual terminal outcome.
        assert!(
            database
                .core
                .shutdown_with(|_| panic!("poison cannot seal"))
                .is_err()
        );
        let retried = metrics.snapshot().counts;
        assert_eq!(leased_latencies(&retried), leased_latencies(&before));
        assert_eq!(retried.completed_write_ops, after.completed_write_ops);
        assert_eq!(retried.completed_write_bytes, after.completed_write_bytes);
        assert_eq!(retried.failed_write_ops, after.failed_write_ops);
        assert_eq!(retried.short_write_ops, after.short_write_ops);
        assert_eq!(retried.short_write_bytes, after.short_write_bytes);
    }
}

/// Local-file io_uring profile, not a hardware NVMe-oF measurement.
#[test]
#[ignore = "allocates a 10 GiB WAL file; requires at least 11 GiB free"]
fn leased_rotation_profile_native_geometry() {
    use crate::distributed::codec::Mutation;
    use crate::key_shape::{KeyShape, KeySpace};

    let fixture = Fixture::native_geometry();
    println!(
        "leased_rotation_profile geometry: fragment_bytes={}, wal_file_bytes={}, puts=8192, group_batches=16, value_bytes=65536, route=local_file_fixture, client_deadline_seconds=60, owner_io_timeout_seconds=60",
        fixture.config.partition.fragment_bytes, fixture.config.partition.wal_file_bytes,
    );
    let shape = KeyShape::from_yaml(&fixture.config.shape_yaml).unwrap();
    let first_failure = Arc::new(std::sync::OnceLock::new());
    let (worker, failure) =
        worker_with_failure_context(&fixture, fixture.writer(), 16, first_failure.clone());
    let mut calls = Vec::with_capacity(8192);
    for first in (0..8192).step_by(16) {
        let batches: Vec<_> = (first..first + 16)
            .map(|sequence| {
                let original = fixture.batch(sequence);
                CompleteBatch::new(
                    &shape,
                    original.id(),
                    original.version(),
                    vec![Mutation::Put {
                        keyspace: KeySpace(0),
                        key: b"key1"[..].into(),
                        value: vec![sequence as u8; 64 * 1024].into(),
                    }],
                    fixture.config.partition.batch_limits,
                )
                .unwrap()
            })
            .collect();
        // Hold the client only while enqueueing, so the actor sees one ready
        // group of sixteen scalar puts. Capacity is exactly sixteen commands.
        let client = worker.client.lock();
        let replies: Vec<_> = batches
            .into_iter()
            .enumerate()
            .map(|(member, batch)| {
                let started = Instant::now();
                let reply = worker.enqueue(1, vec![batch]).unwrap_or_else(|error| {
                    panic!(
                        "profile enqueue failed: {error}; first_failure={:?}; counters={:#?}",
                        first_failure.get(),
                        fixture.metrics.snapshot().counts
                    )
                });
                (first + member as u64, started, reply)
            })
            .collect();
        drop(client);
        for (sequence, started, reply) in replies {
            let receipts = reply
                .recv_timeout(Duration::from_secs(30))
                .unwrap_or_else(|error| {
                    panic!(
                        "profile reply timed out: {error}; first_failure={:?}; counters={:#?}",
                        first_failure.get(),
                        fixture.metrics.snapshot().counts
                    )
                })
                .unwrap_or_else(|error| {
                    panic!(
                        "profile append failed: {error}; first_failure={:?}; counters={:#?}",
                        first_failure.get(),
                        fixture.metrics.snapshot().counts
                    )
                });
            assert_eq!(receipts.len(), 1);
            calls.push((sequence, started.elapsed().as_nanos()));
        }
    }
    // Capture put-path counters before shutdown adds a final close exchange.
    let settled = worker
        .client
        .lock()
        .settle_leased_extent(&mut Vec::new(), None);
    settled.unwrap_or_else(|error| {
        panic!(
            "profile settle failed: {error}; first_failure={:?}; counters={:#?}",
            first_failure.get(),
            fixture.metrics.snapshot().counts
        )
    });
    let counts = fixture.metrics.snapshot().counts;
    println!(
        "leased_rotation_profile extent_switch_samples={}",
        counts.extent_switch_samples
    );
    println!("leased_rotation_profile counters: {counts:#?}");
    let largest_close = worker.client.lock().leased_test_largest_close_profile();
    println!(
        "leased_rotation_profile largest_close: frames={}, adopt_ns={}",
        largest_close.frames, largest_close.adopt_ns,
    );
    calls.sort_unstable_by_key(|(_, ns)| std::cmp::Reverse(*ns));
    for (rank, (sequence, ns)) in calls.into_iter().take(20).enumerate() {
        println!(
            "leased_rotation_profile slow_call: rank={}, sequence={sequence}, api_ns={ns}",
            rank + 1
        );
    }
    assert_eq!(counts.leased_frames, 512);
    assert!(counts.extent_switch_samples >= 3);
    assert!(counts.lease_reserve_ns > 0);
    assert!(counts.extent_preleases >= counts.extent_switch_samples);
    // A debug owner can validate the old backlog more slowly than callers fill
    // the next extent. Report waits rather than changing the approved driver.
    assert_eq!(counts.adoption_sync_exchanges, 1);
    drop(worker);
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn leased_worker_closes_old_extent_and_preleases_while_appends_continue() {
    use crate::distributed::storage::LivePausePoint;
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let (worker, failure) = worker(&fixture, fixture.writer());
    receive(worker.enqueue(1, vec![fixture.batch(0)]).unwrap()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !worker.client.lock().leased_test_ahead_state().1 {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    for sequence in 1..4 {
        receive(worker.enqueue(1, vec![fixture.batch(sequence)]).unwrap()).unwrap();
    }
    let log = fixture.node.log_for_test(1);
    let (entered, resume) = log.pause_live(LivePausePoint::CloseSuffix);
    let before = fixture.metrics.snapshot().counts.adoption_sync_exchanges;
    receive(worker.enqueue(1, vec![fixture.batch(4)]).unwrap()).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // The old close holds no state lock or coordinator reply. New-extent data
    // reaches its CQE while that close is deliberately blocked.
    receive(worker.enqueue(1, vec![fixture.batch(5)]).unwrap()).unwrap();
    assert_eq!(
        fixture.metrics.snapshot().counts.adoption_sync_exchanges,
        before
    );
    assert!(worker.client.lock().leased_test_ahead_state().0);
    resume.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let client = worker.client.lock();
        let (closing, next) = client.leased_test_ahead_state();
        let pending = client.leased_test_frontier().2;
        drop(client);
        if !closing && next && !pending {
            break;
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(fixture.metrics.snapshot().counts.extent_preleases >= 2);
    assert_eq!(fixture.metrics.snapshot().counts.extent_switch_waits, 0);
    assert_eq!(log.receipt_count(), 6);
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn wal_replay_leased_checkpoint_does_not_settle_the_extent() {
    for pending_prelease in [false, true] {
        let fixture = Fixture::new(DurabilityTier::Native, 128 * 1024);
        let mut client = fixture.writer();
        let receipt = client
            .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
            .unwrap()[0];
        let mut release_prelease = if pending_prelease {
            let (entered, release) = fixture.node.pause_inventory();
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            client.submit_leased_adoption().unwrap();
            assert!(client.leased_test_frontier().2);
            assert_eq!(fixture.metrics.snapshot().counts.extent_preleases, 0);
            Some(release)
        } else {
            None
        };
        let before = fixture.metrics.snapshot().counts.adoption_sync_exchanges;
        let (worker, failure) = worker(&fixture, client);
        let mut through =
            receipt.appended().address().offset + u64::from(receipt.appended().address().len);
        for n in 1..=8 {
            let guard = worker.client.lock();
            let append = worker.enqueue(1, vec![fixture.batch(n)]).unwrap();
            let (reply, checkpoint) = mpsc::sync_channel(1);
            worker
                .send(Command::CheckpointPosition(1, 1, through, reply))
                .unwrap();
            drop(guard);
            // The prelease cannot finish before this checkpoint enters the FIFO.
            if let Some(release) = release_prelease.take() {
                release.send(()).unwrap();
            }
            let receipt = receive(append).unwrap()[0];
            let proof = checkpoint
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            assert!(proof.durable_end >= through);
            through =
                receipt.appended().address().offset + u64::from(receipt.appended().address().len);
            assert!(worker.client.lock().leased_test_frontier().1);
            assert_eq!(
                fixture.metrics.snapshot().counts.adoption_sync_exchanges,
                before
            );
            assert_eq!(fixture.metrics.snapshot().counts.adoption_settle_samples, 0);
        }
        if pending_prelease {
            assert!(fixture.metrics.snapshot().counts.extent_preleases > 0);
        }
        drop(worker);
        assert!(!failure.load(Ordering::Acquire));
    }
}

#[test]
fn wal_replay_leased_appends_continue_while_position_sync_is_pending() {
    let fixture = Fixture::new(DurabilityTier::Native, 128 * 1024);
    let mut client = fixture.writer();
    let first = client
        .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
        .unwrap()[0];
    // Finish initial prelease and adoption before holding the position sync.
    for _ in 0..2 {
        client.submit_leased_adoption().unwrap();
        client.wait_leased_control(&mut Vec::new(), None).unwrap();
    }
    let before = fixture.metrics.snapshot().counts;
    let through = first.appended().address().offset + u64::from(first.appended().address().len);
    let (entered, release) = fixture.node.pause_next_persist(1).unwrap();
    let (worker, failure) = worker(&fixture, client);
    let (reply, checkpoint) = mpsc::sync_channel(1);
    worker
        .send(Command::CheckpointPosition(1, 1, through, reply))
        .unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let overlapping = (|| -> Result<Vec<DurableBatch>, String> {
        let mut receipts = Vec::new();
        for n in 1..=8 {
            let append = worker
                .enqueue(1, vec![fixture.batch(n)])
                .map_err(|error| error.to_string())?;
            let result = append
                .recv_timeout(Duration::from_secs(5))
                .map_err(|error| error.to_string())?;
            receipts.push(result.map_err(|error| error.to_string())?[0]);
            if !matches!(checkpoint.try_recv(), Err(mpsc::TryRecvError::Empty)) {
                return Err("position sync completed while its owner was paused".into());
            }
        }
        Ok(receipts)
    })();
    let deadline = Instant::now() + Duration::from_secs(5);
    while fixture.metrics.snapshot().counts.adopt_profile_samples == before.adopt_profile_samples
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(1));
    }
    let pending = fixture.metrics.snapshot().counts;
    // Release before any failure assertion, so teardown cannot wait on this hook.
    release.send(()).unwrap();
    let receipts = overlapping.expect("direct frames must not wait for the position sync");
    assert_eq!(pending.leased_frames, before.leased_frames + 8);
    assert!(
        pending.adopt_profile_samples > before.adopt_profile_samples,
        "adoption must progress before the position reply"
    );
    assert_eq!(pending.extent_preleases, before.extent_preleases);
    assert_eq!(
        pending.adoption_sync_exchanges,
        before.adoption_sync_exchanges
    );
    let proof = checkpoint
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(proof.durable_end >= through);
    // The durability record covers the captured cut, not these later frames.
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.appended().address().offset > proof.durable_end)
    );
    let log = fixture.node.log_for_test(1);
    for (n, receipt) in (1..=8).zip(receipts) {
        let batch = fixture.batch(n);
        let reference = batch
            .record_reference(receipt.appended().address(), 0)
            .unwrap();
        assert_eq!(log.read_record(&reference).unwrap(), batch.mutations()[0]);
    }
    drop(worker);
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn wal_replay_leased_extent_control_progresses_during_position_sync() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    let mut last = None;
    for n in 0..4 {
        last = Some(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(n)],
                    &WriteTimeline::default(),
                    2,
                )
                .unwrap()[0],
        );
    }
    client.submit_leased_adoption().unwrap();
    client.wait_leased_control(&mut Vec::new(), None).unwrap();
    assert!(client.leased_append_needs_control(None));
    assert_eq!(client.leased_test_frontier().0, 16384);
    let last = last.unwrap().appended();
    let through = last.address().offset + u64::from(last.address().len);
    let (entered, release) = fixture.node.pause_next_persist(1).unwrap();
    let (worker, failure) = worker(&fixture, client);
    let (reply, checkpoint) = mpsc::sync_channel(1);
    worker
        .send(Command::CheckpointPosition(1, 1, through, reply))
        .unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let before = fixture.metrics.snapshot().counts;
    let append = worker.enqueue(1, vec![fixture.batch(4)]).unwrap();
    let early = append.recv_timeout(Duration::from_secs(5));
    let pending = fixture.metrics.snapshot().counts;
    release.send(()).unwrap();
    let receipt = early.unwrap().unwrap()[0];
    assert_eq!(pending.leased_frames, before.leased_frames + 1);
    checkpoint
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(receipt.appended().address().offset >= 16384);
    let batch = fixture.batch(4);
    let reference = batch
        .record_reference(receipt.appended().address(), 0)
        .unwrap();
    assert_eq!(
        fixture
            .node
            .log_for_test(1)
            .read_record(&reference)
            .unwrap(),
        batch.mutations()[0]
    );
    drop(worker);
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn position_is_submitted_during_adoption_and_preserves_control_order() {
    use crate::distributed::storage::LivePausePoint;
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 128 * 1024);
        let mut client = fixture.writer();
        let first = client
            .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
            .unwrap()[0];
        client.wait_leased_control(&mut Vec::new(), None).unwrap(); // Initial prelease.
        let log = fixture.node.log_for_test(1);
        let through = first.appended().address().offset + u64::from(first.appended().address().len);
        assert!(through > log.appended_frontier());
        let (entered, release) = log.pause_live(LivePausePoint::FrameProbe);
        client.submit_leased_adoption().unwrap();
        let (worker, failure) = worker(&fixture, client);
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        // Position persistence does not wait for adoption. Hold its own hook so
        // the pending slot cannot disappear before the passing append replies.
        let (position_entered, release_position) =
            log.pause_live(LivePausePoint::PositionBeforeData);
        let guard = worker.client.lock();
        let (reply, proof) = mpsc::sync_channel(1);
        worker
            .send(Command::CheckpointPosition(1, 1, through, reply))
            .unwrap();
        let passing = worker.enqueue(1, vec![fixture.batch(1)]).unwrap();
        let (reply, control) = mpsc::sync_channel(1);
        worker
            .send(Command::Control(Box::new(move |client| {
                let _ = reply.send(client.leased_test_frontier().1);
            })))
            .unwrap();
        let later = worker.enqueue(1, vec![fixture.batch(2)]).unwrap();
        drop(guard);
        let position_submitted = position_entered.recv_timeout(Duration::from_secs(5));
        let passed = passing.recv_timeout(Duration::from_secs(5));
        let submitted =
            position_submitted.is_ok() && worker.client.lock().checkpoint_position_pending();
        let proof_pending = matches!(proof.try_recv(), Err(mpsc::TryRecvError::Empty));
        let control_pending = matches!(control.try_recv(), Err(mpsc::TryRecvError::Empty));
        let later_pending = matches!(later.try_recv(), Err(mpsc::TryRecvError::Empty));
        let position_released = release_position.send(());
        // Force the order that the old test raced: the proof completes while
        // adoption still waits. Lifecycle control and the later append must not.
        let completed = proof.recv_timeout(Duration::from_secs(5));
        let control_waits_for_adoption =
            matches!(control.try_recv(), Err(mpsc::TryRecvError::Empty));
        let later_waits_for_adoption = matches!(later.try_recv(), Err(mpsc::TryRecvError::Empty));
        let source = fixture.config.root.join("epoch_0000000000000001");
        let captured_root = std::fs::read(source.join("partition-wal-root-v1"));
        let adoption_released = release.send(());
        // Release both workers before any assertion can start teardown.
        position_submitted.unwrap();
        position_released.unwrap();
        adoption_released.unwrap();
        let passed = passed.unwrap().unwrap()[0];
        assert!(submitted, "position must be submitted during adoption");
        assert!(
            proof_pending,
            "position must wait at its own persistence hook"
        );
        assert!(control_pending, "control must wait for the position proof");
        assert!(later_pending, "later append must wait behind control");
        assert!(
            control_waits_for_adoption,
            "control must still wait for adoption after the position proof"
        );
        assert!(
            later_waits_for_adoption,
            "later append must still wait behind control after the position proof"
        );
        assert!(passed.appended().address().offset > through);
        assert!(completed.unwrap().unwrap().durable_end >= through);
        let captured_root = captured_root.unwrap();
        assert!(!control.recv_timeout(Duration::from_secs(5)).unwrap());
        let later = later.recv_timeout(Duration::from_secs(5)).unwrap().unwrap()[0];
        assert!(later.appended().address().offset >= 128 * 1024);
        receive(worker.seal(1).unwrap()).unwrap();
        for (n, receipt) in [(0, first), (1, passed), (2, later)] {
            let batch = fixture.batch(n);
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(log.read_record(&reference).unwrap(), batch.mutations()[0]);
        }
        // Recover a real persisted checkpoint root plus its later durable suffix.
        // The owner is sealed before copying; no metadata writer races this image.
        let image = tempdir::TempDir::new("parked-position-recovery").unwrap();
        for entry in std::fs::read_dir(&source).unwrap() {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            std::fs::copy(entry.path(), image.path().join(entry.file_name())).unwrap();
        }
        std::fs::write(image.path().join("partition-wal-root-v1"), captured_root).unwrap();
        let recovered = crate::distributed::storage::PartitionLog::recover_live_sealed(
            image.path(),
            &fixture.config.partition,
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
            // These copied inodes have never been exported to a writable target.
            || Ok(()),
        )
        .unwrap();
        for (n, receipt) in [(0, first), (1, passed), (2, later)] {
            let batch = fixture.batch(n);
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(
                recovered.read_record(&reference).unwrap(),
                batch.mutations()[0]
            );
        }
        drop(worker);
        assert!(!failure.load(Ordering::Acquire));
    }
}

#[test]
fn parked_position_requests_prelease_without_blocking_appends_or_shutdown() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 128 * 1024);
        let mut client = fixture.writer_without_ahead();
        let first = client
            .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
            .unwrap()[0];
        client.leased_test_enable_ahead();
        let (entered, release) = fixture.node.pause_inventory();
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let through = first.appended().address().offset + u64::from(first.appended().address().len);
        let (mut worker, failure) = worker(&fixture, client);
        let guard = worker.client.lock();
        let (reply, proof) = mpsc::sync_channel(1);
        worker
            .send(Command::CheckpointPosition(1, 1, through, reply))
            .unwrap();
        let append = worker.enqueue(1, vec![fixture.batch(1)]).unwrap();
        drop(guard);
        let passed = append.recv_timeout(Duration::from_secs(5));
        let proof_pending = matches!(proof.try_recv(), Err(mpsc::TryRecvError::Empty));
        // Closing the sender must drain the parked proof, not discard it.
        worker.sender.take();
        release.send(()).unwrap();
        let passed = passed.unwrap().unwrap()[0];
        assert!(proof_pending);
        assert!(passed.appended().address().offset > through);
        assert!(
            proof
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap()
                .durable_end
                >= through
        );
        drop(worker);
        assert!(fixture.metrics.snapshot().counts.extent_preleases >= 1);
        assert!(!failure.load(Ordering::Acquire));
    }
}

#[test]
fn failed_append_poison_replies_to_inflight_position() {
    use crate::distributed::storage::LivePausePoint;
    let fixture = Fixture::new(DurabilityTier::Native, 128 * 1024);
    let mut client = fixture.writer();
    let first = client
        .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
        .unwrap()[0];
    client.wait_leased_control(&mut Vec::new(), None).unwrap();
    let (entered, release) = fixture
        .node
        .log_for_test(1)
        .pause_live(LivePausePoint::PositionBeforeData);
    client.leased_test_fail_write(-libc::EIO);
    let (worker, failure) = worker(&fixture, client);
    let through = first.appended().address().offset + u64::from(first.appended().address().len);
    let (reply, proof) = mpsc::sync_channel(1);
    worker
        .send(Command::CheckpointPosition(1, 1, through, reply))
        .unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let append = worker.enqueue(1, vec![fixture.batch(1)]).unwrap();
    let result = append.recv_timeout(Duration::from_secs(5));
    let proof_result = proof.recv_timeout(Duration::from_secs(5));
    release.send(()).unwrap();
    assert!(result.unwrap().is_err());
    assert!(proof_result.unwrap().is_err());
    drop(worker);
    assert!(failure.load(Ordering::Acquire));
}

#[test]
fn actual_frame_can_use_runway_smaller_than_the_maximum_frame() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    let mut first = None;
    for n in 0..2 {
        first = Some(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(n)],
                    &WriteTimeline::default(),
                    2,
                )
                .unwrap()[0],
        );
    }
    client.submit_leased_adoption().unwrap();
    client.wait_leased_control(&mut Vec::new(), None).unwrap();
    assert!(client.leased_append_needs_control(None));
    assert!(!client.leased_append_needs_control(Some(4096)));
    let last = first.unwrap().appended();
    let through = last.address().offset + u64::from(last.address().len);
    let (entered, release) = fixture.node.pause_next_persist(1).unwrap();
    let (worker, failure) = worker(&fixture, client);
    let (reply, proof) = mpsc::sync_channel(1);
    worker
        .send(Command::CheckpointPosition(1, 1, through, reply))
        .unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let append = worker.enqueue(1, vec![fixture.batch(2)]).unwrap();
    let result = append.recv_timeout(Duration::from_secs(5));
    let pending = matches!(proof.try_recv(), Err(mpsc::TryRecvError::Empty));
    release.send(()).unwrap();
    assert!(result.unwrap().is_ok());
    assert!(pending);
    assert!(proof.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
    drop(worker);
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn leased_worker_keeps_window_full_and_replies_in_admission_order() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 64 * 1024);
        let mut client = fixture.writer();
        client.leased_test_set_window(4);
        client.leased_test_single_batch_groups();
        client.leased_test_hold_completions(&[1, 2, 3, 4]);
        let (worker, failure) = worker(&fixture, client);
        let guard = worker.client.lock();
        let replies: Vec<_> = (0..8)
            .map(|n| worker.enqueue(1, vec![fixture.batch(n)]).unwrap())
            .collect();
        drop(guard);
        let deadline = Instant::now() + Duration::from_secs(5);
        while fixture.metrics.snapshot().counts.max_leased_in_flight < 4 {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        for reply in &replies {
            assert!(matches!(reply.try_recv(), Err(mpsc::TryRecvError::Empty)));
        }
        worker.client.lock().leased_test_release_held();
        for (sequence, reply) in replies.into_iter().enumerate() {
            let receipts = reply.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
            assert_eq!(
                receipts[0].appended().version(),
                fixture.batch(sequence as u64).version()
            );
        }
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.max_leased_in_flight, 4);
        assert_eq!(counts.leased_frames, 8);
        drop(worker);
        assert!(!failure.load(Ordering::Acquire));
    }
}

#[test]
fn leased_worker_without_window_reaches_credit_bound_and_preserves_admission_order() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 128 * 1024);
        let mut client = fixture.writer();
        client.leased_test_no_window(16);
        client.leased_test_single_batch_groups();
        assert_eq!(client.leased_write_window(), 16);
        client.leased_test_hold_completions(&(1..=16).collect::<Vec<_>>());
        let (worker, failure) = worker(&fixture, client);
        let guard = worker.client.lock();
        let replies: Vec<_> = (0..16)
            .map(|sequence| worker.enqueue(1, vec![fixture.batch(sequence)]).unwrap())
            .collect();
        drop(guard);
        let deadline = Instant::now() + Duration::from_secs(5);
        while fixture.metrics.snapshot().counts.max_leased_in_flight < 16 {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        for reply in &replies {
            assert!(matches!(reply.try_recv(), Err(mpsc::TryRecvError::Empty)));
        }
        worker.client.lock().leased_test_release_held();
        let mut previous_end = 0;
        for (sequence, reply) in replies.into_iter().enumerate() {
            let receipts = reply.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
            assert_eq!(receipts.len(), 1);
            assert_eq!(
                receipts[0].appended().version(),
                fixture.batch(sequence as u64).version()
            );
            let position = receipts[0].appended().address();
            assert!(position.offset >= previous_end);
            previous_end = position.offset + u64::from(position.len);
        }
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.max_leased_in_flight, 16);
        assert_eq!(counts.leased_frames, 16);
        drop(worker);
        assert!(!failure.load(Ordering::Acquire));
    }
}

#[test]
fn leased_window_failed_middle_write_marks_every_flight_unknown() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 32768);
        let mut client = fixture.writer();
        client.leased_test_set_window(4);
        client.leased_test_single_batch_groups();
        client.leased_test_hold_completions(&[1, 2, 3, 4]);
        client.leased_test_fail_flight(2, -libc::EIO);
        let (worker, failure) = worker(&fixture, client);
        let guard = worker.client.lock();
        let replies: Vec<_> = (0..4)
            .map(|n| worker.enqueue(1, vec![fixture.batch(n)]).unwrap())
            .collect();
        drop(guard);
        let deadline = Instant::now() + Duration::from_secs(5);
        while fixture.metrics.snapshot().counts.max_leased_in_flight < 4 {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        worker.client.lock().leased_test_release_held();
        let mut released_prefix = 0;
        for (sequence, reply) in replies.into_iter().enumerate() {
            let result = reply.recv_timeout(Duration::from_secs(5)).unwrap();
            if sequence == 0
                && let Ok(receipts) = &result
            {
                // Frame 1 may release before frame 2's failed CQE is observed.
                assert_eq!(receipts.len(), 1);
                assert_eq!(receipts[0].appended().version(), fixture.batch(0).version());
                released_prefix = 1;
            } else {
                let error = result.unwrap_err();
                assert!(error.to_string().contains("unknown"), "{error}");
            }
        }
        assert!(failure.load(Ordering::Acquire));
        let client = Arc::clone(&worker.client);
        drop(worker);
        assert!(
            client
                .lock()
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(4)],
                    &WriteTimeline::default(),
                    2
                )
                .is_err()
        );
        assert_eq!(
            fixture.metrics.snapshot().counts.leased_frames,
            released_prefix
        );
    }
}

#[test]
fn leased_worker_position_proof_bypasses_an_unreleased_frame() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 32768);
        let mut client = fixture.writer_without_ahead();
        client.leased_test_single_batch_groups();
        client.leased_test_hold_completions(&[1]);
        let (worker, failure) = worker(&fixture, client);
        let guard = worker.client.lock();
        let first = worker.enqueue(1, vec![fixture.batch(0)]).unwrap();
        let second = worker.enqueue(1, vec![fixture.batch(1)]).unwrap();
        drop(guard);
        // A held CQE is not delivered, so published metrics omit it. CQE 2 can
        // be reaped before CQE 1; wait on the queue's own reaped count.
        let deadline = Instant::now() + Duration::from_secs(5);
        while worker.client.lock().leased_test_reaped_writes() < 2 {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        let through = worker.client.lock().leased_test_frontier().0;
        assert_eq!(through, 0);
        let (reply, proof) = mpsc::sync_channel(1);
        worker
            .send(Command::CheckpointPosition(1, 1, through, reply))
            .unwrap();
        proof.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        assert!(through <= worker.client.lock().leased_test_frontier().0);
        assert!(matches!(first.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert!(matches!(second.try_recv(), Err(mpsc::TryRecvError::Empty)));
        worker.client.lock().leased_test_release_held();
        assert_eq!(
            first.recv_timeout(Duration::from_secs(5)).unwrap().unwrap()[0]
                .appended()
                .version(),
            fixture.batch(0).version()
        );
        assert_eq!(
            second
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap()[0]
                .appended()
                .version(),
            fixture.batch(1).version()
        );
        drop(worker);
        assert!(!failure.load(Ordering::Acquire));
    }
}
