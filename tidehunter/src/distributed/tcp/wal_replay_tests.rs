use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::tcp::live_client::leased_tests::Fixture;
use crate::distributed::write_timeline::WriteTimeline;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Cap of a test wait that follows an observed signal, and of the shortened
/// replay-page control deadline (TST-9). The signal decides, the cap bounds: a
/// busy laptop can delay a thread hand-off, and a fixture page reads in
/// milliseconds, so five seconds bounds both without asserting any duration.
const WAIT_CAP: Duration = Duration::from_secs(5);

// Partitions of the replay-page fixtures. The owner event capture is
// process-global and keyed by envelope id, and envelope ids restart per
// session, so each page test owns a partition and the event watch filters by
// it; a parallel run cannot pair another test's start and reply.
const PARTITION_WAIT: u32 = u32::MAX - 7;
const PARTITION_BARRIER: u32 = u32::MAX - 8;
const PARTITION_COMPLETES: u32 = u32::MAX - 9;
const PARTITION_FULL_QUEUE: u32 = u32::MAX - 10;
const PARTITION_SHUTDOWN: u32 = u32::MAX - 11;
const PARTITION_RETRY: u32 = u32::MAX - 12;
const PARTITION_RECONNECT: u32 = u32::MAX - 13;
const PARTITION_EVICT: u32 = u32::MAX - 14;

fn position_proof(
    node: &StorageNode,
    authority: u64,
    epoch: u64,
    through: u64,
) -> checkpoint::CheckpointPositionProof {
    match node
        .handle(
            Request::CheckpointPosition {
                authority_epoch: authority,
                epoch,
                through,
            },
            TcpLimits::default(),
        )
        .unwrap()
    {
        Response::CheckpointPosition {
            database,
            partition,
            epoch,
            durable_end,
        } => checkpoint::CheckpointPositionProof {
            database,
            partition,
            epoch,
            durable_end,
        },
        _ => panic!("expected position proof"),
    }
}

#[test]
fn checkpoint_position_syncs_and_advances_owner_root() {
    let temp = tempdir::TempDir::new("tcp-wal-position").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let receipt = node
        .inventory
        .lock()
        .active(1)
        .unwrap()
        .append_complete_batch(RequestAuthority { epoch: 1 }, &batch(&config, 1, 0))
        .unwrap();
    let through = receipt.address().offset + u64::from(receipt.address().len);
    let proof = position_proof(&node, 1, 1, through);
    assert_eq!(proof.durable_end, through);
    assert_eq!(position_proof(&node, 1, 1, 0).durable_end, through);
    assert!(
        config
            .root
            .join("epoch_0000000000000001/partition-wal-root-v1")
            .is_file()
    );
    drop(node);
    let node = StorageNode::open(config, metrics()).unwrap();
    node.inventory.lock().fence_to(2).unwrap();
    assert_eq!(position_proof(&node, 2, 1, 0), proof);
}

#[test]
fn replay_live_wal_page_reads_frames_through_namespace_fixture() {
    use crate::distributed::durability::DurabilityTier;
    use crate::distributed::tcp::live_client::leased_tests::Fixture;
    use crate::distributed::write_timeline::WriteTimeline;
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    let batches: Vec<_> = (0..3).map(|n| fixture.batch(n)).collect();
    let receipts = client
        .submit_and_drain_leased_group(1, batches.clone(), &WriteTimeline::default(), 1)
        .unwrap();
    let last = receipts.last().unwrap().appended().address();
    let through = last.offset + u64::from(last.len);
    // The position sync must not settle or close the unadopted extent.
    let before = fixture.metrics.snapshot().counts.adoption_sync_exchanges;
    assert!(
        client
            .checkpoint_position(1, 1, through)
            .unwrap()
            .durable_end
            >= through
    );
    assert_eq!(
        fixture.metrics.snapshot().counts.adoption_sync_exchanges,
        before
    );
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
    client.fence_to(2).unwrap();
    let pool_dir = tempdir::TempDir::new("replay-pool").unwrap();
    let registry = crate::distributed::nvmeof::live_connect::LiveConnectionRegistry::open(
        &pool_dir.path().join("registry"),
        crate::distributed::nvmeof::live_connect::RegistryMode::Retire,
    )
    .unwrap();
    client.set_live_pool(Arc::new(
        crate::distributed::nvmeof::live_pool::LiveControllerPool::new(registry, 8, 4).unwrap(),
    ));
    let page = client.replay_live_wal_page(2, 1, 0, 1, through).unwrap();
    assert_eq!(page.batches.len(), 3);
    for ((actual, receipt), (expected, expected_receipt)) in
        page.batches.iter().zip(batches.iter().zip(&receipts))
    {
        assert_eq!(actual.encoded(), expected.encoded());
        assert_eq!(receipt, expected_receipt);
    }
}

/// One sealed recovery source on a real owner, plus what a page reader needs.
/// Epoch 1 holds three leased batches and is sealed by fencing to epoch 2.
/// Epoch 2 holds one live batch, so the active log has an append beyond its
/// durable prefix for the barrier and persist tests.
struct PageSource {
    /// The writer client, now serving epoch 2. It drops before the fixture:
    /// its connection must close while the owner is still live.
    writer: Client,
    fixture: Fixture,
    _pool_dir: tempdir::TempDir,
    pool: Arc<crate::distributed::nvmeof::live_pool::LiveControllerPool>,
    live_config: LiveClientConfig,
    through: u64,
    /// Durable frontier of the serving epoch that its position proof must cover.
    through2: u64,
}

impl PageSource {
    fn new(partition: u32) -> Self {
        // The owner serves the fixture's first listed partition, so every
        // owner event of this source carries it.
        let fixture = Fixture::new_with_partitions(DurabilityTier::Native, 16384, vec![partition]);
        let (through, live_config, writer, through2) = {
            let mut writer = fixture.writer_without_ahead();
            let batches: Vec<_> = (0..3).map(|n| fixture.batch(n)).collect();
            let receipts = writer
                .submit_and_drain_leased_group(1, batches, &WriteTimeline::default(), 1)
                .unwrap();
            let last = receipts.last().unwrap().appended().address();
            let through = last.offset + u64::from(last.len);
            // The position proof must not settle or close the unadopted extent.
            writer.checkpoint_position(1, 1, through).unwrap();
            writer.close_leased_extent(&mut Vec::new(), None).unwrap();
            writer.fence_to(2).unwrap();
            let live_config = writer
                .live_config
                .clone()
                .expect("the writer is configured");
            let receipts = writer
                .submit_and_drain_leased_group(
                    2,
                    vec![super::batch(&fixture.config, 2, 3)],
                    &WriteTimeline::default(),
                    1,
                )
                .unwrap();
            let last = receipts.last().unwrap().appended().address();
            let through2 = last.offset + u64::from(last.len);
            (through, live_config, writer, through2)
        };
        let pool_dir = tempdir::TempDir::new("replay-page-source").unwrap();
        let registry = crate::distributed::nvmeof::live_connect::LiveConnectionRegistry::open(
            &pool_dir.path().join("registry"),
            crate::distributed::nvmeof::live_connect::RegistryMode::Retire,
        )
        .unwrap();
        let pool = Arc::new(
            crate::distributed::nvmeof::live_pool::LiveControllerPool::new(registry, 8, 4).unwrap(),
        );
        Self {
            fixture,
            _pool_dir: pool_dir,
            pool,
            live_config,
            through,
            writer,
            through2,
        }
    }

    /// One more live batch in the serving epoch. Returns its end position.
    fn append_serving_batch(&mut self, sequence: u64) -> u64 {
        let receipts = self
            .writer
            .submit_and_drain_leased_group(
                2,
                vec![super::batch(&self.fixture.config, 2, sequence)],
                &WriteTimeline::default(),
                1,
            )
            .unwrap();
        let last = receipts.last().unwrap().appended().address();
        last.offset + u64::from(last.len)
    }

    /// A control-only page reader, as at open, where the replay runs before any
    /// live writer is attached.
    fn client(&self) -> Client {
        let mut client = self.fixture.client();
        client
            .configure_live_nvme(
                &self.live_config,
                crate::distributed::remote_db::RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY,
            )
            .unwrap();
        client.live_write_local_fixture = true;
        client
            .set_live_metrics(self.fixture.metrics.clone())
            .unwrap();
        client.set_live_pool(self.pool.clone());
        client
    }
}

/// The suspected wait, proved deterministically. A Fence owns the owner's
/// single exclusive control slot through the existing `before_fence` pause. A
/// replay page that the session already passed to the control owner then
/// produces no reply, so its control deadline expires with the typed timeout
/// class. After the fence is released, a fresh page of the same sealed epoch
/// completes under the newer authority. Every wait follows an observed signal.
#[test]
fn replay_page_waits_behind_a_running_exclusive_control() {
    use std::sync::mpsc::sync_channel;
    let source = PageSource::new(PARTITION_WAIT);
    let through = source.through;
    // Every client is connected before the fence holds the owner's control
    // owner: a Status handshake is a control request too, so a later connect
    // would wait behind the same exclusive slot (which is the point here).
    let mut fencer = source.client();
    let mut blocked = source.client();
    let mut released = source.client();
    let (entered_tx, entered_rx) = sync_channel(1);
    let (resume_tx, resume_rx) = sync_channel(1);
    *source.fixture.node.control_context.before_fence.lock() = Some(Box::new(move || {
        let _ = entered_tx.send(());
        let _ = resume_rx.recv();
    }));
    let fence = std::thread::spawn(move || fencer.fence_to(3));
    assert!(
        entered_rx.recv_timeout(WAIT_CAP).is_ok(),
        "the held fence never started, so no exclusive control owns the slot"
    );
    blocked.reactor.control_cap = Some(WAIT_CAP);
    let page = std::thread::spawn(move || blocked.replay_live_wal_page(2, 1, 0, 1, through));
    // Follow observed progress: the owner holds the fence and now holds the
    // page request too, so the page waits for the exclusive slot.
    let admitted_by = Instant::now() + WAIT_CAP;
    while source.fixture.node.control.pending_requests_for_test() < 2 {
        assert!(
            Instant::now() < admitted_by,
            "the owner never admitted the replay page while the fence ran"
        );
        std::thread::yield_now();
    }
    let error = match page.join().expect("the page thread panicked") {
        Ok(page) => panic!(
            "the page must not answer while the fence owns the exclusive slot; got {} batches",
            page.batches.len()
        ),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("remote operation deadline expired"),
        "the waiting page reports the control deadline: {error}"
    );
    // Release the fence; the same sealed epoch is replayable under authority 3.
    resume_tx.send(()).expect("the fence holder is live");
    fence
        .join()
        .expect("the fence thread panicked")
        .expect("the fence completes after release");
    released.reactor.control_cap = Some(WAIT_CAP);
    let page = released
        .replay_live_wal_page(3, 1, 0, 1, through)
        .expect("the released page completes");
    assert_eq!(
        page.batches.len(),
        3,
        "the released page returned {} batches",
        page.batches.len()
    );
}

/// Production cap override of the flush barrier in the barrier test, not a
/// patience bound (TST-9's one patience bound is `WAIT_CAP`). It is long enough
/// for a healthy flusher on a slow laptop and short enough to keep the test
/// quick.
const BARRIER_CAP: Duration = Duration::from_millis(500);

/// Fix 1: a flusher that cannot answer a barrier fails that one page with the
/// typed, named barrier error; nothing is poisoned, and the same page completes
/// once the flusher resumes.
#[test]
fn replay_page_barrier_names_the_flush_barrier_timeout() {
    let mut source = PageSource::new(PARTITION_BARRIER);
    // Both readers connect before the flusher is held: a Status handshake is
    // an exclusive control and would itself wait at the held barrier.
    let mut blocked = source.client();
    let mut released = source.client();
    let log = source.fixture.node.log_for_test(2);
    let (_, persist_resume) = log.pause_next_persist();
    // An append after the pause is armed keeps the serving log beyond its
    // durable prefix, so the page's barrier must persist it. Either the log's
    // syncer or the barrier takes the pause; both orders block the barrier.
    source.append_serving_batch(4);
    source
        .fixture
        .node
        .control
        .set_flush_barrier_cap_for_test(BARRIER_CAP);
    blocked.reactor.control_cap = Some(WAIT_CAP);
    let error = match blocked.replay_live_wal_page(2, 1, 0, 1, source.through) {
        Ok(page) => panic!(
            "the barrier cannot answer while the flusher is paused; got {} batches",
            page.batches.len()
        ),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::TimedOut,
        "the barrier timeout keeps the typed class: {error}"
    );
    assert!(
        error.to_string().contains("flush barrier"),
        "the error names the flush barrier stage: {error}"
    );
    // Release the flusher: the same page completes, so the timeout poisoned
    // nothing and left no state behind.
    persist_resume.send(()).expect("the paused flush is live");
    source
        .fixture
        .node
        .control
        .set_flush_barrier_cap_for_test(WAIT_CAP);
    released.reactor.control_cap = Some(WAIT_CAP);
    let page = released
        .replay_live_wal_page(2, 1, 0, 1, source.through)
        .expect("the page completes after the flusher resumes");
    assert_eq!(
        page.batches.len(),
        3,
        "the released page returned {} batches",
        page.batches.len()
    );
}

/// Fix 2: the page control of a sealed epoch completes while a stream request
/// of the serving epoch holds an append ticket. The page must not wait for
/// stream work; its own bytes are immutable and the gate orders lifecycle
/// changes. The reply is asserted at the owner (a `ReplayPageStart` event
/// followed by its `ControlReplied`), not by the client call: the call also
/// releases the page's epoch pin, a lifecycle control that stays ordered
/// behind the parked stream request.
#[test]
fn replay_page_completes_while_a_stream_request_is_active() {
    use crate::distributed::owner_events::{self, OwnerEventKind};
    let _capture = owner_events::OWNER_EVENTS_TEST_LOCK.lock();
    owner_events::enable(65_536);
    let source = PageSource::new(PARTITION_COMPLETES);
    // Start from an empty capture: `dropped` then counts only this test.
    owner_events::drain_json(&source._pool_dir.path().join("prior-owner-events.json")).unwrap();
    // Both readers connect before a stream request holds its append ticket: a
    // Status handshake is an exclusive control and waits for stream work.
    let mut prover = source.client();
    let mut page_reader = source.client();
    let (stream_entered, stream_resume) = source.fixture.node.control.pause_next_stream_request();
    let through2 = source.through2;
    let proof = std::thread::spawn(move || prover.checkpoint_position(2, 2, through2));
    assert!(
        stream_entered.recv_timeout(WAIT_CAP).is_ok(),
        "the position proof never took its append ticket"
    );
    let (before, dropped) = owner_events::snapshot();
    assert_eq!(
        dropped, 0,
        "the owner event capture dropped {dropped} events"
    );
    let before = before.len();
    page_reader.reactor.control_cap = Some(WAIT_CAP);
    let through = source.through;
    let page = std::thread::spawn(move || page_reader.replay_live_wal_page(2, 1, 0, 1, through));
    // Follow observed progress: the owner starts the page control and replies
    // to it while the position proof still holds its append ticket.
    let replied_by = Instant::now() + WAIT_CAP;
    let page_id = loop {
        let (events, dropped) = owner_events::snapshot();
        assert_eq!(
            dropped, 0,
            "the owner event capture dropped {dropped} events"
        );
        let started = events[before..]
            .iter()
            .find(|event| {
                event.partition == PARTITION_COMPLETES
                    && event.kind == OwnerEventKind::ReplayPageStart
            })
            .map(|event| event.a);
        if let Some(id) = started
            && events[before..].iter().any(|event| {
                event.partition == PARTITION_COMPLETES
                    && event.kind == OwnerEventKind::ControlReplied
                    && event.a == id
            })
        {
            break id;
        }
        assert!(
            Instant::now() < replied_by,
            "the owner did not reply to a sealed-epoch page while a stream request was active"
        );
        std::thread::yield_now();
    };
    assert!(
        page_id != 0,
        "the page reply is recorded under a request id"
    );
    // Release the stream request: the page's epoch pin release was ordered
    // behind it and the call now returns the page it read.
    stream_resume
        .send(())
        .expect("the paused position proof is live");
    let page = page
        .join()
        .expect("the page thread panicked")
        .expect("the page completes after the stream request resumes");
    assert_eq!(
        page.batches.len(),
        3,
        "the page returned {} batches",
        page.batches.len()
    );
    let proof = proof
        .join()
        .expect("the proof thread panicked")
        .expect("the position proof completes");
    assert!(
        proof.durable_end >= through2,
        "the proof covers the serving epoch: {} of {through2}",
        proof.durable_end
    );
}

/// A barrier control rejected because the flush queue is full fails alone: the
/// control owner still admits a read-owned control, and the flusher drains the
/// abandoned barriers once it resumes.
#[test]
fn a_full_flush_queue_rejects_one_control_and_keeps_the_owner_answering() {
    let mut source = PageSource::new(PARTITION_FULL_QUEUE);
    let mut client = source.client();
    let log = source.fixture.node.log_for_test(2);
    let (_, persist_resume) = log.pause_next_persist();
    // The pending append makes the barrier persist; either the log's syncer or
    // the barrier takes the pause, and both orders block the flusher.
    source.append_serving_batch(4);
    source
        .fixture
        .node
        .control
        .set_flush_barrier_cap_for_test(Duration::ZERO);
    let mut rejected = false;
    for _ in 0..control::FLUSH_QUEUE + 4 {
        match client.status() {
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    error.to_string().contains("flush queue is full"),
                    "the full queue names its stage: {error}"
                );
                rejected = true;
                break;
            }
            Err(error) => assert!(
                error.to_string().contains("flush barrier"),
                "only a barrier deadline is expected while the flusher is held: {error}"
            ),
            Ok(_) => panic!(
                "a barrier control completed while the flusher was held and the cap was zero"
            ),
        }
    }
    assert!(
        rejected,
        "the queue fills within FLUSH_QUEUE plus its margin"
    );
    // The owner still schedules and answers controls: the next one takes the
    // same typed rejection promptly instead of hanging on the queue lock.
    let again = client.status().expect_err("the queue is still full");
    assert_eq!(
        again.kind(),
        std::io::ErrorKind::WouldBlock,
        "the owner answers the next control while the queue is full: {again}"
    );
    // Release the flusher: it drains the abandoned barriers and answers again.
    persist_resume.send(()).expect("the paused flush is live");
    source
        .fixture
        .node
        .control
        .set_flush_barrier_cap_for_test(WAIT_CAP);
    let answered_by = Instant::now() + WAIT_CAP;
    let mut answered = false;
    while Instant::now() < answered_by {
        match client.status() {
            Ok(_) => {
                answered = true;
                break;
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || (error.kind() == std::io::ErrorKind::TimedOut
                        && error.to_string().contains("flush barrier")) =>
            {
                // Still draining: the queue is full or this barrier waited
                // behind the backlog. Retry within the bound.
            }
            Err(error) => panic!("an unexpected control failure while draining: {error}"),
        }
        std::thread::yield_now();
    }
    assert!(
        answered,
        "the owner answers once the flusher drains the queue"
    );
}

/// Shutdown must not hold the queue lock while it waits for room in a full
/// flush channel: the flusher's failure path needs that lock, and every other
/// user of the owner would stall behind a teardown.
#[test]
fn a_shutdown_barrier_does_not_hold_the_queue_lock() {
    let mut source = PageSource::new(PARTITION_SHUTDOWN);
    let mut client = source.client();
    let log = source.fixture.node.log_for_test(2);
    let (_, persist_resume) = log.pause_next_persist();
    // The pending append makes the barrier persist; either the log's syncer or
    // the barrier takes the pause, and both orders block the flusher.
    source.append_serving_batch(4);
    source
        .fixture
        .node
        .control
        .set_flush_barrier_cap_for_test(Duration::ZERO);
    // Fill the channel with the barriers of timed-out controls.
    let mut full = false;
    for _ in 0..control::FLUSH_QUEUE + 4 {
        match client.status() {
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                full = true;
                break;
            }
            Err(error) => assert!(
                error.to_string().contains("flush barrier"),
                "only a barrier deadline is expected while the flusher is held: {error}"
            ),
            Ok(_) => panic!(
                "a barrier control completed while the flusher was held and the cap was zero"
            ),
        }
    }
    assert!(full, "the queue fills within FLUSH_QUEUE plus its margin");
    // Shutdown waits for room after it stops admissions. Its wait must not
    // hold the queue lock: the probe below takes that lock.
    let node = source.fixture.node.clone();
    let shutdown = std::thread::spawn(move || node.control.finish_live_target_shutdown());
    let probe_node = source.fixture.node.clone();
    let (probe_tx, probe_rx) = std::sync::mpsc::sync_channel(1);
    let probe = std::thread::spawn(move || {
        let by = Instant::now() + WAIT_CAP;
        let mut seen = false;
        while Instant::now() < by {
            if probe_node.control.stopped_for_test() {
                seen = true;
                break;
            }
            std::thread::yield_now();
        }
        let _ = probe_tx.send(seen);
    });
    let seen = probe_rx.recv_timeout(WAIT_CAP).unwrap_or(false);
    // Release the flusher on every path, so a failed probe cannot strand the
    // shutdown thread.
    persist_resume.send(()).expect("the paused flush is live");
    assert!(
        seen,
        "the queue lock stays available while shutdown waits for room"
    );
    probe.join().expect("the probe thread panicked");
    let _stats = shutdown
        .join()
        .expect("the shutdown thread panicked")
        .expect("shutdown completes after the flusher drains");
}

/// A page whose controller is verifiably lost twice exhausts the retry budget:
/// the client is marked broken and the replay does not restart the database.
#[test]
fn replay_page_stops_after_two_lost_controllers() {
    let source = PageSource::new(PARTITION_RETRY);
    let mut client = source.client();
    // Reach the real connect path, as on the kernel: the local-file cache hook
    // would install an entry and consume no connect attempt.
    client.live_write_local_fixture = false;
    source.pool.fail_next_connect_lost();
    source.pool.fail_next_connect_lost();
    let error = match client.replay_live_wal_page(2, 1, 0, 1, source.through) {
        Ok(_) => panic!("a page with two lost controllers must fail"),
        Err(error) => error,
    };
    assert_eq!(
        client.session_present(),
        None,
        "a spent retry budget marks the client broken: {error}"
    );
    assert_eq!(
        source.pool.connect_losses_remaining(),
        0,
        "both armed losses were spent by a connect"
    );
    assert_eq!(
        source.pool.lost_connect_injections(),
        2,
        "the second page attempt must also see a lost connect"
    );
    assert_eq!(
        source.pool.connect_attempts(),
        2,
        "RECOVERY_PAGE_ATTEMPTS is two: one connect per page attempt"
    );
}

/// One verifiably lost page read reconnects and then reads the same page. The
/// first connect reports the loss; the retry opens the WAL through the local
/// fixture and returns the frames, so a lost page is not a lost database.
#[test]
fn replay_page_reconnects_after_one_lost_controller() {
    use crate::distributed::nvmeof::NvmeofTransport;
    use crate::distributed::nvmeof::live_connect::LiveConnectTarget;
    use crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess;
    use crate::distributed::nvmeof::live_storage::{FILES_PER_SUBSYSTEM, wal_export_name};
    let source = PageSource::new(PARTITION_RECONNECT);
    let config = &source.fixture.config.partition;
    let wal = source
        .fixture
        .config
        .root
        .join("epoch_0000000000000001/wal_0000000000000000");
    assert!(
        wal.is_file(),
        "the epoch-1 WAL file exists: {}",
        wal.display()
    );
    let name = wal_export_name(
        FILES_PER_SUBSYSTEM,
        config.inventory.database,
        PARTITION_RECONNECT,
        1,
        0,
    );
    let target = LiveConnectTarget {
        nqn: name.nqn,
        uuid: name.uuid,
        nsid: name.nsid,
        bytes: config.wal_file_bytes,
        block_size: 4096,
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        geometry: source.live_config.queue_geometry,
    };
    let mut client = source.client();
    client.live_write_local_fixture = false;
    source
        .pool
        .register_local_target(&target, LiveNamespaceAccess::ReadOnly, wal)
        .unwrap();
    source.pool.fail_next_connect_lost();
    let page = client
        .replay_live_wal_page(2, 1, 0, 1, source.through)
        .expect("the retry reads the page through a fresh connection");
    assert_eq!(
        page.batches.len(),
        3,
        "the page keeps every batch the retry read"
    );
    assert_eq!(
        source.pool.connect_losses_remaining(),
        0,
        "the first attempt reached the connect and consumed the armed loss"
    );
    assert_eq!(
        source.pool.lost_connect_injections(),
        1,
        "only the first attempt saw the armed loss"
    );
    assert_eq!(
        source.pool.connect_attempts(),
        2,
        "the loss cost one connect and the retry cost one"
    );
    assert_eq!(
        client.session_present(),
        Some(true),
        "a successful retry leaves the client usable"
    );
}

/// A failed page read whose lease later probes live must still be retired by
/// the retry. The fresh path reaches the connect, never the cached failed
/// lease as `Reused`.
#[test]
fn replay_retry_retires_the_failed_lease_that_reads_live_again() {
    use crate::distributed::nvmeof::NvmeofTransport;
    use crate::distributed::nvmeof::live_connect::LiveConnectTarget;
    use crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess;
    use crate::distributed::nvmeof::live_storage::{FILES_PER_SUBSYSTEM, wal_export_name};
    let source = PageSource::new(PARTITION_EVICT);
    let config = &source.fixture.config.partition;
    let wal = source
        .fixture
        .config
        .root
        .join("epoch_0000000000000001/wal_0000000000000000");
    assert!(
        wal.is_file(),
        "the epoch-1 WAL file exists: {}",
        wal.display()
    );
    let name = wal_export_name(
        FILES_PER_SUBSYSTEM,
        config.inventory.database,
        PARTITION_EVICT,
        1,
        0,
    );
    let page_target = LiveConnectTarget {
        nqn: name.nqn.clone(),
        uuid: name.uuid,
        nsid: name.nsid,
        bytes: config.wal_file_bytes,
        block_size: 4096,
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        geometry: source.live_config.queue_geometry,
    };
    let mut client = source.client();
    client.live_write_local_fixture = false;
    // The retry's fresh connection uses the real fixture file.
    source
        .pool
        .register_local_target(&page_target, LiveNamespaceAccess::ReadOnly, wal.clone())
        .unwrap();
    // Seed a Ready lease for another namespace of the same subsystem, so the
    // page's namespace misses its cached member and its resolver reports the
    // controller lost.
    let mut seed_target = page_target.clone();
    seed_target.nsid += 1;
    source
        .pool
        .cache_local_shared_namespace(
            &seed_target,
            &source.live_config.host_nqn,
            source.live_config.host_id,
            &wal,
        )
        .unwrap();
    let seed = source
        .pool
        .cached_lease(&name.nqn)
        .expect("the seed lease is cached");
    seed.force_controller_lost(true);
    // The read failed; before the retry, the liveness probe reads live again.
    seed.force_controller_lost(false);
    assert!(
        !seed.controller_lost(),
        "the failed lease must read live at the retry, or the veto is not exercised"
    );
    let page = client
        .replay_live_wal_page(2, 1, 0, 1, source.through)
        .expect("the retry retires the exact failed lease and reads the page");
    assert_eq!(page.batches.len(), 3, "the page keeps every batch");
    let fresh = source
        .pool
        .cached_lease(&name.nqn)
        .expect("the retry caches its fresh lease");
    assert!(
        !Arc::ptr_eq(&fresh, &seed),
        "the retry must not reuse the failed lease as Reused"
    );
    assert_eq!(
        client.session_present(),
        Some(true),
        "a successful retry leaves the client usable"
    );
}
