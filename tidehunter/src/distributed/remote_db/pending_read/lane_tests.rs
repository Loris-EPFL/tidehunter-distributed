//! Lane-level coverage of the hybrid value path: `ValueReader` over the real
//! leased read service, its two admission routes (caller-inline and reactor
//! lane), coalescing of one immutable position, byte-budget wave splitting, and
//! the retirement pause.
//!
//! These tests recreate the coverage the S8 read-path re-cut dropped when the
//! old `pending_read/{tests,live_inline_tests}.rs` were left undeclared. They
//! are written against the post-hybrid interfaces only — `ValueReader`,
//! `ReadService`, `ReadBatch`, `ReadRequest` — so they stay valid once S6 wires
//! the table and S11 deletes the exact index.
//!
//! Harness shape: one owner from the leased ordinary-file fixture, written
//! through the production leased client so the value WAL holds real member
//! frames; the position of each member is derived from the batch encoding and
//! the owner's returned address, never from the file the lane will read, so a
//! wrong offset in `VersionPosition::member` moves the read and the oracle
//! apart and fails. `payload_crc` is `crc32` of the member's native entry, the
//! slice a publisher CRCs, not the identity `MemberChecksum`.
//!
//! Negative-control ledger (each test names the production line whose removal
//! makes it fail; a control that only hangs is marked as such):
//!
//! | Test | Removed production line | Effect |
//! |---|---|---|
//! | `inline_read_serves_a_cached_grant_and_skips_the_actor` | `ReadService::read_inline`'s `self.read_live_inline(..)` dispatch (`pending_read.rs`) | the read takes the actor, `live_inline == 0` |
//! | `inline_fallback_serves_through_the_lane_worker` | `read_live_inline`'s `if !cache.inline_ready(position)` fallback (`live_inline.rs`) | a grantless lane attempts I/O inline, the grant fallback counter stays 0 |
//! | `admission_park_is_bounded_by_the_read_deadline` | `Some(deadline)` in `ValueReader::fetch`'s `wait_admission` (`hybrid/read.rs`) | the read parks until `WAIT`; the test then fails at its `expect`, it does not hang |
//! | `coalesced_submissions_share_one_flight` | the `flights.find(node, position)` reuse arm of `submit_until_planned` (`pending_read.rs`) | a second flight is allocated, `coalesced_members == 0` |
//! | `byte_limited_dispatch_splits_waves_and_preserves_order` | the per-member charge check in `ReadBatch::dispatch_traced` (`pending_read.rs`) | all members are admitted in one wave |
//! | `retirement_pause_refuses_outstanding_reads_and_blocks_admission` | `retained.0 != 0` in `pause_idle_nvme` (`pending_read.rs`) | retirement starts while a caller still holds a read |
//! | `a_batch_read_i_o_deadline_must_stay_typed_and_retryable` | the `TimedOut` arm in `Request::complete_nvme` (`pending_read.rs`), reached through `LiveReadCache::admit`'s early return on a spent remaining budget (`tcp/live_read.rs`) | the lane completion poisons the authority, so the stored result reaches the caller as "read authority is poisoned or closed" and the second read fails |
//! | `a_published_completion_outlives_the_call_deadline` | the ready check before the deadline in `ReadRequest::finish` (`pending_read.rs`) | the published frame is reported as `TimedOut` and never decoded |
//! | `a_copied_flight_result_keeps_its_error_kind` | `error.kind()` in `copied_read_error` (`pending_read.rs`) | the stored deadline reaches the caller as `Other` |
//! | `an_inline_read_i_o_deadline_must_stay_typed_and_retryable` | `live_error`'s `Timeout` arm (`tcp/live_read.rs`) and the `TimedOut` arm in `read_live_inline` (`live_inline.rs`) | the error is `Other` and the inline path poisons the authority |
//! | `a_submission_time_timeout_is_retried_within_the_call_deadline` | the retry loop in `LiveReadCache::advance` (`tcp/live_read.rs`) | the forced timeout surfaces and `read_retries == 0` |

use super::*;
use crate::distributed::codec::{CompleteBatch, Mutation};
use crate::distributed::durability::DurabilityTier;
use crate::distributed::hybrid::position::VersionPosition;
use crate::distributed::hybrid::read::ValueReader;
use crate::distributed::nvmeof::live_connect::LiveConnectionRegistry;
use crate::distributed::nvmeof::live_pool::LiveControllerPool;
use crate::distributed::remote_db::RemoteEndpoint;
use crate::distributed::remote_db::read_cache::ReadCache;
use crate::distributed::tcp::leased_tests::Fixture;
use crate::distributed::types::PhysicalValueAddress;
use crate::distributed::write_timeline::WriteTimeline;
use crate::key_shape::{KeyShape, KeySpace};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Read deadline used by the cases that expect a bounded park.
const SHORT_READ_TIMEOUT: Duration = Duration::from_millis(100);
/// The one patience bound of this module: how long a test waits for work that
/// must happen. A deliberate short timeout (`SHORT_READ_TIMEOUT`) is the only
/// other duration; neither is raised to make a slow run pass.
const WAIT: Duration = Duration::from_secs(10);

/// One owner, its written members and the read service over it.
struct Case {
    fixture: Fixture,
    reads: Arc<ReadService>,
    reader: ValueReader,
    /// The keyspace shape, so a test can decode a returned frame the same way
    /// the value path does.
    shape: Arc<KeyShape>,
    positions: Vec<VersionPosition>,
    /// The member frame each position names, derived from the batch encoding.
    frames: Vec<Bytes>,
    key: Bytes,
    values: Vec<Bytes>,
    /// Keeps the controller registry alive for the pool the lanes use.
    _registry: tempdir::TempDir,
}

impl Case {
    fn new(batches: usize) -> Self {
        Self::with_timeout(batches, WAIT)
    }

    fn with_timeout(batches: usize, read_timeout: Duration) -> Self {
        let fixture = Fixture::single_partition(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        // The pool must exist before the first submission: the write route
        // registers each leased extent's ordinary-file namespace in it, and
        // the read lane's permanent-export grant reuses that entry.
        let registry_dir = tempdir::TempDir::new("lane-registry").unwrap();
        let registry = LiveConnectionRegistry::open(
            &registry_dir.path().join("registry"),
            crate::distributed::nvmeof::live_connect::RegistryMode::Retire,
        )
        .unwrap();
        let pool = Arc::new(LiveControllerPool::new(registry, 64, 8).unwrap());
        client.set_live_pool(pool);
        let partition = fixture.config.partition.partition;
        let mut batches_written = Vec::with_capacity(batches);
        let mut positions = Vec::with_capacity(batches);
        for sequence in 0..batches {
            let batch = fixture.batch(sequence as u64);
            let receipts = client
                .submit_and_drain_leased_group(
                    1,
                    vec![batch.clone()],
                    &WriteTimeline::default(),
                    partition,
                )
                .unwrap();
            positions.push(position(&batch, receipts[0].appended().address()));
            batches_written.push(batch);
        }
        let frames = batches_written
            .iter()
            .zip(&positions)
            .map(|(batch, position)| member_frame(batch, position))
            .collect();
        let values = batches_written.iter().map(put_value).collect();
        let key = put_key(&batches_written[0]);
        // The leased write route leaves its prelease exchange outstanding on
        // the control channel; a read lane's own control exchange would then
        // be refused. Complete it before the client moves to the read service,
        // as the leased fixtures do after a group write.
        client.wait_leased_control(&mut Vec::new(), None).unwrap();

        let mut config = crate::distributed::remote_db::pipeline_tests::coordinator_config(
            1,
            128,
            2,
            fixture.tls.clone(),
        );
        config.storage.database = fixture.config.partition.inventory.database;
        config.storage.partitions = fixture.config.partition.inventory.partitions.clone();
        config.storage.batch_limits = fixture.config.partition.batch_limits;
        config.storage.group_limits = fixture.config.partition.group_limits;
        config.storage.fragment_bytes = fixture.config.partition.fragment_bytes;
        config.storage.wal_file_bytes = fixture.config.partition.wal_file_bytes;
        config.storage.max_batches_per_partition = fixture.config.partition.max_batches;
        config.storage.max_reference_metadata_bytes =
            fixture.config.partition.max_reference_metadata_bytes;
        config.endpoints = vec![RemoteEndpoint {
            partition,
            address: fixture.endpoint.clone(),
            server_name: fixture.tls.server_name.clone(),
        }];
        config.tls = fixture.tls.clone();
        config.read_lanes = Some(1);
        config.max_pending_reads = 32;
        config.max_pending_read_bytes = 8 * 1024 * 1024;
        config.timeout = WAIT;
        config.live_nvme = client.live_config_for_test().unwrap();

        let reads = Arc::new(
            ReadService::from_clients(
                &config,
                1,
                vec![client],
                &mut RecoveryStats::default(),
                Arc::new(std::sync::OnceLock::new()),
            )
            .unwrap(),
        );
        // Arm the finite read-profile sample. `actor_completed` counts only
        // sampled requests, so without it the counter stays 0 whichever route
        // served a read and "the actor did not run" would assert nothing.
        let diagnostics = &reads.state.diagnostics;
        diagnostics.limit.store(1024, Ordering::Relaxed);
        diagnostics.remaining.store(1024, Ordering::Relaxed);
        let shape = Arc::new(KeyShape::from_yaml(&fixture.config.shape_yaml).unwrap());
        let partitions = Arc::new(HashMap::from([(partition, 0usize)]));
        let cache = Arc::new(ReadCache::new(1024 * 1024));
        let reader = ValueReader::new(
            Arc::clone(&reads),
            cache,
            Arc::clone(&shape),
            partitions,
            read_timeout,
        );
        Self {
            fixture,
            reads,
            reader,
            shape,
            positions,
            frames,
            key,
            values,
            _registry: registry_dir,
        }
    }

    fn inline_reads(&self) -> u64 {
        self.reads
            .state
            .diagnostics
            .live_inline
            .load(Ordering::Relaxed)
    }

    fn actor_reads(&self) -> u64 {
        self.reads
            .state
            .diagnostics
            .actor_completed
            .load(Ordering::Relaxed)
    }

    fn grant_fallbacks(&self) -> u64 {
        self.reads
            .state
            .diagnostics
            .live_inline_fallback_grant
            .load(Ordering::Relaxed)
    }

    fn busy_fallbacks(&self) -> u64 {
        self.reads
            .state
            .diagnostics
            .live_inline_fallback_busy
            .load(Ordering::Relaxed)
    }

    fn coalesced(&self) -> u64 {
        self.reads.state.coalesced_members.load(Ordering::Relaxed)
    }
}

impl Drop for Case {
    fn drop(&mut self) {
        self.reads.stop();
    }
}

/// Member position of a fixture batch's first record at its owner address.
///
/// `payload_crc` is `crc32` of the member's native entry: the slice the
/// publisher CRCs and the reader recomputes.
fn position(batch: &CompleteBatch, batch_address: PhysicalValueAddress) -> VersionPosition {
    let descriptor = batch.record_descriptors()[0];
    let entry = &batch.encoded()
        [descriptor.offset as usize + 8..(descriptor.offset + descriptor.len) as usize];
    VersionPosition::member(
        batch.version(),
        batch_address,
        0,
        descriptor,
        crc32fast::hash(entry),
    )
}

/// The member frame `position` names, derived from the batch encoding rather
/// than from the file the lane will read.
fn member_frame(batch: &CompleteBatch, position: &VersionPosition) -> Bytes {
    let descriptor = batch.record_descriptors()[position.operation_index as usize];
    assert_eq!(
        descriptor.len, position.address.len,
        "the position names this member's frame"
    );
    Bytes::copy_from_slice(
        &batch.encoded()[descriptor.offset as usize..(descriptor.offset + descriptor.len) as usize],
    )
}

/// The key and value of the fixture batch's only mutation.
fn only_put(batch: &CompleteBatch) -> (&Bytes, &Bytes) {
    match &batch.mutations()[0] {
        Mutation::Put { key, value, .. } => (key, value),
        other => panic!("the fixture batch is not a put: {other:?}"),
    }
}

fn put_value(batch: &CompleteBatch) -> Bytes {
    only_put(batch).1.clone()
}

fn put_key(batch: &CompleteBatch) -> Bytes {
    only_put(batch).0.clone()
}

/// Verify a returned member frame and decode it exactly as the value path
/// does: the length header, the position's payload CRC and the native entry.
fn decoded_value(case: &Case, position: &VersionPosition, frame: &Bytes) -> Bytes {
    let header = frame.get(..8).expect("a member frame carries its header");
    let declared = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let payload = frame.slice(8..);
    assert_eq!(
        declared,
        payload.len(),
        "the member frame length matches its header"
    );
    assert_eq!(
        crc32fast::hash(&payload),
        position.payload_crc,
        "the member payload matches the publisher's CRC"
    );
    let (key, value) = crate::db::decode_record_frame(
        case.shape.ks(KeySpace(0)),
        &case.key,
        Bytes::copy_from_slice(&payload),
    )
    .expect("the member decodes as a native entry")
    .expect("the member is a put");
    assert_eq!(
        key, case.key,
        "the member names the key the index asked for"
    );
    value
}

/// A stored result reaches each consumer through `try_result`'s copy, so the
/// copy must keep the class: a caller that classifies by kind cannot tell a
/// deadline from a broken lane once the kind is flattened to `Other`.
#[test]
fn a_copied_flight_result_keeps_its_error_kind() {
    let stored = LocalDbError::with_kind(
        std::io::ErrorKind::TimedOut,
        "remote read I/O deadline expired",
    );
    let copy = copied_read_error(&stored);
    assert_eq!(
        copy.kind(),
        std::io::ErrorKind::TimedOut,
        "a copied deadline stays a deadline: {copy}"
    );
    assert_eq!(
        copy.to_string(),
        stored.to_string(),
        "the copy keeps the stored message"
    );
}

/// A cached grant lets the caller read on the lane it already owns; the actor
/// never sees the request.
#[test]
fn inline_read_serves_a_cached_grant_and_skips_the_actor() {
    let case = Case::new(1);
    let parked = case.reads.bench_park_live_lane(0);
    case.reads
        .bench_install_owner_local(&case.fixture.node, case.positions[0]);
    let value = case
        .reader
        .read(KeySpace(0), &case.key, case.positions[0])
        .unwrap()
        .expect("a put member returns its value");
    drop(parked);
    assert_eq!(value, case.values[0]);
    assert_eq!(case.inline_reads(), 1, "the caller-inline route served it");
    assert_eq!(case.actor_reads(), 0, "no reactor lane touched the request");
}

/// Without a cached grant the inline route must decline for that grant, not
/// fail: the same value read completes through the lane worker.
///
/// The worker is parked at its loop-top test hook first. At that point it owns
/// neither `lane.driver` nor `lane.client`, so the decline cannot be the
/// scheduling-dependent `busy` reason: the test proves the missing-grant
/// premise, and the busy counter is a control.
#[test]
fn inline_fallback_serves_through_the_lane_worker() {
    let case = Case::new(1);
    let parked = case.reads.bench_park_live_lane(0);
    let inline = case
        .reads
        .read_inline(0, &case.positions[0], 0)
        .expect("a missing grant is a decline, not a failure");
    assert!(
        inline.is_none(),
        "no cached grant: the inline route hands the member back"
    );
    assert_eq!(
        case.grant_fallbacks(),
        1,
        "the missing grant is the recorded decline"
    );
    assert_eq!(
        case.busy_fallbacks(),
        0,
        "the parked worker owns no lane lock, so busy cannot be the reason"
    );
    drop(parked);
    let value = case
        .reader
        .read(KeySpace(0), &case.key, case.positions[0])
        .unwrap()
        .expect("a put member returns its value");
    assert_eq!(value, case.values[0]);
    assert_eq!(case.inline_reads(), 0, "no grant means no inline serve");
    assert_eq!(case.actor_reads(), 1, "the lane worker served the read");
}

/// A caller that cannot be admitted parks on admission progress with the read
/// deadline as its bound, and reports a typed timeout rather than waiting.
#[test]
fn admission_park_is_bounded_by_the_read_deadline() {
    let case = Case::with_timeout(2, SHORT_READ_TIMEOUT);
    let charge = member_charge(&case.positions[0]).unwrap();
    // One request fits, the second does not: a request charges 512 of framing
    // plus the member charge, so with a limit of one and a half member charges
    // beyond the framing the second request (512 + charge) exceeds what is left
    // after the first (half a charge).
    case.reads
        .set_limits_for_test(Some(512 + charge + charge / 2), None);
    // Park the lane before it submits, so the first request keeps its credit
    // without completing and the second cannot be admitted.
    let parked = case.reads.bench_park_live_lane(0);
    let held = case
        .reads
        .submit_positions(0, &[case.positions[0]], None)
        .unwrap();
    // The read runs on a scoped thread and the test waits with its own bound,
    // so a missing deadline in the value path fails here instead of hanging.
    let (done, outcome) = mpsc::channel();
    let result = std::thread::scope(|scope| {
        scope.spawn(|| {
            done.send(case.reader.read(KeySpace(0), &case.key, case.positions[1]))
                .expect("the receiver outlives the scope");
        });
        let result = outcome.recv_timeout(WAIT);
        // Release the credit and the lane so the thread ends whichever way
        // the wait went.
        drop(held);
        drop(parked);
        result
    });
    let refused = result
        .expect("the read ended within the bound instead of parking without a deadline")
        .unwrap_err();
    assert_eq!(
        refused.kind(),
        std::io::ErrorKind::TimedOut,
        "a full authority must end the park with a typed timeout: {refused}"
    );
}

/// Two callers of the same immutable position share one owner flight and one
/// result, so the second costs no transport work.
#[test]
fn coalesced_submissions_share_one_flight() {
    let case = Case::new(1);
    let parked = case.reads.bench_park_live_lane(0);
    let first = case
        .reads
        .submit_positions(0, &[case.positions[0]], None)
        .unwrap();
    let second = case
        .reads
        .submit_positions(0, &[case.positions[0]], None)
        .unwrap();
    assert_eq!(case.coalesced(), 1, "the second caller joined the flight");
    drop(parked);
    assert_eq!(first.finish_member().unwrap(), case.frames[0]);
    assert_eq!(second.finish_member().unwrap(), case.frames[0]);
    assert_eq!(
        case.reads.state.retained.lock().0,
        0,
        "both callers released their credits"
    );
}

/// A batch is submitted as the largest admitted waves the byte budget allows,
/// and completed frames land in their caller slots, preserving order.
#[test]
fn byte_limited_dispatch_splits_waves_and_preserves_order() {
    let case = Case::new(3);
    let charge = member_charge(&case.positions[0]).unwrap();
    // Exactly two members fit: 512 of framing plus two charges, and less than
    // one more.
    case.reads
        .set_limits_for_test(Some(512 + 2 * charge + charge / 2), None);
    let parked = case.reads.bench_park_live_lane(0);
    // Permuted slots: a result stored by completion index instead of by slot
    // would come back in the wrong order.
    let rows: Vec<(usize, VersionPosition)> = vec![
        (2, case.positions[0]),
        (0, case.positions[1]),
        (1, case.positions[2]),
    ];
    let mut batch = ReadBatch::new(
        &case.reads,
        vec![None; 3],
        vec![rows],
        Instant::now() + WAIT,
    )
    .unwrap();
    batch.dispatch(&case.reads).unwrap();
    assert_eq!(batch.reads.len(), 1, "one wave is one owner request");
    assert_eq!(
        batch.reads[0].0.len(),
        2,
        "one wave admits only the members that fit its byte budget"
    );
    assert!(
        batch.has_unsubmitted(),
        "the remaining member waits for released credit"
    );
    drop(parked);
    let deadline = Instant::now() + WAIT;
    while !batch.complete() {
        let generation = case.reads.admission_generation();
        match batch.dispatch(&case.reads) {
            Ok(()) => {
                if batch.is_idle() {
                    case.reads
                        .wait_admission(generation, Some(deadline))
                        .unwrap();
                } else {
                    batch.finish_one().unwrap();
                }
            }
            Err(error) if admission_error(&error) => {
                case.reads
                    .wait_admission(generation, Some(deadline))
                    .unwrap();
            }
            Err(error) => panic!("read batch dispatch failed: {error}"),
        }
    }
    assert_eq!(
        batch.values,
        vec![
            Some(case.frames[1].clone()),
            Some(case.frames[2].clone()),
            Some(case.frames[0].clone()),
        ],
        "values land in their caller slots, not in completion order"
    );
}

/// Retirement is refused while a caller holds read credit, and while it is in
/// progress every new admission is load, not failure.
#[test]
fn retirement_pause_refuses_outstanding_reads_and_blocks_admission() {
    let case = Case::new(1);
    let held = case
        .reads
        .submit_positions(0, &[case.positions[0]], None)
        .unwrap();
    let Err(refused) = case.reads.pause_live_for_retirement() else {
        panic!("an outstanding read must refuse the retirement pause");
    };
    assert!(
        refused
            .to_string()
            .contains("finish or drop outstanding reads"),
        "an outstanding read keeps the NVMe-oF grants: {refused}"
    );
    held.finish_member().unwrap();
    let pause = case
        .reads
        .pause_live_for_retirement()
        .expect("an idle authority can retire its grants");
    let blocked = case.reads.submit_positions(0, &[case.positions[0]], None);
    let Err(blocked) = blocked else {
        panic!("a paused authority must refuse the submission");
    };
    assert!(
        admission_error(&blocked),
        "a paused authority reports admission load: {blocked}"
    );
    drop(pause);
    let value = case
        .reader
        .read(KeySpace(0), &case.key, case.positions[0])
        .unwrap()
        .expect("the same lanes serve after the pause");
    assert_eq!(value, case.values[0]);
}

/// A transport deadline is a transient, idempotent-read failure, not a
/// permanent authority failure.
///
/// The submission is admitted with a spent call deadline, so `admit` returns
/// early on the spent remaining budget and the lane's terminal completion is a
/// typed `TimedOut`. The terminal completion is held inside `Request::complete_nvme`
/// after the result is stored, so the test reaches the lane completion and its
/// `TimedOut` exemption instead of the batch's own deadline branch. The first
/// read must report `io::ErrorKind::TimedOut`, and the next read must still be
/// served: a deadline is not a broken lane. The old code flattened it to
/// `Other` and poisoned the whole service, which is the observed
/// `drop_cells_tests` flake.
#[test]
fn a_batch_read_i_o_deadline_must_stay_typed_and_retryable() {
    let case = Case::new(1);
    let (held, completion) = case.reads.bench_hold_completion();
    let mut request = case
        .reads
        .submit_positions_before(0, &[case.positions[0]], None, Instant::now())
        .expect("admission does not consume the call deadline");
    held.recv_timeout(WAIT)
        .expect("the lane reaches its timed-out terminal completion");
    let signal = Arc::new(BatchSignal::new());
    request.subscribe(&signal);
    completion.release();
    {
        // Wait for the flight to retire before the result is read, so its slot
        // is reusable and the next read cannot coalesce onto it.
        let mut wait_guard = signal.wait_lock.lock();
        while !request
            .parts
            .iter()
            .all(|(flight, _)| flight._credit.state.flight_ready[flight.slot].is_ready())
        {
            let waited = signal
                .changed
                .wait_until(&mut wait_guard, Instant::now() + WAIT);
            assert!(
                !waited.timed_out(),
                "the timed-out flight retires within the test bound"
            );
        }
    }
    let error = request
        .finish()
        .expect_err("a spent call deadline cannot serve the member");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::TimedOut,
        "the lane deadline must keep its class: {error}"
    );
    // The deadline poisoned nothing: the same authority serves the member.
    let value = case
        .reader
        .read(KeySpace(0), &case.key, case.positions[0])
        .unwrap()
        .expect("one expired deadline must not fail-stop the read authority");
    assert_eq!(value, case.values[0], "the retried read returns the value");
}

/// A flight whose CQE was accepted and published is a success even when the
/// caller observes it at or after the call deadline. The payload is in hand,
/// so reporting `TimedOut` would drop it and repeat the I/O.
///
/// The terminal completion is held inside `Request::complete_nvme` after the
/// result is stored and before the slot retires. The caller's deadline
/// therefore passes with an accepted CQE and no sleep or load decides the
/// outcome; the release and the readiness wait are the only signals.
#[test]
fn a_published_completion_outlives_the_call_deadline() {
    let case = Case::new(1);
    let (held, completion) = case.reads.bench_hold_completion();
    let mut request = case
        .reads
        .submit_positions_before(0, &[case.positions[0]], None, Instant::now() + WAIT)
        .expect("the fixture admits one member");
    held.recv_timeout(WAIT)
        .expect("the lane reaches its terminal completion");
    // The caller's budget is spent while the accepted CQE is still held.
    request.deadline = Instant::now();
    let signal = Arc::new(BatchSignal::new());
    request.subscribe(&signal);
    completion.release();
    {
        let mut wait_guard = signal.wait_lock.lock();
        while !request
            .parts
            .iter()
            .all(|(flight, _)| flight._credit.state.flight_ready[flight.slot].is_ready())
        {
            let waited = signal
                .changed
                .wait_until(&mut wait_guard, Instant::now() + WAIT);
            assert!(
                !waited.timed_out(),
                "the published flight wakes its caller within the test bound"
            );
        }
    }
    let frames = request
        .finish()
        .expect("a published completion is returned, not discarded at the deadline");
    assert_eq!(
        frames,
        vec![case.frames[0].clone()],
        "the frame the flight read"
    );
    assert_eq!(
        decoded_value(&case, &case.positions[0], &frames[0]),
        case.values[0],
        "the returned frame decodes to the value the fixture wrote"
    );
    assert_eq!(
        case.reads
            .state
            .diagnostics
            .read_timeouts
            .load(Ordering::Relaxed),
        0,
        "a read that returned its data is not a timeout"
    );
}

/// The caller-inline path has its own error mapping and its own poison site.
/// A deadline there must also stay typed and leave the authority usable.
#[test]
fn an_inline_read_i_o_deadline_must_stay_typed_and_retryable() {
    let case = Case::with_timeout(1, Duration::ZERO);
    // Own the lane so the reads cannot fall back to its worker: the cached
    // grant is installed while the worker is parked, and the park handle stays
    // alive across both reads.
    let parked = case.reads.bench_park_live_lane(0);
    case.reads
        .bench_install_owner_local(&case.fixture.node, case.positions[0]);
    let error = case
        .reader
        .read(KeySpace(0), &case.key, case.positions[0])
        .expect_err("a spent call deadline cannot serve the member");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::TimedOut,
        "the inline transport deadline must keep its class: {error}"
    );
    assert_eq!(
        case.inline_reads(),
        1,
        "the caller-inline route served the attempt"
    );
    assert_eq!(case.actor_reads(), 0, "no reactor lane touched the request");
    assert_eq!(
        case.reads
            .state
            .diagnostics
            .read_timeouts
            .load(Ordering::Relaxed),
        1,
        "one caller-visible timeout is counted once"
    );
    let again = case
        .reader
        .read(KeySpace(0), &case.key, case.positions[0])
        .expect_err("the fixture keeps the call deadline at zero");
    assert_eq!(
        again.kind(),
        std::io::ErrorKind::TimedOut,
        "one expired inline deadline must not fail-stop the read authority: {again}"
    );
    assert_eq!(
        case.inline_reads(),
        2,
        "the second read takes the same caller-inline route"
    );
    assert_eq!(
        case.reads
            .state
            .diagnostics
            .read_timeouts
            .load(Ordering::Relaxed),
        2,
        "each caller-visible timeout is counted once"
    );
    drop(parked);
}

/// A submission-time timeout that never reached the ring is retried inside the
/// caller's deadline, and the retry is counted. The hook fails exactly one
/// member submission before any SQE exists, which is the only retry-safe
/// timeout: no buffer can still receive DMA.
#[test]
fn a_submission_time_timeout_is_retried_within_the_call_deadline() {
    let case = Case::new(1);
    {
        let lanes = case.reads.lanes.read();
        let mut driver = lanes[0][0].driver.lock();
        driver
            .live_cache
            .as_mut()
            .expect("the lane owns its read cache")
            .fail_next_read_submission();
    }
    let value = case
        .reader
        .read(KeySpace(0), &case.key, case.positions[0])
        .expect("the retry inside the call deadline serves the member")
        .expect("the member holds a value");
    assert_eq!(value, case.values[0]);
    let diagnostics = &case.reads.state.diagnostics;
    assert_eq!(
        diagnostics.read_retries.load(Ordering::Relaxed),
        1,
        "one submission-time timeout is retried once"
    );
    assert_eq!(
        diagnostics.read_timeouts.load(Ordering::Relaxed),
        0,
        "the retried read is not a caller-visible timeout"
    );
}

/// A lane read that completes short is not a read-authority verdict when the
/// controller it used was verifiably deleted: the lane requeues the member,
/// evicts the dead controller, reconnects and the value path returns the
/// value. `reads.failed()` stays false, so the write core keeps accepting
/// writes after the healed read.
#[test]
fn a_short_read_on_a_lost_controller_does_not_poison_the_authority() {
    let case = Case::new(1);
    let position = case.positions[0];
    // The lane route owns the quarantine and the retry budget; the inline
    // route's decline is covered by `tcp::live_read`.
    case.reads.set_inline_reads_for_test(false);
    let path =
        crate::distributed::tcp::live_read::tests::owner_local_path(&case.fixture.node, position);
    // The ordinary-file fixture has no kernel target: register the owner's
    // export as a local namespace, then make the grant the lane opens report
    // the deletion the kernel would report and complete its read short.
    let parked = case.reads.bench_park_live_lane(0);
    case.reads
        .bench_register_permanent_local(0, position, &path);
    case.reads.bench_arm_short_read_after_open(0);
    drop(parked);
    let before = case
        .fixture
        .metrics
        .snapshot()
        .counts
        .read_controller_resumes;
    let value = case
        .reader
        .read(KeySpace(0), &case.key, position)
        .expect("a resumed short read serves the value")
        .expect("a put member returns its value");
    assert_eq!(
        value, case.values[0],
        "the retried read returns the identical member bytes"
    );
    assert!(
        !case.reads.failed(),
        "a short read of a verifiably lost controller must not poison the read authority"
    );
    assert_eq!(
        case.fixture
            .metrics
            .snapshot()
            .counts
            .read_controller_resumes
            - before,
        1,
        "exactly one controller resume is counted"
    );
}
