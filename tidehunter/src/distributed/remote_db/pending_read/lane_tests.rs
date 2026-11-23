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
//! | `inline_fallback_serves_through_the_lane_worker` | `read_live_inline`'s `if !cache.inline_ready(position)` fallback (`live_inline.rs`) | a grantless lane attempts I/O inline, the fallback counters stay 0 |
//! | `admission_park_is_bounded_by_the_read_deadline` | `Some(deadline)` in `ValueReader::fetch`'s `wait_admission` (`hybrid/read.rs`) | the read parks until `WAIT`; the test then fails at its `expect`, it does not hang |
//! | `coalesced_submissions_share_one_flight` | the `flights.find(node, position)` reuse arm of `submit_until_planned` (`pending_read.rs`) | a second flight is allocated, `coalesced_members == 0` |
//! | `byte_limited_dispatch_splits_waves_and_preserves_order` | the per-member charge check in `ReadBatch::dispatch_traced` (`pending_read.rs`) | all members are admitted in one wave |
//! | `retirement_pause_refuses_outstanding_reads_and_blocks_admission` | `retained.0 != 0` in `pause_idle_nvme` (`pending_read.rs`) | retirement starts while a caller still holds a read |

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
        let registry = LiveConnectionRegistry::open(&registry_dir.path().join("registry")).unwrap();
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

    fn grantless_fallbacks(&self) -> u64 {
        self.reads
            .state
            .diagnostics
            .live_inline_fallback_grant
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

/// Without a cached grant the inline route must decline, not fail: the same
/// read completes through the lane worker.
#[test]
fn inline_fallback_serves_through_the_lane_worker() {
    let case = Case::new(1);
    let value = case
        .reader
        .read(KeySpace(0), &case.key, case.positions[0])
        .unwrap()
        .expect("a put member returns its value");
    assert_eq!(value, case.values[0]);
    assert_eq!(case.inline_reads(), 0);
    assert_eq!(
        case.grantless_fallbacks(),
        1,
        "a missing grant falls back instead of reading uncached"
    );
    assert_eq!(case.actor_reads(), 1);
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
    assert!(
        refused.to_string().contains("admission deadline expired"),
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
    let mut batch = ReadBatch::new(&case.reads, vec![None; 3], vec![rows]).unwrap();
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
