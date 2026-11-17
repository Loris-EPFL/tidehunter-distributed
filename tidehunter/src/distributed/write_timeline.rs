//! Opt-in coordinator-local write diagnostics. Timestamps share one monotonic origin;
//! they do not measure network service time or compare clocks across hosts.
use super::types::BatchVersion;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

const MAX_EVENTS: usize = 32;

/// Fixed-size, whole-window wall spans. Stages can nest; these are not CPU
/// timings. Bucket 63 includes everything at or beyond 63 seconds.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WriteBlockStage {
    AdmissionCredit,
    Allocate,
    IdentityLookup,
    AdmissionIndexLock,
    PublicationIndexLock,
    Encode,
    OwnerQueue,
    PublishLock,
    Publish,
    Checkpoint,
    Snapshot,
}
const BLOCK_STAGES: [WriteBlockStage; 11] = [
    WriteBlockStage::AdmissionCredit,
    WriteBlockStage::Allocate,
    WriteBlockStage::IdentityLookup,
    WriteBlockStage::AdmissionIndexLock,
    WriteBlockStage::PublicationIndexLock,
    WriteBlockStage::Encode,
    WriteBlockStage::OwnerQueue,
    WriteBlockStage::PublishLock,
    WriteBlockStage::Publish,
    WriteBlockStage::Checkpoint,
    WriteBlockStage::Snapshot,
];
const BLOCK_BUCKETS: usize = 64;

#[derive(Default)]
struct BlockCounter {
    calls: AtomicU64,
    ns: AtomicU64,
    max_ns: AtomicU64,
    max_depth: AtomicU64,
}

#[derive(Debug, Serialize)]
pub struct WriteBlockBucket {
    stage: WriteBlockStage,
    second: usize,
    calls: u64,
    ns: u64,
    max_ns: u64,
    max_depth: u64,
}

struct BlockCounters {
    origin_ns: AtomicU64,
    buckets: [BlockCounter; BLOCK_STAGES.len() * BLOCK_BUCKETS],
}
impl Default for BlockCounters {
    fn default() -> Self {
        Self {
            origin_ns: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| BlockCounter::default()),
        }
    }
}
impl BlockCounters {
    fn reset(&self, origin_ns: u64) {
        // Relaxed: arming is quiescent; enabled's release publishes this reset.
        self.origin_ns.store(origin_ns, Ordering::Relaxed);
        for bucket in &self.buckets {
            bucket.calls.store(0, Ordering::Relaxed);
            bucket.ns.store(0, Ordering::Relaxed);
            bucket.max_ns.store(0, Ordering::Relaxed);
            bucket.max_depth.store(0, Ordering::Relaxed);
        }
    }
    fn snapshot(&self) -> Vec<WriteBlockBucket> {
        self.buckets
            .iter()
            .enumerate()
            .filter_map(|(index, bucket)| {
                let calls = bucket.calls.load(Ordering::Relaxed);
                (calls != 0).then(|| WriteBlockBucket {
                    stage: BLOCK_STAGES[index / BLOCK_BUCKETS],
                    second: index % BLOCK_BUCKETS,
                    calls,
                    ns: bucket.ns.load(Ordering::Relaxed),
                    max_ns: bucket.max_ns.load(Ordering::Relaxed),
                    max_depth: bucket.max_depth.load(Ordering::Relaxed),
                })
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WriteOwnerQueue {
    WorkerQueued,
    LeasedFlights,
}
const OWNER_QUEUES: [WriteOwnerQueue; 2] = [
    WriteOwnerQueue::WorkerQueued,
    WriteOwnerQueue::LeasedFlights,
];

#[derive(Default)]
struct QueueCounter {
    samples: AtomicU64,
    depth_sum: AtomicU64,
    max_depth: AtomicU64,
}

#[repr(align(64))]
struct OwnerCounters {
    owner: u32,
    queues: [QueueCounter; OWNER_QUEUES.len() * BLOCK_BUCKETS],
    frames: AtomicU64,
    fua_frames: AtomicU64,
    batches: AtomicU64,
    puts: AtomicU64,
}

#[derive(Debug, Serialize)]
pub struct WriteOwnerQueueBucket {
    queue: WriteOwnerQueue,
    second: usize,
    samples: u64,
    depth_sum: u64,
    max_depth: u64,
}

#[derive(Debug, Serialize)]
pub struct WriteOwnerDiagnostics {
    owner: u32,
    submitted_frames: u64,
    submitted_fua_frames: u64,
    submitted_batches: u64,
    submitted_puts: u64,
    queues: Vec<WriteOwnerQueueBucket>,
}

impl OwnerCounters {
    fn new(owner: u32) -> Self {
        Self {
            owner,
            queues: std::array::from_fn(|_| QueueCounter::default()),
            frames: AtomicU64::new(0),
            fua_frames: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            puts: AtomicU64::new(0),
        }
    }
    fn reset(&self) {
        // Relaxed: quiescent reset is published by enabled's Release store.
        for queue in &self.queues {
            queue.samples.store(0, Ordering::Relaxed);
            queue.depth_sum.store(0, Ordering::Relaxed);
            queue.max_depth.store(0, Ordering::Relaxed);
        }
        self.frames.store(0, Ordering::Relaxed);
        self.fua_frames.store(0, Ordering::Relaxed);
        self.batches.store(0, Ordering::Relaxed);
        self.puts.store(0, Ordering::Relaxed);
    }
    fn snapshot(&self) -> WriteOwnerDiagnostics {
        WriteOwnerDiagnostics {
            owner: self.owner,
            submitted_frames: self.frames.load(Ordering::Relaxed),
            submitted_fua_frames: self.fua_frames.load(Ordering::Relaxed),
            submitted_batches: self.batches.load(Ordering::Relaxed),
            submitted_puts: self.puts.load(Ordering::Relaxed),
            queues: self
                .queues
                .iter()
                .enumerate()
                .filter_map(|(index, queue)| {
                    let samples = queue.samples.load(Ordering::Relaxed);
                    (samples != 0).then(|| WriteOwnerQueueBucket {
                        queue: OWNER_QUEUES[index / BLOCK_BUCKETS],
                        second: index % BLOCK_BUCKETS,
                        samples,
                        depth_sum: queue.depth_sum.load(Ordering::Relaxed),
                        max_depth: queue.max_depth.load(Ordering::Relaxed),
                    })
                })
                .collect(),
        }
    }
}

/// Exact local batch counts for the three coordinator write waits.
///
/// Observations count entries. Depth is the number of batches inside the wait
/// window, sampled at entry including the new batch. W1 counts owner-queue
/// READY entries until the owner worker dequeues them into a group. W2 counts
/// caller-allocated batches until the caller marks them READY in their owner
/// queue (validation-free encode and handoff). W3 counts successful owner-RPC
/// receipt batches harvested by the worker until its publication callback.
/// Wake attempts count handoffs to the consuming thread: one per W1 READY entry
/// and one per W3 harvested completion. A handoff is coalesced when it sent no
/// new hint: the W1 worker was not parked (it rechecks its queue before parking)
/// or its bounded `QueueReady` channel was full; W3 publication runs on the
/// harvesting worker, so every W3 handoff is coalesced. W2 has no handoff and
/// its wake counts stay zero.
///
/// No durations or clocks are collected. Counters saturate at `u64::MAX` and
/// set `saturated` when any counter reaches that cap. Storage has a fixed size.
/// `complete` is always true: no wake count is deferred past export.
#[derive(Debug, Serialize)]
pub struct WriteWaitMetrics {
    pub enabled: bool,
    pub saturated: bool,
    pub complete: bool,
    pub windows: [WriteWaitWindowMetrics; 3],
}

#[derive(Debug, Serialize)]
pub struct WriteWaitWindowMetrics {
    pub wait: &'static str,
    pub observations: u64,
    pub depth_sum: u64,
    pub depth_max: u64,
    pub wake_attempts: u64,
    pub wake_coalesced: u64,
}

#[derive(Clone, Copy)]
pub(crate) enum WriteWait {
    OwnerEnqueueToDequeue,
    AdmittedToAppendEnqueue,
    HarvestToDurable,
}

#[derive(Default)]
#[repr(align(64))]
struct WaitCounters {
    depth: AtomicU64,
    observations: AtomicU64,
    depth_sum: AtomicU64,
    depth_max: AtomicU64,
    wake_attempts: AtomicU64,
    wake_coalesced: AtomicU64,
}

#[derive(Default)]
struct WriteWaitCounters {
    windows: [WaitCounters; 3],
    saturated: AtomicBool,
}

impl WriteWaitCounters {
    // Relaxed operations order statistics only; they do not publish write data.
    fn add(&self, counter: &AtomicU64, value: u64) -> u64 {
        let previous = counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                Some(old.saturating_add(value))
            })
            .unwrap_or_else(|old| old);
        if previous.saturating_add(value) == u64::MAX {
            self.saturated.store(true, Ordering::Relaxed);
        }
        previous.saturating_add(value)
    }

    fn reset(&self) {
        for window in &self.windows {
            for counter in [
                &window.depth,
                &window.observations,
                &window.depth_sum,
                &window.depth_max,
                &window.wake_attempts,
                &window.wake_coalesced,
            ] {
                counter.store(0, Ordering::Relaxed);
            }
        }
        self.saturated.store(false, Ordering::Relaxed);
    }

    fn snapshot(&self) -> WriteWaitMetrics {
        let complete = true; // No wake count is deferred past export.
        let names = [
            "owner_enqueue_to_dequeue",
            "admitted_to_append_enqueue",
            "harvest_to_durable",
        ];
        WriteWaitMetrics {
            enabled: true,
            saturated: self.saturated.load(Ordering::Relaxed),
            complete,
            windows: std::array::from_fn(|index| {
                let window = &self.windows[index];
                WriteWaitWindowMetrics {
                    wait: names[index],
                    observations: window.observations.load(Ordering::Relaxed),
                    depth_sum: window.depth_sum.load(Ordering::Relaxed),
                    depth_max: window.depth_max.load(Ordering::Relaxed),
                    wake_attempts: window.wake_attempts.load(Ordering::Relaxed),
                    wake_coalesced: window.wake_coalesced.load(Ordering::Relaxed),
                }
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteTimelineStage {
    CallerStart,
    GateAcquired,
    ValidationStart,
    AdmissionWaitStarted,
    AdmissionAcquired,
    OwnerEnqueueStarted,
    OwnerDequeued,
    Prepared,
    Admitted,
    AppendEnqueueStarted,
    WorkerDequeued,
    LiveGroupBuildStarted,
    LiveGroupBuilt,
    LiveExtentSwitchStarted,
    LiveExtentSwitchCompleted,
    LiveReserveStarted,
    LiveReserveCompleted,
    LiveNamespaceSetupStarted,
    LiveNamespaceSetupCompleted,
    LiveIoStarted,
    /// The CQE was observed. Leased groups can complete out of frame order.
    LiveIoCompleted,
    LiveCompleteStarted,
    LiveCompleteCompleted,
    /// Leased receipts release only after all prior frames complete.
    LiveReceiptsValidated,
    /// The request has been queued in the local reactor, not necessarily sent on the wire.
    TxQueued,
    /// The worker observed a decoded response; actual decoding may have occurred earlier.
    CompletionHarvested,
    DurableObserved,
    Published,
    CallerReplyQueued,
    CallerFinished,
    /// Leased frame encoding and CRC passes are complete.
    LiveFrameEncoded,
    /// The kernel consumed the leased write SQE, before the CQE wait.
    LiveIoSubmitted,
    /// The first sampled group after the writer queue changed its target.
    LiveQueueRebound,
}

#[derive(Debug, Serialize)]
pub struct WriteTimelineEvent {
    pub stage: WriteTimelineStage,
    pub elapsed_ns: u64,
    pub owner: Option<u32>,
    pub envelope_id: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct WriteTimelineRecord {
    pub caller_sample_id: u64,
    pub operation: Option<[u8; 16]>,
    pub batch: Option<BatchVersion>,
    pub events: Vec<WriteTimelineEvent>,
    pub error: Option<String>,
    pub error_truncated: bool,
    pub completed: bool,
}
#[derive(Debug, Serialize)]
pub struct CheckpointTimelineEvent {
    pub stage: &'static str,
    pub elapsed_ns: u64,
    pub owner: Option<u32>,
    pub epoch: u64,
    /// Checkpoint generation, absent for connection-level control events.
    pub generation: Option<u64>,
    pub prefix: Option<usize>,
    pub pressure: Option<CheckpointPressure>,
}

/// JSON-only control diagnostics. This type is not part of the owner wire format.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct ControlTimelineTarget {
    pub kind: &'static str,
    pub epoch: u64,
    pub extent: Option<[u8; 16]>,
    pub through: Option<u64>,
    pub prefix: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct ControlTimelineEvent {
    pub stage: &'static str,
    pub elapsed_ns: u64,
    pub owner: u32,
    pub envelope_id: u64,
    pub target: ControlTimelineTarget,
    pub success: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct CheckpointPressure {
    pub reason: &'static str,
    pub batches: usize,
    pub encoded_bytes: usize,
    pub max_batches: usize,
    pub max_encoded_bytes: usize,
    pub incoming_encoded_bytes: Option<usize>,
}

/// Already-queued same-epoch append commands ahead of the first control barrier.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ReadyAppendPrefix {
    pub commands: usize,
    pub batches: usize,
    pub encoded_bytes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnerTimelineRecord {
    pub owner: u32,
    pub envelope_id: u64,
    pub epoch: Option<u64>,
    pub success: bool,
    pub error: Option<String>,
    pub error_truncated: bool,
    pub control_queue_ns: Option<u64>,
    pub session_admission_wait_ns: Option<u64>,
    pub appender_queue_ns: Option<u64>,
    pub flusher_queue_ns: Option<u64>,
    pub append_barrier_wait_ns: Option<u64>,
    pub append_wal_ns: Option<u64>,
    /// A zero phase on a failed or retried append does not establish that the
    /// corresponding phase was reached.
    pub append_validation_ns: Option<u64>,
    pub append_basic_validation_ns: Option<u64>,
    pub append_conflict_lookup_ns: Option<u64>,
    pub append_geometry_capacity_ns: Option<u64>,
    pub append_metadata_reservation_ns: Option<u64>,
    pub append_descriptor_preparation_ns: Option<u64>,
    pub append_errors: u64,
    pub append_retries: u64,
    pub append_persist_ns: Option<u64>,
    pub wal_file_persist_ns: Option<u64>,
    pub handler_ns: Option<u64>,
    pub response_ready_ns: Option<u64>,
    pub namespace_persist_ns: Option<u64>,
    pub file_persist_calls: u64,
    pub namespace_persist_calls: u64,
    pub appended_groups: u64,
    pub batch_count: Option<usize>,
    pub encoded_bytes: Option<usize>,
    pub persistence_capture: Option<super::storage::PersistenceCapture>,
    pub ready_append_prefix: Option<ReadyAppendPrefix>,
}
/// A single live group; owner durations are returned by the completion RPC and
/// are never compared to coordinator timestamps. Versions include sampled calls only.
#[derive(Debug, Serialize)]
pub struct LiveGroupTimeline {
    pub owner: u32,
    pub batches: Vec<BatchVersion>,
    pub reservation_token: Option<[u8; 16]>,
    pub owner_validation_ns: Option<u64>,
    pub owner_persistence: Option<super::storage::StorageWriteProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_position: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame_len: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_file: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_block_size: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct OwnerTimelineError {
    pub owner: u32,
    pub phase: &'static str,
    pub message: String,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct WriteTimelineExport {
    pub origin_monotonic_ns: u64,
    pub limit: usize,
    pub sample_every: usize,
    pub seen_calls: u64,
    pub skipped_calls: u64,
    pub records: Vec<WriteTimelineRecord>,
    pub dropped_calls: u64,
    pub dropped_events: u64,
    pub incomplete_calls: u64,
    pub deadline_exceeded: bool,
    pub owner_storage_durations_available: bool,
    pub checkpoint_events: Vec<CheckpointTimelineEvent>,
    pub dropped_checkpoint_events: u64,
    pub control_events: Vec<ControlTimelineEvent>,
    pub dropped_control_events: u64,
    pub owner_records: Vec<OwnerTimelineRecord>,
    pub live_groups: Vec<LiveGroupTimeline>,
    pub dropped_live_groups: u64,
    pub owner_dropped_records: u64,
    pub owner_incomplete_records: u64,
    pub owner_expected_connections: usize,
    pub owner_captured_connections: usize,
    pub owner_errors: Vec<OwnerTimelineError>,
    pub write_wait_metrics: WriteWaitMetrics,
    pub blocking_spans: Vec<WriteBlockBucket>,
    pub owner_diagnostics: Vec<WriteOwnerDiagnostics>,
}

pub(crate) struct OwnerTimelineCollection {
    pub records: Vec<OwnerTimelineRecord>,
    pub dropped: u64,
    pub incomplete: u64,
    pub expected: usize,
    pub captured: usize,
    pub errors: Vec<OwnerTimelineError>,
}

#[derive(Clone, Copy)]
pub(crate) struct WriteTimelineSample {
    generation: u64,
    index: usize,
}
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[derive(Clone, Copy)]
pub(crate) struct LiveGroupSample {
    generation: u64,
    index: usize,
}

struct Session {
    generation: u64,
    origin: Instant,
    export: WriteTimelineExport,
    operations: HashMap<[u8; 16], usize>,
    versions: HashMap<BatchVersion, usize>,
    envelopes: HashMap<(u32, u64), Vec<usize>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_memberships: usize,
}

#[derive(Default)]
pub(crate) struct WriteTimeline {
    enabled: AtomicBool,
    generation: AtomicU64,
    session: Mutex<Option<Session>>,
    waits: WriteWaitCounters,
    blocks: BlockCounters,
    owners: Vec<OwnerCounters>,
    sample_every: AtomicU64,
    seen_calls: AtomicU64,
}
impl WriteTimeline {
    pub(crate) fn with_owners(owners: impl Iterator<Item = u32>) -> Self {
        Self {
            owners: owners.map(OwnerCounters::new).collect(),
            ..Self::default()
        }
    }

    pub(crate) fn owner_queue(&self, owner: u32, queue: WriteOwnerQueue, depth: usize) {
        if !self.enabled() {
            return;
        }
        let Some(owner) = self.owners.iter().find(|counters| counters.owner == owner) else {
            return;
        };
        let second = super::owner_events::monotonic_now_ns()
            .saturating_sub(self.blocks.origin_ns.load(Ordering::Relaxed))
            / 1_000_000_000;
        let counter = &owner.queues
            [queue as usize * BLOCK_BUCKETS + second.min((BLOCK_BUCKETS - 1) as u64) as usize];
        // Relaxed: independent queue samples, never authority or wake state.
        counter.samples.fetch_add(1, Ordering::Relaxed);
        counter.depth_sum.fetch_add(depth as u64, Ordering::Relaxed);
        counter.max_depth.fetch_max(depth as u64, Ordering::Relaxed);
    }

    #[cfg(any(test, feature = "experimental-nvmeof"))]
    pub(crate) fn owner_frame(
        &self,
        owner: u32,
        batches: usize,
        puts: impl FnOnce() -> usize,
        fua: bool,
    ) {
        if !self.enabled() {
            return;
        }
        let Some(owner) = self.owners.iter().find(|counters| counters.owner == owner) else {
            return;
        };
        // Relaxed: matched submission-window counts. Skip markers are excluded.
        owner.frames.fetch_add(1, Ordering::Relaxed);
        owner
            .fua_frames
            .fetch_add(u64::from(fua), Ordering::Relaxed);
        owner.batches.fetch_add(batches as u64, Ordering::Relaxed);
        owner.puts.fetch_add(puts() as u64, Ordering::Relaxed);
    }

    pub fn arm(&self, limit: usize, sample_every: usize) -> Result<(), &'static str> {
        if limit > 65_536 {
            return Err("write timeline limit exceeds 65536");
        }
        if !(1..=1_000_000).contains(&sample_every) {
            return Err("write timeline sample interval must be between 1 and 1000000");
        }
        let mut session = self.session.lock();
        if session.is_some() {
            if self.enabled.load(Ordering::Acquire) {
                return Err("write timeline already armed");
            }
            session.take();
        }
        if limit != 0 {
            let generation = self
                .generation
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    value.checked_add(1)
                })
                .map_err(|_| "write timeline generation exhausted")?
                + 1;
            self.waits.reset();
            let origin = Instant::now();
            let origin_monotonic_ns = super::owner_events::monotonic_now_ns();
            self.blocks.reset(origin_monotonic_ns);
            self.sample_every
                .store(sample_every as u64, Ordering::Relaxed);
            self.seen_calls.store(0, Ordering::Relaxed);
            for owner in &self.owners {
                owner.reset();
            }
            *session = Some(Session {
                generation,
                origin,
                export: WriteTimelineExport {
                    origin_monotonic_ns,
                    write_wait_metrics: self.waits.snapshot(),
                    limit,
                    sample_every,
                    seen_calls: 0,
                    skipped_calls: 0,
                    records: Vec::with_capacity(limit),
                    dropped_calls: 0,
                    dropped_events: 0,
                    incomplete_calls: 0,
                    deadline_exceeded: false,
                    owner_storage_durations_available: false,
                    checkpoint_events: Vec::new(),
                    dropped_checkpoint_events: 0,
                    control_events: Vec::new(),
                    dropped_control_events: 0,
                    owner_records: Vec::new(),
                    live_groups: Vec::new(),
                    dropped_live_groups: 0,
                    owner_dropped_records: 0,
                    owner_incomplete_records: 0,
                    owner_expected_connections: 0,
                    owner_captured_connections: 0,
                    owner_errors: Vec::new(),
                    blocking_spans: Vec::new(),
                    owner_diagnostics: Vec::new(),
                },
                operations: HashMap::new(),
                versions: HashMap::new(),
                envelopes: HashMap::new(),
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                live_memberships: 0,
            });
            self.enabled.store(true, Ordering::Release);
        }
        Ok(())
    }
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn wait_enter(&self, wait: WriteWait, batches: usize) {
        // The arm is changed only at quiescent benchmark boundaries. This load
        // gates statistics only, not access to memory or write-path state.
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let window = &self.waits.windows[wait as usize];
        for _ in 0..batches {
            let depth = self.waits.add(&window.depth, 1);
            self.waits.add(&window.observations, 1);
            self.waits.add(&window.depth_sum, depth);
            window.depth_max.fetch_max(depth, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn wait_leave(&self, wait: WriteWait, batches: usize) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let window = &self.waits.windows[wait as usize];
        let _ = window
            .depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                Some(depth.saturating_sub(batches as u64))
            });
    }

    #[inline]
    pub(crate) fn wait_wake(&self, wait: WriteWait, coalesced: bool) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let window = &self.waits.windows[wait as usize];
        self.waits.add(&window.wake_attempts, 1);
        if coalesced {
            self.waits.add(&window.wake_coalesced, 1);
        }
    }

    pub fn set_owner_records(
        &self,
        records: Vec<OwnerTimelineRecord>,
        dropped: u64,
        incomplete: u64,
        expected: usize,
        captured: usize,
        errors: Vec<OwnerTimelineError>,
    ) {
        if let Some(s) = self.session.lock().as_mut() {
            s.export.owner_records = records;
            s.export.owner_dropped_records = dropped;
            s.export.owner_incomplete_records = incomplete;
            s.export.owner_expected_connections = expected;
            s.export.owner_captured_connections = captured;
            s.export.owner_storage_durations_available =
                captured == expected && incomplete == 0 && errors.is_empty();
            s.export.owner_errors = errors;
        }
    }
    /// Stop recording without waiting for a contended diagnostic mutex.
    pub fn abort(&self) {
        self.enabled.store(false, Ordering::Release);
        if let Some(mut session) = self.session.try_lock() {
            session.take();
        }
    }
    pub fn finish_with_owner_until(
        &self,
        deadline: Instant,
        owner: OwnerTimelineCollection,
    ) -> Result<WriteTimelineExport, &'static str> {
        self.enabled.store(false, Ordering::Release);
        let mut guard = if Instant::now() >= deadline {
            self.session.try_lock()
        } else {
            self.session.try_lock_until(deadline)
        }
        .ok_or("timeline recorder deadline expired")?;
        let mut session = guard.take().ok_or("write timeline is not armed")?;
        session.export.owner_records = owner.records;
        session.export.owner_dropped_records = owner.dropped;
        session.export.owner_incomplete_records = owner.incomplete;
        session.export.owner_expected_connections = owner.expected;
        session.export.owner_captured_connections = owner.captured;
        session.export.owner_storage_durations_available =
            owner.captured == owner.expected && owner.incomplete == 0 && owner.errors.is_empty();
        session.export.owner_errors = owner.errors;
        session.export.incomplete_calls = session
            .export
            .records
            .iter()
            .filter(|record| !record.completed)
            .count() as u64;
        if Instant::now() >= deadline {
            session.export.deadline_exceeded = true;
            session.export.owner_storage_durations_available = false;
        }
        session.export.write_wait_metrics = self.waits.snapshot();
        session.export.blocking_spans = self.blocks.snapshot();
        self.snapshot_sampling(&mut session.export);
        session.export.owner_diagnostics =
            self.owners.iter().map(OwnerCounters::snapshot).collect();
        Ok(session.export)
    }
    pub fn finish(&self) -> Result<WriteTimelineExport, &'static str> {
        self.enabled.store(false, Ordering::Release);
        let mut session = self
            .session
            .lock()
            .take()
            .ok_or("write timeline is not armed")?;
        session.export.write_wait_metrics = self.waits.snapshot();
        session.export.blocking_spans = self.blocks.snapshot();
        self.snapshot_sampling(&mut session.export);
        session.export.owner_diagnostics =
            self.owners.iter().map(OwnerCounters::snapshot).collect();
        session.export.incomplete_calls = session
            .export
            .records
            .iter()
            .filter(|r| !r.completed)
            .count() as u64;
        Ok(session.export)
    }
    /// Disabled captures take no clock, lock, or allocation. Counters have fixed
    /// storage, and export allocates only after the workload has drained.
    pub(crate) fn block_started(&self) -> Option<Instant> {
        self.enabled().then(Instant::now)
    }

    pub(crate) fn block_finished(
        &self,
        stage: WriteBlockStage,
        started: Option<Instant>,
        depth: usize,
    ) {
        let Some(started) = started else { return };
        let ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let second = super::owner_events::monotonic_now_ns()
            .saturating_sub(self.blocks.origin_ns.load(Ordering::Relaxed))
            / 1_000_000_000;
        let bucket = &self.blocks.buckets
            [stage as usize * BLOCK_BUCKETS + second.min((BLOCK_BUCKETS - 1) as u64) as usize];
        // Relaxed: independent diagnostics, never admission/durability authority.
        bucket.calls.fetch_add(1, Ordering::Relaxed);
        bucket.ns.fetch_add(ns, Ordering::Relaxed);
        bucket.max_ns.fetch_max(ns, Ordering::Relaxed);
        bucket.max_depth.fetch_max(depth as u64, Ordering::Relaxed);
    }

    fn snapshot_sampling(&self, export: &mut WriteTimelineExport) {
        export.seen_calls = self.seen_calls.load(Ordering::Relaxed);
        export.skipped_calls =
            export.seen_calls - export.seen_calls.div_ceil(export.sample_every as u64);
    }

    pub fn begin(&self, id: u64) -> Option<WriteTimelineSample> {
        if !self.enabled.load(Ordering::Acquire) {
            return None;
        }
        // Relaxed: this ordinal selects diagnostics only. Skipped calls take no
        // recorder mutex, avoiding a new shared lock on unsampled puts.
        let ordinal = self.seen_calls.fetch_add(1, Ordering::Relaxed);
        if !ordinal.is_multiple_of(self.sample_every.load(Ordering::Relaxed)) {
            return None;
        }
        let mut guard = self.session.lock();
        let s = guard.as_mut()?;
        if s.export.records.len() == s.export.limit {
            s.export.dropped_calls += 1;
            return None;
        }
        let index = s.export.records.len();
        s.export.records.push(WriteTimelineRecord {
            caller_sample_id: id,
            operation: None,
            batch: None,
            events: Vec::new(),
            error: None,
            error_truncated: false,
            completed: false,
        });
        Self::push(s, index, WriteTimelineStage::CallerStart, None, None);
        Some(WriteTimelineSample {
            generation: s.generation,
            index,
        })
    }
    pub fn operation(&self, sample: WriteTimelineSample, operation: [u8; 16]) {
        let mut guard = self.session.lock();
        if let Some(s) = guard.as_mut()
            && s.generation == sample.generation
            && let Some(record) = s.export.records.get_mut(sample.index)
        {
            record.operation = Some(operation);
            s.operations.insert(operation, sample.index);
        }
    }
    pub(crate) fn control_event(
        &self,
        stage: &'static str,
        owner: u32,
        envelope_id: u64,
        target: ControlTimelineTarget,
        success: Option<bool>,
    ) {
        if !self.enabled() {
            return;
        }
        let mut guard = self.session.lock();
        if let Some(s) = guard.as_mut() {
            // At most eight control events per sampled call; overflow is counted.
            if s.export.control_events.len() >= s.export.limit.saturating_mul(8) {
                s.export.dropped_control_events += 1;
            } else {
                s.export.control_events.push(ControlTimelineEvent {
                    stage,
                    elapsed_ns: s.origin.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    owner,
                    envelope_id,
                    target,
                    success,
                });
            }
        }
    }

    pub fn checkpoint(
        &self,
        stage: &'static str,
        owner: Option<u32>,
        epoch: u64,
        prefix: Option<usize>,
    ) {
        self.checkpoint_generation(stage, owner, epoch, prefix, None);
    }

    pub fn checkpoint_generation(
        &self,
        stage: &'static str,
        owner: Option<u32>,
        epoch: u64,
        prefix: Option<usize>,
        generation: Option<u64>,
    ) {
        if !self.enabled() {
            return;
        }
        let mut guard = self.session.lock();
        if let Some(s) = guard.as_mut() {
            if s.export.checkpoint_events.len() >= s.export.limit.saturating_mul(64) {
                s.export.dropped_checkpoint_events += 1;
            } else {
                s.export.checkpoint_events.push(CheckpointTimelineEvent {
                    stage,
                    owner,
                    epoch,
                    generation,
                    prefix,
                    pressure: None,
                    elapsed_ns: s.origin.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                });
            }
        }
    }
    pub fn checkpoint_pressure(
        &self,
        epoch: u64,
        generation: Option<u64>,
        pressure: CheckpointPressure,
    ) {
        if !self.enabled() {
            return;
        }
        let mut guard = self.session.lock();
        if let Some(s) = guard.as_mut() {
            if s.export.checkpoint_events.len() >= s.export.limit.saturating_mul(64) {
                s.export.dropped_checkpoint_events += 1;
            } else {
                s.export.checkpoint_events.push(CheckpointTimelineEvent {
                    stage: "admission_pressure",
                    elapsed_ns: s.origin.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    owner: None,
                    epoch,
                    generation,
                    prefix: None,
                    pressure: Some(pressure),
                });
            }
        }
    }
    pub fn batch(&self, operation: [u8; 16], version: BatchVersion) {
        if !self.enabled.load(Ordering::Acquire) {
            return;
        }
        let mut guard = self.session.lock();
        if let Some(s) = guard.as_mut()
            && let Some(&index) = s.operations.get(&operation)
        {
            s.export.records[index].batch = Some(version);
            s.versions.insert(version, index);
        }
    }
    pub fn event(&self, sample: WriteTimelineSample, stage: WriteTimelineStage) {
        let mut guard = self.session.lock();
        if let Some(s) = guard.as_mut()
            && s.generation == sample.generation
        {
            Self::push(s, sample.index, stage, None, None);
        }
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn live_group(
        &self,
        owner: u32,
        batches: impl Iterator<Item = BatchVersion>,
    ) -> Option<LiveGroupSample> {
        if !self.enabled() {
            return None;
        }
        let mut guard = self.session.lock();
        let s = guard.as_mut()?;
        let versions: Vec<_> = batches
            .filter(|version| s.versions.contains_key(version))
            .collect();
        if versions.is_empty() {
            return None;
        }
        if s.export.live_groups.len() >= s.export.limit
            || s.live_memberships.saturating_add(versions.len()) > s.export.limit.saturating_mul(4)
        {
            s.export.dropped_live_groups += 1;
            return None;
        }
        s.live_memberships += versions.len();
        let index = s.export.live_groups.len();
        s.export.live_groups.push(LiveGroupTimeline {
            owner,
            batches: versions,
            reservation_token: None,
            owner_validation_ns: None,
            owner_persistence: None,
            wal_position: None,
            file_offset: None,
            frame_len: None,
            wal_file: None,
            namespace_block_size: None,
        });
        Some(LiveGroupSample {
            generation: s.generation,
            index,
        })
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(crate) fn live_group_position(
        &self,
        group: Option<LiveGroupSample>,
        wal_position: u64,
        file_offset: u64,
        frame_len: u32,
        wal_file: u64,
        namespace_block_size: u32,
    ) {
        let Some(group) = group else { return };
        if let Some(row) = self
            .session
            .lock()
            .as_mut()
            .filter(|s| s.generation == group.generation)
            .and_then(|s| s.export.live_groups.get_mut(group.index))
        {
            row.wal_position = Some(wal_position);
            row.file_offset = Some(file_offset);
            row.frame_len = Some(frame_len);
            row.wal_file = Some(wal_file);
            row.namespace_block_size = Some(namespace_block_size);
        }
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn live_group_event(&self, group: Option<LiveGroupSample>, stage: WriteTimelineStage) {
        if group.is_none() {
            return;
        }
        self.live_group_event_at(group, stage, Instant::now());
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(crate) fn live_group_event_at(
        &self,
        group: Option<LiveGroupSample>,
        stage: WriteTimelineStage,
        at: Instant,
    ) {
        let Some(group) = group else { return };
        let mut guard = self.session.lock();
        if let Some(s) = guard.as_mut()
            && s.generation == group.generation
        {
            let Some(row) = s.export.live_groups.get(group.index) else {
                return;
            };
            let owner = row.owner;
            let indexes: Vec<_> = row
                .batches
                .iter()
                .filter_map(|version| s.versions.get(version).copied())
                .collect();
            for index in indexes {
                Self::push_at(s, index, stage, Some(owner), None, at);
            }
        }
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn live_group_token(&self, group: Option<LiveGroupSample>, token: [u8; 16]) {
        let Some(group) = group else { return };
        if let Some(row) = self
            .session
            .lock()
            .as_mut()
            .filter(|s| s.generation == group.generation)
            .and_then(|s| s.export.live_groups.get_mut(group.index))
        {
            row.reservation_token = Some(token);
        }
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn live_group_owner(
        &self,
        group: Option<LiveGroupSample>,
        validation_ns: Option<u64>,
        persistence: super::storage::StorageWriteProfile,
    ) {
        let Some(group) = group else { return };
        if let Some(row) = self
            .session
            .lock()
            .as_mut()
            .filter(|s| s.generation == group.generation)
            .and_then(|s| s.export.live_groups.get_mut(group.index))
        {
            row.owner_validation_ns = validation_ns;
            row.owner_persistence = Some(persistence);
        }
    }

    pub fn batch_event(
        &self,
        version: BatchVersion,
        stage: WriteTimelineStage,
        owner: Option<u32>,
        envelope: Option<u64>,
    ) {
        if !self.enabled.load(Ordering::Acquire) {
            return;
        }
        let mut guard = self.session.lock();
        if let Some(s) = guard.as_mut()
            && let Some(&index) = s.versions.get(&version)
        {
            Self::push(s, index, stage, owner, envelope);
            if let (Some(owner), Some(envelope)) = (owner, envelope) {
                s.envelopes
                    .entry((owner, envelope))
                    .or_default()
                    .push(index);
            }
        }
    }
    pub fn response(&self, owner: u32, envelope: u64) {
        if !self.enabled() {
            return;
        }
        let mut guard = self.session.lock();
        if let Some(s) = guard.as_mut()
            && let Some(indexes) = s.envelopes.remove(&(owner, envelope))
        {
            for index in indexes {
                Self::push(
                    s,
                    index,
                    WriteTimelineStage::CompletionHarvested,
                    Some(owner),
                    Some(envelope),
                );
            }
        }
    }
    pub fn complete(&self, sample: WriteTimelineSample, error: Option<String>) {
        let mut guard = self.session.lock();
        if let Some(s) = guard.as_mut()
            && s.generation == sample.generation
        {
            Self::push(
                s,
                sample.index,
                WriteTimelineStage::CallerFinished,
                None,
                None,
            );
            if let Some(record) = s.export.records.get_mut(sample.index) {
                record.completed = true;
                let mut truncated = false;
                let error = error.map(|mut message| {
                    if message.len() > 1024 {
                        let mut end = 1024;
                        while !message.is_char_boundary(end) {
                            end -= 1;
                        }
                        message.truncate(end);
                        truncated = true;
                    }
                    message
                });
                record.error_truncated = truncated;
                record.error = error;
            }
        }
    }
    fn push(
        s: &mut Session,
        index: usize,
        stage: WriteTimelineStage,
        owner: Option<u32>,
        envelope_id: Option<u64>,
    ) {
        Self::push_at(s, index, stage, owner, envelope_id, Instant::now());
    }

    fn push_at(
        s: &mut Session,
        index: usize,
        stage: WriteTimelineStage,
        owner: Option<u32>,
        envelope_id: Option<u64>,
        at: Instant,
    ) {
        if let Some(record) = s.export.records.get_mut(index) {
            if record.events.len() >= MAX_EVENTS {
                s.export.dropped_events += 1;
                return;
            }
            record.events.push(WriteTimelineEvent {
                stage,
                elapsed_ns: at
                    .saturating_duration_since(s.origin)
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
                owner,
                envelope_id,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wait_counters_are_untouched_when_off() {
        let timeline = WriteTimeline::default();
        let window = &timeline.waits.windows[0];
        window.depth.store(9, Ordering::Relaxed);
        window.observations.store(7, Ordering::Relaxed);
        window.depth_sum.store(11, Ordering::Relaxed);
        window.depth_max.store(4, Ordering::Relaxed);
        window.wake_attempts.store(3, Ordering::Relaxed);
        window.wake_coalesced.store(2, Ordering::Relaxed);
        timeline.arm(0, 1).unwrap();
        timeline.wait_enter(WriteWait::OwnerEnqueueToDequeue, 2);
        timeline.wait_leave(WriteWait::OwnerEnqueueToDequeue, 1);
        timeline.wait_wake(WriteWait::OwnerEnqueueToDequeue, true);
        assert_eq!(window.depth.load(Ordering::Relaxed), 9);
        let snapshot = timeline.waits.snapshot();
        let row = &snapshot.windows[0];
        assert_eq!(row.observations, 7);
        assert_eq!(row.depth_sum, 11);
        assert_eq!(row.depth_max, 4);
        assert_eq!(row.wake_attempts, 3);
        assert_eq!(row.wake_coalesced, 2);
        assert!(!snapshot.saturated);
        assert!(timeline.finish().is_err());
    }

    #[test]
    fn wait_counts_follow_entries_exits_and_actual_coalesced_hints() {
        let timeline = WriteTimeline::default();
        timeline.arm(1, 1).unwrap();
        for wait in [
            WriteWait::OwnerEnqueueToDequeue,
            WriteWait::AdmittedToAppendEnqueue,
            WriteWait::HarvestToDurable,
        ] {
            timeline.wait_enter(wait, 2);
            timeline.wait_leave(wait, 1);
            timeline.wait_enter(wait, 2);
            timeline.wait_leave(wait, 3);
        }
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        for wait in [
            WriteWait::OwnerEnqueueToDequeue,
            WriteWait::HarvestToDurable,
        ] {
            for _ in 0..2 {
                let result = sender.try_send(());
                timeline.wait_wake(
                    wait,
                    matches!(result, Err(std::sync::mpsc::TrySendError::Full(()))),
                );
            }
            receiver.try_recv().unwrap();
        }
        let export = timeline.finish().unwrap();
        assert!(export.write_wait_metrics.enabled);
        assert!(!export.write_wait_metrics.saturated);
        for (index, row) in export.write_wait_metrics.windows.iter().enumerate() {
            assert_eq!(row.observations, 4);
            assert_eq!(row.depth_sum, 8);
            assert_eq!(row.depth_max, 3);
            assert_eq!(row.wake_attempts, if index == 1 { 0 } else { 2 });
            assert_eq!(row.wake_coalesced, if index == 1 { 0 } else { 1 });
            assert_eq!(
                timeline.waits.windows[index].depth.load(Ordering::Relaxed),
                0
            );
        }
    }

    #[test]
    fn wait_counters_saturate_and_flag_the_cap() {
        let timeline = WriteTimeline::default();
        timeline.arm(1, 1).unwrap();
        let window = &timeline.waits.windows[0];
        for counter in [
            &window.depth,
            &window.observations,
            &window.depth_sum,
            &window.wake_attempts,
            &window.wake_coalesced,
        ] {
            counter.store(u64::MAX - 1, Ordering::Relaxed);
        }
        timeline.wait_enter(WriteWait::OwnerEnqueueToDequeue, 1);
        assert!(timeline.waits.snapshot().saturated);
        timeline.wait_enter(WriteWait::OwnerEnqueueToDequeue, 1);
        timeline.wait_wake(WriteWait::OwnerEnqueueToDequeue, true);
        timeline.wait_wake(WriteWait::OwnerEnqueueToDequeue, true);
        let export = timeline.finish().unwrap();
        let row = &export.write_wait_metrics.windows[0];
        assert!(export.write_wait_metrics.saturated);
        assert_eq!(row.observations, u64::MAX);
        assert_eq!(row.depth_sum, u64::MAX);
        assert_eq!(row.depth_max, u64::MAX);
        assert_eq!(row.wake_attempts, u64::MAX);
        assert_eq!(row.wake_coalesced, u64::MAX);
        assert_eq!(window.depth.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn wait_counters_are_monotone_until_finish_and_reset_on_rearm() {
        let timeline = WriteTimeline::default();
        timeline.arm(1, 1).unwrap();
        timeline.wait_enter(WriteWait::OwnerEnqueueToDequeue, 1);
        let first = timeline.waits.snapshot();
        timeline.wait_leave(WriteWait::OwnerEnqueueToDequeue, 1);
        timeline.wait_enter(WriteWait::OwnerEnqueueToDequeue, 1);
        timeline.wait_wake(WriteWait::OwnerEnqueueToDequeue, false);
        let export = timeline.finish().unwrap();
        let before = &first.windows[0];
        let after = &export.write_wait_metrics.windows[0];
        assert!(after.observations >= before.observations);
        assert!(after.depth_sum >= before.depth_sum);
        assert!(after.depth_max >= before.depth_max);
        assert!(after.wake_attempts >= before.wake_attempts);
        assert!(after.wake_coalesced >= before.wake_coalesced);
        timeline.wait_enter(WriteWait::OwnerEnqueueToDequeue, 1);
        timeline.wait_wake(WriteWait::OwnerEnqueueToDequeue, true);
        assert_eq!(timeline.waits.snapshot().windows[0].observations, 2);
        assert_eq!(timeline.waits.snapshot().windows[0].wake_attempts, 1);
        timeline.arm(1, 1).unwrap();
        let next = timeline.finish().unwrap();
        assert_eq!(next.write_wait_metrics.windows[0].observations, 0);
        assert_eq!(next.write_wait_metrics.windows[0].wake_attempts, 0);
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    #[test]
    fn live_group_links_coalesced_calls_and_preserves_failed_control() {
        let timeline = WriteTimeline::default();
        let versions = [
            BatchVersion {
                epoch: 1,
                sequence: 1,
            },
            BatchVersion {
                epoch: 1,
                sequence: 2,
            },
        ];
        assert!(timeline.live_group(3, versions.into_iter()).is_none());
        timeline.arm(2, 1).unwrap();
        let a = timeline.begin(11).unwrap();
        let b = timeline.begin(12).unwrap();
        timeline.operation(a, [1; 16]);
        timeline.operation(b, [2; 16]);
        timeline.batch([1; 16], versions[0]);
        timeline.batch([2; 16], versions[1]);
        let group = timeline.live_group(3, versions.into_iter()).unwrap();
        timeline.live_group_event(Some(group), WriteTimelineStage::LiveReserveStarted);
        timeline.live_group_token(Some(group), [9; 16]);
        timeline.live_group_event(Some(group), WriteTimelineStage::LiveCompleteStarted);
        timeline.complete(a, Some("control failed".into()));
        timeline.complete(b, Some("control failed".into()));
        let export = timeline.finish().unwrap();
        assert_eq!(export.live_groups[0].batches, versions);
        assert_eq!(export.live_groups[0].reservation_token, Some([9; 16]));
        assert!(export.live_groups[0].owner_persistence.is_none());
        for record in &export.records {
            assert_eq!(record.error.as_deref(), Some("control failed"));
            assert!(
                record
                    .events
                    .iter()
                    .any(|e| e.stage == WriteTimelineStage::LiveCompleteStarted)
            );
            assert!(
                !record
                    .events
                    .iter()
                    .any(|e| e.stage == WriteTimelineStage::LiveCompleteCompleted)
            );
            assert!(
                record
                    .events
                    .windows(2)
                    .all(|e| e[0].elapsed_ns <= e[1].elapsed_ns)
            );
        }
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    #[test]
    fn live_group_limit_and_stale_generation_are_explicit() {
        let timeline = WriteTimeline::default();
        let version = BatchVersion {
            epoch: 1,
            sequence: 1,
        };
        timeline.arm(1, 1).unwrap();
        let sample = timeline.begin(1).unwrap();
        timeline.operation(sample, [1; 16]);
        timeline.batch([1; 16], version);
        let stale = timeline.live_group(1, [version].into_iter()).unwrap();
        assert!(timeline.live_group(1, [version].into_iter()).is_none());
        let first = timeline.finish().unwrap();
        assert_eq!(first.dropped_live_groups, 1);
        timeline.arm(1, 1).unwrap();
        let sample = timeline.begin(2).unwrap();
        timeline.operation(sample, [2; 16]);
        timeline.batch([2; 16], version);
        timeline.live_group(1, [version].into_iter()).unwrap();
        timeline.live_group_token(Some(stale), [7; 16]);
        timeline.live_group_event(Some(stale), WriteTimelineStage::LiveIoCompleted);
        let second = timeline.finish().unwrap();
        assert_eq!(second.live_groups[0].reservation_token, None);
        assert_eq!(second.records[0].events.len(), 1);
    }

    #[test]
    fn checkpoint_events_are_bounded_and_keep_generation() {
        let timeline = WriteTimeline::default();
        timeline.checkpoint_generation("disabled", None, 1, None, Some(9));
        timeline.arm(1, 1).unwrap();
        for _ in 0..65 {
            timeline.checkpoint_generation("capture_start", None, 1, None, Some(9));
        }
        let export = timeline.finish().unwrap();
        assert_eq!(export.checkpoint_events.len(), 64);
        assert_eq!(export.dropped_checkpoint_events, 1);
        assert!(
            export
                .checkpoint_events
                .iter()
                .all(|event| event.generation == Some(9))
        );
    }

    #[test]
    fn control_events_are_bounded_and_keep_request_identity() {
        let timeline = WriteTimeline::default();
        let target = ControlTimelineTarget {
            kind: "position",
            epoch: 1,
            extent: None,
            through: Some(1234),
            prefix: None,
        };
        timeline.control_event("submit", 2, 7, target, None);
        timeline.arm(1, 1).unwrap();
        for id in 0..9 {
            timeline.control_event("submit", 2, id, target, None);
        }
        let export = timeline.finish().unwrap();
        assert_eq!(export.control_events.len(), 8);
        assert_eq!(export.dropped_control_events, 1);
        assert_eq!(export.control_events[7].envelope_id, 7);
        assert_eq!(export.control_events[7].target.through, Some(1234));
    }

    #[test]
    fn bounded_events_and_incomplete_calls_are_explicit() {
        let timeline = WriteTimeline::default();
        timeline.arm(1, 1).unwrap();
        let sample = timeline.begin(7).unwrap();
        for _ in 0..MAX_EVENTS {
            timeline.event(sample, WriteTimelineStage::GateAcquired);
        }
        assert!(timeline.begin(8).is_none());
        let export = timeline.finish().unwrap();
        assert_eq!(export.dropped_calls, 1);
        assert_eq!(export.dropped_events, 1);
        assert_eq!(export.incomplete_calls, 1);
        assert!(!export.records[0].completed);
        assert!(export.records[0].batch.is_none());
    }

    #[test]
    fn stale_sample_cannot_mutate_rearmed_session() {
        let timeline = WriteTimeline::default();
        timeline.arm(1, 1).unwrap();
        let stale = timeline.begin(1).unwrap();
        let first = timeline.finish().unwrap();
        assert_eq!(first.incomplete_calls, 1);
        timeline.arm(1, 1).unwrap();
        let current = timeline.begin(2).unwrap();
        timeline.operation(stale, [7; 16]);
        timeline.event(stale, WriteTimelineStage::GateAcquired);
        timeline.complete(stale, Some("stale".into()));
        timeline.operation(current, [8; 16]);
        timeline.complete(current, None);
        let next = timeline.finish().unwrap();
        assert_eq!(next.records[0].caller_sample_id, 2);
        assert_eq!(next.records[0].operation, Some([8; 16]));
        assert!(next.records[0].error.is_none());
        assert_eq!(next.records[0].events.len(), 2);
        assert_eq!(next.incomplete_calls, 0);
    }

    #[test]
    fn expired_finish_retains_collected_owner_page_as_incomplete() {
        let timeline = WriteTimeline::default();
        timeline.arm(1, 1).unwrap();
        let sample = timeline.begin(19).unwrap();
        timeline.complete(sample, None);
        let record = OwnerTimelineRecord {
            owner: 1,
            envelope_id: 7,
            epoch: Some(1),
            success: true,
            error: None,
            error_truncated: false,
            control_queue_ns: Some(2),
            session_admission_wait_ns: Some(1),
            appender_queue_ns: Some(1),
            flusher_queue_ns: None,
            append_barrier_wait_ns: None,
            append_wal_ns: None,
            append_validation_ns: None,
            append_basic_validation_ns: None,
            append_conflict_lookup_ns: None,
            append_geometry_capacity_ns: None,
            append_metadata_reservation_ns: None,
            append_descriptor_preparation_ns: None,
            append_errors: 0,
            append_retries: 0,
            append_persist_ns: None,
            wal_file_persist_ns: None,
            handler_ns: None,
            response_ready_ns: None,
            namespace_persist_ns: None,
            file_persist_calls: 0,
            namespace_persist_calls: 0,
            appended_groups: 0,
            batch_count: Some(1),
            encoded_bytes: Some(4),
            persistence_capture: None,
            ready_append_prefix: None,
        };
        let export = timeline
            .finish_with_owner_until(
                Instant::now(),
                OwnerTimelineCollection {
                    records: vec![record],
                    dropped: 0,
                    incomplete: 0,
                    expected: 2,
                    captured: 0,
                    errors: Vec::new(),
                },
            )
            .unwrap();
        assert!(export.deadline_exceeded);
        assert!(!export.owner_storage_durations_available);
        assert_eq!(export.records[0].caller_sample_id, 19);
        assert_eq!(export.owner_records[0].envelope_id, 7);
        assert_eq!(export.owner_captured_connections, 0);
    }
}

#[cfg(test)]
mod block_tests {
    use super::*;

    #[test]
    fn block_spans_are_disabled_bounded_and_reset_on_arm() {
        let timeline = WriteTimeline::default();
        assert!(timeline.block_started().is_none());
        timeline.arm(1, 1).unwrap();
        timeline.block_finished(WriteBlockStage::OwnerQueue, timeline.block_started(), 7);
        let export = timeline.finish().unwrap();
        assert_eq!(export.blocking_spans.len(), 1);
        assert_eq!(export.blocking_spans[0].calls, 1);
        assert_eq!(export.blocking_spans[0].max_depth, 7);
        assert!(timeline.block_started().is_none());
        timeline.arm(1, 1).unwrap();
        timeline.blocks.origin_ns.store(0, Ordering::Relaxed);
        timeline.block_finished(WriteBlockStage::Publish, timeline.block_started(), 128);
        let export = timeline.finish().unwrap();
        assert_eq!(export.blocking_spans.len(), 1);
        assert_eq!(export.blocking_spans[0].second, BLOCK_BUCKETS - 1);
        assert_eq!(export.blocking_spans[0].max_depth, 128);
    }
}

#[cfg(test)]
mod distributed_sample_tests {
    use super::*;

    #[test]
    fn stride_retains_calls_across_window_and_stays_bounded() {
        let timeline = WriteTimeline::default();
        assert!(timeline.arm(3, 0).is_err());
        timeline.arm(3, 3).unwrap();
        for id in 0..10 {
            if let Some(sample) = timeline.begin(id) {
                timeline.complete(sample, None);
            }
        }
        let export = timeline.finish().unwrap();
        assert_eq!(
            export
                .records
                .iter()
                .map(|record| record.caller_sample_id)
                .collect::<Vec<_>>(),
            [0, 3, 6]
        );
        assert_eq!(export.seen_calls, 10);
        assert_eq!(export.skipped_calls, 6);
        assert_eq!(export.dropped_calls, 1);
    }

    #[test]
    fn owner_diagnostics_are_window_matched_and_disabled_by_default() {
        let timeline = WriteTimeline::with_owners([2, 7].into_iter());
        timeline.owner_frame(2, 1, || panic!("disabled diagnostics evaluated puts"), true);
        timeline.arm(1, 1).unwrap();
        timeline.owner_frame(2, 2, || 3, true);
        timeline.owner_frame(7, 1, || 1, false);
        timeline.owner_queue(2, WriteOwnerQueue::LeasedFlights, 3);
        let export = timeline.finish().unwrap();
        assert_eq!(export.owner_diagnostics[0].submitted_frames, 1);
        assert_eq!(export.owner_diagnostics[0].submitted_fua_frames, 1);
        assert_eq!(export.owner_diagnostics[0].submitted_batches, 2);
        assert_eq!(export.owner_diagnostics[0].submitted_puts, 3);
        assert_eq!(export.owner_diagnostics[0].queues[0].max_depth, 3);
        assert_eq!(export.owner_diagnostics[1].submitted_frames, 1);
        assert_eq!(export.owner_diagnostics[1].submitted_fua_frames, 0);
        timeline.arm(1, 1).unwrap();
        let export = timeline.finish().unwrap();
        assert_eq!(export.owner_diagnostics[0].submitted_frames, 0);
        assert!(export.owner_diagnostics[0].queues.is_empty());
    }
}
