//! Opt-in coordinator-local write diagnostics. Timestamps share one monotonic origin;
//! they do not measure network service time or compare clocks across hosts.
use super::types::BatchVersion;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

mod counters;
#[cfg(test)]
mod distributed_sample_tests;

#[cfg(test)]
mod tests;

pub use counters::{
    WriteBlockBucket, WriteOwnerDiagnostics, WriteOwnerQueueBucket, WriteWaitMetrics,
    WriteWaitWindowMetrics,
};
pub(crate) use counters::{WriteBlockStage, WriteOwnerQueue, WriteWait};

use counters::*;

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
    LiveNamespaceSetupStarted,
    LiveNamespaceSetupCompleted,
    LiveIoStarted,
    /// The CQE was observed. Leased groups can complete out of frame order.
    LiveIoCompleted,
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
/// A single live group. Versions include sampled calls only.
#[derive(Debug, Serialize)]
pub struct LiveGroupTimeline {
    pub owner: u32,
    pub batches: Vec<BatchVersion>,
    pub reservation_token: Option<[u8; 16]>,
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
    /// Always empty: S11f removed the position-proof path that was the only
    /// producer. The field and `dropped_checkpoint_events` stay in the export
    /// shape because the analyzer (`experiments/kv-comparison/analyze.py`)
    /// requires both keys: it rejects a missing key, a non-integer
    /// `dropped_checkpoint_events`, and a `checkpoint_events` that is not a
    /// list.
    pub checkpoint_events: Vec<CheckpointTimelineEvent>,
    /// Always 0 for the same reason as [`Self::checkpoint_events`].
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

    pub fn live_group_event(&self, group: Option<LiveGroupSample>, stage: WriteTimelineStage) {
        if group.is_none() {
            return;
        }
        self.live_group_event_at(group, stage, Instant::now());
    }

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
