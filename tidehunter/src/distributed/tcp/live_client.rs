//! Initiator configuration for the live owner-WAL NVMe-oF data plane.

use super::LiveQueueGeometry;
use serde::{Deserialize, Serialize};

mod resume;
mod resume_reaper;
mod write_accounting;
pub(crate) use resume::BreakKind;
pub(crate) use resume_reaper::ResumeReaper;
pub(crate) use write_accounting::LiveGroupBudget;
use write_accounting::record_write_io;

use crate::distributed::nvmeof::live_metrics::LiveTimingStage;
use crate::distributed::types::{BatchVersion, PhysicalValueAddress};
use std::time::Instant;

#[cfg(any(test, feature = "test-utils"))]
static BENCH_LOCAL_BUFFERED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(any(test, feature = "test-utils"))]
fn bench_open_local_file(
    path: &std::path::Path,
    bytes: u64,
    block_size: u32,
    access: crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess,
) -> std::io::Result<crate::distributed::nvmeof::live_namespace::LiveNamespace> {
    use crate::distributed::nvmeof::live_namespace::LiveNamespace;
    // Relaxed: the serial ignored benchmark sets this before creating any queue.
    if BENCH_LOCAL_BUFFERED.load(std::sync::atomic::Ordering::Relaxed) {
        LiveNamespace::open_local_file_buffered(path, bytes, block_size, access)
    } else {
        LiveNamespace::open_local_file(path, bytes, block_size, access)
    }
}

/// Synchronous leased durability mechanism. F6b gained 34–40% throughput at
/// 64 KiB c16 with shared flushes, but lost 6–17% at 1–16 KiB without a threshold.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeasedSync {
    PerFrameFua,
    #[default]
    GroupFlush,
}

/// Stable identity for one storage owner when several owners share a target VM.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveInitiatorIdentity {
    pub host_nqn: String,
    pub host_id: [u8; 16],
}

/// Bounded initiator resources. The permanent per-owner export's namespace
/// identity is computed from the WAL file index.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveClientConfig {
    pub host_nqn: String,
    /// Stable initiator UUID; must remain the same across authority reopen.
    pub host_id: [u8; 16],
    /// Empty retains the base identity. Otherwise every partition needs an entry.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub owner_identities: std::collections::BTreeMap<u32, LiveInitiatorIdentity>,
    pub max_inflight_requests: usize,
    /// Optional software window. None uses queue credit and owner append occupancy only.
    /// Receipts always release in frame order.
    #[serde(default)]
    pub leased_write_window: Option<usize>,
    #[serde(default)]
    pub leased_sync: LeasedSync,
    /// GroupFlush only: frames whose physical (padded) length is smaller than this use FUA.
    /// Defaults to 65536: F6b gained 34–40% throughput at 64 KiB c16, but lost
    /// 6–17% at 1–16 KiB without a threshold.
    #[serde(default = "LiveClientConfig::default_leased_flush_min_frame_bytes")]
    pub leased_flush_min_frame_bytes: usize,
    pub max_inflight_bytes: usize,
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
    /// Every granted extent must match this tier.
    pub leased_durability: crate::distributed::durability::DurabilityTier,
    #[serde(default = "default_inline_reads")]
    pub lease_ahead: bool,
    /// Use IOPOLL for leased writes; requires a controller poll queue.
    /// IOPOLL rings cannot submit Fsync, so polled writes keep per-frame FUA.
    #[serde(default)]
    pub polled_writes: bool,
    /// Disable caller-inline reads without changing lane-worker reads.
    #[serde(default = "default_inline_reads")]
    pub inline_reads: bool,
    #[serde(default)]
    pub queue_geometry: LiveQueueGeometry,
}
fn default_inline_reads() -> bool {
    true
}

/// Retained frame bytes per owner worker: two write windows, so one adoption
/// round plus one window of payload is the most a resume can need (D-W1-3).
const RESUME_RETAIN_WINDOWS: usize = 2;
/// Hard cap on retained frame bytes per owner worker.
const RESUME_RETAIN_CAP: usize = 64 * 1024 * 1024;

impl LiveClientConfig {
    /// Physical frame threshold selected from the F6b large/small-frame crossover.
    pub const fn default_leased_flush_min_frame_bytes() -> usize {
        65536
    }
    /// Acknowledgement contract of a leased write at `tier`.
    pub const fn acknowledgement(
        tier: crate::distributed::durability::DurabilityTier,
    ) -> &'static str {
        use crate::distributed::durability::DurabilityTier;
        match tier {
            DurabilityTier::Native => "leased extent: page-cache write",
            DurabilityTier::Synchronous => {
                "synchronous leased: written and covered by a FUA write or a later namespace flush before acknowledgement; owner frontier root asynchronous"
            }
        }
    }

    fn for_owner(&self, partition: u32) -> Result<std::borrow::Cow<'_, Self>, &'static str> {
        if self.owner_identities.is_empty() {
            return Ok(std::borrow::Cow::Borrowed(self));
        }
        let identity = self
            .owner_identities
            .get(&partition)
            .ok_or("missing live NVMe-oF owner initiator identity")?;
        // Copy only scalar settings, not the map of other owners' identities.
        Ok(std::borrow::Cow::Owned(Self {
            host_nqn: identity.host_nqn.clone(),
            host_id: identity.host_id,
            owner_identities: Default::default(),
            max_inflight_requests: self.max_inflight_requests,
            leased_write_window: self.leased_write_window,
            leased_sync: self.leased_sync,
            leased_flush_min_frame_bytes: self.leased_flush_min_frame_bytes,
            max_inflight_bytes: self.max_inflight_bytes,
            connect_timeout_ms: self.connect_timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
            leased_durability: self.leased_durability,
            lease_ahead: self.lease_ahead,
            polled_writes: self.polled_writes,
            inline_reads: self.inline_reads,
            queue_geometry: self.queue_geometry,
        }))
    }

    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        self.queue_geometry.validate()?;
        for (partition, identity) in &self.owner_identities {
            if identity.host_id == [0; 16]
                || !identity.host_nqn.starts_with("nqn.")
                || identity.host_nqn.len() > 223
                || !identity
                    .host_nqn
                    .bytes()
                    .all(|b| b.is_ascii_graphic() && b != b',' && b != b'=')
                || self
                    .owner_identities
                    .range(..partition)
                    .any(|(_, previous)| {
                        previous.host_nqn == identity.host_nqn
                            || previous.host_id == identity.host_id
                    })
            {
                return Err("invalid or duplicate live NVMe-oF owner initiator identity");
            }
        }
        if self.polled_writes && self.queue_geometry.poll_queues == 0 {
            return Err("polled live writes require a poll queue");
        }
        if self.queue_geometry.poll_queues > 0 && !self.inline_reads {
            return Err("polled live reads require inline reads");
        }
        if self.host_id == [0; 16]
            || !self.host_nqn.starts_with("nqn.")
            || self.host_nqn.len() > 223
            || !self
                .host_nqn
                .bytes()
                .all(|b| b.is_ascii_graphic() && b != b',' && b != b'=')
            || !(1..=32768).contains(&self.max_inflight_requests)
            || self
                .leased_write_window
                .is_some_and(|window| window == 0 || window > self.max_inflight_requests)
            || self.max_inflight_bytes == 0
            || !(1..=300_000).contains(&self.connect_timeout_ms)
            || !(1..=300_000).contains(&self.io_timeout_ms)
        {
            return Err("invalid live NVMe-oF initiator configuration");
        }
        Ok(())
    }
}

pub(crate) struct LiveWriteQueue {
    target: crate::distributed::nvmeof::live_connect::LiveConnectTarget,
    io: crate::distributed::nvmeof::live_io::LiveIo,
    _lease: Option<std::sync::Arc<crate::distributed::nvmeof::live_pool::LiveControllerLease>>,
    // Only a failed, admitted leased write owns this snapshot. Detachment moves
    // it with the DMA queue; terminal counts are published once, without timing.
    failed_accounting: Option<FailedWriteAccounting>,
    #[cfg(test)]
    drain_pause: Option<WriteDrainPause>,
}

struct FailedWriteAccounting {
    before: crate::distributed::nvmeof::live_io::LiveIoStats,
    counters: std::sync::Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeCounters>,
}

#[cfg(test)]
struct WriteDrainPause {
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

impl LiveWriteQueue {
    /// The caller owns this detached queue, not the client mutex, while waiting.
    pub(crate) fn drain_failed(&mut self) -> Result<(), super::TcpError> {
        #[cfg(test)]
        if let Some(pause) = self.drain_pause.take() {
            pause
                .entered
                .try_send(())
                .map_err(super::TcpError::component)?;
            pause
                .release
                .recv_timeout(std::time::Duration::from_secs(10))
                .map_err(super::TcpError::component)?;
        }
        let result = self.io.shutdown().map_err(super::TcpError::component);
        if self.io.outstanding() == 0 {
            self.publish_failed_counts();
        }
        result.map(|_| ())
    }

    fn publish_failed_counts(&mut self) {
        let Some(accounting) = self.failed_accounting.take() else {
            return;
        };
        let after = self.io.stats();
        let before = &accounting.before;
        accounting.counters.record_write(
            after
                .completed_write_ops
                .saturating_sub(before.completed_write_ops),
            after
                .completed_write_bytes
                .saturating_sub(before.completed_write_bytes),
        );
        accounting.counters.record_terminal_write_outcomes(
            after
                .failed_write_ops
                .saturating_sub(before.failed_write_ops),
            after.short_write_ops.saturating_sub(before.short_write_ops),
            after
                .short_write_bytes
                .saturating_sub(before.short_write_bytes),
        );
    }
}

impl Drop for LiveWriteQueue {
    fn drop(&mut self) {
        // Direct Client drop also retains DMA to terminal retirement. Worker
        // failures detach first, so this never waits under their client mutex.
        if self.io.outstanding() != 0 && self.io.shutdown().is_err() {
            std::process::abort();
        }
        self.publish_failed_counts();
    }
}

#[derive(Clone, Copy, Debug)]
struct LeasedPrefix {
    end: u64,
    count: u64,
    digest: [u8; 32],
}

#[derive(Clone, Copy)]
struct PendingAdoption {
    through: u64,
    close: bool,
    lease_next: bool,
    target: PendingTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingTarget {
    Current,
    Closing,
    Prelease,
}

struct ExtentState {
    lease: super::LiveLeased,
    cursor: u64,
    written_end: u64,
    acknowledged: LeasedPrefix,
    reserved: LeasedPrefix,
    adopted: LeasedPrefix,
    declared_through: u64,
    /// At most 1024 boundaries; full current history settles before submission.
    prefixes: std::collections::VecDeque<LeasedPrefix>,
}

struct LeasedFlight {
    id: u64,
    start: u64,
    end: u64,
    prefix: LeasedPrefix,
    receipts: Vec<super::DurableBatch>,
    deadline: Instant,
    submitted_at: Instant,
    completed: bool,
    durable_at_completion: bool,
    sample: Option<crate::distributed::write_timeline::LiveGroupSample>,
    frame_len: usize,
}

pub(crate) enum LeasedSubmit {
    Submitted(u64),
    Deferred(Vec<super::CompleteBatch>),
}

/// One write of one encoded frame: the identity its CQE returns, where it lands,
/// how much it covers, and the barrier and deadline it is submitted with.
#[derive(Clone, Copy)]
struct FrameWrite {
    id: u64,
    offset: u64,
    len: usize,
    sync: crate::distributed::nvmeof::live_io::LiveWriteSync,
    deadline: Instant,
}

/// One submitted frame that the owner has not adopted yet, kept so a resume can
/// rewrite it as identical bytes at its identical offset (W1). The group is
/// retained instead of its encoded bytes: re-encoding is deterministic and
/// costs no hot-path copy, and the owner's receipt digest proves the identity.
struct RetainedFrame {
    /// The extent grant that owns these bytes; it names the binding a rewrite
    /// opens and keeps two extents' frames apart during a rollover.
    token: [u8; 16],
    start: u64,
    end: u64,
    /// The sync the original submission used, so a resent `synchronous` frame
    /// still implies the same barrier before its receipt is released.
    durable_at_completion: bool,
    frame: crate::distributed::group::LiveFrame,
}

/// One frame a resume resent, as the test observes it: the grant it belongs to,
/// its absolute start, its length and the completion id the resend used.
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
struct ResumeRewrite {
    token: [u8; 16],
    start: u64,
    len: usize,
    id: u64,
}

/// The existing per-owner worker owns this state, with no additional lock.
#[derive(Default)]
pub(super) struct LeasedWriter {
    current: Option<ExtentState>,
    closing: Option<ExtentState>,
    next: Option<super::LiveLeased>,
    receipt_config: Option<super::PartitionConfig>,
    pending: Option<PendingAdoption>,
    pending_started: Option<Instant>,
    /// Two deques, allocated at configuration and recycled after validated close.
    prefix_buffers: [std::collections::VecDeque<LeasedPrefix>; 2],
    flights: std::collections::VecDeque<LeasedFlight>,
    completions: Vec<crate::distributed::nvmeof::live_io::LiveCompletion>,
    flight_bound: usize,
    next_flight_id: u64,
    /// Capture is immutable until the flush completes; later completions need
    /// another flush even if the device happened to persist them too.
    flush_in_flight: Option<(u64, u64, Instant)>,
    flush_submitted_at: Option<Instant>,
    flushed_through_seq: u64,
    worker_wake: Option<std::os::fd::RawFd>,
    worker_wake_registered: bool,
    queue_rebound: bool,
    /// Frames submitted but not yet adopted, oldest first. Bound:
    /// `resume_retain_bytes`; full retention turns the next submission into
    /// `LeasedSubmit::Deferred`, never into an unbounded queue.
    retained: std::collections::VecDeque<RetainedFrame>,
    retained_bytes: usize,
    /// A detached queue the reaper could not take. It is never dropped on the
    /// worker thread: `take_failed_writer` hands it to the shutdown drain.
    undrained: Option<LiveWriteQueue>,
    /// A resume detached the queue that owned the flights still in the deque,
    /// so no live DMA belongs to any flight. Cleared when a new frame is
    /// submitted, because that frame's CQE the new queue owes.
    queue_detached: bool,
    /// A resume is not re-entrant: it runs on the worker thread, inside the one
    /// client guard of the operation that broke.
    resuming: bool,
    /// Resumes without a frame submitted and adopted between them.
    resume_consecutive: u32,
    /// A probe resume kept the surviving queue and granted the outstanding
    /// work one more window, and no CQE has arrived since. A deadline that
    /// expires while it is set proves the queue does not progress, so the next
    /// resume detaches and rewrites instead of probing again (design 7.3).
    probe_unhealed: bool,
    /// The last failure of this client came from a resume that refused or
    /// failed, so the worker names that stage instead of the operation's own.
    resume_failed: bool,
    #[cfg(test)]
    resume_retain_bytes: Option<usize>,
    /// The window a session resume grants outstanding work instead of one I/O
    /// deadline, so a test can observe the probe's escalation without waiting.
    #[cfg(test)]
    retime_window: Option<std::time::Duration>,
    /// Resume steps completed by the resume now running, so a test can act at
    /// a step boundary.
    #[cfg(test)]
    resume_steps: u32,
    /// The lease stamp to lapse once `resume_steps` reaches the given count.
    #[cfg(test)]
    lapse_lease_after: Option<(
        u32,
        std::sync::Arc<crate::distributed::remote_db::coordinator_lease::LeaseValidity>,
    )>,
    /// The resume attempt now running, counted from zero per resume. The
    /// episode's internal retry advances it, so a test can tell the attempt
    /// that met a condition from the one that retried it.
    #[cfg(test)]
    resume_attempt: u32,
    /// Steps each finished attempt completed, in attempt order.
    #[cfg(test)]
    attempt_steps: Vec<u32>,
    /// The attempt and completed-step count at which the test's lapse landed.
    #[cfg(test)]
    lapse_at: Option<(u32, u32)>,
    /// The lease stamp to lapse once `rewrites.len()` reaches the given count,
    /// so a test can land the lapse between two frames of one rewrite.
    #[cfg(test)]
    lapse_lease_after_rewrite: Option<(
        u32,
        std::sync::Arc<crate::distributed::remote_db::coordinator_lease::LeaseValidity>,
    )>,
    /// Rounds a resume re-probed a close the owner was still executing.
    #[cfg(test)]
    close_reprobes: u32,
    /// Notified once per declaration close the owner reported as in progress.
    #[cfg(test)]
    close_in_progress: Option<std::sync::mpsc::SyncSender<()>>,
    /// A test rendezvous before the declaration step: the resume announces that
    /// it is about to declare, then waits for the test, so a test can put the
    /// owner into a state that is otherwise a race to enter.
    #[cfg(test)]
    declaration_gate: Option<(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
    /// Extents a resume probed, in probe order.
    #[cfg(test)]
    resume_targets: Vec<PendingTarget>,
    /// Frames a resume resent, in submission order.
    #[cfg(test)]
    rewrites: Vec<ResumeRewrite>,
    /// Every completion id a write command used, in submission order.
    #[cfg(test)]
    submitted_ids: Vec<u64>,
    #[cfg(test)]
    next_write_result: Option<i32>,
    #[cfg(test)]
    next_submit_result: Option<i32>,
    /// A raw CQE result that overrides the next group flush's completion.
    #[cfg(test)]
    next_flush_result: Option<i32>,
    #[cfg(test)]
    delay_write: bool,
    #[cfg(test)]
    corrupt_next_digest: bool,
    #[cfg(test)]
    invalidate_adoption_token: bool,
    #[cfg(test)]
    drain_pause: Option<WriteDrainPause>,
    #[cfg(test)]
    flight_adoption_waits: u64,
    #[cfg(test)]
    largest_close_profile: super::AdoptProfile,
    /// Discard the next prelease reply, as a session break loses it, and record
    /// it instead of applying it.
    #[cfg(test)]
    drop_prelease_reply: bool,
    /// The prelease reply [`Self::drop_prelease_reply`] discarded.
    #[cfg(test)]
    lost_prelease: Option<super::LiveLeased>,
    /// Value-WAL allocations of submitted batches, oldest first, when the hybrid
    /// index writer has enabled recording.
    ///
    /// One entry per batch of a submitted group: its logical version and the
    /// batch-frame address that is the input to
    /// [`crate::distributed::hybrid::position::VersionPosition::member`] (the
    /// offset includes the batch's own native CRC header).
    ///
    /// `None` (the default) records nothing, so no other route and no test that
    /// never drains can grow it. The hybrid coordinator enables it with
    /// [`super::Client::enable_lease_reservations`] and drains it with
    /// [`super::Client::take_lease_reservations`] after every successful
    /// submission, so at most one group (`group_limits.max_batches`) is
    /// outstanding: the drain is the bound. Entries are pushed only after the
    /// frame is accepted, so a failed submission leaves none behind.
    reservations: Option<std::collections::VecDeque<(BatchVersion, PhysicalValueAddress)>>,
}

impl LiveWriteQueue {
    fn matches(
        &self,
        binding: &crate::distributed::nvmeof::live_storage::LiveTargetBinding,
    ) -> bool {
        let target = &self.target;
        !binding.readonly
            && target.nqn == binding.nqn
            && target.uuid == binding.uuid
            && target.nsid == binding.nsid
            && target.bytes == binding.bytes
            && target.block_size == binding.block_size
            && target.address == binding.address
            && target.service == binding.service
            && target.transport == binding.transport
    }
}

impl super::Client {
    pub(crate) fn set_live_pool(
        &mut self,
        pool: std::sync::Arc<crate::distributed::nvmeof::live_pool::LiveControllerPool>,
    ) {
        self.live_pool = Some(pool);
    }

    pub(crate) fn set_live_metrics(
        &mut self,
        metrics: std::sync::Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics>,
    ) -> Result<(), super::TcpError> {
        self.live_counters = Some(metrics.register().map_err(super::TcpError::component)?);
        self.live_metrics = Some(metrics);
        Ok(())
    }

    /// Drain accepted DMA before dropping the cached queue and controller lease.
    /// An error retains the queue so its buffers and namespace remain owned.
    pub(crate) fn drain_live_writer(
        &mut self,
        released: &mut Vec<(u64, Vec<super::DurableBatch>)>,
        timeline: Option<&crate::distributed::write_timeline::WriteTimeline>,
    ) -> Result<(), super::TcpError> {
        if !self.live_write_failed {
            self.close_leased_extent(released, timeline)?;
        }
        self.drain_live_queue()
    }

    fn drain_live_queue(&mut self) -> Result<(), super::TcpError> {
        if self
            .live_writer
            .as_ref()
            .is_some_and(|writer| writer.io.outstanding() != 0)
        {
            return Err(super::TcpError::new(
                "live DMA remains owned; detach failed queue for drain",
            ));
        }
        let Some(writer) = self.live_writer.as_mut() else {
            return Ok(());
        };
        let completions = writer.io.shutdown().map_err(super::TcpError::component)?;
        self.live_writer.take();
        if !completions.is_empty() {
            return Err(super::TcpError::new(
                "unexpected outstanding live write during drain",
            ));
        }
        Ok(())
    }

    /// Transfer failed DMA ownership without waiting under the client mutex.
    pub(crate) fn take_failed_live_writer(&mut self) -> Option<LiveWriteQueue> {
        // A queue a resume detached but could not hand over comes first: only
        // the shutdown drain may wait for its DMA.
        if let Some(queue) = self.live_leased.undrained.take() {
            return Some(queue);
        }
        if !self.live_write_failed {
            return None;
        }
        let writer = self.live_writer.take()?;
        #[cfg(test)]
        let writer = {
            let mut writer = writer;
            writer.drain_pause = self.live_leased.drain_pause.take();
            writer
        };
        Some(writer)
    }

    /// Stop admission under the same client mutex used by the owner worker.
    /// A queued append cannot recreate a queue after shutdown drains this one.
    pub(crate) fn close_live_writer(
        &mut self,
        released: &mut Vec<(u64, Vec<super::DurableBatch>)>,
        timeline: Option<&crate::distributed::write_timeline::WriteTimeline>,
    ) -> Result<(), super::TcpError> {
        self.live_writer_closing = true;
        self.drain_live_writer(released, timeline)
    }

    pub(crate) fn configure_live_nvme(
        &mut self,
        config: &LiveClientConfig,
        owner_append_occupancy: usize,
    ) -> Result<(), super::TcpError> {
        config.validate().map_err(super::TcpError::new)?;
        let selected = config
            .for_owner(self.expected.partition)
            .map_err(super::TcpError::new)?;
        let config = selected.as_ref();
        if self.live_writer_closing {
            return Err(super::TcpError::new("live writer admission is closed"));
        }
        if !self.connected_status().live_nvmeof || self.has_pending_io() {
            return Err(super::TcpError::new(
                "live NVMe client is not ready for configuration",
            ));
        }
        let capacity = config.max_inflight_requests.min(owner_append_occupancy);
        self.live_leased.flight_bound = capacity;
        self.live_leased.flights.reserve(capacity);
        self.live_leased.completions.reserve(capacity);
        for buffer in &mut self.live_leased.prefix_buffers {
            if buffer.capacity() == 0 {
                buffer
                    .try_reserve_exact(1024)
                    .map_err(super::TcpError::component)?;
            }
        }
        if self.live_config.as_ref() != Some(config) {
            {
                assert!(
                    self.live_leased.flights.is_empty(),
                    "queue change has waiting leased replies"
                );
                self.drain_live_writer(&mut Vec::new(), None)?;
            }
            self.live_config = Some(selected.into_owned());
            #[cfg(test)]
            {
                self.live_write_local_fixture |= std::env::var_os("TH_MODEL_CHILD_ROOT").is_some();
            }
        }
        Ok(())
    }

    pub(crate) fn live_nvme_configured(&self) -> bool {
        self.live_config.is_some()
    }

    /// This client's live counters, when a benchmark or service registered them.
    pub(crate) fn live_counters(
        &self,
    ) -> Option<&std::sync::Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeCounters>> {
        self.live_counters.as_ref()
    }

    pub(crate) fn record_live_write_error(&mut self) {
        self.live_write_failed = true;
        if let Some(counters) = &self.live_counters {
            counters.record_error();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn io_timings_use_retired_cq_and_submission_counts() {
        use crate::distributed::nvmeof::{live_io::LiveIoStats, live_metrics::LiveNvmeMetrics};
        let metrics = LiveNvmeMetrics::new();
        let counters = metrics.register().unwrap();
        let stats = LiveIoStats {
            completed_write_ops: 1,
            completed_write_bytes: 4096,
            submissions: 1,
            completions: 1,
            queue_ns: 11,
            submit_ns: 13,
            submit_to_cq_ns: 17,
            ..LiveIoStats::default()
        };
        record_write_io(&counters, &LiveIoStats::default(), &stats);
        let second = LiveIoStats {
            completed_write_ops: 2,
            completed_write_bytes: 8192,
            submissions: 2,
            completions: 2,
            queue_ns: 22,
            submit_ns: 26,
            submit_to_cq_ns: 34,
            ..LiveIoStats::default()
        };
        record_write_io(&counters, &stats, &second);
        let counts = metrics.snapshot().counts;
        assert_eq!(
            (counts.completed_write_ops, counts.completed_write_bytes),
            (2, 8192)
        );
        assert_eq!((counts.queue_ns, counts.queue_samples), (22, 2));
        assert_eq!((counts.submit_ns, counts.submit_samples), (26, 2));
        assert_eq!((counts.cq_ns, counts.cq_samples), (34, 2));
    }

    #[test]
    fn local_file_reuses_write_queue_with_per_group_stats() {
        use crate::distributed::nvmeof::{
            live_io::{LiveIo, LiveIoResult, LiveSubmitOutcome},
            live_metrics::LiveNvmeMetrics,
            live_namespace::{LiveNamespace, LiveNamespaceAccess},
        };
        use std::os::unix::fs::FileExt;
        use std::time::{Duration, Instant};
        let dir = tempdir::TempDir::new("live-writer-reuse").unwrap();
        let path = dir.path().join("wal");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(8192).unwrap();
        drop(file);
        let namespace =
            LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadWrite)
                .unwrap();
        let mut io = LiveIo::new(namespace, 1, 8191, false).unwrap();
        let metrics = LiveNvmeMetrics::new();
        let counters = metrics.register().unwrap();
        for index in 0..2 {
            let before = io.stats();
            let frame = vec![index as u8 + 1; 4096];
            let deadline = Instant::now() + Duration::from_secs(10);
            assert_eq!(
                io.submit_write(
                    1,
                    index * 4096,
                    &frame,
                    crate::distributed::nvmeof::live_io::LiveWriteSync::None,
                    deadline,
                )
                .unwrap(),
                LiveSubmitOutcome::Submitted
            );
            loop {
                let mut completions = io.progress().unwrap();
                if let Some(completion) = completions.pop() {
                    assert!(matches!(completion.result, Ok(LiveIoResult::Written)));
                    break;
                }
                assert!(Instant::now() < deadline, "local write did not retire");
                std::thread::yield_now();
            }
            record_write_io(&counters, &before, &io.stats());
        }
        let counts = metrics.snapshot().counts;
        assert_eq!(
            (counts.completed_write_ops, counts.completed_write_bytes),
            (2, 8192)
        );
        assert_eq!((counts.queue_samples, counts.cq_samples), (2, 2));
        assert!(io.shutdown().unwrap().is_empty());
        let file = std::fs::File::open(path).unwrap();
        let mut bytes = [0u8; 8192];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert!(bytes[..4096].iter().all(|&b| b == 1));
        assert!(bytes[4096..].iter().all(|&b| b == 2));
    }

    #[test]
    fn rejects_invalid_initiator_identity_and_queue_limits() {
        let mut config = LiveClientConfig {
            lease_ahead: true,
            polled_writes: false,
            host_nqn: "nqn.2014-08.org.nvmexpress:uuid:11111111-1111-1111-1111-111111111111".into(),
            host_id: [1; 16],
            owner_identities: Default::default(),
            leased_write_window: Some(4),
            leased_sync: Default::default(),
            leased_flush_min_frame_bytes: 0,
            max_inflight_requests: 32,
            max_inflight_bytes: 4096,
            connect_timeout_ms: 1000,
            io_timeout_ms: 1000,
            leased_durability: crate::distributed::durability::DurabilityTier::Native,
            inline_reads: true,
            queue_geometry: Default::default(),
        };
        assert!(config.validate().is_ok());
        let mut old = serde_json::to_value(&config).unwrap();
        old.as_object_mut().unwrap().remove("queue_geometry");
        let mut removed = serde_json::to_value(&config).unwrap();
        removed["export_mode"] = serde_json::json!("per_owner");
        assert!(serde_json::from_value::<LiveClientConfig>(removed).is_err());
        old.as_object_mut().unwrap().remove("leased_write_window");
        old.as_object_mut().unwrap().remove("leased_sync");
        old.as_object_mut()
            .unwrap()
            .remove("leased_flush_min_frame_bytes");
        assert!(!old.as_object().unwrap().contains_key("owner_identities"));
        let restored: LiveClientConfig = serde_json::from_value(old).unwrap();
        assert_eq!(restored.queue_geometry, LiveQueueGeometry::default());
        assert_eq!(restored.leased_write_window, None);
        assert_eq!(restored.leased_sync, LeasedSync::GroupFlush);
        assert_eq!(restored.leased_flush_min_frame_bytes, 65536);
        config.leased_flush_min_frame_bytes = 8192;
        let restored: LiveClientConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        assert_eq!(restored.leased_flush_min_frame_bytes, 8192);
        assert_eq!(
            config.for_owner(1).unwrap().leased_flush_min_frame_bytes,
            8192
        );
        for (mode, name) in [
            (LeasedSync::PerFrameFua, "per_frame_fua"),
            (LeasedSync::GroupFlush, "group_flush"),
        ] {
            assert_eq!(serde_json::to_value(mode).unwrap(), name);
            assert_eq!(
                serde_json::from_value::<LeasedSync>(serde_json::json!(name)).unwrap(),
                mode
            );
        }
        assert!(restored.owner_identities.is_empty());
        config.leased_write_window = None;
        assert!(config.validate().is_ok());
        config.leased_write_window = Some(0);
        assert!(config.validate().is_err());
        config.leased_write_window = Some(config.max_inflight_requests + 1);
        assert!(config.validate().is_err());
        config.leased_write_window = Some(config.max_inflight_requests);
        assert!(config.validate().is_ok());
        config.leased_write_window = Some(4);
        config.queue_geometry.poll_queues = 1;
        assert!(config.validate().is_ok());
        let mut polled = config.clone();
        polled.polled_writes = true;
        polled.leased_durability = crate::distributed::durability::DurabilityTier::Synchronous;
        polled.leased_sync = LeasedSync::GroupFlush;
        assert!(polled.validate().is_ok());
        config.inline_reads = false;
        assert_eq!(
            config.validate(),
            Err("polled live reads require inline reads")
        );
        config.inline_reads = true;
        config.queue_geometry.io_queues = 0;
        assert_eq!(config.validate(), Err("invalid live queue geometry"));
        config.queue_geometry = LiveQueueGeometry::default();
        config.host_id = [0; 16];
        assert!(config.validate().is_err());
        config.host_id = [1; 16];
        config.host_nqn.push(',');
        assert!(config.validate().is_err());
        config.host_nqn.pop();
        config.max_inflight_requests = 0;
        assert!(config.validate().is_err());
        config.max_inflight_requests = 32769;
        assert!(config.validate().is_err());
        config.max_inflight_requests = 1;
        config.leased_write_window = Some(1);
        config.io_timeout_ms = 0;
        assert!(config.validate().is_err());
        let first = LiveInitiatorIdentity {
            host_nqn: "nqn.2026-01.test:owner-0".into(),
            host_id: [2; 16],
        };
        let second = LiveInitiatorIdentity {
            host_nqn: "nqn.2026-01.test:owner-1".into(),
            host_id: [3; 16],
        };
        config.io_timeout_ms = 1000;
        config.owner_identities = [(0, first.clone()), (1, second.clone())].into();
        assert!(config.validate().is_ok());
        assert_eq!(config.for_owner(0).unwrap().host_nqn, first.host_nqn);
        assert_eq!(config.for_owner(1).unwrap().host_id, second.host_id);
        assert!(config.for_owner(2).is_err());
        config.owner_identities.get_mut(&1).unwrap().host_nqn = first.host_nqn;
        assert!(config.validate().is_err());
        config.owner_identities.get_mut(&1).unwrap().host_nqn = second.host_nqn;
        config.owner_identities.get_mut(&1).unwrap().host_id = first.host_id;
        assert!(config.validate().is_err());
    }
}

impl LeasedWriter {
    fn install(&mut self, leased: super::LiveLeased, expected: &super::PartitionConfig) {
        // Client.expected is the immutable handshake identity. Retain one config
        // for shared receipt arithmetic, updating only the granted epoch/generation.
        let receipt_config = self.receipt_config.get_or_insert_with(|| expected.clone());
        receipt_config.inventory.epoch = leased.extent.segment.epoch;
        receipt_config.inventory.segment_generation = leased.extent.segment.generation;
        let start = leased.extent.start;
        let prefixes = std::mem::take(&mut self.prefix_buffers[0]);
        self.prefix_buffers.swap(0, 1);
        self.current = Some(ExtentState {
            lease: leased,
            cursor: start,
            written_end: start,
            acknowledged: LeasedPrefix {
                end: start,
                count: 0,
                digest: [0; 32],
            },
            reserved: LeasedPrefix {
                end: start,
                count: 0,
                digest: [0; 32],
            },
            adopted: LeasedPrefix {
                end: start,
                count: 0,
                digest: [0; 32],
            },
            declared_through: start,
            prefixes,
        });
    }

    fn recycle(&mut self, mut extent: ExtentState) {
        // A closed extent adopted its whole prefix, so its retained frames were
        // pruned already; drop any residue so a later resume cannot rewrite a
        // grant that no longer exists.
        let token = extent.lease.extent.token;
        let mut dropped = 0usize;
        self.retained.retain(|frame| {
            let keep = frame.token != token;
            if !keep {
                dropped += frame.frame.len();
            }
            keep
        });
        self.retained_bytes = self.retained_bytes.saturating_sub(dropped);
        extent.prefixes.clear();
        let index = if self.prefix_buffers[0].capacity() == 0 {
            0
        } else {
            1
        };
        self.prefix_buffers[index] = extent.prefixes;
    }
}

impl ExtentState {
    fn validate_adoption(
        &mut self,
        pending: PendingAdoption,
        reply: &super::LiveAdopted,
    ) -> Result<(), super::TcpError> {
        let current = &self.lease;
        let adoption = &reply.adoption;
        let submitted_end = self.prefixes.back().map_or(self.adopted.end, |p| p.end);
        // A probe of the closing extent can meet a close the owner already
        // applied, whose reply died with the old session. That is the rollover
        // completing, not a mismatch, but only when the owner adopted the whole
        // prefix this coordinator wrote: anything less would retire an extent
        // whose retained frames still owe a resend.
        let rollover_completed = adoption.closed && !pending.close;
        if adoption.token != current.extent.token
            || adoption.adopted_end < pending.through
            || adoption.adopted_end > submitted_end
            || adoption.adopted_end > current.extent.end
            || (adoption.closed != pending.close && !rollover_completed)
            || (rollover_completed
                && (pending.target != PendingTarget::Closing
                    || adoption.adopted_end != self.written_end))
            || reply.next.is_some() != pending.lease_next
        {
            return Err(super::TcpError::new(
                "leased adoption identity or frontier mismatch",
            ));
        }
        let prefix = if adoption.adopted_end == self.adopted.end {
            self.adopted
        } else {
            *self
                .prefixes
                .iter()
                .find(|p| p.end == adoption.adopted_end)
                .ok_or_else(|| {
                    super::TcpError::new("leased adoption is not a submitted frame boundary")
                })?
        };
        if adoption.receipt_count != prefix.count || adoption.receipt_digest != prefix.digest {
            return Err(super::TcpError::new(
                "leased adoption receipt count or digest mismatch",
            ));
        }
        if let Some(next) = &reply.next
            && next.extent.start != current.extent.end
        {
            return Err(super::TcpError::new("next leased extent is not contiguous"));
        }
        while self.prefixes.front().is_some_and(|p| p.end <= prefix.end) {
            self.prefixes.pop_front();
        }
        self.adopted = prefix;
        if pending.close && !self.prefixes.is_empty() {
            return Err(super::TcpError::new(
                "closed extent has unadopted submitted frames",
            ));
        }
        Ok(())
    }
}

impl super::Client {
    pub(crate) fn install_leased_extent(
        &mut self,
        leased: super::LiveLeased,
    ) -> Result<(), super::TcpError> {
        if !self.valid_live_extent(&leased, leased.extent.segment.epoch) {
            self.live_write_failed = true;
            return Err(super::TcpError::new(
                "leased extent geometry or identity differs",
            ));
        }
        let requested = self
            .live_config
            .as_ref()
            .map(|config| config.leased_durability);
        if requested != Some(leased.tier) {
            self.live_write_failed = true;
            return Err(super::TcpError::new(format!(
                "leased durability mismatch: requested {requested:?}, owner {:?}",
                leased.tier,
            )));
        }
        if let Some(counters) = &self.live_counters {
            counters.record_lease_profile(leased.profile);
        }
        self.live_leased.install(leased, &self.expected);
        if self.lease_ahead() {
            self.submit_leased_adoption()?;
        }
        Ok(())
    }

    fn check_leased_adoption(
        &mut self,
        pending: PendingAdoption,
        reply: &super::LiveAdopted,
    ) -> Result<(), super::TcpError> {
        let previous = match pending.target {
            PendingTarget::Current => self.live_leased.current.as_ref(),
            PendingTarget::Closing => self.live_leased.closing.as_ref(),
            PendingTarget::Prelease => None,
        }
        .map_or(0, |extent| extent.adopted.end);
        let extent = match pending.target {
            PendingTarget::Current => self.live_leased.current.as_mut(),
            PendingTarget::Closing => self.live_leased.closing.as_mut(),
            PendingTarget::Prelease => None,
        };
        let result = extent
            .ok_or_else(|| super::TcpError::new("adoption without extent"))?
            .validate_adoption(pending, reply);
        if result.is_err() {
            self.live_write_failed = true;
        }
        if result.is_ok() {
            let adopted_end = reply.adoption.adopted_end;
            self.prune_retained(reply.adoption.token, adopted_end);
            if adopted_end > previous {
                // A frame the owner adopted proves the last resume healed the
                // path, so a later flap starts from a fresh budget.
                self.live_leased.resume_consecutive = 0;
            }
            if let Some(counters) = &self.live_counters {
                counters.record_adopt_profile(reply.profile);
            }
        }
        #[cfg(test)]
        if result.is_ok()
            && pending.close
            && (reply.profile.frames, reply.profile.adopt_ns)
                > (
                    self.live_leased.largest_close_profile.frames,
                    self.live_leased.largest_close_profile.adopt_ns,
                )
        {
            self.live_leased.largest_close_profile = reply.profile;
        }
        result
    }

    fn lease_ahead(&self) -> bool {
        self.live_config
            .as_ref()
            .is_some_and(|config| config.lease_ahead)
    }

    pub(crate) fn leased_adoption_pending(&self) -> bool {
        self.live_leased.pending.is_some()
    }

    fn ensure_leased_healthy(&self) -> Result<(), super::TcpError> {
        if self.live_write_failed {
            Err(super::TcpError::new("live writer authority is poisoned"))
        } else {
            Ok(())
        }
    }

    /// Retire a closing extent the owner closed. The harvest path and the
    /// resume's declaration both run this one bookkeeping.
    fn recycle_closing(&mut self) -> Result<(), super::TcpError> {
        let closing = self
            .live_leased
            .closing
            .take()
            .ok_or_else(|| super::TcpError::new("closed extent missing"))?;
        self.live_leased.recycle(closing);
        Ok(())
    }

    /// Harvest only; the worker drives the shared reactor before this call.
    pub(crate) fn harvest_leased_adoption(&mut self) -> Result<bool, super::TcpError> {
        self.ensure_leased_healthy()?;
        let Some(pending) = self.live_leased.pending else {
            return Ok(false);
        };
        let extent = match pending.target {
            PendingTarget::Closing => self.live_leased.closing.as_ref(),
            PendingTarget::Current | PendingTarget::Prelease => self.live_leased.current.as_ref(),
        }
        .ok_or_else(|| super::TcpError::new("pending adoption without extent"))?;
        let (epoch, token, end) = (
            extent.lease.extent.segment.epoch,
            extent.lease.extent.token,
            extent.lease.extent.end,
        );
        let result = if pending.target == PendingTarget::Prelease {
            let Some(reply) = self.take_prelease_live_extent(epoch) else {
                return Ok(false);
            };
            reply.and_then(|leased| {
                let tier = self
                    .live_config
                    .as_ref()
                    .map(|config| config.leased_durability);
                if leased.extent.start != end || tier != Some(leased.tier) {
                    return Err(super::TcpError::new(
                        "preleased extent identity, tier or continuity differs",
                    ));
                }
                if let Some(counters) = &self.live_counters {
                    counters.record_lease_profile(leased.profile);
                    counters.record_extent_prelease();
                }
                #[cfg(test)]
                if std::mem::take(&mut self.live_leased.drop_prelease_reply) {
                    // The owner reserved the extent; this session never learned
                    // it, as a break before the reply does.
                    self.live_leased.lost_prelease = Some(leased);
                    return Ok(());
                }
                self.live_leased.next = Some(leased);
                Ok(())
            })
        } else {
            let Some(reply) = self.take_adopt_live_extent(
                epoch,
                token,
                pending.through,
                pending.close,
                pending.lease_next,
            ) else {
                return Ok(false);
            };
            reply.and_then(|reply| self.check_leased_adoption(pending, &reply))
        };
        self.live_leased.pending = None;
        let started = self.live_leased.pending_started.take();
        if result.is_err() {
            self.live_write_failed = true;
        }
        if result.is_ok() {
            #[cfg(test)]
            crate::failpoints::write_model::adoption(
                pending.target != PendingTarget::Prelease,
                false,
            );
            if pending.target != PendingTarget::Prelease
                && let (Some(started), Some(counters)) = (started, &self.live_counters)
            {
                counters.record_timing(
                    LiveTimingStage::AdoptionAsync,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    1,
                );
            }
            if pending.target == PendingTarget::Closing && pending.close {
                self.recycle_closing()?;
            } else if pending.target != PendingTarget::Prelease {
                let extent = match pending.target {
                    PendingTarget::Current => self.live_leased.current.as_mut(),
                    PendingTarget::Closing => self.live_leased.closing.as_mut(),
                    PendingTarget::Prelease => None,
                }
                .ok_or_else(|| super::TcpError::new("adopted extent missing"))?;
                extent.declared_through = extent.declared_through.max(extent.adopted.end);
            }
        }
        result.map(|()| true)
    }

    /// Called after delivering the frame receipts. A busy control slot is not an
    /// error: the next pass submits the accumulated prefix when it becomes free.
    pub(crate) fn submit_leased_adoption(&mut self) -> Result<(), super::TcpError> {
        self.ensure_leased_healthy()?;
        if self.live_leased.pending.is_some() {
            return Ok(());
        }
        let (target, extent, close) = if let Some(closing) = &self.live_leased.closing {
            (PendingTarget::Closing, closing, true)
        } else if let Some(current) = &self.live_leased.current {
            if self.lease_ahead() && self.live_leased.next.is_none() {
                (PendingTarget::Prelease, current, false)
            } else if current.written_end > current.declared_through {
                (PendingTarget::Current, current, false)
            } else {
                return Ok(());
            }
        } else {
            return Ok(());
        };
        let (epoch, token, through) = (
            extent.lease.extent.segment.epoch,
            extent.lease.extent.token,
            extent.written_end,
        );
        let result = if target == PendingTarget::Prelease {
            self.submit_prelease_live_extent(epoch, token)
        } else {
            self.submit_adopt_live_extent(epoch, token, through, close, false)
        };
        match result {
            Ok(true) => {
                #[cfg(test)]
                crate::failpoints::write_model::adoption(target != PendingTarget::Prelease, true);
                self.live_leased.pending_started = Some(Instant::now());
                self.live_leased.pending = Some(PendingAdoption {
                    through,
                    close,
                    lease_next: false,
                    target,
                });
                if target != PendingTarget::Prelease {
                    let extent = if target == PendingTarget::Closing {
                        self.live_leased.closing.as_mut()
                    } else {
                        self.live_leased.current.as_mut()
                    }
                    .ok_or_else(|| super::TcpError::new("submitted extent missing"))?;
                    extent.declared_through = through;
                }
                if let Some(counters) = &self.live_counters {
                    counters.record_metadata_request();
                    if target != PendingTarget::Prelease {
                        counters.record_adoption(false);
                    }
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(error) => {
                self.live_write_failed = true;
                Err(error)
            }
        }
    }

    fn wait_leased_adoption(&mut self) -> Result<(), super::TcpError> {
        let mut polling = super::reactor::ActivePoll::default();
        // The existing control slot has a deadline; progress enforces it.
        while self.live_leased.pending.is_some() {
            self.progress_session()?;
            self.harvest_leased_adoption()?;
            if self.live_leased.pending.is_some() {
                self.pause_session_poll(&mut polling);
            }
        }
        Ok(())
    }

    fn wait_closing_extent(&mut self, require_next: bool) -> Result<(), super::TcpError> {
        let mut polling = super::reactor::ActivePoll::default();
        while self.live_leased.closing.is_some()
            || (require_next && self.live_leased.next.is_none())
        {
            self.submit_leased_adoption()?;
            self.progress_session()?;
            self.harvest_leased_adoption()?;
            if self.live_leased.closing.is_some()
                || (require_next && self.live_leased.next.is_none())
            {
                self.pause_session_poll(&mut polling);
            }
        }
        Ok(())
    }

    fn adopt_leased_sync(&mut self, close: bool, lease_next: bool) -> Result<(), super::TcpError> {
        self.ensure_leased_healthy()?;
        self.wait_leased_adoption()?;
        let Some(current) = &self.live_leased.current else {
            return Ok(());
        };
        let (epoch, token) = (
            current.lease.extent.segment.epoch,
            current.lease.extent.token,
        );
        let pending = PendingAdoption {
            through: current.written_end,
            close,
            lease_next,
            target: PendingTarget::Current,
        };
        if let Some(counters) = &self.live_counters {
            counters.record_metadata_request();
            counters.record_adoption(true);
        }
        let reply = self.adopt_live_extent(epoch, token, pending.through, close, lease_next)?;
        self.check_leased_adoption(pending, &reply)?;
        #[cfg(test)]
        crate::failpoints::write_model::boundary("leased_adoption_completed");
        if let Some(current) = self.live_leased.current.as_mut() {
            current.declared_through = current.written_end;
        }
        if close && let Some(current) = self.live_leased.current.take() {
            self.live_leased.recycle(current);
        }
        if let Some(next) = reply.next {
            self.install_leased_extent(next)?;
            if let Some(counters) = &self.live_counters {
                counters.record_extent_lease();
            }
        }
        Ok(())
    }

    pub(crate) fn wait_leased_control(
        &mut self,
        released: &mut Vec<(u64, Vec<super::DurableBatch>)>,
        timeline: Option<&crate::distributed::write_timeline::WriteTimeline>,
    ) -> Result<(), super::TcpError> {
        let result = self.leased_step(BreakKind::Session, |client| {
            client
                .drain_leased_flights(released, timeline)
                .and_then(|()| client.wait_leased_adoption())
        });
        if result.is_err() {
            self.live_write_failed = true;
        }
        result
    }

    pub(crate) fn settle_leased_extent(
        &mut self,
        released: &mut Vec<(u64, Vec<super::DurableBatch>)>,
        timeline: Option<&crate::distributed::write_timeline::WriteTimeline>,
    ) -> Result<(), super::TcpError> {
        let started = Instant::now();
        let has_extent = self.live_leased.current.is_some();
        // The sequence is idempotent, so a transient break resumes once and
        // runs it again from the owner's real frontier.
        let result = self.leased_step(BreakKind::Session, |client| {
            client
                .drain_leased_flights(released, timeline)
                .and_then(|()| client.wait_closing_extent(false))
                .and_then(|()| client.adopt_leased_sync(false, false))
        });
        if result.is_err() {
            self.live_write_failed = true;
        } else if has_extent && let Some(counters) = &self.live_counters {
            counters.record_timing(
                LiveTimingStage::AdoptionSettle,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                1,
            );
        }
        result
    }

    pub(crate) fn close_leased_extent(
        &mut self,
        released: &mut Vec<(u64, Vec<super::DurableBatch>)>,
        timeline: Option<&crate::distributed::write_timeline::WriteTimeline>,
    ) -> Result<(), super::TcpError> {
        let result = self.leased_step(BreakKind::Session, |client| {
            client.ensure_leased_healthy()?;
            client.drain_leased_flights(released, timeline)?;
            client.wait_closing_extent(false)?;
            client.wait_leased_adoption()?;
            if client.live_leased.current.is_none() {
                return Ok(());
            }
            client.write_leased_skip()?;
            client.adopt_leased_sync(true, false)?;
            if let Some(next) = client.live_leased.next.take() {
                client.live_leased.install(next, &client.expected);
                client.write_leased_skip()?;
                client.adopt_leased_sync(true, false)?;
            }
            Ok(())
        });
        if result.is_err() {
            self.live_write_failed = true;
        }
        result
    }

    fn ensure_leased_queue(&mut self) -> Result<(), super::TcpError> {
        let bound = match self.live_leased.current.as_ref() {
            Some(current) => self
                .live_writer
                .as_ref()
                .is_some_and(|w| w.matches(&current.lease.binding)),
            None => return Err(super::TcpError::new("live leased extent missing")),
        };
        if bound {
            return Ok(());
        }
        if self.live_writer.is_some() {
            // Replacing a queue that still owns DMA is safe only when nothing
            // waits on it. After a resume detached that queue, no flight is
            // owed a CQE by the one now bound, so the check does not apply.
            if !self.live_leased.queue_detached {
                debug_assert!(self.live_leased.flights.is_empty());
                if !self.live_leased.flights.is_empty() {
                    return Err(super::TcpError::new(
                        "leased queue rebind with writes in flight",
                    ));
                }
            }
            self.live_leased.queue_rebound = true;
            self.drain_live_queue()?;
        }
        // Move the grant out for the open: the helper needs `&mut self` and a
        // reference to the grant, which the borrow checker cannot split across
        // a call. The move is free and a rebind is not the per-frame path.
        let leased = self
            .live_leased
            .current
            .take()
            .ok_or_else(|| super::TcpError::new("live leased extent missing"))?;
        let opened = self.open_leased_queue_with(&leased.lease);
        self.live_leased.current = Some(leased);
        opened
    }

    /// Open a write queue on `leased`'s binding. No flight deque precondition:
    /// the caller owns the decision that no live DMA remains on an older queue.
    fn open_leased_queue_with(
        &mut self,
        leased: &super::LiveLeased,
    ) -> Result<(), super::TcpError> {
        use crate::distributed::nvmeof::{
            live_connect::LiveConnectTarget, live_io::LiveIo, live_namespace::LiveNamespaceAccess,
        };
        use std::time::{Duration, Instant};
        let binding = &leased.binding;
        let config = self
            .live_config
            .as_ref()
            .ok_or_else(|| super::TcpError::new("live NVMe is not configured"))?;
        let target = LiveConnectTarget {
            nqn: binding.nqn.clone(),
            uuid: binding.uuid,
            nsid: binding.nsid,
            bytes: binding.bytes,
            block_size: binding.block_size,
            address: binding.address.clone(),
            service: binding.service,
            transport: binding.transport,
            geometry: config.queue_geometry,
        };
        #[cfg(any(test, feature = "test-utils"))]
        if self.live_write_local_fixture {
            if let Some(pool) = &self.live_pool {
                pool.cache_local_shared_namespace(
                    &target,
                    &config.host_nqn,
                    config.host_id,
                    &leased.extent.segment.path,
                )
                .map_err(super::TcpError::component)?;
            }
            let namespace = bench_open_local_file(
                &leased.extent.segment.path,
                binding.bytes,
                binding.block_size,
                LiveNamespaceAccess::ReadWrite,
            )
            .map_err(super::TcpError::component)?;
            let io = LiveIo::new(
                namespace,
                config.max_inflight_requests,
                config.max_inflight_bytes,
                false,
            )
            .map_err(super::TcpError::component)?;
            self.live_writer = Some(LiveWriteQueue {
                target,
                io,
                _lease: None,
                failed_accounting: None,
                #[cfg(test)]
                drain_pause: None,
            });
            self.register_leased_worker_wake();
            return Ok(());
        }
        let pool = self
            .live_pool
            .as_ref()
            .ok_or_else(|| super::TcpError::new("live controller pool unavailable"))?;
        let deadline = Instant::now() + Duration::from_millis(config.connect_timeout_ms);
        let (lease, acquisition, _) = pool
            .acquire_shared(
                &target,
                &config.host_nqn,
                config.host_id,
                LiveNamespaceAccess::ReadWrite,
                deadline,
            )
            .map_err(super::TcpError::component)?;
        let (namespace, opened) = lease
            .namespace(
                binding.nsid,
                binding.uuid,
                LiveNamespaceAccess::ReadWrite,
                deadline,
            )
            .map_err(super::TcpError::component)?;
        let io = LiveIo::new(
            namespace,
            config.max_inflight_requests,
            config.max_inflight_bytes,
            config.polled_writes,
        )
        .map_err(super::TcpError::component)?;
        if let Some(counters) = &self.live_counters
            && matches!(
                acquisition,
                crate::distributed::nvmeof::live_pool::LivePoolAcquisition::Connected
            )
        {
            counters.record_namespace_connect();
        }
        if let Some(metrics) = &self.live_metrics {
            // A shared controller serves every member of its subsystem: name
            // the member this queue opened, not the one used to connect.
            let observed = &opened;
            let segment = &leased.extent.segment;
            metrics.record_route(
                crate::distributed::nvmeof::live_metrics::LiveRouteEvidence {
                    transport: observed.transport,
                    address: binding.address.clone(),
                    service: binding.service,
                    host_nqn: config.host_nqn.clone(),
                    host_id: config.host_id,
                    writer: Some(crate::distributed::nvmeof::live_metrics::LiveWriterRoute {
                        epoch: segment.epoch,
                        file_index: segment.file_index,
                        file_identity: Some(
                            crate::distributed::nvmeof::live_target::LiveFileIdentity {
                                path: segment.path.clone(),
                                device: segment.device,
                                inode: segment.inode,
                                bytes: segment.bytes,
                                block_size: segment.block_size,
                            },
                        ),
                    }),
                    namespace_identity:
                        crate::distributed::nvmeof::live_metrics::LiveNamespaceIdentity {
                            namespace_path: observed.namespace_path.clone(),
                            controller_instance: lease.controller_instance(),
                            nqn: observed.nqn.clone(),
                            uuid: observed.uuid,
                            nsid: observed.nsid,
                            bytes: observed.bytes,
                            block_size: observed.block_size,
                            controller: lease.evidence(),
                        },
                },
            );
        }
        self.live_writer = Some(LiveWriteQueue {
            target,
            io,
            _lease: Some(lease),
            failed_accounting: None,
            #[cfg(test)]
            drain_pause: None,
        });
        self.register_leased_worker_wake();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn submit_and_drain_leased_group(
        &mut self,
        epoch: u64,
        batches: Vec<super::CompleteBatch>,
        timeline: &crate::distributed::write_timeline::WriteTimeline,
        owner: u32,
    ) -> Result<Vec<super::DurableBatch>, super::TcpError> {
        let LeasedSubmit::Submitted(id) =
            self.submit_leased_group(epoch, batches, timeline, owner, true)?
        else {
            return Err(super::TcpError::new("test group unexpectedly deferred"));
        };
        let mut released = Vec::new();
        self.drain_leased_flights(&mut released, Some(timeline))?;
        if released.len() != 1 || released[0].0 != id {
            return Err(super::TcpError::new("test group release differs"));
        }
        Ok(released.remove(0).1)
    }

    pub(crate) fn set_worker_wake(&mut self, fd: std::os::fd::RawFd) {
        self.live_leased.worker_wake = Some(fd);
        self.register_leased_worker_wake();
    }

    pub(crate) fn clear_worker_wake(&mut self) {
        self.live_leased.worker_wake = None;
        self.live_leased.worker_wake_registered = false;
    }

    fn register_leased_worker_wake(&mut self) {
        self.live_leased.worker_wake_registered = false;
        if self
            .live_config
            .as_ref()
            .is_some_and(|config| config.polled_writes)
        {
            return;
        }
        if let (Some(fd), Some(writer)) = (self.live_leased.worker_wake, &self.live_writer) {
            if writer.io.register_worker_wake(fd).is_ok() {
                self.live_leased.worker_wake_registered = true;
            } else if let Some(counters) = &self.live_counters {
                counters.record_leased_eventfd_unavailable();
            }
        }
    }

    /// Submit one encoded frame, resuming the write path once if the
    /// submission hits a transient transport break (W1). The retry reuses the
    /// same offset and cursor: the failed frame never advanced either.
    fn submit_leased_frame(
        &mut self,
        offset: u64,
        len: usize,
        queue_empty: bool,
        encode: impl Fn(&mut [u8]) -> Result<(), super::TcpError>,
        trace: Option<(
            &crate::distributed::write_timeline::WriteTimeline,
            crate::distributed::write_timeline::LiveGroupSample,
        )>,
    ) -> Result<(u64, Instant, Instant, bool), super::TcpError> {
        use crate::distributed::nvmeof::live_io::LiveWriteSync;
        self.ensure_leased_healthy()?;
        let mut attempt = 1u32;
        loop {
            // Phase 2 self-fence: the lease is the authority on the right to
            // write, and a resume consumes part of the window the first check
            // ran in, so every attempt re-reads it. One `Acquire` load, and the
            // loop runs once on the unresumed path.
            self.check_lease()?;
            self.ensure_leased_queue()?;
            let config = self
                .live_config
                .as_ref()
                .ok_or_else(|| super::TcpError::new("live config missing"))?;
            let deadline = Instant::now() + std::time::Duration::from_millis(config.io_timeout_ms);
            let id = self
                .live_leased
                .next_flight_id
                .checked_add(1)
                .ok_or_else(|| super::TcpError::new("leased flight ID exhausted"))?;
            let tier = self
                .live_leased
                .current
                .as_ref()
                .ok_or_else(|| super::TcpError::new("leased extent missing"))?
                .lease
                .tier;
            // A window of one frame cannot share a flush; FUA avoids a serial write then flush.
            let durable_at_completion = tier == crate::distributed::durability::DurabilityTier::Native
                || config.leased_sync == LeasedSync::PerFrameFua
                // IOPOLL rings cannot submit Fsync, so polled writes keep per-frame FUA.
                || config.polled_writes
                || self.leased_write_window() == 1
                || len < config.leased_flush_min_frame_bytes
                || (self.live_leased.flights.is_empty() && queue_empty);
            let sync = if tier == crate::distributed::durability::DurabilityTier::Synchronous
                && durable_at_completion
            {
                LiveWriteSync::Data
            } else {
                LiveWriteSync::None
            };
            let write = FrameWrite {
                id,
                offset,
                len,
                sync,
                deadline,
            };
            match self.write_frame(write, &encode, trace) {
                Ok(()) => {
                    let submitted_at = Instant::now();
                    self.live_leased.next_flight_id = id;
                    // This queue owes the frame's CQE, so a later rebind must
                    // wait for the flight deque again.
                    self.live_leased.queue_detached = false;
                    if sync == LiveWriteSync::Data
                        && let Some(counters) = &self.live_counters
                    {
                        counters.record_fua_write();
                    }
                    return Ok((id, deadline, submitted_at, durable_at_completion));
                }
                Err(error) => {
                    if attempt < resume::MAX_RESUME_ATTEMPTS && self.break_kind(&error).is_some() {
                        attempt += 1;
                        // A failed submission lost the queue itself, so this
                        // resume rewrites rather than probing.
                        self.live_leased.probe_unhealed = false;
                        self.resume_after_break(BreakKind::Data)?;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }

    /// One write of one encoded frame into the queue's DMA buffer. The caller
    /// owns the flight bookkeeping; this step only classifies its own errors so
    /// a lost controller stays distinguishable from a rejected command.
    fn write_frame(
        &mut self,
        write: FrameWrite,
        encode: &impl Fn(&mut [u8]) -> Result<(), super::TcpError>,
        trace: Option<(
            &crate::distributed::write_timeline::WriteTimeline,
            crate::distributed::write_timeline::LiveGroupSample,
        )>,
    ) -> Result<(), super::TcpError> {
        use crate::distributed::nvmeof::live_io::{LiveIoError, LiveSubmitOutcome};
        use crate::distributed::nvmeof::live_metrics::LiveWriteFailure;
        let FrameWrite {
            id,
            offset,
            len,
            sync,
            deadline,
        } = write;
        let writer = self
            .live_writer
            .as_mut()
            .ok_or_else(|| super::TcpError::new("live queue missing"))?;
        #[cfg(test)]
        if let Some(result) = self.live_leased.next_write_result.take() {
            writer.io.override_completion_result(id, result);
        }
        #[cfg(test)]
        if let Some(errno) = std::mem::take(&mut self.live_leased.next_submit_result) {
            writer.io.override_submit_result(errno);
        }
        let before = writer.io.stats();
        let mut buffer = match writer.io.write_buffer(len) {
            Ok(buffer) => buffer,
            Err(error) => {
                if let Some(counters) = &self.live_counters {
                    counters.record_leased_write_failure(match error {
                        LiveIoError::Full => LiveWriteFailure::Full,
                        _ => LiveWriteFailure::Failed,
                    });
                }
                // Buffer admission never carries a device errno.
                return Err(resume::classify_io_error(error, None));
            }
        };
        encode(&mut buffer).inspect_err(|_| {
            if let Some(counters) = &self.live_counters {
                counters.record_leased_write_failure(LiveWriteFailure::Failed);
            }
        })?;
        if let Some((timeline, sample)) = trace {
            timeline.live_group_event(
                Some(sample),
                crate::distributed::write_timeline::WriteTimelineStage::LiveFrameEncoded,
            );
        }
        // Consuming the buffer ends its borrow of the queue.
        let outcome = buffer.submit_write_buffer(id, offset, sync, deadline);
        match outcome {
            Ok(LiveSubmitOutcome::Submitted) => {}
            outcome => {
                if let Some(counters) = &self.live_counters {
                    counters.record_leased_write_failure(match &outcome {
                        Ok(LiveSubmitOutcome::Full) | Err(LiveIoError::Full) => {
                            LiveWriteFailure::Full
                        }
                        Err(LiveIoError::Timeout) => LiveWriteFailure::TimedOut,
                        _ => LiveWriteFailure::Failed,
                    });
                }
                let errno = writer.io.last_write_errno();
                return Err(match outcome {
                    Err(error) => resume::classify_io_error(error, errno),
                    _ => super::TcpError::new("live leased write admission full"),
                });
            }
        }
        if writer.failed_accounting.is_none()
            && let Some(counters) = &self.live_counters
        {
            writer.failed_accounting = Some(FailedWriteAccounting {
                before,
                counters: counters.clone(),
            });
        }
        if let Some((timeline, sample)) = trace {
            // Sampled groups pay one extra submit syscall to expose this boundary.
            // Their completion interval can differ from untraced writes.
            if writer.io.submit_pending_write().map_err(|error| {
                if let Some(counters) = &self.live_counters {
                    counters.record_unknown_write();
                    counters.record_leased_write_failure(LiveWriteFailure::Failed);
                }
                super::TcpError::component(error)
            })? {
                timeline.live_group_event(
                    Some(sample),
                    crate::distributed::write_timeline::WriteTimelineStage::LiveIoSubmitted,
                );
            }
        }
        #[cfg(test)]
        self.live_leased.submitted_ids.push(id);
        #[cfg(test)]
        if std::mem::take(&mut self.live_leased.delay_write) {
            let timeout_ms = self
                .live_config
                .as_ref()
                .map_or(0, |config| config.io_timeout_ms);
            std::thread::sleep(std::time::Duration::from_millis(timeout_ms + 1));
        }
        Ok(())
    }

    /// Whether the next frame would exceed the retained-frame budget. The
    /// first frame is always admitted, so a budget smaller than one frame still
    /// makes progress.
    fn retention_full(&self, frame_len: usize) -> bool {
        self.resume_writes
            && !self.live_leased.retained.is_empty()
            && self.live_leased.retained_bytes.saturating_add(frame_len)
                > self.resume_retain_bytes()
    }

    /// Drop the retained frames the owner has adopted. Matched by grant token,
    /// so a partial adoption of a closing extent never drops a current frame.
    fn prune_retained(&mut self, token: [u8; 16], adopted_end: u64) {
        let mut dropped = 0usize;
        while self
            .live_leased
            .retained
            .front()
            .is_some_and(|frame| frame.token == token && frame.end <= adopted_end)
        {
            if let Some(frame) = self.live_leased.retained.pop_front() {
                dropped += frame.frame.len();
            }
        }
        self.live_leased.retained_bytes = self.live_leased.retained_bytes.saturating_sub(dropped);
    }

    /// Retained frame budget: two write windows, hard capped. A test may shrink
    /// it to observe the `Deferred` admission with one window.
    fn resume_retain_bytes(&self) -> usize {
        #[cfg(test)]
        if let Some(bytes) = self.live_leased.resume_retain_bytes {
            return bytes;
        }
        self.live_config.as_ref().map_or(0, |config| {
            config
                .max_inflight_bytes
                .saturating_mul(RESUME_RETAIN_WINDOWS)
                .min(RESUME_RETAIN_CAP)
        })
    }

    pub(crate) fn leased_write_window(&self) -> usize {
        self.live_config.as_ref().map_or(1, |config| {
            config
                .leased_write_window
                .unwrap_or(self.live_leased.flight_bound)
        })
    }

    pub(crate) fn leased_flights_in_flight(&self) -> usize {
        self.live_leased.flights.len()
    }

    pub(crate) fn progress_leased_flights(
        &mut self,
        released: &mut Vec<(u64, Vec<super::DurableBatch>)>,
        timeline: Option<&crate::distributed::write_timeline::WriteTimeline>,
    ) -> Result<(), super::TcpError> {
        let result = self.progress_leased_flights_inner(released, timeline);
        if result.is_err() {
            self.live_write_failed = true;
        }
        result
    }

    /// Harvest completions, resuming once when the queue or a frame reports a
    /// transient break (W1). A resumed pass returns `Ok`, so the wrapper never
    /// poisons the writer for a break the resume healed.
    fn progress_leased_flights_inner(
        &mut self,
        released: &mut Vec<(u64, Vec<super::DurableBatch>)>,
        timeline: Option<&crate::distributed::write_timeline::WriteTimeline>,
    ) -> Result<(), super::TcpError> {
        let mut resumes = 0u32;
        loop {
            match self.progress_leased_flights_once(released, timeline) {
                Err(error) => {
                    let Some(kind) = self.break_kind(&error) else {
                        return Err(error);
                    };
                    if resumes >= resume::MAX_RESUME_ATTEMPTS {
                        return Err(error);
                    }
                    resumes += 1;
                    // A kept queue is a probe of the owner's health: if its
                    // window passes without a CQE the next expiry reports a
                    // data break and only the rewrite remains (design 7.3).
                    self.live_leased.probe_unhealed = kind == BreakKind::Session;
                    self.resume_after_break(kind)?;
                }
                other => return other,
            }
        }
    }

    fn progress_leased_flights_once(
        &mut self,
        released: &mut Vec<(u64, Vec<super::DurableBatch>)>,
        timeline: Option<&crate::distributed::write_timeline::WriteTimeline>,
    ) -> Result<(), super::TcpError> {
        use crate::distributed::nvmeof::live_io::LiveIoResult;
        use crate::distributed::nvmeof::live_metrics::LiveWriteFailure;
        use crate::distributed::write_timeline::WriteTimelineStage as Stage;
        self.ensure_leased_healthy()?;
        if self.live_leased.flights.is_empty() {
            return Ok(());
        }
        if self
            .live_leased
            .flights
            .iter()
            .find(|flight| !flight.completed)
            .is_some_and(|flight| Instant::now() >= flight.deadline)
        {
            if let Some(counters) = &self.live_counters {
                counters.record_leased_write_failure(LiveWriteFailure::TimedOut);
                counters.record_unknown_write();
            }
            // A deadline is a probe, not a verdict: the owner may be overloaded,
            // so the first one keeps the queue and grants one more window. An
            // expiry while `probe_unhealed` is set means that window passed
            // without a single CQE, so the queue does not progress and only a
            // detach-and-rewrite can tell a dead link from an overloaded owner
            // (design 7.3). The data class makes the resume do exactly that.
            return Err(if self.live_leased.probe_unhealed {
                super::TcpError::with_kind(
                    std::io::ErrorKind::ConnectionAborted,
                    "leased write deadline expired after a probe completed no frame",
                )
            } else {
                super::TcpError::with_kind(
                    std::io::ErrorKind::TimedOut,
                    "leased write deadline expired",
                )
            });
        }
        let writer = self
            .live_writer
            .as_mut()
            .ok_or_else(|| super::TcpError::new("live queue missing"))?;
        let before = writer
            .failed_accounting
            .as_ref()
            .map_or_else(|| writer.io.stats(), |accounting| accounting.before.clone());
        self.live_leased.completions.clear();
        let progress = writer.io.progress_into(&mut self.live_leased.completions);
        if let Err(error) = progress {
            if let Some(counters) = &self.live_counters {
                counters.record_leased_write_failure(LiveWriteFailure::Failed);
                counters.record_unknown_write();
            }
            let errno = writer.io.last_write_errno();
            return Err(resume::classify_io_error(error, errno));
        }
        let after = writer.io.stats();
        // The latest completed write's errno, read once under the queue borrow:
        // a lost controller is transient, a rejected or short write is not.
        let write_errno = writer.io.last_write_errno();
        let harvested = !self.live_leased.completions.is_empty();
        if harvested {
            // A CQE proves the kept queue still progresses, so a deadline that
            // expires later is a fresh probe, not an escalation (design 7.3).
            self.live_leased.probe_unhealed = false;
        }
        for completion in self.live_leased.completions.drain(..) {
            if let Some((id, covers_seq, _)) = self.live_leased.flush_in_flight
                && completion.id == id
            {
                if !matches!(completion.result, Ok(LiveIoResult::Flushed)) {
                    let timed_out = after.timeouts > before.timeouts;
                    if let Some(counters) = &self.live_counters {
                        counters.record_leased_write_failure(if timed_out {
                            LiveWriteFailure::TimedOut
                        } else {
                            LiveWriteFailure::Failed
                        });
                        counters.record_unknown_write();
                    }
                    // A flush CQE reports `UnknownWrite` both for a late
                    // deadline and for a device error, so the errno decides:
                    // `EIO`/`ENOSPC`/`EFBIG`/`EINVAL` are the filesystem's
                    // verdict and a retry is never issued, while a lost
                    // controller is the same resumable break as a write. Only
                    // a deadline that recorded no error is the probe of 7.3.
                    return Err(match write_errno {
                        Some(errno) => resume::classify_io_error(
                            crate::distributed::nvmeof::live_io::LiveIoError::UnknownWrite,
                            Some(errno),
                        ),
                        None if timed_out => super::TcpError::with_kind(
                            std::io::ErrorKind::TimedOut,
                            "leased namespace flush deadline expired",
                        ),
                        None => super::TcpError::new(
                            "leased namespace flush failed: the outcome is unknown",
                        ),
                    });
                }
                if let Some(started) = self.live_leased.flush_submitted_at.take()
                    && let Some(counters) = &self.live_counters
                {
                    counters.record_leased_flush_completion(
                        completion
                            .completed_at
                            .saturating_duration_since(started)
                            .as_nanos()
                            .min(u64::MAX as u128) as u64,
                    );
                }
                self.live_leased.flushed_through_seq = covers_seq;
                self.live_leased.flush_in_flight = None;
                #[cfg(test)]
                crate::failpoints::write_model::boundary("leased_flush_completed");
                continue;
            }
            let Some(flight) = self
                .live_leased
                .flights
                .iter_mut()
                .find(|flight| flight.id == completion.id && !flight.completed)
            else {
                if let Some(counters) = &self.live_counters {
                    counters.record_leased_write_failure(LiveWriteFailure::Failed);
                    counters.record_unknown_write();
                }
                // The queue lost this frame's completion: a lost controller is
                // transient, a rejected or short write is not.
                return Err(resume::classify_io_error(
                    crate::distributed::nvmeof::live_io::LiveIoError::UnknownWrite,
                    write_errno,
                ));
            };
            if let Some(timeline) = timeline {
                timeline.live_group_event(flight.sample, Stage::LiveIoCompleted);
            }
            if !matches!(completion.result, Ok(LiveIoResult::Written)) {
                if let Some(counters) = &self.live_counters {
                    let failure = if after.timeouts > before.timeouts {
                        LiveWriteFailure::TimedOut
                    } else if after.short_write_ops > before.short_write_ops {
                        LiveWriteFailure::Short
                    } else {
                        LiveWriteFailure::Failed
                    };
                    counters.record_leased_write_failure(failure);
                    counters.record_unknown_write();
                }
                return Err(resume::classify_io_error(
                    crate::distributed::nvmeof::live_io::LiveIoError::UnknownWrite,
                    write_errno,
                ));
            }
            if let Some(counters) = &self.live_counters {
                counters.record_timing(
                    LiveTimingStage::LeasedWrite,
                    completion
                        .completed_at
                        .saturating_duration_since(flight.submitted_at)
                        .as_nanos()
                        .min(u64::MAX as u128) as u64,
                    1,
                );
            }
            flight.completed = true;
            #[cfg(test)]
            crate::failpoints::write_model::boundary("leased_frame_completed");
        }
        if self
            .live_leased
            .flights
            .iter()
            .find(|flight| !flight.completed)
            .is_some_and(|flight| Instant::now() >= flight.deadline)
        {
            if let Some(counters) = &self.live_counters {
                counters.record_leased_write_failure(LiveWriteFailure::TimedOut);
                counters.record_unknown_write();
            }
            return Err(super::TcpError::with_kind(
                std::io::ErrorKind::TimedOut,
                "leased write deadline expired",
            ));
        }
        if harvested {
            if let Some(counters) = &self.live_counters {
                record_write_io(counters, &before, &after);
            }
            if let Some(accounting) = &mut writer.failed_accounting {
                accounting.before = after;
            }
        }
        if self
            .live_leased
            .flush_in_flight
            .is_some_and(|(_, _, deadline)| Instant::now() >= deadline)
        {
            if let Some(counters) = &self.live_counters {
                counters.record_leased_write_failure(LiveWriteFailure::TimedOut);
                counters.record_unknown_write();
            }
            return Err(super::TcpError::with_kind(
                std::io::ErrorKind::TimedOut,
                "leased namespace flush deadline expired",
            ));
        }
        if self.live_leased.flush_in_flight.is_none()
            && self
                .live_config
                .as_ref()
                .is_some_and(|config| config.leased_sync == LeasedSync::GroupFlush)
        {
            // Capture only the contiguous completed prefix. A completion after
            // this capture is never covered, even if the physical flush sees it.
            let mut covers_seq = self.live_leased.flushed_through_seq;
            let mut covered_frames = 0;
            for flight in self
                .live_leased
                .flights
                .iter()
                .take_while(|flight| flight.completed)
            {
                covers_seq = covers_seq.max(flight.id);
                if !flight.durable_at_completion && flight.id > self.live_leased.flushed_through_seq
                {
                    covered_frames += 1;
                }
            }
            if covered_frames != 0 {
                let timeout_ms = self
                    .live_config
                    .as_ref()
                    .ok_or_else(|| super::TcpError::new("live config missing"))?
                    .io_timeout_ms;
                let submitted_at = Instant::now();
                let deadline = submitted_at + std::time::Duration::from_millis(timeout_ms);
                let id = self
                    .live_leased
                    .next_flight_id
                    .checked_add(1)
                    .ok_or_else(|| super::TcpError::new("leased flight ID exhausted"))?;
                let submit = writer.io.submit_flush(id, deadline);
                let id = match submit {
                    Ok(id) => id,
                    Err(error) => {
                        if let Some(counters) = &self.live_counters {
                            counters.record_leased_write_failure(LiveWriteFailure::Failed);
                            counters.record_unknown_write();
                        }
                        let errno = writer.io.last_write_errno();
                        return Err(resume::classify_io_error(error, errno));
                    }
                };
                self.live_leased.next_flight_id = id;
                #[cfg(test)]
                if let Some(result) = self.live_leased.next_flush_result.take() {
                    writer.io.override_completion_result(id, result);
                }
                self.live_leased.flush_in_flight = Some((id, covers_seq, deadline));
                self.live_leased.flush_submitted_at = Some(submitted_at);
                if let Some(counters) = &self.live_counters {
                    counters.record_leased_flush(covered_frames);
                }
            }
        }
        while self.live_leased.flights.front().is_some_and(|flight| {
            flight.completed
                && (flight.durable_at_completion
                    || flight.id <= self.live_leased.flushed_through_seq)
        }) {
            let Some(flight) = self.live_leased.flights.pop_front() else {
                break;
            };
            let current = self
                .live_leased
                .current
                .as_mut()
                .ok_or_else(|| super::TcpError::new("lease missing"))?;
            debug_assert_eq!(current.written_end, flight.start);
            current.written_end = flight.end;
            current.acknowledged = flight.prefix;
            if flight.frame_len != 0 {
                if let Some(counters) = &self.live_counters {
                    counters
                        .record_leased_frame(flight.frame_len as u64, flight.receipts.len() as u64);
                }
                if let Some(timeline) = timeline {
                    timeline.live_group_event(flight.sample, Stage::LiveReceiptsValidated);
                }
                released.push((flight.id, flight.receipts));
                #[cfg(test)]
                crate::failpoints::write_model::boundary("leased_receipt_released");
            }
        }
        if self.live_leased.flights.is_empty() {
            writer.failed_accounting = None;
        }
        Ok(())
    }

    pub(crate) fn drain_leased_flights(
        &mut self,
        released: &mut Vec<(u64, Vec<super::DurableBatch>)>,
        timeline: Option<&crate::distributed::write_timeline::WriteTimeline>,
    ) -> Result<(), super::TcpError> {
        let result = (|| {
            while !self.live_leased.flights.is_empty() {
                match self.wait_leased_flights(false) {
                    Ok(()) => {}
                    Err(error) => match self.break_kind(&error) {
                        // A lost queue cannot deliver the CQE the wait asks
                        // for; the resume rewrites the unadopted suffix.
                        Some(kind) => {
                            // A kept queue is a probe; its window decides
                            // whether the next break rewrites (design 7.3).
                            self.live_leased.probe_unhealed = kind == BreakKind::Session;
                            self.resume_after_break(kind)?;
                        }
                        None => return Err(error),
                    },
                }
                self.progress_leased_flights(released, timeline)?;
            }
            Ok(())
        })();
        if result.is_err() {
            self.live_write_failed = true;
        }
        result
    }

    pub(crate) fn wait_leased_flights(&mut self, worker_wake: bool) -> Result<(), super::TcpError> {
        let deadline = self
            .live_leased
            .flights
            .iter()
            .find(|flight| !flight.completed)
            .map(|flight| flight.deadline)
            .or_else(|| {
                self.live_leased
                    .flush_in_flight
                    .map(|(_, _, deadline)| deadline)
            });
        let Some(deadline) = deadline else {
            return Ok(());
        };
        let remaining = deadline
            .saturating_duration_since(Instant::now())
            .min(std::time::Duration::from_millis(1));
        let polled = self
            .live_config
            .as_ref()
            .is_some_and(|config| config.polled_writes);
        if worker_wake
            && self.live_leased.worker_wake_registered
            && !polled
            && let Some(fd) = self.live_leased.worker_wake
        {
            let mut descriptor = libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: descriptor points to one valid pollfd; poll does not retain it.
            let result = unsafe { libc::poll(&mut descriptor, 1, i32::from(!remaining.is_zero())) };
            if result < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::Interrupted {
                    return Err(super::TcpError::component(error));
                }
            }
            if let Some(counters) = &self.live_counters {
                counters.record_leased_worker_wait(result == 0);
            }
            let mut value = 0u64;
            // SAFETY: value is writable for eight bytes and fd stays owned by the worker.
            let result = unsafe { libc::read(fd, (&mut value as *mut u64).cast(), 8) };
            if result < 0 {
                let error = std::io::Error::last_os_error();
                if !matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) {
                    return Err(super::TcpError::component(error));
                }
            }
            return Ok(());
        }
        let io = &mut self
            .live_writer
            .as_mut()
            .ok_or_else(|| super::TcpError::new("live queue missing"))?
            .io;
        let waited = io.wait(if polled {
            std::time::Duration::ZERO
        } else {
            remaining
        });
        let completed = match waited {
            Ok(completed) => completed,
            Err(error) => {
                // A wait on a lost queue is a transient break; keep its class so
                // the caller can resume instead of failing the database.
                let errno = io.last_write_errno();
                return Err(resume::classify_io_error(error, errno));
            }
        };
        if let Some(counters) = &self.live_counters {
            counters.record_leased_worker_wait(!polled && !completed);
        }
        Ok(())
    }

    pub(crate) fn write_leased_skip(&mut self) -> Result<(), super::TcpError> {
        debug_assert!(self.live_leased.flights.is_empty());
        if !self.live_leased.flights.is_empty() {
            return Err(super::TcpError::new(
                "leased queue rebind with writes in flight",
            ));
        }
        let Some(current) = &self.live_leased.current else {
            return Ok(());
        };
        if current.cursor == current.lease.extent.end {
            return Ok(());
        }
        let cursor = current.cursor;
        let prefix = current.acknowledged;
        let offset = cursor % current.lease.extent.segment.bytes;
        let (id, deadline, submitted_at, durable_at_completion) = self.submit_leased_frame(
            offset,
            4096,
            true,
            |block| {
                block.fill(0);
                block[..8].copy_from_slice(crate::crc::CrcFrame::skip_marker().as_ref());
                Ok(())
            },
            None,
        )?;
        self.live_leased.flights.push_back(LeasedFlight {
            id,
            start: cursor,
            end: cursor,
            prefix,
            receipts: Vec::new(),
            deadline,
            submitted_at,
            completed: false,
            durable_at_completion,
            sample: None,
            frame_len: 0,
        });
        self.drain_leased_flights(&mut Vec::new(), None)?;
        if let Some(counters) = &self.live_counters {
            counters.record_leased_skip();
        }
        Ok(())
    }

    pub(crate) fn submit_leased_group(
        &mut self,
        epoch: u64,
        batches: Vec<super::CompleteBatch>,
        timeline: &crate::distributed::write_timeline::WriteTimeline,
        owner: u32,
        queue_empty: bool,
    ) -> Result<LeasedSubmit, super::TcpError> {
        let result = self.submit_leased_group_inner(epoch, batches, timeline, owner, queue_empty);
        if result.is_err() {
            self.live_write_failed = true;
        }
        result
    }

    fn submit_leased_group_inner(
        &mut self,
        epoch: u64,
        batches: Vec<super::CompleteBatch>,
        timeline: &crate::distributed::write_timeline::WriteTimeline,
        owner: u32,
        queue_empty: bool,
    ) -> Result<LeasedSubmit, super::TcpError> {
        use crate::distributed::group::LiveFrame;
        use crate::distributed::nvmeof::live_storage::extend_live_receipt_digest;
        use crate::distributed::write_timeline::WriteTimelineStage as Stage;
        self.ensure_leased_healthy()?;
        if self.live_writer_closing || !self.live_nvme_configured() {
            return Err(super::TcpError::new(
                "leased writer admission is closed or mode differs",
            ));
        }
        if batches.iter().any(|batch| batch.version().epoch != epoch) {
            return Err(super::TcpError::new(
                "leased batch epoch differs from stream",
            ));
        }
        let build_started = timeline.enabled().then(Instant::now);
        // Keep ownership of immutable members, but encode only in the queue buffer.
        let group = LiveFrame::new(
            &self.shape,
            batches,
            self.expected.batch_limits,
            self.expected.group_limits,
        )
        .map_err(super::TcpError::component)?;
        let frame_len = group.len();
        let build_completed = build_started.map(|_| Instant::now());
        let config = self
            .live_config
            .as_ref()
            .ok_or_else(|| super::TcpError::new("live configuration missing"))?;
        if frame_len
            .checked_add(4095)
            .is_none_or(|n| n > config.max_inflight_bytes)
            || frame_len as u64 > self.expected.fragment_bytes
            || frame_len > 64 * 1024 * 1024
        {
            return Err(super::TcpError::new(
                "leased frame exceeds queue, fragment or live frame budget",
            ));
        }
        if self.live_leased.flights.len() == self.leased_write_window()
            || self.retention_full(frame_len)
            || (!self.live_leased.flights.is_empty()
                && (self
                    .live_writer
                    .as_ref()
                    .is_some_and(|writer| !writer.io.has_credit(frame_len))
                    || self.live_leased.current.as_ref().is_some_and(|current| {
                        frame_len as u64 > current.lease.extent.end - current.cursor
                            || current.prefixes.len() == 1024
                    })))
        {
            return Ok(LeasedSubmit::Deferred(group.into_batches()));
        }
        // Deferred attempts do not consume timeline membership or group capacity.
        let sample =
            timeline.live_group(owner, group.batches().iter().map(|batch| batch.version()));
        if let (Some(started), Some(completed)) = (build_started, build_completed) {
            timeline.live_group_event_at(sample, Stage::LiveGroupBuildStarted, started);
            timeline.live_group_event_at(sample, Stage::LiveGroupBuilt, completed);
        }
        if self
            .live_leased
            .current
            .as_ref()
            .is_some_and(|current| current.prefixes.len() == 1024)
        {
            self.settle_leased_extent(&mut Vec::new(), Some(timeline))?;
            if let Some(counters) = &self.live_counters {
                counters.record_adoption_history_full_settle();
            }
        }
        if let Some(current) = &self.live_leased.current
            && current.lease.extent.segment.epoch != epoch
        {
            return Err(super::TcpError::new("leased extent epoch differs"));
        }
        if self.live_leased.current.is_none() {
            if let Some(counters) = &self.live_counters {
                counters.record_metadata_request();
                counters.record_lease_exchange();
            }
            let leased =
                self.leased_step(BreakKind::Session, |client| client.lease_live_extent(epoch))?;
            self.leased_step(BreakKind::Session, |client| {
                client.install_leased_extent(leased.clone())
            })?;
            if let Some(counters) = &self.live_counters {
                counters.record_extent_lease();
            }
        } else if self
            .live_leased
            .current
            .as_ref()
            .is_some_and(|current| frame_len as u64 > current.lease.extent.end - current.cursor)
        {
            timeline.live_group_event(sample, Stage::LiveExtentSwitchStarted);
            let started = std::time::Instant::now();
            if self.lease_ahead() {
                if self.live_leased.closing.is_some() || self.live_leased.next.is_none() {
                    if let Some(counters) = &self.live_counters {
                        counters.record_extent_switch_wait();
                    }
                    self.leased_step(BreakKind::Session, |client| {
                        client.wait_closing_extent(true)
                    })?;
                }
                self.leased_step(BreakKind::Session, |client| client.write_leased_skip())?;
                let next = self
                    .live_leased
                    .next
                    .take()
                    .ok_or_else(|| super::TcpError::new("preleased extent missing"))?;
                let current = self
                    .live_leased
                    .current
                    .take()
                    .ok_or_else(|| super::TcpError::new("lease missing"))?;
                if next.extent.start != current.lease.extent.end {
                    return Err(super::TcpError::new("next leased extent is not contiguous"));
                }
                self.live_leased.closing = Some(current);
                if let Some(pending) = self.live_leased.pending.as_mut() {
                    // An adoption already in flight still belongs to the old
                    // extent; its proof must never touch the new prefix ring.
                    if pending.target == PendingTarget::Current {
                        pending.target = PendingTarget::Closing;
                    }
                }
                self.live_leased.install(next, &self.expected);
                if let Some(counters) = &self.live_counters {
                    counters.record_extent_lease();
                }
            } else {
                self.leased_step(BreakKind::Session, |client| client.write_leased_skip())?;
                self.leased_step(BreakKind::Session, |client| {
                    client.adopt_leased_sync(true, true)
                })?;
            }
            if let Some(counters) = &self.live_counters {
                counters.record_timing(
                    crate::distributed::nvmeof::live_metrics::LiveTimingStage::ExtentSwitch,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    1,
                );
            }
            timeline.live_group_event(sample, Stage::LiveExtentSwitchCompleted);
            #[cfg(test)]
            crate::failpoints::write_model::boundary("leased_extent_switched");
        }
        let current = self
            .live_leased
            .current
            .as_ref()
            .ok_or_else(|| super::TcpError::new("lease missing"))?;
        if frame_len as u64 > current.lease.extent.end - current.cursor {
            return Err(super::TcpError::new("frame does not fit granted extent"));
        }
        timeline.live_group_token(sample, current.lease.extent.token);
        let token = current.lease.extent.token;
        let cursor = current.cursor;
        let offset = cursor % current.lease.extent.segment.bytes;
        let end = cursor
            .checked_add(frame_len as u64)
            .ok_or_else(|| super::TcpError::new("leased write offset overflow"))?;
        let position = crate::WalPosition::new(cursor, frame_len as u32);
        let mut prefix = current.reserved;
        prefix.end = end;
        let mut receipts = Vec::with_capacity(group.batches().len());
        let receipt_config = self
            .live_leased
            .receipt_config
            .as_ref()
            .ok_or_else(|| super::TcpError::new("leased receipt configuration missing"))?;
        for (batch, member_offset) in group.batches().iter().zip(group.batch_offsets()) {
            let position =
                crate::distributed::storage::member_position(position, member_offset, batch);
            let appended =
                crate::distributed::storage::receipt_for(receipt_config, batch, position);
            let receipt = self.check_receipt(
                batch,
                super::Receipt {
                    id: appended.id(),
                    version: appended.version(),
                    digest: appended.digest(),
                    address: appended.address(),
                },
            )?;
            prefix.count = prefix
                .count
                .checked_add(1)
                .ok_or_else(|| super::TcpError::new("leased receipt count overflow"))?;
            prefix.digest = extend_live_receipt_digest(prefix.digest, appended);
            receipts.push(receipt);
        }
        #[cfg(test)]
        if std::mem::take(&mut self.live_leased.corrupt_next_digest) {
            prefix.digest[0] ^= 1;
        }
        // Record before submission: owner self-adoption may observe a complete
        // frame before the initiator has harvested its CQE.
        self.live_leased
            .current
            .as_mut()
            .ok_or_else(|| super::TcpError::new("lease missing"))?
            .prefixes
            .push_back(prefix);
        timeline.live_group_event(sample, Stage::LiveIoStarted);
        let (id, deadline, submitted_at, durable_at_completion) = self.submit_leased_frame(
            offset,
            frame_len,
            queue_empty,
            |buffer| {
                group
                    .encode_into(buffer)
                    .map_err(super::TcpError::component)
            },
            sample.map(|sample| (timeline, sample)),
        )?;
        let current = self
            .live_leased
            .current
            .as_mut()
            .ok_or_else(|| super::TcpError::new("lease missing"))?;
        timeline.owner_frame(
            owner,
            group.batches().len(),
            || {
                group
                    .batches()
                    .iter()
                    .map(|batch| {
                        batch
                            .mutations()
                            .iter()
                            .filter(|mutation| {
                                matches!(mutation, crate::distributed::codec::Mutation::Put { .. })
                            })
                            .count()
                    })
                    .sum()
            },
            durable_at_completion
                && current.lease.tier
                    == crate::distributed::durability::DurabilityTier::Synchronous,
        );
        timeline.live_group_position(
            sample,
            cursor,
            offset,
            frame_len as u32,
            cursor / current.lease.extent.segment.bytes,
            self.live_writer
                .as_ref()
                .ok_or_else(|| super::TcpError::new("live queue missing"))?
                .io
                .namespace()
                .block_size,
        );
        current.cursor = end;
        current.reserved = prefix;
        // Publish the owner allocations only for an accepted frame, and only
        // when the hybrid index writer has enabled recording: a failed
        // submission poisons this open, and these addresses would then name
        // bytes that were never written.
        if let Some(queue) = self.live_leased.reservations.as_mut() {
            queue.extend(receipts.iter().map(|receipt| {
                let appended = receipt.appended();
                (appended.version(), appended.address())
            }));
        }
        self.live_leased.flights.push_back(LeasedFlight {
            id,
            start: cursor,
            end,
            prefix,
            receipts,
            deadline,
            submitted_at,
            completed: false,
            durable_at_completion,
            sample,
            frame_len,
        });
        if let Some(counters) = &self.live_counters {
            counters.record_max_leased_in_flight(self.live_leased.flights.len() as u64);
        }
        // Retain the group, not a copy of its bytes: `encode_into` is
        // deterministic, so a resume regenerates the identical frame, and the
        // owner's receipt digest proves it. Bounded admission is what keeps
        // this deque from growing past `resume_retain_bytes`.
        // Only a writer whose database enabled the resume pays for retention.
        if self.resume_writes {
            self.live_leased.retained_bytes =
                self.live_leased.retained_bytes.saturating_add(frame_len);
            self.live_leased.retained.push_back(RetainedFrame {
                token,
                start: cursor,
                end,
                durable_at_completion,
                frame: group,
            });
        }
        if std::mem::take(&mut self.live_leased.queue_rebound) {
            timeline.live_group_event(sample, Stage::LiveQueueRebound);
        }
        #[cfg(test)]
        if std::mem::take(&mut self.live_leased.invalidate_adoption_token)
            && let Some(current) = &mut self.live_leased.current
        {
            current.lease.extent.token[0] ^= 1;
        }
        Ok(LeasedSubmit::Submitted(id))
    }
}

#[cfg(test)]
mod resume_tests;
#[cfg(test)]
pub(crate) mod leased_tests {
    use super::*;
    use crate::distributed::durability::{DurabilityConfig, DurabilityTier};
    use crate::distributed::nvmeof::{
        live_metrics::LiveNvmeMetrics, live_target::LiveTargetManager,
    };
    use crate::distributed::tcp::{self, Client, NodeConfig, StorageNode, TlsFiles, Transport};
    use crate::distributed::test_bounds::PROGRESS_CAP;
    use crate::distributed::write_timeline::WriteTimeline;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::thread;
    use std::time::{Duration, Instant};

    pub(crate) struct Fixture {
        pub(crate) config: NodeConfig,
        pub(crate) node: Arc<StorageNode>,
        pub(crate) metrics: Arc<LiveNvmeMetrics>,
        pub(crate) endpoint: String,
        pub(crate) tls: TlsFiles,
        client_timeout: Duration,
        stop: Arc<AtomicBool>,
        server: Option<thread::JoinHandle<()>>,
        _temp: tempdir::TempDir,
    }

    impl Fixture {
        pub(crate) fn new(tier: DurabilityTier, fragment: u64) -> Self {
            Self::new_with_partitions(tier, fragment, vec![2, 5])
        }

        pub(crate) fn single_partition(tier: DurabilityTier, fragment: u64) -> Self {
            Self::new_with_partitions(tier, fragment, vec![2])
        }
        pub(crate) fn new_with_partitions(t: DurabilityTier, f: u64, parts: Vec<u32>) -> Self {
            let temp = tempdir::TempDir::new("leased-coordinator").unwrap();
            let mut config = tcp::tests::config(&temp.path().join("node"));
            config.partition.partition = parts[0];
            config.partition.inventory.partitions = parts;
            config.partition.fragment_bytes = f;
            config.partition.wal_file_bytes = f * 2;
            config.partition.batch_limits.max_encoded_bytes = 8192;
            config.partition.group_limits.max_encoded_bytes = 16376.min(f as usize - 8);
            config.partition.max_batches = 4096;
            config.partition.max_reference_metadata_bytes = 64 * 1024 * 1024;
            Self::open(t, config, temp, Duration::from_secs(10))
        }

        pub(crate) fn native_geometry() -> Self {
            let temp = tempdir::TempDir::new("leased-native-profile").unwrap();
            let mut config = tcp::tests::config(&temp.path().join("node"));
            config.partition.fragment_bytes = 128 * 1024 * 1024;
            config.partition.wal_file_bytes = 10 * 1024 * 1024 * 1024;
            config.partition.batch_limits.max_encoded_bytes = 64 * 1024 + 1024;
            config.partition.group_limits.max_batches = 16;
            config.partition.group_limits.max_encoded_bytes = 2 * 1024 * 1024;
            config.partition.max_batches = 4096;
            config.partition.max_reference_metadata_bytes = 64 * 1024 * 1024;
            // Debug validation of a full 128 MiB backlog can exceed ten seconds.
            Self::open(
                DurabilityTier::Native,
                config,
                temp,
                Duration::from_secs(60),
            )
        }

        fn open(
            tier: DurabilityTier,
            config: NodeConfig,
            temp: tempdir::TempDir,
            client_timeout: Duration,
        ) -> Self {
            let node = StorageNode::open(config.clone(), tcp::tests::metrics()).unwrap();
            node.set_durability(DurabilityConfig::with_tier(tier))
                .unwrap();
            let state_dir = temp.path().join("target");
            let configfs = temp.path().join("configfs");
            std::fs::create_dir(&state_dir).unwrap();
            std::fs::create_dir(&configfs).unwrap();
            for name in ["hosts", "ports", "subsystems"] {
                std::fs::create_dir(configfs.join(name)).unwrap();
            }
            let target = LiveTargetManager::open_fixture(
                super::super::tests::live_fixture_target_config(&config.partition, state_dir),
                configfs,
            )
            .unwrap();
            node.install_live_target(target).unwrap();
            let tls = TlsFiles {
                transport: Transport::TcpTrusted,
                ca: PathBuf::new(),
                cert: PathBuf::new(),
                key: PathBuf::new(),
                server_name: "trusted-test-cluster".into(),
            };
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = listener.local_addr().unwrap().to_string();
            let stop = Arc::new(AtomicBool::new(false));
            let server_node = node.clone();
            let server_stop = stop.clone();
            let server_tls = tls.clone();
            let limits = tcp::TcpLimits {
                io_timeout: client_timeout,
                ..Default::default()
            };
            let server = thread::spawn(move || {
                server_node
                    .serve(listener, server_tls, limits, server_stop)
                    .unwrap()
            });
            Self {
                _temp: temp,
                config,
                node,
                metrics: LiveNvmeMetrics::new(),
                endpoint,
                tls,
                client_timeout,
                stop,
                server: Some(server),
            }
        }

        pub(crate) fn client(&self) -> Client {
            let shape = crate::key_shape::KeyShape::from_yaml(&self.config.shape_yaml).unwrap();
            Client::connect(
                &self.endpoint,
                &self.tls,
                self.client_timeout,
                self.config.partition.clone(),
                &shape,
            )
            .unwrap()
        }

        pub(crate) fn writer(&self) -> Client {
            let mut client = self.client();
            client.fence_to(1).unwrap();
            self.configure_writer(&mut client, self.node.durability().tier);
            client
        }

        pub(crate) fn writer_without_ahead(&self) -> Client {
            let mut client = self.writer();
            let mut config = client.live_config.clone().unwrap();
            config.lease_ahead = false;
            client
                .configure_live_nvme(
                    &config,
                    crate::distributed::remote_db::RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY,
                )
                .unwrap();
            client
        }

        fn configure_writer(&self, client: &mut Client, tier: DurabilityTier) {
            client
                .configure_live_nvme(
                    &LiveClientConfig {
                        lease_ahead: true,
                        polled_writes: false,
                        host_nqn: "nqn.2026-01.test:leased".into(),
                        host_id: [1; 16],
                        owner_identities: Default::default(),
                        leased_write_window: Some(4),
                        leased_sync: LeasedSync::PerFrameFua,
                        leased_flush_min_frame_bytes: 0,
                        max_inflight_requests: 4,
                        max_inflight_bytes: (self.config.partition.group_limits.max_encoded_bytes
                            + 4096)
                            .max(128 * 1024),
                        connect_timeout_ms: 1000,
                        io_timeout_ms: 1000,
                        leased_durability: tier,
                        inline_reads: true,
                        queue_geometry: Default::default(),
                    },
                    crate::distributed::remote_db::RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY,
                )
                .unwrap();
            client.live_write_local_fixture = true;
            client.set_live_metrics(self.metrics.clone()).unwrap();
        }

        pub(crate) fn batch(&self, sequence: u64) -> tcp::CompleteBatch {
            tcp::tests::batch(&self.config, 1, sequence)
        }

        fn big_batch(&self, sequence: u64) -> tcp::CompleteBatch {
            let original = self.batch(sequence);
            crate::distributed::codec::CompleteBatch::new(
                &crate::key_shape::KeyShape::from_yaml(&self.config.shape_yaml).unwrap(),
                original.id(),
                original.version(),
                vec![crate::distributed::codec::Mutation::Put {
                    keyspace: crate::key_shape::KeySpace(0),
                    key: b"key1"[..].into(),
                    value: vec![sequence as u8; 5000].into(),
                }],
                self.config.partition.batch_limits,
            )
            .unwrap()
        }
    }

    /// Owner-side read of `reference` at the owner's serving boundary, the
    /// oracle that replaced the removed owner-RPC `Client::read_records` (now a
    /// live NVMe-oF read).
    ///
    /// A leased receipt can name bytes the owner adopted whose frontier
    /// publication still lags the acknowledgement, so the production read
    /// boundary persists them before it serves the record
    /// (`load_serving_record_authority` → `persist_for_read`). This oracle takes
    /// the same step instead of asserting the read-side frontier directly.
    pub(crate) fn owner_read(
        fixture: &Fixture,
        reference: &crate::distributed::types::ValueRecordReference,
    ) -> Vec<crate::distributed::codec::Mutation> {
        use crate::distributed::storage::StorageError;
        let log = fixture.node.log_for_test(1);
        if let Err(error) = log.validate_durable_record_reference(reference) {
            assert!(
                matches!(error, StorageError::NeedsPersistence),
                "the owner rejects record {}: {error}",
                reference.batch_address.offset
            );
            // The read's own covering barrier, the same pair a durable-progress
            // nudge runs on the owner: adopt the leased frames, then publish
            // everything appended. Both run off the syncer thread.
            if let Err(error) = log.self_adopt_live_extent() {
                panic!(
                    "the owner cannot adopt record {} before serving it: {error}",
                    reference.batch_address.offset
                );
            }
            if let Err(error) = log.persist_live_progress() {
                panic!(
                    "the owner cannot persist record {} before serving it: {error}",
                    reference.batch_address.offset
                );
            }
        }
        if let Err(error) = log.validate_durable_record_reference(reference) {
            panic!(
                "the owner still cannot serve record {} after persisting it: {error}",
                reference.batch_address.offset
            );
        }
        vec![
            log.read_record(reference)
                .expect("the owner reads its record"),
        ]
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // Release wakes the server's shutdown checks; no stream state travels.
            self.stop.store(true, Ordering::Release);
            if let Some(server) = self.server.take() {
                server.join().unwrap();
            }
        }
    }

    #[test]
    fn configured_client_installs_its_partition_identity() {
        let f = Fixture::single_partition(DurabilityTier::Native, 16384);
        let mut client = f.writer();
        let mut config = client.live_config.clone().unwrap();
        let identity = super::LiveInitiatorIdentity {
            host_nqn: "nqn.2026-01.test:owner-specific".into(),
            host_id: [2; 16],
        };
        config
            .owner_identities
            .insert(client.expected.partition, identity.clone());
        let json = serde_json::to_value(&config).unwrap();
        let key = client.expected.partition.to_string();
        assert_eq!(
            json["owner_identities"][&key]["host_id"],
            serde_json::json!(vec![2; 16])
        );
        let decoded: LiveClientConfig = serde_json::from_value(json).unwrap();
        client
            .configure_live_nvme(
                &decoded,
                crate::distributed::remote_db::RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY,
            )
            .unwrap();
        let selected = client.live_config.as_ref().unwrap();
        assert_eq!(selected.host_nqn, identity.host_nqn);
        assert_eq!(selected.host_id, identity.host_id);
        assert!(selected.owner_identities.is_empty());
    }

    pub(crate) fn leased_latencies(
        counts: &crate::distributed::nvmeof::live_metrics::LiveNvmeCounts,
    ) -> [(u64, u64); 5] {
        [
            (counts.queue_samples, counts.queue_ns),
            (counts.submit_samples, counts.submit_ns),
            (counts.cq_samples, counts.cq_ns),
            (counts.leased_write_samples, counts.leased_write_ns),
            (counts.extent_switch_samples, counts.extent_switch_ns),
        ]
    }

    fn write(client: &mut Client, batch: &tcp::CompleteBatch) -> tcp::DurableBatch {
        client
            .submit_and_drain_leased_group(1, vec![batch.clone()], &WriteTimeline::default(), 2)
            .unwrap()[0]
    }

    #[test]
    fn polled_leased_frame_and_skip_complete_or_report_unsupported() {
        use crate::distributed::nvmeof::live_io::{LiveIo, LiveIoError};
        use crate::distributed::nvmeof::live_namespace::{LiveNamespace, LiveNamespaceAccess};
        let unsupported = |errno| {
            assert!(
                matches!(errno, Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS)),
                "unexpected polled writer errno: {errno:?}"
            );
            eprintln!("local filesystem or kernel rejects polled writes: errno={errno:?}");
        };
        let unsupported_write = |client: &mut Client, error: super::super::TcpError| {
            assert!(
                error.to_string().starts_with("leased write failed"),
                "unexpected polled writer error: {error}"
            );
            let io = &mut client.live_writer.as_mut().unwrap().io;
            unsupported(io.last_write_errno());
            assert_eq!(io.outstanding(), 0);
            assert!(io.shutdown().unwrap().is_empty());
        };
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let fixture = Fixture::new(tier, 16384);
            let mut client = fixture.writer_without_ahead();
            let mut config = client.live_config.clone().unwrap();
            config.polled_writes = true;
            config.leased_sync = LeasedSync::GroupFlush;
            config.leased_flush_min_frame_bytes = 0;
            config.queue_geometry.poll_queues = 1;
            client
                .configure_live_nvme(
                    &config,
                    crate::distributed::remote_db::RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY,
                )
                .unwrap();
            let leased = client.lease_live_extent(1).unwrap();
            let path = leased.extent.segment.path.clone();
            let namespace = LiveNamespace::open_local_file(
                &path,
                leased.binding.bytes,
                leased.binding.block_size,
                LiveNamespaceAccess::ReadWrite,
            )
            .unwrap();
            client.install_leased_extent(leased).unwrap();
            // The ordinary local fixture stays interrupt-driven. Replace only
            // this test's queue so it exercises the actual leased completion path.
            client.ensure_leased_queue().unwrap();
            let io = match LiveIo::new(namespace, 4, config.max_inflight_bytes, true) {
                Ok(io) => io,
                Err(LiveIoError::Io(error)) => {
                    unsupported(error.raw_os_error());
                    return;
                }
                Err(error) => panic!("polled writer setup failed: {error}"),
            };
            client.live_writer.as_mut().unwrap().io = io;
            let batch = fixture.batch(0);
            if let Err(error) = client.submit_and_drain_leased_group(
                1,
                vec![batch.clone()],
                &WriteTimeline::default(),
                2,
            ) {
                unsupported_write(&mut client, error);
                return;
            }
            let cursor = client.live_leased.current.as_ref().unwrap().cursor;
            if let Err(error) = client.write_leased_skip() {
                unsupported_write(&mut client, error);
                return;
            }
            assert_eq!(client.live_writer.as_ref().unwrap().io.outstanding(), 0);
            assert_eq!(fixture.metrics.snapshot().counts.completed_write_ops, 2);
            let counts = fixture.metrics.snapshot().counts;
            assert_eq!(counts.leased_flushes, 0);
            assert_eq!(
                counts.fua_writes,
                if tier == DurabilityTier::Synchronous {
                    2
                } else {
                    0
                }
            );
            let file = std::fs::File::open(path).unwrap();
            let mut marker = [0; 8];
            std::os::unix::fs::FileExt::read_exact_at(&file, &mut marker, cursor).unwrap();
            assert_eq!(&marker, crate::crc::CrcFrame::skip_marker().as_ref());
            // Local IOPOLL can reject an already-written block at close too,
            // not just ring setup or the first write. Keep the documented
            // unsupported outcome through the final closing skip as well.
            if let Err(error) = client.close_live_writer(&mut Vec::new(), None) {
                unsupported_write(&mut client, error);
                return;
            }
            let log = fixture.node.inventory.lock().active(1).unwrap();
            assert_eq!(log.receipt_count(), 1);
        }
    }

    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    #[test]
    fn mapped_residency_probe_accepts_closed_leased_extent() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        write(&mut client, &fixture.batch(0));
        let log = fixture.node.inventory.lock().active(1).unwrap();
        assert_eq!(
            log.read_evidence_for_benchmark(true)
                .unwrap_err()
                .to_string(),
            "mapped residency probe unavailable during live writes"
        );
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
        assert!(log.read_evidence_for_benchmark(true).is_ok());
    }

    #[test]
    fn leased_both_tiers_acknowledge_cqe_and_match_owner_receipts() {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let fixture = Fixture::new(tier, 16384);
            let mut client = fixture.writer();
            let mut owner_reader = fixture.client();
            for sequence in 0..3 {
                let batch = fixture.batch(sequence);
                let previous_adoptions = fixture.metrics.snapshot().counts.adoption_sync_exchanges;
                let receipt = write(&mut client, &batch);
                let reference = batch
                    .record_reference(receipt.appended().address(), 0)
                    .unwrap();
                let descriptor = batch.record_descriptors()[0];
                let entry = &batch.encoded()
                    [descriptor.offset as usize + 8..(descriptor.offset + descriptor.len) as usize];
                let position = crate::distributed::hybrid::position::VersionPosition::member(
                    batch.version(),
                    receipt.appended().address(),
                    0,
                    descriptor,
                    crc32fast::hash(entry),
                );
                // No adoption exchange is needed to acknowledge the frame.
                let counts = fixture.metrics.snapshot().counts;
                assert_eq!(
                    counts.adoption_async_exchanges + counts.adoption_sync_exchanges,
                    previous_adoptions
                );
                assert_eq!(owner_read(&fixture, &reference), batch.mutations());
                // Active-epoch `ResolveLiveRecords` is refused in per-owner
                // export mode; pin the owner's real WAL inode directly instead
                // (the same grant the resolution used to return).
                let mut cache = crate::distributed::tcp::live_read::LiveReadCache::default();
                crate::distributed::tcp::live_read::tests::install_owner_local(
                    &mut cache,
                    &fixture.node,
                    position,
                );
                let member = batch.encoded()
                    [descriptor.offset as usize..(descriptor.offset + descriptor.len) as usize]
                    .to_vec();
                let fetched = client
                    .live_read_records(
                        &mut cache,
                        1,
                        &[position],
                        Instant::now() + Duration::from_secs(2),
                        None,
                    )
                    .unwrap();
                assert_eq!(fetched.len(), 1);
                assert_eq!(fetched[0].as_ref(), member.as_slice());
                cache.close(&mut owner_reader, 1).unwrap();
                client.settle_leased_extent(&mut Vec::new(), None).unwrap();
                let adopted = client.live_leased.current.as_mut().unwrap().adopted;
                assert_eq!(
                    adopted.end,
                    client.live_leased.current.as_mut().unwrap().written_end
                );
                assert_eq!(adopted.count, sequence + 1);
            }
            let counts = fixture.metrics.snapshot().counts;
            assert_eq!(
                (
                    counts.extents_leased,
                    counts.leased_frames,
                    counts.completed_write_ops
                ),
                (1, 3, 3)
            );
            assert_eq!(counts.lease_exchanges, 1);
            assert_eq!(
                counts.fua_writes,
                if tier == DurabilityTier::Synchronous {
                    3
                } else {
                    0
                }
            );
            assert_eq!(counts.leased_write_samples, 3);
            client.close_leased_extent(&mut Vec::new(), None).unwrap();
            assert!(client.live_leased.current.is_none());
        }
    }

    #[test]
    fn leased_coordinator_crash_recovers_all_acknowledged_frames_in_both_tiers() {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let fixture = Fixture::new(tier, 16384);
            let mut client = fixture.writer();
            let batches: Vec<_> = (0..3).map(|n| fixture.batch(n)).collect();
            let receipts: Vec<_> = batches
                .iter()
                .map(|batch| write(&mut client, batch))
                .collect();
            assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 0);
            drop(client);
            let mut recovery = fixture.client();
            recovery.fence_to(2).unwrap();
            let log = fixture
                .node
                .inventory
                .lock()
                .live_recovery_log(2, 1)
                .unwrap()
                .unwrap();
            assert_eq!(log.receipt_count(), receipts.len());
            for (batch, receipt) in batches.iter().zip(receipts) {
                let reference = batch
                    .record_reference(receipt.appended().address(), 0)
                    .unwrap();
                assert_eq!(log.read_record(&reference).unwrap(), batch.mutations()[0]);
            }
        }
    }

    #[test]
    fn leased_duplicate_group_is_rejected_before_lease_or_submission() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        let batch = fixture.batch(0);
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![batch.clone(), batch],
                    &WriteTimeline::default(),
                    2
                )
                .is_err()
        );
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!((counts.lease_exchanges, counts.completed_write_ops), (0, 0));
        assert!(client.live_writer.is_none());
    }

    #[test]
    fn leased_group_limit_is_rejected_before_lease_or_buffer() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        client.expected.group_limits.max_encoded_bytes = 32;
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(0)],
                    &WriteTimeline::default(),
                    2
                )
                .is_err()
        );
        assert!(client.live_writer.is_none());
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!((counts.lease_exchanges, counts.completed_write_ops), (0, 0));
    }

    #[test]
    fn leased_async_adoption_accepts_self_adopted_later_prefix() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        // Capture the same identity used by an asynchronous exchange.
        let pending = PendingAdoption {
            target: PendingTarget::Current,
            through: 4096,
            close: false,
            lease_next: false,
        };
        write(&mut client, &fixture.batch(1));
        let current = client.live_leased.current.as_ref().unwrap();
        let token = current.lease.extent.token;
        let epoch = current.lease.extent.segment.epoch;
        // Real owner self-adoption publishes the intermediate boundary.
        let reference = fixture
            .batch(1)
            .record_reference(
                crate::distributed::storage::receipt_for(
                    &fixture.config.partition,
                    &fixture.batch(1),
                    crate::distributed::storage::member_position(
                        crate::WalPosition::new(4096, 4096),
                        16,
                        &fixture.batch(1),
                    ),
                )
                .address(),
                0,
            )
            .unwrap();
        owner_read(&fixture, &reference);
        let reply = client
            .adopt_live_extent(epoch, token, 4096, false, false)
            .unwrap();
        write(&mut client, &fixture.batch(2));
        assert_eq!(reply.adoption.adopted_end, 8192);
        client
            .live_leased
            .current
            .as_mut()
            .unwrap()
            .validate_adoption(pending, &reply)
            .unwrap();
        assert_eq!(
            client.live_leased.current.as_mut().unwrap().prefixes.len(),
            1
        );
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }

    #[test]
    fn leased_extent_switch_writes_skip_and_continues_at_fragment_start() {
        use std::os::unix::fs::FileExt;
        let fixture = Fixture::new(DurabilityTier::Synchronous, 16384);
        let mut client = fixture.writer_without_ahead();
        let first = fixture.batch(0);
        let second = fixture.big_batch(1);
        let third = fixture.big_batch(2);
        let receipts = [
            write(&mut client, &first),
            write(&mut client, &second),
            write(&mut client, &third),
        ];
        assert_eq!(
            client
                .live_leased
                .current
                .as_ref()
                .unwrap()
                .lease
                .extent
                .start,
            16384
        );
        assert!(receipts[2].appended().address().offset >= 16384);
        let segment = &client
            .live_leased
            .current
            .as_ref()
            .unwrap()
            .lease
            .extent
            .segment;
        let mut marker = [0u8; 4096];
        std::fs::File::open(&segment.path)
            .unwrap()
            .read_exact_at(&mut marker, 12288)
            .unwrap();
        assert_eq!(&marker[..8], &[255; 8]);
        assert!(marker[8..].iter().all(|b| *b == 0));
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(
            (
                counts.extents_leased,
                counts.leased_frames,
                counts.leased_skip_markers
            ),
            (2, 3, 1)
        );
        assert_eq!(
            (
                counts.adoption_sync_exchanges,
                counts.extent_switch_samples,
                counts.fua_writes
            ),
            (1, 1, 4)
        );
        for (batch, receipt) in [&first, &second, &third].into_iter().zip(receipts) {
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(owner_read(&fixture, &reference), batch.mutations());
        }
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }

    #[test]
    fn leased_refused_async_submission_records_no_timing() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        let deadline = Instant::now() + Duration::from_secs(2);
        assert!(
            client
                .submit_control(&tcp::Request::Stats, deadline)
                .unwrap()
        );
        client.submit_leased_adoption().unwrap();
        assert!(!client.leased_adoption_pending());
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.adoption_async_exchanges, 0);
        assert_eq!(counts.adoption_async_samples, 0);
        loop {
            client.progress_session().unwrap();
            if let Some(reply) = client.take_control() {
                reply.unwrap();
                break;
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }

    #[test]
    fn leased_bad_digest_poison_prevents_later_write_or_control() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        write(&mut client, &fixture.batch(0));
        client.live_leased.current.as_mut().unwrap().prefixes[0].digest[0] ^= 1;
        assert!(client.settle_leased_extent(&mut Vec::new(), None).is_err());
        let before = fixture.metrics.snapshot().counts;
        assert_eq!(before.adoption_async_samples, 0);
        assert_eq!(before.adoption_settle_samples, 0);
        assert_eq!(before.adoption_history_full_settles, 0);
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(1)],
                    &WriteTimeline::default(),
                    2
                )
                .is_err()
        );
        assert!(client.close_leased_extent(&mut Vec::new(), None).is_err());
        let after = fixture.metrics.snapshot().counts;
        assert_eq!(after.completed_write_ops, before.completed_write_ops);
        assert_eq!(after.metadata_requests, before.metadata_requests);
        assert_eq!(after.adoption_async_samples, 0);
        assert_eq!(after.adoption_settle_samples, 0);
    }

    #[test]
    fn leased_adoption_rejects_shortfall_nonboundary_and_future_offsets() {
        for (through, adopted_end) in [(8192, 4096), (4096, 6144), (4096, 12288)] {
            let fixture = Fixture::new(DurabilityTier::Native, 16384);
            let mut client = fixture.writer_without_ahead();
            write(&mut client, &fixture.batch(0));
            write(&mut client, &fixture.batch(1));
            let current = client.live_leased.current.as_ref().unwrap();
            let mut reply = client
                .adopt_live_extent(1, current.lease.extent.token, 8192, false, false)
                .unwrap();
            reply.adoption.adopted_end = adopted_end;
            assert!(
                client
                    .check_leased_adoption(
                        PendingAdoption {
                            target: PendingTarget::Current,
                            through,
                            close: false,
                            lease_next: false
                        },
                        &reply,
                    )
                    .is_err()
            );
            assert!(client.live_write_failed);
            let before = fixture.metrics.snapshot().counts.completed_write_ops;
            assert!(
                client
                    .submit_and_drain_leased_group(
                        1,
                        vec![fixture.batch(2)],
                        &WriteTimeline::default(),
                        2
                    )
                    .is_err()
            );
            assert_eq!(
                fixture.metrics.snapshot().counts.completed_write_ops,
                before
            );
        }
    }

    #[test]
    fn leased_coordinator_crash_recovers_acknowledged_prefix_and_drops_torn_tail() {
        use std::os::unix::fs::FileExt;
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        let first = fixture.batch(0);
        let receipt = write(&mut client, &first);
        let second = fixture.batch(1);
        let third = fixture.batch(2);
        let group = crate::distributed::group::CompleteGroup::new_live(
            &client.shape,
            vec![second],
            fixture.config.partition.batch_limits,
            fixture.config.partition.group_limits,
        )
        .unwrap();
        let (_, torn) =
            crate::distributed::nvmeof::live_storage::LiveGroupDescriptor::from_group(&group)
                .unwrap();
        let group = crate::distributed::group::CompleteGroup::new_live(
            &client.shape,
            vec![third],
            fixture.config.partition.batch_limits,
            fixture.config.partition.group_limits,
        )
        .unwrap();
        let (_, later) =
            crate::distributed::nvmeof::live_storage::LiveGroupDescriptor::from_group(&group)
                .unwrap();
        let path = client
            .live_leased
            .current
            .as_ref()
            .unwrap()
            .lease
            .extent
            .segment
            .path
            .clone();
        let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.write_all_at(&torn[..100], 4096).unwrap();
        file.write_all_at(&later, 8192).unwrap();
        drop(file);
        drop(client); // No settle or close: owner must recover from its WAL.
        let mut recovered = fixture.client();
        recovered.fence_to(2).unwrap();
        let log = fixture
            .node
            .inventory
            .lock()
            .live_recovery_log(2, 1)
            .unwrap()
            .unwrap();
        assert_eq!(log.receipt_count(), 1);
        let reference = first
            .record_reference(receipt.appended().address(), 0)
            .unwrap();
        assert_eq!(log.read_record(&reference).unwrap(), first.mutations()[0]);
    }

    #[test]
    fn leased_async_timing_counts_only_validated_completion() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        client.submit_leased_adoption().unwrap();
        client.submit_leased_adoption().unwrap();
        let submitted = fixture.metrics.snapshot().counts;
        assert_eq!(submitted.adoption_async_exchanges, 1);
        assert_eq!(submitted.adoption_async_samples, 0);
        client.wait_leased_control(&mut Vec::new(), None).unwrap();
        let completed = fixture.metrics.snapshot().counts;
        assert_eq!(completed.adoption_async_samples, 1);
        assert!(completed.adoption_async_ns > 0);
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
        assert_eq!(fixture.metrics.snapshot().counts.adoption_settle_samples, 0);
    }

    #[test]
    fn leased_history_full_settles_before_another_submission() {
        let fixture = Fixture::new(DurabilityTier::Native, 8 * 1024 * 1024);
        let mut client = fixture.writer_without_ahead();
        assert_eq!(client.live_leased.prefix_buffers[0].capacity(), 1024);
        for sequence in 0..1024 {
            write(&mut client, &fixture.batch(sequence));
        }
        assert_eq!(
            client.live_leased.current.as_mut().unwrap().prefixes.len(),
            1024
        );
        assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 0);
        write(&mut client, &fixture.batch(1024));
        assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 1);
        assert_eq!(
            fixture
                .metrics
                .snapshot()
                .counts
                .adoption_history_full_settles,
            1
        );
        assert_eq!(fixture.metrics.snapshot().counts.adoption_settle_samples, 1);
        assert_eq!(
            client.live_leased.current.as_mut().unwrap().prefixes.len(),
            1
        );
        client.settle_leased_extent(&mut Vec::new(), None).unwrap();
        assert_eq!(fixture.metrics.snapshot().counts.adoption_settle_samples, 2);
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
        assert_eq!(
            fixture
                .metrics
                .snapshot()
                .counts
                .adoption_history_full_settles,
            1
        );
        assert_eq!(fixture.metrics.snapshot().counts.adoption_settle_samples, 2);
    }

    #[test]
    fn omitted_optional_keys_default_and_removed_write_mode_is_rejected() {
        assert!(
            LeasedWriter::default()
                .prefix_buffers
                .iter()
                .all(|buffer| buffer.capacity() == 0)
        );
        let value = serde_json::json!({
            "host_nqn": "nqn.2026-01.test:leased", "host_id": vec![1; 16],
            "max_inflight_requests": 4, "max_inflight_bytes": 65536,
            "connect_timeout_ms": 1000, "io_timeout_ms": 1000,
            "leased_durability": "native",
        });
        let config: LiveClientConfig = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(config.leased_durability, DurabilityTier::Native);
        assert!(config.inline_reads);
        assert!(config.lease_ahead);
        assert!(!config.polled_writes);
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove("leased_durability");
        assert!(serde_json::from_value::<LiveClientConfig>(missing).is_err());
        let mut removed = value;
        removed["write_mode"] = serde_json::json!("leased");
        let error = serde_json::from_value::<LiveClientConfig>(removed)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unknown field"), "{error}");
    }

    #[test]
    fn leased_traced_failed_and_short_completions_keep_unknown_write_contract() {
        use crate::distributed::write_timeline::WriteTimelineStage as Stage;
        for result in [-libc::EIO, 2048] {
            let fixture = Fixture::new(DurabilityTier::Native, 16384);
            let mut client = fixture.writer();
            client.leased_test_fail_write(result);
            let timeline = WriteTimeline::default();
            timeline.arm(1, 1).unwrap();
            let batch = fixture.batch(0);
            let sample = timeline.begin(0).unwrap();
            timeline.operation(sample, batch.id().operation);
            timeline.batch(batch.id().operation, batch.version());
            assert!(
                client
                    .submit_and_drain_leased_group(1, vec![batch], &timeline, 2)
                    .is_err()
            );
            assert!(client.live_write_failed);
            let export = timeline.finish().unwrap();
            let stages: Vec<_> = export.records[0]
                .events
                .iter()
                .map(|event| event.stage)
                .collect();
            let start = stages
                .iter()
                .position(|stage| *stage == Stage::LiveIoStarted)
                .unwrap();
            assert_eq!(
                &stages[start..],
                &[
                    Stage::LiveIoStarted,
                    Stage::LiveFrameEncoded,
                    Stage::LiveIoSubmitted,
                    Stage::LiveIoCompleted,
                ]
            );
            assert_eq!(fixture.metrics.snapshot().counts.unknown_writes, 1);
            let mut failed = client.take_failed_live_writer().unwrap();
            failed.drain_failed().unwrap();
            assert_eq!(failed.io.outstanding(), 0);
            let counts = fixture.metrics.snapshot().counts;
            assert_eq!(counts.failed_write_ops, u64::from(result < 0));
            assert_eq!(counts.short_write_ops, u64::from(result == 2048));
        }
    }

    #[test]
    fn leased_failed_short_and_timed_out_writes_are_unknown_and_never_retried() {
        for (result, timeout) in [
            (Some(-libc::EIO), false),
            (Some(2048), false),
            (None, true),
            (Some(-libc::EIO), true),
            (Some(2048), true),
        ] {
            let fixture = Fixture::new(DurabilityTier::Native, 16384);
            let mut client = fixture.writer();
            if let Some(result) = result {
                client.leased_test_fail_write(result);
            }
            if timeout {
                client.leased_test_timeout_write();
            }
            assert!(
                client
                    .submit_and_drain_leased_group(
                        1,
                        vec![fixture.batch(0)],
                        &WriteTimeline::default(),
                        2
                    )
                    .is_err()
            );
            assert!(client.live_write_failed);
            let submitted = client
                .live_writer
                .as_ref()
                .unwrap()
                .io
                .stats()
                .write_requests;
            assert_eq!(submitted, 1);
            assert!(
                client
                    .submit_and_drain_leased_group(
                        1,
                        vec![fixture.batch(1)],
                        &WriteTimeline::default(),
                        2
                    )
                    .is_err()
            );
            assert!(client.settle_leased_extent(&mut Vec::new(), None).is_err());
            assert_eq!(
                client
                    .live_writer
                    .as_ref()
                    .unwrap()
                    .io
                    .stats()
                    .write_requests,
                submitted
            );
            assert_eq!(fixture.metrics.snapshot().counts.unknown_writes, 1);
            let counts = fixture.metrics.snapshot().counts;
            assert_eq!(leased_latencies(&counts), [(0, 0); 5]);
            assert_eq!(
                counts.leased_failed_writes,
                u64::from(!timeout && result.is_some_and(|n| n < 0))
            );
            assert_eq!(
                counts.leased_short_writes,
                u64::from(!timeout && result == Some(2048))
            );
            assert_eq!(counts.leased_timed_out_writes, u64::from(timeout));
            let mut failed = client.take_failed_live_writer().unwrap();
            failed.drain_failed().unwrap();
            for retire_again in [false, true] {
                if retire_again {
                    failed.drain_failed().unwrap();
                }
                let counts = fixture.metrics.snapshot().counts;
                assert_eq!(leased_latencies(&counts), [(0, 0); 5]);
                assert_eq!(
                    counts.failed_write_ops,
                    u64::from(result.is_some_and(|n| n < 0))
                );
                assert_eq!(counts.short_write_ops, u64::from(result == Some(2048)));
                assert_eq!(
                    counts.short_write_bytes,
                    if result == Some(2048) { 2048 } else { 0 }
                );
                assert_eq!(counts.completed_write_ops, u64::from(result.is_none()));
                assert_eq!(
                    counts.completed_write_bytes,
                    if result.is_none() { 4096 } else { 0 }
                );
            }
        }
    }

    #[test]
    fn leased_client_drop_publishes_late_terminal_counts_without_latency() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        client.leased_test_timeout_write();
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(0)],
                    &WriteTimeline::default(),
                    2,
                )
                .is_err()
        );
        assert_eq!(client.live_writer.as_ref().unwrap().io.outstanding(), 1);
        drop(client);
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(leased_latencies(&counts), [(0, 0); 5]);
        assert_eq!(counts.completed_write_ops, 1);
        assert_eq!(counts.completed_write_bytes, 4096);
        assert_eq!((counts.failed_write_ops, counts.short_write_ops), (0, 0));
    }

    #[test]
    fn leased_rejects_wrong_geometry_without_submitting_a_write() {
        for bad in 0..3 {
            let fixture = Fixture::new(DurabilityTier::Native, 16384);
            let mut client = fixture.writer();
            let mut leased = client.lease_live_extent(1).unwrap();
            match bad {
                0 => leased.extent.start += 1,
                1 => leased.binding.bytes += 4096,
                _ => leased.extent.end += 4096,
            }
            assert!(client.install_leased_extent(leased).is_err());
            assert!(client.live_write_failed);
            assert!(
                client
                    .submit_and_drain_leased_group(
                        1,
                        vec![fixture.batch(0)],
                        &WriteTimeline::default(),
                        2
                    )
                    .is_err()
            );
            assert_eq!(fixture.metrics.snapshot().counts.completed_write_ops, 0);
        }
    }

    #[test]
    fn leased_noncontiguous_next_extent_poison_stops_writes() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        client.write_leased_skip().unwrap();
        let token = client
            .live_leased
            .current
            .as_ref()
            .unwrap()
            .lease
            .extent
            .token;
        let mut reply = client
            .adopt_live_extent(1, token, 4096, true, true)
            .unwrap();
        reply.next.as_mut().unwrap().extent.start += 4096;
        assert!(
            client
                .check_leased_adoption(
                    PendingAdoption {
                        target: PendingTarget::Current,
                        through: 4096,
                        close: true,
                        lease_next: true
                    },
                    &reply,
                )
                .is_err()
        );
        let before = fixture.metrics.snapshot().counts.completed_write_ops;
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(1)],
                    &WriteTimeline::default(),
                    2
                )
                .is_err()
        );
        assert_eq!(
            fixture.metrics.snapshot().counts.completed_write_ops,
            before
        );
    }

    #[test]
    fn leased_checks_epoch_before_write() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        assert!(
            client
                .submit_and_drain_leased_group(
                    2,
                    vec![fixture.batch(0)],
                    &WriteTimeline::default(),
                    2
                )
                .is_err()
        );
        assert_eq!(fixture.metrics.snapshot().counts.metadata_requests, 0);
    }

    #[test]
    fn polled_writes_require_a_poll_queue() {
        let mut config: LiveClientConfig = serde_json::from_value(serde_json::json!({
            "host_nqn": "nqn.2026-01.test:leased", "host_id": vec![1; 16],
            "max_inflight_requests": 4, "max_inflight_bytes": 65536,
            "connect_timeout_ms": 1000, "io_timeout_ms": 1000,
            "lease_ahead": true, "polled_writes": true, "leased_durability": "native",
        }))
        .unwrap();
        assert_eq!(
            config.validate(),
            Err("polled live writes require a poll queue")
        );
        config.queue_geometry.poll_queues = 1;
        assert!(config.validate().is_ok());
        config.polled_writes = false;
        config.queue_geometry.poll_queues = 0;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn leased_first_and_next_tier_mismatch_fail_before_another_write() {
        for actual in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let requested = if actual == DurabilityTier::Native {
                DurabilityTier::Synchronous
            } else {
                DurabilityTier::Native
            };
            let fixture = Fixture::new(actual, 16384);
            let mut client = fixture.client();
            client.fence_to(1).unwrap();
            fixture.configure_writer(&mut client, requested);
            let error = client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(0)],
                    &WriteTimeline::default(),
                    2,
                )
                .unwrap_err();
            assert!(error.to_string().contains(&format!("{actual:?}")));
            assert!(error.to_string().contains(&format!("{requested:?}")));
            assert_eq!(fixture.metrics.snapshot().counts.completed_write_ops, 0);
            assert!(client.live_write_failed);
            assert!(client.live_writer.is_none());
        }
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        client.write_leased_skip().unwrap();
        let token = client
            .live_leased
            .current
            .as_ref()
            .unwrap()
            .lease
            .extent
            .token;
        let mut reply = client
            .adopt_live_extent(1, token, 4096, true, true)
            .unwrap();
        client
            .check_leased_adoption(
                PendingAdoption {
                    target: PendingTarget::Current,
                    through: 4096,
                    close: true,
                    lease_next: true,
                },
                &reply,
            )
            .unwrap();
        let mut next = reply.next.take().unwrap();
        next.tier = DurabilityTier::Synchronous;
        let before = fixture.metrics.snapshot().counts.completed_write_ops;
        assert!(client.install_leased_extent(next).is_err());
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(1)],
                    &WriteTimeline::default(),
                    2
                )
                .is_err()
        );
        assert_eq!(
            fixture.metrics.snapshot().counts.completed_write_ops,
            before
        );
    }

    #[test]
    fn leased_policy_change_after_connect_uses_frozen_lease_tier() {
        for before in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let after = if before == DurabilityTier::Native {
                DurabilityTier::Synchronous
            } else {
                DurabilityTier::Native
            };
            let fixture = Fixture::new(before, 16384);
            let mut client = fixture.client();
            fixture
                .node
                .set_durability(DurabilityConfig::with_tier(after))
                .unwrap();
            client.fence_to(1).unwrap();
            fixture.configure_writer(&mut client, after);
            write(&mut client, &fixture.batch(0));
            assert_eq!(
                client.live_leased.current.as_ref().unwrap().lease.tier,
                after
            );
            assert_eq!(
                fixture.metrics.snapshot().counts.fua_writes,
                u64::from(after == DurabilityTier::Synchronous)
            );
            client.close_leased_extent(&mut Vec::new(), None).unwrap();
        }
    }

    #[test]
    fn leased_frame_bytes_equal_the_descriptor_path() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let client = fixture.writer();
        let group = crate::distributed::group::CompleteGroup::new_live(
            &client.shape,
            vec![fixture.batch(0), fixture.big_batch(1)],
            fixture.config.partition.batch_limits,
            fixture.config.partition.group_limits,
        )
        .unwrap();
        let (_, expected) =
            crate::distributed::nvmeof::live_storage::LiveGroupDescriptor::from_group(&group)
                .unwrap();
        assert_eq!(group.live_frame().unwrap(), expected);
    }

    #[test]
    fn leased_full_admission_has_no_latency_sample_and_retains_dma() {
        use crate::distributed::nvmeof::live_io::{LiveSubmitOutcome, LiveWriteSync};
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        let leased = client.lease_live_extent(1).unwrap();
        client.install_leased_extent(leased).unwrap();
        client.ensure_leased_queue().unwrap();
        let io = &mut client.live_writer.as_mut().unwrap().io;
        for id in 2..6 {
            assert_eq!(
                io.submit_write(
                    id,
                    0,
                    &[0; 4096],
                    LiveWriteSync::None,
                    Instant::now() + Duration::from_secs(5)
                )
                .unwrap(),
                LiveSubmitOutcome::Submitted
            );
        }
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(0)],
                    &WriteTimeline::default(),
                    2
                )
                .is_err()
        );
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.leased_full_writes, 1);
        assert_eq!(leased_latencies(&counts), [(0, 0); 5]);
        assert!(client.drain_live_writer(&mut Vec::new(), None).is_err());
        let mut failed = client.take_failed_live_writer().unwrap();
        assert_eq!(failed.io.outstanding(), 4);
        failed.drain_failed().unwrap();
        assert_eq!(failed.io.outstanding(), 0);
    }

    #[test]
    fn leased_failed_rollover_skip_does_not_sample_any_latency_stage() {
        for result in [Some(-libc::EIO), Some(2048), None] {
            let fixture = Fixture::new(DurabilityTier::Native, 16384);
            let mut client = fixture.writer();
            write(&mut client, &fixture.batch(0));
            let before = leased_latencies(&fixture.metrics.snapshot().counts);
            if let Some(result) = result {
                client.leased_test_fail_write(result);
            } else {
                client.leased_test_timeout_write();
            }
            assert!(
                client
                    .submit_and_drain_leased_group(
                        1,
                        vec![
                            fixture.big_batch(1),
                            fixture.big_batch(2),
                            fixture.big_batch(3)
                        ],
                        &WriteTimeline::default(),
                        2,
                    )
                    .is_err()
            );
            assert!(client.live_write_failed);
            assert_eq!(leased_latencies(&fixture.metrics.snapshot().counts), before);
            let mut failed = client.take_failed_live_writer().unwrap();
            failed.drain_failed().unwrap();
            assert_eq!(leased_latencies(&fixture.metrics.snapshot().counts), before);
        }
    }

    #[test]
    fn leased_failed_rollover_adoption_does_not_sample_extent_switch() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        client
            .live_leased
            .current
            .as_mut()
            .unwrap()
            .lease
            .extent
            .token[0] ^= 1;
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![
                        fixture.big_batch(1),
                        fixture.big_batch(2),
                        fixture.big_batch(3)
                    ],
                    &WriteTimeline::default(),
                    2,
                )
                .is_err()
        );
        let counts = fixture.metrics.snapshot().counts;
        assert!(client.live_write_failed);
        assert_eq!(
            (counts.extent_switch_samples, counts.extent_switch_ns),
            (0, 0)
        );
        // The marker itself reached a successful CQE, but the switch did not.
        assert_eq!(counts.completed_write_ops, 2);
        assert_eq!(counts.leased_write_samples, 2);
    }

    #[test]
    fn leased_switch_preserves_a_multi_block_zero_suffix() {
        use std::os::unix::fs::FileExt;
        let fixture = Fixture::new(DurabilityTier::Synchronous, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        let old_path = client
            .live_leased
            .current
            .as_ref()
            .unwrap()
            .lease
            .extent
            .segment
            .path
            .clone();
        let group = vec![
            fixture.big_batch(1),
            fixture.big_batch(2),
            fixture.big_batch(3),
        ];
        let receipts = client
            .submit_and_drain_leased_group(1, group.clone(), &WriteTimeline::default(), 2)
            .unwrap();
        assert_eq!(
            client
                .live_leased
                .current
                .as_ref()
                .unwrap()
                .lease
                .extent
                .start,
            16384
        );
        let mut suffix = vec![0; 12288];
        std::fs::File::open(old_path)
            .unwrap()
            .read_exact_at(&mut suffix, 4096)
            .unwrap();
        assert_eq!(&suffix[..8], &[255; 8]);
        assert!(suffix[8..].iter().all(|byte| *byte == 0));
        assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 1);
        for (batch, receipt) in group.iter().zip(receipts) {
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(owner_read(&fixture, &reference), batch.mutations());
        }
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }

    #[test]
    fn leased_lower_through_retry_uses_retired_proof() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        write(&mut client, &fixture.batch(1));
        client.settle_leased_extent(&mut Vec::new(), None).unwrap();
        assert!(
            client
                .live_leased
                .current
                .as_mut()
                .unwrap()
                .prefixes
                .is_empty()
        );
        let token = client
            .live_leased
            .current
            .as_ref()
            .unwrap()
            .lease
            .extent
            .token;
        let reply = client
            .adopt_live_extent(1, token, 4096, false, false)
            .unwrap();
        assert_eq!(
            (reply.adoption.adopted_end, reply.adoption.receipt_count),
            (8192, 2)
        );
        client
            .check_leased_adoption(
                PendingAdoption {
                    target: PendingTarget::Current,
                    through: 4096,
                    close: false,
                    lease_next: false,
                },
                &reply,
            )
            .unwrap();
        assert!(
            client
                .live_leased
                .current
                .as_mut()
                .unwrap()
                .prefixes
                .is_empty()
        );
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }

    #[test]
    fn lease_ahead_switch_sends_no_synchronous_owner_request() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        write(&mut client, &fixture.batch(0));
        client.wait_closing_extent(true).unwrap();
        let before = fixture.metrics.snapshot().counts;
        let receipts = client
            .submit_and_drain_leased_group(
                1,
                vec![
                    fixture.big_batch(1),
                    fixture.big_batch(2),
                    fixture.big_batch(3),
                ],
                &WriteTimeline::default(),
                2,
            )
            .unwrap();
        let after = fixture.metrics.snapshot().counts;
        assert_eq!(
            after.adoption_sync_exchanges,
            before.adoption_sync_exchanges
        );
        assert_eq!(after.extent_switch_waits, 0);
        assert!(client.live_leased.closing.is_some());
        assert_eq!(
            client.live_leased.current.as_ref().unwrap().adopted.count,
            0
        );
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
        for (sequence, receipt) in (1..=3).zip(receipts) {
            let batch = fixture.big_batch(sequence);
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(owner_read(&fixture, &reference), batch.mutations());
        }
    }

    #[test]
    fn lease_ahead_switch_waits_when_prelease_is_late() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        write(&mut client, &fixture.batch(0));
        assert!(client.live_leased.next.is_none());
        client
            .submit_and_drain_leased_group(
                1,
                vec![
                    fixture.big_batch(1),
                    fixture.big_batch(2),
                    fixture.big_batch(3),
                ],
                &WriteTimeline::default(),
                2,
            )
            .unwrap();
        assert_eq!(fixture.metrics.snapshot().counts.extent_switch_waits, 1);
        assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 0);
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
        assert_eq!(fixture.node.log_for_test(1).receipt_count(), 4);
    }

    #[test]
    fn lease_ahead_close_releases_unused_preleased_extent() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        write(&mut client, &fixture.batch(0));
        client.wait_closing_extent(true).unwrap();
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
        assert!(client.live_leased.current.is_none());
        assert!(client.live_leased.closing.is_none());
        assert!(client.live_leased.next.is_none());
        assert!(
            client
                .live_leased
                .prefix_buffers
                .iter()
                .all(|buffer| buffer.capacity() == 1024)
        );
        let next = client.lease_live_extent(1).unwrap();
        assert_eq!(next.extent.start, 32768);
        client.install_leased_extent(next).unwrap();
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }

    #[test]
    fn lease_ahead_off_keeps_the_combined_close_and_lease() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        client
            .submit_and_drain_leased_group(
                1,
                vec![
                    fixture.big_batch(1),
                    fixture.big_batch(2),
                    fixture.big_batch(3),
                ],
                &WriteTimeline::default(),
                2,
            )
            .unwrap();
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.lease_exchanges, 1);
        assert_eq!(counts.adoption_sync_exchanges, 1);
        assert_eq!(counts.extents_leased, 2);
        assert_eq!(counts.extent_preleases, 0);
        assert_eq!(counts.extent_switch_waits, 0);
        assert!(client.live_leased.closing.is_none());
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }

    #[test]
    fn leased_rotation_exports_owner_profiles_and_timeline_stages() {
        for fail in [false, true] {
            let fixture = Fixture::new(DurabilityTier::Native, 16384);
            let mut client = fixture.writer_without_ahead();
            write(&mut client, &fixture.batch(0));
            if fail {
                client.leased_test_fail_write(-libc::EIO);
            }
            let timeline = WriteTimeline::default();
            timeline.arm(3, 1).unwrap();
            let batches: Vec<_> = (1..=3).map(|n| fixture.big_batch(n)).collect();
            let samples: Vec<_> = batches
                .iter()
                .enumerate()
                .map(|(index, batch)| {
                    let sample = timeline.begin(index as u64).unwrap();
                    timeline.operation(sample, batch.id().operation);
                    timeline.batch(batch.id().operation, batch.version());
                    sample
                })
                .collect();
            let result = client.submit_and_drain_leased_group(1, batches, &timeline, 2);
            assert_eq!(result.is_err(), fail);
            for sample in samples {
                timeline.complete(sample, None);
            }
            let export = serde_json::to_value(timeline.finish().unwrap()).unwrap();
            assert_eq!(export["dropped_events"], 0);
            for row in export["records"].as_array().unwrap() {
                let stages: Vec<_> = row["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|event| event["stage"].as_str().unwrap())
                    .collect();
                assert!(stages.contains(&"live_extent_switch_started"));
                assert_eq!(stages.contains(&"live_extent_switch_completed"), !fail);
                if !fail {
                    let start = stages
                        .iter()
                        .position(|s| *s == "live_extent_switch_started")
                        .unwrap();
                    let end = stages
                        .iter()
                        .position(|s| *s == "live_extent_switch_completed")
                        .unwrap();
                    assert!(start < end);
                    let io = stages.iter().position(|s| *s == "live_io_started").unwrap();
                    assert_eq!(
                        &stages[io..io + 4],
                        &[
                            "live_io_started",
                            "live_frame_encoded",
                            "live_io_submitted",
                            "live_io_completed",
                        ]
                    );
                }
            }
            let counts = fixture.metrics.snapshot().counts;
            assert_eq!(counts.lease_profile_samples, if fail { 1 } else { 2 });
            assert_eq!(counts.adopt_profile_samples, if fail { 0 } else { 1 });
            assert_eq!(counts.extent_preleases, 0);
            assert_eq!(counts.extent_switch_waits, 0);
            if fail {
                client
                    .take_failed_live_writer()
                    .unwrap()
                    .drain_failed()
                    .unwrap();
            } else {
                assert!(counts.adopt_owner_ns > 0);
                assert_eq!(counts.adopt_close_scan_bytes, 12288);
                client.close_leased_extent(&mut Vec::new(), None).unwrap();
            }
        }
    }

    #[test]
    fn leased_switch_harvests_delayed_adoption_before_replacing_extent() {
        use crate::distributed::storage::LivePausePoint;
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        let (entered, release) = fixture
            .node
            .log_for_test(1)
            .pause_live(LivePausePoint::FrameProbe);
        client.submit_leased_adoption().unwrap();
        // Shared cap for the pause-entry and skip-marker progress loops below.
        let deadline = Instant::now() + PROGRESS_CAP;
        loop {
            client.progress_session().unwrap();
            if entered.try_recv().is_ok() {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        let group = vec![
            fixture.big_batch(1),
            fixture.big_batch(2),
            fixture.big_batch(3),
        ];
        let (mut client, result) = thread::scope(|scope| {
            let (finished, done) = std::sync::mpsc::sync_channel(1);
            let work = scope.spawn(move || {
                let result =
                    client.submit_and_drain_leased_group(1, group, &WriteTimeline::default(), 2);
                let _ = finished.send(());
                (client, result)
            });
            while fixture.metrics.snapshot().counts.leased_skip_markers == 0 {
                assert!(Instant::now() < deadline);
                thread::sleep(Duration::from_millis(1));
            }
            assert!(matches!(
                done.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ));
            assert_eq!(fixture.metrics.snapshot().counts.leased_frames, 1);
            release.send(()).unwrap();
            work.join().unwrap()
        });
        result.unwrap();
        assert!(!client.leased_adoption_pending());
        assert_eq!(
            client
                .live_leased
                .current
                .as_ref()
                .unwrap()
                .lease
                .extent
                .start,
            16384
        );
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }

    #[test]
    fn control_timeline_captures_leased_requests_and_owner_position_phases() {
        let fixture = Fixture::new(DurabilityTier::Native, 128 * 1024);
        let mut client = fixture.writer();
        let timeline = Arc::new(WriteTimeline::default());
        client.trace_controls(timeline.clone(), 2);
        client.owner_timeline_arm(32).unwrap();
        timeline.arm(32, 1).unwrap();
        let first = client
            .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &timeline, 2)
            .unwrap()[0];
        client.wait_leased_control(&mut Vec::new(), None).unwrap();
        client.submit_leased_adoption().unwrap();
        client.wait_leased_control(&mut Vec::new(), None).unwrap();
        let through = first.appended().address().offset + u64::from(first.appended().address().len);
        client.checkpoint_position(1, 1, through).unwrap();
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
        let (records, dropped, incomplete, error) = client.owner_timeline_fetch_partial();
        assert!(error.is_none());
        assert_eq!((dropped, incomplete), (0, 0));
        let export = timeline.finish().unwrap();
        for kind in [
            "lease",
            "prelease",
            "adoption",
            "closing_adoption",
            "position",
        ] {
            let submitted = export
                .control_events
                .iter()
                .find(|event| event.target.kind == kind && event.stage == "submit")
                .unwrap();
            let completed = export
                .control_events
                .iter()
                .find(|event| {
                    event.envelope_id == submitted.envelope_id && event.stage == "complete"
                })
                .unwrap();
            assert_eq!(completed.success, Some(true));
            assert!(completed.elapsed_ns >= submitted.elapsed_ns);
            let owner = records
                .iter()
                .find(|record| record.envelope_id == submitted.envelope_id)
                .unwrap();
            assert!(owner.control_queue_ns.is_some());
            assert!(owner.handler_ns.is_some());
            if kind == "position" {
                assert_eq!(submitted.target.through, Some(through));
                assert!(owner.wal_file_persist_ns.is_some());
                assert!(owner.namespace_persist_ns.is_some());
                assert!(owner.append_persist_ns.is_some());
            }
        }
    }
    #[test]
    fn leased_eventfd_registration_failure_keeps_bounded_io_wait() {
        let fixture = Fixture::new(DurabilityTier::Native, 32768);
        let mut client = fixture.writer_without_ahead();
        client.leased_test_hold_completions(&[]);
        client.set_worker_wake(-1);
        assert!(!client.live_leased.worker_wake_registered);
        assert_eq!(
            fixture.metrics.snapshot().counts.leased_eventfd_unavailable,
            1
        );
        let timeline = WriteTimeline::default();
        assert!(matches!(
            client
                .submit_leased_group(1, vec![fixture.batch(0)], &timeline, 2, true)
                .unwrap(),
            LeasedSubmit::Submitted(1)
        ));
        let mut released = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while released.is_empty() {
            assert!(Instant::now() < deadline);
            client.wait_leased_flights(true).unwrap();
            client.progress_leased_flights(&mut released, None).unwrap();
        }
        assert_eq!(released[0].0, 1);
        client.clear_worker_wake();
    }

    fn progress_until(
        client: &mut Client,
        released: &mut Vec<(u64, Vec<tcp::DurableBatch>)>,
        timeline: &WriteTimeline,
        ready: impl Fn(&Client) -> bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !ready(client) {
            assert!(
                Instant::now() < deadline,
                "leased test progress deadline expired"
            );
            client.wait_leased_flights(false).unwrap();
            client
                .progress_leased_flights(released, Some(timeline))
                .unwrap();
        }
    }

    #[test]
    fn leased_window_deferred_attempts_do_not_consume_timeline_groups() {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let fixture = Fixture::new(tier, 32768);
            let mut client = fixture.writer_without_ahead();
            client.leased_test_set_window(1);
            let timeline = WriteTimeline::default();
            timeline.arm(1, 1).unwrap();
            assert!(matches!(
                client
                    .submit_leased_group(1, vec![fixture.batch(0)], &timeline, 2, true)
                    .unwrap(),
                LeasedSubmit::Submitted(1)
            ));
            let batch = fixture.batch(1);
            let call = timeline.begin(1).unwrap();
            timeline.operation(call, batch.id().operation);
            timeline.batch(batch.id().operation, batch.version());
            let mut batches = vec![batch];
            for _ in 0..3 {
                let LeasedSubmit::Deferred(returned) = client
                    .submit_leased_group(1, batches, &timeline, 2, true)
                    .unwrap()
                else {
                    panic!("full window admitted another frame");
                };
                batches = returned;
            }
            client
                .drain_leased_flights(&mut Vec::new(), Some(&timeline))
                .unwrap();
            client
                .submit_and_drain_leased_group(1, batches, &timeline, 2)
                .unwrap();
            timeline.complete(call, None);
            let export = timeline.finish().unwrap();
            assert_eq!(export.live_groups.len(), 1);
            assert_eq!(export.dropped_live_groups, 0);
            assert_eq!(export.live_groups[0].wal_position, Some(4096));
            assert!(
                export.records[0]
                    .events
                    .windows(2)
                    .all(|events| events[0].elapsed_ns <= events[1].elapsed_ns)
            );
        }
    }

    #[test]
    fn leased_window_releases_receipts_in_frame_order_when_completions_reorder() {
        for (tier, sync) in [
            (DurabilityTier::Native, LeasedSync::PerFrameFua),
            (DurabilityTier::Synchronous, LeasedSync::PerFrameFua),
            (DurabilityTier::Synchronous, LeasedSync::GroupFlush),
        ] {
            let fixture = Fixture::new(tier, 32768);
            let mut client = fixture.writer_without_ahead();
            client.live_config.as_mut().unwrap().leased_sync = sync;
            client.leased_test_set_window(4);
            client.leased_test_hold_completions(&[1]);
            let timeline = WriteTimeline::default();
            timeline.arm(4, 1).unwrap();
            let mut calls = Vec::new();
            for sequence in 0..4 {
                let batch = fixture.batch(sequence);
                let call = timeline.begin(sequence).unwrap();
                timeline.operation(call, batch.id().operation);
                timeline.batch(batch.id().operation, batch.version());
                calls.push(call);
                assert!(
                    matches!(client.submit_leased_group(1, vec![batch], &timeline, 2, true).unwrap(),
                    LeasedSubmit::Submitted(id) if id == sequence + 1)
                );
            }
            let mut released = Vec::new();
            progress_until(&mut client, &mut released, &timeline, |client| {
                client
                    .live_leased
                    .flights
                    .iter()
                    .skip(1)
                    .all(|flight| flight.completed)
            });
            assert!(released.is_empty());
            let current = client.live_leased.current.as_ref().unwrap();
            assert_eq!((current.cursor, current.written_end), (16384, 0));
            assert!(
                matches!(client.submit_leased_group(1, vec![fixture.batch(4)], &timeline, 2, true).unwrap(),
                LeasedSubmit::Deferred(batches) if batches.len() == 1)
            );
            if sync == LeasedSync::GroupFlush {
                client
                    .live_writer
                    .as_mut()
                    .unwrap()
                    .io
                    .hold_completions(&[5]);
                progress_until(&mut client, &mut released, &timeline, |client| {
                    client.live_leased.flush_in_flight.is_some()
                });
                assert_eq!(client.live_leased.flush_in_flight.unwrap().1, 4);
                assert!(released.len() <= 1); // The first frame alone may use FUA.
                assert!(
                    client
                        .live_leased
                        .flights
                        .iter()
                        .all(|flight| flight.completed)
                );
            }
            client.leased_test_release_held();
            client
                .drain_leased_flights(&mut released, Some(&timeline))
                .unwrap();
            assert_eq!(
                released.iter().map(|entry| entry.0).collect::<Vec<_>>(),
                vec![1, 2, 3, 4]
            );
            assert_eq!(
                client.live_leased.current.as_ref().unwrap().written_end,
                16384
            );
            client
                .settle_leased_extent(&mut Vec::new(), Some(&timeline))
                .unwrap();
            let owner = fixture.node.log_for_test(1);
            assert_eq!(owner.receipt_count(), 4);
            for (sequence, (_, receipts)) in released.iter().enumerate() {
                let batch = fixture.batch(sequence as u64);
                let reference = batch
                    .record_reference(receipts[0].appended().address(), 0)
                    .unwrap();
                assert_eq!(owner_read(&fixture, &reference), batch.mutations());
            }
            for call in calls {
                timeline.complete(call, None);
            }
            let export = timeline.finish().unwrap();
            for (sequence, group) in export.live_groups.iter().enumerate() {
                assert_eq!(group.wal_position, Some(sequence as u64 * 4096));
                assert_eq!(group.file_offset, group.wal_position);
                assert_eq!(group.frame_len, Some(4096));
                assert_eq!(group.wal_file, Some(0));
                assert_eq!(group.namespace_block_size, Some(4096));
            }
        }
    }

    fn group_flush_writer(fixture: &Fixture) -> Client {
        let mut client = fixture.writer_without_ahead();
        client.live_config.as_mut().unwrap().leased_sync = LeasedSync::GroupFlush;
        client
    }

    #[test]
    fn group_flush_defaults_use_fua_for_small_frames_and_flush_large_frames() {
        let timeline = WriteTimeline::default();
        for value_bytes in [1024, 65536] {
            let temp = tempdir::TempDir::new("leased-default-sync").unwrap();
            let mut config = tcp::tests::config(&temp.path().join("node"));
            config.partition.fragment_bytes = 256 * 1024;
            config.partition.wal_file_bytes = 512 * 1024;
            config.partition.batch_limits.max_encoded_bytes = 64 * 1024 + 1024;
            config.partition.group_limits.max_encoded_bytes = 128 * 1024;
            let fixture = Fixture::open(
                DurabilityTier::Synchronous,
                config,
                temp,
                Duration::from_secs(10),
            );
            let mut client = fixture.writer_without_ahead();
            client.live_config.as_mut().unwrap().leased_sync = LeasedSync::default();
            client
                .live_config
                .as_mut()
                .unwrap()
                .leased_flush_min_frame_bytes =
                LiveClientConfig::default_leased_flush_min_frame_bytes();
            let original = fixture.batch(0);
            let batch = tcp::CompleteBatch::new(
                &crate::key_shape::KeyShape::from_yaml(&fixture.config.shape_yaml).unwrap(),
                original.id(),
                original.version(),
                vec![crate::distributed::codec::Mutation::Put {
                    keyspace: crate::key_shape::KeySpace(0),
                    key: b"key1"[..].into(),
                    value: vec![1; value_bytes].into(),
                }],
                fixture.config.partition.batch_limits,
            )
            .unwrap();
            assert!(matches!(
                client
                    .submit_leased_group(1, vec![batch], &timeline, 2, false)
                    .unwrap(),
                LeasedSubmit::Submitted(_)
            ));
            assert_eq!(
                client.live_leased.flights[0].durable_at_completion,
                value_bytes < 65536
            );
            let mut released = Vec::new();
            client.drain_leased_flights(&mut released, None).unwrap();
            assert_eq!(released.len(), 1);
            let counts = fixture.metrics.snapshot().counts;
            assert_eq!(counts.fua_writes, u64::from(value_bytes < 65536));
            assert_eq!(counts.leased_flushes, u64::from(value_bytes == 65536));
            client.close_leased_extent(&mut Vec::new(), None).unwrap();
        }
    }

    #[test]
    fn group_flush_polled_writes_use_fua_with_queued_work() {
        let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
        let mut client = group_flush_writer(&fixture);
        let config = client.live_config.as_mut().unwrap();
        config.polled_writes = true;
        config.queue_geometry.poll_queues = 1;
        config.leased_flush_min_frame_bytes = 0;
        config.validate().unwrap();
        // The local fixture's interrupt ring makes policy coverage independent
        // of IOPOLL kernel support; the real-polled test covers the kernel route.
        client.leased_test_hold_completions(&[1, 2]);
        let timeline = WriteTimeline::default();
        for sequence in 0..2 {
            client
                .submit_leased_group(1, vec![fixture.batch(sequence)], &timeline, 2, false)
                .unwrap();
        }
        assert!(
            client
                .live_leased
                .flights
                .iter()
                .all(|flight| flight.durable_at_completion)
        );
        client.leased_test_release_held();
        let mut released = Vec::new();
        client.drain_leased_flights(&mut released, None).unwrap();
        assert_eq!(released.len(), 2);
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.fua_writes, 2);
        assert_eq!(counts.leased_flushes, 0);
    }

    #[test]
    fn group_flush_lone_frame_uses_fua_without_namespace_flush() {
        let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
        let mut client = group_flush_writer(&fixture);
        let timeline = WriteTimeline::default();
        client
            .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &timeline, 2)
            .unwrap();
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.fua_writes, 1);
        assert_eq!(counts.leased_flushes, 0);
        assert_eq!(counts.leased_frames, 1);
    }

    /// Synchronous leased writes: the acknowledgement of a per-frame FUA write
    /// is released only after that write's completion, and never after a
    /// failed one. The covering-flush form of the same property is
    /// `group_flush_threshold_selects_fua_or_covering_flush_with_queued_work`
    /// and `group_flush_failure_and_timeout_poison_all_unreleased_flights`.
    /// (This replaced the owner-sync check of the removed Reserved route in
    /// `higher_live_fence_joins_only_the_old_syncer_and_keeps_noop_fences_safe`.)
    #[test]
    fn synchronous_leased_ack_follows_its_fua_completion_and_never_a_failed_one() {
        for failed in [false, true] {
            let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
            let mut client = fixture.writer_without_ahead();
            assert_eq!(
                client.live_config.as_ref().unwrap().leased_sync,
                LeasedSync::PerFrameFua
            );
            let timeline = WriteTimeline::default();
            client.leased_test_hold_completions(&[1]);
            if failed {
                client
                    .live_writer
                    .as_mut()
                    .unwrap()
                    .io
                    .override_completion_result(1, -libc::EIO);
            }
            assert!(matches!(
                client
                    .submit_leased_group(1, vec![fixture.batch(0)], &timeline, 2, false)
                    .unwrap(),
                LeasedSubmit::Submitted(1)
            ));
            assert!(client.live_leased.flights[0].durable_at_completion);
            // The write SQE itself carried the data-sync (FUA) flag; the
            // `fua_writes` metric only records the chosen policy.
            assert_eq!(
                client
                    .live_writer
                    .as_ref()
                    .unwrap()
                    .io
                    .stats()
                    .data_sync_writes,
                1,
                "the Synchronous leased write was not submitted with FUA"
            );
            let mut released = Vec::new();
            // Bounded: the FUA write's CQE is reaped but held, and no
            // acknowledgement may be released while it is held.
            let deadline = Instant::now() + Duration::from_secs(5);
            while client.live_writer.as_ref().unwrap().io.outstanding() != 0 {
                assert!(
                    Instant::now() < deadline,
                    "held FUA write CQE was not reaped within 5 s"
                );
                client.wait_leased_flights(false).unwrap();
                client
                    .progress_leased_flights(&mut released, Some(&timeline))
                    .unwrap();
            }
            assert!(
                released.is_empty(),
                "a Synchronous acknowledgement was released while its FUA completion was held"
            );
            assert!(!client.live_leased.flights[0].completed);
            client.leased_test_release_held();
            if failed {
                assert!(client.drain_leased_flights(&mut released, None).is_err());
                assert!(
                    released.is_empty(),
                    "an acknowledgement was released for a failed FUA write"
                );
                assert!(client.live_write_failed);
            } else {
                // Bounded: once the FUA completion is delivered, the
                // acknowledgement must follow without a covering flush.
                let deadline = Instant::now() + Duration::from_secs(5);
                while released.is_empty() {
                    assert!(
                        Instant::now() < deadline,
                        "no acknowledgement within 5 s of the delivered FUA completion"
                    );
                    client.wait_leased_flights(false).unwrap();
                    client
                        .progress_leased_flights(&mut released, Some(&timeline))
                        .unwrap();
                }
                assert_eq!(released.len(), 1);
                let counts = fixture.metrics.snapshot().counts;
                assert_eq!(counts.fua_writes, 1);
                assert_eq!(counts.leased_flushes, 0);
            }
        }
    }

    #[test]
    fn group_flush_window_of_one_uses_fua_with_queued_work() {
        let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
        let mut client = group_flush_writer(&fixture);
        client.live_config.as_mut().unwrap().leased_write_window = Some(1);
        let timeline = WriteTimeline::default();
        assert!(matches!(
            client
                .submit_leased_group(1, vec![fixture.batch(0)], &timeline, 2, false)
                .unwrap(),
            LeasedSubmit::Submitted(1)
        ));
        let mut released = Vec::new();
        client
            .drain_leased_flights(&mut released, Some(&timeline))
            .unwrap();
        assert_eq!(released.len(), 1);
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.fua_writes, 1);
        assert_eq!(counts.leased_flushes, 0);
    }

    #[test]
    fn group_flush_threshold_selects_fua_or_covering_flush_with_queued_work() {
        for (threshold, big, fua) in [
            (4097, false, true),
            (4096, false, false),
            (4095, false, false),
            (0, false, false),
            (8193, true, true),
            (8192, true, false),
        ] {
            let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
            let mut client = group_flush_writer(&fixture);
            client
                .live_config
                .as_mut()
                .unwrap()
                .leased_flush_min_frame_bytes = threshold;
            let timeline = WriteTimeline::default();
            client.leased_test_hold_completions(&[1, 2, 3]);
            for sequence in 0..2 {
                let batch = if big {
                    fixture.big_batch(sequence)
                } else {
                    fixture.batch(sequence)
                };
                assert!(matches!(
                    client.submit_leased_group(1, vec![batch], &timeline, 2, false).unwrap(),
                    LeasedSubmit::Submitted(id) if id == sequence + 1
                ));
            }
            assert_eq!(client.live_leased.flights.len(), 2);
            for flight in &client.live_leased.flights {
                assert_eq!(flight.frame_len, if big { 8192 } else { 4096 });
                assert_eq!(flight.durable_at_completion, fua);
            }
            let mut released = Vec::new();
            progress_until(&mut client, &mut released, &timeline, |client| {
                client.live_writer.as_ref().unwrap().io.outstanding() == 0
            });
            client
                .live_writer
                .as_mut()
                .unwrap()
                .io
                .hold_completions(&[3]);
            progress_until(&mut client, &mut released, &timeline, |client| {
                client
                    .live_leased
                    .flights
                    .iter()
                    .all(|flight| flight.completed)
            });
            if fua {
                assert_eq!(released.len(), 2);
                assert!(client.live_leased.flush_in_flight.is_none());
            } else {
                assert!(released.is_empty());
                assert_eq!(client.live_leased.flush_in_flight.unwrap().1, 2);
            }
            client.leased_test_release_held();
            client.drain_leased_flights(&mut released, None).unwrap();
            assert_eq!(released.len(), 2);
            let counts = fixture.metrics.snapshot().counts;
            assert_eq!(counts.fua_writes, if fua { 2 } else { 0 });
            assert_eq!(counts.leased_flushes, if fua { 0 } else { 1 });
            assert_eq!(counts.leased_flush_covered_frames, if fua { 0 } else { 2 });
        }
    }

    #[test]
    fn group_flush_captures_only_the_completed_prefix_and_drain_covers_later_writes() {
        let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
        let mut client = group_flush_writer(&fixture);
        let timeline = WriteTimeline::default();
        client.leased_test_hold_completions(&[2, 3, 4, 5]);
        for sequence in 0..3 {
            assert!(
                matches!(client.submit_leased_group(1, vec![fixture.batch(sequence)], &timeline, 2, false).unwrap(),
                LeasedSubmit::Submitted(id) if id == sequence + 1)
            );
        }
        let mut released = Vec::new();
        progress_until(&mut client, &mut released, &timeline, |client| {
            client.live_leased.flush_in_flight.is_some()
        });
        assert_eq!(client.live_leased.flush_in_flight.unwrap().1, 1);
        assert!(released.is_empty());
        client
            .live_writer
            .as_mut()
            .unwrap()
            .io
            .hold_completions(&[3, 4, 5]);
        progress_until(&mut client, &mut released, &timeline, |client| {
            client.live_leased.flights[1].completed
        });
        assert_eq!(client.live_leased.flush_in_flight.unwrap().1, 1);
        assert!(released.is_empty());
        client
            .live_writer
            .as_mut()
            .unwrap()
            .io
            .hold_completions(&[3, 5]);
        progress_until(&mut client, &mut released, &timeline, |client| {
            client.live_leased.flushed_through_seq == 1
        });
        assert_eq!(
            released.iter().map(|entry| entry.0).collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(client.live_leased.flush_in_flight.unwrap().1, 2);
        client.leased_test_release_held();
        client.drain_leased_flights(&mut released, None).unwrap();
        assert_eq!(
            released.iter().map(|entry| entry.0).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(client.live_leased.flushed_through_seq >= 3);
        assert!(client.live_leased.flush_in_flight.is_none());
        assert_eq!(client.live_writer.as_ref().unwrap().io.outstanding(), 0);
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.leased_flushes, 3);
        assert_eq!(counts.leased_flush_samples, 3);
        assert_eq!(counts.leased_flush_covered_frames, 3);
        assert_eq!(counts.fua_writes, 0);
    }

    #[test]
    fn group_flush_failure_and_timeout_poison_all_unreleased_flights() {
        for timed_out in [false, true] {
            let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
            let mut client = group_flush_writer(&fixture);
            let timeline = WriteTimeline::default();
            client.leased_test_hold_completions(&[3]);
            if !timed_out {
                client
                    .live_writer
                    .as_mut()
                    .unwrap()
                    .io
                    .override_completion_result(3, -libc::EIO);
            }
            for sequence in 0..2 {
                client
                    .submit_leased_group(1, vec![fixture.batch(sequence)], &timeline, 2, false)
                    .unwrap();
            }
            let mut released = Vec::new();
            progress_until(&mut client, &mut released, &timeline, |client| {
                client.live_leased.flush_in_flight.is_some()
            });
            assert!(released.is_empty());
            if timed_out {
                client.live_leased.flush_in_flight.as_mut().unwrap().2 = Instant::now();
            } else {
                client.leased_test_release_held();
            }
            assert!(client.drain_leased_flights(&mut released, None).is_err());
            assert!(released.is_empty());
            assert!(client.live_write_failed);
            assert_eq!(client.live_leased.flights.len(), 2);
            assert_eq!(client.live_leased.current.as_ref().unwrap().written_end, 0);
            assert!(
                client
                    .submit_leased_group(1, vec![fixture.batch(2)], &timeline, 2, true)
                    .is_err()
            );
            assert!(fixture.metrics.snapshot().counts.unknown_writes > 0);
        }
    }

    #[test]
    fn group_flush_sequence_stays_monotone_across_queue_rebind() {
        let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
        let mut client = group_flush_writer(&fixture);
        let timeline = WriteTimeline::default();
        let mut released = Vec::new();
        for sequence in 0..4 {
            let LeasedSubmit::Submitted(id) = client
                .submit_leased_group(1, vec![fixture.batch(sequence)], &timeline, 2, false)
                .unwrap()
            else {
                panic!("unexpected deferral");
            };
            assert!(id > client.live_leased.flushed_through_seq);
            client.drain_leased_flights(&mut released, None).unwrap();
            assert!(client.live_leased.flushed_through_seq >= id);
            // Force a new queue after each durable cut, like a file rebind.
            let writer = client.live_writer.take().unwrap();
            assert_eq!(writer.io.outstanding(), 0);
            drop(writer);
        }
        assert_eq!(released.len(), 4);
        assert_eq!(fixture.metrics.snapshot().counts.leased_flushes, 4);
        client.close_leased_extent(&mut released, None).unwrap();
        assert_eq!(fixture.node.log_for_test(1).receipt_count(), 4);
    }

    #[test]
    fn leased_window_crash_recovers_only_contiguous_prefix() {
        use std::os::unix::fs::FileExt;
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let fixture = Fixture::new(tier, 32768);
            let mut client = fixture.writer_without_ahead();
            let timeline = WriteTimeline::default();
            client.leased_test_hold_completions(&[2]);
            client.leased_test_fail_flight(2, -libc::EIO);
            for sequence in 0..4 {
                assert!(matches!(
                    client
                        .submit_leased_group(1, vec![fixture.batch(sequence)], &timeline, 2, true)
                        .unwrap(),
                    LeasedSubmit::Submitted(_)
                ));
            }
            let mut released = Vec::new();
            progress_until(&mut client, &mut released, &timeline, |client| {
                client
                    .live_leased
                    .flights
                    .iter()
                    .skip(1)
                    .all(|flight| flight.completed)
                    && client.live_writer.as_ref().unwrap().io.outstanding() == 0
            });
            assert_eq!(released.len(), 1);
            assert_eq!(released[0].0, 1);
            let first = released[0].1[0];
            let current = client.live_leased.current.as_ref().unwrap();
            assert_eq!(current.written_end, 4096);
            // The injected failed CQE does not alter real DMA. Tear the retired frame's CRC.
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&current.lease.extent.segment.path)
                .unwrap();
            file.write_all_at(&[0xff; 4], 4100).unwrap();
            file.sync_data().unwrap();
            drop(file);
            drop(client); // No adoption, close, or receipt release for frames 2..4.
            let mut recovery = fixture.client();
            recovery.fence_to(2).unwrap();
            let log = fixture
                .node
                .inventory
                .lock()
                .live_recovery_log(2, 1)
                .unwrap()
                .unwrap();
            assert_eq!(log.receipt_count(), 1);
            let batch = fixture.batch(0);
            let reference = batch
                .record_reference(first.appended().address(), 0)
                .unwrap();
            assert_eq!(log.read_record(&reference).unwrap(), batch.mutations()[0]);
        }
    }

    #[test]
    fn leased_window_extent_switch_drains_before_skip_marker() {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let fixture = Fixture::new(tier, 6 * 4096);
            let mut client = fixture.writer_without_ahead();
            let timeline = WriteTimeline::default();
            let mut released = Vec::new();
            client.leased_test_hold_completions(&[1]);
            for sequence in 0..4 {
                assert!(matches!(
                    client
                        .submit_leased_group(1, vec![fixture.batch(sequence)], &timeline, 2, true)
                        .unwrap(),
                    LeasedSubmit::Submitted(_)
                ));
            }
            progress_until(&mut client, &mut released, &timeline, |client| {
                client
                    .live_leased
                    .flights
                    .iter()
                    .skip(1)
                    .all(|flight| flight.completed)
            });
            assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 0);
            client.leased_test_release_held();
            client.drain_leased_flights(&mut released, None).unwrap();
            // A two-block frame leaves one block. The next two-block frame needs a skip.
            for sequence in 4..7 {
                let batch = if sequence < 5 {
                    fixture.batch(sequence)
                } else {
                    fixture.big_batch(sequence)
                };
                loop {
                    match client
                        .submit_leased_group(1, vec![batch.clone()], &timeline, 2, true)
                        .unwrap()
                    {
                        LeasedSubmit::Submitted(_) => break,
                        LeasedSubmit::Deferred(_) => {
                            if sequence == 5 {
                                assert_eq!(
                                    fixture.metrics.snapshot().counts.leased_skip_markers,
                                    0
                                );
                            }
                            client.drain_leased_flights(&mut released, None).unwrap();
                        }
                    }
                }
            }
            client.drain_leased_flights(&mut released, None).unwrap();
            for sequence in 7..10 {
                let batch = fixture.big_batch(sequence);
                loop {
                    match client
                        .submit_leased_group(1, vec![batch.clone()], &timeline, 2, true)
                        .unwrap()
                    {
                        LeasedSubmit::Submitted(_) => break,
                        LeasedSubmit::Deferred(_) => {
                            client.drain_leased_flights(&mut released, None).unwrap()
                        }
                    }
                }
            }
            client.close_leased_extent(&mut released, None).unwrap();
            assert_eq!(released.len(), 10);
            let log = fixture.node.log_for_test(1);
            assert_eq!(log.receipt_count(), 10);
            assert!(fixture.metrics.snapshot().counts.leased_skip_markers > 0);
            for (sequence, (_, receipts)) in released.into_iter().enumerate() {
                let batch = if sequence < 5 {
                    fixture.batch(sequence as u64)
                } else {
                    fixture.big_batch(sequence as u64)
                };
                let reference = batch
                    .record_reference(receipts[0].appended().address(), 0)
                    .unwrap();
                assert_eq!(owner_read(&fixture, &reference), batch.mutations());
            }
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl super::Client {
    pub(crate) fn bench_local_live_fixture(&mut self, buffered: bool) {
        self.live_write_local_fixture = true;
        // Relaxed: fixture selection precedes queue creation in serial tests.
        BENCH_LOCAL_BUFFERED.store(buffered, std::sync::atomic::Ordering::Relaxed);
    }
}
#[cfg(test)]
impl super::Client {
    pub(crate) fn leased_test_frontier(&self) -> (u64, bool, bool) {
        (
            self.live_leased
                .current
                .as_ref()
                .map_or(0, |current| current.written_end),
            self.live_leased.current.is_some(),
            self.live_leased.pending.is_some(),
        )
    }
    /// Passive byte frontiers: CQ-complete, released, and synchronous-covered.
    pub(crate) fn leased_test_completion_prefixes(&self) -> (u64, u64, Option<u64>) {
        use crate::distributed::durability::DurabilityTier;
        let released = self
            .live_leased
            .current
            .as_ref()
            .or(self.live_leased.closing.as_ref())
            .map_or(0, |extent| extent.written_end);
        let mut completed = released;
        for flight in self
            .live_leased
            .flights
            .iter()
            .take_while(|flight| flight.completed)
        {
            completed = flight.end;
        }
        let durable = self.live_config.as_ref().and_then(|config| {
            (config.leased_durability == DurabilityTier::Synchronous).then(|| {
                self.live_leased
                    .flights
                    .iter()
                    .take_while(|flight| {
                        flight.completed
                            && (flight.durable_at_completion
                                || flight.id <= self.live_leased.flushed_through_seq)
                    })
                    .last()
                    .map_or(released, |flight| flight.end)
            })
        });
        (completed, released, durable)
    }
    pub(crate) fn leased_test_ahead_state(&self) -> (bool, bool) {
        (
            self.live_leased.closing.is_some(),
            self.live_leased.next.is_some(),
        )
    }
    /// Lose the next prelease reply: the owner keeps the reservation and this
    /// client never records it, as a session break before the reply does.
    pub(crate) fn leased_test_drop_next_prelease_reply(&mut self) {
        self.live_leased.drop_prelease_reply = true;
    }
    /// The prelease reply a test lost, if one was lost.
    pub(crate) fn leased_test_lost_prelease(&self) -> Option<super::LiveLeased> {
        self.live_leased.lost_prelease.clone()
    }
    pub(crate) fn leased_test_enable_ahead(&mut self) {
        self.live_config.as_mut().unwrap().lease_ahead = true;
    }
    pub(crate) fn leased_test_largest_close_profile(&self) -> super::AdoptProfile {
        self.live_leased.largest_close_profile
    }
    pub(crate) fn leased_test_group_flush(&mut self) {
        self.live_config.as_mut().unwrap().leased_sync = LeasedSync::GroupFlush;
    }
    pub(crate) fn leased_test_set_window(&mut self, window: usize) {
        self.live_config.as_mut().unwrap().leased_write_window = Some(window);
        self.live_leased.flights.reserve(window);
        self.live_leased.completions.reserve(window);
    }
    pub(crate) fn leased_test_no_window(&mut self, max_inflight_requests: usize) {
        let mut config = self.live_config.clone().unwrap();
        config.leased_write_window = None;
        config.max_inflight_requests = max_inflight_requests;
        self.configure_live_nvme(
            &config,
            crate::distributed::remote_db::RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY,
        )
        .unwrap();
    }
    pub(crate) fn leased_test_single_batch_groups(&mut self) {
        self.expected.group_limits.max_batches = 1;
    }
    pub(crate) fn leased_test_hold_next_completion(&mut self) {
        let id = self.live_leased.next_flight_id.checked_add(1).unwrap();
        self.leased_test_hold_completions(&[id]);
    }
    pub(crate) fn leased_test_hold_completions(&mut self, ids: &[u64]) {
        if self.live_leased.current.is_none() {
            let leased = self.lease_live_extent(1).unwrap();
            self.install_leased_extent(leased).unwrap();
        }
        self.ensure_leased_queue().unwrap();
        self.live_writer.as_mut().unwrap().io.hold_completions(ids);
    }
    pub(crate) fn leased_test_release_held(&mut self) {
        self.live_writer.as_mut().unwrap().io.release_held();
    }
    /// Write CQEs the queue has reaped, held ones included. Published
    /// counters advance only when a completion is delivered.
    pub(crate) fn leased_test_reaped_writes(&self) -> u64 {
        self.live_writer
            .as_ref()
            .map_or(0, |writer| writer.io.stats().completed_write_ops)
    }
    pub(crate) fn leased_test_fail_flight(&mut self, id: u64, result: i32) {
        self.live_writer
            .as_mut()
            .unwrap()
            .io
            .override_completion_result(id, result);
    }
    pub(crate) fn leased_test_fail_write(&mut self, result: i32) {
        self.live_leased.next_write_result = Some(result);
    }
    /// Fail the next submission before any SQE, as a lost controller does.
    pub(crate) fn leased_test_fail_submit(&mut self, errno: i32) {
        self.live_leased.next_submit_result = Some(errno);
    }
    /// Override the next group flush's CQE with `result`, a raw negative errno.
    pub(crate) fn leased_test_fail_flush(&mut self, result: i32) {
        self.live_leased.next_flush_result = Some(result);
    }
    /// `(flushed through sequence, flush id in flight)`.
    pub(crate) fn leased_test_flush_state(&self) -> (u64, Option<u64>) {
        (
            self.live_leased.flushed_through_seq,
            self.live_leased.flush_in_flight.map(|(id, _, _)| id),
        )
    }
    /// Whether each submitted write carried `LiveWriteSync::Data`, in order.
    pub(crate) fn leased_test_write_syncs(&self) -> Vec<bool> {
        self.live_writer
            .as_ref()
            .map(|writer| writer.io.submitted_data_sync().to_vec())
            .unwrap_or_default()
    }
    /// Enable the write resume with no reaper: a local-file queue owns no
    /// kernel controller, so the fixture path drains it inline.
    pub(crate) fn leased_test_resume(&mut self, enabled: bool) {
        self.set_resume_writes(enabled, None);
    }
    /// Shrink the retained-frame budget so one frame fills it.
    pub(crate) fn leased_test_set_resume_retention(&mut self, bytes: usize) {
        self.live_leased.resume_retain_bytes = Some(bytes);
    }
    /// Replace the window a session resume grants the outstanding work, so a
    /// test reaches the next expiry without waiting on it. `None` keeps
    /// `io_timeout_ms`.
    pub(crate) fn leased_test_set_retime_window(&mut self, window: std::time::Duration) {
        self.live_leased.retime_window = Some(window);
    }
    /// `(retained frames, retained bytes)`.
    pub(crate) fn leased_test_retained(&self) -> (usize, usize) {
        (
            self.live_leased.retained.len(),
            self.live_leased.retained_bytes,
        )
    }
    /// Extents the last resumes probed, in probe order, as `"closing"` or
    /// `"current"`.
    pub(crate) fn leased_test_resume_targets(&self) -> Vec<&'static str> {
        self.live_leased
            .resume_targets
            .iter()
            .map(|target| match target {
                PendingTarget::Closing => "closing",
                PendingTarget::Current | PendingTarget::Prelease => "current",
            })
            .collect()
    }
    /// `(grant token, absolute start, length, completion id)` of every frame a
    /// resume resent, in submission order.
    pub(crate) fn leased_test_rewrites(&self) -> Vec<([u8; 16], u64, usize, u64)> {
        self.live_leased
            .rewrites
            .iter()
            .map(|rewrite| (rewrite.token, rewrite.start, rewrite.len, rewrite.id))
            .collect()
    }
    /// Every completion id a write command used, in submission order.
    pub(crate) fn leased_test_submitted_ids(&self) -> &[u64] {
        &self.live_leased.submitted_ids
    }
    /// Steps each attempt of the last resume completed, in attempt order.
    pub(crate) fn leased_test_attempt_steps(&self) -> &[u32] {
        &self.live_leased.attempt_steps
    }
    /// The attempt and completed-step count at which the test lapse landed.
    pub(crate) fn leased_test_lapse_at(&self) -> Option<(u32, u32)> {
        self.live_leased.lapse_at
    }
    /// Rounds a resume re-probed a close the owner was still executing.
    pub(crate) fn leased_test_close_reprobes(&self) -> u32 {
        self.live_leased.close_reprobes
    }
    /// Notify this channel once per declaration close the owner reports as in
    /// progress, so a test can release a paused owner close at that moment.
    pub(crate) fn leased_test_notify_close_in_progress(
        &mut self,
        notify: std::sync::mpsc::SyncSender<()>,
    ) {
        self.live_leased.close_in_progress = Some(notify);
    }
    /// Announce the declaration step on `entered`, then wait on `release`
    /// before it runs, so a test can place the owner in a state that is
    /// otherwise a race to enter.
    pub(crate) fn leased_test_pause_before_declaration(
        &mut self,
        entered: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        self.live_leased.declaration_gate = Some((entered, release));
    }
    /// Lapse `validity` once `steps` resume steps of one resume have completed,
    /// so the test decides the step that must observe the lapse.
    pub(crate) fn leased_test_lapse_lease_after_step(
        &mut self,
        steps: u32,
        validity: std::sync::Arc<crate::distributed::remote_db::coordinator_lease::LeaseValidity>,
    ) {
        self.live_leased.lapse_lease_after = Some((steps, validity));
    }
    /// Lapse `validity` once `after` frames of one rewrite have been resent, so
    /// the test lands the lapse between two frames of the same rewrite.
    pub(crate) fn leased_test_lapse_lease_after_rewrite(
        &mut self,
        after: u32,
        validity: std::sync::Arc<crate::distributed::remote_db::coordinator_lease::LeaseValidity>,
    ) {
        self.live_leased.lapse_lease_after_rewrite = Some((after, validity));
    }
    /// `(offset, len)` of every write this queue accepted, in order.
    pub(crate) fn leased_test_write_submissions(&self) -> Vec<(u64, usize)> {
        self.live_writer
            .as_ref()
            .map(|writer| writer.io.submitted_writes().to_vec())
            .unwrap_or_default()
    }
    /// `(extent start, cursor, written end, adopted end)` of the current extent.
    pub(crate) fn leased_test_extent_ends(&self) -> Option<(u64, u64, u64, u64)> {
        self.live_leased.current.as_ref().map(|extent| {
            (
                extent.lease.extent.start,
                extent.cursor,
                extent.written_end,
                extent.adopted.end,
            )
        })
    }
    pub(crate) fn leased_test_timeout_write(&mut self) {
        if let Some(config) = &mut self.live_config {
            // Not 1 ms: the deadline must still admit the submission when a
            // parallel debug test competes for the CPU. The delayed write then
            // expires it after the SQE is in the ring, which is the premise.
            config.io_timeout_ms = 50;
        }
        self.live_leased.delay_write = true;
    }
    pub(crate) fn leased_test_pause_drain(
        &mut self,
        entered: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        self.live_leased.drain_pause = Some(WriteDrainPause { entered, release });
    }
    pub(crate) fn leased_test_note_flight_adoption_wait(&mut self) {
        self.live_leased.flight_adoption_waits += 1;
    }
    pub(crate) fn leased_test_flight_adoption_waits(&self) -> u64 {
        self.live_leased.flight_adoption_waits
    }
    pub(crate) fn leased_test_corrupt_adoption(&mut self, bad_token: bool) {
        if bad_token {
            self.live_leased.invalidate_adoption_token = true;
        } else {
            self.live_leased.corrupt_next_digest = true;
        }
    }
}

#[cfg(test)]
mod leased_rollover_tests {
    use super::*;
    use crate::distributed::durability::DurabilityTier;
    use crate::distributed::write_timeline::WriteTimeline;

    #[test]
    fn lease_ahead_switch_into_a_new_wal_file_rebinds_the_writer_queue() {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let fixture = leased_tests::Fixture::new(tier, 16384);
            let mut client = fixture.writer();
            client.leased_test_set_window(4);
            let mut released = Vec::with_capacity(9);
            let mut first_binding = None;
            let timeline = WriteTimeline::default();
            for sequence in 0..9 {
                let mut batches = vec![fixture.batch(sequence)];
                loop {
                    match client
                        .submit_leased_group(1, batches, &timeline, 2, true)
                        .unwrap()
                    {
                        LeasedSubmit::Submitted(_) => break,
                        LeasedSubmit::Deferred(deferred) => {
                            batches = deferred;
                            client.drain_leased_flights(&mut released, None).unwrap();
                            client.wait_closing_extent(true).unwrap();
                        }
                    }
                }
                if sequence == 0 {
                    first_binding = Some(client.live_writer.as_ref().unwrap().target.uuid);
                }
            }
            client.drain_leased_flights(&mut released, None).unwrap();
            let receipts: Vec<_> = released
                .into_iter()
                .map(|(_, receipts)| receipts[0])
                .collect();
            assert_eq!(fixture.metrics.snapshot().counts.max_leased_in_flight, 4);
            let current = client.live_leased.current.as_ref().unwrap();
            assert_eq!(current.lease.extent.segment.file_index, 1);
            assert_eq!(current.lease.extent.start, 32768);
            let writer = client.live_writer.as_ref().unwrap();
            assert!(writer.matches(&current.lease.binding));
            assert_ne!(Some(writer.target.uuid), first_binding);
            let closing = client.live_leased.closing.as_ref().unwrap();
            assert_eq!(closing.lease.extent.segment.file_index, 0);
            // Old-file frames 0..8 precede the new-file frame 8 in the WAL.
            let appended: Vec<_> = receipts.iter().map(|receipt| receipt.appended()).collect();
            assert!(
                appended[..8]
                    .iter()
                    .all(|batch| batch.address().offset < 32768)
            );
            assert!(appended[8].address().offset >= 32768);
            // Drive the background close, not a synchronous settle. Its reply
            // must adopt every old-file frame while the new queue stays bound.
            client.wait_closing_extent(false).unwrap();
            assert!(client.live_leased.closing.is_none());
            assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 0);
            assert_eq!(fixture.metrics.snapshot().counts.extent_switch_waits, 0);
            assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 0);
            // The close reply covered the whole old-file prefix, and the close made
            // the new-file extent the owner's open extent. The owner's opportunistic
            // self-adoption (`self_adopt_live_extent`, run by the background syncer's
            // `persist_appended` and by flush barriers) scans only that open extent,
            // so a syncer run that is late may already have adopted the complete
            // new-file frame here. One consistent page snapshot (taken under the
            // receipt authority's lock) must be the client's receipts in WAL order:
            // every old-file frame exactly once, then at most the new-file frame.
            let owner = fixture.node.log_for_test(1);
            let adopted = owner.receipt_page(0, 16).unwrap();
            assert!(
                matches!(adopted.len(), 8 | 9),
                "{tier:?}: owner adopted {} frames",
                adopted.len()
            );
            assert_eq!(adopted[..], appended[..adopted.len()], "{tier:?}");
            for (sequence, receipt) in receipts.into_iter().enumerate() {
                let batch = fixture.batch(sequence as u64);
                let reference = batch
                    .record_reference(receipt.appended().address(), 0)
                    .unwrap();
                assert_eq!(
                    leased_tests::owner_read(&fixture, &reference),
                    batch.mutations()
                );
            }
            client.close_leased_extent(&mut Vec::new(), None).unwrap();
            // Closing the new-file extent adopts its frame if the syncer or the
            // reads above did not, and never adopts it a second time.
            let owner = fixture.node.log_for_test(1);
            assert_eq!(owner.receipt_count(), 9, "{tier:?}");
            assert_eq!(owner.receipt_page(0, 16).unwrap(), appended, "{tier:?}");
        }
    }

    /// per_owner: the queue rebinds to WAL member 2 on the controller that was
    /// connected for member 1. The writer route must name member 2, which the
    /// kernel qualification's member-transition proof requires.
    #[test]
    fn per_owner_rebind_route_names_the_opened_member() {
        use crate::distributed::nvmeof::live_connect::{LiveConnectTarget, LiveConnectionRegistry};
        use crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess;
        use crate::distributed::nvmeof::live_pool::LiveControllerPool;
        let fixture = leased_tests::Fixture::single_partition(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        client.leased_test_set_window(4);
        let pool_dir = tempdir::TempDir::new("per-owner-route-pool").unwrap();
        let pool = std::sync::Arc::new(
            LiveControllerPool::new(
                LiveConnectionRegistry::open(
                    &pool_dir.path().join("registry"),
                    crate::distributed::nvmeof::live_connect::RegistryMode::Retire,
                )
                .unwrap(),
                8,
                4,
            )
            .unwrap(),
        );
        client.set_live_pool(pool.clone());
        // The ordinary-file queue caches both members under one shared lease
        // and records no route; drive it into member 2.
        let timeline = WriteTimeline::default();
        let mut released = Vec::new();
        for sequence in 0..9 {
            let mut batches = vec![fixture.batch(sequence)];
            loop {
                match client
                    .submit_leased_group(1, batches, &timeline, 2, true)
                    .unwrap()
                {
                    LeasedSubmit::Submitted(_) => break,
                    LeasedSubmit::Deferred(deferred) => {
                        batches = deferred;
                        client.drain_leased_flights(&mut released, None).unwrap();
                        client.wait_closing_extent(true).unwrap();
                    }
                }
            }
        }
        client.drain_leased_flights(&mut released, None).unwrap();
        assert!(fixture.metrics.snapshot().routes.is_empty());
        let lease = &client.live_leased.current.as_ref().unwrap().lease;
        assert_eq!(lease.extent.segment.file_index, 1);
        let (binding, path) = (lease.binding.clone(), lease.extent.segment.path.clone());
        let config = client.live_config.clone().unwrap();
        pool.cache_local_member_for_test(
            &LiveConnectTarget {
                nqn: binding.nqn.clone(),
                uuid: binding.uuid,
                nsid: binding.nsid,
                bytes: binding.bytes,
                block_size: binding.block_size,
                address: binding.address.clone(),
                service: binding.service,
                transport: binding.transport,
                geometry: config.queue_geometry,
            },
            LiveNamespaceAccess::ReadWrite,
            &path,
        )
        .unwrap();
        // Rebind through the controller-pool path the kernel route uses.
        client.drain_live_queue().unwrap();
        client.live_write_local_fixture = false;
        client.ensure_leased_queue().unwrap();
        let routes = fixture.metrics.snapshot().routes;
        assert_eq!(routes.len(), 1);
        let writer = routes[0].writer.as_ref().unwrap();
        let identity = &routes[0].namespace_identity;
        assert_eq!(writer.file_index, 1);
        assert_eq!(binding.nsid, 2);
        assert_eq!(
            (identity.nqn.as_str(), identity.nsid),
            (binding.nqn.as_str(), 2)
        );
        assert_eq!(identity.uuid, binding.uuid);
        assert_eq!(identity.namespace_path, path);
        client.drain_live_queue().unwrap();
    }

    #[test]
    fn leased_full_extents_roll_into_a_new_file_without_skip_or_stale_binding() {
        let fixture = leased_tests::Fixture::new(DurabilityTier::Synchronous, 16384);
        let mut client = fixture.writer_without_ahead();
        let mut receipts = Vec::with_capacity(9);
        for sequence in 0..9 {
            let batch = fixture.batch(sequence);
            receipts.push(
                client
                    .submit_and_drain_leased_group(1, vec![batch], &WriteTimeline::default(), 2)
                    .unwrap()[0],
            );
        }
        let current = client.live_leased.current.as_ref().unwrap();
        assert_eq!(current.lease.extent.segment.file_index, 1);
        assert_eq!(current.lease.extent.start, 32768);
        assert!(
            client
                .live_writer
                .as_ref()
                .unwrap()
                .matches(&current.lease.binding)
        );
        assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 0);
        assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 2);
        for (sequence, receipt) in receipts.into_iter().enumerate() {
            let batch = fixture.batch(sequence as u64);
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(
                leased_tests::owner_read(&fixture, &reference),
                batch.mutations()
            );
        }
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }

    #[test]
    fn leased_receipts_use_new_epoch_generation_after_clean_close_and_fence() {
        let fixture = leased_tests::Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        let first = fixture.batch(0);
        client
            .submit_and_drain_leased_group(1, vec![first], &WriteTimeline::default(), 2)
            .unwrap();
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
        client.fence_to(2).unwrap();
        let batch = super::super::tests::batch(&fixture.config, 2, 0);
        let receipts = client
            .submit_and_drain_leased_group(2, vec![batch.clone()], &WriteTimeline::default(), 2)
            .unwrap();
        assert_eq!(receipts[0].appended().address().segment_generation, 2);
        client.settle_leased_extent(&mut Vec::new(), None).unwrap();
        assert_eq!(
            client.live_leased.current.as_mut().unwrap().adopted.count,
            1
        );
        let reference = batch
            .record_reference(receipts[0].appended().address(), 0)
            .unwrap();
        assert_eq!(
            fixture
                .node
                .log_for_test(2)
                .read_record(&reference)
                .unwrap(),
            batch.mutations()[0]
        );
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }
}
