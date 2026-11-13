//! Initiator configuration for the live owner-WAL NVMe-oF data plane.

use super::{LiveExportMode, LiveQueueGeometry};
use serde::{Deserialize, Serialize};

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
use crate::distributed::nvmeof::live_metrics::LiveTimingStage;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
use std::time::Instant;

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
static BENCH_LOCAL_BUFFERED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
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

/// Explicit live write contract; never falls back to another route.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveWriteMode {
    #[default]
    Reserved,
    Leased,
}

impl LiveWriteMode {
    pub const fn acknowledgement(
        self,
        tier: crate::distributed::durability::DurabilityTier,
    ) -> &'static str {
        use crate::distributed::durability::DurabilityTier;
        match (self, tier) {
            (Self::Leased, DurabilityTier::Native) => "leased extent: page-cache write",
            (Self::Leased, DurabilityTier::Synchronous) => {
                "leased extent: FUA data sync; owner frontier root asynchronous"
            }
            (Self::Reserved, DurabilityTier::Native) => {
                "reserved frame: owner validated page-cache write"
            }
            (Self::Reserved, DurabilityTier::Synchronous) => {
                "reserved frame: owner WAL and frontier root synchronized"
            }
        }
    }
}

/// Stable identity for one storage owner when several owners share a target VM.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveInitiatorIdentity {
    pub host_nqn: String,
    pub host_id: [u8; 16],
}

/// Bounded initiator resources. On-demand namespace identity comes from the
/// owner. Permanent active-epoch identity is computed from the WAL file index.
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
    pub max_inflight_bytes: usize,
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
    #[serde(default)]
    pub write_mode: LiveWriteMode,
    /// Required only for leased mode; every granted extent must match this tier.
    #[serde(default)]
    pub leased_durability: Option<crate::distributed::durability::DurabilityTier>,
    #[serde(default = "default_inline_reads")]
    pub lease_ahead: bool,
    /// Use IOPOLL for leased writes; requires a controller poll queue.
    #[serde(default)]
    pub polled_writes: bool,
    /// Disable caller-inline reads without changing lane-worker reads.
    #[serde(default = "default_inline_reads")]
    pub inline_reads: bool,
    #[serde(default)]
    pub queue_geometry: LiveQueueGeometry,
    #[serde(default)]
    /// Permanent export requires leased writes; on-demand stays the default.
    pub export_mode: LiveExportMode,
}

fn default_inline_reads() -> bool {
    true
}

impl LiveClientConfig {
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
            max_inflight_bytes: self.max_inflight_bytes,
            connect_timeout_ms: self.connect_timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
            write_mode: self.write_mode,
            leased_durability: self.leased_durability,
            lease_ahead: self.lease_ahead,
            polled_writes: self.polled_writes,
            inline_reads: self.inline_reads,
            queue_geometry: self.queue_geometry,
            export_mode: self.export_mode,
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
        if self.polled_writes && self.write_mode != LiveWriteMode::Leased {
            return Err("polled live writes require leased write mode");
        }
        if self.export_mode != LiveExportMode::OnDemand && self.write_mode != LiveWriteMode::Leased
        {
            return Err("permanent live export requires leased write mode");
        }
        if self.queue_geometry.poll_queues > 0 && !self.inline_reads {
            return Err("polled live reads require inline reads");
        }
        if (self.write_mode == LiveWriteMode::Leased) != self.leased_durability.is_some() {
            return Err("leased_durability is required exactly for leased write mode");
        }
        if self.host_id == [0; 16]
            || !self.host_nqn.starts_with("nqn.")
            || self.host_nqn.len() > 223
            || !self
                .host_nqn
                .bytes()
                .all(|b| b.is_ascii_graphic() && b != b',' && b != b'=')
            || !(1..=32768).contains(&self.max_inflight_requests)
            || self.max_inflight_bytes == 0
            || !(1..=300_000).contains(&self.connect_timeout_ms)
            || !(1..=300_000).contains(&self.io_timeout_ms)
        {
            return Err("invalid live NVMe-oF initiator configuration");
        }
        Ok(())
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
struct WriteTiming<'a> {
    counters: &'a crate::distributed::nvmeof::live_metrics::LiveNvmeCounters,
    stage: crate::distributed::nvmeof::live_metrics::LiveTimingStage,
    started: std::time::Instant,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl Drop for WriteTiming<'_> {
    fn drop(&mut self) {
        self.counters.record_timing(
            self.stage,
            self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            1,
        );
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
struct UnknownWriteOnError<'a> {
    counters: Option<&'a crate::distributed::nvmeof::live_metrics::LiveNvmeCounters>,
    armed: bool,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl Drop for UnknownWriteOnError<'_> {
    fn drop(&mut self) {
        if self.armed
            && let Some(counters) = &self.counters
        {
            counters.record_unknown_write();
        }
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
struct FailedWriteAccounting {
    before: crate::distributed::nvmeof::live_io::LiveIoStats,
    counters: std::sync::Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeCounters>,
}

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
struct WriteDrainPause {
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[derive(Clone, Copy, Debug)]
struct LeasedPrefix {
    end: u64,
    count: u64,
    digest: [u8; 32],
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[derive(Clone, Copy)]
struct PendingAdoption {
    through: u64,
    close: bool,
    lease_next: bool,
    target: PendingTarget,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingTarget {
    Current,
    Closing,
    Prelease,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
struct ExtentState {
    lease: super::live_control::LiveLeased,
    cursor: u64,
    written_end: u64,
    acknowledged: LeasedPrefix,
    adopted: LeasedPrefix,
    declared_through: u64,
    /// At most 1024 boundaries; full current history settles before submission.
    prefixes: std::collections::VecDeque<LeasedPrefix>,
}

/// The existing per-owner worker owns this state, with no additional lock.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[derive(Default)]
pub(super) struct LeasedWriter {
    current: Option<ExtentState>,
    closing: Option<ExtentState>,
    next: Option<super::live_control::LiveLeased>,
    receipt_config: Option<super::PartitionConfig>,
    pending: Option<PendingAdoption>,
    pending_started: Option<Instant>,
    /// Two deques, allocated at configuration and recycled after validated close.
    prefix_buffers: [std::collections::VecDeque<LeasedPrefix>; 2],
    #[cfg(test)]
    next_write_result: Option<i32>,
    #[cfg(test)]
    delay_write: bool,
    #[cfg(test)]
    corrupt_next_digest: bool,
    #[cfg(test)]
    invalidate_adoption_token: bool,
    #[cfg(test)]
    drain_pause: Option<WriteDrainPause>,
    #[cfg(test)]
    command_adoption_waits: u64,
    #[cfg(test)]
    largest_close_profile: super::live_control::AdoptProfile,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl LiveWriteQueue {
    fn matches(
        &self,
        binding: &crate::distributed::nvmeof::live_target::LiveTargetBinding,
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
            && target.spdk == binding.spdk
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
fn record_write_io(
    counters: &crate::distributed::nvmeof::live_metrics::LiveNvmeCounters,
    before: &crate::distributed::nvmeof::live_io::LiveIoStats,
    after: &crate::distributed::nvmeof::live_io::LiveIoStats,
) {
    use crate::distributed::nvmeof::live_metrics::LiveTimingStage;
    let ns = |n: u128| n.min(u64::MAX as u128) as u64;
    counters.record_write(
        after
            .completed_write_ops
            .saturating_sub(before.completed_write_ops),
        after
            .completed_write_bytes
            .saturating_sub(before.completed_write_bytes),
    );
    let submissions = after.submissions.saturating_sub(before.submissions);
    if submissions != 0 {
        counters.record_timing(
            LiveTimingStage::Queue,
            ns(after.queue_ns.saturating_sub(before.queue_ns)),
            submissions,
        );
        counters.record_timing(
            LiveTimingStage::Submit,
            ns(after.submit_ns.saturating_sub(before.submit_ns)),
            submissions,
        );
    }
    let completions = after.completions.saturating_sub(before.completions);
    if completions != 0 {
        counters.record_timing(
            LiveTimingStage::Completion,
            ns(after.submit_to_cq_ns.saturating_sub(before.submit_to_cq_ns)),
            completions,
        );
    }
}

/// How many queued append groups one live reservation can carry. One frame has
/// one reservation, one NVMe write, one completion RPC and one persistence
/// sequence, so merging queued groups divides all four by the merge factor.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct LiveGroupBudget {
    max_batches: usize,
    max_payload: usize,
    max_frame: usize,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl LiveGroupBudget {
    /// Size of an empty group: header only.
    pub(crate) const EMPTY_PAYLOAD: usize = crate::distributed::group::GROUP_HEADER_LEN;

    /// Counters of a group after adding `extra`, or `None` if it would exceed the
    /// owner's reservation limits (batch count, payload, aligned frame size).
    pub(crate) fn extend(
        &self,
        count: usize,
        payload: usize,
        extra: &[super::CompleteBatch],
    ) -> Option<(usize, usize)> {
        use crate::distributed::group::CompleteGroup;
        let count = count.checked_add(extra.len())?;
        let mut payload = payload;
        for batch in extra {
            payload = payload
                .checked_add(CompleteGroup::encoded_member_len(batch.encoded().len()).ok()?)?;
        }
        let frame = payload.checked_add(8 + 4095)? & !4095;
        (count <= self.max_batches && payload <= self.max_payload && frame <= self.max_frame)
            .then_some((count, payload))
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl super::Client {
    pub(crate) fn live_group_budget(&self) -> LiveGroupBudget {
        LiveGroupBudget {
            max_batches: self.expected.group_limits.max_batches as usize,
            max_payload: self.expected.group_limits.max_encoded_bytes,
            max_frame: (self.expected.fragment_bytes as usize).min(64 * 1024 * 1024),
        }
    }

    /// A position proof occupies the control slot, not the direct write queue.
    /// Use the next frame's exact aligned size, or the maximum when not known.
    pub(crate) fn leased_append_needs_control(&self, frame_len: Option<usize>) -> bool {
        let Some(current) = &self.live_leased.current else {
            return true;
        };
        if current.prefixes.len() >= 1024 {
            return true;
        }
        let frame = frame_len.unwrap_or_else(|| {
            self.expected
                .group_limits
                .max_encoded_bytes
                .saturating_add(8 + 4095)
                & !4095
        });
        frame as u64 > current.lease.extent.end - current.cursor
            && (!self.lease_ahead()
                || self.live_leased.closing.is_some()
                || self.live_leased.next.is_none())
    }

    /// Reserve the next extent before a position proof takes the control slot.
    /// The worker parks only the proof; appends retain the current lease runway.
    pub(crate) fn prepare_leased_position(&mut self) -> Result<bool, super::TcpError> {
        self.ensure_leased_healthy()?;
        if self.live_leased.pending.is_some() {
            return Ok(false);
        }
        if self.lease_ahead()
            && self.live_leased.current.is_some()
            && (self.live_leased.closing.is_some() || self.live_leased.next.is_none())
        {
            self.submit_leased_adoption()?;
            return Ok(false);
        }
        Ok(true)
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
    pub(crate) fn drain_live_writer(&mut self) -> Result<(), super::TcpError> {
        if !self.live_write_failed {
            self.close_leased_extent()?;
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
    pub(crate) fn close_live_writer(&mut self) -> Result<(), super::TcpError> {
        self.live_writer_closing = true;
        self.drain_live_writer()
    }

    pub(crate) fn configure_live_nvme(
        &mut self,
        config: &LiveClientConfig,
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
        if config.write_mode == LiveWriteMode::Leased {
            for buffer in &mut self.live_leased.prefix_buffers {
                if buffer.capacity() == 0 {
                    buffer
                        .try_reserve_exact(1024)
                        .map_err(super::TcpError::component)?;
                }
            }
        }
        if self.live_config.as_ref() != Some(config) {
            self.drain_live_writer()?;
            self.live_config = Some(selected.into_owned());
        }
        Ok(())
    }

    pub(crate) fn live_nvme_configured(&self) -> bool {
        self.live_config.is_some()
    }

    pub(crate) fn record_live_write_error(&mut self) {
        self.live_write_failed = true;
        if let Some(counters) = &self.live_counters {
            counters.record_error();
        }
    }

    /// Runs on the dedicated owner worker, never the coordinator or a read reactor.
    /// The reservation remains unresolved on any failure and the caller poisons
    /// the authority; a successful CQE alone is not a durable receipt.
    pub(crate) fn write_live_group(
        &mut self,
        epoch: u64,
        batches: &[super::CompleteBatch],
        timeline: &crate::distributed::write_timeline::WriteTimeline,
        owner: u32,
    ) -> Result<Vec<super::DurableBatch>, super::TcpError> {
        use super::live_control::LiveReserveResponse;
        use crate::distributed::group::CompleteGroup;
        use crate::distributed::nvmeof::live_connect::LiveConnectTarget;
        use crate::distributed::nvmeof::live_io::{LiveIo, LiveIoResult, LiveSubmitOutcome};
        use crate::distributed::nvmeof::live_metrics::LiveTimingStage;
        use crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess;
        use crate::distributed::nvmeof::live_storage::LiveGroupDescriptor;
        use crate::distributed::write_timeline::WriteTimelineStage as Stage;
        use std::time::{Duration, Instant};
        if self.live_writer_closing {
            return Err(super::TcpError::new("live writer admission is closed"));
        }

        let config = self
            .live_config
            .as_ref()
            .ok_or_else(|| super::TcpError::new("live NVMe is not configured"))?;
        let (host_id, max_inflight_requests, max_inflight_bytes, connect_timeout_ms, io_timeout_ms) = (
            config.host_id,
            config.max_inflight_requests,
            config.max_inflight_bytes,
            config.connect_timeout_ms,
            config.io_timeout_ms,
        );
        let pool = self
            .live_pool
            .clone()
            .ok_or_else(|| super::TcpError::new("live controller pool unavailable"))?;
        let counters = self.live_counters.clone();
        let timed = |stage| {
            counters.as_deref().map(|counters| WriteTiming {
                counters,
                stage,
                started: Instant::now(),
            })
        };
        let mut unknown = UnknownWriteOnError {
            counters: counters.as_deref(),
            armed: false,
        };
        let sample = timeline.live_group(owner, batches.iter().map(|batch| batch.version()));
        timeline.live_group_event(sample, Stage::LiveGroupBuildStarted);
        let (group, descriptor, frame) = {
            let _timing = timed(LiveTimingStage::Allocation);
            let group = CompleteGroup::new_live(
                &self.shape,
                batches.to_vec(),
                self.expected.batch_limits,
                self.expected.group_limits,
            )
            .map_err(super::TcpError::component)?;
            let (descriptor, frame) =
                LiveGroupDescriptor::from_group(&group).map_err(super::TcpError::component)?;
            (group, descriptor, frame)
        };
        timeline.live_group_event(sample, Stage::LiveGroupBuilt);
        if frame
            .len()
            .checked_add(4095)
            .is_none_or(|n| n > max_inflight_bytes)
        {
            return Err(super::TcpError::new("live frame exceeds queue byte budget"));
        }
        if let Some(counters) = &self.live_counters {
            counters.record_metadata_request();
            counters.record_reserve_exchange();
        }
        timeline.live_group_event(sample, Stage::LiveReserveStarted);
        let reserve = {
            let _timing = timed(LiveTimingStage::Control);
            self.reserve_live_group(epoch, descriptor)
                .map_err(|error| super::TcpError::new(format!("live reserve RPC: {error}")))?
        };
        timeline.live_group_event(sample, Stage::LiveReserveCompleted);
        let (receipts, frame_offset) = match reserve {
            LiveReserveResponse::Existing(receipts) => (receipts, None),
            LiveReserveResponse::Reserved(reserved) => {
                timeline.live_group_token(sample, reserved.reservation.token);
                if reserved.binding.readonly {
                    return Err(super::TcpError::new(
                        "owner returned a read-only write namespace",
                    ));
                }
                let binding = &reserved.binding;
                if !self
                    .live_writer
                    .as_ref()
                    .is_some_and(|writer| writer.matches(binding))
                {
                    timeline.live_group_event(sample, Stage::LiveNamespaceSetupStarted);
                    self.drain_live_writer()?;
                    let target = LiveConnectTarget {
                        nqn: binding.nqn.clone(),
                        uuid: binding.uuid,
                        nsid: binding.nsid,
                        bytes: binding.bytes,
                        block_size: binding.block_size,
                        address: binding.address.clone(),
                        service: binding.service,
                        transport: binding.transport,
                        spdk: binding.spdk.clone(),
                        geometry: self
                            .live_config
                            .as_ref()
                            .ok_or_else(|| super::TcpError::new("live NVMe is not configured"))?
                            .queue_geometry,
                    };
                    let host_nqn = &self
                        .live_config
                        .as_ref()
                        .ok_or_else(|| super::TcpError::new("live NVMe is not configured"))?
                        .host_nqn;
                    let setup_timing = timed(LiveTimingStage::NamespaceSetup);
                    #[cfg(test)]
                    if self.live_write_local_fixture {
                        let namespace = bench_open_local_file(
                            &reserved.reservation.segment.path,
                            binding.bytes,
                            binding.block_size,
                            LiveNamespaceAccess::ReadWrite,
                        )
                        .map_err(super::TcpError::component)?;
                        self.live_writer = Some(LiveWriteQueue {
                            target: target.clone(),
                            io: LiveIo::new(
                                namespace,
                                max_inflight_requests,
                                max_inflight_bytes,
                                false,
                            )
                            .map_err(super::TcpError::component)?,
                            _lease: None,
                            failed_accounting: None,
                            drain_pause: None,
                        });
                    }
                    #[cfg(test)]
                    let needs_controller = !self.live_write_local_fixture;
                    #[cfg(not(test))]
                    let needs_controller = true;
                    if needs_controller {
                        let (lease, acquisition) = pool
                            .get_or_connect(
                                &target,
                                host_nqn,
                                host_id,
                                LiveNamespaceAccess::ReadWrite,
                                Instant::now() + Duration::from_millis(connect_timeout_ms),
                            )
                            .map_err(super::TcpError::component)?;
                        let namespace =
                            lease.open_namespace().map_err(super::TcpError::component)?;
                        let io = LiveIo::new(
                            namespace,
                            max_inflight_requests,
                            max_inflight_bytes,
                            false,
                        )
                        .map_err(super::TcpError::component)?;
                        drop(setup_timing);
                        timeline.live_group_event(sample, Stage::LiveNamespaceSetupCompleted);
                        if let Some(counters) = &self.live_counters
                            && matches!(
                            acquisition,
                            crate::distributed::nvmeof::live_pool::LivePoolAcquisition::Connected
                        ) {
                            counters.record_namespace_connect();
                        }
                        if let Some(metrics) = &self.live_metrics {
                            let observed = lease.binding();
                            metrics.record_route(crate::distributed::nvmeof::live_metrics::LiveRouteEvidence {
                            transport: observed.transport,
                            address: binding.address.clone(),
                            service: binding.service,
                            host_nqn: host_nqn.clone(),
                            host_id,
                            target_implementation: crate::distributed::nvmeof::live_metrics::target_implementation(&observed.spdk),
                            spdk: observed.spdk.clone(),
                            writer: Some(crate::distributed::nvmeof::live_metrics::LiveWriterRoute {
                                epoch: reserved.reservation.segment.epoch,
                                file_index: reserved.reservation.segment.file_index,
                                file_identity: Some(crate::distributed::nvmeof::live_target::LiveFileIdentity {
                                    path: reserved.reservation.segment.path.clone(),
                                    device: reserved.reservation.segment.device,
                                    inode: reserved.reservation.segment.inode,
                                    bytes: reserved.reservation.segment.bytes,
                                    block_size: reserved.reservation.segment.block_size,
                                }),
                            }),
                            namespace_identity: crate::distributed::nvmeof::live_metrics::LiveNamespaceIdentity {
                                namespace_path: observed.namespace_path.clone(),
                                controller_instance: lease.controller_instance(),
                                nqn: observed.nqn.clone(),
                                uuid: observed.uuid,
                                nsid: observed.nsid,
                                bytes: observed.bytes,
                                block_size: observed.block_size,
                                target_implementation: crate::distributed::nvmeof::live_metrics::target_implementation(&observed.spdk),
                            },
                        });
                        }
                        self.live_writer = Some(LiveWriteQueue {
                            target,
                            io,
                            _lease: Some(lease),
                            failed_accounting: None,
                            #[cfg(test)]
                            drain_pause: None,
                        });
                    }
                }
                let io = &mut self
                    .live_writer
                    .as_mut()
                    .ok_or_else(|| super::TcpError::new("live write queue missing"))?
                    .io;
                let before = io.stats();
                let offset = reserved.reservation.frame_offset % reserved.reservation.segment.bytes;
                let deadline = Instant::now() + Duration::from_millis(io_timeout_ms);
                timeline.live_group_event(sample, Stage::LiveIoStarted);
                match io
                    .submit_write(
                        1,
                        offset,
                        &frame,
                        crate::distributed::nvmeof::live_io::LiveWriteSync::None,
                        deadline,
                    )
                    .map_err(|error| {
                        super::TcpError::new(format!("live write submit offset={offset}: {error}"))
                    })? {
                    LiveSubmitOutcome::Submitted => {
                        unknown.armed = true;
                    }
                    LiveSubmitOutcome::Full => {
                        return Err(super::TcpError::new("live write admission is full"));
                    }
                }
                loop {
                    let completions = match io.progress() {
                        Ok(completions) => completions,
                        Err(error) => {
                            let drain = io.shutdown();
                            if let Some(counters) = &counters {
                                record_write_io(counters, &before, &io.stats());
                            }
                            return match drain {
                                Ok(_) => Err(super::TcpError::new(format!(
                                    "live write progress offset={offset}: {error}"
                                ))),
                                Err(drain_error) => Err(super::TcpError::new(format!(
                                    "live write progress failed: {error}; drain failed: {drain_error}"
                                ))),
                            };
                        }
                    };
                    if let Some(completion) = completions.into_iter().next() {
                        if let Some(counters) = &counters {
                            record_write_io(counters, &before, &io.stats());
                        }
                        if completion.id != 1 {
                            return Err(super::TcpError::new("unexpected live write completion"));
                        }
                        match completion.result.map_err(|error| {
                            super::TcpError::new(format!("live write CQE offset={offset}: {error}"))
                        })? {
                            LiveIoResult::Written => break,
                            LiveIoResult::Reads(_) => {
                                return Err(super::TcpError::new("unexpected live write result"));
                            }
                        }
                    }
                    std::thread::yield_now();
                }
                timeline.live_group_event(sample, Stage::LiveIoCompleted);
                if let Some(counters) = &self.live_counters {
                    counters.record_metadata_request();
                    counters.record_complete_exchange();
                }
                timeline.live_group_event(sample, Stage::LiveCompleteStarted);
                let completion = {
                    let _timing = timed(LiveTimingStage::Control);
                    self.complete_live_group_profiled(epoch, reserved.reservation.token)
                        .map_err(|error| {
                            super::TcpError::new(format!(
                                "live completion RPC offset={offset}: {error}"
                            ))
                        })?
                };
                timeline.live_group_event(sample, Stage::LiveCompleteCompleted);
                timeline.live_group_owner(
                    sample,
                    (completion.validation_samples != 0).then_some(completion.validation_ns),
                    completion.persistence,
                );
                if let Some(counters) = &counters {
                    let profile = &completion.persistence;
                    // Persistence contains the WAL and frontier barrier spans;
                    // these nested observations must not be added together.
                    for (stage, ns, samples) in [
                        (
                            LiveTimingStage::OwnerCompletion,
                            completion.validation_ns,
                            completion.validation_samples,
                        ),
                        (
                            LiveTimingStage::Persistence,
                            profile.persist_total_ns,
                            profile.persist_calls,
                        ),
                        (
                            LiveTimingStage::WalPersistence,
                            profile.persist_wal_file_ns,
                            profile.file_persist_calls,
                        ),
                        (
                            LiveTimingStage::WalNamespace,
                            profile.persist_namespace_ns,
                            profile.namespace_persist_calls,
                        ),
                        (
                            LiveTimingStage::FrontierFile,
                            profile.live_frontier_file_ns,
                            profile.live_frontier_file_calls,
                        ),
                        (
                            LiveTimingStage::FrontierNamespace,
                            profile.live_frontier_namespace_ns,
                            profile.live_frontier_namespace_calls,
                        ),
                    ] {
                        if samples != 0 {
                            counters.record_timing(stage, ns, samples);
                        }
                    }
                }
                (completion.receipts, Some(reserved.reservation.frame_offset))
            }
        };
        let _validation_timing = timed(LiveTimingStage::Validation);
        if receipts.len() != batches.len() {
            return Err(super::TcpError::new(
                "live receipt count differs from submitted group",
            ));
        }
        let validated = batches
            .iter()
            .zip(receipts)
            .zip(group.batch_offsets())
            .map(|((batch, receipt), offset)| {
                if let Some(frame_offset) = frame_offset {
                    let expected = frame_offset
                        .checked_add(8)
                        .and_then(|n| n.checked_add(u64::from(*offset)));
                    if expected != Some(receipt.appended().address().offset) {
                        return Err(super::TcpError::new(
                            "live receipt is outside the reserved frame",
                        ));
                    }
                }
                self.check_receipt(batch, super::Receipt::from_durable(receipt))
            })
            .collect::<Result<Vec<_>, _>>()?;
        timeline.live_group_event(sample, Stage::LiveReceiptsValidated);
        unknown.armed = false;
        Ok(validated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    #[test]
    fn unknown_write_counts_only_accepted_unresolved_writes() {
        use crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics;
        let metrics = LiveNvmeMetrics::new();
        let counters = metrics.register().unwrap();
        drop(UnknownWriteOnError {
            counters: Some(&counters),
            armed: false,
        });
        assert_eq!(metrics.snapshot().counts.unknown_writes, 0);
        drop(UnknownWriteOnError {
            counters: Some(&counters),
            armed: true,
        });
        assert_eq!(metrics.snapshot().counts.unknown_writes, 1);
        let mut resolved = UnknownWriteOnError {
            counters: Some(&counters),
            armed: true,
        };
        resolved.armed = false;
        drop(resolved);
        assert_eq!(metrics.snapshot().counts.unknown_writes, 1);
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
            max_inflight_requests: 32,
            max_inflight_bytes: 4096,
            connect_timeout_ms: 1000,
            io_timeout_ms: 1000,
            write_mode: LiveWriteMode::Reserved,
            leased_durability: None,
            inline_reads: true,
            queue_geometry: Default::default(),
            export_mode: Default::default(),
        };
        assert!(config.validate().is_ok());
        let mut old = serde_json::to_value(&config).unwrap();
        old.as_object_mut().unwrap().remove("queue_geometry");
        old.as_object_mut().unwrap().remove("export_mode");
        assert!(!old.as_object().unwrap().contains_key("owner_identities"));
        let restored: LiveClientConfig = serde_json::from_value(old).unwrap();
        assert_eq!(restored.queue_geometry, LiveQueueGeometry::default());
        assert_eq!(restored.export_mode, LiveExportMode::OnDemand);
        assert!(restored.owner_identities.is_empty());
        for mode in [LiveExportMode::PerFile, LiveExportMode::PerOwner] {
            config.export_mode = mode;
            assert_eq!(
                config.validate(),
                Err("permanent live export requires leased write mode")
            );
            config.write_mode = LiveWriteMode::Leased;
            config.leased_durability = Some(crate::distributed::durability::DurabilityTier::Native);
            assert!(config.validate().is_ok());
            config.write_mode = LiveWriteMode::Reserved;
            config.leased_durability = None;
        }
        config.export_mode = LiveExportMode::OnDemand;
        config.queue_geometry.poll_queues = 1;
        assert!(config.validate().is_ok());
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

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl LeasedWriter {
    fn install(
        &mut self,
        leased: super::live_control::LiveLeased,
        expected: &super::PartitionConfig,
    ) {
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
        extent.prefixes.clear();
        let index = if self.prefix_buffers[0].capacity() == 0 {
            0
        } else {
            1
        };
        self.prefix_buffers[index] = extent.prefixes;
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl ExtentState {
    fn validate_adoption(
        &mut self,
        pending: PendingAdoption,
        reply: &super::live_control::LiveAdopted,
    ) -> Result<(), super::TcpError> {
        let current = &self.lease;
        let adoption = &reply.adoption;
        let submitted_end = self.prefixes.back().map_or(self.adopted.end, |p| p.end);
        if adoption.token != current.extent.token
            || adoption.adopted_end < pending.through
            || adoption.adopted_end > submitted_end
            || adoption.adopted_end > current.extent.end
            || adoption.closed != pending.close
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

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl super::Client {
    fn install_leased_extent(
        &mut self,
        leased: super::live_control::LiveLeased,
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
            .and_then(|config| config.leased_durability);
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
        reply: &super::live_control::LiveAdopted,
    ) -> Result<(), super::TcpError> {
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
        if result.is_ok()
            && let Some(counters) = &self.live_counters
        {
            counters.record_adopt_profile(reply.profile);
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

    pub(crate) fn live_leased_mode(&self) -> bool {
        self.live_config
            .as_ref()
            .is_some_and(|c| c.write_mode == LiveWriteMode::Leased)
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
                    .and_then(|config| config.leased_durability);
                if leased.extent.start != end || tier != Some(leased.tier) {
                    return Err(super::TcpError::new(
                        "preleased extent identity, tier or continuity differs",
                    ));
                }
                if let Some(counters) = &self.live_counters {
                    counters.record_lease_profile(leased.profile);
                    counters.record_extent_prelease();
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
                let closing = self
                    .live_leased
                    .closing
                    .take()
                    .ok_or_else(|| super::TcpError::new("closed extent missing"))?;
                self.live_leased.recycle(closing);
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
        let deadline = Instant::now() + self.timeout;
        let result = if target == PendingTarget::Prelease {
            self.submit_prelease_live_extent(epoch, token, deadline)
        } else {
            self.submit_adopt_live_extent(epoch, token, through, close, false, deadline)
        };
        match result {
            Ok(true) => {
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
            self.progress_appends()?;
            self.harvest_leased_adoption()?;
            if self.live_leased.pending.is_some() {
                self.pause_append_poll(&mut polling);
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
            self.progress_appends()?;
            self.harvest_leased_adoption()?;
            if self.live_leased.closing.is_some()
                || (require_next && self.live_leased.next.is_none())
            {
                self.pause_append_poll(&mut polling);
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

    pub(crate) fn wait_leased_control(&mut self) -> Result<(), super::TcpError> {
        let result = self.wait_leased_adoption();
        if result.is_err() {
            self.live_write_failed = true;
        }
        result
    }

    pub(crate) fn settle_leased_extent(&mut self) -> Result<(), super::TcpError> {
        let started = Instant::now();
        let has_extent = self.live_leased.current.is_some();
        let result = self
            .wait_closing_extent(false)
            .and_then(|()| self.adopt_leased_sync(false, false));
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

    pub(crate) fn close_leased_extent(&mut self) -> Result<(), super::TcpError> {
        let result = (|| {
            self.ensure_leased_healthy()?;
            self.wait_closing_extent(false)?;
            self.wait_leased_adoption()?;
            if self.live_leased.current.is_none() {
                return Ok(());
            }
            self.write_leased_skip()?;
            self.adopt_leased_sync(true, false)?;
            if let Some(next) = self.live_leased.next.take() {
                self.live_leased.install(next, &self.expected);
                self.write_leased_skip()?;
                self.adopt_leased_sync(true, false)?;
            }
            Ok(())
        })();
        if result.is_err() {
            self.live_write_failed = true;
        }
        result
    }

    fn ensure_leased_queue(&mut self) -> Result<(), super::TcpError> {
        use crate::distributed::nvmeof::{
            live_connect::LiveConnectTarget, live_io::LiveIo, live_namespace::LiveNamespaceAccess,
        };
        use std::time::{Duration, Instant};
        let leased = self
            .live_leased
            .current
            .as_ref()
            .ok_or_else(|| super::TcpError::new("live leased extent missing"))?;
        let leased = &leased.lease;
        if self
            .live_writer
            .as_ref()
            .is_some_and(|w| w.matches(&leased.binding))
        {
            return Ok(());
        }
        self.drain_live_queue()?;
        let leased = self
            .live_leased
            .current
            .as_ref()
            .ok_or_else(|| super::TcpError::new("live leased extent missing"))?;
        let leased = &leased.lease;
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
            spdk: binding.spdk.clone(),
            geometry: config.queue_geometry,
        };
        #[cfg(test)]
        if self.live_write_local_fixture {
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
                drain_pause: None,
            });
            return Ok(());
        }
        let pool = self
            .live_pool
            .as_ref()
            .ok_or_else(|| super::TcpError::new("live controller pool unavailable"))?;
        let deadline = Instant::now() + Duration::from_millis(config.connect_timeout_ms);
        let shared = config.export_mode.files_per_subsystem().is_some();
        let (lease, acquisition) = if shared {
            pool.acquire_shared(
                &target,
                &config.host_nqn,
                config.host_id,
                LiveNamespaceAccess::ReadWrite,
                deadline,
            )
            .map(|(lease, acquisition, _)| (lease, acquisition))
        } else {
            pool.get_or_connect(
                &target,
                &config.host_nqn,
                config.host_id,
                LiveNamespaceAccess::ReadWrite,
                deadline,
            )
        }
        .map_err(super::TcpError::component)?;
        let namespace = if shared {
            lease.namespace(
                binding.nsid,
                binding.uuid,
                LiveNamespaceAccess::ReadWrite,
                deadline,
            )
        } else {
            lease.open_namespace().map(std::sync::Arc::new)
        }
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
            let observed = lease.binding();
            let segment = &leased.extent.segment;
            metrics.record_route(
                crate::distributed::nvmeof::live_metrics::LiveRouteEvidence {
                    transport: observed.transport,
                    address: binding.address.clone(),
                    service: binding.service,
                    host_nqn: config.host_nqn.clone(),
                    host_id: config.host_id,
                    target_implementation:
                        crate::distributed::nvmeof::live_metrics::target_implementation(
                            &observed.spdk,
                        ),
                    spdk: observed.spdk.clone(),
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
                            target_implementation:
                                crate::distributed::nvmeof::live_metrics::target_implementation(
                                    &observed.spdk,
                                ),
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
        Ok(())
    }

    fn write_leased_block(
        &mut self,
        offset: u64,
        len: usize,
        encode: impl FnOnce(&mut [u8]) -> Result<(), super::TcpError>,
        trace: Option<(
            &crate::distributed::write_timeline::WriteTimeline,
            crate::distributed::write_timeline::LiveGroupSample,
        )>,
    ) -> Result<(), super::TcpError> {
        use crate::distributed::nvmeof::live_io::{
            LiveIoError, LiveIoResult, LiveSubmitOutcome, LiveWriteSync,
        };
        use crate::distributed::nvmeof::live_metrics::{LiveTimingStage, LiveWriteFailure};
        use std::time::{Duration, Instant};
        self.ensure_leased_healthy()?;
        self.ensure_leased_queue()?;
        let io_timeout_ms = self
            .live_config
            .as_ref()
            .ok_or_else(|| super::TcpError::new("live config missing"))?
            .io_timeout_ms;
        let tier = self
            .live_leased
            .current
            .as_ref()
            .ok_or_else(|| super::TcpError::new("leased extent missing its durability tier"))?
            .lease
            .tier;
        let sync = match tier {
            crate::distributed::durability::DurabilityTier::Native => LiveWriteSync::None,
            crate::distributed::durability::DurabilityTier::Synchronous => LiveWriteSync::Data,
        };
        let counters = self.live_counters.as_deref();
        let mut unknown = UnknownWriteOnError {
            counters,
            armed: false,
        };
        let io = &mut self
            .live_writer
            .as_mut()
            .ok_or_else(|| super::TcpError::new("live queue missing"))?
            .io;
        #[cfg(test)]
        if let Some(result) = self.live_leased.next_write_result.take() {
            io.override_next_completion_result(result);
        }
        let before = io.stats();
        let deadline = Instant::now() + Duration::from_millis(io_timeout_ms);
        let outcome = (|| {
            let mut buffer = io.write_buffer(len)?;
            encode(&mut buffer)
                .map_err(|error| LiveIoError::Io(std::io::Error::other(error.to_string())))?;
            if let Some((timeline, sample)) = trace {
                timeline.live_group_event(
                    Some(sample),
                    crate::distributed::write_timeline::WriteTimelineStage::LiveFrameEncoded,
                );
            }
            buffer.submit_write_buffer(1, offset, sync, deadline)
        })();
        match outcome {
            Ok(LiveSubmitOutcome::Submitted) => unknown.armed = true,
            outcome => {
                let failure = match &outcome {
                    Ok(LiveSubmitOutcome::Full) | Err(LiveIoError::Full) => LiveWriteFailure::Full,
                    Err(LiveIoError::Timeout) => LiveWriteFailure::TimedOut,
                    _ => LiveWriteFailure::Failed,
                };
                if let Some(counters) = counters {
                    counters.record_leased_write_failure(failure);
                }
                return match outcome {
                    Err(error) => Err(super::TcpError::component(error)),
                    _ => Err(super::TcpError::new("live leased write admission full")),
                };
            }
        }
        // Successful admission is the timing origin; encoding and rejected
        // submissions are not part of the successful completion population.
        let submitted_at = Instant::now();
        if sync == LiveWriteSync::Data
            && let Some(counters) = counters
        {
            counters.record_fua_write();
        }
        #[cfg(test)]
        if std::mem::take(&mut self.live_leased.delay_write) {
            std::thread::sleep(Duration::from_millis(io_timeout_ms + 1));
        }
        let mut timed_out = false;
        let mut completed_at = None;
        let mut submission_recorded = false;
        let result = (|| {
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    timed_out = true;
                    return Err(super::TcpError::new("leased write deadline expired"));
                }
                if trace.is_some() && !submission_recorded {
                    // Sampled groups pay one extra submit syscall to expose
                    // this boundary. Their CQE-wait interval and total can
                    // differ from untraced submit_with_args(1) writes.
                    if !io
                        .submit_pending_write()
                        .map_err(super::TcpError::component)?
                    {
                        continue;
                    }
                    if let Some((timeline, sample)) = trace {
                        timeline.live_group_event(
                            Some(sample),
                            crate::distributed::write_timeline::WriteTimelineStage::LiveIoSubmitted,
                        );
                    }
                    submission_recorded = true;
                }
                io.wait(remaining).map_err(super::TcpError::component)?;
                let completion = io.progress_one().map_err(super::TcpError::component)?;
                let Some(completion) = completion else {
                    continue;
                };
                if completion.id != 1 {
                    return Err(super::TcpError::new("unexpected leased write completion"));
                }
                return match completion.result.map_err(super::TcpError::component)? {
                    LiveIoResult::Written => {
                        completed_at = Some(completion.completed_at);
                        Ok(())
                    }
                    LiveIoResult::Reads(_) => {
                        Err(super::TcpError::new("unexpected leased read completion"))
                    }
                };
            }
        })();
        let after = io.stats();
        if let Some(counters) = counters {
            if let Some(completed_at) = completed_at {
                record_write_io(counters, &before, &after);
                counters.record_timing(
                    LiveTimingStage::LeasedWrite,
                    completed_at
                        .saturating_duration_since(submitted_at)
                        .as_nanos()
                        .min(u64::MAX as u128) as u64,
                    1,
                );
            } else {
                let failure = if after.short_write_ops > before.short_write_ops {
                    LiveWriteFailure::Short
                } else if after.failed_write_ops > before.failed_write_ops {
                    LiveWriteFailure::Failed
                } else if timed_out || after.timeouts > before.timeouts {
                    LiveWriteFailure::TimedOut
                } else {
                    LiveWriteFailure::Failed
                };
                counters.record_leased_write_failure(failure);
            }
        }
        if result.is_ok() {
            unknown.armed = false;
        } else if let Some(counters) = &self.live_counters
            && let Some(writer) = &mut self.live_writer
        {
            // A timeout may precede the terminal CQE. Keep the pre-submit
            // snapshot with its unique queue owner, including on direct Drop.
            writer.failed_accounting = Some(FailedWriteAccounting {
                before,
                counters: counters.clone(),
            });
        }
        // Return failures immediately. The worker detaches this queue and drains
        // outside its mutex; the queue Drop also preserves terminal DMA ownership.
        result
    }

    fn write_leased_skip(&mut self) -> Result<(), super::TcpError> {
        let state = &self.live_leased;
        let Some(current) = &state.current else {
            return Ok(());
        };
        if current.cursor == current.lease.extent.end {
            return Ok(());
        }
        let offset = current.cursor % current.lease.extent.segment.bytes;
        self.write_leased_block(
            offset,
            4096,
            |block| {
                block.fill(0);
                block[..8].copy_from_slice(crate::crc::CrcFrame::skip_marker().as_ref());
                Ok(())
            },
            None,
        )?;
        if let Some(counters) = &self.live_counters {
            counters.record_leased_skip();
        }
        Ok(())
    }

    pub(crate) fn write_leased_group(
        &mut self,
        epoch: u64,
        batches: Vec<super::CompleteBatch>,
        timeline: &crate::distributed::write_timeline::WriteTimeline,
        owner: u32,
    ) -> Result<Vec<super::DurableBatch>, super::TcpError> {
        let result = self.write_leased_group_inner(epoch, batches, timeline, owner);
        if result.is_err() {
            self.live_write_failed = true;
        }
        result
    }

    fn write_leased_group_inner(
        &mut self,
        epoch: u64,
        batches: Vec<super::CompleteBatch>,
        timeline: &crate::distributed::write_timeline::WriteTimeline,
        owner: u32,
    ) -> Result<Vec<super::DurableBatch>, super::TcpError> {
        use crate::distributed::group::LiveFrame;
        use crate::distributed::nvmeof::live_storage::extend_live_receipt_digest;
        use crate::distributed::write_timeline::WriteTimelineStage as Stage;
        self.ensure_leased_healthy()?;
        if self.live_writer_closing || !self.live_leased_mode() {
            return Err(super::TcpError::new(
                "leased writer admission is closed or mode differs",
            ));
        }
        if batches.iter().any(|batch| batch.version().epoch != epoch) {
            return Err(super::TcpError::new(
                "leased batch epoch differs from stream",
            ));
        }
        let sample = timeline.live_group(owner, batches.iter().map(|batch| batch.version()));
        timeline.live_group_event(sample, Stage::LiveGroupBuildStarted);
        // Keep ownership of immutable members, but encode only in the queue buffer.
        let group = LiveFrame::new(
            &self.shape,
            batches,
            self.expected.batch_limits,
            self.expected.group_limits,
        )
        .map_err(super::TcpError::component)?;
        let frame_len = group.len();
        timeline.live_group_event(sample, Stage::LiveGroupBuilt);
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
        if self
            .live_leased
            .current
            .as_ref()
            .is_some_and(|current| current.prefixes.len() == 1024)
        {
            self.settle_leased_extent()?;
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
            let leased = self.lease_live_extent(epoch)?;
            self.install_leased_extent(leased)?;
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
                    self.wait_closing_extent(true)?;
                }
                self.write_leased_skip()?;
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
                self.write_leased_skip()?;
                self.adopt_leased_sync(true, true)?;
            }
            if let Some(counters) = &self.live_counters {
                counters.record_timing(
                    crate::distributed::nvmeof::live_metrics::LiveTimingStage::ExtentSwitch,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    1,
                );
            }
            timeline.live_group_event(sample, Stage::LiveExtentSwitchCompleted);
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
        let cursor = current.cursor;
        let offset = cursor % current.lease.extent.segment.bytes;
        let end = cursor
            .checked_add(frame_len as u64)
            .ok_or_else(|| super::TcpError::new("leased write offset overflow"))?;
        let position = crate::WalPosition::new(cursor, frame_len as u32);
        let mut prefix = current.acknowledged;
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
        self.write_leased_block(
            offset,
            frame_len,
            |buffer| {
                group
                    .encode_into(buffer)
                    .map_err(super::TcpError::component)
            },
            sample.map(|sample| (timeline, sample)),
        )?;
        timeline.live_group_event(sample, Stage::LiveIoCompleted);
        let current = self
            .live_leased
            .current
            .as_mut()
            .ok_or_else(|| super::TcpError::new("lease missing"))?;
        current.cursor = end;
        current.written_end = end;
        current.acknowledged = prefix;
        #[cfg(test)]
        if std::mem::take(&mut self.live_leased.invalidate_adoption_token)
            && let Some(current) = &mut self.live_leased.current
        {
            current.lease.extent.token[0] ^= 1;
        }
        if let Some(counters) = &self.live_counters {
            counters.record_leased_frame(frame_len as u64);
        }
        timeline.live_group_event(sample, Stage::LiveReceiptsValidated);
        Ok(receipts)
    }
}

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
pub(crate) mod leased_tests {
    use super::*;
    use crate::distributed::durability::{DurabilityConfig, DurabilityTier};
    use crate::distributed::nvmeof::{
        LiveTargetConfig, NvmeofTransport, live_metrics::LiveNvmeMetrics,
        live_target::LiveTargetManager,
    };
    use crate::distributed::tcp::{self, Client, NodeConfig, StorageNode, TlsFiles, Transport};
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

        fn new_with_partitions(tier: DurabilityTier, fragment: u64, partitions: Vec<u32>) -> Self {
            let temp = tempdir::TempDir::new("leased-coordinator").unwrap();
            let mut config = tcp::tests::config(&temp.path().join("node"));
            config.partition.inventory.partitions = partitions;
            config.partition.fragment_bytes = fragment;
            config.partition.wal_file_bytes = fragment * 2;
            config.partition.batch_limits.max_encoded_bytes = 8192;
            config.partition.group_limits.max_encoded_bytes = 16376.min(fragment as usize - 8);
            config.partition.max_batches = 4096;
            config.partition.max_reference_metadata_bytes = 64 * 1024 * 1024;
            Self::open(tier, config, temp, Duration::from_secs(10))
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
                LiveTargetConfig {
                    address: "127.0.0.1".into(),
                    service: 4420,
                    transport: NvmeofTransport::Tcp,
                    host_nqn: "nqn.2026-01.test:leased".into(),
                    port_id: 42,
                    state_dir,
                    commissioning_port_id: 43,
                    commissioning_service: 4421,
                    spdk: None,
                    export_mode: Default::default(),
                },
                configfs,
            )
            .unwrap();
            node.inventory.lock().live_target = Some(target);
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
            client.configure_live_nvme(&config).unwrap();
            client
        }

        fn configure_writer(&self, client: &mut Client, tier: DurabilityTier) {
            client
                .configure_live_nvme(&LiveClientConfig {
                    lease_ahead: true,
                    polled_writes: false,
                    host_nqn: "nqn.2026-01.test:leased".into(),
                    host_id: [1; 16],
                    owner_identities: Default::default(),
                    max_inflight_requests: 4,
                    max_inflight_bytes: (self.config.partition.group_limits.max_encoded_bytes
                        + 4096)
                        .max(128 * 1024),
                    connect_timeout_ms: 1000,
                    io_timeout_ms: 1000,
                    write_mode: LiveWriteMode::Leased,
                    leased_durability: Some(tier),
                    inline_reads: true,
                    queue_geometry: Default::default(),
                    export_mode: Default::default(),
                })
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

    impl Drop for Fixture {
        fn drop(&mut self) {
            // Release wakes the server's shutdown checks; no stream state travels
            // through this flag.
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
        client.configure_live_nvme(&decoded).unwrap();
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
            .write_leased_group(1, vec![batch.clone()], &WriteTimeline::default(), 2)
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
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let fixture = Fixture::new(tier, 16384);
            let mut client = fixture.writer_without_ahead();
            let mut config = client.live_config.clone().unwrap();
            config.polled_writes = true;
            config.queue_geometry.poll_queues = 1;
            client.configure_live_nvme(&config).unwrap();
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
            if let Err(error) =
                client.write_leased_group(1, vec![batch.clone()], &WriteTimeline::default(), 2)
            {
                assert!(error.to_string().contains("UnknownWrite"), "{error}");
                let io = &mut client.live_writer.as_mut().unwrap().io;
                unsupported(io.last_write_errno());
                assert_eq!(io.outstanding(), 0);
                assert!(io.shutdown().unwrap().is_empty());
                return;
            }
            let cursor = client.live_leased.current.as_ref().unwrap().cursor;
            client.write_leased_skip().unwrap();
            assert_eq!(client.live_writer.as_ref().unwrap().io.outstanding(), 0);
            assert_eq!(fixture.metrics.snapshot().counts.completed_write_ops, 2);
            let file = std::fs::File::open(path).unwrap();
            let mut marker = [0; 8];
            std::os::unix::fs::FileExt::read_exact_at(&file, &mut marker, cursor).unwrap();
            assert_eq!(&marker, crate::crc::CrcFrame::skip_marker().as_ref());
            client.close_live_writer().unwrap();
            let log = fixture.node.inventory.lock().active(1).unwrap();
            assert_eq!(log.receipt_count(), 1);
        }
    }

    #[cfg(feature = "test-utils")]
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
        client.close_leased_extent().unwrap();
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
                // No adoption exchange is needed to acknowledge the frame.
                let counts = fixture.metrics.snapshot().counts;
                assert_eq!(
                    counts.adoption_async_exchanges + counts.adoption_sync_exchanges,
                    previous_adoptions
                );
                assert_eq!(
                    owner_reader.read_records(1, &[reference]).unwrap(),
                    batch.mutations()
                );
                let locations = owner_reader.resolve_live_records(1, &[reference]).unwrap();
                let mut cache = crate::distributed::tcp::live_read::LiveReadCache::default();
                let location = locations.entries.into_iter().next().unwrap();
                crate::distributed::tcp::live_read::tests::install_existing_local(
                    &mut cache,
                    location,
                    locations.epoch_pin_token,
                );
                assert_eq!(
                    client
                        .live_read_records(
                            &mut cache,
                            1,
                            &[reference],
                            Instant::now() + Duration::from_secs(2),
                            None
                        )
                        .unwrap(),
                    batch.mutations()
                );
                cache.close(&mut owner_reader, 1).unwrap();
                client.settle_leased_extent().unwrap();
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
            assert_eq!(
                (
                    counts.lease_exchanges,
                    counts.reserve_exchanges,
                    counts.complete_exchanges
                ),
                (1, 0, 0)
            );
            assert_eq!(
                counts.fua_writes,
                if tier == DurabilityTier::Synchronous {
                    3
                } else {
                    0
                }
            );
            assert_eq!(counts.leased_write_samples, 3);
            client.close_leased_extent().unwrap();
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
                .write_leased_group(1, vec![batch.clone(), batch], &WriteTimeline::default(), 2)
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
                .write_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
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
        let mut reader = fixture.client();
        reader.read_records(1, &[reference]).unwrap();
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
        client.close_leased_extent().unwrap();
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
        let mut reader = fixture.client();
        for (batch, receipt) in [&first, &second, &third].into_iter().zip(receipts) {
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(
                reader.read_records(1, &[reference]).unwrap(),
                batch.mutations()
            );
        }
        client.close_leased_extent().unwrap();
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
            client.progress_appends().unwrap();
            if let Some(reply) = client.take_control() {
                reply.unwrap();
                break;
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        client.close_leased_extent().unwrap();
    }

    #[test]
    fn leased_bad_digest_poison_prevents_later_write_or_control() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        write(&mut client, &fixture.batch(0));
        client.live_leased.current.as_mut().unwrap().prefixes[0].digest[0] ^= 1;
        assert!(client.settle_leased_extent().is_err());
        let before = fixture.metrics.snapshot().counts;
        assert_eq!(before.adoption_async_samples, 0);
        assert_eq!(before.adoption_settle_samples, 0);
        assert_eq!(before.adoption_history_full_settles, 0);
        assert!(
            client
                .write_leased_group(1, vec![fixture.batch(1)], &WriteTimeline::default(), 2)
                .is_err()
        );
        assert!(client.close_leased_extent().is_err());
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
                    .write_leased_group(1, vec![fixture.batch(2)], &WriteTimeline::default(), 2)
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
        client.wait_leased_control().unwrap();
        let completed = fixture.metrics.snapshot().counts;
        assert_eq!(completed.adoption_async_samples, 1);
        assert!(completed.adoption_async_ns > 0);
        client.close_leased_extent().unwrap();
        assert_eq!(fixture.metrics.snapshot().counts.adoption_settle_samples, 0);
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn memory_only_leased_frames_adopt_without_history_waits() {
        let fixture = Fixture::new(DurabilityTier::Native, 16 * 1024 * 1024);
        fixture
            .node
            .set_history_persistence(crate::distributed::storage::HistoryPersistence::MemoryOnly)
            .unwrap();
        fixture.node.set_history_maintenance_diagnostics(true);
        let mut client = fixture.writer();
        for sequence in 0..3 * 1024 {
            write(&mut client, &fixture.batch(sequence));
        }
        client.close_leased_extent().unwrap();
        let log = fixture.node.inventory.lock().active(1).unwrap();
        assert_eq!(log.receipt_count(), 3 * 1024);
        assert_eq!(log.history_maintenance_stats().waits, 0);
        assert_eq!(client.checkpoint(1, 1, 3 * 1024).unwrap().prefix, 3 * 1024);
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
        client.settle_leased_extent().unwrap();
        assert_eq!(fixture.metrics.snapshot().counts.adoption_settle_samples, 2);
        client.close_leased_extent().unwrap();
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
    fn omitted_write_mode_is_reserved_and_unknown_mode_is_rejected() {
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
        });
        let config: LiveClientConfig = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(config.write_mode, LiveWriteMode::Reserved);
        assert!(config.inline_reads);
        assert!(config.lease_ahead);
        assert!(!config.polled_writes);
        let mut invalid = value;
        invalid["write_mode"] = serde_json::json!("fallback");
        assert!(serde_json::from_value::<LiveClientConfig>(invalid).is_err());
    }

    #[test]
    fn leased_traced_failed_and_short_completions_keep_unknown_write_contract() {
        use crate::distributed::write_timeline::WriteTimelineStage as Stage;
        for result in [-libc::EIO, 2048] {
            let fixture = Fixture::new(DurabilityTier::Native, 16384);
            let mut client = fixture.writer();
            client.leased_test_fail_write(result);
            let timeline = WriteTimeline::default();
            timeline.arm(1).unwrap();
            let batch = fixture.batch(0);
            let sample = timeline.begin(0).unwrap();
            timeline.operation(sample, batch.id().operation);
            timeline.batch(batch.id().operation, batch.version());
            assert!(
                client
                    .write_leased_group(1, vec![batch], &timeline, 2)
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
                    Stage::LiveIoSubmitted
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
                    .write_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
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
                    .write_leased_group(1, vec![fixture.batch(1)], &WriteTimeline::default(), 2)
                    .is_err()
            );
            assert!(client.settle_leased_extent().is_err());
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
                .write_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2,)
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
                    .write_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
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
                .write_leased_group(1, vec![fixture.batch(1)], &WriteTimeline::default(), 2)
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
                .write_leased_group(2, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
                .is_err()
        );
        assert_eq!(fixture.metrics.snapshot().counts.metadata_requests, 0);
    }

    #[test]
    fn leased_config_requires_an_explicit_tier_only_in_leased_mode() {
        let mut config: LiveClientConfig = serde_json::from_value(serde_json::json!({
            "host_nqn": "nqn.2026-01.test:leased", "host_id": vec![1; 16],
            "max_inflight_requests": 4, "max_inflight_bytes": 65536,
            "connect_timeout_ms": 1000, "io_timeout_ms": 1000,
        }))
        .unwrap();
        assert!(config.validate().is_ok());
        config.leased_durability = Some(DurabilityTier::Native);
        assert!(config.validate().is_err());
        config.write_mode = LiveWriteMode::Leased;
        assert!(config.validate().is_ok());
        config.leased_durability = None;
        assert!(config.validate().is_err());
    }

    #[test]
    fn polled_writes_require_a_poll_queue_and_leased_mode() {
        let mut config: LiveClientConfig = serde_json::from_value(serde_json::json!({
            "host_nqn": "nqn.2026-01.test:leased", "host_id": vec![1; 16],
            "max_inflight_requests": 4, "max_inflight_bytes": 65536,
            "connect_timeout_ms": 1000, "io_timeout_ms": 1000,
            "lease_ahead": true, "polled_writes": true,
        }))
        .unwrap();
        assert_eq!(
            config.validate(),
            Err("polled live writes require a poll queue")
        );
        config.queue_geometry.poll_queues = 1;
        assert_eq!(
            config.validate(),
            Err("polled live writes require leased write mode")
        );
        config.write_mode = LiveWriteMode::Leased;
        config.leased_durability = Some(DurabilityTier::Native);
        assert!(config.validate().is_ok());
        config.polled_writes = false;
        config.queue_geometry.poll_queues = 0;
        config.write_mode = LiveWriteMode::Reserved;
        config.leased_durability = None;
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
                .write_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
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
                .write_leased_group(1, vec![fixture.batch(1)], &WriteTimeline::default(), 2)
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
            client.close_leased_extent().unwrap();
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
                .write_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
                .is_err()
        );
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.leased_full_writes, 1);
        assert_eq!(leased_latencies(&counts), [(0, 0); 5]);
        assert!(client.drain_live_writer().is_err());
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
                    .write_leased_group(
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
                .write_leased_group(
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
            .write_leased_group(1, group.clone(), &WriteTimeline::default(), 2)
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
        let mut reader = fixture.client();
        for (batch, receipt) in group.iter().zip(receipts) {
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(
                reader.read_records(1, &[reference]).unwrap(),
                batch.mutations()
            );
        }
        client.close_leased_extent().unwrap();
    }

    #[test]
    fn leased_lower_through_retry_uses_retired_proof() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        write(&mut client, &fixture.batch(1));
        client.settle_leased_extent().unwrap();
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
        client.close_leased_extent().unwrap();
    }

    #[test]
    fn lease_ahead_switch_sends_no_synchronous_owner_request() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        write(&mut client, &fixture.batch(0));
        client.wait_closing_extent(true).unwrap();
        let before = fixture.metrics.snapshot().counts;
        let receipts = client
            .write_leased_group(
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
        client.close_leased_extent().unwrap();
        let mut reader = fixture.client();
        for (sequence, receipt) in (1..=3).zip(receipts) {
            let batch = fixture.big_batch(sequence);
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(
                reader.read_records(1, &[reference]).unwrap(),
                batch.mutations()
            );
        }
    }

    #[test]
    fn lease_ahead_switch_waits_when_prelease_is_late() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        write(&mut client, &fixture.batch(0));
        assert!(client.live_leased.next.is_none());
        client
            .write_leased_group(
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
        client.close_leased_extent().unwrap();
        assert_eq!(fixture.node.log_for_test(1).receipt_count(), 4);
    }

    #[test]
    fn lease_ahead_close_releases_unused_preleased_extent() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        write(&mut client, &fixture.batch(0));
        client.wait_closing_extent(true).unwrap();
        client.close_leased_extent().unwrap();
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
        client.close_leased_extent().unwrap();
    }

    #[test]
    fn lease_ahead_off_keeps_the_combined_close_and_lease() {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        client
            .write_leased_group(
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
        client.close_leased_extent().unwrap();
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
            timeline.arm(3).unwrap();
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
            let result = client.write_leased_group(1, batches, &timeline, 2);
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
                client.close_leased_extent().unwrap();
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
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            client.progress_appends().unwrap();
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
                let result = client.write_leased_group(1, group, &WriteTimeline::default(), 2);
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
        client.close_leased_extent().unwrap();
    }

    #[test]
    fn control_timeline_captures_leased_requests_and_owner_position_phases() {
        let fixture = Fixture::new(DurabilityTier::Native, 128 * 1024);
        let mut client = fixture.writer();
        let timeline = Arc::new(WriteTimeline::default());
        client.trace_controls(timeline.clone(), 2);
        client.owner_timeline_arm(32).unwrap();
        timeline.arm(32).unwrap();
        let first = client
            .write_leased_group(1, vec![fixture.batch(0)], &timeline, 2)
            .unwrap()[0];
        client.wait_leased_control().unwrap();
        client.submit_leased_adoption().unwrap();
        client.wait_leased_control().unwrap();
        let through = first.appended().address().offset + u64::from(first.appended().address().len);
        client.checkpoint_position(1, 1, through).unwrap();
        client.close_leased_extent().unwrap();
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
}

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
impl super::Client {
    pub(crate) fn bench_local_live_fixture(&mut self, buffered: bool) {
        self.live_write_local_fixture = true;
        // Relaxed: test-only selection precedes queue setup in the serial benchmark.
        BENCH_LOCAL_BUFFERED.store(buffered, std::sync::atomic::Ordering::Relaxed);
    }
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
    pub(crate) fn leased_test_ahead_state(&self) -> (bool, bool) {
        (
            self.live_leased.closing.is_some(),
            self.live_leased.next.is_some(),
        )
    }
    pub(crate) fn leased_test_enable_ahead(&mut self) {
        self.live_config.as_mut().unwrap().lease_ahead = true;
    }
    pub(crate) fn leased_test_largest_close_profile(&self) -> super::live_control::AdoptProfile {
        self.live_leased.largest_close_profile
    }
    pub(crate) fn leased_test_fail_write(&mut self, result: i32) {
        self.live_leased.next_write_result = Some(result);
    }
    pub(crate) fn leased_test_timeout_write(&mut self) {
        if let Some(config) = &mut self.live_config {
            config.io_timeout_ms = 1;
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
    pub(crate) fn leased_test_note_command_adoption_wait(&mut self) {
        self.live_leased.command_adoption_waits += 1;
    }
    pub(crate) fn leased_test_command_adoption_waits(&self) -> u64 {
        self.live_leased.command_adoption_waits
    }
    pub(crate) fn leased_test_corrupt_adoption(&mut self, bad_token: bool) {
        if bad_token {
            self.live_leased.invalidate_adoption_token = true;
        } else {
            self.live_leased.corrupt_next_digest = true;
        }
    }
}

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
impl super::StorageNode {
    pub(crate) fn bench_install_live_target(
        &self,
        target: crate::distributed::nvmeof::live_target::LiveTargetManager,
    ) {
        self.inventory.lock().live_target = Some(target);
    }
}

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
mod leased_rollover_tests {
    use super::*;
    use crate::distributed::durability::DurabilityTier;
    use crate::distributed::write_timeline::WriteTimeline;

    #[test]
    fn lease_ahead_switch_into_a_new_wal_file_rebinds_the_writer_queue() {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let fixture = leased_tests::Fixture::new(tier, 16384);
            let mut client = fixture.writer();
            let mut receipts = Vec::with_capacity(9);
            let mut first_binding = None;
            for sequence in 0..9 {
                receipts.push(
                    client
                        .write_leased_group(
                            1,
                            vec![fixture.batch(sequence)],
                            &WriteTimeline::default(),
                            2,
                        )
                        .unwrap()[0],
                );
                if sequence == 0 {
                    first_binding = Some(client.live_writer.as_ref().unwrap().target.uuid);
                }
                if sequence < 8 {
                    client.wait_closing_extent(true).unwrap();
                }
            }
            let current = client.live_leased.current.as_ref().unwrap();
            assert_eq!(current.lease.extent.segment.file_index, 1);
            assert_eq!(current.lease.extent.start, 32768);
            let writer = client.live_writer.as_ref().unwrap();
            assert!(writer.matches(&current.lease.binding));
            assert_ne!(Some(writer.target.uuid), first_binding);
            let closing = client.live_leased.closing.as_ref().unwrap();
            assert_eq!(closing.lease.extent.segment.file_index, 0);
            // Drive the background close, not a synchronous settle. Its reply
            // must adopt every old-file frame while the new queue stays bound.
            client.wait_closing_extent(false).unwrap();
            assert!(client.live_leased.closing.is_none());
            assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 0);
            assert_eq!(fixture.metrics.snapshot().counts.extent_switch_waits, 0);
            assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 0);
            assert_eq!(fixture.node.log_for_test(1).receipt_count(), 8);
            let mut reader = fixture.client();
            for (sequence, receipt) in receipts.into_iter().enumerate() {
                let batch = fixture.batch(sequence as u64);
                let reference = batch
                    .record_reference(receipt.appended().address(), 0)
                    .unwrap();
                assert_eq!(
                    reader.read_records(1, &[reference]).unwrap(),
                    batch.mutations()
                );
            }
            client.close_leased_extent().unwrap();
            assert_eq!(fixture.node.log_for_test(1).receipt_count(), 9);
        }
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
                    .write_leased_group(1, vec![batch], &WriteTimeline::default(), 2)
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
        let mut reader = fixture.client();
        for (sequence, receipt) in receipts.into_iter().enumerate() {
            let batch = fixture.batch(sequence as u64);
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(
                reader.read_records(1, &[reference]).unwrap(),
                batch.mutations()
            );
        }
        client.close_leased_extent().unwrap();
    }

    #[test]
    fn leased_receipts_use_new_epoch_generation_after_clean_close_and_fence() {
        let fixture = leased_tests::Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        let first = fixture.batch(0);
        client
            .write_leased_group(1, vec![first], &WriteTimeline::default(), 2)
            .unwrap();
        client.close_leased_extent().unwrap();
        client.fence_to(2).unwrap();
        let batch = super::super::tests::batch(&fixture.config, 2, 0);
        let receipts = client
            .write_leased_group(2, vec![batch.clone()], &WriteTimeline::default(), 2)
            .unwrap();
        assert_eq!(receipts[0].appended().address().segment_generation, 2);
        client.settle_leased_extent().unwrap();
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
        client.close_leased_extent().unwrap();
    }
}
