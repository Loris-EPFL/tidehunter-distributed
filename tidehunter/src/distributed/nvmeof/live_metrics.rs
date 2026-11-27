//! Per-client counters for live NVMe payloads. Snapshots are weakly consistent
//! while operations run; benchmark phase deltas must be taken after quiescence.
//! Only connection setup and evidence collection take registry locks.
use super::LiveTargetStats;
use super::NvmeofTransport;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const MAX_PRODUCERS: usize = 1 << 20;
const MAX_ROUTES: usize = 1024;
const MAX_FENCES: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LiveNamespaceIdentity {
    pub namespace_path: PathBuf,
    pub controller_instance: u32,
    pub nqn: String,
    pub uuid: [u8; 16],
    pub nsid: u32,
    pub bytes: u64,
    pub block_size: u32,
    /// What the connect-time geometry check read from the controller. `None`
    /// for a local-file fixture that has no kernel controller.
    #[serde(default)]
    pub(crate) controller: Option<LiveControllerEvidence>,
}

/// Sysfs facts of one connected controller, recorded after its queue geometry
/// was verified. The sysfs device is shared by every sysfs entry: only the
/// pair of device and inode names one controller directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct LiveControllerEvidence {
    pub(crate) queue_count: u32,
    pub(crate) sqsize: u32,
    /// The keep-alive timeout the connect requested, in seconds.
    #[serde(default)]
    pub(crate) keep_alive_tmo: u32,
    pub(crate) sysfs_device: u64,
    pub(crate) sysfs_inode: u64,
}

/// Native member identity from an authenticated writable reservation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LiveWriterRoute {
    pub epoch: u64,
    pub file_index: u64,
    #[serde(default)]
    pub file_identity: Option<super::live_target::LiveFileIdentity>,
}

/// Record only after opening and verifying the actual kernel namespace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LiveRouteEvidence {
    pub transport: NvmeofTransport,
    pub address: String,
    pub service: u16,
    pub host_nqn: String,
    pub host_id: [u8; 16],
    pub namespace_identity: LiveNamespaceIdentity,
    #[serde(default)]
    pub writer: Option<LiveWriterRoute>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveFenceEvidence {
    pub partition: u32,
    pub authority_epoch: u64,
    pub target: LiveTargetStats,
}

// `sum` fields add across producers; `max` fields keep the largest value (a
// gauge peak, not an additive count).
macro_rules! counter_fields {
    (sum: $($(#[$attr:meta])* $name:ident),+ $(,)?; max: $($peak:ident),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
        #[serde(default)]
        pub struct LiveNvmeCounts { $($(#[$attr])* pub $name: u64,)+ $(pub $peak: u64,)+ }

        #[repr(align(64))]
        #[derive(Default)]
        pub struct LiveNvmeCounters { $($(#[$attr])* $name: AtomicU64,)+ $($peak: AtomicU64,)+ }

        impl LiveNvmeCounters {
            fn snapshot(&self) -> LiveNvmeCounts {
                LiveNvmeCounts {
                    $($name: self.$name.load(Ordering::Relaxed),)+
                    $($peak: self.$peak.load(Ordering::Relaxed),)+
                }
            }
        }

        impl LiveNvmeCounts {
            fn add(&mut self, other: Self) {
                $(self.$name = self.$name.saturating_add(other.$name);)+
                $(self.$peak = self.$peak.max(other.$peak);)+
            }
        }
    };
}

counter_fields!(
    sum:
    completed_read_ops,
    completed_read_bytes,
    completed_write_ops,
    completed_write_bytes,
    failed_write_ops,
    short_write_ops,
    short_write_bytes,
    lease_exchanges,
    leased_frames,
    leased_eventfd_unavailable,
    /// Waits on the acknowledgement path: at least one leased frame in flight.
    leased_worker_waits,
    /// Timed-out waits on the acknowledgement path: at least one leased frame in flight.
    leased_worker_wait_timeouts,
    leased_flushes,
    /// Flush submission to local CQE harvest; includes worker wake delay.
    leased_flush_ns,
    leased_flush_samples,
    leased_flush_covered_frames,
    read_lane_pin_failures,
    /// Reads that survived a verified controller loss: the lane requeued the
    /// members, evicted the dead lease and re-issued the same bytes on a new
    /// controller (completion requeue, reconnect after a lost namespace open,
    /// or an inline decline handed to the lane).
    read_controller_resumes,
    leased_bytes,
    leased_skip_markers,
    extents_leased,
    adoption_async_exchanges,
    adoption_sync_exchanges,
    adoption_history_full_settles,
    /// Post-submit to validated reply harvest; includes harvest delay.
    adoption_async_ns,
    adoption_async_samples,
    adoption_settle_ns,
    adoption_settle_samples,
    fua_writes,
    /// Submission return to local CQE harvest; includes worker wake delay; not device time.
    leased_write_ns,
    /// Samples of submission return to local CQE harvest; includes worker wake delay; not device time.
    leased_write_samples,
    extent_switch_ns,
    extent_switch_samples,
    lease_reserve_ns,
    lease_namespace_sync_ns,
    lease_publish_ns,
    lease_profile_samples,
    adopt_owner_ns,
    adopt_close_scan_ns,
    adopt_close_scan_bytes,
    adopt_profile_samples,
    extent_preleases,
    extent_switch_waits,
    leased_failed_writes,
    leased_short_writes,
    leased_timed_out_writes,
    leased_full_writes,
    recovery_read_ops,
    recovery_read_bytes,
    metadata_requests,
    namespace_connects,
    unknown_writes,
    errors,
    queue_ns,
    queue_samples,
    submit_ns,
    submit_samples,
    cq_ns,
    cq_samples,
    control_ns,
    control_samples,
    validation_ns,
    validation_samples,
    publication_ns,
    publication_samples,
    namespace_setup_ns,
    namespace_setup_samples,
    read_io_ns,
    read_io_samples,
    // Lane in-flight occupancy, sampled at every flight submission: requests
    // and member operations in flight right after the submission.
    lane_inflight_samples,
    lane_inflight_request_sum,
    lane_inflight_op_sum,
    // CQE observed to result delivered to the caller (validation included).
    delivery_ns,
    delivery_samples,
    // Blocking controller connect (kernel fabrics create and verification) and
    // idle-controller eviction (delete), excluding pool waits.
    controller_connect_ns,
    controller_connect_samples,
    controller_disconnect_ns,
    controller_disconnect_samples,
    // Lane file-grant cache.
    grant_hits,
    grant_misses,
    grant_evictions,
    // Member bytes delivered as slices of the I/O block instead of a copy.
    copy_bytes_avoided,
    /// Complete batches carried by released leased frames (batches per frame).
    leased_frame_batches,
    /// io_uring submit system calls of the write queue (frames per submission).
    write_submit_calls,
    /// Owner-worker group drains that took a batch or stopped at a nonempty
    /// head (empty polls excluded), and the batches they took.
    owner_drains,
    owner_drain_batches,
    /// Why each drain stopped: queue head empty, head ALLOCATED, ordered control
    /// or snapshot bound, group geometry limit, transport credit or occupancy.
    owner_drain_stop_empty,
    owner_drain_stop_allocated,
    owner_drain_stop_fence,
    owner_drain_stop_limit,
    owner_drain_stop_credit,
    /// Leased frame submissions with spare frame capacity that left READY
    /// batches behind (owner-queue head or unmerged queued groups), and those
    /// batches: the packing opportunity.
    leased_submit_ready_left,
    leased_submit_ready_left_batches,
    /// The same for frames that were full (geometry or count limit): not a
    /// packing opportunity.
    leased_submit_ready_left_full,
    leased_submit_ready_left_full_batches,
    /// Submissions of a group that an earlier attempt deferred for credit.
    leased_deferred_submits,
    /// Write-resume episodes (W1): every attempt past the decision checks, the
    /// attempts that rewrote at least one frame, the frames they rewrote, and
    /// the attempts a decision check refused.
    resume_attempts,
    resume_rewrites,
    resume_frames,
    resume_refused;
    max:
    // Peak over each producer's lifetime; aggregation keeps the largest peak.
    max_leased_in_flight,
    lane_inflight_request_max,
    lane_inflight_op_max,
);

/// A zero event count is a real count. A timing with zero samples is unavailable,
/// not a zero-latency observation. These sums are not percentile populations.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LiveNvmeStats {
    #[serde(flatten)]
    pub counts: LiveNvmeCounts,
    pub routes: Vec<LiveRouteEvidence>,
    pub fences: Vec<LiveFenceEvidence>,
    pub route_evidence_dropped: u64,
    pub fence_evidence_dropped: u64,
}

#[derive(Clone, Copy)]
pub enum LiveTimingStage {
    Queue,
    Submit,
    Completion,
    Control,
    Validation,
    Publication,
    NamespaceSetup,
    ReadIo,
    Delivery,
    ControllerConnect,
    ControllerDisconnect,
    LeasedWrite,
    ExtentSwitch,
    AdoptionAsync,
    AdoptionSettle,
}

#[derive(Clone, Copy)]
pub(crate) enum LiveWriteFailure {
    Failed,
    Short,
    TimedOut,
    Full,
}

/// Why one owner-worker drain of the READY owner queue stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnerDrainStop {
    /// The next owner-queue entry is not allocated yet.
    Empty,
    /// The next entry is allocated but its caller has not made it READY.
    Allocated,
    /// The next entry lies at or after an ordered control or snapshot bound.
    Fence,
    /// The next READY batch exceeds group, reference or frame geometry.
    Limit,
    /// Transport byte credit or owner append occupancy is exhausted.
    Credit,
}

impl LiveNvmeCounters {
    pub(crate) fn record_lease_profile(&self, profile: super::super::tcp::LeaseProfile) {
        // Relaxed: owner-local timing evidence publishes no stream state.
        self.lease_reserve_ns
            .fetch_add(profile.reserve_ns, Ordering::Relaxed);
        self.lease_namespace_sync_ns
            .fetch_add(profile.namespace_sync_ns, Ordering::Relaxed);
        self.lease_publish_ns
            .fetch_add(profile.publish_ns, Ordering::Relaxed);
        self.lease_profile_samples.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_adopt_profile(&self, profile: super::super::tcp::AdoptProfile) {
        // Relaxed: owner-local timing evidence publishes no stream state.
        self.adopt_owner_ns
            .fetch_add(profile.adopt_ns, Ordering::Relaxed);
        self.adopt_close_scan_ns
            .fetch_add(profile.close_scan_ns, Ordering::Relaxed);
        self.adopt_close_scan_bytes
            .fetch_add(profile.close_scan_bytes, Ordering::Relaxed);
        self.adopt_profile_samples.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_leased_write_failure(&self, failure: LiveWriteFailure) {
        let counter = match failure {
            LiveWriteFailure::Failed => &self.leased_failed_writes,
            LiveWriteFailure::Short => &self.leased_short_writes,
            LiveWriteFailure::TimedOut => &self.leased_timed_out_writes,
            LiveWriteFailure::Full => &self.leased_full_writes,
        };
        // Relaxed: independent failure evidence, not DMA ownership publication.
        counter.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_lease_exchange(&self) {
        // Relaxed: evidence only, not control-slot synchronization.
        self.lease_exchanges.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_leased_frame(&self, bytes: u64, batches: u64) {
        // Relaxed: these independent evidence counters publish no stream state.
        self.leased_frames.fetch_add(1, Ordering::Relaxed);
        self.leased_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.leased_frame_batches
            .fetch_add(batches, Ordering::Relaxed);
    }
    pub(crate) fn record_write_submit_calls(&self, calls: u64) {
        // Relaxed: syscall evidence, not SQE ownership publication.
        self.write_submit_calls.fetch_add(calls, Ordering::Relaxed);
    }
    pub(crate) fn record_owner_drain(&self, batches: u64, stop: OwnerDrainStop) {
        let counter = match stop {
            OwnerDrainStop::Empty => &self.owner_drain_stop_empty,
            OwnerDrainStop::Allocated => &self.owner_drain_stop_allocated,
            OwnerDrainStop::Fence => &self.owner_drain_stop_fence,
            OwnerDrainStop::Limit => &self.owner_drain_stop_limit,
            OwnerDrainStop::Credit => &self.owner_drain_stop_credit,
        };
        // Relaxed: worker-local evidence; queue order is published by its own atomics.
        self.owner_drains.fetch_add(1, Ordering::Relaxed);
        self.owner_drain_batches
            .fetch_add(batches, Ordering::Relaxed);
        counter.fetch_add(1, Ordering::Relaxed);
    }
    /// One frame submission that left `batches` READY batches unsubmitted.
    pub(crate) fn record_submit_ready_left(&self, batches: u64, frame_full: bool) {
        if batches != 0 {
            let (frames, left) = if frame_full {
                (
                    &self.leased_submit_ready_left_full,
                    &self.leased_submit_ready_left_full_batches,
                )
            } else {
                (
                    &self.leased_submit_ready_left,
                    &self.leased_submit_ready_left_batches,
                )
            };
            // Relaxed: packing evidence only.
            frames.fetch_add(1, Ordering::Relaxed);
            left.fetch_add(batches, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_deferred_submit(&self) {
        // Relaxed: evidence only.
        self.leased_deferred_submits.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_max_leased_in_flight(&self, count: u64) {
        // Relaxed: this peak is evidence, not publication of flight state.
        self.max_leased_in_flight
            .fetch_max(count, Ordering::Relaxed);
    }
    pub(crate) fn record_leased_eventfd_unavailable(&self) {
        // Relaxed: this count does not publish queue ownership.
        self.leased_eventfd_unavailable
            .fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_leased_worker_wait(&self, timed_out: bool) {
        // Relaxed: independent wait evidence, not queue-state publication.
        self.leased_worker_waits.fetch_add(1, Ordering::Relaxed);
        if timed_out {
            self.leased_worker_wait_timeouts
                .fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_leased_flush(&self, covered_frames: u64) {
        // Relaxed: evidence counters publish no durability state.
        self.leased_flushes.fetch_add(1, Ordering::Relaxed);
        self.leased_flush_covered_frames
            .fetch_add(covered_frames, Ordering::Relaxed);
    }
    pub(crate) fn record_leased_flush_completion(&self, ns: u64) {
        // Relaxed: local observation times do not publish the durable prefix.
        self.leased_flush_ns.fetch_add(ns, Ordering::Relaxed);
        self.leased_flush_samples.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_read_lane_pin_failure(&self) {
        // Relaxed: diagnostic evidence, not lane-state publication.
        self.read_lane_pin_failures.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_leased_skip(&self) {
        // Relaxed: evidence only, not a stream ordering barrier.
        self.leased_skip_markers.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_extent_lease(&self) {
        // Relaxed: evidence only, not an allocation publication.
        self.extents_leased.fetch_add(1, Ordering::Relaxed);
    }
    /// One write-resume attempt that reached the reconnect (W1).
    pub(crate) fn record_resume_attempt(&self) {
        // Relaxed: evidence only, not a stream ordering barrier.
        self.resume_attempts.fetch_add(1, Ordering::Relaxed);
    }
    /// One resume that rewrote `frames` retained frames.
    pub(crate) fn record_resume_rewrite(&self, frames: u64) {
        // Relaxed: evidence only; the CQEs establish completion.
        self.resume_rewrites.fetch_add(1, Ordering::Relaxed);
        self.resume_frames.fetch_add(frames, Ordering::Relaxed);
    }
    /// One resume refused by a decision check or by its budget.
    pub(crate) fn record_resume_refused(&self) {
        // Relaxed: evidence only, not a failure publication.
        self.resume_refused.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_extent_prelease(&self) {
        // Relaxed: grant evidence, not publication of the writer state.
        self.extent_preleases.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_extent_switch_wait(&self) {
        // Relaxed: independent stall evidence, not control-slot synchronization.
        self.extent_switch_waits.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_adoption(&self, synchronous: bool) {
        let counter = if synchronous {
            &self.adoption_sync_exchanges
        } else {
            &self.adoption_async_exchanges
        };
        // Relaxed: evidence only, not control-slot synchronization.
        counter.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_adoption_history_full_settle(&self) {
        // Relaxed: success evidence only, not prefix-history publication.
        self.adoption_history_full_settles
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_fua_write(&self) {
        // Relaxed: evidence only; the CQE establishes completion.
        self.fua_writes.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_read(&self, operations: u64, bytes: u64) {
        self.completed_read_ops
            .fetch_add(operations, Ordering::Relaxed);
        self.completed_read_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn record_write(&self, operations: u64, bytes: u64) {
        self.completed_write_ops
            .fetch_add(operations, Ordering::Relaxed);
        self.completed_write_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }
    /// Terminal CQE evidence, independent of the coordinator's earlier timeout.
    pub(crate) fn record_terminal_write_outcomes(&self, failed: u64, short: u64, short_bytes: u64) {
        // Relaxed: terminal event counts are evidence, not CQE publication.
        self.failed_write_ops.fetch_add(failed, Ordering::Relaxed);
        // Relaxed: terminal event counts are evidence, not CQE publication.
        self.short_write_ops.fetch_add(short, Ordering::Relaxed);
        // Relaxed: byte counts are evidence, not DMA ownership transfer.
        self.short_write_bytes
            .fetch_add(short_bytes, Ordering::Relaxed);
    }
    pub fn record_recovery_read(&self, operations: u64, bytes: u64) {
        self.recovery_read_ops
            .fetch_add(operations, Ordering::Relaxed);
        self.recovery_read_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn record_metadata_request(&self) {
        self.metadata_requests.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_namespace_connect(&self) {
        self.namespace_connects.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_unknown_write(&self) {
        self.unknown_writes.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    /// One flight submission: requests and member operations in flight right
    /// after it. Sum and sample count give the mean; the peaks are gauges.
    pub fn record_lane_inflight(&self, requests: u64, operations: u64) {
        self.lane_inflight_samples.fetch_add(1, Ordering::Relaxed);
        self.lane_inflight_request_sum
            .fetch_add(requests, Ordering::Relaxed);
        self.lane_inflight_op_sum
            .fetch_add(operations, Ordering::Relaxed);
        self.lane_inflight_request_max
            .fetch_max(requests, Ordering::Relaxed);
        self.lane_inflight_op_max
            .fetch_max(operations, Ordering::Relaxed);
    }
    pub fn record_grant_hit(&self) {
        self.grant_hits.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_grant_miss(&self) {
        self.grant_misses.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_grant_eviction(&self) {
        self.grant_evictions.fetch_add(1, Ordering::Relaxed);
    }
    /// One read that a verified controller loss could not fail: it is re-issued
    /// on a reconnected controller. Not `read_retries`, which is the
    /// submission-time `TimedOut` retry.
    pub fn record_read_controller_resume(&self) {
        self.read_controller_resumes.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_copy_bytes_avoided(&self, bytes: u64) {
        self.copy_bytes_avoided.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn record_timing(&self, stage: LiveTimingStage, nanoseconds: u64, samples: u64) {
        let (sum, count) = match stage {
            LiveTimingStage::LeasedWrite => (&self.leased_write_ns, &self.leased_write_samples),
            LiveTimingStage::ExtentSwitch => (&self.extent_switch_ns, &self.extent_switch_samples),
            LiveTimingStage::AdoptionAsync => {
                (&self.adoption_async_ns, &self.adoption_async_samples)
            }
            LiveTimingStage::AdoptionSettle => {
                (&self.adoption_settle_ns, &self.adoption_settle_samples)
            }
            LiveTimingStage::Queue => (&self.queue_ns, &self.queue_samples),
            LiveTimingStage::Submit => (&self.submit_ns, &self.submit_samples),
            LiveTimingStage::Completion => (&self.cq_ns, &self.cq_samples),
            LiveTimingStage::Control => (&self.control_ns, &self.control_samples),
            LiveTimingStage::Validation => (&self.validation_ns, &self.validation_samples),
            LiveTimingStage::Publication => (&self.publication_ns, &self.publication_samples),
            LiveTimingStage::NamespaceSetup => {
                (&self.namespace_setup_ns, &self.namespace_setup_samples)
            }
            LiveTimingStage::ReadIo => (&self.read_io_ns, &self.read_io_samples),
            LiveTimingStage::Delivery => (&self.delivery_ns, &self.delivery_samples),
            LiveTimingStage::ControllerConnect => (
                &self.controller_connect_ns,
                &self.controller_connect_samples,
            ),
            LiveTimingStage::ControllerDisconnect => (
                &self.controller_disconnect_ns,
                &self.controller_disconnect_samples,
            ),
        };
        sum.fetch_add(nanoseconds, Ordering::Relaxed);
        count.fetch_add(samples, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct Registry {
    producers: Vec<Arc<LiveNvmeCounters>>,
    routes: Vec<LiveRouteEvidence>,
    fences: Vec<LiveFenceEvidence>,
    route_evidence_dropped: u64,
    fence_evidence_dropped: u64,
}

/// Retains closed-client counters for this database lifetime, so reconnect or
/// cache eviction cannot erase already observed phase activity.
#[derive(Default)]
pub struct LiveNvmeMetrics {
    registry: Mutex<Registry>,
}

impl LiveNvmeMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register(&self) -> std::io::Result<Arc<LiveNvmeCounters>> {
        let mut registry = self.registry.lock();
        if registry.producers.len() == MAX_PRODUCERS {
            return Err(std::io::Error::other(
                "live NVMe metrics producer capacity exhausted",
            ));
        }
        let counters = Arc::new(LiveNvmeCounters::default());
        registry.producers.push(Arc::clone(&counters));
        Ok(counters)
    }

    pub fn record_route(&self, route: LiveRouteEvidence) {
        let mut registry = self.registry.lock();
        if registry.routes.contains(&route) {
            return;
        }
        if registry.routes.len() == MAX_ROUTES {
            registry.route_evidence_dropped = registry.route_evidence_dropped.saturating_add(1);
        } else {
            registry.routes.push(route);
        }
    }

    pub fn record_fence(&self, evidence: LiveFenceEvidence) {
        let mut registry = self.registry.lock();
        if registry.fences.len() == MAX_FENCES {
            registry.fence_evidence_dropped = registry.fence_evidence_dropped.saturating_add(1);
        } else {
            registry.fences.push(evidence);
        }
    }

    pub fn snapshot(&self) -> LiveNvmeStats {
        let registry = self.registry.lock();
        let mut counts = LiveNvmeCounts::default();
        for producer in &registry.producers {
            counts.add(producer.snapshot());
        }
        LiveNvmeStats {
            counts,
            routes: registry.routes.clone(),
            fences: registry.fences.clone(),
            route_evidence_dropped: registry.route_evidence_dropped,
            fence_evidence_dropped: registry.fence_evidence_dropped,
        }
    }
}

#[cfg(test)]
mod tests;
