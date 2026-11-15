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

pub(crate) fn target_implementation(
    spdk: &Option<Arc<super::SpdkTarget>>,
) -> super::LiveTargetImplementation {
    if spdk.is_some() {
        super::LiveTargetImplementation::Spdk
    } else {
        super::LiveTargetImplementation::Kernel
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LiveNamespaceIdentity {
    pub namespace_path: PathBuf,
    pub controller_instance: u32,
    pub nqn: String,
    pub uuid: [u8; 16],
    pub nsid: u32,
    pub bytes: u64,
    pub block_size: u32,
    #[serde(default)]
    pub target_implementation: super::LiveTargetImplementation,
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
    #[serde(default)]
    pub target_implementation: super::LiveTargetImplementation,
    #[serde(default)]
    pub spdk: Option<Arc<super::SpdkTarget>>,
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
    reserve_exchanges,
    complete_exchanges,
    lease_exchanges,
    leased_frames,
    leased_eventfd_unavailable,
    /// Waits on the acknowledgement path: at least one leased frame in flight.
    leased_worker_waits,
    /// Timed-out waits on the acknowledgement path: at least one leased frame in flight.
    leased_worker_wait_timeouts,
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
    payload_rpc_requests,
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
    persistence_ns,
    persistence_samples,
    allocation_ns,
    allocation_samples,
    publication_ns,
    publication_samples,
    namespace_setup_ns,
    namespace_setup_samples,
    read_io_ns,
    read_io_samples,
    owner_completion_ns,
    owner_completion_samples,
    wal_persistence_ns,
    wal_persistence_samples,
    wal_namespace_ns,
    wal_namespace_samples,
    frontier_file_ns,
    frontier_file_samples,
    frontier_namespace_ns,
    frontier_namespace_samples,
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
    copy_bytes_avoided;
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
    Persistence,
    Allocation,
    Publication,
    NamespaceSetup,
    ReadIo,
    OwnerCompletion,
    WalPersistence,
    WalNamespace,
    FrontierFile,
    FrontierNamespace,
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
    pub fn record_reserve_exchange(&self) {
        // Relaxed: evidence only, not control-slot synchronization.
        self.reserve_exchanges.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_complete_exchange(&self) {
        // Relaxed: evidence only, not control-slot synchronization.
        self.complete_exchanges.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_lease_exchange(&self) {
        // Relaxed: evidence only, not control-slot synchronization.
        self.lease_exchanges.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_leased_frame(&self, bytes: u64) {
        // Relaxed: these independent evidence counters publish no stream state.
        self.leased_frames.fetch_add(1, Ordering::Relaxed);
        self.leased_bytes.fetch_add(bytes, Ordering::Relaxed);
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
    pub fn record_leased_skip(&self) {
        // Relaxed: evidence only, not a stream ordering barrier.
        self.leased_skip_markers.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_extent_lease(&self) {
        // Relaxed: evidence only, not an allocation publication.
        self.extents_leased.fetch_add(1, Ordering::Relaxed);
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
    pub fn record_payload_rpc(&self) {
        self.payload_rpc_requests.fetch_add(1, Ordering::Relaxed);
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
            LiveTimingStage::Persistence => (&self.persistence_ns, &self.persistence_samples),
            LiveTimingStage::Allocation => (&self.allocation_ns, &self.allocation_samples),
            LiveTimingStage::Publication => (&self.publication_ns, &self.publication_samples),
            LiveTimingStage::NamespaceSetup => {
                (&self.namespace_setup_ns, &self.namespace_setup_samples)
            }
            LiveTimingStage::ReadIo => (&self.read_io_ns, &self.read_io_samples),
            LiveTimingStage::OwnerCompletion => {
                (&self.owner_completion_ns, &self.owner_completion_samples)
            }
            LiveTimingStage::WalPersistence => {
                (&self.wal_persistence_ns, &self.wal_persistence_samples)
            }
            LiveTimingStage::WalNamespace => (&self.wal_namespace_ns, &self.wal_namespace_samples),
            LiveTimingStage::FrontierFile => (&self.frontier_file_ns, &self.frontier_file_samples),
            LiveTimingStage::FrontierNamespace => (
                &self.frontier_namespace_ns,
                &self.frontier_namespace_samples,
            ),
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
mod tests {
    use super::*;

    #[test]
    fn leased_worker_waits_sum_across_producers() {
        let metrics = LiveNvmeMetrics::new();
        let first = metrics.register().unwrap();
        let second = metrics.register().unwrap();
        first.record_leased_worker_wait(false);
        first.record_leased_worker_wait(true);
        second.record_leased_worker_wait(true);
        let counts = metrics.snapshot().counts;
        assert_eq!(counts.leased_worker_waits, 3);
        assert_eq!(counts.leased_worker_wait_timeouts, 2);
    }
}
