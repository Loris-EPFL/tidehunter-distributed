//! Opt-in write-diagnostics profile of one storage node and its stage timers.

use crate::distributed::reference_directory::LookupWork;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Opt-in diagnostic timings. Ordinary reads do not take these timestamps.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecordReadProfile {
    pub membership_ns: u64,
    pub wal_range_ns: u64,
    pub verification_decode_ns: u64,
}

/// Snapshot under the existing serialized persistence state lock, not an ACK.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersistenceCapture {
    pub durable_end: u64,
    pub captured_appended_end: u64,
    pub cumulative_appended_groups: u64,
}

/// Opt-in local monotonic elapsed sums, not CPU, device-only or network time.
/// Reset by assigning `Default::default()`; `add` saturates every counter.
/// Stage sums are disjoint subsets of their operation's total, including errors
/// and unwinding. Totals nest inside caller service times and may overlap work on
/// other partitions. Ordinary writes take no diagnostic timestamps.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageWriteProfile {
    /// Append invocations, including retries, rejected requests and unwinding.
    pub append_calls: u64,
    /// Append invocations that did not return success (including unwinding).
    pub append_errors: u64,
    /// Successful all-existing append invocations; no native append occurred.
    pub append_retries: u64,
    /// Newly appended and reference-published complete groups.
    pub appended_groups: u64,
    /// Receipts returned by successful appends, including exact retries.
    pub appended_receipts: u64,
    /// Entire append implementation, including lock waits and failed stages.
    pub append_total_ns: u64,
    /// Time acquiring the append state mutex, excluding validation.
    pub append_state_lock_wait_ns: u64,
    /// Inclusive sum of the five exclusive validation/preparation stages below.
    pub append_validation_ns: u64,
    /// Epoch, state, group/schema and per-batch admission validation.
    #[serde(default)]
    pub append_basic_validation_ns: u64,
    /// Receipt and version lookups, conflict checks and exact-retry handling.
    #[serde(default)]
    pub append_conflict_lookup_ns: u64,
    /// Identity/version lookups, one per batch checked by an append.
    #[serde(default)]
    pub append_conflict_lookups: u64,
    /// Pending-table slots examined by those lookups.
    #[serde(default)]
    pub append_conflict_pending_probes: u64,
    /// Receipt-window reads performed by those lookups.
    #[serde(default)]
    pub append_conflict_history_lookups: u64,
    /// Receipt/descriptor counts, fresh capacity and WAL frame geometry.
    #[serde(default)]
    pub append_geometry_capacity_ns: u64,
    /// Resident metadata capacity checks and credit reservation, including
    /// maintenance initiation on a reservation failure.
    #[serde(default)]
    pub append_metadata_reservation_ns: u64,
    /// Descriptor copies and prepared native-write construction.
    #[serde(default)]
    pub append_descriptor_preparation_ns: u64,
    /// Native writer call through guard drop/storage copy; includes native locks,
    /// allocation and rollover, not just memory copying or device time.
    pub append_wal_ns: u64,
    /// Receipt/map/reference-directory publication after native guard drop.
    pub append_publication_ns: u64,
    /// Persistence invocations, including empty input and durable retries.
    pub persist_calls: u64,
    /// Persistence invocations not returning success (including unwinding).
    pub persist_errors: u64,
    /// Successful invocations requiring no barrier, including empty input.
    pub persist_noops: u64,
    /// Input receipts on successful persistence, including duplicate retries.
    pub persisted_receipts: u64,
    /// Persistence implementation, excluding returned DurableBatch vector creation.
    pub persist_total_ns: u64,
    /// Acquisition of the serialization mutex shared with sealing.
    pub persist_serialization_lock_wait_ns: u64,
    /// All persistence state-mutex acquisitions: initial check, recheck and
    /// post-I/O publication. Excluded from validation/publication spans below.
    pub persist_state_lock_wait_ns: u64,
    /// Receipt/state validation, summed across initial and serialized recheck.
    pub persist_validation_ns: u64,
    /// Actual native WAL file-persistence call, including its internal lookup,
    /// locking and file sync. Not an isolated device-latency measurement.
    pub persist_wal_file_ns: u64,
    /// Actual WAL-directory persistence call (or injected namespace failure).
    pub persist_namespace_ns: u64,
    /// New-file witness creation/write/file-sync span, not a per-Append mean.
    pub persist_inventory_file_ns: u64,
    /// Witness directory-sync span, separate from the native WAL namespace.
    pub persist_inventory_namespace_ns: u64,
    pub inventory_file_persist_calls: u64,
    pub inventory_file_persist_successes: u64,
    pub inventory_namespace_persist_calls: u64,
    pub inventory_namespace_persist_successes: u64,
    /// Live durable-frontier root file and directory barriers, separate from WAL.
    #[serde(default)]
    pub live_frontier_file_ns: u64,
    #[serde(default)]
    pub live_frontier_namespace_ns: u64,
    #[serde(default)]
    pub live_frontier_file_calls: u64,
    #[serde(default)]
    pub live_frontier_file_successes: u64,
    #[serde(default)]
    pub live_frontier_namespace_calls: u64,
    #[serde(default)]
    pub live_frontier_namespace_successes: u64,
    /// Post-I/O error/poison handling, counters and durable-frontier publication.
    pub persist_publication_ns: u64,
    /// Actual native WAL file-persistence calls attempted.
    pub file_persist_calls: u64,
    /// File calls returning success, even if later namespace/publication fails.
    pub file_persist_successes: u64,
    /// Required namespace barriers attempted (including injected failures).
    pub namespace_persist_calls: u64,
    /// Namespace calls returning success; does not alone prove publication.
    pub namespace_persist_successes: u64,
    /// Successful durable-frontier publications after all required barriers.
    pub durable_publications: u64,
    /// Persistence invocations that ran a WAL barrier, and the receipts those
    /// invocations covered together (receipts per sync = second / first).
    #[serde(default)]
    pub persist_barriers: u64,
    #[serde(default)]
    pub persist_barrier_receipts: u64,
    /// Flusher batches that ran one barrier, and the queued append commands they
    /// completed under it (queued commands per barrier = second / first). Set
    /// only by the owner flusher; direct storage calls leave both zero.
    #[serde(default)]
    pub flush_batches: u64,
    #[serde(default)]
    pub flush_batch_commands: u64,
    /// Present only for a non-noop single persist invocation; aggregates omit it.
    pub persistence_capture: Option<PersistenceCapture>,
}

impl StorageWriteProfile {
    pub fn add(&mut self, other: Self) {
        macro_rules! add_fields {
            ($($field:ident),+ $(,)?) => {
                $(self.$field = self.$field.saturating_add(other.$field);)+
            };
        }
        add_fields!(
            append_calls,
            append_errors,
            append_retries,
            appended_groups,
            appended_receipts,
            append_total_ns,
            append_state_lock_wait_ns,
            append_validation_ns,
            append_basic_validation_ns,
            append_conflict_lookup_ns,
            append_conflict_lookups,
            append_conflict_pending_probes,
            append_conflict_history_lookups,
            append_geometry_capacity_ns,
            append_metadata_reservation_ns,
            append_descriptor_preparation_ns,
            append_wal_ns,
            append_publication_ns,
            persist_calls,
            persist_errors,
            persist_noops,
            persisted_receipts,
            persist_total_ns,
            persist_serialization_lock_wait_ns,
            persist_state_lock_wait_ns,
            persist_validation_ns,
            persist_wal_file_ns,
            persist_namespace_ns,
            persist_inventory_file_ns,
            persist_inventory_namespace_ns,
            inventory_file_persist_calls,
            inventory_file_persist_successes,
            inventory_namespace_persist_calls,
            inventory_namespace_persist_successes,
            live_frontier_file_ns,
            live_frontier_namespace_ns,
            live_frontier_file_calls,
            live_frontier_file_successes,
            live_frontier_namespace_calls,
            live_frontier_namespace_successes,
            persist_publication_ns,
            file_persist_calls,
            file_persist_successes,
            namespace_persist_calls,
            namespace_persist_successes,
            durable_publications,
            persist_barriers,
            persist_barrier_receipts,
            flush_batches,
            flush_batch_commands,
        );
        self.persistence_capture = None;
    }

    pub(super) fn add_lookup(&mut self, work: LookupWork) {
        self.append_conflict_lookups = self.append_conflict_lookups.saturating_add(1);
        self.append_conflict_pending_probes = self
            .append_conflict_pending_probes
            .saturating_add(work.pending_probes);
        self.append_conflict_history_lookups = self
            .append_conflict_history_lookups
            .saturating_add(work.history_lookups);
    }
}

/// A stage selector avoids borrowing individual profile fields across storage
/// operations. Drop closes the last stage on every return and during unwinding.
#[derive(Clone, Copy)]
pub(super) enum WriteStage {
    AppendLock,
    AppendBasicValidation,
    AppendConflictLookup,
    AppendGeometryCapacity,
    AppendMetadataReservation,
    AppendDescriptorPreparation,
    AppendWal,
    AppendPublication,
    PersistSerializationLock,
    PersistStateLock,
    PersistValidation,
    PersistFile,
    PersistNamespace,
    PersistInventoryFile,
    PersistInventoryNamespace,
    LiveFrontierFile,
    LiveFrontierNamespace,
    PersistPublication,
}

impl WriteStage {
    fn elapsed(self, profile: &mut StorageWriteProfile) -> &mut u64 {
        match self {
            Self::AppendLock => &mut profile.append_state_lock_wait_ns,
            Self::AppendBasicValidation => &mut profile.append_basic_validation_ns,
            Self::AppendConflictLookup => &mut profile.append_conflict_lookup_ns,
            Self::AppendGeometryCapacity => &mut profile.append_geometry_capacity_ns,
            Self::AppendMetadataReservation => &mut profile.append_metadata_reservation_ns,
            Self::AppendDescriptorPreparation => &mut profile.append_descriptor_preparation_ns,
            Self::AppendWal => &mut profile.append_wal_ns,
            Self::AppendPublication => &mut profile.append_publication_ns,
            Self::PersistSerializationLock => &mut profile.persist_serialization_lock_wait_ns,
            Self::PersistStateLock => &mut profile.persist_state_lock_wait_ns,
            Self::PersistValidation => &mut profile.persist_validation_ns,
            Self::PersistFile => &mut profile.persist_wal_file_ns,
            Self::PersistNamespace => &mut profile.persist_namespace_ns,
            Self::PersistInventoryFile => &mut profile.persist_inventory_file_ns,
            Self::PersistInventoryNamespace => &mut profile.persist_inventory_namespace_ns,
            Self::LiveFrontierFile => &mut profile.live_frontier_file_ns,
            Self::LiveFrontierNamespace => &mut profile.live_frontier_namespace_ns,
            Self::PersistPublication => &mut profile.persist_publication_ns,
        }
    }
}

pub(super) struct WriteTiming<'a> {
    profile: Option<&'a mut StorageWriteProfile>,
    pub(super) started: Option<Instant>,
    pub(super) stage_started: Option<Instant>,
    stage: Option<WriteStage>,
    append: bool,
    success: bool,
}

impl<'a> WriteTiming<'a> {
    pub(super) fn new(profile: Option<&'a mut StorageWriteProfile>, append: bool) -> Self {
        let started = profile.as_ref().map(|_| Instant::now());
        let mut timing = Self {
            profile,
            started,
            stage_started: started,
            stage: None,
            append,
            success: false,
        };
        timing.record(|profile| {
            let calls = if append {
                &mut profile.append_calls
            } else {
                &mut profile.persist_calls
            };
            *calls = calls.saturating_add(1);
        });
        timing
    }

    pub(super) fn record(&mut self, update: impl FnOnce(&mut StorageWriteProfile)) {
        if let Some(profile) = self.profile.as_deref_mut() {
            update(profile);
        }
    }

    pub(super) fn stage(&mut self, next: Option<WriteStage>) {
        if let Some(profile) = self.profile.as_deref_mut() {
            let now = Instant::now();
            if let (Some(stage), Some(started)) = (self.stage, self.stage_started) {
                let ns = duration_ns(now.duration_since(started));
                let elapsed = stage.elapsed(profile);
                *elapsed = elapsed.saturating_add(ns);
                if matches!(
                    stage,
                    WriteStage::AppendBasicValidation
                        | WriteStage::AppendConflictLookup
                        | WriteStage::AppendGeometryCapacity
                        | WriteStage::AppendMetadataReservation
                        | WriteStage::AppendDescriptorPreparation
                ) {
                    profile.append_validation_ns = profile.append_validation_ns.saturating_add(ns);
                }
            }
            self.stage_started = Some(now);
            self.stage = next;
        }
    }

    pub(super) fn success(&mut self, receipts: usize) {
        self.success = true;
        let append = self.append;
        self.record(|profile| {
            let count = if append {
                &mut profile.appended_receipts
            } else {
                &mut profile.persisted_receipts
            };
            *count = count.saturating_add(receipts as u64);
        });
    }
}

impl Drop for WriteTiming<'_> {
    fn drop(&mut self) {
        self.stage(None);
        if let (Some(profile), Some(started)) = (self.profile.as_deref_mut(), self.started) {
            let (total, errors) = if self.append {
                (&mut profile.append_total_ns, &mut profile.append_errors)
            } else {
                (&mut profile.persist_total_ns, &mut profile.persist_errors)
            };
            *total = total.saturating_add(duration_ns(started.elapsed()));
            *errors = errors.saturating_add(u64::from(!self.success));
        }
    }
}

pub(super) fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}
