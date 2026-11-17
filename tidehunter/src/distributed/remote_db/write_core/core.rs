//! Shared write engine. Callers allocate/encode; owner workers publish durable
//! batches. Checkpoint persistence owns no foreground or publication lock.

use super::super::receipt_history::ReceiptHistory;
use super::super::{
    AppendedBatch, AuthorityManifest, AuthorityWriteStats, BackgroundCheckpoint, BatchId,
    CheckpointDiagnostic, CheckpointDiagnosticSummary, ControlHandle, IndexReadCut, LocalDbError,
    LocalDbStats, MutationVersion, NativeIndex, ReadCache, ReadService, RecentReceipt,
    RecoveryStats, RemoteDatabaseConfig, ShutdownStats, StaticInventory, StreamPosition,
    WriteProfileSnapshot, WriteTimeline, concurrent, failure_context, record_first_failure,
};
use super::dependencies::DependencyRegistry;
use super::identity::{IdentityClass, IdentityEntry, IdentityTable};
use super::owner_sender::OwnerSender;
use super::sequencer::{Resolution, Sequencer};
use crate::key_shape::KeyShape;
use parking_lot::{Condvar, Mutex};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque, hash_map::RandomState};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
#[cfg(test)]
use std::sync::mpsc;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
};

/// Only D9 publisher-owned fields cross this boundary. Returning disjoint field
/// references permits capture to scan known receipts while updating stream cuts
/// without cloning maps or exposing the legacy authority's clients/admission.
pub(in crate::distributed::remote_db) struct PublisherCheckpoint<'a> {
    pub known: &'a mut HashMap<BatchId, AppendedBatch>,
    pub receipt_history: &'a mut Option<ReceiptHistory>,
    pub stream_last: &'a mut BTreeMap<(u32, u64), (u64, u32)>,
    pub positions: &'a mut BTreeMap<(u32, u64), StreamPosition>,
    pub committed_complete: &'a mut BTreeSet<(u32, u64)>,
    pub recent: &'a mut VecDeque<RecentReceipt>,
    pub latest: &'a mut Option<MutationVersion>,
    pub live_latest: &'a mut Option<MutationVersion>,
    pub snapshot_generation: &'a mut u64,
    pub manifest: &'a mut AuthorityManifest,
    pub retained_epochs: &'a mut Vec<Vec<u64>>,
    pub checkpoint_tail_batches: &'a mut usize,
    pub checkpoint_tail_bytes: &'a mut usize,
    pub background_checkpoint: &'a mut Option<BackgroundCheckpoint>,
    pub checkpoint_diagnostics: &'a mut Option<VecDeque<CheckpointDiagnostic>>,
    pub checkpoint_diagnostic_summary: &'a mut CheckpointDiagnosticSummary,
    pub next_checkpoint_diagnostic: &'a mut u64,
    pub dropped_checkpoint_diagnostics: &'a mut u64,
    pub last_checkpoint_diagnostic: &'a mut Option<CheckpointDiagnostic>,
}

pub(in crate::distributed::remote_db) struct PublisherState {
    /// Constant sparse-view quota, reserved once at open. Only the serialized
    /// publication path (writer → publisher) installs views from it.
    pub view_budget: crate::distributed::native_index::StandingViewBudget,
    pub known: HashMap<BatchId, AppendedBatch>,
    pub receipt_history: Option<ReceiptHistory>,
    pub stream_last: BTreeMap<(u32, u64), (u64, u32)>,
    pub positions: BTreeMap<(u32, u64), StreamPosition>,
    pub committed_complete: BTreeSet<(u32, u64)>,
    pub recent: VecDeque<RecentReceipt>,
    pub latest: Option<MutationVersion>,
    pub live_latest: Option<MutationVersion>,
    pub snapshot_generation: u64,
    pub manifest: AuthorityManifest,
    pub retained_epochs: Vec<Vec<u64>>,
    pub checkpoint_tail_batches: usize,
    pub checkpoint_tail_bytes: usize,
    pub background_checkpoint: Option<BackgroundCheckpoint>,
    pub checkpoint_diagnostics: Option<VecDeque<CheckpointDiagnostic>>,
    pub checkpoint_diagnostic_summary: CheckpointDiagnosticSummary,
    pub next_checkpoint_diagnostic: u64,
    pub dropped_checkpoint_diagnostics: u64,
    pub last_checkpoint_diagnostic: Option<CheckpointDiagnostic>,
    /// Exclusive contiguous publication frontier, including an empty epoch 0.
    pub published_next: u64,
    pub upper: Option<MutationVersion>,
    pub dependents: HashMap<u64, Vec<u64>>,
}

impl PublisherState {
    pub(in crate::distributed::remote_db) fn checkpoint_state(
        &mut self,
    ) -> PublisherCheckpoint<'_> {
        PublisherCheckpoint {
            known: &mut self.known,
            receipt_history: &mut self.receipt_history,
            stream_last: &mut self.stream_last,
            positions: &mut self.positions,
            committed_complete: &mut self.committed_complete,
            recent: &mut self.recent,
            latest: &mut self.latest,
            live_latest: &mut self.live_latest,
            snapshot_generation: &mut self.snapshot_generation,
            manifest: &mut self.manifest,
            retained_epochs: &mut self.retained_epochs,
            checkpoint_tail_batches: &mut self.checkpoint_tail_batches,
            checkpoint_tail_bytes: &mut self.checkpoint_tail_bytes,
            background_checkpoint: &mut self.background_checkpoint,
            checkpoint_diagnostics: &mut self.checkpoint_diagnostics,
            checkpoint_diagnostic_summary: &mut self.checkpoint_diagnostic_summary,
            next_checkpoint_diagnostic: &mut self.next_checkpoint_diagnostic,
            dropped_checkpoint_diagnostics: &mut self.dropped_checkpoint_diagnostics,
            last_checkpoint_diagnostic: &mut self.last_checkpoint_diagnostic,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(in crate::distributed::remote_db) struct CoreStats {
    pub recovery: RecoveryStats,
    pub write: AuthorityWriteStats,
    pub shutdown: ShutdownStats,
    pub database: LocalDbStats,
    pub pipeline: super::super::PipelineStats,
}

/// Releases are computed during checkpoint adoption, then applied only after
/// dropping index writer/publisher. Recent-window identities are not charged.
#[derive(Default)]
pub(in crate::distributed::remote_db) struct CheckpointCredits {
    pub identities: usize,
    pub replay_batches: usize,
    pub replay_bytes: usize,
}

pub(in crate::distributed::remote_db) type CheckpointJobs = Vec<(Arc<OwnerSender>, u32, Vec<u64>)>;

#[cfg(test)]
pub(in crate::distributed::remote_db) struct CheckpointTestHooks {
    pub interruption: Option<u8>,
    pub entered: Option<mpsc::SyncSender<()>>,
    pub pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub prefix_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub corrupt_proof: bool,
    pub adoption_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub cleanup_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
}

/// Recovered, validated ownership transferred once at the end of open.
pub(in crate::distributed::remote_db) struct OpenState {
    pub config: RemoteDatabaseConfig,
    pub shape: KeyShape,
    pub path: PathBuf,
    pub epoch: u64,
    pub index: Arc<NativeIndex>,
    pub partition_indexes: HashMap<u32, usize>,
    pub inventory: StaticInventory,
    pub reads: Arc<ReadService>,
    pub read_cache: Arc<ReadCache>,
    pub write_profile: Arc<Mutex<WriteProfileSnapshot>>,
    pub write_timeline: Arc<WriteTimeline>,
    pub clients: Vec<super::super::ClientWorker>,
    pub operational_sampler: super::super::OperationalReadIoSampler,
    pub transport_pool: Option<Arc<super::super::RemoteTransportPool>>,
    pub transport_key: Option<[u8; 32]>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub live_pool: Option<Arc<crate::distributed::nvmeof::live_pool::LiveControllerPool>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub live_metrics: Option<Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics>>,
    pub publisher: PublisherState,
    pub stats: CoreStats,
    pub concurrent_failure: Arc<AtomicBool>,
    pub first_failure: Arc<OnceLock<super::super::FirstFailure>>,
}

#[cfg(test)]
#[derive(Default)]
pub(in crate::distributed::remote_db) struct TestHooks {
    pub checkpoint_interruption: Option<u8>,
    pub retire_preflight_busy_once: bool,
    pub after_grant: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub before_preparation: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub fail_prepared_operation: Option<[u8; 16]>,
    pub concurrent_progress: Option<mpsc::SyncSender<super::super::PipelineStats>>,
    pub drain_wait_entered: Option<mpsc::SyncSender<()>>,
    pub before_checkpoint_io: Option<mpsc::SyncSender<()>>,
    pub checkpoint_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub checkpoint_prefix_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub corrupt_checkpoint_proof: bool,
    pub checkpoint_adoption_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub checkpoint_cleanup_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub panic_after_concurrent_admissions: Option<u64>,
    pub skip_shutdown_checkpoint: bool,
    pub before_publish: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub before_release: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub fail_publication: Option<u64>,
}

/// Lock order for the final engine: identity → ascending key shards → owner
/// order, release all, then index writer → publisher → slot data. Admission is
/// never held with these locks. Transport I/O, encoding, checkpoint persistence
/// and worker joins happen after all foreground/publication guards are dropped.
pub(in crate::distributed::remote_db) struct WriteCore {
    pub config: RemoteDatabaseConfig,
    pub shape: KeyShape,
    pub path: PathBuf,
    pub epoch: u64,
    pub index: Arc<NativeIndex>,
    pub partition_indexes: HashMap<u32, usize>,
    pub inventory: StaticInventory,
    pub reads: Arc<ReadService>,
    pub read_cache: Arc<ReadCache>,
    pub write_profile: Arc<Mutex<WriteProfileSnapshot>>,
    pub write_timeline: Arc<WriteTimeline>,
    pub publisher: Mutex<PublisherState>,
    pub floor_changed: Condvar,
    pub(crate) retirement: Mutex<()>,
    pub stats: Mutex<CoreStats>,
    pub read_resolve_ns: AtomicU64,
    pub value_read_ns: AtomicU64,
    pub admission: Arc<concurrent::Admission>,
    pub concurrent_failure: Arc<AtomicBool>,
    pub first_failure: Arc<OnceLock<super::super::FirstFailure>>,
    pub stopped: AtomicBool,
    pub closing: AtomicBool,
    pub clients: Vec<super::super::ClientWorker>,
    pub operational_sampler: super::super::OperationalReadIoSampler,
    pub transport_pool: Option<Arc<super::super::RemoteTransportPool>>,
    pub transport_key: Option<[u8; 32]>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub live_pool: Option<Arc<crate::distributed::nvmeof::live_pool::LiveControllerPool>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub live_metrics: Option<Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics>>,
    #[cfg(test)]
    pub test_hooks: Mutex<TestHooks>,
    owner_controls: Vec<(u32, ControlHandle)>,
    pub(super) sequencer: Sequencer,
    pub(super) dependencies: DependencyRegistry,
    pub(super) identities: IdentityTable,
}

impl WriteCore {
    pub(in crate::distributed::remote_db) fn new(open: OpenState) -> Arc<Self> {
        let parallelism = crate::parallelism::available();
        let admission = concurrent::Admission::for_open(&open);
        let hash = RandomState::new();
        let identities = IdentityTable::new(parallelism, hash.clone());
        for receipt in &open.publisher.recent {
            identities.with_operation(receipt.id.operation, |entries| {
                entries.insert(
                    receipt.id.operation,
                    IdentityEntry {
                        version: receipt.version,
                        digest: Some(receipt.digest),
                        seq: receipt.version.sequence,
                        class: IdentityClass::Recent,
                    },
                );
            });
        }
        for receipt in open.publisher.known.values() {
            identities.with_operation(receipt.id().operation, |entries| {
                entries.insert(
                    receipt.id().operation,
                    IdentityEntry {
                        version: receipt.version(),
                        digest: Some(receipt.digest()),
                        seq: receipt.version().sequence,
                        class: IdentityClass::Suffix,
                    },
                );
            });
        }
        let capacity = open.config.max_pending_batches;
        let owner_controls = open
            .clients
            .iter()
            .zip(&open.config.endpoints)
            .map(|(worker, endpoint)| (endpoint.partition, worker.control_handle()))
            .collect();
        Arc::new(Self {
            config: open.config,
            shape: open.shape,
            path: open.path,
            epoch: open.epoch,
            index: open.index,
            partition_indexes: open.partition_indexes,
            inventory: open.inventory,
            reads: open.reads,
            read_cache: open.read_cache,
            write_profile: open.write_profile,
            write_timeline: open.write_timeline,
            floor_changed: Condvar::new(),
            retirement: Mutex::new(()),
            stats: Mutex::new(open.stats),
            admission,
            read_resolve_ns: AtomicU64::new(0),
            value_read_ns: AtomicU64::new(0),
            concurrent_failure: open.concurrent_failure,
            first_failure: open.first_failure,
            stopped: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            owner_controls,
            clients: open.clients,
            operational_sampler: open.operational_sampler,
            transport_pool: open.transport_pool,
            transport_key: open.transport_key,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_pool: open.live_pool,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_metrics: open.live_metrics,
            #[cfg(test)]
            test_hooks: Mutex::new(TestHooks::default()),
            sequencer: Sequencer::new(capacity),
            dependencies: DependencyRegistry::new(parallelism, hash),
            identities,
            publisher: Mutex::new(open.publisher),
        })
    }

    pub(in crate::distributed::remote_db) fn ensure_writable(&self) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        // Acquire pairs with shutdown's Release store under every owner order.
        if self.closing.load(Ordering::Acquire) {
            return Err(LocalDbError::new("write authority closed"));
        }
        Ok(())
    }

    pub(super) fn close_allocations(&self) -> Option<u64> {
        let owners: Vec<_> = self
            .clients
            .iter()
            .enumerate()
            .map(|(owner, worker)| (owner, worker.queue.as_ref()))
            .collect();
        let first = super::owner_queue::OwnerQueue::with_owners(&owners, |_| {
            // Every allocation holds at least one of these orders while
            // checking closing and fetch_add. It either allocated before this
            // store (and is included in next below), or sees closing after it.
            !self.closing.swap(true, Ordering::AcqRel)
        });
        if !first {
            return None;
        }
        self.admission.close();
        Some(self.sequencer.next())
    }

    pub(in crate::distributed::remote_db) fn ensure_healthy(&self) -> Result<(), LocalDbError> {
        // Acquire observes the first fail-stop transition before control work.
        if self.concurrent_failure.load(Ordering::Acquire)
            || self.reads.failed()
            || self.stopped.load(Ordering::Acquire)
        {
            Err(LocalDbError::new(failure_context(
                &self.first_failure,
                "write core requires recovery",
            )))
        } else {
            Ok(())
        }
    }

    /// Returns whether the QueueReady hint was coalesced: `true` when the
    /// bounded command channel was full (the worker already has pending
    /// commands and rechecks its queue), `false` when a hint was sent.
    pub(super) fn wake_owner(&self, owner: usize) -> Result<bool, LocalDbError> {
        let handle = &self
            .owner_controls
            .get(owner)
            .ok_or_else(|| LocalDbError::new("unknown write owner"))?
            .1;
        let sender = handle
            .sender
            .upgrade()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?;
        let coalesced = match sender.try_send(super::super::Command::QueueReady) {
            Ok(()) => false,
            Err(std::sync::mpsc::TrySendError::Full(_)) => true,
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                return Err(LocalDbError::new("TCP worker stopped"));
            }
        };
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        handle.wake_worker()?;
        Ok(coalesced)
    }

    /// Check the in-flight table before snapshotting persisted retry authority.
    /// Checkpoint installs the new history root before removing covered table
    /// entries; cloning only after a shard miss therefore cannot miss a retired
    /// identity. The allocation critical section must recheck the shard after
    /// this no-lock I/O, because a concurrent first writer can insert meanwhile.
    pub(super) fn lookup_identity(
        &self,
        operation: [u8; 16],
    ) -> Result<Option<IdentityEntry>, LocalDbError> {
        self.lookup_identity_at(operation)
            .map(|(identity, _)| identity)
    }

    /// `lookup_identity` plus the identity shard's retirement generation,
    /// captured with the table miss and before the history snapshot. A miss
    /// is a valid first-write decision at allocation only while the generation
    /// is unchanged: a concurrent writer of the same operation may otherwise
    /// have published and been retired into a newer history root meanwhile.
    pub(super) fn lookup_identity_at(
        &self,
        operation: [u8; 16],
    ) -> Result<(Option<IdentityEntry>, u64), LocalDbError> {
        self.ensure_healthy()?;
        let (identity, retirements) = self.identities.lookup(operation);
        if identity.is_some()
            || self.config.history_persistence
                == crate::distributed::storage::HistoryPersistence::WalReplay
        {
            return Ok((identity, retirements));
        }
        let history = self
            .publisher
            .lock()
            .checkpoint_state()
            .receipt_history
            .clone();
        let receipt = history
            .as_ref()
            .map(|history| {
                history.lookup(BatchId {
                    database: self.config.storage.database,
                    operation,
                })
            })
            .transpose()
            .inspect_err(|error| self.fail(LocalDbError::new(error.message())))?
            .flatten();
        self.ensure_healthy()?;
        let identity = receipt.map(|receipt| IdentityEntry {
            version: receipt.version(),
            digest: Some(receipt.digest()),
            seq: receipt.version().sequence,
            class: IdentityClass::Recent,
        });
        Ok((identity, retirements))
    }

    /// Resident completed/in-flight write bookkeeping, excluding pageable
    /// receipt files, allocator overhead and the separately reported index.
    /// Publisher, identity and key shards are locked one at a time.
    pub(in crate::distributed::remote_db) fn metadata_history_bytes(&self) -> usize {
        let publisher = Self::publisher_resident_bytes(&self.publisher.lock());
        publisher + self.identities.resident_bytes() + self.dependencies.resident_bytes()
    }

    /// Metrics variant: contention on the publisher or on any identity or key
    /// shard makes the value unavailable; sampling never blocks a writer.
    pub(in crate::distributed::remote_db) fn try_metadata_history_bytes(&self) -> Option<usize> {
        let publisher = Self::publisher_resident_bytes(&*self.publisher.try_lock()?);
        Some(
            publisher
                + self.identities.try_resident_bytes()?
                + self.dependencies.try_resident_bytes()?,
        )
    }

    fn publisher_resident_bytes(publisher: &PublisherState) -> usize {
        publisher.known.capacity()
            * (std::mem::size_of::<BatchId>() + std::mem::size_of::<AppendedBatch>())
            + publisher
                .receipt_history
                .as_ref()
                .map_or(0, ReceiptHistory::cached_metadata_bytes)
            + publisher.recent.capacity() * std::mem::size_of::<RecentReceipt>()
    }

    pub(super) fn release_sequence(&self, sequence: u64) -> Result<(), LocalDbError> {
        let slots = self.sequencer.complete_release(sequence);
        self.admission.return_slots(slots).inspect_err(|error| {
            self.fail(LocalDbError::new(error.message()));
        })
    }

    pub(in crate::distributed::remote_db) fn next_sequence(&self) -> u64 {
        self.sequencer.next()
    }

    /// A failed wait carries the uncertain version named by the first causal
    /// failure record, which is written before any failure flag is stored.
    pub(in crate::distributed::remote_db) fn wait_published_before(
        &self,
        next: u64,
    ) -> Result<(), LocalDbError> {
        self.wait_published_inner(next).map_err(|mut error| {
            if error.uncertain_version.is_none() {
                error.uncertain_version = self
                    .first_failure
                    .get()
                    .and_then(super::super::FirstFailure::version);
            }
            error
        })
    }

    /// The old authority filled the post-publish read cache and installed the
    /// outcome before any drain could return. Workers do that after dropping
    /// writer → publisher, so after the published prefix covers `next` this
    /// also waits, per slot, until each generation below `next` has its
    /// outcome (installed after its read-cache fill) or has been reused.
    fn wait_published_inner(&self, next: u64) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        let deadline = std::time::Instant::now() + self.config.timeout;
        let mut publisher = self.publisher.lock();
        while publisher.published_next < next {
            self.ensure_healthy()?;
            if self
                .floor_changed
                .wait_until(&mut publisher, deadline)
                .timed_out()
                && publisher.published_next < next
            {
                return Err(LocalDbError::new("published prefix wait deadline expired"));
            }
        }
        drop(publisher);
        // Live sequences span at most the ring capacity above reuse_next. The
        // resolver sets the outcome under slot.data and finalize_slot notifies
        // slot.done under it: the standard predicate wait, no lost wakeup.
        // Allocation resets the outcome under slot.data before publishing a
        // new generation, so a matching generation never shows a stale outcome.
        for sequence in (self.sequencer.reuse_next()..next).take(self.sequencer.capacity()) {
            let slot = self.sequencer.slot(sequence);
            let mut data = slot.data.lock();
            loop {
                let current = self
                    .sequencer
                    .generation(sequence)
                    .is_some_and(|generation| generation == sequence);
                if !current || data.outcome.is_some() {
                    break;
                }
                self.ensure_healthy()?;
                if slot.done.wait_until(&mut data, deadline).timed_out()
                    && self.sequencer.generation(sequence) == Some(sequence)
                    && data.outcome.is_none()
                {
                    return Err(LocalDbError::new("published prefix wait deadline expired"));
                }
            }
        }
        self.ensure_healthy()
    }

    pub(in crate::distributed::remote_db) fn checkpoint_jobs(
        &self,
        retained_epochs: &[Vec<u64>],
    ) -> Result<CheckpointJobs, LocalDbError> {
        if retained_epochs.len() != self.owner_controls.len() {
            return Err(LocalDbError::new("checkpoint owner inventory differs"));
        }
        self.owner_controls
            .iter()
            .zip(retained_epochs)
            .map(|((owner, handle), epochs)| {
                Ok((
                    handle
                        .sender
                        .upgrade()
                        .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?,
                    *owner,
                    epochs.clone(),
                ))
            })
            .collect()
    }

    pub(in crate::distributed::remote_db) fn control_owner<T: Send + 'static>(
        &self,
        owner: usize,
        operation: impl FnOnce(
            &mut crate::distributed::tcp::Client,
        ) -> Result<T, crate::distributed::tcp::TcpError>
        + Send
        + 'static,
    ) -> Result<T, LocalDbError> {
        self.owner_controls
            .get(owner)
            .ok_or_else(|| LocalDbError::new("unknown checkpoint owner"))?
            .1
            .control(operation)
    }

    pub(in crate::distributed::remote_db) fn retire_checkpoint_identities(
        &self,
        cut: Option<MutationVersion>,
        recent: &VecDeque<RecentReceipt>,
    ) -> usize {
        self.identities
            .checkpoint(cut.map(|version| version.batch), recent)
    }

    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn take_checkpoint_test_hooks(
        &self,
    ) -> CheckpointTestHooks {
        let mut hooks = self.test_hooks.lock();
        CheckpointTestHooks {
            interruption: hooks.checkpoint_interruption,
            entered: hooks.before_checkpoint_io.clone(),
            pause: hooks.checkpoint_pause.take(),
            prefix_pause: hooks.checkpoint_prefix_pause.take(),
            corrupt_proof: hooks.corrupt_checkpoint_proof,
            adoption_pause: hooks.checkpoint_adoption_pause.take(),
            cleanup_pause: hooks.checkpoint_cleanup_pause.take(),
        }
    }

    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn take_retirement_preflight_busy(&self) -> bool {
        std::mem::take(&mut self.test_hooks.lock().retire_preflight_busy_once)
    }

    pub(in crate::distributed::remote_db) fn request_checkpoint(
        self: &Arc<Self>,
    ) -> Result<(), LocalDbError> {
        super::super::checkpoint_lifecycle::core_request_checkpoint(self)
    }

    pub(in crate::distributed::remote_db) fn start_checkpoint(
        self: &Arc<Self>,
    ) -> Result<(), LocalDbError> {
        let result = super::super::checkpoint_lifecycle::core_start_checkpoint(self);
        self.checkpoint_result(result)
    }

    pub(in crate::distributed::remote_db) fn progress_checkpoint(
        self: &Arc<Self>,
        wait: bool,
    ) -> Result<(), LocalDbError> {
        let result = super::super::checkpoint_lifecycle::core_progress_checkpoint(self, wait);
        self.checkpoint_result(result)
    }

    pub(in crate::distributed::remote_db) fn commit_checkpoint(
        self: &Arc<Self>,
    ) -> Result<(), LocalDbError> {
        let result = super::super::checkpoint_lifecycle::core_commit_checkpoint(self);
        self.checkpoint_result(result)
    }

    pub(in crate::distributed::remote_db) fn retire_history_before(
        self: &Arc<Self>,
        floor: IndexReadCut,
    ) -> Result<(), LocalDbError> {
        super::super::checkpoint_lifecycle::core_retire_history_before(self, floor)
    }

    fn checkpoint_result(&self, result: Result<(), LocalDbError>) -> Result<(), LocalDbError> {
        if let Err(error) = &result {
            self.fail(LocalDbError::new(error.message()));
        }
        result
    }

    pub(in crate::distributed::remote_db) fn release_checkpoint_credits(
        &self,
        credits: CheckpointCredits,
    ) -> Result<(), LocalDbError> {
        self.admission.release_checkpoint(credits)?;
        self.admission.regrant();
        Ok(())
    }

    /// Called before dropping writer→publisher on a publication failure.
    /// Only the causal cell and atomic serving gates change here; notification
    /// locks, Admission and owner.order belong to finish_failure instead.
    pub(in crate::distributed::remote_db) fn begin_failure(&self, cause: &LocalDbError) {
        record_first_failure(
            &self.first_failure,
            None,
            cause.uncertain_version,
            "write_core",
            cause,
        );
        // Release fences all later reads/admissions before a partial native
        // index mutation can escape the publication critical section.
        self.concurrent_failure.store(true, Ordering::Release);
        self.reads.begin_poison();
    }

    /// Idempotent completion, called with no identity/key/owner/index/publisher
    /// lock held. Each outcome is installed under its own condition predicate.
    pub(super) fn finish_failure(&self) {
        self.admission.close();
        self.reads.poison();
        let next = self.sequencer.next();
        for sequence in (self.sequencer.reuse_next()..next).take(self.sequencer.capacity()) {
            let slot = self.sequencer.slot(sequence);
            let mut data = slot.data.lock();
            let Some(resolution) = self.sequencer.resolution(sequence) else {
                continue;
            };
            if resolution == Resolution::Aborted {
                continue;
            }
            if matches!(
                resolution,
                Resolution::Allocated | Resolution::Ready | Resolution::Durable
            ) && let Err(error) =
                self.sequencer
                    .transition(sequence, resolution, Resolution::Unknown)
            {
                record_first_failure(&self.first_failure, None, None, "failure_slot", &error);
            }
            if data.outcome.is_none() {
                data.outcome = Some(Err(LocalDbError::unknown(
                    crate::distributed::types::BatchVersion {
                        epoch: self.epoch,
                        sequence,
                    },
                    failure_context(&self.first_failure, "write outcome requires recovery"),
                )));
            }
            slot.done.notify_all();
        }
        {
            // Pair the failure wake with the floor waiter's predicate mutex,
            // so failure between its health check and wait cannot be lost.
            let _publisher = self.publisher.lock();
            self.floor_changed.notify_all();
        }
        for (_, owner) in &self.owner_controls {
            if let Some(sender) = owner.sender.upgrade() {
                let _ = sender.try_send(super::super::Command::QueueReady);
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                let _ = owner.wake_worker();
            }
        }
    }

    pub(in crate::distributed::remote_db) fn fail(&self, cause: LocalDbError) {
        self.begin_failure(&cause);
        self.finish_failure();
    }
}

/// Exact test observations of the shared write state. Each helper takes at
/// most one lock at a time and never holds it across another call.
#[cfg(test)]
impl WriteCore {
    /// True when this epoch's version is published in its live slot, or has
    /// left the ring below the published floor (aborts are not distinguished
    /// after reuse; tests that abort inspect the slot before reuse).
    pub(in crate::distributed::remote_db) fn is_published(
        &self,
        version: crate::distributed::types::BatchVersion,
    ) -> bool {
        if version.epoch != self.epoch {
            return false;
        }
        if self.sequencer.generation(version.sequence) == Some(version.sequence) {
            return self.sequencer.resolution(version.sequence) == Some(Resolution::Published);
        }
        version.sequence < self.sequencer.reuse_next()
            && version.sequence < self.publisher.lock().published_next
    }

    pub(in crate::distributed::remote_db) fn published_next(&self) -> u64 {
        self.publisher.lock().published_next
    }

    /// Allocated generations whose slot has not yet returned to the ring.
    pub(in crate::distributed::remote_db) fn retained_slots(&self) -> u64 {
        self.sequencer.next() - self.sequencer.reuse_next()
    }

    pub(in crate::distributed::remote_db) fn pending_dependencies(&self) -> usize {
        self.dependencies.len()
    }

    pub(in crate::distributed::remote_db) fn active_identities(&self) -> usize {
        self.identities.count(IdentityClass::Active)
    }

    /// True when the identity table holds `operation` in the recent window.
    pub(in crate::distributed::remote_db) fn recent_identity(&self, operation: [u8; 16]) -> bool {
        self.identities.with_operation(operation, |entries| {
            entries
                .get(&operation)
                .is_some_and(|entry| entry.class == IdentityClass::Recent)
        })
    }

    /// Total bucket capacity across every key shard.
    pub(in crate::distributed::remote_db) fn dependency_capacity(&self) -> usize {
        self.dependencies.capacity()
    }

    /// Retained generations that are neither PUBLISHED nor ABORTED.
    pub(in crate::distributed::remote_db) fn unresolved_slots(&self) -> usize {
        (self.sequencer.reuse_next()..self.sequencer.next())
            .filter(|&sequence| !self.sequencer.resolved(sequence))
            .count()
    }

    /// The receipt of a live generation (DURABLE or PUBLISHED). None before
    /// its owner receipt arrives, or after its slot has been reused.
    pub(in crate::distributed::remote_db) fn durable_receipt(
        &self,
        sequence: u64,
    ) -> Option<super::super::DurableBatch> {
        let data = self.sequencer.slot(sequence).data.lock();
        if self.sequencer.generation(sequence) == Some(sequence) {
            data.receipt
        } else {
            None
        }
    }

    /// Canonical encoded length of a READY or DURABLE generation whose
    /// publication payload has not yet been consumed by the publisher.
    pub(in crate::distributed::remote_db) fn pending_encoded_len(
        &self,
        sequence: u64,
    ) -> Option<usize> {
        let data = self.sequencer.slot(sequence).data.lock();
        if self.sequencer.generation(sequence) != Some(sequence) {
            return None;
        }
        data.publication
            .as_ref()
            .map(|publication| publication.batch.encoded().len())
    }

    /// True once the owner worker has fully processed this generation under
    /// writer→publisher: PUBLISHED or ABORTED, or DURABLE and parked behind an
    /// unresolved predecessor. The publisher lock orders this observation
    /// after the view install of the same publication pass.
    pub(in crate::distributed::remote_db) fn publication_settled(&self, sequence: u64) -> bool {
        let publisher = self.publisher.lock();
        self.sequencer.resolved(sequence)
            || (self.sequencer.resolution(sequence) == Some(Resolution::Durable)
                && publisher
                    .dependents
                    .values()
                    .any(|dependents| dependents.contains(&sequence)))
    }

    /// Wait until every allocated generation has returned its slot and every
    /// per-window and per-publication Admission charge has been released.
    /// Identity and checkpoint-tail charges stay until checkpoint retirement.
    /// Publication finalization runs after the published floor is notified,
    /// so a drain alone does not order these releases.
    pub(in crate::distributed::remote_db) fn wait_settled(&self) -> Result<(), LocalDbError> {
        let settled = self.admission.wait_charges(
            std::time::Instant::now() + self.config.timeout,
            |charges| {
                self.sequencer.reuse_next() == self.sequencer.next()
                    && charges.requests == 0
                    && charges.bytes == 0
                    && charges.slots == 0
                    && charges.index_bytes == 0
                    && charges.partition_batches == 0
                    && charges.partition_records == 0
            },
        );
        if settled {
            Ok(())
        } else {
            Err(LocalDbError::new(
                "write core did not settle before deadline",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::remote_db::pipeline_tests::Fixture;

    #[test]
    fn checkpoint_boundary_delegates_real_capture_and_shutdown() {
        let fixture = Fixture::new();
        let database = fixture.open();
        let core = Arc::clone(&database.core);
        assert_eq!(core.next_sequence(), 0);
        core.ensure_healthy().unwrap();
        assert!(!super::super::super::checkpoint_lifecycle::core_checkpoint_due(&core));
        core.request_checkpoint().unwrap();
        {
            let mut publisher = core.publisher.lock();
            let checkpoint = publisher.checkpoint_state();
            assert!(checkpoint.known.is_empty());
            assert_eq!(*checkpoint.checkpoint_tail_batches, 0);
            assert_eq!(*checkpoint.checkpoint_tail_bytes, 0);
            assert_eq!(
                core.checkpoint_jobs(checkpoint.retained_epochs)
                    .unwrap()
                    .len(),
                5
            );
        }
        core.start_checkpoint().unwrap();
        core.progress_checkpoint(true).unwrap();
        core.commit_checkpoint().unwrap();
        core.release_checkpoint_credits(CheckpointCredits::default())
            .unwrap();
        core.shutdown().unwrap();
    }

    #[test]
    fn begin_failure_fences_cached_and_completed_values_before_notifications() {
        use crate::distributed::codec::Mutation;
        use crate::key_shape::KeySpace;

        let fixture = Fixture::new();
        let mut database = fixture.open();
        let key = 17u32.to_be_bytes();
        database
            .write_batch(
                fixture.operation(0, 17),
                vec![Mutation::Put {
                    keyspace: KeySpace(0),
                    key: key.to_vec().into(),
                    value: vec![7; 32].into(),
                }],
            )
            .unwrap();
        let cached = database.submit_get(KeySpace(0), &key).unwrap().wait();
        let reference = database
            .index_reference(KeySpace(0), &key, IndexReadCut::Latest)
            .unwrap()
            .unwrap();
        database.clear_read_cache();
        let record = database.submit_record(reference).unwrap().wait();
        let core = Arc::clone(&database.core);
        {
            let _writer = core.index.writer();
            let _publisher = core.publisher.lock();
            core.begin_failure(&LocalDbError::new("injected publication failure"));
            // begin_failure is atomic-only: even while both publication locks
            // remain held, a previously prepared cache hit and a completed
            // transport value must fail their final serving gates. None of
            // these read gates takes the index writer or publisher.
            assert!(core.ensure_healthy().is_err());
            assert!(core.reads.failed());
            assert!(database.finish_ready_get(cached).is_err());
            assert!(database.finish_record(record, KeySpace(0), &key).is_err());
            assert!(database.get(KeySpace(0), &key).is_err());
            assert!(database.published_cut().is_err());
        }
        core.finish_failure();
        core.finish_failure();
        assert!(core.shutdown().is_err());
        assert!(
            core.first_failure
                .get()
                .unwrap()
                .contains("injected publication failure")
        );
    }

    #[test]
    fn finish_failure_releases_every_waiter_without_reusing_unknowns() {
        use crate::distributed::types::BatchVersion;
        use std::time::{Duration, Instant};

        let fixture = Fixture::new();
        let database = fixture.open();
        let core = Arc::clone(&database.core);
        let sequence = core.sequencer.allocate(BatchId {
            database: fixture.config.storage.database,
            operation: fixture.operation(0, 18),
        });
        let version = BatchVersion {
            epoch: core.epoch,
            sequence,
        };
        let credit = core
            .admission
            .reserve(core.config.max_pending_bytes, || core.ensure_healthy())
            .unwrap();
        let read_generation = core.reads.admission_generation();
        let (started, starting) = mpsc::sync_channel(4);
        let (finished, finishing) = mpsc::sync_channel(4);
        std::thread::scope(|scope| {
            let slot_started = started.clone();
            let slot_finished = finished.clone();
            let core = &core;
            scope.spawn(move || {
                slot_started.send(()).unwrap();
                let result = core
                    .sequencer
                    .wait(version, Instant::now() + Duration::from_secs(5));
                slot_finished.send(result.map(|_| ())).unwrap();
            });
            let prefix_started = started.clone();
            let prefix_finished = finished.clone();
            scope.spawn(move || {
                prefix_started.send(()).unwrap();
                prefix_finished
                    .send(core.wait_published_before(sequence + 1))
                    .unwrap();
            });
            let read_started = started.clone();
            let read_finished = finished.clone();
            scope.spawn(move || {
                read_started.send(()).unwrap();
                read_finished
                    .send(
                        core.reads.wait_admission(
                            read_generation,
                            Instant::now() + Duration::from_secs(5),
                        ),
                    )
                    .unwrap();
            });
            scope.spawn(move || {
                started.send(()).unwrap();
                finished
                    .send(
                        core.admission
                            .reserve(1, || core.ensure_healthy())
                            .map(drop),
                    )
                    .unwrap();
            });
            for _ in 0..4 {
                starting.recv_timeout(Duration::from_secs(1)).unwrap();
            }
            let deadline = Instant::now() + Duration::from_secs(1);
            while core.admission.queued_waiters() != 1 {
                assert!(Instant::now() < deadline, "admission waiter did not queue");
                std::thread::yield_now();
            }
            core.begin_failure(&LocalDbError::new("injected waiter failure"));
            core.finish_failure();
            for _ in 0..4 {
                assert!(
                    finishing
                        .recv_timeout(Duration::from_secs(1))
                        .unwrap()
                        .is_err()
                );
            }
        });
        assert_eq!(
            core.sequencer.resolution(sequence),
            Some(Resolution::Unknown)
        );
        core.release_sequence(sequence).unwrap();
        assert_eq!(core.sequencer.reuse_next(), sequence);
        assert_eq!(
            core.sequencer
                .slot(sequence)
                .data
                .lock()
                .outcome
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .uncertain_version,
            Some(version)
        );
        drop(credit);
        assert!(core.shutdown().is_err());
    }

    #[test]
    fn history_lookup_covers_identity_retired_by_concurrent_checkpoint() {
        use crate::distributed::codec::Mutation;
        use crate::distributed::storage::HistoryPersistence;
        use crate::key_shape::KeySpace;
        use std::time::Duration;

        let fixture = Fixture::new().history(HistoryPersistence::OnDisk);
        let database = fixture.open();
        let operation = fixture.operation(0, 19);
        let version = database
            .write_batch(
                operation,
                vec![Mutation::Put {
                    keyspace: KeySpace(0),
                    key: 19u32.to_be_bytes().to_vec().into(),
                    value: vec![9; 32].into(),
                }],
            )
            .unwrap();
        assert_eq!(version.sequence, 0);
        let (entered, waiting) = mpsc::sync_channel(1);
        let (resume, gate) = mpsc::sync_channel(1);
        database.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
        let core = Arc::clone(&database.core);
        let before = core.lookup_identity(operation).unwrap().unwrap();
        assert_eq!(before.version, version);
        assert_eq!(before.class, IdentityClass::Suffix);
        core.start_checkpoint().unwrap();
        waiting.recv_timeout(Duration::from_secs(5)).unwrap();
        let during = core.lookup_identity(operation).unwrap().unwrap();
        assert_eq!(during.version, version);
        assert_eq!(during.digest, before.digest);
        assert_eq!(during.class, IdentityClass::Suffix);
        resume.send(()).unwrap();
        core.progress_checkpoint(true).unwrap();
        assert!(
            core.identities
                .with_operation(operation, |entries| entries.get(&operation).copied())
                .is_none()
        );
        let after = core.lookup_identity(operation).unwrap().unwrap();
        assert_eq!(after.version, version);
        assert_eq!(after.digest, before.digest);
        assert_eq!(after.class, IdentityClass::Recent);
        core.wait_published_before(1).unwrap();
        core.shutdown().unwrap();
    }

    /// Replaces the deleted PublicationTracker sparse test on the live
    /// publisher: an independent later batch publishes past an unresolved
    /// floor, the same-key chain waits as dependents, the installed view
    /// excludes exactly the unpublished generations in [floor, upper] (an
    /// ABORTED generation included), and no slot returns to the ring until the
    /// floor passes every PUBLISHED and ABORTED generation.
    #[test]
    fn sparse_view_excludes_exactly_unpublished_and_floor_passes_resolved() {
        use crate::distributed::codec::Mutation;
        use crate::distributed::types::{BatchVersion, MutationVersion};
        use crate::key_shape::KeySpace;
        use std::time::{Duration, Instant};

        let put = |key: u32, value: u8| Mutation::Put {
            keyspace: KeySpace(0),
            key: key.to_be_bytes().to_vec().into(),
            value: vec![value; 32].into(),
        };
        let fixture = Fixture::new();
        let mut database = fixture.open();
        let core = Arc::clone(&database.core);
        let version = |sequence| BatchVersion {
            epoch: core.epoch,
            sequence,
        };
        let held_client = Arc::clone(&core.clients[0].client);
        let held = held_client.lock();
        let gap = database
            .submit(fixture.operation(0, 1), vec![put(0, 1)])
            .unwrap();
        let chained = database
            .submit(fixture.operation(1, 2), vec![put(0, 2), put(1, 2)])
            .unwrap();
        let successor = database
            .submit(fixture.operation(1, 3), vec![put(1, 3)])
            .unwrap();
        let failed = fixture.operation(3, 4);
        core.test_hooks.lock().fail_prepared_operation = Some(failed);
        assert!(database.submit(failed, vec![put(4, 4)]).is_err());
        let independent = database
            .submit(fixture.operation(2, 5), vec![put(3, 5)])
            .unwrap();
        assert_eq!(
            [gap, chained, successor, independent].map(|version| version.sequence),
            [0, 1, 2, 4]
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        for sequence in [1, 2, 4] {
            while !core.publication_settled(sequence) {
                core.ensure_healthy().unwrap();
                assert!(
                    Instant::now() < deadline,
                    "owner receipts were not processed"
                );
                std::thread::yield_now();
            }
        }
        // Owner 0 is held: the gap is READY in its queue, never durable.
        assert_eq!(core.sequencer.resolution(0), Some(Resolution::Ready));
        assert_eq!(core.sequencer.resolution(1), Some(Resolution::Durable));
        assert_eq!(core.sequencer.resolution(2), Some(Resolution::Durable));
        assert_eq!(core.sequencer.resolution(3), Some(Resolution::Aborted));
        assert_eq!(core.sequencer.resolution(4), Some(Resolution::Published));
        {
            let publisher = core.publisher.lock();
            assert_eq!(publisher.published_next, 0);
            assert_eq!(publisher.dependents.get(&0), Some(&vec![1]));
            assert_eq!(publisher.dependents.get(&1), Some(&vec![2]));
        }
        assert_eq!(
            core.sequencer.reuse_next(),
            0,
            "no slot passes the unresolved floor"
        );
        assert_eq!(core.retained_slots(), 5);
        match core.index.latest_cut() {
            IndexReadCut::Sparse(view) => {
                let (upper, floor, excluded) = view.parts();
                assert_eq!(
                    upper,
                    MutationVersion {
                        batch: independent,
                        operation_index: 0,
                    }
                );
                assert_eq!(floor, None);
                assert_eq!(excluded, [0, 1, 2, 3].map(version));
            }
            cut => panic!("expected a sparse publication view, found {cut:?}"),
        }
        assert_eq!(
            database
                .get(KeySpace(0), &3u32.to_be_bytes())
                .unwrap()
                .unwrap()
                .as_ref(),
            &[5; 32]
        );
        assert!(
            database
                .get(KeySpace(0), &0u32.to_be_bytes())
                .unwrap()
                .is_none()
        );
        assert!(
            database
                .get(KeySpace(0), &1u32.to_be_bytes())
                .unwrap()
                .is_none()
        );

        drop(held);
        database.drain().unwrap();
        assert_eq!(core.published_next(), 5);
        assert!(!matches!(core.index.latest_cut(), IndexReadCut::Sparse(_)));
        for sequence in [0, 1, 2, 4] {
            assert!(core.is_published(version(sequence)));
        }
        core.wait_settled().unwrap();
        assert_eq!(core.sequencer.reuse_next(), 5);
        for (key, expected) in [(0u32, Some(2u8)), (1, Some(3)), (3, Some(5)), (4, None)] {
            assert_eq!(
                database
                    .get(KeySpace(0), &key.to_be_bytes())
                    .unwrap()
                    .map(|value| value[0]),
                expected
            );
        }
        drop(core);
        database.shutdown().unwrap();
    }
}
