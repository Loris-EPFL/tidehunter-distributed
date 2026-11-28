//! Shared write engine. Callers allocate/encode; owner workers publish durable
//! batches. Checkpoint persistence owns no foreground or publication lock.

use super::super::{
    AuthorityManifest, AuthorityWriteStats, ControlHandle, LocalDbError, LocalDbStats,
    MutationVersion, ReadCache, ReadService, RecentReceipt, RecoveryStats, RemoteDatabaseConfig,
    ShutdownStats, StaticInventory, StreamPosition, WriteProfileSnapshot, WriteTimeline,
    concurrent, failure_context, record_first_failure,
};
use super::dependencies::DependencyRegistry;
use super::inflight::InFlight;
use super::receipts::{ReceiptIndex, ReceiptWindow};
use super::sequencer::{Resolution, Sequencer};
use crate::distributed::hybrid::table::HybridTable;
use crate::key_shape::KeyShape;
use parking_lot::{Condvar, Mutex};
use std::collections::{BTreeMap, HashMap, hash_map::RandomState};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
#[cfg(test)]
use std::sync::mpsc;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
};

/// The publisher-owned fields that the authority's open, relocation, trim and
/// retirement code reads and updates under the publisher lock. Returning
/// disjoint field references avoids cloning maps or exposing the clients and
/// admission of the authority.
pub(in crate::distributed::remote_db) struct PublisherCheckpoint<'a> {
    pub positions: &'a mut BTreeMap<(u32, u64), StreamPosition>,
    /// Adopted R1 callback starts, keyed by `(partition, fencing_generation)`.
    pub callback_starts: &'a mut BTreeMap<(u32, u64), u64>,
    pub latest: &'a mut Option<MutationVersion>,
    pub live_latest: &'a mut Option<MutationVersion>,
    pub manifest: &'a mut AuthorityManifest,
    pub retained_epochs: &'a mut Vec<Vec<u64>>,
    pub checkpoint_tail_batches: &'a mut usize,
    pub checkpoint_tail_bytes: &'a mut usize,
}

pub(in crate::distributed::remote_db) struct PublisherState {
    /// The retry window: the last `WAL_REPLAY_WINDOW_BATCHES` published batches,
    /// oldest first. Filled at publication and replaced by a persisted window at
    /// open.
    pub recent: ReceiptWindow,
    pub positions: BTreeMap<(u32, u64), StreamPosition>,
    /// Adopted R1 callback starts; they only rise.
    pub callback_starts: BTreeMap<(u32, u64), u64>,
    pub latest: Option<MutationVersion>,
    pub live_latest: Option<MutationVersion>,
    pub manifest: AuthorityManifest,
    pub retained_epochs: Vec<Vec<u64>>,
    pub checkpoint_tail_batches: usize,
    pub checkpoint_tail_bytes: usize,
    /// Exclusive contiguous publication frontier, including an empty epoch 0.
    pub published_next: u64,
    pub upper: Option<MutationVersion>,
    pub dependents: HashMap<u64, Vec<u64>>,
    /// The one unresolved fresh `DropCells` slot, if any. While present no
    /// sequence above it publishes; see `RangeBarrier`.
    pub range_barrier: Option<RangeBarrier>,
}

/// Global publication barrier of one fresh `DropCells` slot. It never stops
/// WAL admission, owner queues or durable completion of other sequences: it
/// only defers publication of sequences above `sequence`. Lower sequences
/// publish normally; the range publishes once `published_next == sequence`.
/// Each deferred durable sequence is queued here exactly once and requeued
/// in order when the range is published or aborted before READY.
///
/// Bound: `deferred_ready` holds distinct admitted sequences above
/// `sequence`, so its length is below the sequencer window. Owner: the
/// publisher mutex. Lifetime: the admitted slot, not the caller's timeout.
pub(in crate::distributed::remote_db) struct RangeBarrier {
    pub sequence: u64,
    pub deferred_ready: Vec<u64>,
}

impl PublisherState {
    pub(in crate::distributed::remote_db) fn checkpoint_state(
        &mut self,
    ) -> PublisherCheckpoint<'_> {
        PublisherCheckpoint {
            positions: &mut self.positions,
            callback_starts: &mut self.callback_starts,
            latest: &mut self.latest,
            live_latest: &mut self.live_latest,
            manifest: &mut self.manifest,
            retained_epochs: &mut self.retained_epochs,
            checkpoint_tail_batches: &mut self.checkpoint_tail_batches,
            checkpoint_tail_bytes: &mut self.checkpoint_tail_bytes,
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

/// Recovered, validated ownership transferred once at the end of open.
pub(in crate::distributed::remote_db) struct OpenState {
    pub config: RemoteDatabaseConfig,
    pub shape: KeyShape,
    pub path: PathBuf,
    pub epoch: u64,
    /// The native index stack (LargeTable + IndexFlusher + index WAL) and its
    /// loader. Publication inserts version-carrying positions here.
    pub table: Arc<HybridTable>,
    pub partition_indexes: HashMap<u32, usize>,
    pub inventory: StaticInventory,
    pub reads: Arc<ReadService>,
    pub read_cache: Arc<ReadCache>,
    pub write_profile: Arc<Mutex<WriteProfileSnapshot>>,
    pub write_timeline: Arc<WriteTimeline>,
    pub clients: Vec<super::super::ClientWorker>,
    pub live_pool: Option<Arc<crate::distributed::nvmeof::live_pool::LiveControllerPool>>,
    pub live_metrics: Option<Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics>>,
    pub publisher: PublisherState,
    pub stats: CoreStats,
    pub concurrent_failure: Arc<AtomicBool>,
    pub first_failure: Arc<OnceLock<super::super::FirstFailure>>,
}

/// Sends the keyspace and first reduced key, then waits for the resume signal.
#[cfg(test)]
type RelocationCopyPause = (
    mpsc::SyncSender<(crate::key_shape::KeySpace, Option<minibytes::Bytes>)>,
    mpsc::Receiver<()>,
);

#[cfg(test)]
#[derive(Default)]
pub(in crate::distributed::remote_db) struct TestHooks {
    pub after_grant: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub before_preparation: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub fail_prepared_operation: Option<[u8; 16]>,
    pub concurrent_progress: Option<mpsc::SyncSender<super::super::PipelineStats>>,
    /// Sent once by the first drain, or acknowledged write, that is about to
    /// wait for the published prefix.
    pub drain_wait_entered: Option<mpsc::SyncSender<()>>,
    pub panic_after_concurrent_admissions: Option<u64>,
    pub skip_shutdown_checkpoint: bool,
    pub before_publish: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub before_release: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub fail_publication: Option<u64>,
    /// One-shot: the next caller's resolution deadline is already expired, so a
    /// held resolution times that caller out on an observed event instead of
    /// after a real delay.
    pub expire_next_write_deadline: bool,
    /// Replaces the configuration-derived bound of `await_copies_durable`.
    pub copy_durable_timeout: Option<std::time::Duration>,
    /// Sends the keyspace and first reduced key, then parks before re-point.
    pub relocation_copy_pause: Option<RelocationCopyPause>,
    /// Parks the shutdown snapshot: sends on the first, waits on the second.
    pub checkpoint_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    /// Fails the shutdown snapshot before it stores anything.
    pub checkpoint_interruption: Option<usize>,
    /// Fails a relocation pass after its covering store and its callback-start
    /// adoption, before any physical pruning: the crash boundary between the
    /// durable relocation capture and the owner-file retirement.
    pub relocation_prune_interruption: bool,
}

/// Lock order for the final engine: ascending key shards → owner order,
/// release all, then publisher → slot data. Admission is never held with these
/// locks. Transport I/O, encoding, index flush, checkpoint persistence and
/// worker joins happen after all foreground/publication guards are dropped.
pub(in crate::distributed::remote_db) struct WriteCore {
    pub config: RemoteDatabaseConfig,
    pub shape: KeyShape,
    pub path: PathBuf,
    pub epoch: u64,
    /// Native index stack; publication inserts into it and the loader reads its
    /// frontiers. No Exact index writer exists any more.
    pub table: Arc<HybridTable>,
    pub partition_indexes: HashMap<u32, usize>,
    pub inventory: StaticInventory,
    pub reads: Arc<ReadService>,
    pub read_cache: Arc<ReadCache>,
    pub write_profile: Arc<Mutex<WriteProfileSnapshot>>,
    pub write_timeline: Arc<WriteTimeline>,
    pub publisher: Mutex<PublisherState>,
    pub floor_changed: Condvar,
    /// Serialises owner-file retirement with the callback-start adoption of a
    /// relocation pass. It carries no state: the watermark is computed from the
    /// stored terms under this lock.
    pub(crate) retirement: Mutex<()>,
    /// Opt-in ordered R1 evidence stream; inert until enabled.
    pub(crate) r1_events: super::super::r1_events::R1EventSlot,
    pub stats: Mutex<CoreStats>,
    pub read_resolve_ns: AtomicU64,
    /// Relocation passes that applied nothing because the durable cut did not
    /// cover their copies in time. Diagnostic only (a persistent value means an
    /// owner cannot sync): relaxed, nothing is ordered against it.
    pub(crate) copy_deferrals: AtomicU64,
    /// Seeded start streams that a retirement pass found with no row in the
    /// region this process stored (`dead_wal_files::take_snapshot`), summed
    /// over passes. Every store names every retained generation, so a
    /// non-zero value is a broken invariant: the stream is skipped (nothing
    /// retires without a replay floor) and the state stays visible.
    /// Diagnostic only: relaxed, nothing is ordered against it.
    pub(crate) retirement_missing_rows: AtomicU64,
    /// Retirement requests an owner or the controller pool answered `Busy`
    /// (a pin on a victim file): one count per reply, summed over passes.
    /// Non-zero for long means a reader holds old files and a generation stays
    /// untrimmed. Diagnostic only: relaxed, nothing is ordered against it.
    pub(crate) retirement_busy_replies: AtomicU64,
    pub value_read_ns: AtomicU64,
    pub admission: Arc<concurrent::Admission>,
    pub concurrent_failure: Arc<AtomicBool>,
    pub first_failure: Arc<OnceLock<super::super::FirstFailure>>,
    pub stopped: AtomicBool,
    pub closing: AtomicBool,
    pub clients: Vec<super::super::ClientWorker>,
    /// Set when this epoch is tracked under durable-prefix recovery. Open
    /// attaches it after offline fencing-generation maintenance, before the
    /// handle is returned and before any write can be admitted.
    pub durable_cut: OnceLock<super::super::durable_cut::DurableCut>,
    pub live_pool: Option<Arc<crate::distributed::nvmeof::live_pool::LiveControllerPool>>,
    pub live_metrics: Option<Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics>>,
    #[cfg(test)]
    pub test_hooks: Mutex<TestHooks>,
    owner_controls: Vec<(u32, ControlHandle)>,
    pub(super) sequencer: Sequencer,
    pub(super) dependencies: DependencyRegistry,
    /// Operation id → the one generation that owns it between allocation and
    /// publication. Sharded; taken before the key shards.
    pub(super) in_flight: InFlight,
    /// The retry window's probe index, shared with `publisher.recent`. Read on
    /// the write hot path, so it never takes the publisher lock.
    pub(super) receipts: Arc<ReceiptIndex>,
}

impl WriteCore {
    pub(in crate::distributed::remote_db) fn new(open: OpenState) -> Arc<Self> {
        let parallelism = crate::parallelism::available();
        let admission = concurrent::Admission::for_open(&open);
        let hash = RandomState::new();
        let in_flight = InFlight::new(parallelism, hash.clone());
        let receipts = open.publisher.recent.probe();
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
            table: open.table,
            partition_indexes: open.partition_indexes,
            inventory: open.inventory,
            reads: open.reads,
            read_cache: open.read_cache,
            write_profile: open.write_profile,
            write_timeline: open.write_timeline,
            floor_changed: Condvar::new(),
            retirement: Mutex::new(()),
            r1_events: Default::default(),
            stats: Mutex::new(open.stats),
            admission,
            read_resolve_ns: AtomicU64::new(0),
            copy_deferrals: AtomicU64::new(0),
            retirement_missing_rows: AtomicU64::new(0),
            retirement_busy_replies: AtomicU64::new(0),
            value_read_ns: AtomicU64::new(0),
            concurrent_failure: open.concurrent_failure,
            first_failure: open.first_failure,
            stopped: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            owner_controls,
            clients: open.clients,
            durable_cut: OnceLock::new(),
            live_pool: open.live_pool,
            live_metrics: open.live_metrics,
            #[cfg(test)]
            test_hooks: Mutex::new(TestHooks::default()),
            sequencer: Sequencer::new(capacity),
            dependencies: DependencyRegistry::new(parallelism, hash),
            in_flight,
            receipts,
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
        handle.wake_worker()?;
        Ok(coalesced)
    }

    /// The published receipt of `operation`, when it is still in the retry
    /// window. A hit returns the original version and spends no owner byte; a
    /// miss allocates a fresh version and re-applies the write, which is the
    /// owner-accepted behaviour outside the window. One index shard, never the
    /// publisher lock.
    pub(super) fn lookup_receipt(
        &self,
        operation: [u8; 16],
    ) -> Result<Option<RecentReceipt>, LocalDbError> {
        self.ensure_healthy()?;
        Ok(self.receipts.get(&operation))
    }

    /// Admitted-but-unpublished operation ids (test observation).
    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn active_identities(&self) -> usize {
        self.in_flight.len()
    }

    /// Resident completed/in-flight write bookkeeping, excluding allocator
    /// overhead and the separately reported index. Publisher, in-flight and key
    /// shards are locked one at a time.
    pub(in crate::distributed::remote_db) fn metadata_history_bytes(&self) -> usize {
        let publisher = Self::publisher_resident_bytes(&self.publisher.lock());
        publisher + self.in_flight.resident_bytes() + self.dependencies.resident_bytes()
    }

    /// Static upper bound of [`Self::metadata_history_bytes`] under the fixed
    /// WAL-replay retry window (`WAL_REPLAY_WINDOW_BATCHES` receipts), from
    /// configuration and entry sizes only. Live entries: the retry window is
    /// bounded by its cap; one in-flight entry exists per unresolved admitted
    /// generation, so at most `max_admitted_batches`; dependency keys belong to
    /// admitted batches. A std `HashMap` holding at most `n` live entries never
    /// keeps more than `4 * (n + 1) + 8` slots (it grows only when over half
    /// full, and the registry shrinks a shard above four slots per entry), so the
    /// shard sums add at most 12 slots per shard. A `VecDeque` at most doubles
    /// past its peak length or its initial capacity.
    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn metadata_history_bound(&self) -> usize {
        let admitted = self.config.max_admitted_batches;
        // Spilled key storage of one batch is below its encoded size; a
        // spilled buffer at most doubles past its length.
        let dependencies =
            (4 * admitted * self.config.storage.batch_limits.max_operations as usize
                + 12 * self.dependencies.shard_count())
                * DependencyRegistry::entry_bytes()
                + 2 * admitted * self.config.storage.batch_limits.max_encoded_bytes;
        let in_flight =
            (4 * admitted + 12 * self.in_flight.shard_count()) * InFlight::entry_bytes();
        ReceiptWindow::resident_bound() + in_flight + dependencies
    }

    /// Metrics variant: contention on the publisher, on an in-flight shard or on
    /// any key shard makes the value unavailable; sampling never blocks a
    /// writer.
    pub(in crate::distributed::remote_db) fn try_metadata_history_bytes(&self) -> Option<usize> {
        let publisher = Self::publisher_resident_bytes(&*self.publisher.try_lock()?);
        Some(
            publisher
                + self.in_flight.try_resident_bytes()?
                + self.dependencies.try_resident_bytes()?,
        )
    }

    fn publisher_resident_bytes(publisher: &PublisherState) -> usize {
        publisher.recent.resident_bytes()
    }

    pub(super) fn release_sequence(&self, sequence: u64) -> Result<(), LocalDbError> {
        let slots = self.sequencer.complete_release(sequence);
        self.admission.return_slots(slots).inspect_err(|error| {
            self.fail(LocalDbError::new(error.message()));
        })
    }

    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn next_sequence(&self) -> u64 {
        self.sequencer.next()
    }

    /// A failed wait carries the uncertain version named by the first causal
    /// failure record, which is written before any failure flag is stored.
    pub(in crate::distributed::remote_db) fn wait_published_before(
        &self,
        next: u64,
    ) -> Result<(), LocalDbError> {
        self.wait_published_inner(next)
            .map_err(|error| self.with_first_failure_version(error))
    }

    /// Names the first causal failure's uncertain version on `error` unless
    /// it already names one. Every fail-stop error a waiter can observe
    /// (early health check or wait) must carry it.
    pub(super) fn with_first_failure_version(&self, mut error: LocalDbError) -> LocalDbError {
        if error.uncertain_version.is_none() {
            error.uncertain_version = self
                .first_failure
                .get()
                .and_then(super::super::FirstFailure::version);
        }
        error
    }

    /// The old authority filled the post-publish read cache and installed the
    /// outcome before any drain could return. Workers do that after dropping
    /// writer → publisher, so after the published prefix covers `next` this
    /// also waits, per slot, until each generation below `next` has its
    /// outcome (installed after its read-cache fill) or has been reused.
    fn wait_published_inner(&self, next: u64) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        let deadline = std::time::Instant::now() + self.config.timeout;
        self.wait_floor_covers(next, deadline)?;
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

    /// Waits until the published contiguous prefix covers every sequence below
    /// `next`. The publisher sets the table's publication cut under the same
    /// lock that advances `published_next`, so a read at `table.current()` sees
    /// every batch below `next` once this returns. Holds no lock but the
    /// publisher mutex, which the condvar releases while parked.
    fn wait_floor_covers(
        &self,
        next: u64,
        deadline: std::time::Instant,
    ) -> Result<(), LocalDbError> {
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
        Ok(())
    }

    /// Returns `version` once a read can see it. Reads resolve at the published
    /// prefix (paper 3.2; PARITY.md "Read visibility past an unresolved
    /// prefix"), and a batch can publish beyond an unresolved earlier one, so
    /// an acknowledged write waits here for the prefix to cover it: Ok means
    /// readable, as in native, where the index insert precedes the return.
    ///
    /// The caller holds no slot, credit or lock here, and the earlier batches
    /// resolve through the owners and the publisher, never through this caller,
    /// so the wait has no cycle. The publisher crosses an aborted earlier slot.
    /// When the prefix can never advance (fail-stop, fence, shutdown) or the
    /// deadline passes, the error names `version` as uncertain: the batch is
    /// durable at its owner and recovery replays it.
    pub(super) fn wait_readable(
        &self,
        version: crate::distributed::types::BatchVersion,
        deadline: std::time::Instant,
    ) -> Result<crate::distributed::types::BatchVersion, LocalDbError> {
        if version.epoch != self.epoch {
            return Ok(version);
        }
        #[cfg(test)]
        if self.publisher.lock().published_next <= version.sequence {
            // The same signal a drain sends before it waits for the prefix.
            let entered = self.test_hooks.lock().drain_wait_entered.take();
            if let Some(entered) = entered {
                let _ = entered.try_send(());
            }
        }
        self.wait_floor_covers(version.sequence + 1, deadline)
            .map(|()| version)
            .map_err(|error| LocalDbError::unknown(version, error))
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

    /// Called before dropping publisher and slot guards on a publication failure.
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
                debug_assert!(
                    data.outcome.as_ref().is_some_and(|outcome| outcome
                        .as_ref()
                        .is_err_and(|error| error.uncertain_version.is_some())),
                    "a fail-stop outcome must carry the uncertain version",
                );
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
                let _ = owner.wake_worker();
            }
        }
    }

    pub(in crate::distributed::remote_db) fn fail(&self, cause: LocalDbError) {
        self.begin_failure(&cause);
        self.finish_failure();
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
    /// Publication finalization runs after the published floor is notified, so
    /// a drain alone does not order these releases.
    pub(in crate::distributed::remote_db) fn wait_settled(&self) -> Result<(), LocalDbError> {
        let settled = self.admission.wait_charges(
            std::time::Instant::now() + self.config.timeout,
            |charges| {
                self.sequencer.reuse_next() == self.sequencer.next()
                    && charges.requests == 0
                    && charges.bytes == 0
                    && charges.slots == 0
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
mod tests;
