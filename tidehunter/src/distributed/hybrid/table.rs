//! The hybrid table: the coordinator's native index over remote value positions.
//!
//! Convention (A13): cuts are exclusive (`below(cut)` is `version < cut`),
//! starts are inclusive and ends are exclusive.
//!
//! Native `Db` owns one `LargeTable`, its `IndexFlusher` threads and the local
//! value WAL that gives every position a meaning. The hybrid keeps the first
//! two unchanged and replaces the third with the remote value WAL: a
//! [`VersionPosition`] names its member frame on its owner, so the index can be
//! read, flushed, relocated and snapshotted by exactly the native code while
//! the values are stored on the owners. A keyspace that sets `value_cache_size`
//! also keeps the native per-cell LRU here: it is a bounded copy, filled at
//! publication and by a live read, never the store of record.
//!
//! Four substitutions, and nothing else:
//!
//! * **The flush frontier.** Native reads it from `WalTracker`; here it is
//!   `min(durable cut, applied cut, lowest held latch)` ([`HybridLoader`]).
//! * **The byte cadence.** `set_last_processed` writes `cut.byte_position()`
//!   natively, which is 0 for a [`BatchVersion`]; the loader supplies the
//!   value-byte counter instead (A26/A27).
//! * **Recovery.** Native replays the value WAL below its control region; here
//!   the suffix of every owner stream is replayed into this same table by
//!   [`super::replay::replay_into`], before any index work is served.
//! * **The drain.** A snapshot pass must know that no owner extent is still
//!   unindexed; it sends one `DrainTo` per owner worker before it stores.
//!
//! # Shape
//!
//! [`HybridTable`] is a cheap handle over an `Arc<TableInner>`. The flusher
//! threads hold a `Weak<TableInner>` and exit when the last handle drops, as the
//! native flushers do with `Weak<Db>`; the host cannot be the handle itself
//! because the handle is created after the threads must already exist.
//!
//! # Locks
//!
//! `region` is taken alone and only by the snapshot pass, after both flush
//! passes completed; a cell row lock is taken inside `LargeTable` and never
//! with the region held. Nothing here runs on a put or a get: publication calls
//! `insert_published`/`advance_applied`/`drop_cells`, which touch `LargeTable`
//! row locks and the lock-free cut cells only.

use super::config::HybridIndexConfig;
use super::loader::{AllocationView, CutLatch, CutSources, HybridLoader, next_stored_target};
use super::position::VersionPosition;
use super::region::{
    HybridRegionStore, MAX_REGION_STREAMS, OlderStream, StreamFront, max_flushed_at,
    max_region_cells,
};
use super::snapshot::{self, StoredSnapshot};
use crate::background_health::{BackgroundHealth, BackgroundThread};
use crate::cell::CellId;
use crate::container::LargeTableContainer;
use crate::context::KsContext;
use crate::db::{CONTROL_REGION_FILE, DbError};
use crate::distributed::durability::DurabilityTier;
use crate::distributed::hybrid::loader::KsContextVec;
use crate::distributed::remote_db::StreamControls;
use crate::distributed::storage::LocalDbError;
use crate::distributed::types::{BatchVersion, RecordKind};
use crate::flusher::{FlusherHost, IndexFlusher};
use crate::index::index_format::IndexIterCaches;
use crate::index::index_table::IndexTable;
use crate::index::levels::IndexLevels;
use crate::index_blob_cache::IndexBlobCache;
use crate::iterators::IteratorResult;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc};
use crate::large_table::{CellUpdateError, GetResult, LargeTable, Loader, SnapshotEntryData};
use crate::metrics::{Metrics, TimerExt};
use crate::wal::layout::WalKind;
use crate::wal::position::{IndexCut, IndexEntryKind, IndexPosition, WalPosition};
use crate::wal::tracker::SharedLastProcessed;
use crate::wal::{Wal, WalRandomRead};
use minibytes::Bytes;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashSet};
use std::ops::Deref;
use std::path::Path;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, mpsc};
use std::thread;

/// One published member: what the write path inserts, and where its value is.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PublishedMutation<'a> {
    pub(crate) keyspace: KeySpace,
    /// The full key, unreduced; the index stores the keyspace's own reduction.
    pub(crate) key: &'a [u8],
    /// `Put` inserts at `position`; `Delete` inserts a tombstone there, which is
    /// how the native index makes a later cut read as not-found.
    pub(crate) kind: RecordKind,
    /// The member frame, including its own 8-byte CRC header (plan §3.1).
    pub(crate) position: VersionPosition,
    /// The value this put wrote. It fills the native per-cell LRU exactly as
    /// native `Db::insert` does, so a later read of the key needs no owner round
    /// trip. `None` for a `Delete`; a `Put` without it is refused, because an
    /// empty resident value would be served as data.
    pub(crate) value: Option<&'a Bytes>,
}

/// What the index answers for a key: a member position to fetch, or a value the
/// native per-cell LRU already holds.
#[derive(Clone, Debug)]
pub(crate) enum ResolvedValue {
    /// The member frame, which the value path fetches from its owner.
    Position(VersionPosition),
    /// A resident value with the full key it was stored under. A `Reduction`
    /// keyspace compares that key before serving it (native
    /// `need_check_index_key`); every other indexing stored the key it was asked
    /// for.
    Resident { full_key: Bytes, value: Bytes },
}

/// The coordinator's index: native `LargeTable` + `IndexFlusher` + index WAL.
#[derive(Clone)]
pub(crate) struct HybridTable {
    inner: Arc<TableInner>,
}

impl Deref for HybridTable {
    type Target = TableInner;

    fn deref(&self) -> &TableInner {
        &self.inner
    }
}

/// The table state; also the flusher threads' host.
pub(crate) struct TableInner {
    shape: Arc<KeyShape>,
    config: HybridIndexConfig,
    metrics: Arc<Metrics>,
    contexts: Arc<KsContextVec>,
    loader: Arc<HybridLoader>,
    table: LargeTable<VersionPosition>,
    /// Cold: the snapshot pass is its only writer.
    region: Mutex<HybridRegionStore>,
    /// The per-stream replay table the region loaded at open named.
    opened_fronts: Vec<StreamFront>,
    /// The cut of the region loaded at open ([`IndexCut::NONE`] without one).
    /// Fixed at open: recovery replays from it, never from a later store.
    opened_cut: BatchVersion,
    /// Whether the open found a stored control region (native: a snapshot to
    /// load), and how long reading and validating it took. Recovery
    /// diagnostics only (`RecoveryStats::checkpoint_loaded/_load_ns`).
    region_loaded: bool,
    region_load_ns: u64,
    /// Single-flight of the snapshot pass. Every caller (the cadence thread,
    /// `flush`, a relocation pass, the named force rebuild, the shutdown
    /// snapshot) runs `rebuild_control_region`, which reads the stored cut, picks
    /// its head and later stores; two interleaved passes would race a stale head
    /// against the other's store (the store refuses a regression, which would
    /// fail the database). The pass takes this lock first and holds it to the
    /// end, so a concurrent caller waits and then runs its own pass on the
    /// fresh stored cut. Lock order: `pass` -> cut latch -> `region`. Cold path.
    pass: Mutex<()>,
    /// Test hook: park a pass after it chose its head, to prove single-flight.
    #[cfg(test)]
    pass_pause: Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
    cuts: CutSources,
    allocation: Arc<AllocationView>,
    health: Arc<BackgroundHealth>,
    /// Owner drain lanes, attached once the coordinator's worker control
    /// handles exist. Unset for an index-only table (unit tests), which has no
    /// owner stream to drain.
    streams: OnceLock<StreamControls>,
    /// Snapshot passes that were a no-op only because a reader latch held the
    /// flush frontier below the stored head (S5c2). Diagnostic; a relaxed
    /// counter is enough, nothing orders against it.
    latch_deferrals: AtomicU64,
    /// Older-generation rows written without a known durable end (carried, not
    /// advanced). The coordinator seeds a position for every retained
    /// generation, so a non-zero value is a broken invariant that stays
    /// visible instead of silently dropping the row. Relaxed: diagnostic only.
    older_rows_without_end: AtomicU64,
    /// Event-driven promotion threads, one per mutex shard, registered by the
    /// opener (native `Db::pending_promotion_threads`). `len()` is the shard
    /// count publication hashes against.
    pending_promotion_threads: Box<[OnceLock<thread::Thread>]>,
    /// See [`HybridTable::set_after_pin_sample`].
    #[cfg(test)]
    after_pin_sample: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// See [`HybridTable::set_naive_pin`].
    #[cfg(test)]
    naive_pin: AtomicBool,
}

impl HybridTable {
    /// Opens the index stack for a database whose owner streams are `partitions`.
    ///
    /// Order matters: the index WAL is mapped before the control region is
    /// trusted (the region names blobs by index-WAL position), the cells are
    /// built unloaded from that region, and the flusher threads start last,
    /// holding a `Weak` to the finished table.
    ///
    /// A reopen needs nothing seeded for the cut fronts: every open is a new
    /// fencing generation whose owner streams start empty, and
    /// `CutSources::new(epoch, ..)` starts all fronts at `{epoch, 0}`, which is
    /// above every earlier-generation batch (A17). The first snapshot therefore
    /// advances once to cover the replayed suffix, or is a no-op when nothing
    /// was replayed.
    pub(crate) fn open_with_partitions(
        dir: &Path,
        shape: Arc<KeyShape>,
        config: &HybridIndexConfig,
        cuts: CutSources,
        metrics: Arc<Metrics>,
        partitions: &[u32],
    ) -> Result<Self, LocalDbError> {
        config.validate()?;
        let native_config = Arc::new(config.to_native_config());
        let health = Arc::new(BackgroundHealth::default());
        let index_wal = Wal::open_with_background_health(
            dir,
            native_config.wal_layout(WalKind::Index),
            metrics.clone(),
            health.clone(),
        )
        .map_err(|error| LocalDbError::new(format!("open index WAL: {error}")))?;
        let region_load_started = std::time::Instant::now();
        let (region_store, region) =
            HybridRegionStore::open(dir.join(CONTROL_REGION_FILE), &shape)?;
        if let Some(region) = &region {
            region.validate_bounds(MAX_REGION_STREAMS, max_region_cells(&shape))?;
        }
        let region_load_ns =
            u64::try_from(region_load_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let empty;
        let snapshot = match &region {
            Some(region) => region.region.snapshot(),
            None => {
                empty = empty_snapshot(&shape);
                &empty
            }
        };
        let index_writer = Arc::new(
            index_wal
                .writer_after_premapped(
                    snapshot.iter_valid_val_positions().max(),
                    snapshot.iter_valid_val_positions(),
                )
                .map_err(|error| LocalDbError::new(format!("open index WAL writer: {error:?}")))?,
        );
        let contexts = Arc::new(KsContextVec::new(
            &shape,
            native_config.clone(),
            metrics.clone(),
            SharedLastProcessed::default(),
        ));
        // Built exactly as native `Db::open` builds it: one cache over the
        // index WAL, reachability and whole-frame reads from the same `Wal`.
        let index_blob_cache = config.index_blob_cache_bytes.map(|max_bytes| {
            let reachable_indexes = index_wal.clone();
            let frame_indexes = index_wal.clone();
            Arc::new(IndexBlobCache::new(
                max_bytes,
                config.index_blob_cache_min_bytes,
                Box::new(move |position| reachable_indexes.is_reachable(position)),
                Box::new(move |position| frame_indexes.read(position).map(|(_, payload)| payload)),
                &metrics,
            ))
        });
        let loader = Arc::new(HybridLoader::new(
            index_wal,
            index_writer,
            contexts.clone(),
            cuts.clone(),
            index_blob_cache,
        ));
        let (senders, receivers): (Vec<_>, Vec<_>) = (0..config.num_flusher_threads)
            .map(|_| mpsc::channel())
            .unzip();
        let table = LargeTable::from_unloaded(
            &shape,
            snapshot,
            native_config,
            IndexFlusher::new(senders, metrics.clone()),
            metrics.clone(),
            SharedLastProcessed::default(),
            loader.as_ref(),
        );
        let opened_fronts = region
            .as_ref()
            .map(|region| region.streams.clone())
            .unwrap_or_default();
        let opened_cut = region
            .as_ref()
            .map_or(<BatchVersion as IndexCut>::NONE, |region| {
                region.last_position()
            });
        let stored_cell_bytes = region
            .as_ref()
            .map(|region| region.cell_bytes.clone())
            .unwrap_or_default();
        let allocation = Arc::new(AllocationView::new(partitions.len()));
        for (stream, partition) in partitions.iter().enumerate() {
            let stream = u32::try_from(stream)
                .map_err(|_| LocalDbError::new("more owner streams than a u32 can index"))?;
            allocation.publish_partition(stream, *partition);
        }
        let inner = Arc::new(TableInner {
            shape,
            config: config.clone(),
            metrics: metrics.clone(),
            contexts,
            loader,
            table,
            region: Mutex::new(region_store),
            opened_fronts,
            opened_cut,
            region_loaded: region.is_some(),
            region_load_ns,
            pass: Mutex::new(()),
            #[cfg(test)]
            pass_pause: Mutex::new(None),
            cuts,
            allocation,
            health,
            streams: OnceLock::new(),
            latch_deferrals: AtomicU64::new(0),
            older_rows_without_end: AtomicU64::new(0),
            pending_promotion_threads: (0..config.num_pending_promotion_threads.max(1))
                .map(|_| OnceLock::new())
                .collect(),
            #[cfg(test)]
            after_pin_sample: Mutex::new(None),
            #[cfg(test)]
            naive_pin: AtomicBool::new(false),
        });
        inner.seed_cell_bytes(&stored_cell_bytes);
        // The threads hold only a `Weak`; dropping the handles detaches them and
        // they exit when the last table handle drops, as in native `Db`.
        drop(IndexFlusher::start_threads(
            receivers,
            Arc::downgrade(&inner),
            metrics,
        ));
        // Register the promotion threads before the open returns, as native
        // `Db::open` does: the first publication must never miss a wakeup
        // because a worker had not registered yet.
        let num_shards = inner.pending_promotion_threads.len();
        for shard_idx in 0..num_shards {
            let handle =
                start_pending_promotion_job(Arc::downgrade(&inner), shard_idx, num_shards)?;
            inner.pending_promotion_threads[shard_idx]
                .set(handle.thread().clone())
                .map_err(|_| LocalDbError::new("pending-promotion thread registered twice"))?;
        }
        Ok(Self { inner })
    }

    /// Opens an index-only table (no owner streams), for unit tests.
    #[cfg(test)]
    pub(crate) fn open(
        dir: &Path,
        shape: Arc<KeyShape>,
        config: &HybridIndexConfig,
        cuts: CutSources,
        metrics: Arc<Metrics>,
    ) -> Result<Self, LocalDbError> {
        Self::open_with_partitions(dir, shape, config, cuts, metrics, &[])
    }

    /// One native snapshot pass (the cadence thread's call). `older` names every
    /// retained older generation; the stored region keeps a row for each.
    pub(crate) fn snapshot_with(
        &self,
        older: &[OlderStream],
    ) -> Result<StoredSnapshot, LocalDbError> {
        self.check_background_health()?;
        snapshot::rebuild_control_region(self, false, None, older)
    }

    /// [`Self::snapshot_with`] for an index-only table, which has no older
    /// generation.
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Result<StoredSnapshot, LocalDbError> {
        self.snapshot_with(&[])
    }

    /// A snapshot that must cover the relocation pass's scan cut `q` (A22/A23).
    ///
    /// Relocation re-points *old* versions, so the head does not move and the
    /// ordinary pass is a no-op on an idle database or under a held checkpoint,
    /// forever. The forced pass re-writes the region at the stored head after
    /// force-flushing every dirty cell: native `rebuild_control_region_from`,
    /// where the cut stays and the blob content changes. The flush clamp is
    /// `last_processed_at_capture`; a `q` above it cannot be covered (promotion
    /// would pass a held cut), so the caller defers.
    pub(crate) fn snapshot_for_relocation_with(
        &self,
        q: BatchVersion,
        older: &[OlderStream],
    ) -> Result<StoredSnapshot, LocalDbError> {
        self.check_background_health()?;
        let promotion_cut = self.last_processed();
        if q > promotion_cut {
            return Ok(StoredSnapshot {
                cut: self.stored_cut(),
                stored: false,
                forced: false,
                promotion_cut,
            });
        }
        snapshot::rebuild_control_region(self, true, Some(promotion_cut), older)
    }

    /// [`Self::snapshot_for_relocation_with`] for an index-only table.
    #[cfg(test)]
    pub(crate) fn snapshot_for_relocation(
        &self,
        q: BatchVersion,
    ) -> Result<StoredSnapshot, LocalDbError> {
        self.snapshot_for_relocation_with(q, &[])
    }
}

/// One relocation operation on a cell: native's two filter-driven flush kinds
/// (`RelocationFlush::Updates` for a copied survivor, `RelocationFlush::Cutoff`
/// for a filtered keyspace).
pub(crate) enum RelocationApply<'a> {
    /// Compare-and-set re-point of the entries a pass copied, on their exact
    /// scanned positions.
    Repoint(&'a [(Bytes, VersionPosition, VersionPosition)]),
    /// Drop every live entry of this cell below `targets`, the cutoff a
    /// filtered keyspace takes.
    ///
    /// Only entries the cell holds in memory are dropped: native's cutoff
    /// prunes the L0 it rebuilds (`flusher.rs` `handle_command`,
    /// `RelocationFlush::Cutoff`) and never a deeper level, so a key that
    /// lives only in a promoted L1 blob stays readable, as it does natively.
    Drop,
}

impl TableInner {
    /// Seeds the per-cell byte positions and the byte counter from the region
    /// this open loaded (A26).
    ///
    /// A reopened cell starts with `flushed_at = 0`, which the cadence reads as
    /// "as old as the counter": with the counter also starting at 0 every dirty
    /// cell would be force-flushed on the first pass (a storm, not one stale
    /// flush). The stored per-cell positions are what the previous run last
    /// wrote; the counter restarts at their maximum so no cell looks older than
    /// the counter it is compared with. A missing entry leaves the cell at 0,
    /// which the cadence treats as stale: the documented one-time flush.
    fn seed_cell_bytes(&self, stored: &BTreeMap<(u8, CellId), u64>) {
        let seed = max_flushed_at(stored);
        if seed != 0 {
            self.cuts.add_bytes(seed);
        }
        for ((ks, cell), bytes) in stored {
            let context = self.contexts.ks_context(KeySpace(*ks));
            self.table.set_flushed_at(context, cell, *bytes);
        }
    }

    /// Attaches the coordinator's owner drain lanes; once per open, before the
    /// database is handed to a caller, so the first snapshot can drain.
    pub(crate) fn attach_stream_controls(&self, streams: StreamControls) {
        // A second attach is a programming error, not a state to merge.
        // The `set` must run in release builds too, so it is not inside the assert.
        let attached = self.streams.set(streams).is_ok();
        debug_assert!(attached, "owner lanes attached twice");
    }

    /// The single-flight lock of the snapshot pass.
    pub(crate) fn pass_lock(&self) -> &Mutex<()> {
        &self.pass
    }

    /// Test hook: park the next pass after it chose its head. Returns
    /// (entered, resume).
    #[cfg(test)]
    pub(crate) fn pause_next_pass(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered, started) = std::sync::mpsc::sync_channel(1);
        let (resume, released) = std::sync::mpsc::sync_channel(1);
        *self.pass_pause.lock() = Some((entered, released));
        (started, resume)
    }

    #[cfg(test)]
    pub(crate) fn run_pass_pause(&self) {
        let pause = self.pass_pause.lock().take();
        if let Some((entered, resume)) = pause {
            let _ = entered.send(());
            let _ = resume.recv_timeout(std::time::Duration::from_secs(120));
        }
    }

    /// The cut the stored region names.
    pub(crate) fn stored_cut(&self) -> BatchVersion {
        self.region.lock().last_position()
    }

    /// The cut the next store may name: `max(previous, min(durable, applied,
    /// lowest held latch))` (A9), where `previous` is `self.stored_cut()`, the
    /// cut the stored control region names. Never moves back, so a checkpoint
    /// held below the region cut only makes the store a no-op.
    pub(crate) fn stored_target(&self) -> BatchVersion {
        next_stored_target(
            self.stored_cut(),
            self.cuts.durable(),
            self.cuts.applied(),
            self.cuts.lowest_held(),
        )
    }

    /// The native flush frontier: `min(durable, applied, held latch)` (I2/I6).
    pub(crate) fn last_processed(&self) -> BatchVersion {
        self.loader.last_processed()
    }

    /// The publication watermark, native `current`.
    pub(crate) fn current(&self) -> BatchVersion {
        self.loader.current()
    }

    /// Pins the current publication cut as a read view (native `DbCheckpoint`).
    ///
    /// The cut is the publication watermark, which is monotone and advances only
    /// after every member of every batch below it is inserted: a committed
    /// prefix, so no wait is needed. A latch must be in the flush frontier's
    /// minimum for the whole interval in which a flusher could promote past the
    /// cut, so the pin latches low first:
    ///
    /// 1. latch `NONE`: from here every frontier a flusher computes is `<= NONE`
    ///    (it can only no-op, A11);
    /// 2. sample the publication watermark: it is `>=` every frontier computed
    ///    before step 1, because those are `<= applied <= published` at their
    ///    instant and `published` is monotone;
    /// 3. install the real latch at the sample **before** releasing the low
    ///    one, so there is never an instant with no latch in the minimum.
    ///
    /// Latching the sample after taking it would leave a window in which a
    /// flusher saw `applied > cut` and no latch, promoted past `cut`, and folded
    /// the version this view must still read. Sampling `last_processed()`
    /// instead is wrong for the same reason.
    pub(crate) fn pin_checkpoint(&self) -> (BatchVersion, CutLatch) {
        // Test-only mutation: skip the low latch, i.e. the naive
        // sample-then-latch order, so a negative control can run production code.
        #[cfg(test)]
        let low = (!self.naive_pin.load(std::sync::atomic::Ordering::Relaxed))
            .then(|| self.cuts.pin_cut(<BatchVersion as IndexCut>::NONE));
        #[cfg(not(test))]
        let low = self.cuts.pin_cut(<BatchVersion as IndexCut>::NONE);
        // `pin_cut` ends with a SeqCst fence after storing the latch, so this
        // sample cannot be satisfied from before the latch became visible; the
        // flusher's `last_processed` fences the same way before it reads the
        // latch (`CutLatches::publish_min`).
        let cut = self.current();
        #[cfg(test)]
        self.run_after_pin_sample();
        let latch = self.cuts.pin_cut(cut);
        drop(low);
        (cut, latch)
    }

    /// Test hook run between the publication sample and the real latch of
    /// [`Self::pin_checkpoint`], where a concurrent flusher would act.
    #[cfg(test)]
    pub(crate) fn set_after_pin_sample(&self, hook: Box<dyn Fn() + Send + Sync>) {
        *self.after_pin_sample.lock() = Some(hook);
    }

    /// Test-only: make [`Self::pin_checkpoint`] use the naive order (no low
    /// latch), the mutation the interleaving tests must catch.
    #[cfg(test)]
    pub(crate) fn set_naive_pin(&self) {
        self.naive_pin
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    fn run_after_pin_sample(&self) {
        if let Some(hook) = &*self.after_pin_sample.lock() {
            hook();
        }
    }

    /// Pins `cut`, clamping promotion for as long as the latch lives (I6).
    pub(crate) fn pin_cut(&self, cut: BatchVersion) -> CutLatch {
        self.cuts.pin_cut(cut)
    }

    /// The monotone value-byte counter, native's `wal_writer.position()` in the
    /// byte cadence (A26/A27).
    pub(crate) fn value_bytes(&self) -> u64 {
        self.cuts.bytes()
    }

    pub(crate) fn index_value_bytes(&self) -> u64 {
        self.cuts.bytes()
    }

    pub(crate) fn loader(&self) -> &HybridLoader {
        &self.loader
    }

    pub(crate) fn large_table(&self) -> &LargeTable<VersionPosition> {
        &self.table
    }

    pub(crate) fn config(&self) -> &HybridIndexConfig {
        &self.config
    }

    pub(crate) fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub(crate) fn shape(&self) -> &KeyShape {
        &self.shape
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn contexts(&self) -> &KsContextVec {
        &self.contexts
    }

    pub(crate) fn ks_desc(&self, ks: KeySpace) -> &KeySpaceDesc {
        &self.contexts.ks_context(ks).ks_config
    }

    /// Cold: the snapshot pass is the only writer.
    pub(crate) fn region(&self) -> &Mutex<HybridRegionStore> {
        &self.region
    }

    /// The replay table the last store (or the region loaded at open) named.
    pub(crate) fn stored_fronts(&self) -> Vec<StreamFront> {
        self.region.lock().stream_fronts().to_vec()
    }

    /// The replay table the region loaded at open named, for recovery.
    pub(crate) fn opened_fronts(&self) -> &[StreamFront] {
        &self.opened_fronts
    }

    /// The cut of the region loaded at open: every batch below it is in the
    /// stored blobs, so recovery replay applies none (`replay::replay_into`).
    pub(crate) fn opened_cut(&self) -> BatchVersion {
        self.opened_cut
    }

    /// A stored control region was loaded at open.
    pub(crate) fn region_loaded(&self) -> bool {
        self.region_loaded
    }

    /// Time to read and validate the control region at open, in nanoseconds.
    pub(crate) fn region_load_ns(&self) -> u64 {
        self.region_load_ns
    }

    /// The per-stream allocation view shared with the owner workers.
    pub(crate) fn allocation(&self) -> &Arc<AllocationView> {
        &self.allocation
    }

    pub(crate) fn cuts(&self) -> &CutSources {
        &self.cuts
    }

    /// The serving generation's rows of the replay table the next store writes
    /// (I3/I4); the snapshot pass adds one row per retained older generation
    /// (`snapshot::region_rows`).
    ///
    /// Each stream's front is the minimum of its oldest unapplied offset, its
    /// published tail and its dropped floor, so every allocated extent is
    /// covered; a stream nothing was allocated on reports its tail.
    pub(crate) fn stream_fronts(&self) -> Vec<StreamFront> {
        self.allocation.fronts()
    }

    /// Every cell's byte position, for the V4 region (A26).
    pub(crate) fn cell_bytes(&self) -> BTreeMap<(u8, CellId), u64> {
        self.table.flushed_at_snapshot()
    }

    /// Drains every owner stream up to `target` and waits for each ack.
    ///
    /// One cold request per real store. A worker that already failed drops its
    /// acknowledgement, which surfaces as an error instead of covering a queue
    /// that never drained. An index-only table has no owner stream.
    pub(crate) fn drain_owner_streams(&self, target: BatchVersion) -> Result<(), LocalDbError> {
        match self.streams.get() {
            Some(streams) => streams.drain_to(target),
            None => Ok(()),
        }
    }

    /// Rejects a control region naming a partition this database does not have.
    pub(crate) fn validate_partitions(&self, configured: &[u32]) -> Result<(), LocalDbError> {
        match self
            .opened_fronts
            .iter()
            .find(|front| !configured.contains(&front.partition))
        {
            Some(front) => Err(LocalDbError::new(format!(
                "control region names owner partition {} which this database does not configure",
                front.partition
            ))),
            None => Ok(()),
        }
    }

    /// The index position of `key` as of `cut`, or `None`.
    ///
    /// A cut is exclusive: it must be above the version being read. A delete is
    /// answered by the index itself, which collapses a removed entry to
    /// `INVALID`, so a returned position is always a live put.
    ///
    /// A keyspace with a value cache (`value_cache_size`) can answer from the
    /// native per-cell LRU instead of a position; callers that read values use
    /// [`Self::get_value`], which carries both answers. This method is for the
    /// index-position callers of the test suites and the benchmark evidence
    /// hooks, and reports the LRU answer as an error rather than a wrong
    /// position.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn get(
        &self,
        ks: KeySpace,
        key: &[u8],
        cut: BatchVersion,
    ) -> Result<Option<VersionPosition>, LocalDbError> {
        match self.get_value(ks, key, cut)? {
            Some(ResolvedValue::Position(position)) => Ok(Some(position)),
            Some(ResolvedValue::Resident { .. }) => Err(LocalDbError::new(format!(
                "hybrid index answered from the value cache; {} needs the member position",
                self.ks_desc(ks).name()
            ))),
            None => Ok(None),
        }
    }

    /// What the index answers for `key` as of `cut`: a position to fetch, or
    /// the value already resident in the native per-cell LRU.
    ///
    /// The LRU is the native [`LargeTable`] cache, unchanged: it is filled at
    /// publication and by the read refill, bounded per cell by the keyspace's
    /// `value_cache_size`, and consulted by `LargeTable::get_checkpoint` only
    /// when the key's as-of position is also its latest. `value_cache_size = 0`
    /// allocates no LRU, so a position is always returned.
    ///
    /// A read never resolves like native `Db::get`, which consults the LRU
    /// before the cell's index state: that path also serves a value whose batch
    /// sparse publication installed above the published prefix, and so could
    /// answer later than a key the caller already saw. Every read therefore goes
    /// through the cut-checked lookup, at the watermark a live read samples.
    pub(crate) fn get_value(
        &self,
        ks: KeySpace,
        key: &[u8],
        cut: BatchVersion,
    ) -> Result<Option<ResolvedValue>, LocalDbError> {
        let context = self.contexts.ks_context(ks);
        let reduced = context.ks_config.reduce_key(key);
        match self
            .table
            .get_checkpoint(context, reduced.as_ref(), cut, self.loader.as_ref())?
        {
            GetResult::WalPosition(position) => Ok(position
                .is_valid()
                .then_some(ResolvedValue::Position(position))),
            GetResult::NotFound => Ok(None),
            GetResult::Value(full_key, value) => {
                Ok(Some(ResolvedValue::Resident { full_key, value }))
            }
        }
    }

    /// Installs a value the coordinator just fetched from its owner into the
    /// native per-cell LRU, exactly as native `Db::get` does after a WAL record
    /// read.
    ///
    /// `reduced` is the index key the LRU is keyed by; `position` is the member
    /// position the fetch resolved; `full_key` is the key the member carries,
    /// which a reducing keyspace compares before serving it. A keyspace without
    /// a value cache has no LRU and this is a no-op.
    ///
    /// Only a position that is still the key's latest fills the LRU
    /// ([`LargeTable::refill_lru`]): a remote fetch can outlive a publication
    /// that supersedes it, and the cut check serves a resident value on the
    /// premise that it is the key's latest.
    pub(crate) fn refill_value(
        &self,
        ks: KeySpace,
        reduced: Bytes,
        position: VersionPosition,
        full_key: Bytes,
        value: Bytes,
    ) {
        let context = self.contexts.ks_context(ks);
        self.table
            .refill_lru(context, reduced, position, full_key, value);
    }

    /// The next index entry of a cell, as of `cut` when one is given.
    ///
    /// Index level only: the caller resolves a position through the value path
    /// and advances past an entry its owner no longer holds, exactly as native
    /// `Db::next_entry` does after a failed record read. An unlatched live walk
    /// (`cut: None`) also consults the native per-cell value LRU, as native
    /// `Db::next_entry` does, and yields the resident value instead of a
    /// position; a latched walk (`cut: Some`) never consults it, as native
    /// `DbCheckpoint::next_entry` never does.
    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors native `LargeTable::next_entry`, which takes the same list plus the loader"
    )]
    pub(crate) fn next_entry(
        &self,
        ks: KeySpace,
        cell: CellId,
        prev_key: Option<Bytes>,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
        cache: &mut IndexIterCaches,
        cut: Option<BatchVersion>,
    ) -> Result<Option<IteratorResult<ResolvedValue>>, LocalDbError> {
        let context = self.contexts.ks_context(ks);
        let Some(result) = self.table.next_entry(
            context,
            cell,
            prev_key,
            self.loader.as_ref(),
            end_cell_exclusive,
            reverse,
            cache,
            cut,
        )?
        else {
            return Ok(None);
        };
        let value = match result.value {
            GetResult::WalPosition(position) => ResolvedValue::Position(position),
            GetResult::Value(full_key, value) => ResolvedValue::Resident { full_key, value },
            GetResult::NotFound => {
                return Err(LocalDbError::new(
                    "native index walk returned an entry without a position",
                ));
            }
        };
        Ok(Some(IteratorResult {
            cell: result.cell,
            key: result.key,
            value,
        }))
    }

    /// The next cell of a keyspace, in walk order.
    pub(crate) fn next_cell(
        &self,
        ks: &KeySpaceDesc,
        cell: &CellId,
        reverse: bool,
    ) -> Option<CellId> {
        let context = self.contexts.ks_context(ks.id());
        self.table.next_cell(context, cell, reverse)
    }

    /// The view of one cell at the relocation scan cut `q`, with the latch that
    /// keeps it valid (A22/A23).
    ///
    /// **Guarantee: the view shows every entry at or above `q`, not the prefix
    /// `q`.** An earlier flush may already have promoted entries newer than `q`
    /// into a flat or on-disk level before the latch landed (the flush frontier
    /// can be above `q` until this pin), and a later overwrite of a key never
    /// removes its older version while a latch is held. What the latch fixes is
    /// the lower side: from the pin on no flush promotes past `q`, so the overlay
    /// filter (`retain_processed(q)`) and the on-disk levels describe the same
    /// prefix for every version below `q`. A pass therefore re-points an entry
    /// only by a `same_physical` compare-and-set against the position it read
    /// (`relocation_apply`), never by "version below `q`", so an entry above `q`
    /// that the view happens to contain is re-pointed only when it is still the
    /// physical entry the pass copied, and a newer overwrite is left alone.
    ///
    /// Dropping the latch releases the clamp; a pass holds it for the cell it is
    /// copying and no longer. `relocation_apply` needs the latch held until its
    /// compare-and-set has run (the re-pointed entries are lifted into the
    /// overlay below the flush frontier, the same hazard a request-time frontier
    /// has), so the durable-copy wait of `flush_relocated` also runs under it.
    ///
    /// **Bound on the latch hold.** That wait (`WriteCore::await_copies_durable`)
    /// is at most `8 * durable_cut.lag_ms + config.timeout`, capped at 120 s
    /// (`COPY_DURABLE_LAG_MULTIPLE`, `COPY_DURABLE_WAIT_CAP`), per chunk; on
    /// expiry the pass ends with the typed `copies_not_durable` deferral and the
    /// latch drops with it. While it is held `last_processed` cannot pass `q`,
    /// so no snapshot store moves the stored cut beyond `q`: a stalled owner
    /// delays index promotion and the retirement witness that follows the
    /// stored cut by at most that bound per chunk.
    pub(crate) fn relocation_view(
        &self,
        ks: KeySpace,
        cell: &CellId,
        q: BatchVersion,
    ) -> Result<(IndexTable<VersionPosition>, CutLatch), LocalDbError> {
        self.check_background_health()?;
        let latch = self.cuts.pin_cut(q);
        let context = self.contexts.ks_context(ks);
        let view = self
            .table
            .get_index_for_cell(context, cell, self.loader.as_ref(), q)?
            .map(|view| view.into_table(context))
            .unwrap_or_default();
        Ok((view, latch))
    }

    /// Applies one chunk of a relocation pass to a cell's in-memory index.
    ///
    /// `targets` are `(partition, generation, exclusive byte end)`: an entry is
    /// *below its target* iff its address is in that stream and before the end,
    /// so each entry is judged by its own stream (a single cut would drop live
    /// entries of a higher-target stream and keep dead ones of a lower one).
    ///
    /// * Re-point: a listed key whose current position is `same_physical` with
    ///   the expected one and is below its target takes the new address. The
    ///   logical version never changes, the kind is preserved, and this is never
    ///   an `insert` (which compares equal on the logical identity and would be
    ///   dropped as stale).
    /// * Drop ([`RelocationApply::Drop`]): every live in-memory entry below
    ///   its target leaves the index at every read cut, as native's cutoff
    ///   `retain_above_position` does. The key's other versions below its
    ///   target go with it, and one tombstone at the dropped live position
    ///   keeps a deeper level from resurfacing the key.
    ///
    /// A stale expectation is skipped, never forced: a newer overwrite keeps
    /// its own position. The cell is only marked for flush when something
    /// changed; the next covering store persists it.
    ///
    /// Before the compare-and-set, on-disk L1 data is consulted only for
    /// below-target keys absent from the in-memory table:
    ///
    /// * Unsharded cell: loads `levels.l1()` once and merges the fresher live
    ///   table on top, so a post-promote `[INVALID, L1]` cell is re-pointed
    ///   instead of skipped.
    /// * Sharded cell: uses [`IndexLevels::shard_for`] to load only the shards
    ///   owning missing keys (at most once per shard, one shard in memory at a
    ///   time) and lifts only re-pointed entries into `index`, matching native
    ///   `flusher.rs` `lift_shard_entries`. Untouched shards are never read and
    ///   never copied into live `index`; `RelocationApply::Drop` scans `index`
    ///   (L0 plus overlay) alone.
    pub(crate) fn relocation_apply(
        &self,
        ks: KeySpace,
        cell: &CellId,
        mode: RelocationApply<'_>,
        targets: &[(u32, u64, u64)],
    ) -> Result<(), LocalDbError> {
        self.check_background_health()?;
        let (updates, drop_entries) = match mode {
            RelocationApply::Repoint(updates) => (updates, false),
            RelocationApply::Drop => (&[][..], true),
        };
        let below_target = |position: VersionPosition| {
            let address = position.address;
            targets.iter().any(|&(partition, generation, end)| {
                address.partition == partition
                    && address.segment_generation == generation
                    && address.offset < end
            })
        };
        let context = self.contexts.ks_context(ks);
        let loader = self.loader.as_ref();
        let apply_in_index = |index: &mut IndexTable<VersionPosition>,
                              key: &[u8],
                              expected: &VersionPosition,
                              new: &VersionPosition,
                              changed: &mut bool|
         -> bool {
            let mut examined = false;
            index.apply_update(key, |tagged| {
                examined = true;
                let (current, kind) = VersionPosition::untag(*tagged);
                if kind != IndexEntryKind::Removed
                    && current.same_physical(expected)
                    && below_target(current)
                {
                    *tagged = new.tag(kind);
                    *changed = true;
                }
            });
            examined
        };
        self.table
            .update_cell_index(context, cell, loader, |index, levels| {
                let mut changed = false;
                if levels.is_sharded() {
                    // Sharded cell: inspect L0/overlay first and load only the
                    // L1 shards owning below-target keys absent from `index`,
                    // matching native `flusher.rs` `lift_shard_entries`.
                    let mut missing: Vec<(
                        WalPosition,
                        usize,
                        &(Bytes, VersionPosition, VersionPosition),
                    )> = Vec::new();
                    for (idx, item) in updates.iter().enumerate() {
                        let (key, expected, new) = item;
                        if index.get_update_position(key).is_some() {
                            let examined = apply_in_index(index, key, expected, new, &mut changed);
                            debug_assert!(examined, "key present in index must be examined");
                        } else if below_target(*expected)
                            && let Some(shard) = levels.shard_for(key)
                        {
                            missing.push((shard.position, idx, item));
                        }
                    }
                    // Group by shard position so each needed shard is loaded at
                    // most once regardless of input order, holding one shard in
                    // memory at a time; tie-break by `idx` for duplicate keys.
                    missing.sort_unstable_by_key(|&(position, idx, _)| (position, idx));
                    let mut cached: Option<(WalPosition, IndexTable<VersionPosition>)> = None;
                    for (shard_pos, _, (key, expected, new)) in missing {
                        if index.get_update_position(key).is_some() {
                            let examined = apply_in_index(index, key, expected, new, &mut changed);
                            debug_assert!(examined, "key present in index must be examined");
                            continue;
                        }
                        if cached
                            .as_ref()
                            .is_none_or(|(cached_pos, _)| *cached_pos != shard_pos)
                        {
                            let loaded = loader.load(&context.ks_config, shard_pos)?;
                            cached = Some((shard_pos, loaded));
                        }
                        match cached.as_ref().and_then(|(_, table)| table.get(key)) {
                            Some(current)
                                if current.same_physical(expected) && below_target(current) =>
                            {
                                // Shard blobs are deepest-level L1 (no tombstones)
                                // and `key` is absent from `index`: lift only the
                                // re-pointed entry so untouched shard rows stay
                                // out of live `index` and untouched shards are not
                                // rewritten on the next promote.
                                index.insert(key.clone(), *new);
                                changed = true;
                            }
                            // A different shard entry (or none) means the
                            // compare-and-set has nothing to re-point.
                            Some(_) | None => {}
                        }
                    }
                } else {
                    // Pre-load: `apply_update` sees only the in-memory table,
                    // so a key that lives only in L1 (the common post-promote
                    // `[INVALID, L1]` cell) is invisible to it. Loaded only
                    // when a below-target key is absent from `index`.
                    if let Some(position) = levels.l1()
                        && updates.iter().any(|(key, expected, _)| {
                            below_target(*expected) && index.get_update_position(key).is_none()
                        })
                    {
                        let mut merged = loader.load(&context.ks_config, position)?;
                        // The live table is the fresher side: its L0 rows and
                        // its overlay win over an L1 entry for the same key.
                        merged.merge_dirty_no_clean(index);
                        *index = merged;
                    }
                    for (key, expected, new) in updates {
                        apply_in_index(index, key, expected, new, &mut changed);
                    }
                }
                if drop_entries {
                    drop_index_rows_preserving_resident_values(index, &below_target, &mut changed);
                }
                Ok(((), changed))
            })
            .map_err(|error| match error {
                CellUpdateError::Loader(error) => error,
            })?;
        Ok(())
    }

    /// Counts a cadence pass that stored nothing because a reader latch held the
    /// flush frontier below the stored head (never for an unmoved cut).
    pub(crate) fn record_snapshot_latch_deferral(&self) {
        // Relaxed: a diagnostic counter; no other memory is published by it.
        self.latch_deferrals.fetch_add(1, Ordering::Relaxed);
    }

    /// Total of [`Self::record_snapshot_latch_deferral`].
    pub(crate) fn snapshot_latch_deferrals(&self) -> u64 {
        self.latch_deferrals.load(Ordering::Relaxed)
    }

    /// Counts an older-generation row written without a known durable end.
    pub(crate) fn record_older_row_without_end(&self) {
        // Relaxed: a diagnostic counter; no other memory is published by it.
        self.older_rows_without_end.fetch_add(1, Ordering::Relaxed);
    }

    /// Total of [`Self::record_older_row_without_end`].
    pub(crate) fn older_rows_without_end(&self) -> u64 {
        self.older_rows_without_end.load(Ordering::Relaxed)
    }

    /// Inserts one published batch's members and charges its bytes.
    ///
    /// Publication calls this once per batch under its own ordering, and a reader
    /// may only ask at a cut the applied front has reached, so no reader observes
    /// a partially inserted batch even though members land one at a time.
    ///
    /// A `Put` carries the value the batch wrote, so the native per-cell LRU is
    /// filled at the visibility point and only there: a batch that never
    /// published (an uncertain or aborted write) never reaches this call and
    /// leaves no resident value behind.
    pub(crate) fn insert_published(
        &self,
        mutations: &[PublishedMutation<'_>],
        value_bytes: u64,
    ) -> Result<(), LocalDbError> {
        self.check_background_health()?;
        // Refuse a construction bug before the first member lands: a half
        // inserted batch would be visible at the applied cut.
        if let Some(missing) = mutations
            .iter()
            .find(|mutation| mutation.kind == RecordKind::Put && mutation.value.is_none())
        {
            return Err(LocalDbError::new(format!(
                "published put of keyspace {} carries no resident value",
                self.contexts.ks_context(missing.keyspace).name()
            )));
        }
        self.cuts.add_bytes(value_bytes);
        let shards = self.pending_promotion_threads.len();
        let mut wake = vec![false; shards];
        for mutation in mutations {
            let context = self.contexts.ks_context(mutation.keyspace);
            let key = Bytes::copy_from_slice(mutation.key);
            let reduced = context.ks_config.reduced_key_bytes(key.clone());
            match (mutation.kind, mutation.value) {
                (RecordKind::Put, Some(value)) => self.table.insert(
                    context,
                    reduced.clone(),
                    key,
                    mutation.position,
                    value,
                    self.loader.as_ref(),
                )?,
                (RecordKind::Put, None) => {
                    // Checked above; the arm keeps the match total without a
                    // panic if the two ever disagree.
                    return Err(LocalDbError::new("published put carries no resident value"));
                }
                (RecordKind::Delete, _) => {
                    self.table.remove(
                        context,
                        reduced.clone(),
                        mutation.position,
                        self.loader.as_ref(),
                    )?;
                }
            }
            let mutex = self.table.mark_pending_dirty_for(context, &reduced);
            wake[mutex % shards] = true;
        }
        // Native unparks the shards its batch's mutex shards hash to; the hybrid
        // publishes cell by cell, so it does the same for every touched shard.
        for (shard, touched) in wake.iter().enumerate() {
            if *touched && let Some(worker) = self.pending_promotion_threads[shard].get() {
                worker.unpark();
            }
        }
        Ok(())
    }

    /// Advances the contiguous applied prefix, which is also the visible one.
    ///
    /// `next` is the exclusive cut below which every published batch is fully
    /// inserted; `published_next` is the only value that satisfies it (I2), and
    /// the publisher is the only component that knows it. Publication inserts
    /// before it advances, so the applied cut and the publication watermark are
    /// the same prefix and are advanced together.
    ///
    /// An untracked epoch (a Synchronous owner set) has no durable-cut record to
    /// feed the durable front, and none is needed: a batch is acknowledged, and
    /// so published, only after its owner synced it, so the published prefix is
    /// already durable (A26). A tracked epoch's durable front is fed only by the
    /// persisted durable-cut record (A25), never from here.
    pub(crate) fn advance_applied(&self, next: BatchVersion) {
        // Published first: an `applied` value is only ever visible after the
        // `published` value that covers it (a flusher that read `applied` is
        // ordered after `published`, which the pin's sample relies on).
        self.cuts.set_published(next);
        self.cuts.set_applied(next);
        if self.cuts.tier() == DurabilityTier::Synchronous {
            self.cuts.set_durable(next);
        }
    }

    /// Clears an index cell range (native `DropCells`). Index-only: the values
    /// stay on their owners until retirement.
    pub(crate) fn drop_cells(
        &self,
        ks: KeySpace,
        from: CellId,
        to: CellId,
        _version: BatchVersion,
    ) -> Result<(), LocalDbError> {
        self.check_background_health()?;
        let context = self.contexts.ks_context(ks);
        self.table.drop_cells_in_range(context, &from, &to);
        Ok(())
    }

    /// Native `Db::check_background_health`: once a flusher (or any worker the
    /// shared health tracks) failed, every later mutation and snapshot is
    /// refused instead of acknowledging writes into cells that can never flush.
    fn check_background_health(&self) -> Result<(), LocalDbError> {
        self.health.check().map_err(|error| {
            LocalDbError::new(format!("hybrid index background worker failed: {error:?}"))
        })
    }
}

/// Drops every version of each key a relocation filter pruned below its target,
/// leaving one `Removed` tombstone per dropped key.
///
/// Native's relocation cutoff retains only entries at or above it
/// (`IndexTable::retain_above_position`), so a key whose live positions all sit
/// below the cutoff leaves no entry behind at any read cut. The hybrid keeps one
/// tombstone per dropped key so a deeper level cannot resurface it, and drops
/// the key's older versions: leaving them would let a read cut below the latest
/// version still resolve one, a property native does not have. They cannot be
/// removed one at a time (`apply_update` sees only the latest), so the key is
/// removed and its tombstone re-inserted at the dropped live position.
///
/// The per-cell value LRU is deliberately left alone, and this helper is the one
/// place that rule lives. Native prunes index rows on a relocation cutoff
/// (`IndexTable::retain_above_position`) and never touches the value LRU, because
/// a read resolves the key's entry first: a `Removed` position is not valid at
/// any cut, so `LargeTable::get_checkpoint` answers not-found before it consults
/// the LRU. The other native rule is a dropped *cell*: `drop_cells_in_range`
/// clears the cell together with its LRU, which [`TableInner::drop_cells`]
/// inherits unchanged. A rebase that adds a drop path must route it through one
/// of these two helpers rather than inventing its own cache invalidation.
fn drop_index_rows_preserving_resident_values(
    index: &mut IndexTable<VersionPosition>,
    below_target: &impl Fn(VersionPosition) -> bool,
    changed: &mut bool,
) {
    let doomed: Vec<(Bytes, VersionPosition)> = index
        .iter()
        .filter(|(_, position)| below_target(*position))
        .collect();
    if doomed.is_empty() {
        return;
    }
    let dropped: HashSet<&[u8]> = doomed.iter().map(|(key, _)| key.as_ref()).collect();
    index.compact_with(|keys| {
        keys.filter(|key| !dropped.contains(key.as_ref()))
            .cloned()
            .collect()
    });
    for (key, position) in doomed {
        index.remove(key, position);
    }
    *changed = true;
}

/// An all-empty snapshot container for a directory without a control region.
fn empty_snapshot(shape: &KeyShape) -> LargeTableContainer<SnapshotEntryData<BatchVersion>> {
    LargeTableContainer {
        data: shape
            .iter_ks()
            .map(|ks| LargeTableContainer::new_keyspace(ks, SnapshotEntryData::empty()))
            .collect(),
    }
}

impl Loader for TableInner {
    type Position = VersionPosition;
    type Error = LocalDbError;

    fn load(
        &self,
        ks: &KeySpaceDesc,
        position: WalPosition,
    ) -> Result<IndexTable<VersionPosition>, LocalDbError> {
        self.loader.load(ks, position)
    }

    fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, LocalDbError> {
        self.loader.index_reader(position)
    }

    fn index_scan_reader(&self, position: WalPosition) -> Result<WalRandomRead, LocalDbError> {
        self.loader.index_scan_reader(position)
    }

    fn flush_supported(&self) -> bool {
        self.loader.flush_supported()
    }

    fn flush(
        &self,
        ks: KeySpace,
        data: &IndexTable<VersionPosition>,
    ) -> Result<WalPosition, LocalDbError> {
        self.loader.flush(ks, data)
    }

    fn last_processed(&self) -> BatchVersion {
        self.loader.last_processed()
    }

    fn current(&self) -> BatchVersion {
        self.loader.current()
    }

    fn min_position(&self) -> BatchVersion {
        self.loader.min_position()
    }

    fn current_position(&self) -> VersionPosition {
        self.loader.current_position()
    }

    fn flushed_at_bytes(&self, cut: BatchVersion) -> u64 {
        self.loader.flushed_at_bytes(cut)
    }
}

impl FlusherHost<VersionPosition> for TableInner {
    fn update_flushed_index(
        &self,
        ks: KeySpace,
        cell: CellId,
        original: Arc<IndexTable<VersionPosition>>,
        new_levels: IndexLevels,
    ) {
        let context = self.contexts.ks_context(ks);
        let dropped = self
            .table
            .update_flushed_index(context, &cell, original, new_levels);
        self.loader.retire_index_blobs(&dropped);
    }

    fn update_relocated_index(&self, ks: KeySpace, cell: CellId, new_levels: IndexLevels) {
        let context = self.contexts.ks_context(ks);
        let dropped = self
            .table
            .update_relocated_index(context, &cell, new_levels);
        self.loader.retire_index_blobs(&dropped);
    }

    fn ks_context(&self, ks: KeySpace) -> &KsContext {
        self.contexts.ks_context(ks)
    }

    fn before_flush_completion(&self) {
        self.table.fp.fp_flush_before_completion();
    }

    fn background_health(&self) -> Arc<BackgroundHealth> {
        self.health.clone()
    }
}

impl Drop for TableInner {
    fn drop(&mut self) {
        // The parked promotion threads hold only a `Weak`; wake each so it can
        // observe the dead table and exit, as native `Db::drop` does.
        for slot in self.pending_promotion_threads.iter() {
            if let Some(thread) = slot.get() {
                thread.unpark();
            }
        }
    }
}

/// Starts the event-driven promotion thread of one mutex shard (native
/// `Db::start_pending_promotion_job`). It parks until a publication that
/// touched the shard unparks it, then promotes and checks the flush threshold
/// of every cell in the shard.
///
/// The thread holds the table only for one pass, so a dropped table lets it
/// exit; the opener registered the handle before returning so the first
/// publication cannot miss the wakeup.
fn start_pending_promotion_job(
    table: std::sync::Weak<TableInner>,
    shard_idx: usize,
    num_shards: usize,
) -> Result<thread::JoinHandle<()>, LocalDbError> {
    let health = Arc::clone(
        &table
            .upgrade()
            .ok_or_else(|| LocalDbError::new("table dropped before its workers started"))?
            .health,
    );
    thread::Builder::new()
        .name(format!("pending-promotion-{shard_idx}"))
        .spawn(move || {
            health.run(BackgroundThread::PendingPromotion, || {
                loop {
                    thread::park();
                    let Some(table) = table.upgrade() else { break };
                    let _timer = table
                        .metrics
                        .pending_promotion_job_time_mcs
                        .clone()
                        .mcs_timer();
                    if let Err(error) =
                        table
                            .table
                            .promote_dirty_pending(table.as_ref(), shard_idx, num_shards)
                    {
                        // `BackgroundHealth::record_error` speaks native `DbError`;
                        // the hybrid error carries the same kind and text.
                        let failure = DbError::Io(std::io::Error::new(
                            error.kind(),
                            error.message().to_owned(),
                        ));
                        table
                            .health
                            .record_error(BackgroundThread::PendingPromotion, &failure);
                        eprintln!("Error in hybrid pending promotion job: {error:?}");
                    }
                }
            });
        })
        .map_err(|error| {
            LocalDbError::new(format!(
                "start pending-promotion thread {shard_idx}: {error}"
            ))
        })
}

#[cfg(test)]
mod tests;
