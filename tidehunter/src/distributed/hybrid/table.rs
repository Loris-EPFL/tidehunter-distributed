//! The hybrid table: the coordinator's native index over remote value positions.
//!
//! Native `Db` owns one `LargeTable`, its `IndexFlusher` threads and the local
//! value WAL that gives every position a meaning. The hybrid keeps the first
//! two unchanged and replaces the third with the remote value WAL: a
//! [`VersionPosition`] names its member frame on its owner, so the index can be
//! read, flushed, relocated and snapshotted by exactly the native code while
//! the values stay on the owners.
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
    HybridRegionStore, MAX_REGION_STREAMS, StreamFront, max_flushed_at, max_region_cells,
};
use super::snapshot::{self, StoredSnapshot};
use crate::background_health::BackgroundHealth;
use crate::cell::CellId;
use crate::container::LargeTableContainer;
use crate::context::{KsContext, KsContextVec};
use crate::db::CONTROL_REGION_FILE;
use crate::db::DbError;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::remote_db::StreamControls;
use crate::distributed::storage::LocalDbError;
use crate::distributed::types::{BatchVersion, RecordKind};
use crate::flusher::{FlusherHost, IndexFlusher};
use crate::index::index_format::IndexIterCaches;
use crate::index::index_table::IndexTable;
use crate::index::levels::IndexLevels;
use crate::iterators::IteratorResult;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc};
use crate::large_table::{CellUpdateError, GetResult, LargeTable, Loader, SnapshotEntryData};
use crate::metrics::Metrics;
use crate::wal::layout::WalKind;
use crate::wal::position::{IndexCut, IndexEntryKind, IndexPosition, WalPosition};
use crate::wal::{Wal, WalRandomRead};
use minibytes::Bytes;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashSet};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock, mpsc};

/// The value a `put` is indexed with.
///
/// The coordinator never holds a published value: it forwarded the member to
/// its owner before publication, and the native per-cell LRU is refused at open
/// ([`require_owner_resident_values`]). Native `insert` takes `&Bytes` only to
/// fill that LRU, so the empty value is the faithful argument.
static NO_RESIDENT_VALUE: LazyLock<Bytes> = LazyLock::new(Bytes::new);

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
    dir: PathBuf,
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
        require_owner_resident_values(&shape)?;
        let native_config = Arc::new(config.to_native_config());
        let health = Arc::new(BackgroundHealth::default());
        let index_wal = Wal::open_with_background_health(
            dir,
            native_config.wal_layout(WalKind::Index),
            metrics.clone(),
            health.clone(),
        )
        .map_err(|error| LocalDbError::new(format!("open index WAL: {error}")))?;
        let (region_store, region) =
            HybridRegionStore::open(dir.join(CONTROL_REGION_FILE), &shape)?;
        if let Some(region) = &region {
            region.validate_bounds(MAX_REGION_STREAMS, max_region_cells(&shape))?;
        }
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
        ));
        let loader = Arc::new(HybridLoader::new(
            index_wal,
            index_writer,
            contexts.clone(),
            cuts.clone(),
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
            loader.as_ref(),
            health.clone(),
        );
        let opened_fronts = region
            .as_ref()
            .map(|region| region.streams.clone())
            .unwrap_or_default();
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
            dir: dir.to_path_buf(),
            shape,
            config: config.clone(),
            metrics: metrics.clone(),
            contexts,
            loader,
            table,
            region: Mutex::new(region_store),
            opened_fronts,
            cuts,
            allocation,
            health,
            streams: OnceLock::new(),
            latch_deferrals: AtomicU64::new(0),
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

    /// One native snapshot pass (the cadence thread's call).
    pub(crate) fn snapshot(&self) -> Result<StoredSnapshot, LocalDbError> {
        self.check_background_health()?;
        snapshot::rebuild_control_region(self, false, None)
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
    pub(crate) fn snapshot_for_relocation(
        &self,
        q: BatchVersion,
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
        snapshot::rebuild_control_region(self, true, Some(promotion_cut))
    }
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

    /// The cut the stored region names.
    pub(crate) fn stored_cut(&self) -> BatchVersion {
        self.region.lock().last_position()
    }

    /// The cut the next store may name: `max(previous, min(durable, applied,
    /// lowest held latch))` (A9). Never moves back, so a checkpoint held below
    /// the stored head only makes the store a no-op.
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

    /// The per-stream allocation view shared with the owner workers.
    pub(crate) fn allocation(&self) -> &Arc<AllocationView> {
        &self.allocation
    }

    pub(crate) fn cuts(&self) -> &CutSources {
        &self.cuts
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// The replay table the next store writes (I3/I4).
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
    pub(crate) fn get(
        &self,
        ks: KeySpace,
        key: &[u8],
        cut: BatchVersion,
    ) -> Result<Option<VersionPosition>, LocalDbError> {
        let context = self.contexts.ks_context(ks);
        let reduced = context.ks_config.reduce_key(key);
        match self
            .table
            .get_checkpoint(context, reduced.as_ref(), cut, self.loader.as_ref())?
        {
            GetResult::WalPosition(position, _) => Ok(position.is_valid().then_some(position)),
            GetResult::NotFound => Ok(None),
            // `require_owner_resident_values` refuses a value cache at open.
            GetResult::Value(..) => Err(LocalDbError::new(
                "hybrid index returned a locally cached value; the value lives on its owner",
            )),
        }
    }

    /// The next index entry of a cell, as of `cut` when one is given.
    ///
    /// Index level only: the caller resolves the position through the value path
    /// and advances past an entry its owner no longer holds, exactly as native
    /// `Db::next_entry` does after a failed record read.
    pub(crate) fn next_entry(
        &self,
        ks: KeySpace,
        cell: CellId,
        prev_key: Option<Bytes>,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
        cache: &mut IndexIterCaches,
        cut: Option<BatchVersion>,
    ) -> Result<Option<IteratorResult<VersionPosition>>, LocalDbError> {
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
        match result.value {
            GetResult::WalPosition(position, _) => Ok(Some(IteratorResult {
                cell: result.cell,
                key: result.key,
                value: position,
            })),
            GetResult::Value(..) => Err(LocalDbError::new(
                "hybrid index returned a locally cached value; the value lives on its owner",
            )),
            GetResult::NotFound => Err(LocalDbError::new(
                "native index walk returned an entry without a position",
            )),
        }
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

    /// The as-of view of one cell at the relocation scan cut `q`, with the latch
    /// that keeps it valid (A22/A23).
    ///
    /// The latch goes in first: from then on no flush promotes past `q`, so the
    /// overlay filter (`retain_processed(q)`) and the on-disk levels describe the
    /// same prefix. Dropping the latch releases the clamp; a pass holds it for the
    /// cell it is copying and no longer.
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
            // A freshly merged table is unshared; the clone is the cold fallback.
            .map(|view| Arc::try_unwrap(view).unwrap_or_else(|shared| (*shared).clone()))
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
    /// * Drop: with `drop_removed`, every live entry not listed and below its
    ///   target becomes a tombstone at its own position (as native's
    ///   `tombstone_below_position`: dropping would expose an older level).
    ///
    /// A stale expectation is skipped, never forced. The cell is only marked for
    /// flush when something changed; the next covering store persists it.
    pub(crate) fn relocation_apply(
        &self,
        ks: KeySpace,
        cell: &CellId,
        updates: Option<&[(Bytes, VersionPosition, VersionPosition)]>,
        targets: &[(u32, u64, u64)],
        drop_removed: bool,
    ) -> Result<(), LocalDbError> {
        self.check_background_health()?;
        let below_target = |position: VersionPosition| {
            let address = position.address;
            targets.iter().any(|&(partition, generation, end)| {
                address.partition == partition
                    && address.segment_generation == generation
                    && address.offset < end
            })
        };
        let updates = updates.unwrap_or_default();
        let listed: HashSet<&[u8]> = updates.iter().map(|(key, ..)| key.as_ref()).collect();
        let context = self.contexts.ks_context(ks);
        self.table
            .update_cell_index(context, cell, self.loader.as_ref(), |index| {
                let mut changed = false;
                for (key, expected, new) in updates {
                    index.apply_update(key, |tagged| {
                        let (current, kind) = VersionPosition::untag(*tagged);
                        if kind != IndexEntryKind::Removed
                            && current.same_physical(expected)
                            && below_target(current)
                        {
                            *tagged = new.tag(kind);
                            changed = true;
                        }
                    });
                }
                if drop_removed {
                    let doomed: Vec<Bytes> = index
                        .iter()
                        .filter(|(key, position)| {
                            !listed.contains(key.as_ref()) && below_target(*position)
                        })
                        .map(|(key, _)| key)
                        .collect();
                    for key in doomed {
                        index.apply_update(&key, |tagged| {
                            *tagged = VersionPosition::with_kind(*tagged, IndexEntryKind::Removed);
                            changed = true;
                        });
                    }
                }
                ((), changed)
            })
            .map(|_| ())
            .map_err(|error| match error {
                CellUpdateError::Background(error) => db_error("relocation apply", error),
                CellUpdateError::Loader(error) => error,
            })
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

    /// Inserts one published batch's members and charges its bytes.
    ///
    /// Publication calls this once per batch under its own ordering, and a reader
    /// may only ask at a cut the applied front has reached, so no reader observes
    /// a partially inserted batch even though members land one at a time.
    pub(crate) fn insert_published(
        &self,
        mutations: &[PublishedMutation<'_>],
        value_bytes: u64,
    ) -> Result<(), LocalDbError> {
        self.check_background_health()?;
        self.cuts.add_bytes(value_bytes);
        for mutation in mutations {
            let context = self.contexts.ks_context(mutation.keyspace);
            let key = Bytes::copy_from_slice(mutation.key);
            let reduced = context.ks_config.reduced_key_bytes(key.clone());
            match mutation.kind {
                RecordKind::Put => self.table.insert(
                    context,
                    reduced,
                    key,
                    mutation.position,
                    &NO_RESIDENT_VALUE,
                    self.loader.as_ref(),
                )?,
                RecordKind::Delete => {
                    self.table
                        .remove(context, reduced, mutation.position, self.loader.as_ref())?
                }
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
        self.table
            .drop_cells_in_range(context, &from, &to)
            .map_err(|error| db_error("drop cells in range", error))
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

/// Maps a native `DbError` keeping the kind the value path keys on: an I/O error
/// keeps its own kind and corruption is `InvalidData` (never retried); anything
/// else is a background failure, kind `Other`.
fn db_error(context: &str, error: DbError) -> LocalDbError {
    match error {
        DbError::Io(error) => LocalDbError::with_kind(error.kind(), format!("{context}: {error}")),
        DbError::CrCorrupted | DbError::CorruptedIndexEntry(_) | DbError::WalError(_) => {
            LocalDbError::with_kind(
                std::io::ErrorKind::InvalidData,
                format!("{context}: {error:?}"),
            )
        }
        DbError::BackgroundFailure(_) => LocalDbError::new(format!("{context}: {error:?}")),
    }
}

/// A hybrid database never keeps the value it publishes.
///
/// Native `Db::insert` fills the per-cell value LRU from the value it just
/// wrote. The coordinator does not hold the value after publication, so a
/// keyspace with a value cache would answer empty values from that LRU. Sui sets
/// `value_cache_size = 0`; anything else is refused at open rather than silently
/// returning empty bytes.
fn require_owner_resident_values(shape: &KeyShape) -> Result<(), LocalDbError> {
    match shape.iter_ks().find(|ks| ks.value_cache_size().is_some()) {
        Some(ks) => Err(LocalDbError::new(format!(
            "hybrid index requires value_cache_size = 0 for keyspace {}; values live on their \
             owners",
            ks.name()
        ))),
        None => Ok(()),
    }
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
        self.table
            .update_flushed_index(context, &cell, original, new_levels)
    }

    fn update_relocated_index(&self, ks: KeySpace, cell: CellId, new_levels: IndexLevels) {
        let context = self.contexts.ks_context(ks);
        self.table
            .update_relocated_index(context, &cell, new_levels)
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

#[cfg(test)]
mod tests;
