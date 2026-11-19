use crate::WalPosition;
use crate::batch::RelocatedWriteBatch;
use crate::db::{BackgroundThread, Db, DbResult, WalEntry};
use crate::index::index_table::IndexTable;
use crate::key_shape::KeySpace;
use crate::metrics::Metrics;
use crate::relocation::watermark::RelocationWatermarks;
use crate::wal::WalError;
use crate::wal::position::LastProcessed;
pub use cell_reference::CellReference;
use minibytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Weak, mpsc};
use std::thread::JoinHandle;

mod cell_reference;
pub(crate) mod watermark;

#[cfg(test)]
mod relocation_tests;
pub mod updates;

/// Computes a target WAL position based on a ratio of the total WAL range.
///
/// # Arguments
/// * `db` - The database instance
/// * `ratio` - A value between 0.0 and 1.0 representing the fraction of WAL to process.
///   0.0 = start of WAL, 0.5 = middle, 1.0 = end (last_processed)
///
/// # Returns
/// * `Some(position)` - The computed target position in bytes
/// * `None` - If there is no WAL data to process (last_processed <= min_position)
///
/// # Examples
/// ```ignore
/// // Relocate the first 30% of the WAL
/// let target = compute_target_position_from_ratio(&db, 0.3);
/// db.start_relocation_with_strategy(RelocationStrategy::IndexBased(target));
/// ```
pub fn compute_target_position_from_ratio(db: &Arc<Db>, ratio: f64) -> Option<u64> {
    // Clamp ratio to valid range [0.0, 1.0]
    let ratio = ratio.clamp(0.0, 1.0);

    // Get the WAL range
    let min_position = db.wal.min_wal_position();
    let last_processed = db.wal_writer.last_processed().as_u64();

    // Handle edge cases
    if last_processed <= min_position {
        return None; // WAL is empty or in invalid state
    }

    // Compute the total range
    let total_range = last_processed - min_position;

    // Calculate target position
    let offset = (total_range as f64 * ratio) as u64;
    let target = min_position + offset;

    // Ensure we don't exceed last_processed
    Some(target.min(last_processed))
}

pub(crate) struct Relocator(pub(crate) mpsc::Sender<RelocationCommand>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelocationStrategy {
    /// WAL-based sequential relocation
    WalBased,
    /// Index-based relocation that processes entire cells atomically
    /// with an optional target position limit
    IndexBased(Option<u64>),
}

#[allow(clippy::derivable_impls)] // Can't derive Default with tuple variant
impl Default for RelocationStrategy {
    fn default() -> Self {
        RelocationStrategy::WalBased
    }
}

pub enum RelocationCommand {
    Start(RelocationStrategy),
    Cancel(mpsc::Sender<()>),
    StartBlocking(RelocationStrategy, mpsc::Sender<()>),
    // The requesting test owns only the reply receiver. Capacity one lets the
    // worker send without waiting; a timed-out test cannot kill the worker.
    #[cfg(feature = "test_methods")]
    ObserveWal {
        deadline: std::time::Instant,
        reply: mpsc::SyncSender<Result<RelocationObservation, RelocationTestError>>,
    },
}

#[cfg_attr(feature = "test_methods", derive(Debug, Clone, Copy, PartialEq, Eq))]
pub enum Decision {
    Keep,
    Remove,
    StopRelocation,
}

pub trait RelocationFilter: Fn(&[u8], &[u8]) -> Decision + Send + Sync + 'static {}
impl<F> RelocationFilter for F where F: Fn(&[u8], &[u8]) -> Decision + Send + Sync + 'static {}

#[cfg(feature = "test_methods")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelocationGate {
    NoWork,
    DidWork,
}

#[cfg(feature = "test_methods")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelocationWorkerWitness {
    Completed,
}

#[cfg(feature = "test_methods")]
#[derive(Debug, PartialEq, Eq)]
pub struct RelocationCallback {
    pub keyspace: KeySpace,
    pub key: Bytes,
    pub value: Bytes,
    pub wal_position: u64,
    pub decision: Decision,
}

#[cfg(feature = "test_methods")]
#[derive(Debug, PartialEq, Eq)]
pub struct RelocationStreamId {
    pub partition: u32,
    pub fencing_generation: u64,
}

#[cfg(feature = "test_methods")]
#[derive(Debug, PartialEq, Eq)]
pub struct StreamObservation {
    pub stream_id: RelocationStreamId,
    pub processed_upper: u64,
    pub terminal_position: u64,
    pub rounded_target: u64,
}

#[cfg(feature = "test_methods")]
#[derive(Debug, PartialEq, Eq)]
pub struct RelocationObservation {
    pub gate: RelocationGate,
    pub streams: Vec<StreamObservation>,
    pub callbacks: Vec<RelocationCallback>,
    pub copied_survivors: u64,
    pub witness: RelocationWorkerWitness,
}

#[cfg(feature = "test_methods")]
#[derive(Debug)]
pub enum RelocationTestError {
    Busy,
    DeadlineExceeded,
    WorkerStopped,
    UnsupportedStrategy,
    UnsupportedCodec,
    Native(crate::db::DbError),
}

#[cfg(feature = "test_methods")]
impl std::fmt::Display for RelocationTestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => f.write_str("observed relocation was rejected: worker busy"),
            Self::DeadlineExceeded => f.write_str("observed relocation exceeded its deadline"),
            Self::WorkerStopped => f.write_str("observed relocation worker stopped"),
            Self::UnsupportedStrategy => f.write_str("observed relocation requires WalBased"),
            Self::UnsupportedCodec => {
                f.write_str("WalBased relocation does not support batch compression")
            }
            Self::Native(error) => write!(f, "observed relocation failed: {error:?}"),
        }
    }
}

#[cfg(feature = "test_methods")]
impl std::error::Error for RelocationTestError {}

struct CellProcessingContext {
    batch: RelocatedWriteBatch,
    highest_wal_position: WalPosition,
    entries_removed: u64,
    entries_kept: u64,
}

impl CellProcessingContext {
    fn new(batch: RelocatedWriteBatch) -> Self {
        Self {
            batch,
            highest_wal_position: WalPosition::new(0, 0),
            entries_removed: 0,
            entries_kept: 0,
        }
    }

    fn add_entry_to_relocate(&mut self, key: Bytes, value: Bytes, position: WalPosition) {
        self.batch.write(key, value);
        self.entries_kept += 1;
        if position.offset() > self.highest_wal_position.offset() {
            self.highest_wal_position = position;
        }
    }

    fn mark_entry_removed(&mut self, position: WalPosition) {
        self.entries_removed += 1;
        if position.offset() > self.highest_wal_position.offset() {
            self.highest_wal_position = position;
        }
    }
}

pub(crate) struct RelocationDriver {
    db: Weak<Db>,
    path: PathBuf,
    receiver: mpsc::Receiver<RelocationCommand>,
    metrics: Arc<Metrics>,
}

impl RelocationDriver {
    const NUM_ITERATIONS_IN_BATCH: usize = 1000;
    const NUM_ITERATIONS_TILL_SAVE: usize = 100000;

    pub fn start(
        db: Weak<Db>,
        path: PathBuf,
        receiver: mpsc::Receiver<RelocationCommand>,
        metrics: Arc<Metrics>,
    ) -> JoinHandle<()> {
        let health = Arc::clone(
            &db.upgrade()
                .expect("opener keeps the database alive")
                .background_health,
        );
        let driver = Self {
            db,
            path,
            receiver,
            metrics,
        };
        std::thread::Builder::new()
            .name("relocator".to_string())
            .spawn(move || health.run(BackgroundThread::Relocator, || driver.run()))
            .unwrap()
    }

    pub fn run(mut self) {
        while let Ok(command) = self.receiver.recv() {
            match command {
                RelocationCommand::Start(strategy) => {
                    self.relocation_run(strategy).expect("relocation error");
                }
                RelocationCommand::Cancel(callback) => {
                    callback.send(()).expect("failed to send ");
                }
                RelocationCommand::StartBlocking(strategy, cb) => {
                    self.relocation_run(strategy).unwrap();
                    cb.send(()).unwrap()
                }
                #[cfg(feature = "test_methods")]
                RelocationCommand::ObserveWal { deadline, reply } => {
                    let result = self.observe_wal_relocation(deadline);
                    let _ = reply.send(result);
                }
            }
        }
    }

    fn save_progress(
        &mut self,
        db: &Db,
        watermarks: &RelocationWatermarks,
        watermark_only: bool,
    ) -> DbResult<()> {
        watermarks.save()?;
        if watermark_only {
            return Ok(());
        }

        let gc_watermark = std::cmp::min(
            watermarks.gc_watermark(),
            db.control_region_store.lock().last_position(),
        );

        db.wal_writer.gc(gc_watermark)?;
        Ok(())
    }

    fn relocation_run(&mut self, strategy: RelocationStrategy) -> DbResult<()> {
        let Some(db) = self.db.upgrade() else {
            return Ok(());
        };
        match strategy {
            RelocationStrategy::WalBased => self.wal_based_relocation(db),
            RelocationStrategy::IndexBased(target_position) => {
                self.index_based_relocation(db, target_position)
            }
        }
    }

    #[cfg(feature = "test_methods")]
    fn observe_wal_relocation(
        &mut self,
        deadline: std::time::Instant,
    ) -> Result<RelocationObservation, RelocationTestError> {
        if std::time::Instant::now() >= deadline {
            return Err(RelocationTestError::DeadlineExceeded);
        }
        let db = self
            .db
            .upgrade()
            .ok_or(RelocationTestError::WorkerStopped)?;
        let mut observed = RelocationObservation {
            gate: RelocationGate::NoWork,
            streams: Vec::new(),
            callbacks: Vec::new(),
            copied_survivors: 0,
            witness: RelocationWorkerWitness::Completed,
        };
        if let Some(target_position) = self
            .relocate_wal_prefix_with(&db, Some(&mut observed))
            .map_err(RelocationTestError::Native)?
        {
            Self::reclaim_wal_prefix(&db, target_position).map_err(RelocationTestError::Native)?;
        }
        // Do not send a witness until the worker has completed the gate and
        // any writes, control-region rebuild, and GC.
        Ok(observed)
    }

    fn index_based_relocation(
        &mut self,
        db: Arc<Db>,
        target_position: Option<u64>,
    ) -> DbResult<()> {
        let mut watermarks = RelocationWatermarks::read_or_create(&self.path)?;
        // Capture the upper WAL limit to avoid race conditions
        // Only process entries written before this point. This is the last position that was written
        // and made its way into the large table
        let upper_limit = db.wal_writer.last_processed().as_u64();

        // Compute effective limit based on target_position
        let effective_limit =
            target_position.map_or(upper_limit, |t| std::cmp::min(t, upper_limit));

        // Restart from beginning if target_position changed or if previous run completed
        let should_restart = watermarks.data.next_to_process.is_none()
            || watermarks.data.target_position != target_position;

        // Get starting cell reference from saved progress or restart
        let mut current_cell_ref = if should_restart {
            CellReference::first(&db, KeySpace::first())
        } else {
            watermarks.data.next_to_process.clone()
        };
        // A saved watermark may point into a keyspace destroyed since it was
        // written; its cells no longer exist, so re-anchor to the next live
        // keyspace.
        if let Some(cell_ref) = &current_cell_ref
            && db.key_shape.ks(cell_ref.keyspace).destroyed()
        {
            let mut next_keyspace = cell_ref.keyspace;
            next_keyspace.increment();
            current_cell_ref = CellReference::first(&db, next_keyspace);
        }

        let mut cells_processed = 0;
        let mut highest_wal_position = 0u64;
        let mut current_ks_id = None;

        while let Some(cell_ref) = current_cell_ref.take() {
            // Check for cancellation periodically
            if cells_processed % Self::NUM_ITERATIONS_IN_BATCH == 0 {
                if self.should_cancel_relocation() {
                    break;
                }
                // Save progress periodically
                if cells_processed % Self::NUM_ITERATIONS_TILL_SAVE == 0 {
                    watermarks.set(
                        Some(cell_ref.clone()),
                        highest_wal_position,
                        upper_limit,
                        target_position,
                    );
                    self.save_progress(&db, &watermarks, true)?;
                    // Save watermark only
                }
            }

            // Update current keyspace metric when it changes
            let ks_id = cell_ref.keyspace.as_usize();
            if current_ks_id != Some(ks_id) {
                current_ks_id = Some(ks_id);
                self.metrics.relocation_current_keyspace.set(ks_id as i64);
            }

            // Process each cell
            let context = self.process_single_cell(&cell_ref, &db, effective_limit)?;

            // Track the highest WAL position seen
            if context.highest_wal_position.offset() > highest_wal_position {
                highest_wal_position = context.highest_wal_position.offset();
            }

            // Relocate entries if any were marked for keeping
            let keyspace_desc = &db.ks_context(cell_ref.keyspace).ks_config;
            if !context.batch.is_empty() {
                let successful = self.relocate_entries(context.batch, &db)?;
                // Track successful relocations with existing metrics (same as WAL-based)
                self.metrics
                    .relocation_kept
                    .with_label_values(&[keyspace_desc.name()])
                    .inc_by(successful);
            }

            // Track cells processed
            self.metrics
                .relocation_cells_processed
                .with_label_values(&[keyspace_desc.name()])
                .inc();

            cells_processed += 1;

            // Get next cell reference
            current_cell_ref = cell_ref.next(&db);
        }

        // Save final progress with upper_limit and highest WAL position
        watermarks.set(
            current_cell_ref.clone(),
            highest_wal_position,
            upper_limit,
            target_position,
        );
        self.save_progress(&db, &watermarks, false)?;
        Ok(())
    }

    fn wal_based_relocation(&mut self, db: Arc<Db>) -> DbResult<()> {
        let Some(target_position) = self.relocate_wal_prefix(&db)? else {
            return Ok(());
        };
        Self::reclaim_wal_prefix(&db, target_position)
    }

    /// Persists the index after `relocate_wal_prefix`, then reclaims the WAL
    /// files below `target_position`.
    fn reclaim_wal_prefix(db: &Db, target_position: u64) -> DbResult<()> {
        db.rebuild_control_region_from(target_position)?;
        db.wal_writer.gc(std::cmp::min(
            target_position,
            db.control_region_store.lock().last_position(),
        ))?;
        Ok(())
    }

    /// Scans the WAL for the relocation terminal, then rewrites every live
    /// entry below the whole-file target at the WAL head and re-points the
    /// index to the copy. Returns the target, or `None` when no whole file
    /// can be reclaimed. The caller persists the index, then reclaims the
    /// files below the target.
    fn relocate_wal_prefix(&self, db: &Arc<Db>) -> DbResult<Option<u64>> {
        self.relocate_wal_prefix_with(
            db,
            #[cfg(feature = "test_methods")]
            None,
        )
    }

    /// [`Self::relocate_wal_prefix`], reporting to `observed` in tests.
    fn relocate_wal_prefix_with(
        &self,
        db: &Arc<Db>,
        #[cfg(feature = "test_methods")] mut observed: Option<&mut RelocationObservation>,
    ) -> DbResult<Option<u64>> {
        // todo: support compressed-batch frames in the terminal_position scan
        // below. Today it only matches `WalEntry::Record(..)`, so records
        // buried inside a `CompressedBatch` never see the relocation filter
        // (e.g. `Decision::StopRelocation` is silently skipped). Until that
        // path decompresses inner entries, refuse to run with the codec on.
        assert!(
            db.config.batch_codec.is_none(),
            "wal-based relocation does not support Config::batch_codec yet"
        );
        let last_processed = db.wal_writer.last_processed();
        let upper_limit = last_processed.as_u64();
        let min_wal_position = db.wal.min_wal_position();
        let mut wal_iterator = db.wal.wal_iterator_for_scan(min_wal_position)?;

        // Calculate the maximum amount we can reclaim based on configured percentage
        let max_target_position = min_wal_position
            + (upper_limit.saturating_sub(min_wal_position)
                * db.config.relocation_max_reclaim_pct as u64
                / 100)
                .max(db.wal.wal_file_size());
        // find target cut-off position
        let mut terminal_position = 0;
        // Per keyspace: its filter returned `Keep` for a record below the terminal.
        let mut keep_seen = vec![false; db.key_shape.num_ks()];
        // WAL offsets of those records below the reclaim cap, ascending. The
        // copy loop selects survivors from them, so each record gets exactly
        // one filter call. Bounded by the records in the reclaim span.
        let mut kept_positions = Vec::new();
        loop {
            let entry = wal_iterator.next();
            if let Err(error @ (WalError::Crc(_) | WalError::EndOfWal)) = &entry {
                // The scan read every frame below the processed frontier, and
                // nothing is written at the frontier: the processed prefix is
                // complete. A stop below the frontier, or a torn or corrupt
                // frame at it, leaves the terminal at zero.
                if wal_iterator.position() == upper_limit
                    && (matches!(error, WalError::EndOfWal) || wal_iterator.is_zeroed_header())
                {
                    terminal_position = upper_limit;
                }
                break;
            }
            let (position, raw_entry) = entry?;
            if position.offset() >= upper_limit {
                terminal_position = position.offset();
                break;
            }
            let entry = WalEntry::from_bytes(raw_entry);
            assert!(
                !matches!(entry, WalEntry::CompressedBatch(..)),
                "wal-based relocation reached a CompressedBatch frame at {} but does not yet \
                 understand its inner records (todo)",
                position.offset()
            );
            if let WalEntry::Record(ks, key, value, _relocated) = entry
                && let Some(filter) = db.key_shape.ks(ks).relocation_filter()
            {
                let decision = filter(&key, &value);
                #[cfg(feature = "test_methods")]
                if let Some(observed) = &mut observed {
                    observed.callbacks.push(RelocationCallback {
                        keyspace: ks,
                        // Do not keep a WAL mapping pinned across reclamation.
                        key: Bytes::copy_from_slice(&key),
                        value: Bytes::copy_from_slice(&value),
                        wal_position: position.offset(),
                        decision,
                    });
                }
                match decision {
                    Decision::Keep => {
                        keep_seen[ks.as_usize()] = true;
                        if position.offset() < max_target_position {
                            kept_positions.push(position.offset());
                        }
                    }
                    Decision::Remove => {}
                    Decision::StopRelocation => {
                        terminal_position = position.offset();
                        break;
                    }
                }
            }
        }
        let mut target_position = terminal_position.min(max_target_position);
        target_position -= target_position % db.wal.wal_file_size();
        self.metrics
            .relocation_target_position
            .set(target_position as i64);
        self.metrics
            .relocation_terminal_position
            .set(terminal_position as i64);
        #[cfg(feature = "test_methods")]
        if let Some(observed) = &mut observed {
            observed.streams.push(StreamObservation {
                stream_id: RelocationStreamId {
                    partition: 0,
                    fencing_generation: 0,
                },
                processed_upper: upper_limit,
                terminal_position,
                rounded_target: target_position,
            });
        }
        // ensure the target position is big enough to cut
        if target_position < (db.wal.wal_file_size() + min_wal_position) {
            return Ok(None);
        }
        #[cfg(feature = "test_methods")]
        if let Some(observed) = &mut observed {
            observed.gate = RelocationGate::DidWork;
        }
        let mut current_cell = CellReference::first(db, KeySpace::first());
        while let Some(cell) = current_cell.take() {
            current_cell = cell.next(db);
            let ks = db.key_shape.ks(cell.keyspace);
            let filter = ks.relocation_filter();
            // Every record of this keyspace below the terminal scanned as
            // `Remove`: drop its index entries below the terminal unread.
            if filter.is_some() && !keep_seen[cell.keyspace.as_usize()] {
                db.large_table.sync_flush_for_relocation(
                    db.ks_context(cell.keyspace),
                    &cell.cell_id,
                    db.as_ref(),
                    None,
                    Some(terminal_position),
                )?;
                continue;
            }
            let batch_limit = db.config.relocation_batch_max_bytes();
            let mut batch =
                RelocatedWriteBatch::new(cell.keyspace, cell.cell_id.clone(), target_position);
            // Relocate the value as of the checkpoint frontier, not the live
            // latest: a post-frontier overwrite would otherwise strand the
            // as-of value (which GC reclaims) that a held checkpoint reads.
            // `get_index_for_cell` returns that as-of view directly — the overlay
            // is filtered to < `last_processed` before any latest-per-key collapse,
            // and the on-disk levels it merges under are already as-of-safe.
            let index = db
                .large_table
                .get_index_for_cell(
                    db.ks_context(cell.keyspace),
                    &cell.cell_id,
                    db.as_ref(),
                    last_processed,
                )?
                .unwrap_or(IndexTable::default().into());
            let context = db.ks_context(cell.keyspace);
            for (reduced_key, position) in index.iter() {
                // In a filtered keyspace, an entry is live only if the scan
                // kept its record.
                if position.offset() < target_position
                    && (filter.is_none()
                        || kept_positions.binary_search(&position.offset()).is_ok())
                    && let Some((key, value)) =
                        db.read_record_for_indexed_key(context, position, reduced_key.as_ref())?
                {
                    batch.write(key, value);
                    self.metrics
                        .relocation_kept
                        .with_label_values(&[ks.name()])
                        .inc();
                    if batch.size_bytes() >= batch_limit {
                        #[cfg(feature = "test_methods")]
                        let copied = batch.len() as u64;
                        db.write_relocated_batch(batch)?;
                        #[cfg(feature = "test_methods")]
                        if let Some(observed) = &mut observed {
                            observed.copied_survivors += copied;
                        }
                        batch = RelocatedWriteBatch::new(
                            cell.keyspace,
                            cell.cell_id.clone(),
                            target_position,
                        );
                    }
                }
            }
            if !batch.is_empty() {
                #[cfg(feature = "test_methods")]
                let copied = batch.len() as u64;
                db.write_relocated_batch(batch)?;
                #[cfg(feature = "test_methods")]
                if let Some(observed) = &mut observed {
                    observed.copied_survivors += copied;
                }
            }
            if filter.is_some() {
                // Kept entries now point to their copies at the WAL head.
                // Drop the removed entries below the target. Entries from the
                // target to the terminal stay: their files are not reclaimed.
                db.large_table.sync_flush_for_relocation(
                    context,
                    &cell.cell_id,
                    db.as_ref(),
                    None,
                    Some(target_position),
                )?;
            }
        }
        Ok(Some(target_position))
    }

    fn should_cancel_relocation(&self) -> bool {
        loop {
            match self.receiver.try_recv() {
                // consume and ignore all Start commands while relocation is in progress
                Ok(RelocationCommand::Start(_)) => {}
                Ok(RelocationCommand::Cancel(cb)) => {
                    cb.send(())
                        .expect("Failed to send cancel relocation command");
                    return true;
                }
                Err(mpsc::TryRecvError::Empty) => return false,
                Err(mpsc::TryRecvError::Disconnected) => return true,
                Ok(RelocationCommand::StartBlocking(_, cb)) => cb.send(()).unwrap(),
                #[cfg(feature = "test_methods")]
                Ok(RelocationCommand::ObserveWal { reply, .. }) => {
                    let _ = reply.send(Err(RelocationTestError::Busy));
                }
            }
        }
    }

    /// Process a single cell: collect keys, read values from WAL, make decisions
    fn process_single_cell(
        &self,
        cell_ref: &CellReference,
        db: &Arc<Db>,
        effective_limit: u64,
    ) -> DbResult<CellProcessingContext> {
        // The CAS threshold must be the same frontier the as-of index view
        // below is built with. Any write newer than that view sits at an
        // offset >= `effective_limit`; a higher threshold (e.g. the live
        // frontier) would let the CAS re-point such a write to the relocated
        // copy of the older as-of value, losing the overwrite. (The WAL-based
        // path passes its `target_position` for the same reason.)
        let batch =
            RelocatedWriteBatch::new(cell_ref.keyspace, cell_ref.cell_id.clone(), effective_limit);
        let mut context = CellProcessingContext::new(batch);
        let mut removed_count = 0;

        // Phase A: Get shared reference to cell index, as of the relocation
        // frontier (filters the overlay before any latest-per-key collapse).
        let index = match db.large_table.get_index_for_cell(
            db.ks_context(cell_ref.keyspace),
            &cell_ref.cell_id,
            db.as_ref(),
            LastProcessed::new(effective_limit),
        )? {
            Some(index) => index,
            None => {
                // Cell doesn't exist or is empty
                return Ok(context);
            }
        };

        // Phase B: Read values from WAL and make decisions (no lock held, efficient iteration)
        // TODO(#74): Optimization needed - add support for making relocation decisions without loading values
        // This would be beneficial in two scenarios:
        // 1. Applications without pruner callbacks - relocation just moves non-deleted entries
        // 2. Applications that can make decisions based on key only, without needing the value
        //
        // This strategy is most useful for applications that don't need values for decision making.
        // For applications like Sui that require values (similar to current behavior),
        // WAL-based relocation remains the better choice.
        //
        // Implementation could involve:
        // - Optional key-only decision callback in RelocationFilter trait
        // - Skip WAL value reads when key-only decisions are possible
        // - Fall back to current value-based approach when needed
        let ks_context = db.ks_context(cell_ref.keyspace);
        let keyspace_desc = &ks_context.ks_config;
        // Relocate the value as of the checkpoint frontier, not the live latest
        // (see the WAL-based path for why). `get_index_for_cell` already returns
        // the as-of view; the loop below skips any on-disk entry still at >= the
        // cutoff.
        for (key, position) in index.iter() {
            if position.offset() >= effective_limit {
                continue;
            }

            // Read the actual value from WAL
            let value = match db.read_record_for_indexed_key(ks_context, position, key.as_ref())? {
                Some((_, val)) => val,
                None => {
                    // Entry might have been deleted or corrupted, skip it
                    context.mark_entry_removed(position);
                    removed_count += 1;
                    continue;
                }
            };

            // Simplified decision logic for index-based relocation. Since we're iterating through
            // current index entries, we only need to check the relocation filter
            let decision = keyspace_desc
                .relocation_filter()
                .map_or(Decision::Keep, |filter| filter(&key, &value));

            match decision {
                Decision::Keep => {
                    context.add_entry_to_relocate(key.clone(), value, position);
                }
                Decision::Remove => {
                    context.mark_entry_removed(position);
                    removed_count += 1;
                }
                Decision::StopRelocation => {
                    break;
                }
            }
        }

        // Track removed entries with existing metrics (same as WAL-based)
        if removed_count > 0 {
            self.metrics
                .relocation_removed
                .with_label_values(&[keyspace_desc.name()])
                .inc_by(removed_count);
        }

        Ok(context)
    }

    /// Relocate entries following the same pattern as WAL-based relocation
    fn relocate_entries(&self, batch: RelocatedWriteBatch, db: &Arc<Db>) -> DbResult<u64> {
        let successful_inserts = batch.len() as u64;

        db.write_relocated_batch(batch)?;

        Ok(successful_inserts)
    }
}

#[cfg(all(test, feature = "test_methods"))]
mod observed_tests {
    use super::*;
    use crate::config::Config;
    use crate::key_shape::{KeyShape, KeyType};
    use std::time::{Duration, Instant};

    #[test]
    fn observed_wal_busy_does_not_return_completion() {
        let (sender, receiver) = mpsc::channel();
        let (reply, result) = mpsc::sync_channel(1);
        sender
            .send(RelocationCommand::ObserveWal {
                deadline: Instant::now() + Duration::from_secs(10),
                reply,
            })
            .unwrap();
        let driver = RelocationDriver {
            db: Weak::new(),
            path: PathBuf::new(),
            receiver,
            metrics: Metrics::new(),
        };
        assert!(!driver.should_cancel_relocation());
        assert!(matches!(
            result.try_recv(),
            Ok(Err(RelocationTestError::Busy))
        ));
    }

    #[test]
    fn observed_wal_dropped_reply_keeps_worker_alive() {
        let directory = tempdir::TempDir::new("observe_wal_relocation").unwrap();
        let db = Db::open(
            directory.path(),
            KeyShape::new_single(8, 1, KeyType::uniform(1)),
            Arc::new(Config::small()),
            Metrics::new(),
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel();
        let (abandoned, abandoned_result) = mpsc::sync_channel(1);
        let (reply, result) = mpsc::sync_channel(1);
        let deadline = Instant::now() + Duration::from_secs(10);
        drop(abandoned_result);
        sender
            .send(RelocationCommand::ObserveWal {
                deadline,
                reply: abandoned,
            })
            .unwrap();
        sender
            .send(RelocationCommand::ObserveWal { deadline, reply })
            .unwrap();
        drop(sender);
        RelocationDriver {
            db: Arc::downgrade(&db),
            path: directory.path().to_path_buf(),
            receiver,
            metrics: Metrics::new(),
        }
        .run();
        let observation = result.try_recv().unwrap().unwrap();
        assert_eq!(observation.witness, RelocationWorkerWitness::Completed);
        assert_eq!(observation.gate, RelocationGate::NoWork);
        assert_eq!(
            observation.streams,
            [StreamObservation {
                stream_id: RelocationStreamId {
                    partition: 0,
                    fencing_generation: 0,
                },
                processed_upper: db.wal_writer.last_processed().as_u64(),
                terminal_position: 0,
                rounded_target: 0,
            }],
        );
        assert!(observation.callbacks.is_empty());
        assert_eq!(observation.copied_survivors, 0);
    }
}
