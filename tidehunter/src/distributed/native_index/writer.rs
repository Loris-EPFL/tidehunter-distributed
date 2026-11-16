//! Publication and maintenance handle of the sharded native index.
//!
//! Readers never take the `writer` mutex. It serializes the callers that change
//! what the index holds or how it is laid out: publication (`promote_committed`,
//! live views) and checkpoint capture/installation. In the distributed design
//! one owner already serializes those calls (the paper's single background
//! tracker); the mutex makes the contract explicit and keeps `generation`,
//! `touched`, `latest` and the capture/installation cursors consistent.
//!
//! The writer applies entries to shards one shard at a time, in ascending shard
//! index, holding at most one shard lock. Applying never changes what readers
//! see: only [`IndexWriter::publish_latest`],
//! [`IndexWriter::install_reserved_live_view`] and
//! [`IndexWriter::install_standing_live_view`] move the visible cut (see the
//! module documentation of `shards`).

use super::shards::{CellKey, add, sub};
use super::snapshot::FrozenCell;
use super::*;
use crate::distributed::native_publication::NativePendingSink;
use parking_lot::MutexGuard;
use std::ops::Deref;
use std::sync::Arc;

pub(super) struct ImageInstallation {
    cells: std::vec::IntoIter<FrozenCell>,
    cut: IndexReadCut,
    lease: CaptureLease,
}

pub(super) struct ImageCapture {
    cursor: usize,
    end: usize,
    cells: Vec<FrozenCell>,
    cut: IndexReadCut,
    lease: CaptureLease,
    schema: [u8; 32],
}

/// State only the writer touches. Held under `NativeIndex::writer`.
#[derive(Default)]
pub(super) struct WriterState {
    /// Cells with staged pending entries, awaiting `promote_committed`.
    pub touched: HashSet<CellKey>,
    /// Append-only cell directory: capture and cleanup walk it in order.
    pub cell_order: Vec<CellKey>,
    pub generation: u64,
    pub capture: Option<ImageCapture>,
    pub sealed_count: usize,
    pub cleanup_cursor: usize,
    /// Newest promoted version. Not what readers see; see `published`.
    pub latest: Option<MutationVersion>,
    pub installation: Option<ImageInstallation>,
    /// Reused by `promote_committed` so a promotion allocates nothing.
    scratch: Vec<(usize, CellKey)>,
}

/// Exclusive publication/maintenance access. Dereferences to the index so every
/// reader method stays available. Never block on it while holding a shard lock.
pub struct IndexWriter<'a> {
    index: &'a NativeIndex,
    state: MutexGuard<'a, WriterState>,
}

impl Deref for IndexWriter<'_> {
    type Target = NativeIndex;
    fn deref(&self) -> &NativeIndex {
        self.index
    }
}

impl NativeIndex {
    /// Take the publication/maintenance handle. Blocks only other writers.
    pub fn writer(&self) -> IndexWriter<'_> {
        IndexWriter {
            index: self,
            state: self.writer.lock(),
        }
    }
}

impl NativePendingSink for IndexWriter<'_> {
    type Error = NativeIndexError;
    fn with_pending_table(
        &mut self,
        keyspace: KeySpace,
        cell: &CellId,
        apply: impl FnOnce(&mut PendingTable<ValueRecordReference>),
    ) -> Result<(), Self::Error> {
        let index = self.index;
        let descriptor = index.descriptor(keyspace)?;
        match (descriptor.key_type(), cell) {
            (KeyType::Uniform(config), CellId::Integer(id))
                if config
                    .cells_per_mutex()
                    .checked_mul(descriptor.num_mutexes())
                    .and_then(|count| u32::try_from(count).ok())
                    .is_some_and(|count| (*id as u128) < u128::from(count)) => {}
            (KeyType::PrefixedUniform(_), CellId::Bytes(prefix))
                if matches!(descriptor.first_cell(), CellId::Bytes(first) if first.len() == prefix.len())
                    && descriptor.cell_id(prefix) == *cell => {}
            _ => {
                return Err(NativeIndexError::Invalid(
                    "cell does not belong to keyspace",
                ));
            }
        }
        let identity = (keyspace, cell.clone());
        let state = &mut *self.state;
        // One shard lock for the whole staged entry; no other shard is touched.
        let mut shard = index.shard_for(&identity).write();
        if !shard.contains_key(&identity) {
            index.check_capacity(cell_directory_charge(&identity.1))?;
            add(
                &index.accounting.directory,
                cell_directory_charge(&identity.1),
            );
            state.cell_order.push(identity.clone());
            shard.insert(
                identity.clone(),
                CellIndex {
                    generation: state.generation,
                    ..Default::default()
                },
            );
        }
        state.touched.insert(identity.clone());
        apply(&mut shard.entry(identity).or_default().pending);
        Ok(())
    }
}

impl IndexWriter<'_> {
    /// Promote committed native pending entries while retaining every logical
    /// version. Invoke inside the authority's atomic publication callback. Errors
    /// can follow partial promotion and therefore require authority recovery.
    ///
    /// This applies entries to shards only. It does not change what `Latest`
    /// readers see; call [`Self::publish_latest`] or
    /// [`Self::install_reserved_live_view`] once the round's batches are all
    /// applied.
    ///
    /// Touched cells are applied grouped by shard in ascending shard index, one
    /// shard lock at a time, so a publication blocks only the shards it touches
    /// and only while it applies that shard's cells.
    pub fn promote_committed(&mut self) -> Result<usize, NativeIndexError> {
        let index = self.index;
        let state = &mut *self.state;
        let mut order = std::mem::take(&mut state.scratch);
        order.clear();
        order.extend(
            state
                .touched
                .drain()
                .map(|identity| (index.shard_index(identity.0, &identity.1), identity)),
        );
        order.sort_unstable_by_key(|(shard, _)| *shard);
        let mut promoted = 0;
        let mut resident = index.resident_bytes();
        let mut shards_touched = 0;
        let mut at = 0;
        while at < order.len() {
            let shard_id = order[at].0;
            let run = order[at..]
                .iter()
                .take_while(|(shard, _)| *shard == shard_id)
                .count();
            let mut shard = index.shards[shard_id].write();
            shards_touched += 1;
            for (_, identity) in &order[at..at + run] {
                let cell = shard.get_mut(identity).expect("touched cell exists");
                let (changes, _) = cell.pending.take_committed();
                if cell.pending.len() != 0 {
                    state.touched.insert(identity.clone());
                }
                for change in changes {
                    let reference = change.value;
                    if reference.batch_id.database != index.database
                        || reference.descriptor.keyspace != identity.0.0
                        || change.is_modified != (reference.descriptor.kind == RecordKind::Put)
                    {
                        return Err(NativeIndexError::Invalid(
                            "pending reference domain or operation kind",
                        ));
                    }
                    if let Some(existing) = cell.exact(&change.key, reference.mutation_version()) {
                        if existing != reference {
                            return Err(NativeIndexError::ConflictingVersion);
                        }
                        continue;
                    }
                    let charge = VERSION_CHARGE + change.key.len();
                    index
                        .accounting
                        .retained_bytes()
                        .checked_add(charge)
                        .ok_or(NativeIndexError::Capacity)?;
                    resident = resident
                        .checked_add(charge)
                        .ok_or(NativeIndexError::Capacity)?;
                    if resident > index.max_metadata_bytes {
                        return Err(NativeIndexError::Capacity);
                    }
                    if state.capture.is_some() {
                        if cell.rotate(state.generation) {
                            state.sealed_count += 1;
                        }
                    } else {
                        cell.generation = state.generation;
                    }
                    let key = change.key.into_owned();
                    let values = cell.overlay.entry(key).or_default();
                    if let Some(old) = insert_unique_by(values, reference, |a, b| {
                        a.mutation_version().cmp(&b.mutation_version())
                    }) && old != reference
                    {
                        return Err(NativeIndexError::ConflictingVersion);
                    }
                    add(&index.accounting.retained_bytes, charge);
                    add(&index.accounting.overlay, charge);
                    add(&index.accounting.retained_versions, 1);
                    state.latest = Some(state.latest.map_or(reference.mutation_version(), |v| {
                        v.max(reference.mutation_version())
                    }));
                    promoted += 1;
                }
            }
            drop(shard);
            at += run;
        }
        index.publication_shards.record(shards_touched);
        order.clear();
        state.scratch = order;
        Ok(promoted)
    }

    /// Make everything promoted so far visible to `Latest` readers. Valid when
    /// no unpublished version is lower than a promoted one. Otherwise install a
    /// sparse view instead.
    ///
    /// This is the publication watermark: one atomic store, issued after every
    /// shard apply, that readers acquire before looking at any shard.
    pub fn publish_latest(&mut self) {
        self.index.published.store(Arc::new(
            self.state
                .latest
                .map_or(IndexReadCut::BeforeAll, IndexReadCut::At),
        ));
    }

    /// Install only after the entire promoted batch is available. An empty
    /// exclusion list restores the cheap prefix.
    pub fn install_live_view(
        &mut self,
        upper: MutationVersion,
        stable_floor: IndexReadCut,
        excluded: Vec<BatchVersion>,
    ) -> Result<(), NativeIndexError> {
        let max_excluded = if excluded.is_empty() {
            0
        } else {
            excluded.capacity()
        };
        let reservation = self.index.reserve_publication_view(max_excluded)?;
        self.install_reserved_live_view(upper, stable_floor, excluded, reservation)
    }

    pub(crate) fn install_reserved_live_view(
        &mut self,
        upper: MutationVersion,
        stable_floor: IndexReadCut,
        excluded: Vec<BatchVersion>,
        mut reservation: PublicationViewReservation,
    ) -> Result<(), NativeIndexError> {
        let index = self.index;
        if !reservation.belongs_to(&index.retention) {
            return Err(NativeIndexError::Invalid(
                "foreign publication view reservation",
            ));
        }
        let charge = if excluded.is_empty() {
            0
        } else {
            if excluded.capacity() > reservation.max_excluded() {
                return Err(NativeIndexError::Invalid(
                    "publication view exceeds reservation",
                ));
            }
            view_charge(excluded.capacity())?
        };
        if charge > reservation.charge() {
            return Err(NativeIndexError::Invalid(
                "publication view exceeds reservation",
            ));
        }
        self.install_view(
            upper,
            stable_floor,
            excluded,
            charge,
            0,
            Some(&mut reservation),
        )
    }

    /// Install a live view under the publisher's standing budget. No view
    /// quota is taken here, so after validation (programming errors only) the
    /// install cannot fail on view capacity once its batches are durable.
    pub(crate) fn install_standing_live_view(
        &mut self,
        upper: MutationVersion,
        stable_floor: IndexReadCut,
        excluded: Vec<BatchVersion>,
        budget: &StandingViewBudget,
    ) -> Result<(), NativeIndexError> {
        if !budget.belongs_to(&self.index.retention) {
            return Err(NativeIndexError::Invalid("foreign standing view budget"));
        }
        if !excluded.is_empty() && excluded.capacity() > budget.max_excluded() {
            return Err(NativeIndexError::Invalid(
                "publication view exceeds standing budget",
            ));
        }
        // The budget owns this view's bytes at the full worst-case size. The
        // actual size is reported while the view is live (reader-held metric).
        let uncharged = if excluded.is_empty() {
            0
        } else {
            view_charge(excluded.capacity())?
        };
        self.install_view(upper, stable_floor, excluded, 0, uncharged, None)
    }

    fn install_view(
        &mut self,
        upper: MutationVersion,
        stable_floor: IndexReadCut,
        excluded: Vec<BatchVersion>,
        charge: usize,
        uncharged: usize,
        reservation: Option<&mut PublicationViewReservation>,
    ) -> Result<(), NativeIndexError> {
        let index = self.index;
        let latest = self.state.latest;
        if matches!(stable_floor, IndexReadCut::Latest | IndexReadCut::Sparse(_))
            || position(&stable_floor).is_some_and(|floor| floor > upper)
            || latest.is_none_or(|latest| upper > latest)
            || excluded.windows(2).any(|pair| pair[0] >= pair[1])
            || excluded.iter().any(|batch| {
                *batch > upper.batch
                    || position(&stable_floor).is_some_and(|floor| *batch <= floor.batch)
            })
        {
            return Err(NativeIndexError::Invalid("invalid sparse publication view"));
        }
        index.register_live_view(
            &stable_floor,
            charge,
            uncharged,
            reservation,
            !excluded.is_empty(),
        )?;
        let cut = if excluded.is_empty() {
            // The view covers exactly `upper`: a newer promoted-but-unpublished
            // batch (version above `upper`) must stay invisible.
            IndexReadCut::At(upper)
        } else {
            IndexReadCut::Sparse(Arc::new(SparseIndexView {
                upper,
                floor: position(&stable_floor),
                excluded,
                retention: Arc::clone(&index.retention),
                charge,
                uncharged,
            }))
        };
        // The watermark store: all entries of the round are already applied.
        index.published.store(Arc::new(cut));
        Ok(())
    }

    /// Write a new immutable, metadata-only image, sync it and its directory,
    /// then unload eligible history into file-backed compact blocks. Existing
    /// files are never overwritten; a root manifest may name the image only
    /// after success. Post-cut versions remain in the live overlay.
    pub fn flush_and_unload(
        &mut self,
        path: &Path,
        cut: IndexReadCut,
    ) -> Result<IndexImageMetadata, NativeIndexError> {
        let image = self.prepare_image(cut)?.persist(path)?;
        let metadata = image.metadata;
        self.install_image(image);
        Ok(metadata)
    }

    pub(crate) fn prepare_image(
        &mut self,
        cut: IndexReadCut,
    ) -> Result<PreparedIndexImage, NativeIndexError> {
        let floor = self.index.retention_floor();
        self.prepare_compacted_image(cut, floor)
    }

    /// Non-serving wrapper. Serving owners use bounded capture steps.
    pub(crate) fn prepare_compacted_image(
        &mut self,
        cut: IndexReadCut,
        floor: IndexReadCut,
    ) -> Result<PreparedIndexImage, NativeIndexError> {
        self.cleanup_failed_capture_step(usize::MAX)?;
        self.begin_compacted_capture(cut, floor)?;
        Ok(self.capture_step(usize::MAX).expect("capture drained"))
    }

    pub(crate) fn begin_capture(&mut self, cut: IndexReadCut) -> Result<(), NativeIndexError> {
        let floor = self.index.retention_floor();
        self.begin_compacted_capture(cut, floor)
    }

    /// Freeze the publication generation and the cell-directory boundary. The
    /// writer mutex serializes this with every promotion, so the boundary is one
    /// exact point in the promotion sequence for all shards. Even a late older
    /// version goes into the new generation and cannot enter this image. No
    /// reader is affected: begin touches no shard.
    pub(crate) fn begin_compacted_capture(
        &mut self,
        cut: IndexReadCut,
        floor: IndexReadCut,
    ) -> Result<(), NativeIndexError> {
        let index = self.index;
        let state = &mut *self.state;
        if state.sealed_count != 0 {
            return Err(NativeIndexError::CheckpointInProgress);
        }
        for identity in &state.touched {
            if index.shard_for(identity).read()[identity].pending.len() != 0 {
                return Err(NativeIndexError::PendingPublication);
            }
        }
        let cut = index.resolve_cut(cut);
        // An image must never freeze a batch that readers cannot see yet.
        index.check_published(&cut)?;
        let schema = schema_digest(&index.shape)?;
        let generation = state
            .generation
            .checked_add(1)
            .ok_or(NativeIndexError::Capacity)?;
        let lease = index.capture_lease(&cut, floor, state.latest)?;
        state.generation = generation;
        state.capture = Some(ImageCapture {
            cursor: 0,
            end: state.cell_order.len(),
            cells: Vec::with_capacity(state.cell_order.len()),
            cut,
            lease,
            schema,
        });
        Ok(())
    }

    /// Each step freezes at most `max_cells` directory entries, without copying
    /// their reference histories. Persistence filters the immutable cut later.
    /// Each cell is frozen under its own shard lock; no two are held together.
    pub(crate) fn capture_step(&mut self, max_cells: usize) -> Option<PreparedIndexImage> {
        assert!(max_cells != 0, "capture step must make progress");
        let index = self.index;
        let state = &mut *self.state;
        let mut capture = state.capture.take().expect("capture is active");
        let end = capture.cursor.saturating_add(max_cells).min(capture.end);
        for identity in &state.cell_order[capture.cursor..end] {
            let mut shard = index.shard_for(identity).write();
            let cell = shard.get_mut(identity).expect("directory cell exists");
            if cell.rotate(state.generation) {
                state.sealed_count += 1;
            }
            capture.cells.push(FrozenCell {
                identity: identity.clone(),
                base: cell.base.clone(),
                overlay: Arc::clone(cell.sealed.as_ref().expect("sealed generation")),
            });
        }
        capture.cursor = end;
        if end != capture.end {
            state.capture = Some(capture);
            return None;
        }
        Some(PreparedIndexImage::new(
            index.database,
            capture.schema,
            capture.cells,
            capture.cut,
            capture.lease,
        ))
    }

    /// Restore abandoned generations before another barrier. No history is
    /// discarded; this is also safe when a prepared image was dropped.
    pub(crate) fn cleanup_failed_capture_step(
        &mut self,
        max_cells: usize,
    ) -> Result<bool, NativeIndexError> {
        assert!(max_cells != 0, "cleanup step must make progress");
        let index = self.index;
        let state = &mut *self.state;
        if index.retention.capture_active() {
            return Err(NativeIndexError::CheckpointInProgress);
        }
        if state.sealed_count == 0 {
            state.cleanup_cursor = 0;
            return Ok(true);
        }
        let end = state
            .cleanup_cursor
            .saturating_add(max_cells)
            .min(state.cell_order.len());
        for identity in &state.cell_order[state.cleanup_cursor..end] {
            let mut shard = index.shard_for(identity).write();
            let cell = shard.get_mut(identity).expect("directory cell exists");
            if let Some(sealed) = cell.sealed.take() {
                let sealed =
                    Arc::try_unwrap(sealed).unwrap_or_else(|_| unreachable!("capture lease ended"));
                cell.merge_overlay(sealed);
                state.sealed_count -= 1;
            }
        }
        state.cleanup_cursor = end;
        if state.sealed_count == 0 {
            state.cleanup_cursor = 0;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Abort drops the already-collected cell handles in one pass. The serving
    /// owner uses this on cancellation or fencing, not on healthy progress.
    pub(crate) fn abort_capture(&mut self) {
        self.state.capture = None;
    }

    /// Start installing an already durable root. The index owns the lease until
    /// all cells are installed, even if the caller pauses between steps.
    pub(crate) fn begin_install_image(&mut self, persisted: PersistedIndexImage) {
        assert!(
            self.state.installation.is_none(),
            "index installation already active"
        );
        self.state.installation = Some(ImageInstallation {
            cells: persisted.cells.into_iter(),
            cut: persisted.cut,
            lease: persisted.lease,
        });
    }

    /// Install at most `max_cells` captured cells, each under its own shard
    /// lock. Until completion the lease rejects older new read cuts and
    /// preserves every existing pin. The owner must not release checkpoint
    /// credits or authorize cleanup before `true`.
    pub(crate) fn install_image_step(&mut self, max_cells: usize) -> bool {
        assert!(max_cells != 0, "installation step must make progress");
        let index = self.index;
        let state = &mut *self.state;
        let accounting = &index.accounting;
        let mut installation = state.installation.take().expect("installation is active");
        for captured in installation.cells.by_ref().take(max_cells) {
            let mut shard = index.shard_for(&captured.identity).write();
            let cell = shard
                .get_mut(&captured.identity)
                .expect("captured cell exists");
            let newer_base = cell
                .base
                .as_ref()
                .is_some_and(|base| position(&base.cut) > position(&installation.cut));
            let sealed = cell.sealed.take().expect("captured generation exists");
            drop(captured.overlay);
            let mut sealed = Arc::try_unwrap(sealed)
                .unwrap_or_else(|_| unreachable!("installation owns captured generation"));
            state.sealed_count -= 1;
            if newer_base {
                cell.merge_overlay(sealed);
                continue;
            }
            if let Some(base) = &cell.base {
                sub(&accounting.retained_bytes, base.retained_bytes);
                sub(&accounting.retained_versions, base.retained_versions);
                if !base.mapped {
                    sub(&accounting.loaded, base.bytes.len());
                }
            }
            // Unmerge only references in the captured cut. Later publications
            // live in the active generation, even when their logical versions
            // are older than the cut.
            sealed.retain(|key, values| {
                values.retain(|reference| {
                    if installation.cut.contains(reference) {
                        sub(&accounting.overlay, VERSION_CHARGE + key.len());
                        sub(&accounting.retained_bytes, VERSION_CHARGE + key.len());
                        sub(&accounting.retained_versions, 1);
                        false
                    } else {
                        true
                    }
                });
                !values.is_empty()
            });
            cell.merge_overlay(sealed);
            if let Some(base) = &captured.base {
                add(&accounting.retained_bytes, base.retained_bytes);
                add(&accounting.retained_versions, base.retained_versions);
                if !base.mapped {
                    add(&accounting.loaded, base.bytes.len());
                }
            }
            cell.base = captured.base;
        }
        if installation.cells.len() == 0 {
            installation.lease.install();
            true
        } else {
            state.installation = Some(installation);
            false
        }
    }

    /// Native LargeTable's exact-reference unmerge retains later publications.
    pub(crate) fn install_image(&mut self, persisted: PersistedIndexImage) {
        self.begin_install_image(persisted);
        self.install_image_step(usize::MAX);
    }

    /// Materialize compact cell metadata in heap buffers. User values remain in
    /// their partition WAL; this does not expand blocks into per-key heap maps.
    pub fn reload(&mut self) -> Result<(), NativeIndexError> {
        let index = self.index;
        let mut additional = 0usize;
        for shard in index.shards.iter() {
            additional += shard
                .read()
                .values()
                .filter_map(|cell| cell.base.as_ref())
                .filter(|block| block.mapped)
                .map(|block| block.bytes.len())
                .sum::<usize>();
        }
        index.check_capacity(additional)?;
        for shard in index.shards.iter() {
            for cell in shard.write().values_mut() {
                if let Some(block) = &mut cell.base
                    && block.mapped
                {
                    block.bytes = block.bytes.as_ref().to_vec().into();
                    block.mapped = false;
                }
            }
        }
        add(&index.accounting.loaded, additional);
        Ok(())
    }
}
