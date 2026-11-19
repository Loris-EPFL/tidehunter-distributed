//! Index-only suppression marks (COMPACTOR-PARITY.md §3). A mark is a
//! selection barrier, not a logical mutation: a read selects the newest
//! reference at its cut as usual and returns absence when a visible mark covers
//! that selected version. It never falls back to an older version. No user
//! sequence, WAL member, owner byte or admission credit is created.
//!
//! Visibility of one mark to one lookup:
//! - the lookup's cut contains `visible_from` (the Exact cut `C` that created
//!   the mark), so views at cuts below `C` keep the old state;
//! - the selected version is at or below `suppress_through`, so a write
//!   published after `C` wins normally;
//! - the mark's process-local install `epoch` is at or below the index's
//!   published prune epoch. A stepped installation inserts marks cell by cell
//!   with the next epoch and publishes that epoch once, after the last cell.
//!
//! Reads carry no mark revision. A view at a cut at or above `C` that was
//! opened before the installation sees the new marks after it, as a native
//! reader sees the compacted index after the flush that ran the compactor
//! (native protects no held reader from it). Views at cuts below `C` are
//! unaffected by the visibility rule above.
//!
//! Marks are stored in the frame of their cell, after the rows, so a cell's
//! marks are rewritten only when the cell is rewritten. They fold (with every
//! covered version of their key) only in a later image whose retention floor
//! contains `visible_from`: then no supported cut below `C` remains. A new
//! mark never folds in the image that adopts it.

use super::snapshot::FrozenCell;
use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// One index-only suppression interval of one reduced key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PruneMark {
    pub key: Bytes,
    /// The Exact cut that created the mark. Cuts below it ignore the mark.
    pub visible_from: MutationVersion,
    /// The exact head selected for removal at that cut. Newer versions win.
    pub suppress_through: MutationVersion,
    /// Process-local install epoch, never persisted. Marks restored at open
    /// have epoch 0, which every published epoch reaches.
    pub epoch: u64,
}

/// Bytes charged to the index directory for one resident mark: the struct,
/// its key and vector slack.
pub(super) fn mark_charge(mark: &PruneMark) -> usize {
    key_charge(mark.key.len())
}

/// Charge of one mark whose key has `key_len` bytes.
pub(super) fn key_charge(key_len: usize) -> usize {
    std::mem::size_of::<PruneMark>() + key_len + 16
}

pub(super) fn marks_charge(marks: Option<&Arc<[PruneMark]>>) -> usize {
    marks.map_or(0, |marks| marks.iter().map(mark_charge).sum())
}

/// Marks are sorted by key, then by `visible_from`; this is the first index of
/// `key`'s run.
fn first_of(marks: &[PruneMark], key: &[u8]) -> usize {
    marks.partition_point(|mark| mark.key.as_ref() < key)
}

/// True when a mark visible to this lookup suppresses the selected `version`
/// of `key`. Loads the published epoch only when a covering mark exists, so a
/// cell without marks costs one branch.
pub(super) fn suppressed(
    marks: Option<&Arc<[PruneMark]>>,
    key: &[u8],
    version: MutationVersion,
    cut: &IndexReadCut,
    published_epoch: &AtomicU64,
) -> bool {
    let Some(marks) = marks else {
        return false;
    };
    let mut published = None;
    marks[first_of(marks, key)..]
        .iter()
        .take_while(|mark| mark.key.as_ref() == key)
        .any(|mark| {
            version <= mark.suppress_through
                && cut.contains_mutation(mark.visible_from)
                // Acquire pairs with the Release store that ends an
                // installation: a mark of an unfinished installation has a
                // larger epoch and stays hidden.
                && mark.epoch
                    <= *published.get_or_insert_with(|| published_epoch.load(AtomicOrdering::Acquire))
        })
}

/// Marks the frozen image applies at its own cut `C` (all adopted marks: each
/// has `visible_from` at or below the previous cut). Used to skip heads
/// already suppressed, without the epoch check.
pub(super) fn suppressed_at_capture(
    marks: &[PruneMark],
    key: &[u8],
    version: MutationVersion,
    cut: &IndexReadCut,
) -> bool {
    marks[first_of(marks, key)..]
        .iter()
        .take_while(|mark| mark.key.as_ref() == key)
        .any(|mark| version <= mark.suppress_through && cut.contains_mutation(mark.visible_from))
}

/// Sorted unique `(key, fold_through)` pairs of the marks an image folds.
pub(super) type Folds = Vec<(Bytes, MutationVersion)>;

/// Split the adopted marks of a frozen cell into the marks the next image keeps
/// and a sorted unique `(key, fold_through)` list: a mark folds when the
/// image's retention floor contains its `visible_from`. The image then omits
/// every version of the key at or below the largest folded `suppress_through`.
pub(super) fn split_folds(
    marks: &[PruneMark],
    floor: &IndexReadCut,
) -> Option<(Vec<PruneMark>, Folds)> {
    if !marks
        .iter()
        .any(|mark| floor.contains_mutation(mark.visible_from))
    {
        return None;
    }
    let mut kept = Vec::new();
    let mut folds = Folds::new();
    for mark in marks {
        if floor.contains_mutation(mark.visible_from) {
            match folds.last_mut() {
                Some((key, through)) if *key == mark.key => {
                    *through = (*through).max(mark.suppress_through);
                }
                _ => folds.push((mark.key.clone(), mark.suppress_through)),
            }
        } else {
            kept.push(mark.clone());
        }
    }
    // A later mark of a folded key covers a newer head (a new mark never
    // covers a head an adopted mark already suppresses), so it stays valid on
    // its own. Both lists stay sorted because `marks` is.
    Some((kept, folds))
}

/// Fold bound of `key` in a sorted fold list.
pub(super) fn fold_through(
    folds: &[(Bytes, MutationVersion)],
    key: &[u8],
) -> Option<MutationVersion> {
    if folds.is_empty() {
        return None;
    }
    folds
        .binary_search_by(|(fold, _)| fold.as_ref().cmp(key))
        .ok()
        .map(|at| folds[at].1)
}

/// Fixed bytes of one encoded mark besides its key: the key length and two
/// versions (`be_u64 generation, be_u64 sequence, be_u32 ordinal` each).
const MARK_FIXED: usize = 4 + 2 * 20;

/// Exact length of the cell trailer that stores `marks`, or zero without
/// marks (the frame is then byte-identical to a frame without trailer).
pub(super) fn trailer_len(marks: Option<&Arc<[PruneMark]>>) -> Option<usize> {
    let Some(marks) = marks.filter(|marks| !marks.is_empty()) else {
        return Some(0);
    };
    marks.iter().try_fold(4usize, |total, mark| {
        total.checked_add(MARK_FIXED.checked_add(mark.key.len())?)
    })
}

fn encode_version(version: MutationVersion, output: &mut [u8]) {
    output[..8].copy_from_slice(&version.batch.epoch.to_be_bytes());
    output[8..16].copy_from_slice(&version.batch.sequence.to_be_bytes());
    output[16..20].copy_from_slice(&version.operation_index.to_be_bytes());
}

fn decode_version(cursor: &mut Cursor<'_>) -> Result<MutationVersion, NativeIndexError> {
    Ok(MutationVersion {
        batch: BatchVersion {
            epoch: cursor.u64()?,
            sequence: cursor.u64()?,
        },
        operation_index: cursor.u32()?,
    })
}

/// Cell trailer, after the last row of the cell payload and covered by the
/// cell's CRC and digest: `be_u32 mark_count` (nonzero), then per mark sorted
/// by key then `visible_from`: `be_u32 key_len, key, visible_from,
/// suppress_through`. Absent (zero bytes) when the cell has no mark.
pub(super) fn encode_trailer(
    marks: Option<&Arc<[PruneMark]>>,
    write: &mut dyn FnMut(&[u8]) -> Result<(), NativeIndexError>,
) -> Result<(), NativeIndexError> {
    let Some(marks) = marks.filter(|marks| !marks.is_empty()) else {
        return Ok(());
    };
    write(
        &u32::try_from(marks.len())
            .map_err(|_| NativeIndexError::Capacity)?
            .to_be_bytes(),
    )?;
    let mut versions = [0u8; 40];
    for mark in marks.iter() {
        write(
            &u32::try_from(mark.key.len())
                .map_err(|_| NativeIndexError::Capacity)?
                .to_be_bytes(),
        )?;
        write(&mark.key)?;
        encode_version(mark.visible_from, &mut versions[..20]);
        encode_version(mark.suppress_through, &mut versions[20..]);
        write(&versions)?;
    }
    Ok(())
}

/// Bounded decoding of one cell trailer. Marks are strictly sorted by `(key,
/// visible_from)`, route to their cell, lie within the root cut and suppress
/// no version above their own `visible_from`. Returns the marks and their
/// directory charge; the caller checks and adds the charge.
pub(super) fn decode_trailer(
    trailer: &[u8],
    descriptor: &KeySpaceDesc,
    cell: &CellId,
    cut: &IndexReadCut,
) -> Result<(Vec<PruneMark>, usize), NativeIndexError> {
    let mut cursor = Cursor::new(trailer);
    let count = cursor.u32()? as usize;
    // A mark is at least its fixed bytes and one key byte.
    if count == 0 || count > cursor.remaining() / (MARK_FIXED + 1) {
        return Err(NativeIndexError::Invalid("prune mark count"));
    }
    let mut marks: Vec<PruneMark> = Vec::with_capacity(count);
    let mut charge = 0usize;
    for _ in 0..count {
        let length = cursor.u32()? as usize;
        let reduced = cursor.take(length)?;
        if length == 0
            || length > crate::db::MAX_KEY_LEN
            || descriptor
                .index_key_size()
                .is_some_and(|width| width != length)
        {
            return Err(NativeIndexError::Invalid("prune mark key width"));
        }
        validate_cell_routing(descriptor, reduced)?;
        if descriptor.cell_id(reduced) != *cell {
            return Err(NativeIndexError::Invalid("prune mark cell key mismatch"));
        }
        let visible_from = decode_version(&mut cursor)?;
        let suppress_through = decode_version(&mut cursor)?;
        if !cut.contains_mutation(visible_from) || suppress_through > visible_from {
            return Err(NativeIndexError::Invalid("prune mark version domain"));
        }
        if marks.last().is_some_and(|prior| {
            (prior.key.as_ref(), prior.visible_from) >= (reduced, visible_from)
        }) {
            return Err(NativeIndexError::Invalid("unsorted prune marks"));
        }
        let mark = PruneMark {
            key: Bytes::copy_from_slice(reduced),
            visible_from,
            suppress_through,
            epoch: 0,
        };
        charge = charge
            .checked_add(mark_charge(&mark))
            .ok_or(NativeIndexError::Capacity)?;
        marks.push(mark);
    }
    if cursor.remaining() != 0 {
        return Err(NativeIndexError::Invalid("trailing prune mark bytes"));
    }
    Ok((marks, charge))
}

/// Resident index bytes for the prune marks one image adds, reserved before
/// the marks are created and released, for marks kept, only after the image
/// is live-installed (installation charges the marks to the index). The
/// checkpoint worker backs it with admission's index reservation.
pub(crate) trait MarkBudget: Send {
    /// Nonblocking: false when the bytes do not fit.
    fn reserve(&mut self, bytes: usize) -> bool;
    fn release(&mut self, bytes: usize);
}

/// Budget of an image persisted outside a serving database (tests, offline
/// images): no admission exists, every reservation succeeds.
pub(crate) struct UnboundedMarkBudget;

impl MarkBudget for UnboundedMarkBudget {
    fn reserve(&mut self, _: usize) -> bool {
        true
    }

    fn release(&mut self, _: usize) {}
}

/// Per-image prune-mark counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PruneStats {
    /// Keys whose suppressed versions this image omits with their marks.
    pub keys_folded: u64,
    /// Plan candidates that became marks.
    pub plan_accepted: u64,
    /// Plan candidates whose cell birth or exact head changed since the plan.
    pub plan_stale: u64,
    /// Plan candidates of cells whose new marks did not fit the budget.
    pub plan_capacity: u64,
}

/// Fold the adopted marks of a frozen cell that the image's retention floor
/// reached: the image omits every version they cover and keeps the rest.
/// Once per cell, before any new mark and before encoding.
pub(super) fn fold_cell(cell: &mut FrozenCell, floor: &IndexReadCut, stats: &mut PruneStats) {
    let Some((kept, folds)) = cell
        .marks
        .as_deref()
        .and_then(|marks| split_folds(marks, floor))
    else {
        return;
    };
    stats.keys_folded += folds.len() as u64;
    cell.fold = folds;
    cell.marks = (!kept.is_empty()).then(|| Arc::from(kept));
}

/// True when the newest reference of `key` at the capture cut, `head`, can
/// still be selected: above the newest `DropCells` mask, not covered by an
/// adopted mark and not folded by this image.
pub(super) fn selectable_at_capture(
    cell: &FrozenCell,
    key: &[u8],
    head: MutationVersion,
    cut: &IndexReadCut,
) -> bool {
    cell.masks.versions.last().is_none_or(|mask| head > *mask)
        && !cell
            .marks
            .as_deref()
            .is_some_and(|marks| suppressed_at_capture(marks, key, head, cut))
        && fold_through(&cell.fold, key).is_none_or(|fold| head > fold)
}

/// Newest reference of `key` in a frozen cell within `cut`.
fn newest_at(cell: &FrozenCell, key: &[u8], cut: &IndexReadCut) -> Option<ValueRecordReference> {
    let base = cell.base.as_ref().and_then(|block| block.get(key, cut));
    let overlay = cell.overlay.get(key).and_then(|values| {
        let values = values.as_slice();
        let end = values.partition_point(|value| cut.contains(value));
        (end > 0).then(|| values[end - 1])
    });
    newest(base, overlay)
}

/// Merge key-sorted new marks into the cell's marks and mark the cell's
/// stored frame stale. Every new mark has the image's cut as `visible_from`,
/// the newest of its key, so it follows the adopted marks of the same key.
pub(super) fn adopt_new_marks(cell: &mut FrozenCell, new: Vec<PruneMark>) {
    if new.is_empty() {
        return;
    }
    let adopted = cell.marks.as_deref().unwrap_or(&[]);
    let mut merged = Vec::with_capacity(adopted.len() + new.len());
    let mut adopted = adopted.iter().peekable();
    for mark in new {
        while let Some(prior) = adopted.next_if(|prior| prior.key <= mark.key) {
            merged.push(prior.clone());
        }
        merged.push(mark);
    }
    merged.extend(adopted.cloned());
    cell.marks = Some(Arc::from(merged));
    cell.marks_changed = true;
}

/// One planned removal: suppress `key` at the image's cut if, at that cut,
/// its cell has birth `birth` and its newest selectable reference is exactly
/// `expected`.
#[derive(Clone, Debug)]
pub(crate) struct PruneCandidate {
    pub keyspace: KeySpace,
    pub cell: CellId,
    pub birth: u64,
    pub key: Bytes,
    pub expected: ValueRecordReference,
}

/// Removals planned on an earlier cut, sorted by `(keyspace, cell, key)`,
/// checked again on the frozen image (CAPACITY-R0-R1.md §5).
#[derive(Clone, Debug, Default)]
pub(crate) struct PrunePlan {
    pub candidates: Vec<PruneCandidate>,
}

/// Apply the candidates of one frozen cell (a contiguous run of the plan).
/// Accepts only exact heads; reserves the cell's new marks as a whole, so a
/// refused reservation adds none of them.
pub(super) fn apply_plan(
    cell: &mut FrozenCell,
    candidates: &[PruneCandidate],
    cut: &IndexReadCut,
    install_epoch: u64,
    budget: &mut dyn MarkBudget,
    stats: &mut PruneStats,
) -> Result<(), NativeIndexError> {
    if candidates.is_empty() {
        return Ok(());
    }
    let visible_from =
        retention::position(cut).ok_or(NativeIndexError::Invalid("prune plan cut is empty"))?;
    let mut new = Vec::new();
    for candidate in candidates {
        let accepted = candidate.birth == cell.birth
            && newest_at(cell, &candidate.key, cut).is_some_and(|head| {
                head == candidate.expected
                    && head.descriptor.kind == RecordKind::Put
                    && selectable_at_capture(cell, &candidate.key, head.mutation_version(), cut)
            });
        if !accepted {
            stats.plan_stale += 1;
            continue;
        }
        if new
            .last()
            .is_some_and(|prior: &PruneMark| prior.key >= candidate.key)
        {
            return Err(NativeIndexError::Invalid("unsorted prune plan"));
        }
        new.push(PruneMark {
            key: candidate.key.clone(),
            visible_from,
            suppress_through: candidate.expected.mutation_version(),
            epoch: install_epoch,
        });
    }
    let charge = new.iter().map(mark_charge).sum::<usize>();
    if charge != 0 && !budget.reserve(charge) {
        stats.plan_capacity += new.len() as u64;
        return Ok(());
    }
    stats.plan_accepted += new.len() as u64;
    adopt_new_marks(cell, new);
    Ok(())
}
