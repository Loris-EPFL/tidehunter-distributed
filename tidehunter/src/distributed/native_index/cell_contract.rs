//! Option C contract, native-index side (checkpoint option C spec §§2.1, 3.1,
//! 4.3, 7.1, 8.1): batch-prefix cuts, the flusher count, index-store file ids,
//! and the fields and validity rules of the format-5 index root.
//!
//! Exact mode does not use these types: it keeps root format 4 and its global
//! cut. No code here reads, writes or deletes a file. Per-cell stages build on
//! these declarations; they must not weaken the rules in `CellRoot::check`.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "option C contract, wired by the per-cell stages")
)]

use super::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Fixed cap of the cell flusher pool (spec §3.1). Not a measured optimum.
pub(crate) const MAX_CELL_FLUSHERS: usize = 8;

/// Flusher count `P = min(available parallelism, shards, MAX_CELL_FLUSHERS)`,
/// at least one. Shard ranges, not cells, are assigned to flushers.
pub(crate) fn cell_flushers(shards: usize) -> usize {
    crate::parallelism::available()
        .min(shards)
        .clamp(1, MAX_CELL_FLUSHERS)
}

/// Index root format of per-cell coverage roots. `FORMAT` (4) stays the
/// exact-checkpoint root; a loader never reads one format as the other.
pub(crate) const PER_CELL_FORMAT: u16 = 5;

const PREFIX_BEFORE_ALL: u8 = 0;
const PREFIX_THROUGH: u8 = 1;
/// Encoded length of a [`PrefixCut`]: tag, epoch, sequence.
pub(crate) const PREFIX_CUT_BYTES: usize = 17;

/// An inclusive, complete batch prefix (spec §2.1). It can include resolved
/// aborted sequence gaps. It has no `Latest` or sparse form, so a sparse
/// published upper bound can never become a checkpoint cut. Order is the
/// recovery order: `BeforeAll` first, then batch versions by epoch, sequence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) enum PrefixCut {
    BeforeAll,
    Through(BatchVersion),
}

impl PrefixCut {
    pub(crate) fn contains(self, batch: BatchVersion) -> bool {
        match self {
            Self::BeforeAll => false,
            Self::Through(last) => batch <= last,
        }
    }

    /// The index selection of this prefix. It includes every operation of each
    /// included batch, never only the last member one cell happens to hold.
    pub(crate) fn index_cut(self) -> IndexReadCut {
        match self {
            Self::BeforeAll => IndexReadCut::BeforeAll,
            Self::Through(batch) => IndexReadCut::At(MutationVersion {
                batch,
                operation_index: u32::MAX,
            }),
        }
    }

    /// True if the scalar history floor (`None` = `BeforeAll`) is at or
    /// below this prefix. A floor inside an included batch is below it.
    pub(crate) fn holds_floor(self, floor: Option<MutationVersion>) -> bool {
        floor.is_none_or(|floor| self.contains(floor.batch))
    }

    /// Fixed-width big-endian form: tag 0 with sixteen zero bytes, or tag 1,
    /// epoch, sequence.
    pub(crate) fn encode_into(self, output: &mut Vec<u8>) {
        match self {
            Self::BeforeAll => {
                output.push(PREFIX_BEFORE_ALL);
                output.extend_from_slice(&[0; 16]);
            }
            Self::Through(batch) => {
                output.push(PREFIX_THROUGH);
                output.extend_from_slice(&batch.epoch.to_be_bytes());
                output.extend_from_slice(&batch.sequence.to_be_bytes());
            }
        }
    }

    /// Any tag other than 0 or 1 (including former `Latest`/sparse forms)
    /// and a nonzero `BeforeAll` body are invalid.
    pub(super) fn decode(cursor: &mut Cursor<'_>) -> Result<Self, NativeIndexError> {
        let tag = cursor.u8()?;
        let epoch = cursor.u64()?;
        let sequence = cursor.u64()?;
        match tag {
            PREFIX_BEFORE_ALL if epoch == 0 && sequence == 0 => Ok(Self::BeforeAll),
            PREFIX_THROUGH => Ok(Self::Through(BatchVersion { epoch, sequence })),
            _ => Err(NativeIndexError::Invalid("prefix cut tag")),
        }
    }
}

/// Immutable id of a file that a format-5 root can reference (spec §4.3,
/// §7.1). The full id is never reused: a restart gets a new authority epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum IndexStoreFileId {
    /// Container of an exact checkpoint, referenced unchanged after migration.
    LegacyContainer {
        authority_epoch: u64,
        generation: u64,
    },
    /// Append-only segment of one flusher stream.
    Segment {
        authority_epoch: u64,
        stream_id: u16,
        segment_seq: u64,
    },
}

impl IndexStoreFileId {
    /// The only name of this id, relative to the database directory.
    pub(crate) fn canonical_name(self) -> String {
        match self {
            Self::LegacyContainer {
                authority_epoch,
                generation,
            } => format!("index-{authority_epoch}-{generation}.snapshot.cells"),
            Self::Segment {
                authority_epoch,
                stream_id,
                segment_seq,
            } => format!("index-store-{authority_epoch}-{stream_id}-{segment_seq}.cells"),
        }
    }

    /// Inverse of [`Self::canonical_name`]. A name that does not round-trip
    /// (leading zeros, signs, extra parts) is not canonical.
    pub(crate) fn parse_canonical(name: &str) -> Option<Self> {
        let id = if let Some(rest) = name
            .strip_prefix("index-store-")
            .and_then(|rest| rest.strip_suffix(".cells"))
        {
            let mut parts = rest.split('-');
            let id = Self::Segment {
                authority_epoch: parts.next()?.parse().ok()?,
                stream_id: parts.next()?.parse().ok()?,
                segment_seq: parts.next()?.parse().ok()?,
            };
            parts.next().is_none().then_some(id)?
        } else {
            let rest = name
                .strip_prefix("index-")?
                .strip_suffix(".snapshot.cells")?;
            let (epoch, generation) = rest.split_once('-')?;
            Self::LegacyContainer {
                authority_epoch: epoch.parse().ok()?,
                generation: generation.parse().ok()?,
            }
        };
        (id.canonical_name() == name).then_some(id)
    }
}

/// Header fields of a format-5 index root (spec §7.1). The encoded header
/// also carries the cell and file-table counts: the lengths of
/// [`CellRoot::cells`] and [`CellRoot::files`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CellRootHeader {
    pub format: u16,
    /// Reserved; zero.
    pub flags: u16,
    pub database: [u8; 16],
    pub schema: [u8; 32],
    pub authority_epoch: u64,
    pub snapshot_generation: u64,
    pub lifecycle_generation: u64,
    /// `T`: the directory-capture target.
    pub directory_target: PrefixCut,
    /// `F`: the global replay floor; the minimum cell coverage, or `T`.
    pub replay_floor: PrefixCut,
    /// `U`: at least `T` and every frame cut. Owner proofs cover it.
    pub max_frame_cut: PrefixCut,
    /// `H`: the global history floor, a scalar mutation cut (`None` is
    /// `BeforeAll`).
    pub history_floor: Option<MutationVersion>,
}

/// One entry of the sorted file table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RootFile {
    pub id: IndexStoreFileId,
    /// Canonical relative name of `id`.
    pub name: String,
    /// Length of the sealed file. Every referenced range ends within it.
    pub sealed_len: u64,
}

/// One entry of the sorted cell directory, including cells with no base.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RootCell {
    pub keyspace: KeySpace,
    pub cell: CellId,
    /// `C_i`: complete-through coverage exported by this root.
    pub coverage: PrefixCut,
    /// Encoded as the `base_present` flag and, if set, the base fields.
    pub base: Option<RootCellBase>,
}

/// The immutable frame of one cell. Its rows and versions are not persisted:
/// frame validation gives them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RootCellBase {
    pub file: IndexStoreFileId,
    pub offset: u64,
    /// CRC-framed length, including the eight-byte frame header.
    pub framed_len: u64,
    /// BLAKE2b-32 of the framed range.
    pub digest: [u8; 32],
    /// `B_i`: the cut used to encode the frame. No reference is newer.
    pub frame_cut: PrefixCut,
    /// `H_i`: the frame's compaction floor, at or below `H`.
    pub frame_floor: Option<MutationVersion>,
}

/// The decoded content of a format-5 index root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CellRoot {
    pub header: CellRootHeader,
    pub files: Vec<RootFile>,
    pub cells: Vec<RootCell>,
}

impl CellRoot {
    /// Structural rules of spec §7.1, checked before any frame is opened.
    /// Frame contents (CRC, digest, rows, references at or below `B_i` and
    /// above `H_i`) are checked by the loader after this passes.
    pub(crate) fn check(
        &self,
        database: [u8; 16],
        schema: [u8; 32],
    ) -> Result<(), NativeIndexError> {
        let header = &self.header;
        if header.format != PER_CELL_FORMAT || header.flags != 0 {
            return Err(NativeIndexError::Invalid("cell root format"));
        }
        if header.database != database || header.schema != schema {
            return Err(NativeIndexError::Invalid("cell root database or schema"));
        }
        if header.snapshot_generation == 0 {
            return Err(NativeIndexError::Invalid("cell root generation"));
        }
        let target = header.directory_target;
        if !header.replay_floor.holds_floor(header.history_floor) {
            return Err(NativeIndexError::Invalid(
                "history floor above replay floor",
            ));
        }
        if header.max_frame_cut < target {
            return Err(NativeIndexError::Invalid("frame cut bound below target"));
        }
        if self.files.windows(2).any(|pair| pair[0].id >= pair[1].id) {
            return Err(NativeIndexError::Invalid("file table order"));
        }
        if self
            .files
            .iter()
            .any(|file| file.name != file.id.canonical_name())
        {
            return Err(NativeIndexError::Invalid("file table name"));
        }
        if self.cells.windows(2).any(|pair| {
            directory_order(
                (pair[0].keyspace, &pair[0].cell),
                (pair[1].keyspace, &pair[1].cell),
            ) != std::cmp::Ordering::Less
        }) {
            return Err(NativeIndexError::Invalid("cell directory order"));
        }
        let mut floor = target;
        let mut ranges: BTreeMap<IndexStoreFileId, BTreeMap<u64, u64>> = BTreeMap::new();
        for cell in &self.cells {
            if cell.coverage > target {
                return Err(NativeIndexError::Invalid("cell coverage above target"));
            }
            floor = floor.min(cell.coverage);
            let Some(base) = &cell.base else { continue };
            if base.frame_cut == PrefixCut::BeforeAll || base.frame_cut > header.max_frame_cut {
                return Err(NativeIndexError::Invalid("cell frame cut"));
            }
            // `None` (`BeforeAll`) orders below every mutation version.
            if !base.frame_cut.holds_floor(base.frame_floor)
                || base.frame_floor > header.history_floor
            {
                return Err(NativeIndexError::Invalid("cell frame floor"));
            }
            let file = self
                .files
                .binary_search_by(|file| file.id.cmp(&base.file))
                .map(|at| &self.files[at])
                .map_err(|_| NativeIndexError::Invalid("cell file id"))?;
            let end = base
                .offset
                .checked_add(base.framed_len)
                .ok_or(NativeIndexError::Invalid("cell range overflow"))?;
            if !(12..=MAX_IMAGE_BYTES as u64).contains(&base.framed_len) || end > file.sealed_len {
                return Err(NativeIndexError::Invalid("cell range bounds"));
            }
            let file_ranges = ranges.entry(base.file).or_default();
            if file_ranges
                .range(..=base.offset)
                .next_back()
                .is_some_and(|(_, previous_end)| *previous_end > base.offset)
                || file_ranges
                    .range(base.offset..)
                    .next()
                    .is_some_and(|(start, _)| *start < end)
            {
                return Err(NativeIndexError::Invalid("overlapping cell ranges"));
            }
            file_ranges.insert(base.offset, end);
        }
        if header.replay_floor != floor {
            return Err(NativeIndexError::Invalid(
                "replay floor is not the minimum coverage",
            ));
        }
        // The file table is the authoritative referenced-file list for GC:
        // an unreferenced entry would retain a dead file.
        if ranges.len() != self.files.len() {
            return Err(NativeIndexError::Invalid("unreferenced file table entry"));
        }
        Ok(())
    }
}

/// Cell directory order: keyspace, then cell id. The key shape gives every
/// keyspace one id kind, and `CellId::cmp` panics on mixed kinds; corrupt
/// input that mixes them orders by kind here and fails frame validation.
fn directory_order(left: (KeySpace, &CellId), right: (KeySpace, &CellId)) -> std::cmp::Ordering {
    let bytes = |cell: &CellId| matches!(cell, CellId::Bytes(_));
    left.0
        .0
        .cmp(&right.0.0)
        .then_with(|| bytes(left.1).cmp(&bytes(right.1)))
        .then_with(|| left.1.cmp(right.1))
}

/// Test oracle of mixed-cut replay (spec §8.1 steps 5-6, §8.2), the reference
/// that the recovery stage is tested against.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MemberReplay {
    /// The cell base already holds this member: compare the full expected
    /// reference and do not insert or charge it again. Absent or different
    /// is a coverage or integrity error, not a duplicate.
    CompareBase,
    /// Prepare and publish the member with its original batch id, digest,
    /// address and operation index.
    Apply,
}

#[cfg(test)]
impl CellRoot {
    /// Replay of the authenticated whole batch `version` whose members route
    /// to `members` (in operation order). `None`: the batch is at or below `F`
    /// and recovery skips it. `Some`: recovery authenticates the whole batch,
    /// accounts its identity, receipt and replay bytes once (also when every
    /// member is covered), and handles each member as given. A cell absent
    /// from the directory had no mutation through `T`: its members apply.
    pub(crate) fn replay_plan(
        &self,
        version: BatchVersion,
        members: &[(KeySpace, CellId)],
    ) -> Option<Vec<MemberReplay>> {
        if self.header.replay_floor.contains(version) {
            return None;
        }
        Some(
            members
                .iter()
                .map(|(keyspace, cell)| {
                    let covered = self
                        .cells
                        .binary_search_by(|entry| {
                            directory_order((entry.keyspace, &entry.cell), (*keyspace, cell))
                        })
                        .is_ok_and(|at| {
                            let entry = &self.cells[at];
                            entry.coverage.contains(version)
                                || entry
                                    .base
                                    .as_ref()
                                    .is_some_and(|base| base.frame_cut.contains(version))
                        });
                    if covered {
                        MemberReplay::CompareBase
                    } else {
                        MemberReplay::Apply
                    }
                })
                .collect(),
        )
    }
}

#[cfg(test)]
#[path = "cell_contract_tests.rs"]
mod tests;
