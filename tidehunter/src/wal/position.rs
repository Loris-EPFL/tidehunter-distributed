//! WAL positions and the position and frontier traits the index stack is
//! generic over.
//!
//! [`IndexCut`], [`IndexPosition`] and [`IndexEntryKind`] are `pub` and
//! `#[doc(hidden)]`, not `pub(crate)`: they bound the parameters of types that
//! are publicly reachable (`IndexTable<P>` through `pub mod index` and the
//! `test_utils` re-exports, `SnapshotEntryData<C>` and `LargeTable<P>` through
//! `pub mod large_table`), and a `pub(crate)` bound on a public type is a
//! `private_bounds` error under `-D warnings`. They are not supported API.

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub trait HasOffset {
    fn offset(&self) -> u64;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct WalPosition {
    pub(super) offset: u64,
    pub(super) len: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct WalFileId(pub(super) u64);

#[cfg(feature = "experimental-distributed")]
impl WalFileId {
    pub(crate) fn from_u64(id: u64) -> Self {
        Self(id)
    }

    pub(crate) fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct MapId(pub(super) u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct LastProcessed(pub(super) u64);

impl WalPosition {
    pub const MAX: WalPosition = WalPosition {
        offset: u64::MAX,
        len: u32::MAX,
    };
    pub const INVALID: WalPosition = Self::MAX;
    pub const LENGTH: usize = 12;
    #[cfg(test)]
    pub const TEST: WalPosition = WalPosition::new(3311, 12);

    // Creates new wal position.
    // This should only be called from wal.rs or from conversion IndexWalPosition<->WalPosition
    pub(crate) const fn new(offset: u64, len: u32) -> Self {
        Self { offset, len }
    }

    pub fn write_to_buf(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.offset);
        buf.put_u32(self.len);
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(Self::LENGTH);
        self.write_to_buf(&mut buf);
        buf.into()
    }

    pub fn read_from_buf(buf: &mut impl Buf) -> Self {
        Self::new(buf.get_u64(), buf.get_u32())
    }

    pub fn from_slice(mut slice: &[u8]) -> Self {
        let r = Self::read_from_buf(&mut slice);
        assert_eq!(0, slice.len());
        r
    }

    /// Returns unaligned length of the wal frame.
    /// The length includes frame header but does not account for alignment
    pub fn frame_len(&self) -> usize {
        self.len as usize
    }

    /// Same as frame_len but returns u32
    pub fn frame_len_u32(&self) -> u32 {
        self.len
    }

    /// Returns length of the payload at given wal position, without crc frame header and alignment.
    /// This is the length of the buffer that was passed when WalWriter::write was
    /// called that created this wal position.
    pub fn payload_len(&self) -> usize {
        self.frame_len()
            .checked_sub(crate::crc::CrcFrame::CRC_HEADER_LENGTH)
            .expect("Frame length must be greater or equal to crc header length")
    }

    pub fn valid(self) -> Option<Self> {
        if self == Self::INVALID {
            None
        } else {
            Some(self)
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }

    #[allow(dead_code)]
    pub fn test_value(v: u64) -> Self {
        Self::new(v, 0)
    }
}

impl HasOffset for WalPosition {
    fn offset(&self) -> u64 {
        self.offset
    }
}

impl MapId {
    pub(super) fn new(id: u64) -> Self {
        Self(id)
    }

    pub(super) fn as_u64(self) -> u64 {
        self.0
    }

    pub(super) fn prev_map(self) -> Option<Self> {
        self.0.checked_sub(1).map(MapId)
    }

    pub(super) fn next_map(self) -> Self {
        MapId(self.0 + 1)
    }
}

impl LastProcessed {
    pub(crate) fn new(value: u64) -> Self {
        Self(value)
    }

    #[cfg(test)]
    pub(crate) fn none() -> Self {
        Self(0)
    }

    #[doc(hidden)] // Used by tools/tideconsole for control region inspection
    pub fn as_u64(self) -> u64 {
        self.0
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)] // Used by benches/index_iter_benchmarks to drive as-of lookups
    pub fn from_u64_for_test(value: u64) -> Self {
        Self(value)
    }

    /// Returns whether the position is processed(included in the index).
    pub(crate) fn is_processed<T: HasOffset>(self, pos: &T) -> bool {
        // Offset is processed if it is below (but not equal) to last_processed position
        // last_processed points to a next allocated position
        pos.offset() < self.0
    }

    #[cfg(test)]
    pub(crate) fn new_test(value: u64) -> Self {
        Self(value)
    }
}

/// Kind of an index entry. Persisted in the native flat/blob row as two bits
/// of the length word (`to_u8`); the hybrid row packs it the same way into its
/// operation-index word.
#[derive(Copy, Clone, Eq, PartialEq, Debug, PartialOrd, Ord, Hash)]
#[doc(hidden)]
pub enum IndexEntryKind {
    Clean,
    Modified,
    Removed,
}

impl IndexEntryKind {
    pub(crate) fn to_u8(self) -> u8 {
        match self {
            IndexEntryKind::Clean => 0,
            IndexEntryKind::Modified => 1,
            IndexEntryKind::Removed => 2,
        }
    }

    pub(crate) fn from_u8(b: u8) -> Self {
        match b {
            1 => IndexEntryKind::Modified,
            2 => IndexEntryKind::Removed,
            _ => IndexEntryKind::Clean,
        }
    }
}

/// An exclusive index frontier: every position strictly below it is processed
/// (present in the index). Natively it is a value-WAL byte offset; a multi-owner
/// index uses a batch version.
#[doc(hidden)]
pub trait IndexCut:
    Copy + Eq + Ord + std::fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// The frontier before any write (`LastProcessed(0)`).
    const NONE: Self;
    /// Control-region payload version this instantiation writes and accepts.
    /// A reader refuses any other version instead of reinterpreting the payload.
    const CONTROL_REGION_VERSION: u32;

    /// Distance used by metrics that compare two frontiers.
    fn saturating_lag(self, later: Self) -> u64;

    /// Byte position this frontier maps to for the native byte-cadence
    /// thresholds (`snapshot_written_bytes`, `snapshot_unload_threshold`).
    /// A version-based instantiation keeps its byte counter separately and may
    /// return 0 here.
    fn byte_position(self) -> u64;

    /// Inverse of [`Self::byte_position`] for the legacy control-region
    /// layouts (V1/V2), which store the frontier as a bare `u64` byte offset.
    /// `None` when this instantiation's frontier is not a byte offset: such a
    /// build refuses a legacy file with `CONTROL_REGION_VERSION_MISMATCH`
    /// instead of reinterpreting it.
    fn from_byte_position(bytes: u64) -> Option<Self> {
        let _ = bytes;
        None
    }
}

/// A position stored in the cell index. Native instantiates it with
/// [`WalPosition`] (12 B); the distributed hybrid uses a version-carrying
/// position.
///
/// `Tagged` is the kind-bearing form the index actually stores (native:
/// `IndexWalPosition`, 16 B). Keeping it an associated type rather than a
/// `{pos, kind}` wrapper preserves the native in-memory layout, which the
/// codegen and bench gates pin.
#[doc(hidden)]
pub trait IndexPosition:
    Copy + Eq + Ord + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static
{
    type Cut: IndexCut;
    type Tagged: Copy + Eq + Ord + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static;

    /// Bytes this position contributes to one flat/blob row.
    const LENGTH: usize;
    /// Sentinel for "no value" (a tombstone's position on disk).
    const INVALID: Self;

    fn is_valid(self) -> bool;
    /// Exclusive cut at this position.
    fn frontier(self) -> Self::Cut;
    /// True if this position is processed relative to `cut`.
    fn below(self, cut: Self::Cut) -> bool;
    /// Strict order of two positions in write order. Native compares the WAL
    /// offset alone (unchanged from the pre-generic asserts).
    fn precedes(self, other: Self) -> bool;

    fn tag(self, kind: IndexEntryKind) -> Self::Tagged;
    fn untag(tagged: Self::Tagged) -> (Self, IndexEntryKind);

    /// Build the tagged entry for a position read from an index blob: the
    /// invalid sentinel is the on-disk tombstone marker, everything else is a
    /// clean entry.
    fn from_disk_position(position: Self) -> Self::Tagged {
        if position.is_valid() {
            Self::tag(position, IndexEntryKind::Clean)
        } else {
            Self::tag(position, IndexEntryKind::Removed)
        }
    }

    /// Write this position's flat row (`LENGTH` bytes), kind included.
    /// In-memory flat buffer only: an on-disk tombstone row is not a flat row.
    fn write_flat<B: BufMut>(&self, kind: u8, buf: &mut B);
    /// Read this position's flat row (`LENGTH` bytes).
    fn read_flat<B: Buf>(buf: &mut B) -> (Self, u8);
    /// Decode the flat row at `buf[at..at + LENGTH]` straight into its tagged
    /// entry. Native overrides this with the pre-generic field-by-field decode
    /// (same slice expressions, so the hot index lookups keep their bounds
    /// checks); the default goes through [`Self::read_flat`] and [`Self::tag`].
    #[inline]
    fn read_flat_tagged_at(buf: &[u8], at: usize) -> Self::Tagged {
        let mut row = &buf[at..at + Self::LENGTH];
        let (position, kind) = Self::read_flat(&mut row);
        debug_assert!(row.is_empty(), "flat row shorter than P::LENGTH");
        Self::tag(position, IndexEntryKind::from_u8(kind))
    }

    /// Write this position's on-disk index-blob row (`LENGTH` bytes).
    /// Distinct from [`Self::write_flat`]: native tombstone rows are written
    /// verbatim (`len = u32::MAX`), not normalised to the flat encoding.
    fn write_blob<B: BufMut>(&self, buf: &mut B);
    /// Read this position's on-disk index-blob row (`LENGTH` bytes).
    fn read_blob<B: Buf>(buf: &mut B) -> Self;
    /// Read exactly one blob row from `row` (`row.len() == LENGTH`). Native
    /// overrides this with `WalPosition::from_slice`, the pre-generic call.
    #[inline]
    fn read_blob_exact(row: &[u8]) -> Self {
        let mut row = row;
        let position = Self::read_blob(&mut row);
        assert!(row.is_empty(), "blob row longer than P::LENGTH");
        position
    }

    #[inline]
    fn kind(tagged: Self::Tagged) -> IndexEntryKind {
        Self::untag(tagged).1
    }

    #[inline]
    fn tagged_position(tagged: Self::Tagged) -> Self {
        Self::untag(tagged).0
    }

    #[inline]
    fn with_kind(tagged: Self::Tagged, kind: IndexEntryKind) -> Self::Tagged {
        Self::tag(Self::tagged_position(tagged), kind)
    }

    #[inline]
    fn tagged_is_removed(tagged: Self::Tagged) -> bool {
        matches!(Self::kind(tagged), IndexEntryKind::Removed)
    }

    #[inline]
    fn tagged_is_clean(tagged: Self::Tagged) -> bool {
        matches!(Self::kind(tagged), IndexEntryKind::Clean)
    }

    /// The value position a reader fetches for this entry: a tombstone has no
    /// value, so it collapses to [`Self::INVALID`].
    #[inline]
    fn tagged_into_position(tagged: Self::Tagged) -> Self {
        match Self::kind(tagged) {
            IndexEntryKind::Removed => Self::INVALID,
            IndexEntryKind::Clean | IndexEntryKind::Modified => Self::tagged_position(tagged),
        }
    }

    /// The position of the write itself, valid for tombstones too.
    #[inline]
    fn tagged_update_position(tagged: Self::Tagged) -> Self {
        Self::tagged_position(tagged)
    }

    #[inline]
    fn tagged_as_clean_modified(tagged: Self::Tagged) -> Self::Tagged {
        match Self::kind(tagged) {
            IndexEntryKind::Clean | IndexEntryKind::Modified => {
                Self::with_kind(tagged, IndexEntryKind::Clean)
            }
            IndexEntryKind::Removed => {
                panic!("Can't call as_clean_modified on IndexEntryKind::Removed")
            }
        }
    }

    /// True when `a` and `b` are the same physical index entry: same ordering
    /// position *and* same stored address and kind. `==` on the tagged type is
    /// the ordering equality (a version-carrying position orders by version
    /// alone), so it cannot tell a relocation re-point from "unchanged".
    /// Native: the derived `==` already compares every stored field.
    #[inline]
    fn tagged_same_physical(a: Self::Tagged, b: Self::Tagged) -> bool {
        a == b
    }

    /// True when the tagged entry's position is the on-disk tombstone sentinel.
    ///
    /// A tombstone a flush wrote at its own position carries a genuine offset
    /// and is not a sentinel; a disk-derived tombstone decodes to the sentinel
    /// with a rewritten length word, so comparing against [`Self::INVALID`]
    /// field-wise would not recognise it.
    #[inline]
    fn tagged_is_sentinel(tagged: Self::Tagged) -> bool {
        Self::tagged_position(tagged)
            == Self::tagged_position(Self::from_disk_position(Self::INVALID))
    }

    /// The frontier a cell stores when a flush it requested commits at `new`
    /// while the cell currently stores `old` (see `commit_flushed_cut`).
    ///
    /// Native: `new`, a plain store, exactly as before the position type was
    /// generic. That includes an `Empty` cell anchored at its first write `P`
    /// by `mark_dirty` and then committed by a flush requested at
    /// `new <= P` (an older write was still in flight): the cell stores the
    /// more conservative `new`.
    ///
    /// A version-carrying position whose loader frontier can fall (a latch
    /// pins it below an earlier commit) overrides this with `max(old, new)`,
    /// so a transient low commit never drags a cell's frontier, and with it
    /// the snapshot's `replay_from`, back below what is already stored.
    #[inline]
    fn committed_cut(old: Self::Cut, new: Self::Cut) -> Self::Cut {
        let _ = old;
        new
    }

    /// The frontier an `Empty` cell is anchored at by its first write `first`
    /// (`mark_dirty`). `bound` yields the loader's insertion bound
    /// (`Loader::insert_anchor_bound`): a cut below which every write is already
    /// in the index.
    ///
    /// Keeps the invariant I(cell): `cell.last_processed` is at or below the
    /// version of every overlay entry the cell's blobs do not cover.
    ///
    /// Every instantiation takes that minimum. Native passes the tracker's
    /// externally observed frontier, so a cell a batch anchored above a lower
    /// op of the same batch is pulled back down (a batch drops its guards at
    /// commit, before its ops are applied). A version-carrying engine passes
    /// its contiguous index-applied cut for the same reason: a lower version
    /// can follow a higher one into the same cell in durable-arrival order.
    /// `bound` must be read before the insert, under the row lock; both bounds
    /// are monotone, so a later read could exceed an in-flight lower write.
    #[inline]
    fn anchor_cut(first: Self, bound: impl FnOnce() -> Self::Cut) -> Self::Cut {
        let frontier = first.frontier();
        std::cmp::min(frontier, bound())
    }
}

impl IndexCut for LastProcessed {
    const NONE: Self = LastProcessed(0);
    const CONTROL_REGION_VERSION: u32 = 3;

    fn saturating_lag(self, later: Self) -> u64 {
        later.0.saturating_sub(self.0)
    }

    fn byte_position(self) -> u64 {
        self.0
    }

    fn from_byte_position(bytes: u64) -> Option<Self> {
        Some(LastProcessed(bytes))
    }
}

impl IndexPosition for WalPosition {
    type Cut = LastProcessed;
    type Tagged = super::super::index::index_table::TaggedWalPosition;

    const LENGTH: usize = WalPosition::LENGTH;
    const INVALID: Self = WalPosition::INVALID;

    #[inline]
    fn is_valid(self) -> bool {
        self != Self::INVALID
    }

    #[inline]
    fn frontier(self) -> Self::Cut {
        LastProcessed::new(self.offset)
    }

    #[inline]
    fn below(self, cut: Self::Cut) -> bool {
        // Native "processed" and "below the frontier" are the same
        // offset-only comparison.
        cut.is_processed(&self)
    }

    #[inline]
    fn precedes(self, other: Self) -> bool {
        self.offset < other.offset
    }

    #[inline]
    fn tag(self, kind: IndexEntryKind) -> Self::Tagged {
        super::super::index::index_table::TaggedWalPosition::from_parts(self.offset, self.len, kind)
    }

    /// Native normalization: the on-disk tombstone sentinel is
    /// `WalPosition::INVALID` with `len = u32::MAX`, which must be stored as
    /// `len = 0` so the flat `[MAX][0x8000_0000]` row stays decodable and
    /// byte-identical to the pre-generic format.
    #[inline]
    fn from_disk_position(position: Self) -> Self::Tagged {
        super::super::index::index_table::TaggedWalPosition::from_disk(position)
    }

    #[inline]
    fn untag(tagged: Self::Tagged) -> (Self, IndexEntryKind) {
        // The raw position of the write, valid for tombstones too: using the
        // reader-facing position would collapse a Removed entry to INVALID and
        // break ordering comparisons against it.
        (tagged.into_update_position(), tagged.kind())
    }

    // The accessors below are the pre-generic inherent methods, forwarded
    // verbatim: the trait defaults route through `untag` and re-derive the
    // same fields, which is equivalent but was measured to change register
    // allocation in the hot index lookups.
    #[inline]
    fn kind(tagged: Self::Tagged) -> IndexEntryKind {
        tagged.kind()
    }

    #[inline]
    fn with_kind(tagged: Self::Tagged, kind: IndexEntryKind) -> Self::Tagged {
        tagged.with_kind(kind)
    }

    #[inline]
    fn tagged_position(tagged: Self::Tagged) -> Self {
        tagged.into_update_position()
    }

    #[inline]
    fn tagged_is_removed(tagged: Self::Tagged) -> bool {
        tagged.is_removed()
    }

    #[inline]
    fn tagged_is_clean(tagged: Self::Tagged) -> bool {
        tagged.is_clean()
    }

    #[inline]
    fn tagged_into_position(tagged: Self::Tagged) -> Self {
        tagged.into_wal_position()
    }

    #[inline]
    fn tagged_update_position(tagged: Self::Tagged) -> Self {
        tagged.into_update_position()
    }

    #[inline]
    fn tagged_as_clean_modified(tagged: Self::Tagged) -> Self::Tagged {
        tagged.as_clean_modified()
    }

    #[inline]
    fn write_flat<B: BufMut>(&self, kind: u8, buf: &mut B) {
        // The kind shares the length word, so a flat (in-memory) row requires
        // len < 2^30. The only rows that exceed it are on-disk tombstone
        // sentinels (`len = u32::MAX`), which are written verbatim and decoded
        // by offset, exactly as before.
        // Release-mode assert, as in the pre-generic flat codec: an oversized
        // length would silently corrupt the kind bits.
        assert!(self.len < (1 << 30), "flat row length must fit in 30 bits");
        buf.put_u64(self.offset);
        buf.put_u32(self.len | ((kind as u32) << 30));
    }

    #[inline]
    fn read_flat<B: Buf>(buf: &mut B) -> (Self, u8) {
        let offset = buf.get_u64();
        let encoded = buf.get_u32();
        (
            WalPosition::new(offset, encoded & 0x3FFF_FFFF),
            (encoded >> 30) as u8,
        )
    }

    /// The pre-generic flat row decode: the same two big-endian reads over the
    /// same slice expressions, and the arithmetic of the former
    /// `decode_kind_from_len`. Measured against base (objdump), this keeps the
    /// hot index lookups' operations and bounds checks unchanged.
    #[inline]
    fn read_flat_tagged_at(buf: &[u8], at: usize) -> Self::Tagged {
        let offset = u64::from_be_bytes(buf[at..at + 8].try_into().unwrap());
        let encoded = u32::from_be_bytes(buf[at + 8..at + 12].try_into().unwrap());
        super::super::index::index_table::TaggedWalPosition::from_parts(
            offset,
            encoded & 0x3FFF_FFFF,
            IndexEntryKind::from_u8((encoded >> 30) as u8),
        )
    }

    #[inline]
    fn write_blob<B: BufMut>(&self, buf: &mut B) {
        self.write_to_buf(buf);
    }

    #[inline]
    fn read_blob<B: Buf>(buf: &mut B) -> Self {
        WalPosition::read_from_buf(buf)
    }

    #[inline]
    fn read_blob_exact(row: &[u8]) -> Self {
        WalPosition::from_slice(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_position() {
        let mut buf = BytesMut::new();
        WalPosition::TEST.write_to_buf(&mut buf);
        let bytes: bytes::Bytes = buf.into();
        let mut buf = bytes.as_ref();
        let position = WalPosition::read_from_buf(&mut buf);
        assert_eq!(position, WalPosition::TEST);
    }
}
