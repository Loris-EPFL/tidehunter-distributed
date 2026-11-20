//! Version-carrying index positions for the hybrid coordinator index.
//!
//! Native Tidehunter indexes a single local WAL offset
//! ([`crate::wal::position::WalPosition`]). The hybrid coordinator indexes
//! values that live in several independent owner WALs, so an index entry must
//! carry the write's logical version and the physical location of the value
//! member. [`VersionPosition`] is that entry; [`BatchVersion`] is its cut.
//!
//! Ordering is over the logical identity `(batch, operation_index)` only
//! (plan invariant I1/I4b): the physical address is payload, so a relocation
//! that re-points an entry, or an owner compaction that repacks members into
//! different frames, cannot change which write wins. Two positions that differ
//! only in their address or payload CRC compare equal and hash equal; the
//! compare-and-set that installs a relocation therefore keeps the first
//! winner.
//!
//! The encoded (flat-row) layout is exactly [`VersionPosition::LENGTH`] = 48
//! bytes:
//!
//! ```text
//! [kind<<30 | operation_index : u32][batch epoch : u64][batch sequence : u64]
//! [payload_crc : u32][partition : u32][segment generation : u64]
//! [member offset : u64][member len : u32]
//! ```
//!
//! `kind` occupies the top two bits of the first word, as the native flat row
//! does, so the operation index must stay below `2^30` (a batch never comes
//! close; the publication path bounds batch members).

use crate::crc::CrcFrame;
use crate::distributed::storage::LocalDbError;
use crate::distributed::types::{
    BatchVersion, MutationVersion, PhysicalValueAddress, RecordDescriptor,
};
use crate::wal::position::{IndexCut, IndexEntryKind, IndexPosition};
use bytes::{Buf, BufMut};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// Bits reserved for the entry kind in the first flat word.
const KIND_SHIFT: u32 = 30;
/// Mask of the operation index in the first flat word.
const OPERATION_MASK: u32 = (1 << KIND_SHIFT) - 1;

// CRCs are stored next to the identity but are not part of it.

/// A value position in the hybrid index: logical version plus member address.
///
/// `payload_crc` is the CRC-32 of the member payload after its native 8-byte
/// frame header. It is carried in the index so a replayed or relocated entry
/// can be checked against the member it names without re-deriving the batch.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct VersionPosition {
    /// Logical batch version `(epoch, sequence)`.
    pub(crate) batch: BatchVersion,
    /// The member's logical index inside its batch; orders repeated keys.
    pub(crate) operation_index: u32,
    /// CRC-32 of the member payload (after the native frame header).
    ///
    /// `0` marks a non-member cutoff (`batch_start`) or the flusher's
    /// placeholder (`tombstone`); a stored member's CRC is always compared,
    /// never used as a marker, so a member whose real CRC is 0 is still valid.
    pub(crate) payload_crc: u32,
    /// Physical location of the member frame in its owner's value WAL.
    pub(crate) address: PhysicalValueAddress,
}

impl VersionPosition {
    /// The only constructor from a published batch member.
    ///
    /// `batch_address` locates the complete batch frame; `descriptor` locates
    /// the member inside the batch payload (offsets exclude the outer native
    /// CRC header). The resulting address points at the member frame itself,
    /// so a read fetches exactly the member bytes and the native CRC check
    /// covers the member, not the whole batch.
    pub(crate) fn member(
        batch: BatchVersion,
        batch_address: PhysicalValueAddress,
        operation_index: u32,
        descriptor: RecordDescriptor,
        payload_crc: u32,
    ) -> Self {
        // Both terms are validated before publication: the batch address and
        // length cover the frame, and the descriptor offset lies inside the
        // batch payload. Plain addition cannot overflow for valid input; a
        // corrupt address is not a fallible user path.
        //
        // `descriptor.checksum` is the on-disk identity checksum, not the
        // payload CRC; this constructor never uses it for `payload_crc`.
        let offset = batch_address.offset
            + CrcFrame::CRC_HEADER_LENGTH as u64
            + u64::from(descriptor.offset);
        debug_assert!(
            batch_address
                .offset
                .checked_add(CrcFrame::CRC_HEADER_LENGTH as u64)
                .and_then(|offset| offset.checked_add(u64::from(descriptor.offset)))
                .is_some(),
            "replay must use member_checked for peer-provided addresses",
        );
        Self::from_member(
            batch,
            operation_index,
            PhysicalValueAddress {
                partition: batch_address.partition,
                segment_generation: batch_address.segment_generation,
                offset,
                len: descriptor.len,
            },
            payload_crc,
        )
    }

    /// Fallible member constructor for the replay path.
    ///
    /// The batch address and member offset of a replayed frame are peer data,
    /// not validated publication input; an overflow is a bad frame, not a
    /// corruption to report later. The result is byte-identical to
    /// [`Self::member`] for every valid address.
    pub(crate) fn member_checked(
        batch: BatchVersion,
        batch_address: PhysicalValueAddress,
        operation_index: u32,
        descriptor: RecordDescriptor,
        payload_crc: u32,
    ) -> Result<Self, LocalDbError> {
        let offset = batch_address
            .offset
            .checked_add(CrcFrame::CRC_HEADER_LENGTH as u64)
            .and_then(|offset| offset.checked_add(u64::from(descriptor.offset)))
            .ok_or_else(|| {
                LocalDbError::new(format!(
                    "batch address {} plus member offset {} overflows u64",
                    batch_address.offset, descriptor.offset
                ))
            })?;
        Ok(Self::from_member(
            batch,
            operation_index,
            PhysicalValueAddress {
                partition: batch_address.partition,
                segment_generation: batch_address.segment_generation,
                offset,
                len: descriptor.len,
            },
            payload_crc,
        ))
    }

    /// Builds a position from an already-computed member address.
    ///
    /// Replay and publication must produce byte-identical positions;
    /// [`Self::member`] computes the member address and delegates here, so the
    /// two paths cannot drift.
    pub(crate) const fn from_member(
        batch: BatchVersion,
        operation_index: u32,
        member_address: PhysicalValueAddress,
        payload_crc: u32,
    ) -> Self {
        Self {
            batch,
            operation_index,
            payload_crc,
            address: member_address,
        }
    }

    /// Cutoff anchor for a batch (`BatchStart`/`DropCells` replay bounds).
    ///
    /// It sorts at member 0 of the batch and names the batch frame, so it is
    /// logically equal to a real member 0 but physically different
    /// ([`Self::same_physical`] is false). It is a cutoff, never a buffered or
    /// stored member: the publication and replay paths debug-assert that it is
    /// not buffered. `DropCells` builds its own real [`Self::member`] position.
    pub(crate) const fn batch_start(
        batch: BatchVersion,
        batch_address: PhysicalValueAddress,
    ) -> Self {
        Self {
            batch,
            operation_index: 0,
            payload_crc: 0,
            address: batch_address,
        }
    }

    /// Physical identity: the same write at the same address with the same
    /// payload. Every compare-and-set uses this, never `==`, because logical
    /// equality deliberately ignores the address (I4b).
    pub(crate) fn same_physical(&self, other: &Self) -> bool {
        self.batch == other.batch
            && self.operation_index == other.operation_index
            && self.payload_crc == other.payload_crc
            && self.address == other.address
    }

    /// The same logical write re-pointed at a relocated member copy.
    #[cfg(test)]
    pub(crate) const fn moved_to(self, address: PhysicalValueAddress) -> Self {
        Self {
            batch: self.batch,
            operation_index: self.operation_index,
            payload_crc: self.payload_crc,
            address,
        }
    }

    /// Placeholder position at an exclusive cut.
    ///
    /// The native flusher uses the current WAL position as a valid
    /// non-inserted position for sharded-promote tombstones. The hybrid uses
    /// the published cut with the codec's maximum operation index and a
    /// one-byte address: it sorts after every real member of that batch, so it
    /// never ties member 0, and it is a **placeholder, never a cutoff**. It is
    /// not below the cut itself.
    pub(crate) const fn tombstone(cut: BatchVersion) -> Self {
        Self {
            batch: cut,
            operation_index: OPERATION_MASK,
            payload_crc: 0,
            address: PhysicalValueAddress {
                partition: 0,
                segment_generation: 0,
                offset: 0,
                len: 1,
            },
        }
    }

    /// The write's logical identity (batch plus operation index), the version
    /// a read checks against the decoded batch header.
    pub(crate) const fn identity(self) -> MutationVersion {
        MutationVersion {
            batch: self.batch,
            operation_index: self.operation_index,
        }
    }

    /// False only for the invalid sentinel (batch `NONE`). Inherent so callers
    /// need not import the `IndexPosition` trait; the trait method delegates.
    pub(crate) fn is_valid(self) -> bool {
        self.batch != <BatchVersion as IndexCut>::NONE
    }

    /// Logical identity: the order key and the equality/hash basis.
    const fn key(self) -> (BatchVersion, u32) {
        (self.batch, self.operation_index)
    }
}

impl PartialEq for VersionPosition {
    /// Logical identity only; physical identity is [`VersionPosition::same_physical`].
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for VersionPosition {}

impl PartialOrd for VersionPosition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VersionPosition {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

impl Hash for VersionPosition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key().hash(state);
    }
}

/// A value position plus the index entry kind, the flat-row unit.
///
/// The address and payload CRC are payload; ordering and equality use
/// `(batch, operation_index, kind)` so the tagged value agrees with
/// [`VersionPosition`] and only distinguishes the entry state.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TaggedVersionPosition {
    position: VersionPosition,
    kind: IndexEntryKind,
}

impl TaggedVersionPosition {
    pub(crate) const fn new(position: VersionPosition, kind: IndexEntryKind) -> Self {
        Self { position, kind }
    }

    pub(crate) const fn position(self) -> VersionPosition {
        self.position
    }

    pub(crate) const fn kind(self) -> IndexEntryKind {
        self.kind
    }

    const fn key(self) -> (BatchVersion, u32, IndexEntryKind) {
        (
            self.position.batch,
            self.position.operation_index,
            self.kind,
        )
    }
}

impl PartialEq for TaggedVersionPosition {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for TaggedVersionPosition {}

impl PartialOrd for TaggedVersionPosition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaggedVersionPosition {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

impl Hash for TaggedVersionPosition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key().hash(state);
    }
}

impl IndexPosition for VersionPosition {
    type Cut = BatchVersion;
    type Tagged = TaggedVersionPosition;

    const LENGTH: usize = 48;

    const INVALID: Self = Self {
        batch: <BatchVersion as IndexCut>::NONE,
        // Zero, not u32::MAX: the flat codec reserves the top two bits of this
        // word for the kind, so the sentinel must still be encodable.
        operation_index: 0,
        payload_crc: 0,
        address: PhysicalValueAddress {
            partition: 0,
            segment_generation: 0,
            offset: u64::MAX,
            len: u32::MAX,
        },
    };

    fn is_valid(self) -> bool {
        VersionPosition::is_valid(self)
    }

    fn frontier(self) -> Self::Cut {
        self.batch
    }

    fn below(self, cut: Self::Cut) -> bool {
        // The invalid sentinel is never below a real cut (native's
        // `WalPosition::INVALID` has offset u64::MAX for the same reason), and
        // `below(NONE)` is false for every position.
        self.is_valid() && cut != <BatchVersion as IndexCut>::NONE && self.batch < cut
    }

    fn precedes(self, other: Self) -> bool {
        // Strict logical write order. Address-only differences (a relocation
        // re-point) are not a new write.
        self.key() < other.key()
    }

    fn write_flat<B: BufMut>(&self, kind: u8, buf: &mut B) {
        assert!(
            self.operation_index <= OPERATION_MASK,
            "operation_index {} does not fit below the flat row kind bits",
            self.operation_index,
        );
        assert!(kind < 4, "entry kind {kind} does not fit in two bits");
        buf.put_u32((u32::from(kind) << KIND_SHIFT) | self.operation_index);
        buf.put_u64(self.batch.epoch);
        buf.put_u64(self.batch.sequence);
        buf.put_u32(self.payload_crc);
        buf.put_u32(self.address.partition);
        buf.put_u64(self.address.segment_generation);
        buf.put_u64(self.address.offset);
        buf.put_u32(self.address.len);
    }

    fn read_flat<B: Buf>(buf: &mut B) -> (Self, u8) {
        let packed = buf.get_u32();
        let kind = (packed >> KIND_SHIFT) as u8;
        let operation_index = packed & OPERATION_MASK;
        let epoch = buf.get_u64();
        let sequence = buf.get_u64();
        let payload_crc = buf.get_u32();
        let partition = buf.get_u32();
        let segment_generation = buf.get_u64();
        let offset = buf.get_u64();
        let len = buf.get_u32();
        (
            Self {
                batch: BatchVersion { epoch, sequence },
                operation_index,
                payload_crc,
                address: PhysicalValueAddress {
                    partition,
                    segment_generation,
                    offset,
                    len,
                },
            },
            kind,
        )
    }

    fn tag(self, kind: IndexEntryKind) -> Self::Tagged {
        TaggedVersionPosition::new(self, kind)
    }

    fn untag(tagged: Self::Tagged) -> (Self, IndexEntryKind) {
        (tagged.position(), tagged.kind())
    }

    /// Physical identity of two index entries: the same write at the same
    /// address with the same payload checksum and the same kind, compared
    /// exactly as relocation does ([`VersionPosition::same_physical`]). The
    /// tagged `==` is logical (identity and kind only) and ignores the
    /// address, so a relocation re-point that changes only the address would
    /// otherwise look unchanged and never be applied.
    fn tagged_same_physical(a: Self::Tagged, b: Self::Tagged) -> bool {
        a.kind() == b.kind() && a.position().same_physical(&b.position())
    }

    /// A flush commit never moves a cell's frontier back. The hybrid loader's
    /// `last_processed` can fall (a latch such as the `NONE` latch of a pinned
    /// checkpoint pins it below an earlier commit), and a cell's frontier feeds
    /// the snapshot's `replay_from`; a transient low commit must not drag it
    /// below what the cell has already stored. Native positions keep the plain
    /// store.
    fn committed_cut(old: BatchVersion, new: BatchVersion) -> BatchVersion {
        old.max(new)
    }

    /// An `Empty` cell is anchored at its first write's batch, bounded by the
    /// loader's `applied` cut. Owner workers publish in durable-arrival order,
    /// so a lower version can reach the same cell after a higher one; every
    /// version below `applied` is already in the index, so any write still to
    /// arrive is at or above it and the anchor never sits above one.
    /// Native positions keep `first.frontier()` and never call `bound`.
    fn anchor_cut(first: Self, bound: impl FnOnce() -> BatchVersion) -> BatchVersion {
        first.batch.min(bound())
    }

    /// On-disk row: byte-identical to a clean flat row (kind 0). The index table
    /// decoder hands a tombstone-free blob straight to its flat buffer
    /// (`IndexTable::deserialize_fixed_size`), so the two layouts must agree
    /// byte for byte.
    /// A tombstone is the invalid sentinel, written verbatim (including
    /// `len = u32::MAX`), exactly as native writes its own sentinel; its row is
    /// still a kind-0 flat row, and the decoder routes it through the slow path.
    fn write_blob<B: BufMut>(&self, buf: &mut B) {
        self.write_flat(0, buf);
    }

    fn read_blob<B: Buf>(buf: &mut B) -> Self {
        let (position, kind) = Self::read_flat(buf);
        debug_assert_eq!(kind, 0, "an on-disk row carries no entry kind");
        position
    }
}

const _: () =
    assert!(std::mem::size_of::<VersionPosition>() == <VersionPosition as IndexPosition>::LENGTH);

/// Encoded width of one [`VersionPosition`] in a flat index row. Exposed for
/// callers outside the engine that size coordinator index storage (the replay
/// benchmark's disk reservation); derived from the codec, never repeated.
pub const HYBRID_POSITION_ENCODED_LEN: usize = <VersionPosition as IndexPosition>::LENGTH;

impl IndexCut for BatchVersion {
    /// `(0, 0)` is reserved: fencing generations start at 1, so no real
    /// position carries it. A cut of `NONE` means "nothing is processed".
    const NONE: Self = Self {
        epoch: 0,
        sequence: 0,
    };

    /// Hybrid control-region format (native `LastProcessed` keeps 3).
    const CONTROL_REGION_VERSION: u32 = 4;

    fn saturating_lag(self, _later: Self) -> u64 {
        // Metrics only. Sequences restart per fencing generation, so a
        // cross-epoch difference is not a batch count and a same-epoch one is
        // not a byte distance; report no lag rather than a misleading number.
        0
    }

    fn byte_position(self) -> u64 {
        // Version cuts carry no byte position. The byte cadence is a separate
        // scalar (`cell_bytes` in control-region V4 and the publication byte
        // counter), never a property of a cut.
        0
    }
}

#[cfg(test)]
mod tests;
