//! Test-only second instantiation of [`IndexPosition`].
//!
//! The native index stack is generic over the position type, and the only
//! shipped instantiation is [`WalPosition`]. A test that must tell "generic"
//! from "accidentally native" needs a position whose cut has *no byte
//! meaning*, as a version-carrying engine's cut has none. [`TestPosition`]
//! reuses the native row layout (so the codec, flat buffers and blobs are the
//! real ones) but frontiers it with [`TestCut`], whose `byte_position` is 0.
//! Any code that still derives a byte cadence from the cut therefore stores 0
//! for it, which the cadence tests assert against.

use super::position::{IndexCut, IndexEntryKind, IndexPosition, WalPosition};
use crate::index::index_table::TaggedWalPosition;
use crate::large_table::Loader;
use crate::wal::WalRandomRead;
use crate::{
    index::index_table::IndexTable,
    key_shape::{KeySpace, KeySpaceDesc},
};
use bytes::{Buf, BufMut};
use minibytes::Bytes;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::marker::PhantomData;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Hash, Serialize, Deserialize)]
pub(crate) struct TestCut(pub(crate) u64);

impl IndexCut for TestCut {
    const NONE: Self = Self(0);
    const CONTROL_REGION_VERSION: u32 = 9;

    fn saturating_lag(self, later: Self) -> u64 {
        later.0.saturating_sub(self.0)
    }

    /// No byte meaning: a cadence derived from here is 0.
    fn byte_position(self) -> u64 {
        0
    }
}

/// A native-layout position that carries a [`TestCut`] frontier.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Hash)]
pub(crate) struct TestPosition(pub(crate) WalPosition);

impl IndexPosition for TestPosition {
    type Cut = TestCut;
    type Tagged = TaggedWalPosition;

    const LENGTH: usize = WalPosition::LENGTH;
    const INVALID: Self = Self(WalPosition::INVALID);

    fn is_valid(self) -> bool {
        self.0.is_valid()
    }

    fn frontier(self) -> TestCut {
        TestCut(self.0.offset())
    }

    fn below(self, cut: TestCut) -> bool {
        self.0.offset() < cut.0
    }

    fn precedes(self, other: Self) -> bool {
        self.0.offset() < other.0.offset()
    }

    fn tag(self, kind: IndexEntryKind) -> TaggedWalPosition {
        self.0.tag(kind)
    }

    fn untag(tagged: TaggedWalPosition) -> (Self, IndexEntryKind) {
        let (position, kind) = WalPosition::untag(tagged);
        (Self(position), kind)
    }

    fn from_disk_position(position: Self) -> TaggedWalPosition {
        WalPosition::from_disk_position(position.0)
    }

    fn write_flat<B: BufMut>(&self, kind: u8, buf: &mut B) {
        self.0.write_flat(kind, buf);
    }

    fn read_flat<B: Buf>(buf: &mut B) -> (Self, u8) {
        let (position, kind) = WalPosition::read_flat(buf);
        (Self(position), kind)
    }

    fn write_blob<B: BufMut>(&self, buf: &mut B) {
        self.0.write_blob(buf);
    }

    fn read_blob<B: Buf>(buf: &mut B) -> Self {
        Self(WalPosition::read_blob(buf))
    }
}

/// A [`Loader`] for the replay and cadence tests: no disk, no flushing. Its
/// `flushed_at_bytes` is the supplied mapping, so a test chooses a cadence
/// that is distinguishable from the cut's own byte position.
pub(crate) struct CadenceLoader<P: IndexPosition> {
    cadence: fn(P::Cut) -> u64,
    position: PhantomData<fn() -> P>,
}

impl<P: IndexPosition> CadenceLoader<P> {
    pub(crate) fn new(cadence: fn(P::Cut) -> u64) -> Self {
        Self {
            cadence,
            position: PhantomData,
        }
    }
}

impl<P: IndexPosition> Loader for CadenceLoader<P> {
    type Position = P;
    type Error = Infallible;

    fn load(
        &self,
        _ks: &KeySpaceDesc,
        _position: WalPosition,
    ) -> Result<IndexTable<P>, Infallible> {
        Ok(IndexTable::default())
    }

    fn index_reader(&self, _position: WalPosition) -> Result<WalRandomRead, Infallible> {
        Ok(WalRandomRead::Mapped(Bytes::new()))
    }

    fn flush_supported(&self) -> bool {
        false
    }

    fn flush(&self, _ks: KeySpace, _data: &IndexTable<P>) -> Result<WalPosition, Infallible> {
        unreachable!("the cadence loader never flushes")
    }

    fn last_processed(&self) -> P::Cut {
        P::Cut::NONE
    }

    fn current(&self) -> P::Cut {
        P::Cut::NONE
    }

    fn min_position(&self) -> P::Cut {
        P::Cut::NONE
    }

    fn current_position(&self) -> P {
        P::INVALID
    }

    fn flushed_at_bytes(&self, cut: P::Cut) -> u64 {
        (self.cadence)(cut)
    }
}

/// A position that orders by `ordinal` alone, as a version-carrying position
/// orders by version, while also carrying a stored `address`. Two values with
/// the same ordinal and different addresses are `==` yet physically distinct:
/// exactly the shape of a relocation re-point.
#[derive(Copy, Clone, Debug)]
pub(crate) struct LogicalPosition {
    pub(crate) ordinal: u64,
    pub(crate) address: u32,
}

impl LogicalPosition {
    pub(crate) fn new(ordinal: u64, address: u32) -> Self {
        Self { ordinal, address }
    }
}

impl PartialEq for LogicalPosition {
    fn eq(&self, other: &Self) -> bool {
        self.ordinal == other.ordinal
    }
}
impl Eq for LogicalPosition {}
impl PartialOrd for LogicalPosition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for LogicalPosition {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ordinal.cmp(&other.ordinal)
    }
}
impl std::hash::Hash for LogicalPosition {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ordinal.hash(state);
    }
}

/// The kind-bearing form of [`LogicalPosition`]; orders and compares by
/// ordinal only.
#[derive(Copy, Clone, Debug)]
pub(crate) struct LogicalTagged {
    pub(crate) position: LogicalPosition,
    pub(crate) kind: IndexEntryKind,
}

impl PartialEq for LogicalTagged {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position
    }
}
impl Eq for LogicalTagged {}
impl PartialOrd for LogicalTagged {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for LogicalTagged {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.position.cmp(&other.position)
    }
}
impl std::hash::Hash for LogicalTagged {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.position.hash(state);
    }
}

impl IndexPosition for LogicalPosition {
    type Cut = TestCut;
    type Tagged = LogicalTagged;

    // [ordinal: u64][address | kind << 30: u32], the native flat row size.
    const LENGTH: usize = 12;
    const INVALID: Self = Self {
        ordinal: u64::MAX,
        address: u32::MAX,
    };

    fn is_valid(self) -> bool {
        self.ordinal != u64::MAX
    }

    fn frontier(self) -> TestCut {
        TestCut(self.ordinal)
    }

    fn below(self, cut: TestCut) -> bool {
        self.ordinal < cut.0
    }

    fn precedes(self, other: Self) -> bool {
        self.ordinal < other.ordinal
    }

    fn tag(self, kind: IndexEntryKind) -> LogicalTagged {
        LogicalTagged {
            position: self,
            kind,
        }
    }

    fn untag(tagged: LogicalTagged) -> (Self, IndexEntryKind) {
        (tagged.position, tagged.kind)
    }

    fn write_flat<B: BufMut>(&self, kind: u8, buf: &mut B) {
        buf.put_u64(self.ordinal);
        buf.put_u32(self.address | (u32::from(kind) << 30));
    }

    fn read_flat<B: Buf>(buf: &mut B) -> (Self, u8) {
        let ordinal = buf.get_u64();
        let word = buf.get_u32();
        (Self::new(ordinal, word & 0x3FFF_FFFF), (word >> 30) as u8)
    }

    fn write_blob<B: BufMut>(&self, buf: &mut B) {
        buf.put_u64(self.ordinal);
        buf.put_u32(self.address);
    }

    fn read_blob<B: Buf>(buf: &mut B) -> Self {
        let ordinal = buf.get_u64();
        Self::new(ordinal, buf.get_u32())
    }

    fn tagged_same_physical(a: LogicalTagged, b: LogicalTagged) -> bool {
        a.position.ordinal == b.position.ordinal
            && a.position.address == b.position.address
            && a.kind == b.kind
    }

    /// The hybrid policy: a commit never moves a cell's frontier back.
    fn committed_cut(old: TestCut, new: TestCut) -> TestCut {
        old.max(new)
    }

    /// The hybrid policy: the anchor never sits above the loader's insertion
    /// bound, because a lower version can still arrive in the same cell.
    fn anchor_cut(first: LogicalPosition, bound: impl FnOnce() -> TestCut) -> TestCut {
        first.frontier().min(bound())
    }
}
